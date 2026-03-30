"""Skill 4: message-sender — Send follow-up message sequences with reply detection.

Triggered by: Daily cron (11 AM PT weekdays).
Finds due messages (approved + scheduled_for <= now), checks for replies first,
sends message, schedules next step.
"""

import logging
import sys
from datetime import datetime, timezone

import config
from db.connect import get_supabase
from lib.unipile import UnipileClient
from skills.helpers import (
    check_rate_limit,
    get_active_accounts,
    is_business_hours,
    log_event,
    random_delay,
    setup_logging,
)

logger = logging.getLogger(__name__)


def get_due_messages(sb, linkedin_account_id: str) -> list[dict]:
    """Get messages that are approved and due for sending."""
    now = datetime.now(timezone.utc).isoformat()
    result = (
        sb.table("messages")
        .select("*, prospects(*)")
        .eq("linkedin_account_id", linkedin_account_id)
        .in_("status", ["approved", "scheduled"])
        .lte("scheduled_for", now)
        .is_("sent_at", "null")
        .order("scheduled_for")
        .execute()
    )
    return result.data or []


def check_for_replies(unipile: UnipileClient, chat_id: str, account_id: str) -> dict | None:
    """Check if prospect has replied in the chat. Returns reply message or None."""
    if not chat_id:
        return None

    try:
        result = unipile.get_messages(chat_id, account_id=account_id)
        messages = result.get("items", [])
        for msg in messages:
            # is_sender: 0 means the OTHER person sent it (a reply)
            if msg.get("is_sender") == 0 or msg.get("is_sender") is False:
                return msg
    except Exception as e:
        logger.warning("Failed to check messages for chat %s: %s", chat_id, e)

    return None


def handle_reply(sb, prospect: dict, message: dict, reply: dict, tenant_id: str):
    """Stop sequence and notify on reply detection."""
    prospect_id = prospect["id"]
    campaign_id = message["campaign_id"]

    # Update prospect to replied
    sb.table("prospects").update({
        "status": "replied",
        "status_changed_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", prospect_id).execute()

    # Cancel all pending messages for this prospect
    sb.table("messages").update({
        "status": "cancelled",
    }).eq("prospect_id", prospect_id).in_("status", ["draft", "pending_approval", "approved", "scheduled"]).execute()

    # Log reply event
    log_event(
        tenant_id=tenant_id,
        event_type="reply_detected",
        actor="agent:message-sender",
        data={
            "reply_text": reply.get("text", "")[:500],
            "chat_id": message.get("chat_id"),
            "detected_before_step": message.get("step"),
        },
        campaign_id=campaign_id,
        prospect_id=prospect_id,
    )

    logger.info("Reply detected from %s %s — sequence stopped",
                 prospect.get("first_name"), prospect.get("last_name"))
    print(f"Reply detected from {prospect.get('first_name')} {prospect.get('last_name')} — sequence stopped")


def send_message(
    sb,
    unipile: UnipileClient,
    message: dict,
    prospect: dict,
    account_id: str,
    provider_account_id: str,
    tenant_id: str,
) -> bool:
    """Send a single message. Returns True if sent successfully."""
    message_id = message["id"]
    prospect_id = prospect["id"]
    campaign_id = message["campaign_id"]
    step = message["step"]
    text = message.get("approved_text") or message.get("original_text")
    chat_id = message.get("chat_id")
    provider_id = prospect.get("linkedin_provider_id") or prospect.get("linkedin_slug")

    if not text:
        logger.warning("No text for message %s step %d — skipping", message_id, step)
        return False

    try:
        if step == 1 and not chat_id:
            # First message — start a new chat
            if not provider_id:
                logger.warning("No provider_id for prospect %s — cannot start chat", prospect_id)
                return False

            result = unipile.start_chat(
                account_id=provider_account_id,
                provider_id=provider_id,
                text=text,
                campaign_id=campaign_id,
                prospect_id=prospect_id,
            )
            chat_id = result.get("chat_id") or result.get("id")

            # Store chat_id on prospect sequence and on the message
            sb.table("prospects").update({
                "sequence": {"chat_id": chat_id, "current_step": step, "started_at": datetime.now(timezone.utc).isoformat()},
                "status": "msg1_sent",
                "status_changed_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", prospect_id).execute()

        else:
            # Follow-up message in existing chat
            if not chat_id:
                logger.warning("No chat_id for follow-up message %s — skipping", message_id)
                return False

            result = unipile.send_followup(
                chat_id=chat_id,
                text=text,
                account_id=provider_account_id,
                campaign_id=campaign_id,
                prospect_id=prospect_id,
            )

            # Update prospect status
            status_map = {2: "msg2_sent", 3: "msg3_sent"}
            new_status = status_map.get(step, f"msg{step}_sent")
            sb.table("prospects").update({
                "status": new_status,
                "status_changed_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", prospect_id).execute()

        # Update message record
        external_id = result.get("message_id") or result.get("id")
        sb.table("messages").update({
            "status": "sent",
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "chat_id": chat_id,
            "external_message_id": external_id,
        }).eq("id", message_id).execute()

        # Update chat_id on subsequent messages for this prospect
        if chat_id:
            sb.table("messages").update({
                "chat_id": chat_id,
            }).eq("prospect_id", prospect_id).is_("chat_id", "null").execute()

        # Log event
        log_event(
            tenant_id=tenant_id,
            event_type="message_sent",
            actor="agent:message-sender",
            data={"step": step, "chat_id": chat_id},
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )

        logger.info("Sent step %d to %s %s", step, prospect.get("first_name"), prospect.get("last_name"))
        return True

    except Exception as e:
        logger.error("Failed to send message %s: %s", message_id, e)
        sb.table("messages").update({
            "status": "failed",
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "failure_reason": str(e)[:500],
        }).eq("id", message_id).execute()
        return False


def run():
    """Main entry point for the message-sender skill."""
    sb = get_supabase()
    tenant_id = config.DEFAULT_TENANT_ID

    if not is_business_hours():
        print("Outside business hours — skipping message run")
        return

    accounts = get_active_accounts(tenant_id)
    if not accounts:
        print("No active LinkedIn accounts found")
        return

    total_sent = 0
    total_replies = 0

    for account in accounts:
        account_id = account["id"]
        provider_account_id = account["provider_account_id"]
        owner = account["owner_name"]

        # Check rate limit for messages
        if not check_rate_limit(account_id, "message"):
            logger.info("Message rate limit reached for %s — skipping", owner)
            continue

        unipile = UnipileClient(tenant_id=tenant_id)

        # Get due messages
        due_messages = get_due_messages(sb, account_id)
        if not due_messages:
            logger.info("No due messages for %s", owner)
            continue

        sent_count = 0
        for msg in due_messages:
            prospect = msg.get("prospects")
            if not prospect:
                continue

            # Check for replies BEFORE sending
            chat_id = msg.get("chat_id") or (prospect.get("sequence") or {}).get("chat_id")
            if chat_id:
                reply = check_for_replies(unipile, chat_id, provider_account_id)
                if reply:
                    handle_reply(sb, prospect, msg, reply, tenant_id)
                    total_replies += 1
                    continue

            # Re-check rate limit
            if not check_rate_limit(account_id, "message"):
                break

            success = send_message(sb, unipile, msg, prospect, account_id, provider_account_id, tenant_id)
            if success:
                sent_count += 1
                total_sent += 1

                # Random delay between messages
                random_delay(config.MESSAGE_DELAY_RANGE)

        print(f"{owner}: {sent_count} messages sent")

    print(f"Total messages sent: {total_sent}, replies detected: {total_replies}")


def main():
    setup_logging()
    try:
        run()
    except Exception as e:
        logger.error("message_sender failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
