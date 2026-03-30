"""Skill 3: acceptance-detector — Poll Unipile relations to detect accepted connections.

Triggered by: Cron 3x/day (9 AM, 1 PM, 5 PM PT weekdays).
For each active account: paginate through all connections, compare against 'sent'
invitations, detect new acceptances, create message records, send notification email.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone

import config
from db.connect import get_supabase
from lib.outlook import OutlookClient
from lib.unipile import UnipileClient
from skills.helpers import get_active_accounts, log_event, setup_logging
from templates.acceptance_email import build_acceptance_html

logger = logging.getLogger(__name__)


def get_all_connections(unipile: UnipileClient, provider_account_id: str) -> set[str]:
    """Paginate through all connections and return set of provider_ids."""
    connected = set()
    cursor = None

    while True:
        result = unipile.get_relations(provider_account_id, limit=100, cursor=cursor)
        items = result.get("items", [])
        if not items:
            break

        for item in items:
            pid = item.get("provider_id")
            if pid:
                connected.add(pid)

        cursor = result.get("cursor")
        if not cursor:
            break

    logger.info("Found %d total connections for account %s", len(connected), provider_account_id)
    return connected


def get_pending_invitations(sb, account_id: str) -> list[dict]:
    """Get all 'sent' invitations for an account."""
    result = (
        sb.table("invitations")
        .select("*, prospects(*)")
        .eq("linkedin_account_id", account_id)
        .eq("status", "sent")
        .execute()
    )
    return result.data or []


def create_message_records(
    sb,
    tenant_id: str,
    prospect: dict,
    campaign_id: str,
    linkedin_account_id: str,
    chat_id: str | None = None,
):
    """Create message records (steps 1-3) from pre-generated text in prospect.raw_data."""
    prospect_id = prospect["id"]
    raw_messages = (prospect.get("raw_data") or {}).get("messages", {})

    # Get campaign timing
    camp = sb.table("campaigns").select("timing").eq("id", campaign_id).single().execute()
    timing = (camp.data or {}).get("timing", {})
    msg1_delay = timing.get("msg1_delay_days", 1)
    msg2_delay = timing.get("msg2_delay_days", 14)
    msg3_delay = timing.get("msg3_delay_days", 14)

    now = datetime.now(timezone.utc)
    msg1_schedule = now + timedelta(days=msg1_delay)
    msg2_schedule = msg1_schedule + timedelta(days=msg2_delay)
    msg3_schedule = msg2_schedule + timedelta(days=msg3_delay)

    messages_to_insert = []
    for step, (key, schedule) in enumerate(
        [("msg1", msg1_schedule), ("msg2", msg2_schedule), ("msg3", msg3_schedule)],
        start=1,
    ):
        text = raw_messages.get(key, "")
        if not text:
            continue

        messages_to_insert.append({
            "tenant_id": tenant_id,
            "prospect_id": prospect_id,
            "linkedin_account_id": linkedin_account_id,
            "campaign_id": campaign_id,
            "step": step,
            "original_text": text,
            "approved_text": text,  # Pre-generated = auto-approved for now
            "status": "approved",
            "scheduled_for": schedule.isoformat(),
            "chat_id": chat_id,
        })

    if messages_to_insert:
        sb.table("messages").insert(messages_to_insert).execute()
        logger.info("Created %d message records for prospect %s", len(messages_to_insert), prospect_id)


def send_acceptance_notification(
    prospect: dict,
    company: dict,
    messages: list[dict],
    recipient_email: str,
):
    """Send acceptance notification email to the partner."""
    html = build_acceptance_html(prospect, company, messages)

    first = prospect.get("first_name", "")
    last = prospect.get("last_name", "")
    title = prospect.get("title", "")
    company_name = prospect.get("company_name", "")

    subject = f"New Connection: {first} {last}"
    if title and company_name:
        subject += f", {title} at {company_name}"

    outlook = OutlookClient()
    outlook.send_email(
        to=recipient_email,
        subject=subject,
        html_body=html,
        cc="christopher@yorcmo.com",
    )
    logger.info("Sent acceptance notification for %s %s to %s", first, last, recipient_email)


def process_acceptance(
    sb,
    invitation: dict,
    prospect: dict,
    tenant_id: str,
    connected_set: set[str],
):
    """Process a single detected acceptance."""
    invitation_id = invitation["id"]
    prospect_id = prospect["id"]
    campaign_id = invitation["campaign_id"]
    linkedin_account_id = invitation["linkedin_account_id"]

    # Try to find the chat_id from the relations data
    # (Bare invites may not create a chat immediately)
    chat_id = None

    # Update invitation
    sb.table("invitations").update({
        "status": "accepted",
        "accepted_at": datetime.now(timezone.utc).isoformat(),
        "detection_method": "poll_relations",
        "chat_id": chat_id,
    }).eq("id", invitation_id).execute()

    # Update prospect
    sb.table("prospects").update({
        "status": "connected",
        "status_changed_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", prospect_id).execute()

    # Create message records from pre-generated text
    create_message_records(sb, tenant_id, prospect, campaign_id, linkedin_account_id, chat_id)

    # Load company for notification email
    company = {}
    if prospect.get("company_id"):
        comp_result = sb.table("companies").select("*").eq("id", prospect["company_id"]).single().execute()
        if comp_result.data:
            company = comp_result.data

    # Load messages for notification
    msg_result = (
        sb.table("messages")
        .select("*")
        .eq("prospect_id", prospect_id)
        .order("step")
        .execute()
    )
    messages = msg_result.data or []

    # Get the batch review recipient email for this prospect
    batch_result = (
        sb.table("batch_reviews")
        .select("sent_to_email")
        .contains("prospect_ids", [prospect_id])
        .limit(1)
        .execute()
    )
    recipient_email = "christopher@yorcmo.com"  # Default
    if batch_result.data:
        recipient_email = batch_result.data[0]["sent_to_email"]

    # Send notification email
    try:
        send_acceptance_notification(prospect, company, messages, recipient_email)
    except Exception as e:
        logger.error("Failed to send acceptance notification: %s", e)

    # Log event
    log_event(
        tenant_id=tenant_id,
        event_type="connection_accepted",
        actor="agent:acceptance-detector",
        data={
            "provider_id": invitation.get("provider_id"),
            "detection_method": "poll_relations",
            "invitation_id": invitation_id,
        },
        campaign_id=campaign_id,
        prospect_id=prospect_id,
    )

    logger.info("Acceptance detected: %s %s", prospect.get("first_name"), prospect.get("last_name"))


def run():
    """Main entry point for the acceptance-detector skill."""
    sb = get_supabase()
    tenant_id = config.DEFAULT_TENANT_ID

    accounts = get_active_accounts(tenant_id)
    if not accounts:
        print("No active LinkedIn accounts found")
        return

    total_detected = 0

    for account in accounts:
        account_id = account["id"]
        provider_account_id = account["provider_account_id"]
        owner = account["owner_name"]

        logger.info("Checking acceptance for %s (%s)", owner, account_id)

        unipile = UnipileClient(tenant_id=tenant_id)

        # 1. Get all current connections
        connected_set = get_all_connections(unipile, provider_account_id)

        # 2. Get pending invitations
        pending = get_pending_invitations(sb, account_id)
        if not pending:
            logger.info("No pending invitations for %s", owner)
            continue

        # 3. Check each pending invitation
        detected = 0
        for inv in pending:
            provider_id = inv.get("provider_id")
            if provider_id and provider_id in connected_set:
                prospect = inv.get("prospects")
                if prospect:
                    process_acceptance(sb, inv, prospect, tenant_id, connected_set)
                    detected += 1

            # Update last_checked_at regardless
            sb.table("invitations").update({
                "last_checked_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", inv["id"]).execute()

        total_detected += detected
        print(f"{owner}: {detected} new acceptances detected ({len(pending)} pending checked)")

    print(f"Total acceptances detected: {total_detected}")


def main():
    setup_logging()
    try:
        run()
    except Exception as e:
        logger.error("acceptance_detector failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
