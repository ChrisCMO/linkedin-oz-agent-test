"""Skill 2: invite-sender — Send bare LinkedIn connection invites with rate limiting.

Triggered by: Oz API call from approve-batch Edge Function + daily cron (10 AM PT weekdays).
Sends bare invites (NO connection notes) for approved prospects, max 5/day.
"""

import logging
import sys

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


def get_approved_prospects(sb, linkedin_account_id: str, limit: int = 10) -> list[dict]:
    """Get approved prospects ordered by ICP score, ready for invites."""
    result = (
        sb.table("prospects")
        .select("*")
        .eq("linkedin_account_id", linkedin_account_id)
        .eq("status", "approved")
        .order("scoring->>score", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def preflight_check(unipile: UnipileClient, account_id: str, prospect: dict) -> str | None:
    """Check if prospect is already connected. Returns 'skip' reason or None if OK."""
    provider_id = prospect.get("linkedin_provider_id")
    slug = prospect.get("linkedin_slug")
    identifier = provider_id or slug

    if not identifier:
        return "no_linkedin_id"

    try:
        profile = unipile.get_profile(identifier, account_id)
        distance = profile.get("network_distance", "")
        if distance == "FIRST_DEGREE" or profile.get("is_relationship"):
            return "already_connected"
    except Exception as e:
        logger.warning("Profile lookup failed for %s: %s", identifier, e)
        return "lookup_failed"

    return None


def send_invite_for_prospect(
    sb,
    unipile: UnipileClient,
    account_id: str,
    prospect: dict,
    tenant_id: str,
) -> bool:
    """Send a bare invite for one prospect. Returns True if sent."""
    prospect_id = prospect["id"]
    campaign_id = prospect["campaign_id"]
    provider_id = prospect.get("linkedin_provider_id")
    slug = prospect.get("linkedin_slug")

    # Pre-flight: check connection status
    skip_reason = preflight_check(unipile, account_id, prospect)

    if skip_reason == "already_connected":
        logger.info("Already connected to %s %s — marking connected",
                     prospect.get("first_name"), prospect.get("last_name"))
        sb.table("prospects").update({
            "status": "connected",
            "status_changed_at": "now()",
        }).eq("id", prospect_id).execute()
        return False

    if skip_reason:
        logger.warning("Skipping %s: %s", prospect.get("linkedin_slug"), skip_reason)
        return False

    # Use provider_id if available, otherwise slug
    target_id = provider_id or slug

    # Send bare invite (NO note)
    try:
        result = unipile.send_invite(
            account_id=account_id,
            provider_id=target_id,
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )
    except Exception as e:
        logger.error("Invite failed for %s: %s", target_id, e)
        return False

    # Extract invitation ID from response
    external_id = result.get("id") or result.get("invitation_id")

    # Insert invitation record
    sb.table("invitations").insert({
        "tenant_id": tenant_id,
        "linkedin_account_id": account_id,
        "prospect_id": prospect_id,
        "campaign_id": campaign_id,
        "provider_id": target_id,
        "status": "sent",
        "external_invitation_id": external_id,
    }).execute()

    # Update prospect status
    sb.table("prospects").update({
        "status": "invite_sent",
        "status_changed_at": "now()",
    }).eq("id", prospect_id).execute()

    # Log event
    log_event(
        tenant_id=tenant_id,
        event_type="invite_sent",
        actor="agent:invite-sender",
        data={"provider_id": target_id, "bare_invite": True},
        campaign_id=campaign_id,
        prospect_id=prospect_id,
    )

    logger.info("Sent bare invite to %s %s (%s)",
                 prospect.get("first_name"), prospect.get("last_name"), target_id)
    return True


def run():
    """Main entry point for the invite-sender skill."""
    sb = get_supabase()
    tenant_id = config.DEFAULT_TENANT_ID

    if not is_business_hours():
        print("Outside business hours — skipping invite run")
        return

    accounts = get_active_accounts(tenant_id)
    if not accounts:
        print("No active LinkedIn accounts found")
        return

    total_sent = 0

    for account in accounts:
        account_id = account["id"]
        provider_account_id = account["provider_account_id"]
        owner = account["owner_name"]

        # Check rate limit
        if not check_rate_limit(account_id, "connection"):
            logger.info("Rate limit reached for %s (%s) — skipping", owner, account_id)
            print(f"Rate limit reached for {owner} — skipping")
            continue

        unipile = UnipileClient(tenant_id=tenant_id)

        # Get approved prospects for this account
        prospects = get_approved_prospects(sb, account_id)
        if not prospects:
            logger.info("No approved prospects for %s", owner)
            continue

        sent_count = 0
        for prospect in prospects:
            # Re-check rate limit before each invite
            if not check_rate_limit(account_id, "connection"):
                logger.info("Daily limit reached for %s after %d invites", owner, sent_count)
                break

            success = send_invite_for_prospect(sb, unipile, provider_account_id, prospect, tenant_id)
            if success:
                sent_count += 1
                total_sent += 1

                # Random delay between invites
                if sent_count < len(prospects):
                    random_delay(config.INVITE_DELAY_RANGE)

        print(f"Sent {sent_count} invites for {owner}")

    print(f"Total invites sent: {total_sent}")


def main():
    setup_logging()
    try:
        run()
    except Exception as e:
        logger.error("invite_sender failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
