"""Skill 2: invite-sender — Send bare LinkedIn connection invites with rate limiting.

Triggered by: Oz API call from approve-batch Edge Function + daily cron (10 AM PT weekdays).
Sends bare invites (NO connection notes) for approved prospects, max 5/day.
"""

import argparse
import logging
import random
import sys

import config
from db.connect import get_supabase
from lib.unipile import UnipileClient
from skills.helpers import (
    check_rate_limit,
    get_active_accounts,
    get_effective_limit,
    is_business_hours,
    log_event,
    random_delay,
    setup_logging,
)

logger = logging.getLogger(__name__)


def get_approved_prospects(sb, linkedin_account_id: str, limit: int = 10) -> list[dict]:
    """Get approved prospects ordered by ICP score, ready for invites.

    v2 schema: prospects don't have linkedin_account_id — linked via campaign.
    """
    # Find campaigns for this account
    campaigns = sb.table("campaigns").select("id").eq("linkedin_account_id", linkedin_account_id).execute()
    campaign_ids = [c["id"] for c in (campaigns.data or [])]
    if not campaign_ids:
        return []

    result = (
        sb.table("prospects")
        .select("*")
        .in_("campaign_id", campaign_ids)
        .eq("status", "approved")
        .order("icp_score", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


def preflight_check(
    unipile: UnipileClient,
    account_id: str,
    prospect: dict,
    db_account_id: str | None = None,
) -> tuple[str | None, str | None]:
    """Check if prospect is already connected.

    Returns (skip_reason, resolved_provider_id).
    skip_reason is None if OK to proceed; resolved_provider_id is the
    LinkedIn member ID returned by Unipile (needed for send_invite).
    """
    provider_id = prospect.get("linkedin_provider_id")
    slug = prospect.get("linkedin_slug")
    identifier = provider_id or slug

    if not identifier:
        return "no_linkedin_id", None

    try:
        profile = unipile.get_profile(identifier, account_id, db_account_id=db_account_id)
        distance = profile.get("network_distance", "")
        if distance == "FIRST_DEGREE" or profile.get("is_relationship"):
            return "already_connected", None
        # Extract the real LinkedIn member ID from the profile
        resolved_id = profile.get("provider_id") or provider_id or identifier
    except Exception as e:
        logger.warning("Profile lookup failed for %s: %s", identifier, e)
        return "lookup_failed", None

    return None, resolved_id


def send_invite_for_prospect(
    sb,
    unipile: UnipileClient,
    db_account_id: str,
    provider_account_id: str,
    prospect: dict,
    tenant_id: str,
) -> bool:
    """Send a bare invite for one prospect. Returns True if sent.

    Args:
        db_account_id: UUID from linkedin_accounts table (for DB inserts)
        provider_account_id: Unipile account ID (for API calls)

    Mimics human behavior:
    1. View the profile first (like a human would before connecting)
    2. Short pause after viewing (reading the profile)
    3. Then send the bare invite
    """
    prospect_id = prospect["id"]
    campaign_id = prospect["campaign_id"]
    provider_id = prospect.get("linkedin_provider_id")
    slug = prospect.get("linkedin_slug")

    # Step 1: View the profile (pre-flight + human-like behavior)
    skip_reason, resolved_provider_id = preflight_check(
        unipile, provider_account_id, prospect, db_account_id=db_account_id
    )

    if skip_reason == "already_connected":
        logger.info("Already connected to %s %s — marking connected",
                     prospect.get("first_name"), prospect.get("last_name"))
        sb.table("prospects").update({
            "status": "connected",
        }).eq("id", prospect_id).execute()
        return False

    if skip_reason:
        logger.warning("Skipping %s: %s", prospect.get("linkedin_slug"), skip_reason)
        return False

    # Save resolved provider_id back to DB so future runs don't need a slug lookup
    if resolved_provider_id and not provider_id:
        sb.table("prospects").update({
            "linkedin_provider_id": resolved_provider_id,
        }).eq("id", prospect_id).execute()
        logger.info("Saved resolved provider_id %s for prospect %s", resolved_provider_id, prospect_id)

    # Step 2: Pause after viewing profile (simulates reading it — 10-30 seconds)
    random_delay((10, 30))

    # Step 3: Send bare invite (NO note) — use resolved provider_id (not slug)
    target_id = resolved_provider_id or provider_id or slug

    try:
        result = unipile.send_invite(
            account_id=provider_account_id,
            provider_id=target_id,
            db_account_id=db_account_id,
            campaign_id=campaign_id,
            prospect_id=prospect_id,
        )
    except Exception as e:
        logger.error("Invite failed for %s: %s", target_id, e)
        return False

    # Extract invitation ID from response
    external_id = result.get("id") or result.get("invitation_id")

    # Insert invitation record (use DB account ID, not Unipile provider ID)
    sb.table("invitations").insert({
        "tenant_id": tenant_id,
        "linkedin_account_id": db_account_id,
        "prospect_id": prospect_id,
        "campaign_id": campaign_id,
        "provider_id": target_id,
        "status": "sent",
        "external_invitation_id": external_id,
    }).execute()

    # Update prospect status
    sb.table("prospects").update({
        "status": "invite_sent",
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


def run(force: bool = False, limit: int | None = None):
    """Main entry point for the invite-sender skill."""
    sb = get_supabase()
    tenant_id = config.DEFAULT_TENANT_ID

    if not force and not is_business_hours():
        print("Outside business hours — skipping invite run (use --force to override)")
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

        # Randomize today's target — don't always max out the limit
        # A human doesn't send exactly 5 invites every single day
        effective_limit = get_effective_limit(account_id, "connection") or config.MAX_DAILY_INVITES
        today_target = random.randint(max(1, effective_limit - 2), effective_limit)
        if limit is not None:
            today_target = min(limit, today_target)
        logger.info("Today's invite target for %s: %d (limit: %d)", owner, today_target, effective_limit)

        # Get approved prospects for this account
        prospects = get_approved_prospects(sb, account_id, limit=today_target)
        if not prospects:
            logger.info("No approved prospects for %s", owner)
            continue

        sent_count = 0
        for prospect in prospects:
            # Re-check rate limit before each invite
            if not check_rate_limit(account_id, "connection"):
                logger.info("Daily limit reached for %s after %d invites", owner, sent_count)
                break

            # Stop if we hit today's random target
            if sent_count >= today_target:
                logger.info("Hit today's random target (%d) for %s", today_target, owner)
                break

            success = send_invite_for_prospect(sb, unipile, account_id, provider_account_id, prospect, tenant_id)
            if success:
                sent_count += 1
                total_sent += 1

                # Stop early if a hard limit was specified (e.g. --limit 1 for testing)
                if limit is not None and total_sent >= limit:
                    logger.info("Reached --limit %d — stopping", limit)
                    print(f"Reached limit of {limit} invite(s) — stopping early")
                    print(f"Sent {sent_count} invites for {owner}")
                    print(f"Total invites sent: {total_sent}")
                    return

                # Random delay between invites (45-120 seconds — like a human browsing)
                if sent_count < today_target:
                    random_delay(config.INVITE_DELAY_RANGE)

        print(f"Sent {sent_count} invites for {owner}")

    print(f"Total invites sent: {total_sent}")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Send bare LinkedIn invites for approved prospects")
    parser.add_argument("--force", action="store_true", help="Override business hours check")
    parser.add_argument("--limit", type=int, default=None, help="Max number of invites to send (for testing)")
    args = parser.parse_args()
    try:
        run(force=args.force, limit=args.limit)
    except Exception as e:
        logger.error("invite_sender failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
