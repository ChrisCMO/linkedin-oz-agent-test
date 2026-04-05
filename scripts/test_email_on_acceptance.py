"""
Dummy test — Email notification on acceptance detection.

Seeds a fake "sent" invitation pointing at Christopher Castro (already a
1st-degree connection on Laikah's account).  When poll_acceptance() runs,
Unipile sees FIRST_DEGREE → marks accepted → fires the email to
christopher@yorcmo.com.

Usage:
    python scripts/test_email_on_acceptance.py
"""

import sys
from pathlib import Path

# Ensure project root is on sys.path so `mvp.backend.*` imports work
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("test_email_on_acceptance")

from mvp.backend.config import get_sb, get_unipile
from mvp.backend.services.outreach_svc import poll_acceptance

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LAIKAH_UNIPILE_ID = "_hq8nPyLRyeuJVOYpO3xCA"
CHRISTOPHER_PROVIDER_ID = "ACoAAB6xqSMBRD76tkWFiBXHGlZOH5FhDBG4DKA"


def main():
    sb = get_sb()
    uni = get_unipile()

    # ------------------------------------------------------------------
    # 0. Pre-flight: verify Christopher is FIRST_DEGREE on Laikah's account
    # ------------------------------------------------------------------
    logger.info("Pre-flight: checking Christopher's network distance …")
    try:
        profile = uni.get_profile(CHRISTOPHER_PROVIDER_ID, LAIKAH_UNIPILE_ID)
        distance = profile.get("network_distance")
        is_rel = profile.get("is_relationship")
        logger.info("  network_distance=%s  is_relationship=%s", distance, is_rel)
        if distance != "FIRST_DEGREE" and is_rel is not True:
            logger.error(
                "Christopher is NOT first-degree connected to Laikah — "
                "test will not detect acceptance. Aborting."
            )
            sys.exit(1)
        logger.info("Pre-flight PASSED — Christopher is 1st-degree connected.")
    except Exception as e:
        logger.error("Pre-flight Unipile call failed: %s — aborting.", e)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Look up Laikah's linkedin_accounts row
    # ------------------------------------------------------------------
    logger.info("Looking up Laikah's linkedin_accounts row …")
    la_result = (
        sb.table("linkedin_accounts")
        .select("id, tenant_id, owner_name")
        .eq("unipile_account_id", LAIKAH_UNIPILE_ID)
        .limit(1)
        .execute()
    )
    if not la_result.data:
        logger.error("Laikah's linkedin_accounts row not found — aborting.")
        sys.exit(1)

    la = la_result.data[0]
    linkedin_account_id = la["id"]
    tenant_id = la["tenant_id"]
    logger.info("Found: id=%s  tenant=%s  owner=%s", linkedin_account_id, tenant_id, la["owner_name"])

    # ------------------------------------------------------------------
    # 2. Create test campaign
    # ------------------------------------------------------------------
    logger.info("Creating test campaign …")
    c_result = (
        sb.table("campaigns")
        .insert({
            "name": "Email Notification Test",
            "linkedin_account_id": linkedin_account_id,
            "tenant_id": tenant_id,
            "status": "active",
        })
        .execute()
    )
    campaign_id = c_result.data[0]["id"]
    logger.info("Campaign created: %s", campaign_id)

    # ------------------------------------------------------------------
    # 3. Insert test prospect
    # ------------------------------------------------------------------
    logger.info("Inserting test prospect (Christopher Castro) …")
    p_result = (
        sb.table("prospects")
        .insert({
            "first_name": "Christopher",
            "last_name": "Castro",
            "title": "Test CFO",
            "company_name": "Test Corp",
            "linkedin_provider_id": CHRISTOPHER_PROVIDER_ID,
            "status": "invite_sent",
            "campaign_id": campaign_id,
            "icp_score": 85,
            "icp_reasoning": "Dummy test — verifying email notification flow",
            "tenant_id": tenant_id,
        })
        .execute()
    )
    prospect_id = p_result.data[0]["id"]
    logger.info("Prospect created: %s", prospect_id)

    # ------------------------------------------------------------------
    # 4. Insert "sent" invitation
    # ------------------------------------------------------------------
    logger.info("Inserting sent invitation …")
    i_result = (
        sb.table("invitations")
        .insert({
            "prospect_id": prospect_id,
            "campaign_id": campaign_id,
            "linkedin_account_id": linkedin_account_id,
            "provider_id": CHRISTOPHER_PROVIDER_ID,
            "status": "sent",
            "tenant_id": tenant_id,
        })
        .execute()
    )
    invitation_id = i_result.data[0]["id"]
    logger.info("Invitation created: %s", invitation_id)

    # ------------------------------------------------------------------
    # 5. Run poll_acceptance
    # ------------------------------------------------------------------
    logger.info("Running poll_acceptance …")
    result = poll_acceptance(campaign_id, tenant_id)
    logger.info("poll_acceptance result: %s", result)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"  campaign_id    : {campaign_id}")
    print(f"  prospect_id    : {prospect_id}")
    print(f"  invitation_id  : {invitation_id}")
    print(f"  newly_accepted : {result.get('newly_accepted')}")
    print(f"  checked        : {result.get('checked')}")
    print(f"  still_pending  : {result.get('still_pending')}")
    print(f"  errors         : {result.get('errors')}")

    # ------------------------------------------------------------------
    # 6. Check email_notifications table
    # ------------------------------------------------------------------
    n_result = (
        sb.table("email_notifications")
        .select("id, status, error_message, subject, created_at")
        .eq("prospect_id", prospect_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if n_result.data:
        notif = n_result.data[0]
        print(f"\n  Email notification:")
        print(f"    id           : {notif['id']}")
        print(f"    status       : {notif['status']}")
        print(f"    subject      : {notif['subject']}")
        print(f"    error        : {notif.get('error_message')}")
        print(f"    created_at   : {notif['created_at']}")
    else:
        print("\n  No email_notifications row found for this prospect.")

    print("\n  NOTE: Test data persists (campaign, prospect, invitation).")
    print("        Visible in Campaigns/Prospects pages. Delete manually if needed.")
    print("=" * 60)


if __name__ == "__main__":
    main()
