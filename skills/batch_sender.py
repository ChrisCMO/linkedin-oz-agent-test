"""Skill 1: batch-sender — Parse Excel/CSV, import prospects, send batch review email.

Triggered by Oz agent from Slack: "@Oz send this to Chris at christopher@yorcmo.com"
The Oz agent extracts the name, email, and file path, then runs:
    python -m skills.batch_sender --file <path> --name "<name>" --email <email>
"""

import argparse
import hashlib
import logging
import secrets
import sys

import pandas as pd

import config
from db.connect import get_supabase
from lib.outlook import OutlookClient
from skills.helpers import log_event, setup_logging
from templates.batch_review_email import build_batch_review_html

logger = logging.getLogger(__name__)


def parse_prospect_file(file_path: str) -> pd.DataFrame:
    """Parse Excel or CSV file, filter to PROCEED rows, sort by ICP score."""
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    # Normalize column names (strip whitespace)
    df.columns = df.columns.str.strip()

    # Filter to PROCEED only
    if "Pipeline Action" in df.columns:
        df = df[df["Pipeline Action"].str.strip().str.upper() == "PROCEED"]

    # Sort by ICP score descending
    if "Company ICP Score" in df.columns:
        df = df.sort_values("Company ICP Score", ascending=False)

    logger.info("Parsed %d PROCEED prospects from %s", len(df), file_path)
    return df


def extract_linkedin_slug(url: str) -> str | None:
    """Extract the LinkedIn slug from a profile URL."""
    if not url or not isinstance(url, str):
        return None
    url = url.rstrip("/")
    parts = url.split("/")
    # Handle linkedin.com/in/slug format
    try:
        idx = parts.index("in")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    except ValueError:
        pass
    return None


def upsert_company(sb, tenant_id: str, row: pd.Series) -> str | None:
    """Try to upsert a company record. Returns ID if companies table exists, None otherwise."""
    company_name = str(row.get("Company", "")).strip()
    if not company_name:
        return None

    try:
        existing = (
            sb.table("companies")
            .select("id")
            .eq("tenant_id", tenant_id)
            .eq("name", company_name)
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]["id"]

        linkedin_url = str(row.get("Company LinkedIn URL", "")).strip() or None
        industry = str(row.get("Industry", "")).strip() or None
        location = str(row.get("Company Location", "")).strip() or None

        result = sb.table("companies").insert({
            "tenant_id": tenant_id,
            "name": company_name,
            "industry": industry,
            "linkedin_url": linkedin_url,
            "data": {"location": location, "followers": row.get("Company LI Followers")},
        }).execute()

        return result.data[0]["id"] if result.data else None
    except Exception:
        # companies table may not exist in v2 schema — company info is on the prospect row
        return None


def upsert_prospect(
    sb,
    tenant_id: str,
    campaign_id: str,
    linkedin_account_id: str,
    company_id: str | None,
    row: pd.Series,
    partner_name: str,
) -> str | None:
    """Upsert a prospect record and return its ID."""
    linkedin_url = str(row.get("LinkedIn URL", "")).strip()
    slug = extract_linkedin_slug(linkedin_url)

    if not slug:
        logger.warning("Skipping row — no LinkedIn URL: %s %s", row.get("First Name"), row.get("Last Name"))
        return None

    # Check for existing prospect by slug + campaign
    existing = (
        sb.table("prospects")
        .select("id")
        .eq("campaign_id", campaign_id)
        .eq("linkedin_slug", slug)
        .limit(1)
        .execute()
    )
    if existing.data:
        logger.info("Prospect already exists: %s", slug)
        return existing.data[0]["id"]

    icp_score = row.get("Company ICP Score")
    scoring = {}
    if pd.notna(icp_score):
        scoring = {"score": int(icp_score)}

    # Determine which partner's messages to use
    partner_key = partner_name.split()[0] if partner_name else ""
    msg1 = str(row.get(f"Message 1 - {partner_key}", "")).strip() or None
    msg2 = str(row.get(f"Message 2 - {partner_key}", "")).strip() or None
    msg3 = str(row.get(f"Message 3 - {partner_key}", "")).strip() or None

    activity_data = {}
    for col in ["Activity Level", "Activity Score", "Activity Recommendation",
                 "Days Since Last Activity", "Activity Insights", "Recent Post Date", "Posts Count"]:
        val = row.get(col)
        if pd.notna(val):
            activity_data[col] = val

    result = sb.table("prospects").insert({
        "tenant_id": tenant_id,
        "campaign_id": campaign_id,
        "linkedin_account_id": linkedin_account_id,
        "company_id": company_id,
        "linkedin_slug": slug,
        "linkedin_url": linkedin_url,
        "first_name": str(row.get("First Name", "")).strip() or None,
        "last_name": str(row.get("Last Name", "")).strip() or None,
        "email": str(row.get("Email", "")).strip() or None,
        "headline": str(row.get("LinkedIn Headline", "")).strip() or None,
        "title": str(row.get("Title", "")).strip() or None,
        "seniority": str(row.get("Seniority", "")).strip() or None,
        "location": str(row.get("Company Location", "")).strip() or None,
        "company_name": str(row.get("Company", "")).strip() or None,
        "status": "scored",
        "source": "import",
        "scoring": scoring,
        "raw_data": {
            "messages": {"msg1": msg1, "msg2": msg2, "msg3": msg3},
            "activity": activity_data,
            "data_source": str(row.get("Data Source", "")).strip() or None,
        },
    }).execute()

    return result.data[0]["id"] if result.data else None


def create_batch_review(
    sb,
    tenant_id: str,
    campaign_id: str,
    prospect_ids: list[str],
    recipient_email: str,
    admin_user_id: str | None = None,
) -> tuple[str, str]:
    """Create a batch_review record with a magic link token.

    Returns: (batch_id, raw_token)
    """
    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    row = {
        "tenant_id": tenant_id,
        "campaign_id": campaign_id,
        "token_hash": token_hash,
        "prospect_ids": prospect_ids,
        "total_count": len(prospect_ids),
        "sent_to_email": recipient_email,
    }
    if admin_user_id:
        row["sent_by"] = admin_user_id

    result = sb.table("batch_reviews").insert(row).execute()
    batch_id = result.data[0]["id"]
    return batch_id, raw_token


def resolve_linkedin_account(sb, tenant_id: str, campaign_id: str) -> str:
    """Get the linkedin_account_id for a campaign, or the first active account for the tenant."""
    if campaign_id:
        camp = sb.table("campaigns").select("linkedin_account_id").eq("id", campaign_id).single().execute()
        if camp.data:
            return camp.data["linkedin_account_id"]
    # Fallback: first active account
    acct = (
        sb.table("linkedin_accounts")
        .select("id")
        .eq("tenant_id", tenant_id)
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    if acct.data:
        return acct.data[0]["id"]
    raise Exception("No active LinkedIn account found for tenant")


def run(file_path: str, recipient_name: str, recipient_email: str):
    """Main entry point for the batch-sender skill."""
    sb = get_supabase()
    tenant_id = config.DEFAULT_TENANT_ID
    campaign_id = config.DEFAULT_CAMPAIGN_ID

    if not tenant_id:
        raise Exception("DEFAULT_TENANT_ID not configured")
    if not campaign_id:
        raise Exception("DEFAULT_CAMPAIGN_ID not configured")

    linkedin_account_id = resolve_linkedin_account(sb, tenant_id, campaign_id)

    # 1. Parse file
    df = parse_prospect_file(file_path)
    if df.empty:
        print(f"No PROCEED prospects found in {file_path}")
        return

    # 2. Determine partner name from the recipient (for message column matching)
    partner_name = recipient_name

    # 3. Upsert companies + prospects
    prospect_ids = []
    for _, row in df.iterrows():
        company_id = upsert_company(sb, tenant_id, row)
        prospect_id = upsert_prospect(
            sb, tenant_id, campaign_id, linkedin_account_id,
            company_id, row, partner_name,
        )
        if prospect_id:
            prospect_ids.append(prospect_id)

    if not prospect_ids:
        print("No prospects imported (all may have been duplicates or missing LinkedIn URLs)")
        return

    print(f"Imported {len(prospect_ids)} prospects")

    # 4. Create batch review with magic link
    batch_id, raw_token = create_batch_review(
        sb, tenant_id, campaign_id, prospect_ids, recipient_email,
    )

    # 5. Load prospects for email template
    prospects = (
        sb.table("prospects")
        .select("*")
        .in_("id", prospect_ids)
        .order("scoring->>score", desc=True)
        .execute()
    ).data or []

    # 6. Build and send email
    html = build_batch_review_html(
        batch_id=batch_id,
        token=raw_token,
        recipient_name=recipient_name,
        prospects=prospects,
        supabase_url=config.SUPABASE_URL,
    )

    outlook = OutlookClient()
    outlook.send_email(
        to=recipient_email,
        subject=f"{len(prospect_ids)} new prospects ready — Batch #{batch_id[:8]}",
        html_body=html,
        cc="christopher@yorcmo.com",
    )

    # 7. Log event
    log_event(
        tenant_id=tenant_id,
        event_type="batch_sent",
        actor="agent:batch-sender",
        data={
            "batch_id": batch_id,
            "prospect_count": len(prospect_ids),
            "sent_to": recipient_email,
            "recipient_name": recipient_name,
        },
        campaign_id=campaign_id,
    )

    # 8. Print confirmation (Oz relays to Slack)
    print(f"Sent batch #{batch_id[:8]} ({len(prospect_ids)} prospects) to {recipient_name} at {recipient_email}")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Import prospects and send batch review email")
    parser.add_argument("--file", required=True, help="Path to Excel/CSV file")
    parser.add_argument("--name", required=True, help="Recipient name")
    parser.add_argument("--email", required=True, help="Recipient email")
    args = parser.parse_args()

    try:
        run(args.file, args.name, args.email)
    except Exception as e:
        logger.error("batch_sender failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
