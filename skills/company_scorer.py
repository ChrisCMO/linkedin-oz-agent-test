"""Company scoring pipeline — enrich via Apollo + score via GPT-5.

Triggered by Oz agent or run directly:
    python3 -m skills.company_scorer --batch-id X --tenant-id Y

Processes companies with pipeline_status = 'raw':
  raw → enriching → enriched → scoring → scored
  On error: → error (with enrichment_error or scoring_error)

Resumable: picks up where it left off on restart.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests

import config
from db.connect import get_supabase
from skills.helpers import setup_logging

logger = logging.getLogger(__name__)

OPENAI_MODEL = "gpt-5"  # Always use latest


def get_raw_companies(sb, tenant_id: str, batch_id: str | None = None, limit: int = 100) -> list[dict]:
    """Get companies ready for processing (raw or error status for retry)."""
    query = (
        sb.table("companies_universe")
        .select("*")
        .eq("tenant_id", tenant_id)
        .in_("pipeline_status", ["raw", "error"])
    )
    if batch_id:
        query = query.eq("batch_id", batch_id)
    query = query.order("created_at").limit(limit)
    result = query.execute()
    return result.data or []


def enrich_via_apollo(domain: str) -> dict | None:
    """Call Apollo org_enrich to get company data. Returns enrichment dict or None."""
    api_key = config.APOLLO_API_KEY if hasattr(config, 'APOLLO_API_KEY') else None
    if not api_key:
        logger.warning("APOLLO_API_KEY not configured — skipping enrichment")
        return None

    if not domain:
        return None

    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/organizations/enrich",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
            json={"api_key": api_key, "domain": domain},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json().get("organization", {})
            return {
                "name": data.get("name"),
                "industry": data.get("industry"),
                "employees": data.get("estimated_num_employees"),
                "revenue": data.get("annual_revenue_printed"),
                "annual_revenue": data.get("annual_revenue"),
                "founded_year": data.get("founded_year"),
                "linkedin_url": data.get("linkedin_url"),
                "website_url": data.get("website_url"),
                "phone": data.get("phone"),
                "city": data.get("city"),
                "state": data.get("state"),
                "country": data.get("country"),
                "short_description": data.get("short_description"),
                "seo_description": data.get("seo_description"),
                "ownership_type": data.get("ownership_type"),
                "apollo_id": data.get("id"),
            }
        elif resp.status_code == 402:
            logger.error("Apollo credits exhausted — cannot enrich")
            raise Exception("Apollo credits exhausted")
        else:
            logger.warning("Apollo enrichment failed for %s: %d %s", domain, resp.status_code, resp.text[:200])
            return None
    except requests.RequestException as e:
        logger.warning("Apollo request failed for %s: %s", domain, e)
        return None


def merge_enrichment(sb, company: dict, apollo_data: dict):
    """Merge Apollo enrichment data into the company record."""
    updates = {}

    # Fill gaps in standard columns (don't overwrite existing data)
    field_map = {
        "revenue": "revenue",
        "employees": "employees_apollo",
        "linkedin_url": "linkedin_url",
        "website_url": "website",
        "industry": "industry",
        "ownership_type": "ownership",
    }
    for apollo_key, db_key in field_map.items():
        if apollo_data.get(apollo_key) and not company.get(db_key):
            val = apollo_data[apollo_key]
            if isinstance(val, int) and db_key in ("employees_apollo",):
                updates[db_key] = val
            else:
                updates[db_key] = str(val) if val else None

    # Merge into source_data JSONB
    source_data = company.get("source_data") or {}
    source_data["apollo"] = apollo_data
    updates["source_data"] = source_data
    updates["pipeline_status"] = "enriched"
    updates["enriched_at"] = datetime.now(timezone.utc).isoformat()
    updates["enrichment_error"] = None

    sb.table("companies_universe").update(updates).eq("id", company["id"]).execute()


def load_icp_config(sb, tenant_id: str) -> dict:
    """Load ICP config from tenant settings."""
    result = sb.table("tenants").select("settings").eq("id", tenant_id).single().execute()
    settings = result.data.get("settings", {}) if result.data else {}
    return settings.get("icp", {})


def build_scoring_prompt(company: dict, icp_config: dict) -> str:
    """Build the GPT-5 scoring prompt for a company."""
    # Gather all available data about the company
    data_summary = f"""
Company: {company.get('name', 'Unknown')}
Domain: {company.get('domain', 'N/A')}
Industry: {company.get('industry', 'N/A')}
Location: {company.get('location', 'N/A')}
Revenue: {company.get('revenue', 'N/A')}
Employees (LinkedIn): {company.get('employees_linkedin', 'N/A')}
Employees (Apollo): {company.get('employees_apollo', 'N/A')}
Ownership: {company.get('ownership', 'N/A')}
LinkedIn URL: {company.get('linkedin_url', 'N/A')}
LinkedIn Followers: {company.get('li_followers', 'N/A')}
LinkedIn Description: {company.get('li_description', 'N/A')}
LinkedIn Tagline: {company.get('li_tagline', 'N/A')}
Website: {company.get('website', 'N/A')}
Category: {company.get('category', 'N/A')}
"""

    # Add source_data extras
    source_data = company.get("source_data") or {}
    if source_data:
        data_summary += "\nAdditional source data:\n"
        for source, data in source_data.items():
            data_summary += f"  {source}: {json.dumps(data, default=str)[:500]}\n"

    # ICP criteria
    icp_text = "ICP Criteria:\n"
    if icp_config:
        for key, value in icp_config.items():
            icp_text += f"  {key}: {value}\n"
    else:
        icp_text += "  No specific ICP configured — use general B2B criteria.\n"

    return f"""Score this company against the Ideal Customer Profile (ICP).

{data_summary}

{icp_text}

Score on 8 dimensions (100 points total):
1. Industry Fit (20 pts): Does the company's industry match the ICP targets?
2. Revenue Fit (15 pts): Is the revenue in the sweet spot range?
3. Company Size (15 pts): Are employee counts in the target range?
4. Geography (12 pts): Is the company in a target location?
5. Ownership Type (10 pts): Private = full points, PE-backed = 0 + flag, Public/Govt = disqualify
6. Growth Signals (10 pts): Evidence of growth, hiring, expansion?
7. Leadership Change (10 pts): New C-suite in past 12 months?
8. Digital Footprint (8 pts): LinkedIn presence, website quality, activity?

Respond in this exact JSON format:
{{
  "score": <0-100>,
  "pipeline_action": "PROCEED" | "REVIEW" | "SKIP",
  "score_breakdown": "industry_fit: X | revenue_fit: X | company_size: X | geography: X | ownership: X | growth: X | leadership: X | digital_footprint: X",
  "reasoning": "<2-3 sentence explanation of why this score>",
  "why_this_score": "<1 sentence summary>"
}}

Rules:
- Score >= 70 → PROCEED
- Score 50-69 → REVIEW
- Score < 50 → SKIP
- If ownership is Public or Government → auto SKIP with score 0
- Be specific in reasoning — reference actual data points
"""


def score_company_via_openai(prompt: str) -> dict | None:
    """Call OpenAI GPT-5 to score a company."""
    api_key = config.OPENAI_API_KEY if hasattr(config, 'OPENAI_API_KEY') else None
    if not api_key:
        logger.error("OPENAI_API_KEY not configured")
        return None

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "messages": [
                    {"role": "system", "content": "You are an ICP scoring assistant. Always respond in valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )

        if resp.status_code != 200:
            logger.error("OpenAI API error %d: %s", resp.status_code, resp.text[:200])
            return None

        content = resp.json()["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception as e:
        logger.error("OpenAI scoring failed: %s", e)
        return None


def process_company(sb, company: dict, icp_config: dict) -> bool:
    """Process a single company: enrich + score. Returns True if successful."""
    company_id = company["id"]
    name = company.get("name", "Unknown")

    try:
        # Step 1: Enrich via Apollo
        if company.get("pipeline_status") in ("raw", "error"):
            sb.table("companies_universe").update({
                "pipeline_status": "enriching",
            }).eq("id", company_id).execute()

            domain = company.get("domain")
            if domain:
                apollo_data = enrich_via_apollo(domain)
                if apollo_data:
                    merge_enrichment(sb, company, apollo_data)
                    logger.info("Enriched %s via Apollo", name)
                else:
                    # No Apollo data — still mark as enriched with what we have
                    sb.table("companies_universe").update({
                        "pipeline_status": "enriched",
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                    }).eq("id", company_id).execute()
            else:
                # No domain — mark as enriched with existing data
                sb.table("companies_universe").update({
                    "pipeline_status": "enriched",
                    "enriched_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", company_id).execute()

        # Reload company data after enrichment
        refreshed = sb.table("companies_universe").select("*").eq("id", company_id).single().execute()
        company = refreshed.data

        # Step 2: Score via GPT-5
        sb.table("companies_universe").update({
            "pipeline_status": "scoring",
        }).eq("id", company_id).execute()

        prompt = build_scoring_prompt(company, icp_config)
        result = score_company_via_openai(prompt)

        if not result:
            sb.table("companies_universe").update({
                "pipeline_status": "error",
                "scoring_error": "GPT-5 returned no result",
            }).eq("id", company_id).execute()
            return False

        # Write scoring results
        sb.table("companies_universe").update({
            "icp_score": result.get("score", 0),
            "pipeline_action": result.get("pipeline_action", "REVIEW"),
            "score_breakdown": result.get("score_breakdown", ""),
            "reasoning": result.get("reasoning", ""),
            "why_this_score": result.get("why_this_score", ""),
            "pipeline_status": "scored",
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "scoring_error": None,
        }).eq("id", company_id).execute()

        logger.info("Scored %s: %d (%s)", name, result.get("score", 0), result.get("pipeline_action", "?"))
        return True

    except Exception as e:
        logger.error("Failed to process %s: %s", name, e)
        sb.table("companies_universe").update({
            "pipeline_status": "error",
            "scoring_error": str(e)[:500],
        }).eq("id", company_id).execute()
        return False


def run(tenant_id: str, batch_id: str | None = None, limit: int = 100):
    """Main entry point for the company scoring pipeline."""
    sb = get_supabase()

    # Load ICP config
    icp_config = load_icp_config(sb, tenant_id)
    if not icp_config:
        logger.warning("No ICP config found for tenant %s — using defaults", tenant_id)

    # Get raw companies
    companies = get_raw_companies(sb, tenant_id, batch_id, limit)
    if not companies:
        print("No raw companies to process")
        return

    print(f"Processing {len(companies)} companies...")

    scored = 0
    errors = 0
    for i, company in enumerate(companies):
        print(f"  [{i+1}/{len(companies)}] {company.get('name', 'Unknown')}...", end=" ", flush=True)

        success = process_company(sb, company, icp_config)
        if success:
            scored += 1
            print(f"scored {company.get('icp_score', '?')}")
        else:
            errors += 1
            print("ERROR")

        # Small delay between API calls
        if i < len(companies) - 1:
            time.sleep(1)

    print(f"\nDone: {scored} scored, {errors} errors out of {len(companies)} companies")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Enrich and score companies via Apollo + GPT-5")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID")
    parser.add_argument("--batch-id", default=None, help="Batch UUID (optional, processes all raw if omitted)")
    parser.add_argument("--limit", type=int, default=100, help="Max companies to process")
    args = parser.parse_args()

    try:
        run(args.tenant_id, args.batch_id, args.limit)
    except Exception as e:
        logger.error("company_scorer failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
