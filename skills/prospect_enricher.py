"""Prospect enrichment pipeline — discover contacts at PROCEED companies, enrich, score, generate messages.

Triggered by Oz agent or run directly:
    python3 -m skills.prospect_enricher --tenant-id Y [--company-ids ID1,ID2] [--limit N]

Pipeline per company:
  Phase 1: Contact Discovery (4-tier: Apollo → ZoomInfo → X-ray → Profile verify)
  Phase 2: Person Enrichment (Apollo $1/person)
  Phase 3: LinkedIn Validation (profile scrape + Activity Index)
  Phase 4: Contact Scoring (score_prospects)
  Phase 5: Message Generation (connection notes + 3-message sequence)

Writes to: prospects table
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests

import config
from db.connect import get_supabase
from lib.apollo import ApolloClient
from lib.apify import (
    run_actor, extract_domain, build_company_match_terms,
    PROFILE_SCRAPER, SERP_ACTOR, JUNK_DOMAINS,
)
from lib.xray import xray_discover_finance_contacts, xray_find_contact_linkedin
from mvp.backend.services.scoring import score_prospects, classify_contact_activity
from mvp.backend.services.message_gen_svc import generate_outreach_for_prospect
from skills.helpers import setup_logging

logger = logging.getLogger(__name__)

ACTIVITY_INDEX_ACTOR = "kog75ERz9lcVNujbQ"

# Titles to search for (primary finance + secondary executive)
FINANCE_TITLES = ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance"]
EXEC_TITLES = ["President", "Owner", "CEO", "Founder", "Managing Director", "Partner"]
ALL_TARGET_TITLES = FINANCE_TITLES + EXEC_TITLES

PROSPECTS_TABLE = "prospects"
COMPANIES_TABLE = "companies_universe"

# City → zip code mapping for ZoomInfo
CITY_ZIP = {
    "seattle": "98101", "bellevue": "98004", "tacoma": "98402",
    "redmond": "98052", "kirkland": "98033", "everett": "98201",
    "renton": "98057", "kent": "98032", "auburn": "98002",
    "olympia": "98501", "lynnwood": "98036", "lakewood": "98499",
    "federal way": "98003", "vancouver": "98660", "ferndale": "98248",
    "puyallup": "98371", "bainbridge island": "98110", "woodinville": "98072",
}


def get_zip_for_location(location: str) -> str | None:
    """Extract zip code from company location string."""
    if not location:
        return None
    loc_lower = location.lower()
    for city, zip_code in CITY_ZIP.items():
        if city in loc_lower:
            return zip_code
    return None


# ---------------------------------------------------------------------------
# Phase 1: Contact Discovery
# ---------------------------------------------------------------------------

def discover_contacts_apollo(apollo: ApolloClient, domain: str) -> list[dict]:
    """Tier 1: Apollo free people search for target titles."""
    if not domain:
        return []
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_organization_domains_list": [domain],
            "person_titles": ALL_TARGET_TITLES,
            "per_page": 10,
        })
        contacts = []
        for p in result.get("people", []):
            contacts.append({
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "name": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                "title": p.get("title", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "apollo_id": p.get("id", ""),
                "email": p.get("email", ""),
                "source": "apollo",
            })
        return contacts
    except Exception as e:
        logger.warning("Apollo contact search failed for %s: %s", domain, e)
        return []


def discover_contacts_zoominfo(company_name: str, location: str) -> list[dict]:
    """Tier 2: ZoomInfo free contact search (SEARCH ONLY, no enrichment credits)."""
    username = os.environ.get("ZOOMINFO_USERNAME", "")
    password = os.environ.get("ZOOMINFO_PASSWORD", "")
    if not username or not password:
        logger.info("ZoomInfo credentials not configured — skipping")
        return []

    zip_code = get_zip_for_location(location)
    if not zip_code:
        logger.info("No zip code for location '%s' — skipping ZoomInfo", location)
        return []

    try:
        # Authenticate
        auth_resp = requests.post("https://api.zoominfo.com/authenticate", json={
            "username": username, "password": password,
        }, timeout=15)
        if auth_resp.status_code != 200:
            logger.warning("ZoomInfo auth failed: %d", auth_resp.status_code)
            return []
        token = auth_resp.json().get("jwt", "")

        # Search contacts
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        search_resp = requests.post("https://api.zoominfo.com/search/contact", headers=headers, json={
            "companyName": company_name,
            "jobTitle": " OR ".join(FINANCE_TITLES),
            "zipCode": zip_code,
            "zipCodeRadiusMiles": "50",
            "rpp": 5,
        }, timeout=15)

        contacts = []
        if search_resp.status_code == 200:
            for c in search_resp.json().get("data", []):
                contacts.append({
                    "first_name": c.get("firstName", ""),
                    "last_name": c.get("lastName", ""),
                    "name": f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
                    "title": c.get("jobTitle", ""),
                    "linkedin_url": "",  # ZoomInfo doesn't return LinkedIn URLs
                    "apollo_id": "",
                    "zoominfo_id": str(c.get("id", "")),
                    "source": "zoominfo",
                })
        return contacts
    except Exception as e:
        logger.warning("ZoomInfo search failed for %s: %s", company_name, e)
        return []


def apollo_crossmatch_contact(apollo: ApolloClient, first_name: str, last_name: str, company_name: str) -> dict | None:
    """Cross-match a ZoomInfo/X-ray contact against Apollo to get Apollo ID + LinkedIn URL."""
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_keywords": f"{first_name} {last_name} {company_name}",
            "per_page": 3,
        })
        for p in result.get("people", []):
            p_first = p.get("first_name", "").lower()
            p_last = p.get("last_name", "").lower()
            if p_last == last_name.lower() and (p_first == first_name.lower() or p_first[:3] == first_name.lower()[:3]):
                return {
                    "apollo_id": p.get("id", ""),
                    "linkedin_url": p.get("linkedin_url", ""),
                    "email": p.get("email", ""),
                }
        return None
    except Exception as e:
        logger.warning("Apollo cross-match failed for %s %s: %s", first_name, last_name, e)
        return None


def discover_contacts_for_company(apollo: ApolloClient, company: dict) -> list[dict]:
    """Run the 4-tier contact discovery for a single company."""
    name = company.get("name", "")
    domain = company.get("domain", "")
    location = company.get("location", "")
    seen_urls = set()
    all_contacts = []

    def add_contacts(contacts, tier_name):
        added = 0
        for c in contacts:
            key = c.get("linkedin_url") or f"{c.get('first_name','')}-{c.get('last_name','')}-{name}".lower()
            if key and key in seen_urls:
                continue
            if key:
                seen_urls.add(key)
            all_contacts.append(c)
            added += 1
        if added:
            logger.info("  %s: +%d contacts", tier_name, added)

    # Tier 1: Apollo
    print(f"    Tier 1 (Apollo)...", end=" ", flush=True)
    apollo_contacts = discover_contacts_apollo(apollo, domain)
    add_contacts(apollo_contacts, "Apollo")
    print(f"{len(apollo_contacts)} found")
    time.sleep(0.5)

    # Tier 2: ZoomInfo (only for finance titles not found in Apollo)
    apollo_has_finance = any(
        any(ft.lower() in c.get("title", "").lower() for ft in FINANCE_TITLES)
        for c in apollo_contacts
    )
    if not apollo_has_finance:
        print(f"    Tier 2 (ZoomInfo)...", end=" ", flush=True)
        zi_contacts = discover_contacts_zoominfo(name, location)
        # Cross-match against Apollo for IDs + LinkedIn URLs
        for c in zi_contacts:
            match = apollo_crossmatch_contact(apollo, c["first_name"], c["last_name"], name)
            if match:
                c["apollo_id"] = match.get("apollo_id", "")
                c["linkedin_url"] = match.get("linkedin_url", "")
                c["email"] = match.get("email", "") or c.get("email", "")
            time.sleep(0.3)
        add_contacts(zi_contacts, "ZoomInfo")
        print(f"{len(zi_contacts)} found")
        time.sleep(0.5)

    # Tier 3: X-ray (only if still 0 contacts)
    if not all_contacts:
        print(f"    Tier 3 (X-ray)...", end=" ", flush=True)
        xray_result = xray_discover_finance_contacts(name, domain=domain or None)
        xray_verified = xray_result.get("verified", [])
        # Cross-match verified contacts against Apollo
        for c in xray_verified:
            match = apollo_crossmatch_contact(apollo, c.get("first_name", ""), c.get("last_name", ""), name)
            if match:
                c["apollo_id"] = match.get("apollo_id", "")
                c["email"] = match.get("email", "")
            c["source"] = "xray"
            time.sleep(0.3)
        add_contacts(xray_verified, "X-ray")
        print(f"{len(xray_verified)} verified")

    # Find LinkedIn URLs for contacts missing them
    needs_linkedin = [c for c in all_contacts if not c.get("linkedin_url")]
    if needs_linkedin:
        all_contacts = xray_find_contact_linkedin(all_contacts, name)

    return all_contacts


# ---------------------------------------------------------------------------
# Phase 2: Person Enrichment
# ---------------------------------------------------------------------------

def enrich_person(apollo: ApolloClient, contact: dict) -> dict:
    """Enrich a contact via Apollo person enrich ($1/credit)."""
    apollo_id = contact.get("apollo_id")
    if not apollo_id:
        return contact

    try:
        result = apollo._request("POST", "/api/v1/people/match", json_body={
            "id": apollo_id,
            "reveal_personal_emails": True,
        })
        person = result.get("person", {})
        if person:
            contact["email"] = person.get("email") or contact.get("email", "")
            contact["title"] = person.get("title") or contact.get("title", "")
            contact["seniority"] = person.get("seniority") or ""
            contact["headline"] = person.get("headline") or ""
            contact["linkedin_url"] = person.get("linkedin_url") or contact.get("linkedin_url", "")
            contact["location"] = person.get("city", "") + (", " + person.get("state", "") if person.get("state") else "")
            contact["enriched"] = True
            contact["enriched_at"] = datetime.now(timezone.utc).isoformat()

            # Store raw Apollo data
            org = person.get("organization", {})
            contact["company_industry"] = org.get("industry", "")
            contact["company_employees"] = org.get("estimated_num_employees")
            contact["company_revenue"] = org.get("annual_revenue_printed", "")
    except Exception as e:
        logger.warning("Apollo person enrich failed for %s: %s", contact.get("name", "?"), e)

    return contact


# ---------------------------------------------------------------------------
# Phase 3: LinkedIn Validation
# ---------------------------------------------------------------------------

def validate_linkedin(contact: dict) -> dict:
    """Profile scrape + Activity Index for a single contact."""
    linkedin_url = contact.get("linkedin_url", "")
    if not linkedin_url:
        contact["role_verified"] = False
        contact["activity_level"] = "Unknown"
        return contact

    # Normalize URL
    if not linkedin_url.startswith("https://"):
        linkedin_url = linkedin_url.replace("http://", "https://")

    # Profile scrape for role verification
    profiles = run_actor(PROFILE_SCRAPER, {"urls": [linkedin_url]})
    if profiles:
        p = profiles[0]
        contact["headline"] = p.get("headline") or contact.get("headline", "")
        contact["linkedin_connections"] = p.get("connectionsCount", 0)
        contact["linkedin_followers"] = p.get("followerCount", 0)
        contact["open_to_work"] = p.get("openToWork", False)

        # Role verification
        current_positions = p.get("currentPosition") or []
        if current_positions:
            current_title = current_positions[0].get("title", "")
            current_company = current_positions[0].get("companyName", "")
            contact["role_verified"] = True
            if current_title:
                contact["title"] = current_title
        else:
            contact["role_verified"] = False

    # Activity Index
    activity = run_actor(ACTIVITY_INDEX_ACTOR, {"linkedinUrl": linkedin_url})
    if activity:
        a = activity[0] if isinstance(activity, list) else activity
        contact["activity_score"] = a.get("activity_score", 0)
        contact["activity_level"] = a.get("recommendation", "Unknown")
        contact["activity_recommendation"] = a.get("recommendation", "")

        metrics = a.get("activity_metrics", {})
        contact["posts_last_30_days"] = metrics.get("posts_last_30_days", 0)
        contact["reactions_last_30_days"] = metrics.get("reactions_last_30_days", 0)
        contact["last_activity_date"] = metrics.get("last_activity_date", "")
        contact["days_since_last_activity"] = metrics.get("days_since_last_activity")

        # Classify
        contact["linkedin_active_status"] = classify_contact_activity(contact)

        # Map activity_score to level labels
        score = contact.get("activity_score", 0)
        if score >= 7:
            contact["activity_level"] = "Very Active"
        elif score >= 5:
            contact["activity_level"] = "Active"
        elif score >= 3:
            contact["activity_level"] = "Moderate"
        elif score >= 1:
            contact["activity_level"] = "Low"
        else:
            contact["activity_level"] = "Inactive"

    return contact


# ---------------------------------------------------------------------------
# Phase 4 & 5: Score + Generate Messages
# ---------------------------------------------------------------------------

def score_and_generate(contacts: list[dict], company: dict, icp_config: dict) -> list[dict]:
    """Score contacts and generate outreach messages."""
    if not contacts:
        return []

    # Build scoring input
    scoring_input = []
    for c in contacts:
        scoring_input.append({
            "apollo_id": c.get("apollo_id", c.get("name", "")),
            "name": c.get("name", ""),
            "title": c.get("title", ""),
            "seniority": c.get("seniority", ""),
            "company_name": company.get("name", ""),
            "company_industry": company.get("industry", ""),
            "company_employees": company.get("employees_linkedin") or company.get("employees_apollo"),
            "company_revenue": company.get("revenue", ""),
            "company_location": company.get("location", ""),
            "person_location": c.get("location", ""),
            "has_linkedin": bool(c.get("linkedin_url")),
        })

    # Score
    try:
        scores = score_prospects(scoring_input, icp_config or {})
        score_map = {}
        for s in scores:
            key = s.get("apollo_id", s.get("name", ""))
            score_map[key] = s

        for c in contacts:
            key = c.get("apollo_id", c.get("name", ""))
            result = score_map.get(key, {})
            c["icp_score"] = result.get("score", 0)
            c["score_breakdown"] = result.get("breakdown", {})
            c["scoring_reasoning"] = result.get("reasoning", "")
    except Exception as e:
        logger.error("Contact scoring failed: %s", e)

    # Generate messages for each contact
    for c in contacts:
        try:
            outreach = generate_outreach_for_prospect(c, company)
            c["connection_notes"] = json.dumps(outreach.get("connection_notes", {}))
            c["partner_messages"] = json.dumps(outreach.get("messages", {}))
        except Exception as e:
            logger.warning("Message generation failed for %s: %s", c.get("name", "?"), e)

    return contacts


# ---------------------------------------------------------------------------
# Database: Write prospect to Supabase
# ---------------------------------------------------------------------------

def upsert_prospect(sb, tenant_id: str, campaign_id: str, company: dict, contact: dict):
    """Write a discovered + enriched contact to the prospects table."""
    linkedin_url = contact.get("linkedin_url", "")
    slug = ""
    if linkedin_url:
        slug = linkedin_url.rstrip("/").split("/")[-1]

    record = {
        "tenant_id": tenant_id,
        "campaign_id": campaign_id,
        "first_name": contact.get("first_name", ""),
        "last_name": contact.get("last_name", ""),
        "title": contact.get("title", ""),
        "seniority": contact.get("seniority", ""),
        "email": contact.get("email", ""),
        "email_status": "valid" if contact.get("email") else None,
        "headline": contact.get("headline", ""),
        "linkedin_url": linkedin_url,
        "linkedin_slug": slug,
        "location": contact.get("location", ""),
        "company_name": company.get("name", ""),
        "company_linkedin_url": company.get("linkedin_url", ""),
        "company_universe_id": company.get("id"),
        "status": "scored",
        "source": contact.get("source", "apollo"),
        "icp_score": contact.get("icp_score"),
        "activity_score": contact.get("activity_score"),
        "activity_level": contact.get("activity_level"),
        "activity_recommendation": contact.get("activity_recommendation", ""),
        "linkedin_active_status": contact.get("linkedin_active_status", ""),
        "posts_last_30_days": contact.get("posts_last_30_days"),
        "reactions_last_30_days": contact.get("reactions_last_30_days"),
        "last_activity_date": contact.get("last_activity_date"),
        "days_since_last_activity": contact.get("days_since_last_activity"),
        "linkedin_connections": contact.get("linkedin_connections"),
        "linkedin_followers": contact.get("linkedin_followers"),
        "open_to_work": contact.get("open_to_work", False),
        "role_verified": contact.get("role_verified", False),
        "apollo_person_id": contact.get("apollo_id", ""),
        "zoominfo_contact_id": contact.get("zoominfo_id", ""),
        "connection_notes": contact.get("connection_notes", ""),
        "partner_messages": contact.get("partner_messages", ""),
        "data_source": contact.get("source", ""),
        "contact_batch_name": f"prospect_enrichment_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
    }

    # Remove None values to avoid overwriting existing data
    record = {k: v for k, v in record.items() if v is not None}

    # Upsert by LinkedIn slug + tenant
    if slug:
        existing = sb.table(PROSPECTS_TABLE).select("id").eq("tenant_id", tenant_id).eq("linkedin_slug", slug).execute()
        if existing.data:
            sb.table(PROSPECTS_TABLE).update(record).eq("id", existing.data[0]["id"]).execute()
            return existing.data[0]["id"]

    result = sb.table(PROSPECTS_TABLE).insert(record).execute()
    return result.data[0]["id"] if result.data else None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_company(sb, apollo: ApolloClient, tenant_id: str, campaign_id: str,
                    company: dict, icp_config: dict) -> int:
    """Run the full prospect pipeline for a single company. Returns contact count."""
    name = company.get("name", "Unknown")
    print(f"\n=== {name} ===")

    # Phase 1: Contact Discovery
    print(f"  Phase 1: Contact Discovery")
    contacts = discover_contacts_for_company(apollo, company)
    if not contacts:
        print(f"  No contacts found at {name}")
        return 0
    print(f"  → {len(contacts)} contacts discovered")

    # Phase 2: Person Enrichment
    print(f"  Phase 2: Person Enrichment ({len(contacts)} contacts)")
    for i, c in enumerate(contacts):
        if c.get("apollo_id"):
            print(f"    [{i+1}/{len(contacts)}] Enriching {c.get('name', '?')}...", end=" ", flush=True)
            enrich_person(apollo, c)
            print("done" if c.get("enriched") else "no data")
            time.sleep(1)

    # Phase 3: LinkedIn Validation
    print(f"  Phase 3: LinkedIn Validation")
    for i, c in enumerate(contacts):
        if c.get("linkedin_url"):
            print(f"    [{i+1}/{len(contacts)}] Validating {c.get('name', '?')}...", end=" ", flush=True)
            validate_linkedin(c)
            print(f"{c.get('activity_level', '?')} ({c.get('linkedin_connections', '?')} connections)")
            time.sleep(1)

    # Phase 4 & 5: Score + Generate Messages
    print(f"  Phase 4-5: Scoring + Message Generation")
    contacts = score_and_generate(contacts, company, icp_config)

    # Write to DB
    print(f"  Writing {len(contacts)} prospects to database...")
    written = 0
    for c in contacts:
        try:
            pid = upsert_prospect(sb, tenant_id, campaign_id, company, c)
            if pid:
                written += 1
                print(f"    ✓ {c.get('name', '?')} ({c.get('title', '?')}) — score: {c.get('icp_score', '?')}")
        except Exception as e:
            logger.error("Failed to write prospect %s: %s", c.get("name", "?"), e)

    # Update contacts_found on company
    sb.table(COMPANIES_TABLE).update({
        "contacts_found": written,
    }).eq("id", company["id"]).execute()

    return written


def run(tenant_id: str, company_ids: list[str] | None = None, limit: int = 10):
    """Main entry point."""
    sb = get_supabase()

    # Load ICP config
    result = sb.table("tenants").select("settings").eq("id", tenant_id).single().execute()
    settings = result.data.get("settings", {}) if result.data else {}
    icp_config = settings.get("icp", {})

    campaign_id = config.DEFAULT_CAMPAIGN_ID or tenant_id

    # Get PROCEED companies
    query = sb.table(COMPANIES_TABLE).select("*").eq("tenant_id", tenant_id).eq("pipeline_action", "PROCEED")
    if company_ids:
        query = query.in_("id", company_ids)
    query = query.order("icp_score", desc=True).limit(limit)
    companies = query.execute().data or []

    if not companies:
        print("No PROCEED companies to process")
        return

    print(f"Processing {len(companies)} PROCEED companies...")
    apollo = ApolloClient()

    total_contacts = 0
    for i, company in enumerate(companies):
        print(f"\n[{i+1}/{len(companies)}]", end="")
        count = process_company(sb, apollo, tenant_id, campaign_id, company, icp_config)
        total_contacts += count

    print(f"\n{'='*60}")
    print(f"Done: {total_contacts} contacts enriched across {len(companies)} companies")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Discover and enrich contacts at PROCEED companies")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID")
    parser.add_argument("--company-ids", default=None,
                        help="Comma-separated company UUIDs (optional, all PROCEED if omitted)")
    parser.add_argument("--limit", type=int, default=10,
                        help="Max companies to process")
    args = parser.parse_args()

    company_ids = args.company_ids.split(",") if args.company_ids else None

    try:
        run(args.tenant_id, company_ids, args.limit)
    except Exception as e:
        logger.error("prospect_enricher failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
