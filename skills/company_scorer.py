"""Company scoring pipeline v2 — enrich, pre-process, score via GPT-5.4.

Triggered by Oz agent or run directly:
    python3 -m skills.company_scorer --tenant-id Y [--batch-id X] [--limit N]

Pipeline per company:
  0.  LinkedIn company scrape — batch Apify call for li_followers, employees, etc.
  1b. Blacklist check        — skip known VWC clients
  2.  Apollo org enrich      — fill revenue, employees, ownership (junk domain filter)
  3b. Finance title scan     — Apollo free people search for CFO/Controller
  3c. PSBJ cross-reference   — validate revenue, confirm family ownership
  3d. Revenue mismatch       — flag suspect revenue vs employee count
  4.  Score via score_companies_v2() — 7-dimension scoring with organizational complexity
  4b. X-ray rescue           — Tier 2+3 for REVIEW companies with 0 finance contacts
  4c. Rescore rescued        — REVIEW → PROCEED if finance contacts found

Status flow: raw → enriching → enriched → scoring → scored
On error: → error (with enrichment_error or scoring_error)
Resumable: picks up where it left off on restart.
"""

import argparse
import csv
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
    run_actor, extract_domain, JUNK_DOMAINS,
    COMPANY_SCRAPER,
)
from lib.xray import xray_discover_finance_contacts, xray_find_contact_linkedin
from mvp.backend.services.scoring import score_companies_v2, detect_revenue_mismatch
from skills.helpers import setup_logging

logger = logging.getLogger(__name__)

FINANCE_TITLES = [
    "CFO", "Chief Financial Officer", "Controller",
    "VP Finance", "Director of Finance",
]

# Relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLACKLIST_FILE = os.path.join(BASE_DIR, "data", "blacklist.csv")
PSBJ_FILE = os.path.join(BASE_DIR, "docs", "deliverables", "week2", "universe",
                          "private", "psbj_family_owned_wa_2026_86.csv")

TABLE = "raw_companies"  # Dashboard pipeline table
SCORING_BATCH_SIZE = 15  # Companies per GPT call


# ---------------------------------------------------------------------------
# Blacklist
# ---------------------------------------------------------------------------

def load_blacklist() -> dict:
    """Load blacklisted company names and domains."""
    blacklist = {"names": set(), "domains": set()}
    if not os.path.exists(BLACKLIST_FILE):
        logger.warning("Blacklist file not found at %s", BLACKLIST_FILE)
        return blacklist
    with open(BLACKLIST_FILE) as f:
        for row in csv.DictReader(f):
            name = row.get("company_name", "").strip().lower()
            domain = row.get("domain", "").strip().lower()
            if name:
                blacklist["names"].add(name)
            if domain:
                blacklist["domains"].add(domain)
    return blacklist


def is_blacklisted(company_name: str, domain: str | None, blacklist: dict) -> bool:
    """Substring name match or exact domain match."""
    name_lower = company_name.strip().lower()
    domain_lower = (domain or "").strip().lower()
    if domain_lower and domain_lower in blacklist["domains"]:
        return True
    for bl_name in blacklist["names"]:
        if bl_name in name_lower or name_lower in bl_name:
            return True
    return False


# ---------------------------------------------------------------------------
# PSBJ cross-reference
# ---------------------------------------------------------------------------

def load_psbj() -> dict:
    """Load PSBJ family-owned companies for revenue cross-reference."""
    psbj = {}
    if not os.path.exists(PSBJ_FILE):
        logger.info("PSBJ file not found, skipping cross-reference")
        return psbj
    with open(PSBJ_FILE) as f:
        for row in csv.DictReader(f):
            name = row.get("company_name", "").strip().lower()
            if name:
                psbj[name] = {
                    "revenue": row.get("revenue_2025", ""),
                    "employees": row.get("employees_wa", ""),
                    "exec1_title": row.get("exec1_title", ""),
                    "exec1_name": f"{row.get('exec1_first', '')} {row.get('exec1_last', '')}".strip(),
                    "ownership_type": row.get("ownership_type", ""),
                }
    return psbj


def psbj_match(company_name: str, psbj_data: dict) -> dict | None:
    """Check if company name matches PSBJ list (substring)."""
    name_lower = company_name.strip().lower()
    for psbj_name, data in psbj_data.items():
        if psbj_name in name_lower or name_lower in psbj_name:
            return data
    return None


# ---------------------------------------------------------------------------
# Phase 0: LinkedIn company page scrape (batch)
# ---------------------------------------------------------------------------

def linkedin_scrape_batch(sb, companies: list[dict]):
    """Batch-scrape LinkedIn company pages for companies missing li_followers.

    Fills: li_followers, li_description, li_tagline, li_founded, employees (from LI).
    Also extracts domain from LinkedIn website when company has no domain.
    """
    needs_scrape = [
        c for c in companies
        if c.get("linkedin_url") and not c.get("li_followers")
    ]
    if not needs_scrape:
        print("  Phase 0: All companies already have LinkedIn data — skipping")
        return

    urls = [c["linkedin_url"] for c in needs_scrape]
    print(f"  Phase 0: LinkedIn scrape for {len(urls)} companies...")
    li_results = run_actor(COMPANY_SCRAPER, {"companies": urls})
    print(f"    Got {len(li_results)} results")

    # Index results by URL
    li_by_url = {}
    for item in li_results:
        url = item.get("linkedinUrl", item.get("url", ""))
        if url:
            li_by_url[url.rstrip("/")] = item

    for c in needs_scrape:
        li_url = c.get("linkedin_url", "").rstrip("/")
        li = li_by_url.get(li_url)
        if not li:
            continue

        updates = {}
        emp = li.get("employeeCount")
        if emp and not c.get("employees"):
            updates["employees"] = int(emp)
        followers = li.get("followerCount")
        if followers:
            updates["li_followers"] = int(followers)
        tagline = li.get("tagline")
        if tagline:
            updates["li_tagline"] = tagline
        desc = li.get("description")
        if desc:
            updates["li_description"] = desc
        founded = (li.get("foundedOn") or {}).get("year")
        if founded:
            updates["li_founded"] = str(founded)

        # Extract domain from LinkedIn website field
        website = li.get("website") or li.get("websiteUrl") or ""
        if website and not c.get("domain"):
            new_domain = extract_domain(website)
            if new_domain:
                updates["domain"] = new_domain
                updates["website"] = website

        # Store full scrape data in enrichment_data
        enrichment_data = c.get("enrichment_data") or {}
        enrichment_data["linkedin_scrape"] = {
            "employeeCount": emp,
            "followerCount": followers,
            "tagline": tagline,
            "description": (desc or "")[:200],
            "founded": founded,
            "website": website,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        updates["enrichment_data"] = enrichment_data

        if updates:
            sb.table(TABLE).update(updates).eq("id", c["id"]).execute()
            # Update in-memory copy for subsequent phases
            c.update(updates)
            logger.info("  LinkedIn scraped %s: %s emp, %s followers",
                        c.get("name", "?"), emp, followers)


# ---------------------------------------------------------------------------
# Finance title scan (Apollo free search)
# ---------------------------------------------------------------------------

def apollo_finance_scan(apollo: ApolloClient, domain: str) -> list[dict]:
    """Free Apollo people search for finance titles at a company."""
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_organization_domains_list": [domain],
            "person_titles": FINANCE_TITLES,
            "per_page": 5,
        })
        contacts = []
        for p in result.get("people", []):
            contacts.append({
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "title": p.get("title", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "company": p.get("organization", {}).get("name", ""),
            })
        return contacts
    except Exception as e:
        logger.warning("Apollo finance scan failed for %s: %s", domain, e)
        return []


def pick_best_finance_contact(contacts: list[dict]) -> dict | None:
    """Pick highest-priority finance contact (CFO > Controller > VP > Director)."""
    if not contacts:
        return None
    priority_map = {
        "cfo": 1, "chief financial officer": 1,
        "controller": 2, "financial controller": 2,
        "vp finance": 3, "vice president of finance": 3,
        "director of finance": 4,
    }

    def priority(c):
        t = c.get("title", "").lower()
        for key, pri in priority_map.items():
            if key in t:
                return pri
        return 99

    contacts.sort(key=priority)
    return contacts[0]


# ---------------------------------------------------------------------------
# Apollo org enrichment (existing v1 logic)
# ---------------------------------------------------------------------------

def enrich_via_apollo(domain: str) -> dict | None:
    """Call Apollo org_enrich. Returns enrichment dict or None."""
    api_key = config.APOLLO_API_KEY
    if not api_key:
        logger.warning("APOLLO_API_KEY not configured — skipping enrichment")
        return None
    if not domain:
        return None

    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/organizations/enrich",
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "X-Api-Key": api_key,
            },
            json={"domain": domain},
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
            logger.warning("Apollo enrichment failed for %s: %d %s",
                           domain, resp.status_code, resp.text[:200])
            return None
    except requests.RequestException as e:
        logger.warning("Apollo request failed for %s: %s", domain, e)
        return None


def merge_enrichment(sb, company: dict, apollo_data: dict):
    """Merge Apollo enrichment data into the company record."""
    updates = {}
    field_map = {
        "revenue": "revenue",
        "linkedin_url": "linkedin_url",
        "website_url": "website",
        "industry": "industry",
        "ownership_type": "ownership",
    }
    for apollo_key, db_key in field_map.items():
        if apollo_data.get(apollo_key) and not company.get(db_key):
            updates[db_key] = str(apollo_data[apollo_key])

    # Store employee count from Apollo in enrichment_data (raw_companies has
    # a single 'employees' integer column — don't overwrite LinkedIn count)
    enrichment_data = company.get("enrichment_data") or {}
    enrichment_data["apollo"] = apollo_data
    updates["enrichment_data"] = enrichment_data
    updates["pipeline_status"] = "enriched"
    updates["enriched_at"] = datetime.now(timezone.utc).isoformat()
    updates["enrichment_error"] = None

    sb.table(TABLE).update(updates).eq("id", company["id"]).execute()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_raw_companies(sb, tenant_id: str, batch_id: str | None = None,
                      limit: int = 100) -> list[dict]:
    """Get companies ready for processing."""
    query = (
        sb.table(TABLE)
        .select("*")
        .eq("tenant_id", tenant_id)
        .in_("pipeline_status", ["raw", "error"])
    )
    if batch_id:
        query = query.eq("batch_id", batch_id)
    query = query.order("created_at").limit(limit)
    result = query.execute()
    return result.data or []


def load_icp_config(sb, tenant_id: str) -> dict:
    """Load ICP config from tenant settings."""
    result = sb.table("tenants").select("settings").eq("id", tenant_id).single().execute()
    settings = result.data.get("settings", {}) if result.data else {}
    return settings.get("icp", {})


# ---------------------------------------------------------------------------
# Pre-processing: build scoring input for a company
# ---------------------------------------------------------------------------

def preprocess_company(company: dict, apollo: ApolloClient,
                       psbj_data: dict) -> dict:
    """Run finance scan, PSBJ cross-ref, revenue mismatch for one company.

    Returns a dict ready for score_companies_v2().
    """
    name = company.get("name", "Unknown")
    domain = company.get("domain", "")

    # --- Finance title scan (free Apollo search) ---
    finance_contacts = []
    if domain:
        finance_contacts = apollo_finance_scan(apollo, domain)
        if finance_contacts:
            logger.info("  Finance contacts at %s: %d found", name, len(finance_contacts))
        time.sleep(0.5)  # gentle rate limit

    best_finance = pick_best_finance_contact(finance_contacts)
    has_cfo = False
    has_controller = False
    finance_titles_found = ""
    finance_contact_name = ""
    finance_contact_linkedin = ""

    if best_finance:
        title_lower = best_finance.get("title", "").lower()
        has_cfo = "cfo" in title_lower or "chief financial" in title_lower
        has_controller = "controller" in title_lower
        finance_contact_name = f"{best_finance.get('first_name', '')} {best_finance.get('last_name', '')}".strip()
        finance_contact_linkedin = best_finance.get("linkedin_url", "")

    all_titles = list(set(fc.get("title", "") for fc in finance_contacts if fc.get("title")))
    finance_titles_found = ", ".join(all_titles)

    # --- PSBJ cross-reference ---
    revenue = company.get("revenue", "")
    ownership = company.get("ownership", "")
    notes = ""

    match = psbj_match(name, psbj_data)
    if match:
        logger.info("  PSBJ match for %s", name)
        psbj_rev = match.get("revenue", "")
        if psbj_rev and not revenue:
            revenue = psbj_rev
            notes += f"Revenue from PSBJ: {psbj_rev}. "
        if match.get("ownership_type"):
            ownership = f"Private ({match['ownership_type']}, confirmed via PSBJ)"

    # --- Store finance data in enrichment_data for DB persistence ---
    enrichment_data = company.get("enrichment_data") or {}
    enrichment_data["finance_scan"] = {
        "contacts": finance_contacts,
        "best_contact": best_finance,
        "has_cfo": has_cfo,
        "has_controller": has_controller,
        "titles_found": finance_titles_found,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
    }
    if match:
        enrichment_data["psbj"] = match

    # Apollo employees from enrichment_data (if enriched)
    apollo_employees = ""
    apollo_enrich = enrichment_data.get("apollo", {})
    if apollo_enrich.get("employees"):
        apollo_employees = str(apollo_enrich["employees"])

    # Build dict for score_companies_v2()
    return {
        "company_id": company.get("id", ""),
        "company_name": name,
        "industry": company.get("industry", ""),
        "linkedin_employees": company.get("employees", ""),  # raw_companies.employees
        "apollo_employees": apollo_employees,
        "revenue": revenue,
        "location": company.get("location", ""),
        "ownership": ownership,
        "linkedin_page": company.get("linkedin_url", ""),
        "linkedin_followers": company.get("li_followers", ""),
        "website": company.get("website", company.get("domain", "")),
        "finance_titles": finance_titles_found,
        "has_cfo": has_cfo,
        "has_controller": has_controller,
        "finance_contact_name": finance_contact_name,
        "finance_contact_linkedin": finance_contact_linkedin,
        "notes": notes,
        # Pass through for DB update
        "_enrichment_data": enrichment_data,
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_companies(sb, companies: list[dict], icp_config: dict) -> tuple[int, int, int]:
    """Process a list of companies: enrich → preprocess → batch score.

    Returns (scored, errors, skipped).
    """
    apollo = ApolloClient()
    blacklist = load_blacklist()
    psbj_data = load_psbj()

    print(f"  Blacklist: {len(blacklist['names'])} names, {len(blacklist['domains'])} domains")
    print(f"  PSBJ: {len(psbj_data)} companies loaded")

    # Phase 0: Batch LinkedIn company page scrape
    linkedin_scrape_batch(sb, companies)

    scored_count = 0
    error_count = 0
    skipped_count = 0

    # Phase 1: Enrich + preprocess each company
    scoring_queue = []  # (company_db_row, scoring_input_dict)

    for i, company in enumerate(companies):
        company_id = company["id"]
        name = company.get("name", "Unknown")
        domain = company.get("domain", "")

        # Junk domain filter — facebook.com, instagram.com, etc.
        if domain and domain.lower() in JUNK_DOMAINS:
            logger.info("Junk domain filtered: %s → %s", name, domain)
            domain = ""
            sb.table(TABLE).update({"domain": None}).eq("id", company_id).execute()
            company["domain"] = ""

        print(f"  [{i+1}/{len(companies)}] {name}...", end=" ", flush=True)

        # --- Step 1b: Blacklist check ---
        if is_blacklisted(name, domain, blacklist):
            print("BLACKLISTED — skipping")
            sb.table(TABLE).update({
                "pipeline_status": "scored",
                "pipeline_action": "SKIP",
                "icp_score": 0,
                "reasoning": "Blacklisted: existing VWC client",
                "scored_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", company_id).execute()
            skipped_count += 1
            continue

        try:
            # --- Step 2: Apollo org enrichment ---
            if company.get("pipeline_status") in ("raw", "error"):
                sb.table(TABLE).update({
                    "pipeline_status": "enriching",
                }).eq("id", company_id).execute()

                if domain:
                    apollo_data = enrich_via_apollo(domain)
                    if apollo_data:
                        merge_enrichment(sb, company, apollo_data)
                        logger.info("Enriched %s via Apollo", name)
                    else:
                        sb.table(TABLE).update({
                            "pipeline_status": "enriched",
                            "enriched_at": datetime.now(timezone.utc).isoformat(),
                        }).eq("id", company_id).execute()
                else:
                    sb.table(TABLE).update({
                        "pipeline_status": "enriched",
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                    }).eq("id", company_id).execute()

            # Reload after enrichment
            refreshed = sb.table(TABLE).select("*").eq("id", company_id).single().execute()
            company = refreshed.data

            # --- Steps 3b/3c/3d: Finance scan + PSBJ + revenue mismatch ---
            scoring_input = preprocess_company(company, apollo, psbj_data)

            # Persist enrichment_data with finance scan results
            sb.table(TABLE).update({
                "enrichment_data": scoring_input["_enrichment_data"],
            }).eq("id", company_id).execute()

            scoring_queue.append((company, scoring_input))
            print("enriched + preprocessed")

        except Exception as e:
            logger.error("Failed to process %s: %s", name, e)
            sb.table(TABLE).update({
                "pipeline_status": "error",
                "enrichment_error": str(e)[:500],
            }).eq("id", company_id).execute()
            error_count += 1
            print(f"ERROR: {e}")

        # Small delay between API calls
        if i < len(companies) - 1:
            time.sleep(1)

    # Phase 2: Batch score via score_companies_v2()
    if not scoring_queue:
        print("\nNo companies to score.")
        return scored_count, error_count, skipped_count

    print(f"\n--- Scoring {len(scoring_queue)} companies with v2 ---")

    # Mark all as scoring
    for company, _ in scoring_queue:
        sb.table(TABLE).update({
            "pipeline_status": "scoring",
        }).eq("id", company["id"]).execute()

    # Score in batches
    for batch_start in range(0, len(scoring_queue), SCORING_BATCH_SIZE):
        batch = scoring_queue[batch_start:batch_start + SCORING_BATCH_SIZE]
        batch_inputs = [si for _, si in batch]
        batch_num = batch_start // SCORING_BATCH_SIZE + 1
        print(f"  Batch {batch_num} ({len(batch)} companies)...", end=" ", flush=True)

        try:
            scores = score_companies_v2(batch_inputs)

            # Build lookup by company_id or company_name
            score_lookup = {}
            for s in scores:
                key = s.get("company_id", s.get("company_name", ""))
                score_lookup[key] = s
                # Also index by name for fuzzy matching
                name_key = s.get("company_name", "")
                if name_key:
                    score_lookup[name_key] = s

            for company, scoring_input in batch:
                company_id = company["id"]
                name = company.get("name", "Unknown")

                result = score_lookup.get(company_id) or score_lookup.get(name)
                if not result:
                    logger.warning("No score returned for %s", name)
                    sb.table(TABLE).update({
                        "pipeline_status": "error",
                        "scoring_error": "No score returned from v2 scorer",
                    }).eq("id", company_id).execute()
                    error_count += 1
                    continue

                score = result.get("score", 0)
                if score >= 80:
                    action = "PROCEED"
                elif score >= 60:
                    action = "REVIEW"
                elif score == 0:
                    action = "HARD EXCLUDE"
                else:
                    action = "SKIP"

                breakdown = result.get("breakdown", {})
                breakdown_str = " | ".join(f"{k}: {v}" for k, v in breakdown.items())

                sb.table(TABLE).update({
                    "icp_score": score,
                    "pipeline_action": action,
                    "score_breakdown": breakdown_str,
                    "reasoning": result.get("reasoning", ""),
                    "why_this_score": result.get("calibration_notes", ""),
                    "pipeline_status": "scored",
                    "scored_at": datetime.now(timezone.utc).isoformat(),
                    "scoring_error": None,
                }).eq("id", company_id).execute()

                scored_count += 1
                logger.info("Scored %s: %d (%s)", name, score, action)

            print(f"{len(scores)} scores returned")

        except Exception as e:
            logger.error("Batch scoring failed: %s", e)
            for company, _ in batch:
                sb.table(TABLE).update({
                    "pipeline_status": "error",
                    "scoring_error": f"Batch scoring failed: {str(e)[:400]}",
                }).eq("id", company["id"]).execute()
                error_count += 1
            print(f"ERROR: {e}")

        time.sleep(1)

    # Phase 3: X-ray rescue for REVIEW companies with 0 finance contacts
    review_no_contacts = []
    for company, si in scoring_queue:
        refreshed = sb.table(TABLE).select("pipeline_action, enrichment_data").eq("id", company["id"]).single().execute()
        if not refreshed.data:
            continue
        action = refreshed.data.get("pipeline_action")
        enrich = refreshed.data.get("enrichment_data") or {}
        finance_contacts = (enrich.get("finance_scan") or {}).get("contacts", [])
        if action == "REVIEW" and not finance_contacts:
            review_no_contacts.append((company, si))

    if review_no_contacts:
        print(f"\n--- Phase 3: X-ray rescue for {len(review_no_contacts)} REVIEW companies ---")
        rescore_needed = []

        for company, si in review_no_contacts:
            name = company.get("name", "Unknown")
            domain = company.get("domain", "")
            print(f"  X-ray: {name}...", end=" ", flush=True)

            xray_result = xray_discover_finance_contacts(name, domain=domain or None)
            verified = xray_result.get("verified", [])

            # Store X-ray results in enrichment_data
            enrichment_data = company.get("enrichment_data") or {}
            enrichment_data["xray_rescue"] = {
                "raw_contacts": len(xray_result.get("contacts", [])),
                "verified": [
                    {"name": c["name"], "title": c["title"],
                     "linkedin_url": c.get("linkedin_url", "")}
                    for c in verified
                ],
                "rejected": [
                    {"name": c["name"], "reason": c.get("reason", "")}
                    for c in xray_result.get("rejected", [])
                ],
                "searched_at": datetime.now(timezone.utc).isoformat(),
            }

            sb.table(TABLE).update({
                "enrichment_data": enrichment_data,
            }).eq("id", company["id"]).execute()

            if verified:
                has_cfo = any("cfo" in c["title"].lower() or "chief financial" in c["title"].lower() for c in verified)
                has_controller = any("controller" in c["title"].lower() for c in verified)
                titles_found = ", ".join(c["title"] for c in verified)

                # Update finance_scan in enrichment_data
                enrichment_data["finance_scan"] = enrichment_data.get("finance_scan", {})
                enrichment_data["finance_scan"]["contacts"] = verified
                enrichment_data["finance_scan"]["has_cfo"] = has_cfo
                enrichment_data["finance_scan"]["has_controller"] = has_controller
                enrichment_data["finance_scan"]["titles_found"] = titles_found
                enrichment_data["finance_scan"]["source"] = "xray_tier2"

                sb.table(TABLE).update({
                    "enrichment_data": enrichment_data,
                }).eq("id", company["id"]).execute()

                company["enrichment_data"] = enrichment_data
                rescore_needed.append((company, si, verified, has_cfo, has_controller, titles_found))
                print(f"found {len(verified)} verified → will rescore")
            else:
                print("no contacts found")

            time.sleep(1)

        # Phase 4: Rescore rescued companies
        if rescore_needed:
            print(f"\n--- Phase 4: Rescoring {len(rescore_needed)} companies ---")
            rescore_inputs = []
            rescore_companies = []

            for company, si, verified, has_cfo, has_controller, titles_found in rescore_needed:
                best = verified[0]
                rescore_inputs.append({
                    **si,
                    "finance_titles": titles_found,
                    "has_cfo": has_cfo,
                    "has_controller": has_controller,
                    "finance_contact_name": best["name"],
                    "finance_contact_linkedin": best.get("linkedin_url", ""),
                })
                rescore_companies.append(company)

            try:
                new_scores = score_companies_v2(rescore_inputs)
                score_map = {
                    s.get("company_id", s.get("company_name", "")): s
                    for s in new_scores
                }

                for company, _ in zip(rescore_companies, rescore_inputs):
                    result = score_map.get(company["id"]) or score_map.get(company.get("name", ""))
                    if not result:
                        continue

                    old_score = sb.table(TABLE).select("icp_score").eq("id", company["id"]).single().execute().data.get("icp_score", 0)
                    new_score = result.get("score", 0)
                    action = "PROCEED" if new_score >= 80 else "REVIEW" if new_score >= 60 else "HARD EXCLUDE" if new_score == 0 else "SKIP"
                    breakdown = result.get("breakdown", {})
                    breakdown_str = " | ".join(f"{k}: {v}" for k, v in breakdown.items())

                    sb.table(TABLE).update({
                        "icp_score": new_score,
                        "pipeline_action": action,
                        "score_breakdown": breakdown_str,
                        "reasoning": result.get("reasoning", ""),
                        "why_this_score": result.get("calibration_notes", ""),
                        "scored_at": datetime.now(timezone.utc).isoformat(),
                    }).eq("id", company["id"]).execute()

                    print(f"  {company.get('name', '?')}: {old_score} → {new_score} ({action})")

            except Exception as e:
                logger.error("Rescore failed: %s", e)
                print(f"  Rescore ERROR: {e}")

    return scored_count, error_count, skipped_count


def run(tenant_id: str, batch_id: str | None = None, limit: int = 100):
    """Main entry point."""
    sb = get_supabase()

    icp_config = load_icp_config(sb, tenant_id)
    if not icp_config:
        logger.warning("No ICP config for tenant %s — using VWC defaults", tenant_id)

    companies = get_raw_companies(sb, tenant_id, batch_id, limit)
    if not companies:
        print("No raw companies to process")
        return

    print(f"Processing {len(companies)} companies (v2 pipeline)...")

    scored, errors, skipped = process_companies(sb, companies, icp_config)

    print(f"\nDone: {scored} scored, {errors} errors, {skipped} skipped "
          f"out of {len(companies)} companies")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Enrich and score companies — v2 pipeline "
                    "(blacklist, finance scan, PSBJ, revenue mismatch, organizational complexity)")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID")
    parser.add_argument("--batch-id", default=None,
                        help="Batch UUID (optional, processes all raw if omitted)")
    parser.add_argument("--limit", type=int, default=100,
                        help="Max companies to process")
    args = parser.parse_args()

    try:
        run(args.tenant_id, args.batch_id, args.limit)
    except Exception as e:
        logger.error("company_scorer failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
