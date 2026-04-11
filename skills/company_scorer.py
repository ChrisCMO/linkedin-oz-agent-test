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

from lib.title_tiers import TIER_1_TITLES, TIER_3_TITLES, classify_title_tier

FINANCE_TITLES = TIER_1_TITLES + TIER_3_TITLES

# Relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLACKLIST_FILE = os.path.join(BASE_DIR, "data", "blacklist.csv")
FORM5500_FILE = os.path.join(BASE_DIR, "data", "form5500_big_firm_clients.csv")
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
        if bl_name == name_lower:
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
    """Check if company name matches PSBJ list (exact or contained)."""
    name_lower = company_name.strip().lower()
    # Exact match first
    if name_lower in psbj_data:
        return psbj_data[name_lower]
    # PSBJ names are trusted data — allow substring match only if PSBJ name
    # is long enough (4+ words) to avoid false positives
    for psbj_name, data in psbj_data.items():
        if len(psbj_name.split()) >= 3 and (psbj_name in name_lower or name_lower in psbj_name):
            return data
    return None


# ---------------------------------------------------------------------------
# Form 5500 cross-reference (Big Firm auditor clients)
# ---------------------------------------------------------------------------

def load_form5500() -> dict:
    """Load Form 5500 data — companies audited by Big Four/large CPA firms.

    Being a client of a large firm is a POSITIVE signal for VWC:
    these companies may feel underserved ("small fish in a big pond").
    """
    f5500 = {}
    if not os.path.exists(FORM5500_FILE):
        logger.info("Form 5500 file not found, skipping cross-reference")
        return f5500
    with open(FORM5500_FILE) as f:
        for row in csv.DictReader(f):
            name = row.get("company", "").strip().lower()
            if name:
                # Keep first match (may have multiple plans)
                if name not in f5500:
                    f5500[name] = {
                        "auditor": row.get("auditor", "").strip(),
                        "city": row.get("city", "").strip(),
                        "state": row.get("state", "").strip(),
                        "plan": row.get("plan", "").strip(),
                        "participants": row.get("participants", "").strip(),
                    }
    return f5500


def form5500_match(company_name: str, f5500_data: dict) -> dict | None:
    """Check if company appears in Form 5500 big firm client list."""
    name_lower = company_name.strip().lower()
    # Exact match
    if name_lower in f5500_data:
        return f5500_data[name_lower]
    # Try with common suffixes removed
    for suffix in [", inc.", ", inc", " inc.", " inc", ", llc", " llc",
                   ", ltd", " ltd", " co.", " co", " corp.", " corp",
                   ", l.p.", " l.p."]:
        stripped = name_lower.rstrip(".").removesuffix(suffix)
        if stripped != name_lower and stripped in f5500_data:
            return f5500_data[stripped]
    # Try adding suffixes to match Form 5500 names
    for suffix in [", inc.", " inc.", ", llc", " llc", " corporation",
                   " company", " co."]:
        candidate = name_lower + suffix
        if candidate in f5500_data:
            return f5500_data[candidate]
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

    # Index results by normalized URL
    def _normalize_li_url(url: str) -> str:
        return url.lower().replace("http://", "https://").replace("www.", "").rstrip("/")

    li_by_url = {}
    for item in li_results:
        url = item.get("linkedinUrl", item.get("url", ""))
        if url:
            li_by_url[_normalize_li_url(url)] = item

    for c in needs_scrape:
        li_url = _normalize_li_url(c.get("linkedin_url", ""))
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

        # Extract HQ and branch locations
        locations = li.get("locations") or []
        hq_location = None
        branch_locations = []
        for loc in locations:
            loc_str = ", ".join(filter(None, [
                loc.get("line1", ""),
                loc.get("city", ""),
                loc.get("geographicArea", loc.get("state", "")),
                loc.get("postalCode", ""),
                loc.get("country", ""),
            ]))
            # Apify uses "headquarter" (not "isHQ")
            if loc.get("headquarter"):
                hq_location = loc_str
            else:
                branch_locations.append(loc_str)

        # Check if the company's listed location is HQ or a branch
        PNW_CITIES = {
            "seattle", "bellevue", "tacoma", "redmond", "kirkland", "everett",
            "renton", "kent", "auburn", "olympia", "lynnwood", "lakewood",
            "federal way", "vancouver", "portland", "tukwila", "bainbridge island",
        }
        current_location = (c.get("location") or "").lower()
        is_local = any(city in current_location for city in PNW_CITIES)

        hq_city = ""
        for loc in locations:
            if loc.get("headquarter"):
                hq_city = (loc.get("city") or "").lower()
                break

        hq_is_local = any(city in hq_city for city in PNW_CITIES) if hq_city else False
        is_branch = is_local and not hq_is_local and hq_location

        if is_branch:
            # Don't modify location — Seattle branch still counts for geography scoring
            # Branch info stored in enrichment_data for Chad's review
            logger.info("  %s: Seattle is a branch — HQ: %s", c.get("name", "?"), hq_location)

        # Fix wrong location: if DB says non-PNW but HQ is actually PNW, correct it
        if not is_local and hq_is_local and hq_location:
            hq_display = ", ".join(filter(None, [hq_city.title(), "WA"]))
            for loc in locations:
                if loc.get("headquarter"):
                    hq_display = ", ".join(filter(None, [
                        loc.get("city", ""),
                        loc.get("geographicArea", ""),
                    ]))
                    break
            updates["location"] = hq_display
            logger.info("  %s: corrected location from '%s' to HQ '%s'",
                        c.get("name", "?"), current_location, hq_display)

        # Store full scrape data in enrichment_data
        enrichment_data = c.get("enrichment_data") or {}
        enrichment_data["linkedin_scrape"] = {
            "employeeCount": emp,
            "followerCount": followers,
            "tagline": tagline,
            "description": (desc or "")[:200],
            "founded": founded,
            "website": website,
            "hq_location": hq_location,
            "branch_locations": branch_locations,
            "is_branch": is_branch,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        # Store FULL raw Apify response — never discard data
        enrichment_data["raw_linkedin_scrape"] = {
            **{k: v for k, v in li.items() if k not in ("logos", "backgroundCovers", "similarOrganizations", "profilePicture")},  # skip large image arrays
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        # Check employeeCountRange for hard exclude
        emp_range = li.get("employeeCountRange") or {}
        range_start = emp_range.get("start", 0) if isinstance(emp_range, dict) else 0
        if range_start > 10000:
            logger.warning("  %s: employeeCountRange %d+ (associated members: %s) — flagging for hard exclude",
                          c.get("name", "?"), range_start, emp)
            updates["pipeline_status"] = "scored"
            updates["pipeline_action"] = "HARD EXCLUDE"
            updates["icp_score"] = 0
            updates["reasoning"] = f"Company size {range_start:,}+ employees (LinkedIn employeeCountRange). Only {emp} associated members."
            enrichment_data["linkedin_scrape"]["employeeCountRange"] = emp_range

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
                "apollo_id": p.get("id", ""),
                "email": p.get("email", ""),
                "seniority": p.get("seniority", ""),
                "headline": p.get("headline", ""),
                "city": p.get("city", ""),
                "state": p.get("state", ""),
                "_raw": p,  # Full raw Apollo person record
            })
        return contacts
    except Exception as e:
        logger.warning("Apollo finance scan failed for %s: %s", domain, e)
        return []


def pick_best_finance_contact(contacts: list[dict]) -> dict | None:
    """Pick highest-priority finance contact using centralized tier system.

    Priority: Tier 1 (CFO/Controller) > Tier 3 (Accounting Manager) > Tier 2 (Exec) > Unknown.
    Within same tier, prefer CFO > Controller > VP > Director > Accounting Manager.
    """
    if not contacts:
        return None

    # Sub-priority within tiers (lower = better)
    _SUB_PRIORITY = {
        "cfo": 1, "chief financial officer": 1,
        "controller": 2, "financial controller": 2,
        "vp finance": 3, "vice president of finance": 3,
        "director of finance": 4, "finance director": 4,
        "accounting manager": 5, "finance manager": 6,
        "treasurer": 7, "bookkeeper": 8, "staff accountant": 9,
    }

    def sort_key(c):
        title = c.get("title", "")
        tier, _ = classify_title_tier(title)
        # Tier 1 = best, Tier 3 = next, Tier 2 = after, 0 = last
        tier_order = {1: 0, 3: 1, 2: 2, 0: 3}
        t_lower = title.lower()
        sub = 99
        for key, pri in _SUB_PRIORITY.items():
            if key in t_lower:
                sub = pri
                break
        return (tier_order.get(tier, 3), sub)

    contacts.sort(key=sort_key)
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
                # Full raw response for future reference
                "_raw_response": {**data, "extracted_at": datetime.now(timezone.utc).isoformat()},
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
    # Store cherry-picked fields for quick access
    enrichment_data["apollo"] = {k: v for k, v in apollo_data.items() if k != "_raw_response"}
    # Store full raw response for future reference
    if apollo_data.get("_raw_response"):
        enrichment_data["raw_apollo_enrich"] = apollo_data["_raw_response"]
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
        .in_("pipeline_status", ["raw", "error", "enriched"])
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
                       psbj_data: dict, f5500_data: dict | None = None) -> dict:
    """Run finance scan, PSBJ cross-ref, Form 5500 cross-ref for one company.

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

    has_accounting_mgr = False
    if best_finance:
        title_lower = best_finance.get("title", "").lower()
        has_cfo = "cfo" in title_lower or "chief financial" in title_lower
        has_controller = "controller" in title_lower
        has_accounting_mgr = "accounting manager" in title_lower or "finance manager" in title_lower
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
    # Store contacts with raw data stripped for quick access
    contacts_clean = [{k: v for k, v in c.items() if k != "_raw"} for c in finance_contacts]
    enrichment_data["finance_scan"] = {
        "contacts": contacts_clean,
        "best_contact": {k: v for k, v in best_finance.items() if k != "_raw"} if best_finance else None,
        "has_cfo": has_cfo,
        "has_controller": has_controller,
        "has_accounting_manager": has_accounting_mgr,
        "titles_found": finance_titles_found,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
    }
    # Store full raw Apollo people search response
    enrichment_data["raw_apollo_finance_scan"] = {
        "contacts": [c.get("_raw", c) for c in finance_contacts],
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    if match:
        enrichment_data["psbj"] = match

    # --- Form 5500 cross-reference (big firm auditor = positive signal) ---
    big_firm_auditor = ""
    f5500_match_data = form5500_match(name, f5500_data or {})
    if f5500_match_data:
        big_firm_auditor = f5500_match_data.get("auditor", "")
        notes += f"Current auditor: {big_firm_auditor} (Form 5500 filing). "
        logger.info("  Form 5500 match for %s: audited by %s", name, big_firm_auditor)
        enrichment_data["form5500"] = f5500_match_data

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
        "has_accounting_manager": has_accounting_mgr,
        "finance_contact_name": finance_contact_name,
        "finance_contact_linkedin": finance_contact_linkedin,
        "big_firm_auditor": big_firm_auditor,
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
    f5500_data = load_form5500()

    print(f"  Blacklist: {len(blacklist['names'])} names, {len(blacklist['domains'])} domains")
    print(f"  PSBJ: {len(psbj_data)} companies loaded")
    print(f"  Form 5500: {len(f5500_data)} big-firm clients loaded")

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
            # --- Pre-filter: skip unenrichable companies to save Apollo credits ---
            li_url = company.get("linkedin_url", "")
            if not domain and not li_url:
                print("INCOMPLETE — holding for later (no domain or LinkedIn URL)")
                sb.table(TABLE).update({
                    "pipeline_status": "incomplete",
                    "reasoning": "No domain or LinkedIn URL — holding for manual review or data enrichment",
                }).eq("id", company_id).execute()
                skipped_count += 1
                continue

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
            scoring_input = preprocess_company(company, apollo, psbj_data, f5500_data)

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

        # Progress summary every 25 companies
        if (i + 1) % 25 == 0:
            print(f"\n  --- Progress [{i+1}/{len(companies)}] "
                  f"{scored_count} scored, {error_count} errors, {skipped_count} skipped, "
                  f"{len(scoring_queue)} queued for scoring ---\n")

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

            # Build lookup by company_id or company_name (case-insensitive)
            score_lookup = {}
            for s in scores:
                key = s.get("company_id", s.get("company_name", ""))
                score_lookup[key] = s
                # Also index by lowercase name for case-insensitive matching
                name_key = s.get("company_name", "")
                if name_key:
                    score_lookup[name_key] = s
                    score_lookup[name_key.lower()] = s

            for company, scoring_input in batch:
                company_id = company["id"]
                name = company.get("name", "Unknown")

                result = score_lookup.get(company_id) or score_lookup.get(name) or score_lookup.get(name.lower())
                if not result:
                    logger.warning("No score returned for %s", name)
                    sb.table(TABLE).update({
                        "pipeline_status": "error",
                        "scoring_error": "No score returned from v2 scorer",
                    }).eq("id", company_id).execute()
                    error_count += 1
                    continue

                score = result.get("score", 0)
                breakdown = result.get("breakdown", {})

                # Apply guardrails: rule overrides + AI review for borderline
                from lib.score_guardrails import apply_guardrails
                enrich = company.get("enrichment_data") or {}
                finance_scan = enrich.get("finance_scan", {})

                score, breakdown, action = apply_guardrails(
                    score, breakdown, company, finance_scan
                )

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
            logger.error("Batch scoring failed: %s — retrying individually", e)
            print(f"BATCH ERROR: {e} — retrying individually...")

            for company, scoring_input in batch:
                try:
                    individual_scores = score_companies_v2([scoring_input])
                    if individual_scores:
                        result = individual_scores[0]
                        score = result.get("score", 0)
                        action = "PROCEED" if score >= 80 else "REVIEW" if score >= 60 else "HARD EXCLUDE" if score == 0 else "SKIP"
                        breakdown = result.get("breakdown", {})
                        sb.table(TABLE).update({
                            "icp_score": score,
                            "pipeline_action": action,
                            "score_breakdown": " | ".join(f"{k}: {v}" for k, v in breakdown.items()),
                            "reasoning": result.get("reasoning", ""),
                            "why_this_score": result.get("calibration_notes", ""),
                            "pipeline_status": "scored",
                            "scored_at": datetime.now(timezone.utc).isoformat(),
                            "scoring_error": None,
                        }).eq("id", company["id"]).execute()
                        scored_count += 1
                        print(f"    {company.get('name', '?')}: {score} ({action})")
                    else:
                        raise ValueError("No score returned")
                except Exception as e2:
                    sb.table(TABLE).update({
                        "pipeline_status": "error",
                        "scoring_error": f"Individual scoring failed: {str(e2)[:400]}",
                    }).eq("id", company["id"]).execute()
                    error_count += 1
                    print(f"    {company.get('name', '?')}: ERROR — {e2}")

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
        # Skip if already X-ray rescued (checkpoint)
        xray_done = bool(enrich.get("xray_rescue"))
        if action == "REVIEW" and not finance_contacts and not xray_done:
            review_no_contacts.append((company, si))

    if review_no_contacts:
        print(f"\n--- Phase 3: X-ray rescue for {len(review_no_contacts)} REVIEW companies ---")
        rescore_needed = []

        for company, si in review_no_contacts:
            name = company.get("name", "Unknown")
            domain = company.get("domain", "")
            print(f"  X-ray: {name}...", end=" ", flush=True)

            xray_result = xray_discover_finance_contacts(name, domain=domain or None, max_tier=3)
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


def reset_stale_statuses(sb, tenant_id: str):
    """Reset companies stuck in transitional statuses back to retryable state.

    Companies in 'enriching' or 'scoring' for >30 min are likely from a crashed run.
    Smart reset: companies in 'scoring' that already have an icp_score get
    promoted to 'scored' (not re-enriched), saving Apollo + GPT credits.
    """
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

    # Reset enriching → raw (needs re-enrichment)
    result = (
        sb.table(TABLE)
        .update({"pipeline_status": "raw"})
        .eq("tenant_id", tenant_id)
        .eq("pipeline_status", "enriching")
        .lt("updated_at", cutoff)
        .execute()
    )
    count = len(result.data) if result.data else 0
    if count:
        print(f"  Reset {count} stale 'enriching' → 'raw'")

    # Smart reset for scoring: if already has a score, promote to scored
    stale_scoring = (
        sb.table(TABLE)
        .select("id, icp_score")
        .eq("tenant_id", tenant_id)
        .eq("pipeline_status", "scoring")
        .lt("updated_at", cutoff)
        .execute()
    )
    if stale_scoring.data:
        scored_count = 0
        enriched_count = 0
        for c in stale_scoring.data:
            if c.get("icp_score") and c["icp_score"] > 0:
                # Already scored — promote to scored (skip re-scoring)
                sb.table(TABLE).update({"pipeline_status": "scored"}).eq("id", c["id"]).execute()
                scored_count += 1
            else:
                # Not scored yet — reset to enriched for re-scoring
                sb.table(TABLE).update({"pipeline_status": "enriched"}).eq("id", c["id"]).execute()
                enriched_count += 1
        if scored_count:
            print(f"  Recovered {scored_count} stale 'scoring' → 'scored' (already had scores)")
        if enriched_count:
            print(f"  Reset {enriched_count} stale 'scoring' → 'enriched' (no score yet)")


# ---------------------------------------------------------------------------
# Phase 5: Contact discovery for scored companies
# ---------------------------------------------------------------------------

def run_contact_discovery(sb, companies: list[dict], tenant_id: str) -> tuple[int, int]:
    """Run full multi-source contact discovery for scored companies.

    Stores stub contacts in prospects table (status: sourced) with all
    source IDs preserved. Logs each API call to discovery_log.

    Returns (total_contacts, errors).
    """
    from lib.contact_discovery import discover_all_contacts

    apollo = ApolloClient()
    campaign_id = "00000000-0000-0000-0000-000000000001"
    total_contacts = 0
    total_errors = 0

    for i, company in enumerate(companies):
        company_id = company["id"]
        name = company.get("name", "Unknown")

        print(f"  [{i+1}/{len(companies)}] {name} (score: {company.get('icp_score', '?')})...")

        def log_discovery(source, endpoint, found, verified, rejected, duration_ms, error, params):
            """Audit logger — writes to discovery_log table."""
            try:
                sb.table("discovery_log").insert({
                    "tenant_id": tenant_id,
                    "company_id": company_id,
                    "company_name": name,
                    "source": source,
                    "actor_or_endpoint": endpoint,
                    "contacts_found": found,
                    "contacts_verified": verified,
                    "contacts_rejected": rejected,
                    "duration_ms": duration_ms,
                    "error_message": error,
                    "request_params": params,
                }).execute()
            except Exception as e:
                logger.warning("Failed to log discovery for %s: %s", name, e)

        try:
            contacts = discover_all_contacts(apollo, company, log_fn=log_discovery)

            # Store stub contacts in prospects table
            stubs_created = 0
            for c in contacts:
                slug = c.get("linkedin_slug", "")

                # Dedup: check if prospect already exists
                if slug:
                    existing = (
                        sb.table("prospects")
                        .select("id")
                        .eq("tenant_id", tenant_id)
                        .eq("linkedin_slug", slug)
                        .limit(1)
                        .execute()
                    )
                    if existing.data:
                        continue

                # Use valid prospect_source enum
                source_enum = "apollo_search" if c.get("apollo_id") else "linkedin_search"

                sb.table("prospects").insert({
                    "tenant_id": tenant_id,
                    "campaign_id": campaign_id,
                    "first_name": c.get("first_name", ""),
                    "last_name": c.get("last_name", ""),
                    "title": c.get("title", ""),
                    "headline": c.get("headline", ""),
                    "seniority": c.get("seniority", ""),
                    "email": c.get("email") or None,
                    "linkedin_url": c.get("linkedin_url", ""),
                    "linkedin_slug": slug,
                    "apollo_person_id": c.get("apollo_id") or None,
                    "zoominfo_contact_id": c.get("zoominfo_id") or None,
                    "location": ", ".join(filter(None, [
                        c.get("city", ""), c.get("state", "")
                    ])) or None,
                    "company_name": name,
                    "company_domain": company.get("domain") or None,
                    "company_linkedin_url": company.get("linkedin_url", ""),
                    "icp_score": company.get("icp_score"),
                    "status": "sourced",
                    "source": source_enum,
                    "data_source": ", ".join(c.get("sources", [])),
                }).execute()
                stubs_created += 1

            # Update enrichment_data with discovery summary
            enrichment_data = company.get("enrichment_data") or {}
            enrichment_data["contact_discovery"] = {
                "total_found": len(contacts),
                "stubs_created": stubs_created,
                "sources_used": list(set(
                    s for c in contacts for s in c.get("sources", [])
                )),
                "tier_breakdown": {
                    "tier1": sum(1 for c in contacts if c.get("tier") == 1),
                    "tier2": sum(1 for c in contacts if c.get("tier") == 2),
                    "tier3": sum(1 for c in contacts if c.get("tier") == 3),
                },
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
            sb.table(TABLE).update({
                "enrichment_data": enrichment_data,
            }).eq("id", company_id).execute()

            total_contacts += stubs_created
            print(f"    → {len(contacts)} contacts, {stubs_created} new stubs")

        except Exception as e:
            logger.error("Contact discovery failed for %s: %s", name, e)
            print(f"    → ERROR: {e}")
            total_errors += 1

            # Log the error
            try:
                sb.table("discovery_log").insert({
                    "tenant_id": tenant_id,
                    "company_id": company_id,
                    "company_name": name,
                    "source": "pipeline_error",
                    "actor_or_endpoint": "discover_all_contacts",
                    "error_message": str(e)[:500],
                }).execute()
            except Exception:
                pass

        time.sleep(1)

    return total_contacts, total_errors


def run(tenant_id: str, batch_id: str | None = None, limit: int = 100,
        company_ids: list[str] | None = None):
    """Main entry point."""
    sb = get_supabase()

    # Reset any companies stuck from a previous crashed run
    reset_stale_statuses(sb, tenant_id)

    icp_config = load_icp_config(sb, tenant_id)
    if not icp_config:
        logger.warning("No ICP config for tenant %s — using VWC defaults", tenant_id)

    if company_ids:
        # Fetch specific companies by ID (for re-scoring)
        companies = []
        for cid in company_ids:
            result = sb.table(TABLE).select("*").eq("id", cid).single().execute()
            if result.data:
                companies.append(result.data)
        print(f"Re-scoring {len(companies)} specific companies...")
    else:
        companies = get_raw_companies(sb, tenant_id, batch_id, limit)

    if not companies:
        print("No companies to process")
        return

    print(f"Processing {len(companies)} companies (v2 pipeline)...")

    scored, errors, skipped = process_companies(sb, companies, icp_config)

    print(f"\nScoring done: {scored} scored, {errors} errors, {skipped} skipped "
          f"out of {len(companies)} companies")

    # Phase 5: Full contact discovery for all scored companies
    scored_companies = (
        sb.table(TABLE)
        .select("id, name, domain, location, linkedin_url, icp_score, pipeline_action, enrichment_data")
        .eq("tenant_id", tenant_id)
        .eq("pipeline_status", "scored")
        .not_.is_("pipeline_action", "null")
        .order("icp_score", desc=True)
    )
    if batch_id:
        scored_companies = scored_companies.eq("batch_id", batch_id)
    if company_ids:
        scored_companies = scored_companies.in_("id", company_ids)
    else:
        scored_companies = scored_companies.limit(limit)

    scored_data = scored_companies.execute().data or []

    # Filter to companies that haven't had discovery run yet
    needs_discovery = []
    for c in scored_data:
        enrich = c.get("enrichment_data") or {}
        if "contact_discovery" not in enrich:
            needs_discovery.append(c)

    if needs_discovery:
        print(f"\n--- Phase 5: Contact discovery for {len(needs_discovery)} scored companies ---")
        discovery_contacts, discovery_errors = run_contact_discovery(
            sb, needs_discovery, tenant_id
        )
        print(f"Discovery done: {discovery_contacts} contacts found, {discovery_errors} errors")
    else:
        print("\nNo companies need contact discovery (all already processed)")

    print(f"\nPipeline complete.")


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
    parser.add_argument("--company-ids", default=None,
                        help="Comma-separated company UUIDs (re-score specific companies)")
    args = parser.parse_args()

    company_ids = args.company_ids.split(",") if args.company_ids else None

    try:
        run(args.tenant_id, args.batch_id, args.limit, company_ids)
    except Exception as e:
        logger.error("company_scorer failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)

    # Auto-backup after scoring run
    try:
        export_script = os.path.join(BASE_DIR, "scripts", "export_pipeline_data.py")
        if os.path.exists(export_script):
            print("\nRunning auto-backup...")
            os.system(f'"{sys.executable}" "{export_script}" --tenant-id {args.tenant_id}')
    except Exception as e:
        logger.warning("Auto-backup failed (non-fatal): %s", e)


if __name__ == "__main__":
    main()
