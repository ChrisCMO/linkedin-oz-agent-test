#!/usr/bin/env python3
"""Chad Demo: Apollo x ZoomInfo ICP Pipeline

Runs two ICP passes (Audit & Tax PNW + Benefit Plan National), each doing:
1. Apollo free search -> get pool + Apollo IDs
2. ZoomInfo free search -> get pool
3. Deduplicate ZoomInfo vs Apollo, cross-match unique ZoomInfo prospects via Apollo free search
4. Enrich top 25 per ICP via Apollo person enrichment (1 credit each)
5. AI score all enriched prospects
6. Apify LinkedIn activity check on top 15 per ICP
7. Generate connection notes + 3-message sequences for top 5 per ICP
8. Export CSVs

Usage:
    python scripts/chad_demo.py
"""

import csv
import json
import os
import random
import sys
import time
import logging
import traceback
from datetime import datetime

# -- Path setup --
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from lib.apollo import ApolloClient
from db.connect import get_supabase
from mvp.backend.services.scoring import score_prospects
from mvp.backend.services.message_gen_svc import generate_messages, generate_connection_note

# -- Logging --
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("chad_demo")

# -- Constants --
TENANT_ID = "00000000-0000-0000-0000-000000000001"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
APIFY_TOKEN = os.environ.get("APIFY_API_KEY", "")
POSTS_ACTOR_ID = "LQQIXN9Othf8f7R5n"

SENDERS = ["Adrienne Nordland", "Melinda Johnson"]

# -- ICP Configs --

ICP_1 = {
    "name": "VWC ICP 1 — Audit & Tax (PNW)",
    "target_titles": [
        "CFO", "Chief Financial Officer", "Controller", "Comptroller",
        "VP Finance", "Vice President Finance", "Director of Finance",
        "Owner", "President", "CEO",
    ],
    "target_seniorities": ["c_suite", "vp", "director", "owner", "founder"],
    "target_industries": [
        "manufacturing", "construction", "real estate",
        "professional services", "hospitality", "nonprofit",
    ],
    "target_locations": ["Washington, United States", "Oregon, United States"],
    "employee_count_ranges": ["11,50", "51,200", "201,500", "501,1000"],
    "revenue_ranges": ["25000000,150000000"],
    "keywords": [
        "manufacturing", "construction", "real estate",
        "professional services", "hospitality", "nonprofit",
    ],
    "scoring_config": {
        "custom_notes": (
            "VWC CPAs Audit & Tax. Sweet spot: $50M-$100M revenue, 100-300 employees "
            "in Seattle/PNW. Hard ceiling: >$150M revenue = exclude. Priority industries: "
            "manufacturing (#1), commercial RE (#2), professional services (#3), "
            "hospitality (#4), nonprofit (#5), construction (#6). Exclude: public companies, "
            "PE-backed, government, banking."
        ),
        "hard_exclusions": [
            "Revenue > $150M",
            "Public companies",
            "PE-backed firms",
            "Government agencies",
            "Banking/financial institutions",
        ],
    },
}

ICP_2 = {
    "name": "VWC ICP 2 — Benefit Plan Audit (National)",
    "target_titles": [
        "CFO", "Chief Financial Officer", "Controller",
        "HR Director", "VP Human Resources", "Chief People Officer",
        "Director of Human Resources", "VP Benefits",
    ],
    "target_seniorities": ["c_suite", "vp", "director"],
    "target_industries": [],
    "target_locations": [],  # National
    "employee_count_ranges": ["51,200", "201,500", "501,1000", "1001,5000"],
    "revenue_ranges": [],
    "keywords": [],  # No keywords — ICP 2 is driven by employee count, not industry
    "scoring_config": {
        "custom_notes": (
            "VWC Benefit Plan Audit. Companies with 120+ eligible plan participants "
            "legally require audit. First-time threshold crossers are highest priority. "
            "National scope. All industries valid except government. HR leader is primary "
            "contact at companies >$150M or 500+ employees or public."
        ),
        "hard_exclusions": [
            "Government entities",
            "Companies with fewer than 100 employees",
        ],
    },
}

# Apollo search params per ICP
APOLLO_SEARCH_ICP1 = {
    "person_titles": ICP_1["target_titles"],
    "person_seniorities": ICP_1["target_seniorities"],
    "organization_num_employees_ranges": ICP_1["employee_count_ranges"],
    "person_locations": ICP_1["target_locations"],
    "q_organization_keyword_tags": ICP_1["keywords"],
    "per_page": 25,
    "page": 1,
}

APOLLO_SEARCH_ICP2 = {
    "person_titles": ICP_2["target_titles"],
    "person_seniorities": ICP_2["target_seniorities"],
    "organization_num_employees_ranges": ICP_2["employee_count_ranges"],
    # No keyword tags — ICP 2 is employee-count driven, not industry-specific
    "per_page": 25,
    "page": 1,
}

# ZoomInfo search params per ICP
ZI_SEARCH_ICP1 = {
    "job_title": "CFO OR Controller OR VP Finance OR Director of Finance",
    "job_function": "Finance",
    "states": ["Washington", "Oregon"],
    "employee_counts": "50to99,100to249,250to499,500to999",
    "company_type": "private",
}

ZI_SEARCH_ICP2 = {
    "job_title": "CFO OR Controller OR HR Director OR VP Human Resources",
    "job_function": "Finance,Human Resources",
    "states": ["Texas", "California", "Illinois", "New York", "Florida"],
    "employee_counts": "100to249,250to499,500to999,1000to4999",
    "company_type": "private",
}


# ---------------------------------------------------------------------------
# ZoomInfo helpers
# ---------------------------------------------------------------------------

def authenticate_zoominfo() -> dict:
    """Auth with ZoomInfo, return JWT headers."""
    import requests

    username = os.environ["ZOOMINFO_USERNAME"]
    password = os.environ["ZOOMINFO_PASSWORD"]

    log.info("Authenticating with ZoomInfo...")
    resp = requests.post(
        "https://api.zoominfo.com/authenticate",
        json={"username": username, "password": password},
        timeout=30,
    )
    resp.raise_for_status()
    jwt = resp.json()["jwt"]
    log.info("ZoomInfo auth successful.")
    return {
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json",
    }


def search_zoominfo(
    headers: dict,
    job_title: str,
    job_function: str,
    states: list[str],
    employee_counts: str,
    company_type: str = "private",
    rpp: int = 25,
) -> list[dict]:
    """Search ZoomInfo contacts across multiple states. Return list of contacts."""
    import requests

    all_contacts = []
    for state in states:
        log.info(f"  ZoomInfo search: state={state}")
        try:
            resp = requests.post(
                "https://api.zoominfo.com/search/contact",
                headers=headers,
                json={
                    "jobTitle": job_title,
                    "jobFunction": job_function,
                    "state": state,
                    "employeeCount": employee_counts,
                    "companyType": company_type,
                    "locationSearchType": "Person",
                    "rpp": rpp,
                    "page": 1,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            contacts = data.get("data", [])
            total = data.get("totalResults", 0)
            log.info(f"    -> {len(contacts)} contacts returned (total pool: {total})")
            all_contacts.extend(contacts)
        except Exception as e:
            log.error(f"    ZoomInfo search failed for {state}: {e}")

        time.sleep(random.uniform(0.5, 1.0))

    return all_contacts


# ---------------------------------------------------------------------------
# Apollo cross-match
# ---------------------------------------------------------------------------

def search_apollo_for_person(
    apollo: ApolloClient,
    first_name: str,
    last_name: str,
    company_name: str,
    titles: list[str],
) -> str | None:
    """Free Apollo search to find a specific person. Return Apollo ID or None."""
    try:
        result = apollo._request(
            "POST",
            "/api/v1/mixed_people/api_search",
            json_body={
                "q_keywords": f"{first_name} {last_name} {company_name}",
                "person_titles": titles[:5],
                "per_page": 5,
                "page": 1,
            },
        )
        if "error" in result:
            return None

        for person in result.get("people", []):
            org = person.get("organization") or {}
            org_name = (org.get("name") or "").lower()
            if company_name.lower() in org_name or org_name in company_name.lower():
                return person.get("id")
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Apify LinkedIn activity
# ---------------------------------------------------------------------------

def check_linkedin_activity(linkedin_url: str, apify_token: str) -> dict | None:
    """Run Apify posts scraper for one profile. Return activity dict or None."""
    import requests

    if not linkedin_url or not apify_token:
        return None

    try:
        # Start run
        resp = requests.post(
            f"https://api.apify.com/v2/acts/{POSTS_ACTOR_ID}/runs",
            headers={
                "Authorization": f"Bearer {apify_token}",
                "Content-Type": "application/json",
            },
            json={"profileUrl": linkedin_url, "maxPosts": 10},
            timeout=30,
        )
        resp.raise_for_status()
        run_data = resp.json()["data"]
        run_id = run_data["id"]
        dataset_id = run_data["defaultDatasetId"]

        # Poll for completion (max ~2 minutes)
        for _ in range(24):
            time.sleep(5)
            status_resp = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                headers={"Authorization": f"Bearer {apify_token}"},
                timeout=15,
            )
            status = status_resp.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED"):
                break

        if status != "SUCCEEDED":
            log.warning(f"  Apify run {run_id} ended with status: {status}")
            return None

        # Get results
        items_resp = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            headers={"Authorization": f"Bearer {apify_token}"},
            timeout=15,
        )
        items = items_resp.json()

        # Extract slug from linkedin_url for author matching
        slug = linkedin_url.rstrip("/").split("/")[-1]

        authored_posts = []
        for item in items:
            author = item.get("author", {})
            if isinstance(author, dict) and author.get("username", "") == slug:
                authored_posts.append(item)

        # Determine recency
        recent_posts = 0
        for post in authored_posts:
            posted_at = post.get("posted_at", {})
            if isinstance(posted_at, dict):
                date_str = posted_at.get("date", "")
            else:
                date_str = str(posted_at)
            if date_str:
                try:
                    post_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    days_ago = (datetime.now(post_date.tzinfo) - post_date).days
                    if days_ago <= 90:
                        recent_posts += 1
                except Exception:
                    pass

        return {
            "total_posts": len(authored_posts),
            "recent_posts_90d": recent_posts,
            "is_active": recent_posts >= 2,
            "checked_at": datetime.now().isoformat(),
        }

    except Exception as e:
        log.error(f"  Apify activity check failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate_zi_vs_apollo(zi_contacts: list[dict], apollo_people: list[dict]) -> list[dict]:
    """Return ZoomInfo contacts that don't appear in Apollo results (by name+company)."""
    apollo_keys = set()
    for p in apollo_people:
        fn = (p.get("first_name") or "").lower().strip()
        ln = (p.get("last_name") or "").lower().strip()
        co = (p.get("organization", {}) or {}).get("name", "").lower().strip()
        apollo_keys.add((fn, ln, co))

    unique = []
    for c in zi_contacts:
        fn = (c.get("firstName") or "").lower().strip()
        ln = (c.get("lastName") or "").lower().strip()
        co_obj = c.get("company") or {}
        co = (co_obj.get("name") or "").lower().strip()
        if (fn, ln, co) not in apollo_keys:
            unique.append(c)

    return unique


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_icp_pipeline(
    icp_name: str,
    icp_config: dict,
    apollo_search_params: dict,
    zi_search_params: dict,
    campaign_id: str,
    icp_id: str,
    apollo: ApolloClient,
    zi_headers: dict,
    sb,
) -> tuple[list[dict], list[dict]]:
    """Run full pipeline for one ICP. Returns (enriched_prospects, unenriched_pool)."""

    log.info("=" * 70)
    log.info(f"PIPELINE START: {icp_name}")
    log.info("=" * 70)

    # -----------------------------------------------------------------------
    # Step 1: Apollo free search
    # -----------------------------------------------------------------------
    log.info("Step 1: Apollo free search...")
    apollo_result = apollo.search_people(
        **apollo_search_params,
        tenant_id=TENANT_ID,
        campaign_id=campaign_id,
    )
    apollo_people = apollo_result.get("people", [])
    apollo_total = apollo_result.get("pagination", {}).get("total_entries", 0)
    log.info(f"  Apollo: {len(apollo_people)} returned, {apollo_total} total pool")

    # -----------------------------------------------------------------------
    # Step 2: ZoomInfo free search
    # -----------------------------------------------------------------------
    log.info("Step 2: ZoomInfo free search...")
    zi_contacts = search_zoominfo(
        zi_headers,
        job_title=zi_search_params["job_title"],
        job_function=zi_search_params["job_function"],
        states=zi_search_params["states"],
        employee_counts=zi_search_params["employee_counts"],
        company_type=zi_search_params.get("company_type", "private"),
    )
    log.info(f"  ZoomInfo: {len(zi_contacts)} contacts total")

    # -----------------------------------------------------------------------
    # Step 3: Deduplicate & cross-match
    # -----------------------------------------------------------------------
    log.info("Step 3: Deduplicating ZoomInfo vs Apollo...")
    unique_zi = deduplicate_zi_vs_apollo(zi_contacts, apollo_people)
    log.info(f"  {len(unique_zi)} unique ZoomInfo contacts (not in Apollo results)")

    # Cross-match unique ZoomInfo contacts via Apollo search
    cross_matched = []
    for i, contact in enumerate(unique_zi):
        fn = contact.get("firstName", "")
        ln = contact.get("lastName", "")
        co_name = (contact.get("company") or {}).get("name", "")
        log.info(f"  Cross-matching {i+1}/{len(unique_zi)}: {fn} {ln} @ {co_name}")

        apollo_id = search_apollo_for_person(
            apollo, fn, ln, co_name, icp_config["target_titles"],
        )
        if apollo_id:
            cross_matched.append({
                "apollo_id": apollo_id,
                "first_name": fn,
                "last_name": ln,
                "company_name": co_name,
                "source": "zoominfo",
                "zi_accuracy_score": contact.get("contactAccuracyScore"),
                "zi_title": contact.get("jobTitle"),
            })
            log.info(f"    -> Matched Apollo ID: {apollo_id}")
        else:
            log.info(f"    -> No Apollo match found")

        time.sleep(random.uniform(0.5, 1.0))

    log.info(f"  Cross-matched {len(cross_matched)} ZoomInfo contacts to Apollo")

    # -----------------------------------------------------------------------
    # Build combined candidate list (Apollo IDs for enrichment)
    # -----------------------------------------------------------------------
    # Apollo-sourced candidates
    candidates = []
    for p in apollo_people:
        candidates.append({
            "apollo_id": p.get("id"),
            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "company_name": (p.get("organization") or {}).get("name", ""),
            "source": "apollo",
            "zi_accuracy_score": None,
        })

    # Add cross-matched ZoomInfo candidates
    # Avoid duplicates by apollo_id
    existing_ids = {c["apollo_id"] for c in candidates}
    for cm in cross_matched:
        if cm["apollo_id"] not in existing_ids:
            candidates.append(cm)
            existing_ids.add(cm["apollo_id"])

    log.info(f"  Combined candidate pool: {candidates.__len__()} (for enrichment ranking)")

    # -----------------------------------------------------------------------
    # Build unenriched pool for CSV
    # -----------------------------------------------------------------------
    unenriched_pool = []
    for p in apollo_people:
        org = p.get("organization") or {}
        unenriched_pool.append({
            "source": "apollo",
            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "title": p.get("title"),
            "company_name": org.get("name"),
            "company_industry": org.get("industry"),
            "city": p.get("city"),
            "state": p.get("state"),
            "linkedin_url": p.get("linkedin_url"),
            "apollo_id": p.get("id"),
        })
    for c in zi_contacts:
        co = c.get("company") or {}
        unenriched_pool.append({
            "source": "zoominfo",
            "first_name": c.get("firstName"),
            "last_name": c.get("lastName"),
            "title": c.get("jobTitle"),
            "company_name": co.get("name"),
            "company_industry": co.get("industry"),
            "city": c.get("city"),
            "state": c.get("state"),
            "linkedin_url": None,
            "apollo_id": None,
            "zi_accuracy_score": c.get("contactAccuracyScore"),
        })

    # -----------------------------------------------------------------------
    # Step 4: Enrich top 25 via Apollo
    # -----------------------------------------------------------------------
    log.info("Step 4: Enriching top 25 candidates via Apollo...")
    enrich_ids = [c["apollo_id"] for c in candidates[:25] if c.get("apollo_id")]
    enriched = []
    for i, aid in enumerate(enrich_ids):
        log.info(f"  Enriching prospect {i+1}/{len(enrich_ids)}...")
        try:
            result = apollo.enrich_person(aid, tenant_id=TENANT_ID, campaign_id=campaign_id)
            person = result.get("person")
            if person:
                extracted = ApolloClient._extract_person(person)
                # Attach source origin from candidates
                matching_candidate = next((c for c in candidates if c["apollo_id"] == aid), None)
                if matching_candidate:
                    extracted["source_origin"] = matching_candidate["source"]
                    extracted["zi_accuracy_score"] = matching_candidate.get("zi_accuracy_score")
                enriched.append(extracted)
            else:
                log.warning(f"    No person data for Apollo ID {aid}")
        except Exception as e:
            log.error(f"    Enrichment failed for {aid}: {e}")

        time.sleep(random.uniform(1.0, 2.0))

    log.info(f"  Enriched {len(enriched)} prospects")

    # -----------------------------------------------------------------------
    # Step 5: AI score all enriched prospects
    # -----------------------------------------------------------------------
    log.info("Step 5: AI scoring enriched prospects...")
    try:
        scores = score_prospects(enriched, icp_config, model="gpt-4o-mini")
        # Merge scores back into enriched list
        score_map = {s["apollo_id"]: s for s in scores}
        for p in enriched:
            sid = p.get("apollo_id")
            if sid and sid in score_map:
                p["icp_score"] = score_map[sid]["score"]
                p["icp_breakdown"] = score_map[sid].get("breakdown", {})
                p["icp_reasoning"] = score_map[sid].get("reasoning", "")
            else:
                p["icp_score"] = 0
                p["icp_breakdown"] = {}
                p["icp_reasoning"] = "Scoring failed"

        # Sort by score descending
        enriched.sort(key=lambda x: x.get("icp_score", 0), reverse=True)
        log.info(f"  Scored. Top score: {enriched[0]['icp_score'] if enriched else 'N/A'}")
    except Exception as e:
        log.error(f"  Scoring failed: {e}")
        traceback.print_exc()
        for p in enriched:
            p["icp_score"] = 0
            p["icp_breakdown"] = {}
            p["icp_reasoning"] = "Scoring failed"

    # -----------------------------------------------------------------------
    # Step 6: Apify LinkedIn activity check on top 15
    # -----------------------------------------------------------------------
    log.info("Step 6: Checking LinkedIn activity for top 15...")
    for i, p in enumerate(enriched[:15]):
        url = p.get("linkedin_url")
        if not url:
            log.info(f"  {i+1}/15: {p.get('name', '?')} — no LinkedIn URL, skipping")
            p["linkedin_activity"] = None
            continue

        log.info(f"  {i+1}/15: {p.get('name', '?')} — checking activity...")
        activity = check_linkedin_activity(url, APIFY_TOKEN)
        p["linkedin_activity"] = activity
        if activity:
            log.info(f"    -> {activity['total_posts']} posts, {activity['recent_posts_90d']} recent, active={activity['is_active']}")
        else:
            log.info(f"    -> Activity check returned no data")

    # -----------------------------------------------------------------------
    # Step 7: Generate connection notes + messages for top 5
    # -----------------------------------------------------------------------
    log.info("Step 7: Generating messages for top 5...")
    for i, p in enumerate(enriched[:5]):
        log.info(f"  {i+1}/5: {p.get('name', '?')} — generating messages...")

        company_data = {
            "name": p.get("company_name"),
            "industry": p.get("company_industry"),
            "employee_count_range": p.get("company_employee_range"),
            "domain": p.get("company_domain"),
        }

        # Connection notes for both senders
        notes = {}
        for sender in SENDERS:
            try:
                note = generate_connection_note(p, company_data, sender)
                notes[sender.split()[0].lower()] = note
                log.info(f"    Note ({sender.split()[0]}): {note[:60]}...")
            except Exception as e:
                log.error(f"    Connection note failed for {sender}: {e}")
                notes[sender.split()[0].lower()] = ""
            time.sleep(random.uniform(0.5, 1.0))

        p["connection_notes"] = notes

        # 3-message sequence
        try:
            messages = generate_messages(p, company_data, icp_config)
            p["message_sequence"] = messages
            for msg in messages:
                log.info(f"    Msg {msg['step']}: {msg['text'][:60]}...")
        except Exception as e:
            log.error(f"    Message generation failed: {e}")
            traceback.print_exc()
            p["message_sequence"] = []

        time.sleep(random.uniform(0.5, 1.0))

    # -----------------------------------------------------------------------
    # Step 8: Store enriched prospects in Supabase
    # -----------------------------------------------------------------------
    log.info("Step 8: Storing enriched prospects in Supabase...")
    for i, p in enumerate(enriched):
        try:
            slug = None
            li_url = p.get("linkedin_url")
            if li_url:
                slug = li_url.rstrip("/").split("/")[-1]

            location = f"{p.get('city', '')}, {p.get('state', '')}".strip(", ")

            row = {
                "tenant_id": TENANT_ID,
                "campaign_id": campaign_id,
                "apollo_person_id": p.get("apollo_id"),
                "linkedin_url": li_url,
                "linkedin_slug": slug,
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
                "email": p.get("email"),
                "title": p.get("title"),
                "seniority": p.get("seniority"),
                "headline": p.get("headline"),
                "location": location,
                "company_name": p.get("company_name"),
                "company_domain": p.get("company_domain"),
                "status": "scored",
                "source": "apollo_search",
                "icp_score": p.get("icp_score", 0),
                "icp_score_breakdown": p.get("icp_breakdown", {}),
                "icp_reasoning": p.get("icp_reasoning", ""),
                "raw_apollo_data": {
                    "source_origin": p.get("source_origin", "apollo"),
                    "zoominfo_accuracy_score": p.get("zi_accuracy_score"),
                    "linkedin_activity": p.get("linkedin_activity"),
                    "connection_notes": p.get("connection_notes"),
                },
            }

            sb.table("prospects").insert(row).execute()
            log.info(f"  Stored {i+1}/{len(enriched)}: {p.get('name', '?')} (score={p.get('icp_score', 0)})")
        except Exception as e:
            log.error(f"  Failed to store prospect {p.get('name', '?')}: {e}")

    log.info(f"PIPELINE COMPLETE: {icp_name}")
    return enriched, unenriched_pool


# ---------------------------------------------------------------------------
# CSV Export
# ---------------------------------------------------------------------------

ENRICHED_FIELDS = [
    "apollo_id", "name", "first_name", "last_name", "title", "seniority",
    "email", "linkedin_url", "city", "state", "headline",
    "company_name", "company_industry", "company_employees",
    "company_employee_range", "company_website", "company_domain",
    "company_revenue", "company_founded",
    "icp_score", "icp_reasoning", "source_origin", "zi_accuracy_score",
]

POOL_FIELDS = [
    "source", "first_name", "last_name", "title", "company_name",
    "company_industry", "city", "state", "linkedin_url", "apollo_id",
    "zi_accuracy_score",
]


def export_csv(prospects: list[dict], filename: str, enriched: bool = True):
    """Write prospects to CSV in the output/ directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    fields = ENRICHED_FIELDS if enriched else POOL_FIELDS

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for p in prospects:
            writer.writerow(p)

    log.info(f"  Exported {len(prospects)} rows -> {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()
    log.info("=" * 70)
    log.info("CHAD DEMO PIPELINE")
    log.info(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    # -- Setup --
    apollo = ApolloClient()
    sb = get_supabase()

    # Authenticate ZoomInfo
    zi_headers = authenticate_zoominfo()

    # Ensure linkedin_accounts row exists
    la_result = sb.table("linkedin_accounts").select("id").eq(
        "tenant_id", TENANT_ID,
    ).limit(1).execute()

    linkedin_account_id = None
    if la_result.data:
        linkedin_account_id = la_result.data[0]["id"]
        log.info(f"Using existing linkedin_account: {linkedin_account_id}")
    else:
        la_insert = sb.table("linkedin_accounts").insert({
            "tenant_id": TENANT_ID,
            "unipile_account_id": "placeholder",
            "owner_name": "Demo Account",
            "account_type": "standard",
            "status": "ok",
        }).execute()
        linkedin_account_id = la_insert.data[0]["id"]
        log.info(f"Created placeholder linkedin_account: {linkedin_account_id}")

    # -- ICP 1: Audit & Tax (PNW) --
    log.info("\nCreating Campaign & ICP for ICP 1...")
    campaign1 = sb.table("campaigns").insert({
        "tenant_id": TENANT_ID,
        "name": "Chad Demo — ICP 1 Audit & Tax",
        "status": "draft",
        "linkedin_account_id": linkedin_account_id or "placeholder",
    }).execute()
    campaign1_id = campaign1.data[0]["id"]
    log.info(f"  Campaign 1 ID: {campaign1_id}")

    icp1_row = sb.table("icps").insert({
        "tenant_id": TENANT_ID,
        "campaign_id": campaign1_id,
        **ICP_1,
        "is_active": True,
    }).execute()
    icp1_id = icp1_row.data[0]["id"]
    log.info(f"  ICP 1 ID: {icp1_id}")

    enriched1, pool1 = run_icp_pipeline(
        icp_name="ICP 1 — Audit & Tax (PNW)",
        icp_config=ICP_1,
        apollo_search_params=APOLLO_SEARCH_ICP1,
        zi_search_params=ZI_SEARCH_ICP1,
        campaign_id=campaign1_id,
        icp_id=icp1_id,
        apollo=apollo,
        zi_headers=zi_headers,
        sb=sb,
    )

    # -- ICP 2: Benefit Plan Audit (National) --
    log.info("\nCreating Campaign & ICP for ICP 2...")
    campaign2 = sb.table("campaigns").insert({
        "tenant_id": TENANT_ID,
        "name": "Chad Demo — ICP 2 Benefit Plan",
        "status": "draft",
        "linkedin_account_id": linkedin_account_id or "placeholder",
    }).execute()
    campaign2_id = campaign2.data[0]["id"]
    log.info(f"  Campaign 2 ID: {campaign2_id}")

    icp2_row = sb.table("icps").insert({
        "tenant_id": TENANT_ID,
        "campaign_id": campaign2_id,
        **ICP_2,
        "is_active": True,
    }).execute()
    icp2_id = icp2_row.data[0]["id"]
    log.info(f"  ICP 2 ID: {icp2_id}")

    enriched2, pool2 = run_icp_pipeline(
        icp_name="ICP 2 — Benefit Plan Audit (National)",
        icp_config=ICP_2,
        apollo_search_params=APOLLO_SEARCH_ICP2,
        zi_search_params=ZI_SEARCH_ICP2,
        campaign_id=campaign2_id,
        icp_id=icp2_id,
        apollo=apollo,
        zi_headers=zi_headers,
        sb=sb,
    )

    # -- Export CSVs --
    log.info("\n" + "=" * 70)
    log.info("EXPORTING CSVs")
    log.info("=" * 70)
    export_csv(enriched1, "chad_demo_icp1_audit_tax.csv", enriched=True)
    export_csv(enriched2, "chad_demo_icp2_benefit_plan.csv", enriched=True)
    export_csv(pool1, "chad_demo_icp1_full_pool.csv", enriched=False)
    export_csv(pool2, "chad_demo_icp2_full_pool.csv", enriched=False)

    # -- Summary --
    elapsed = time.time() - start_time
    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    log.info(f"{'Metric':<40} {'ICP 1 (Audit/Tax)':<20} {'ICP 2 (Benefit)':<20}")
    log.info("-" * 80)
    log.info(f"{'Apollo pool size':<40} {len([p for p in pool1 if p['source']=='apollo']):<20} {len([p for p in pool2 if p['source']=='apollo']):<20}")
    log.info(f"{'ZoomInfo pool size':<40} {len([p for p in pool1 if p['source']=='zoominfo']):<20} {len([p for p in pool2 if p['source']=='zoominfo']):<20}")
    log.info(f"{'Enriched prospects':<40} {len(enriched1):<20} {len(enriched2):<20}")

    avg1 = sum(p.get("icp_score", 0) for p in enriched1) / max(len(enriched1), 1)
    avg2 = sum(p.get("icp_score", 0) for p in enriched2) / max(len(enriched2), 1)
    log.info(f"{'Avg ICP score':<40} {avg1:<20.1f} {avg2:<20.1f}")

    top1 = enriched1[0]["icp_score"] if enriched1 else 0
    top2 = enriched2[0]["icp_score"] if enriched2 else 0
    log.info(f"{'Top ICP score':<40} {top1:<20} {top2:<20}")

    active1 = sum(1 for p in enriched1[:15] if (p.get("linkedin_activity") or {}).get("is_active"))
    active2 = sum(1 for p in enriched2[:15] if (p.get("linkedin_activity") or {}).get("is_active"))
    log.info(f"{'LinkedIn-active (top 15)':<40} {active1:<20} {active2:<20}")

    msgs1 = sum(1 for p in enriched1[:5] if p.get("message_sequence"))
    msgs2 = sum(1 for p in enriched2[:5] if p.get("message_sequence"))
    log.info(f"{'Messages generated (top 5)':<40} {msgs1:<20} {msgs2:<20}")

    log.info("-" * 80)
    log.info(f"Total credits used: ~{len(enriched1) + len(enriched2)} Apollo enrichment credits")
    log.info(f"Total time: {elapsed:.0f}s ({elapsed/60:.1f}m)")
    log.info(f"CSV files in: {OUTPUT_DIR}/")
    log.info("=" * 70)
    log.info("DONE")


if __name__ == "__main__":
    main()
