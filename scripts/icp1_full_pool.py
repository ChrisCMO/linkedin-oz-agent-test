#!/usr/bin/env python3
"""Pull full ICP 1 Audit & Tax prospect pool from ZoomInfo + Apollo for Chad review."""

import os, sys, csv, time, random, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from lib.apollo import ApolloClient
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

apollo = ApolloClient()

# Auth ZoomInfo
zi_resp = requests.post("https://api.zoominfo.com/authenticate", json={
    "username": os.environ["ZOOMINFO_USERNAME"],
    "password": os.environ["ZOOMINFO_PASSWORD"],
})
zi_jwt = zi_resp.json()["jwt"]
zi_headers = {"Authorization": f"Bearer {zi_jwt}", "Content-Type": "application/json"}

all_contacts = []
titles_zi = "CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance OR Owner OR President"

# --- ZOOMINFO: Pull up to 40 pages per state (1,000 per state) ---
log.info("=" * 60)
log.info("ZOOMINFO SEARCH — WA, OR, ID")
log.info("=" * 60)

for state in ["Washington", "Oregon", "Idaho"]:
    state_count = 0
    max_pages = 40
    for page in range(1, max_pages + 1):
        resp = requests.post("https://api.zoominfo.com/search/contact", headers=zi_headers, json={
            "jobTitle": titles_zi,
            "jobFunction": "Finance",
            "state": state,
            "employeeCount": "10to19,20to49,50to99,100to249,250to499,500to999",
            "companyType": "private",
            "locationSearchType": "Person",
            "rpp": 25,
            "page": page,
        })
        data = resp.json()
        contacts = data.get("data", [])
        max_results = data.get("maxResults", 0)

        if not contacts:
            break

        for c in contacts:
            co = c.get("company", {})
            all_contacts.append({
                "source": "ZoomInfo",
                "first_name": c.get("firstName", ""),
                "last_name": c.get("lastName", ""),
                "title": c.get("jobTitle", ""),
                "company": co.get("name", ""),
                "state": state,
                "accuracy_score": c.get("contactAccuracyScore", ""),
                "has_email": c.get("hasEmail", False),
                "has_phone": c.get("hasDirectPhone", False),
                "has_revenue": c.get("hasCompanyRevenue", False),
                "has_employees": c.get("hasCompanyEmployeeCount", False),
                "valid_date": c.get("validDate", ""),
                "zoominfo_id": c.get("id", ""),
            })

        state_count += len(contacts)
        if page % 10 == 0:
            log.info(f"  {state}: page {page}, {state_count} so far (pool: {max_results})")

        if len(contacts) < 25:
            break
        time.sleep(random.uniform(0.3, 0.6))

    log.info(f"  {state}: DONE — {state_count} contacts (pool: {max_results})")

log.info(f"ZoomInfo total: {len(all_contacts)}")

# --- APOLLO: Pull OR + WA ---
log.info("")
log.info("=" * 60)
log.info("APOLLO SEARCH — OR, WA (with ICP industry keywords)")
log.info("=" * 60)

apollo_contacts = []
apollo_titles = ["CFO", "Chief Financial Officer", "Controller", "VP Finance",
                 "Director of Finance", "Owner", "President"]
apollo_keywords = ["manufacturing", "construction", "real estate",
                   "professional services", "hospitality", "nonprofit"]

for loc in ["Oregon, United States", "Washington, United States"]:
    loc_count = 0
    state_name = "Oregon" if "Oregon" in loc else "Washington"
    for page_num in range(1, 11):  # up to 1,000 per state
        result = apollo.search_people(
            person_titles=apollo_titles,
            person_seniorities=["c_suite", "vp", "director", "owner"],
            person_locations=[loc],
            organization_num_employees_ranges=["11-50", "51-200", "201-500", "501-1000"],
            q_organization_keyword_tags=apollo_keywords,
            per_page=100,
            page=page_num,
        )
        people = result.get("people", [])

        if not people:
            break

        for p in people:
            org = p.get("organization") or {}
            apollo_contacts.append({
                "source": "Apollo",
                "apollo_id": p.get("id", ""),
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name_obfuscated", ""),
                "title": p.get("title", ""),
                "company": org.get("name", ""),
                "state": state_name,
                "accuracy_score": "",
                "has_email": p.get("has_email", False),
                "has_phone": str(p.get("has_direct_phone", "")),
                "has_revenue": org.get("has_revenue", False),
                "has_employees": org.get("has_employee_count", False),
                "valid_date": p.get("last_refreshed_at", ""),
                "zoominfo_id": "",
            })

        loc_count += len(people)
        if page_num % 5 == 0:
            log.info(f"  Apollo {state_name}: page {page_num}, {loc_count} contacts")

        if len(people) < 100:
            break
        time.sleep(0.3)

    log.info(f"  Apollo {state_name}: DONE — {loc_count} contacts")

log.info(f"Apollo total: {len(apollo_contacts)}")

# --- COMBINE + EXPORT ---
combined = all_contacts + apollo_contacts

log.info("")
log.info("=" * 60)
log.info(f"COMBINED TOTAL: {len(combined)} contacts")
log.info(f"  ZoomInfo: {len(all_contacts)} (WA/OR/ID)")
log.info(f"  Apollo:   {len(apollo_contacts)} (WA/OR, obfuscated last names)")
log.info("=" * 60)

# NOTE: Apollo 'Washington' includes some DC results — flagged in notes
os.makedirs("output", exist_ok=True)
outfile = "output/icp1_full_prospect_pool.csv"
fields = ["source", "apollo_id", "zoominfo_id", "first_name", "last_name", "title",
          "company", "state", "accuracy_score", "has_email", "has_phone",
          "has_revenue", "has_employees", "valid_date"]

with open(outfile, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fields, restval="", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(combined)

log.info(f"\nExported to: {outfile}")

# Breakdown
source_state = Counter((c["source"], c["state"]) for c in combined)
for (src, st), count in sorted(source_state.items()):
    log.info(f"  {src} {st}: {count}")

log.info("")
log.info("NOTE: Apollo 'Washington' results may include some Washington DC contacts.")
log.info("ZoomInfo results are properly filtered to Washington State only.")
log.info(f"\nZoomInfo pool sizes (total available, not all pulled):")
log.info(f"  Washington: ~6,728 | Oregon: ~3,358 | Idaho: ~1,329 | TOTAL: ~11,415")
