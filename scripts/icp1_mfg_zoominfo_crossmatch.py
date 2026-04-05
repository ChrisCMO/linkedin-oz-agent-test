#!/usr/bin/env python3
"""Cross-match 1,380 manufacturing companies against ZoomInfo to get employee/revenue indicators."""

import os, sys, csv, time, random, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

# Auth ZoomInfo
zi_resp = requests.post("https://api.zoominfo.com/authenticate", json={
    "username": os.environ["ZOOMINFO_USERNAME"],
    "password": os.environ["ZOOMINFO_PASSWORD"],
})
zi_jwt = zi_resp.json()["jwt"]
zi_headers = {"Authorization": f"Bearer {zi_jwt}", "Content-Type": "application/json"}

INPUT = "docs/ICP-Prospects/icp1_by_industry/1_Manufacturing.csv"
OUTPUT = "docs/ICP-Prospects/icp1_by_industry/1_Manufacturing_enriched.csv"

# Read companies
with open(INPUT) as f:
    reader = csv.DictReader(f)
    original_headers = reader.fieldnames
    companies = list(reader)

log.info(f"Loaded {len(companies)} manufacturing companies")

# New columns to add
extra_cols = [
    "zi_found", "zi_company_id", "zi_employee_count", "zi_revenue",
    "zi_industry", "zi_city", "zi_state", "zi_has_contacts",
    "zi_cfo_found", "zi_cfo_name", "zi_cfo_title", "zi_cfo_accuracy",
]

results = []
found = 0
has_cfo = 0

for i, co in enumerate(companies):
    name = co.get("company_name", "").strip()
    if not name:
        co.update({k: "" for k in extra_cols})
        results.append(co)
        continue

    # Search ZoomInfo for this company
    try:
        # Company search
        resp = requests.post("https://api.zoominfo.com/search/company", headers=zi_headers, json={
            "companyName": name,
            "state": co.get("state", ""),
            "rpp": 3,
            "page": 1,
        }, timeout=15)
        data = resp.json()
        zi_companies = data.get("data", [])

        if zi_companies:
            zi_co = zi_companies[0]  # Best match
            co["zi_found"] = "Yes"
            co["zi_company_id"] = zi_co.get("id", "")
            co["zi_employee_count"] = zi_co.get("employeeCount", "")
            co["zi_revenue"] = zi_co.get("revenue", "")
            co["zi_industry"] = zi_co.get("industry", "")
            co["zi_city"] = zi_co.get("city", "")
            co["zi_state"] = zi_co.get("state", "")
            found += 1

            # Now search for CFO/Controller at this company
            time.sleep(0.3)
            contact_resp = requests.post("https://api.zoominfo.com/search/contact", headers=zi_headers, json={
                "companyName": name,
                "jobTitle": "CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance",
                "jobFunction": "Finance",
                "rpp": 3,
                "page": 1,
            }, timeout=15)
            contact_data = contact_resp.json()
            contacts = contact_data.get("data", [])

            if contacts:
                best = contacts[0]
                co["zi_has_contacts"] = "Yes"
                co["zi_cfo_found"] = "Yes"
                co["zi_cfo_name"] = f"{best.get('firstName', '')} {best.get('lastName', '')}".strip()
                co["zi_cfo_title"] = best.get("jobTitle", "")
                co["zi_cfo_accuracy"] = best.get("contactAccuracyScore", "")
                has_cfo += 1
            else:
                co["zi_has_contacts"] = "No"
                co["zi_cfo_found"] = "No"
                co["zi_cfo_name"] = ""
                co["zi_cfo_title"] = ""
                co["zi_cfo_accuracy"] = ""
        else:
            co.update({k: "" for k in extra_cols})
            co["zi_found"] = "No"

    except Exception as e:
        log.warning(f"  Error on {name}: {str(e)[:80]}")
        co.update({k: "" for k in extra_cols})
        co["zi_found"] = "Error"

    results.append(co)

    if (i + 1) % 50 == 0:
        log.info(f"  Progress: {i+1}/{len(companies)} | Found: {found} | Has CFO: {has_cfo}")
        # Save progress
        with open(OUTPUT, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=original_headers + extra_cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)

    time.sleep(0.5)  # Rate limit

# Final save
with open(OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=original_headers + extra_cols, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

log.info("")
log.info("=" * 60)
log.info("RESULTS")
log.info("=" * 60)
log.info(f"Total companies: {len(companies)}")
log.info(f"Found in ZoomInfo: {found} ({found/len(companies)*100:.1f}%)")
log.info(f"Has CFO/Controller: {has_cfo} ({has_cfo/len(companies)*100:.1f}%)")
log.info(f"Exported to: {OUTPUT}")
