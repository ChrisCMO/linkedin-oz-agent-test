#!/usr/bin/env python3
"""Cross-match manufacturing companies against Apollo free search to find contacts."""

import os, sys, csv, time, random, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from lib.apollo import ApolloClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

apollo = ApolloClient()

INPUT = "docs/ICP-Prospects/icp1_by_industry/manufacturing/1_Manufacturing.csv"
OUTPUT = "docs/ICP-Prospects/icp1_by_industry/manufacturing/1_Manufacturing_apollo.csv"

# Read companies
with open(INPUT) as f:
    reader = csv.DictReader(f)
    original_headers = reader.fieldnames
    companies = list(reader)

log.info(f"Loaded {len(companies)} manufacturing companies")

# New columns
extra_cols = [
    "apollo_org_found", "apollo_org_id",
    "apollo_contact_found", "apollo_contact_id", "apollo_contact_name",
    "apollo_contact_title", "apollo_contact_seniority",
    "apollo_has_email", "apollo_has_phone",
]

TITLES = ["CFO", "Chief Financial Officer", "Controller", "Comptroller",
          "VP Finance", "Vice President Finance", "Director of Finance",
          "Owner", "President", "CEO"]

results = []
org_found = 0
contact_found = 0

for i, co in enumerate(companies):
    name = co.get("company_name", "").strip()
    domain = co.get("domain", "").strip()

    if not name:
        co.update({k: "" for k in extra_cols})
        results.append(co)
        continue

    try:
        # Method 1: Search by domain (more precise) if available
        if domain:
            result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
                "q_organization_domains_list": [domain],
                "person_titles": TITLES,
                "person_seniorities": ["c_suite", "vp", "director", "owner"],
                "per_page": 3,
                "page": 1,
            })
        else:
            # Method 2: Search by company name keyword
            result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
                "q_keywords": name,
                "person_titles": TITLES,
                "per_page": 3,
                "page": 1,
            })

        people = result.get("people", [])

        if people:
            # Check if any result actually matches the company
            best = None
            for p in people:
                org_name = (p.get("organization") or {}).get("name", "").lower()
                # Fuzzy match — first 8 chars of company name or domain match
                if (name.lower()[:8] in org_name or org_name[:8] in name.lower() or
                    (domain and domain.lower() in (p.get("organization") or {}).get("primary_domain", "").lower())):
                    best = p
                    break

            if not best and people:
                # Take first result if domain matched
                if domain:
                    best = people[0]

            if best:
                org = best.get("organization") or {}
                co["apollo_org_found"] = "Yes"
                co["apollo_org_id"] = org.get("id", "")
                co["apollo_contact_found"] = "Yes"
                co["apollo_contact_id"] = best.get("id", "")
                co["apollo_contact_name"] = f"{best.get('first_name', '')} {best.get('last_name_obfuscated', '')}".strip()
                co["apollo_contact_title"] = best.get("title", "")
                co["apollo_contact_seniority"] = best.get("seniority", "")
                co["apollo_has_email"] = str(best.get("has_email", False))
                co["apollo_has_phone"] = str(best.get("has_direct_phone", ""))
                org_found += 1
                contact_found += 1
            else:
                co["apollo_org_found"] = "Partial"
                co["apollo_org_id"] = ""
                co["apollo_contact_found"] = "No match"
                co.update({k: "" for k in extra_cols if k not in ("apollo_org_found", "apollo_contact_found")})
                org_found += 1
        else:
            co.update({k: "" for k in extra_cols})
            co["apollo_org_found"] = "No"
            co["apollo_contact_found"] = "No"

    except Exception as e:
        log.warning(f"  Error on {name}: {str(e)[:80]}")
        co.update({k: "" for k in extra_cols})
        co["apollo_org_found"] = "Error"
        co["apollo_contact_found"] = "Error"

    results.append(co)

    if (i + 1) % 50 == 0:
        log.info(f"  Progress: {i+1}/{len(companies)} | Orgs found: {org_found} | Contacts: {contact_found}")
        # Save progress
        os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
        with open(OUTPUT, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=original_headers + extra_cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)

    time.sleep(0.4)

# Final save
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
with open(OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=original_headers + extra_cols, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

log.info("")
log.info("=" * 60)
log.info("RESULTS")
log.info("=" * 60)
log.info(f"Total companies: {len(companies)}")
log.info(f"Found in Apollo: {org_found} ({org_found/len(companies)*100:.1f}%)")
log.info(f"Has CFO/Controller contact: {contact_found} ({contact_found/len(companies)*100:.1f}%)")
log.info(f"Exported to: {OUTPUT}")
log.info(f"Credits used: 0 (free search only)")
