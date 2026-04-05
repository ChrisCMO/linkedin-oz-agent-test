#!/usr/bin/env python3
"""Cross-match ZoomInfo contacts against Apollo free search to find Apollo IDs."""

import os, sys, csv, time, random, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from lib.apollo import ApolloClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

apollo = ApolloClient()

INPUT_CSV = "output/icp1_full_prospect_pool.csv"
OUTPUT_CSV = "output/icp1_full_prospect_pool.csv"  # overwrite with updated data

# Read all rows
with open(INPUT_CSV, "r") as f:
    reader = csv.DictReader(f)
    fields = reader.fieldnames
    rows = list(reader)

zi_rows = [r for r in rows if r["source"] == "ZoomInfo"]
apollo_rows = [r for r in rows if r["source"] == "Apollo"]

log.info(f"Total rows: {len(rows)} (ZoomInfo: {len(zi_rows)}, Apollo: {len(apollo_rows)})")
log.info(f"Cross-matching {len(zi_rows)} ZoomInfo contacts against Apollo...")

matched = 0
not_found = 0
errors = 0

for i, row in enumerate(zi_rows):
    fn = row["first_name"].strip()
    ln = row["last_name"].strip()
    co = row["company"].strip()

    if not fn or not co:
        not_found += 1
        continue

    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_keywords": f"{fn} {ln} {co}",
            "per_page": 3,
            "page": 1,
        })

        people = result.get("people", [])
        found_id = None

        for p in people:
            org_name = (p.get("organization") or {}).get("name", "").lower()
            # Match if company name overlaps (first 8 chars or contains)
            if co.lower()[:8] in org_name or org_name[:8] in co.lower():
                found_id = p.get("id")
                break

        if found_id:
            row["apollo_id"] = found_id
            matched += 1
        else:
            not_found += 1

    except Exception as e:
        errors += 1
        if errors <= 3:
            log.error(f"  Error on {fn} {ln}: {e}")

    # Progress every 100
    if (i + 1) % 100 == 0:
        log.info(f"  Progress: {i+1}/{len(zi_rows)} | Matched: {matched} | Not found: {not_found} | Errors: {errors}")

    # Rate limit: stay under 200/min
    time.sleep(random.uniform(0.3, 0.5))

log.info(f"\nCross-match complete:")
log.info(f"  Matched:   {matched}/{len(zi_rows)} ({matched/len(zi_rows)*100:.1f}%)")
log.info(f"  Not found: {not_found}")
log.info(f"  Errors:    {errors}")

# Write back
with open(OUTPUT_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fields, restval="", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

log.info(f"\nUpdated CSV: {OUTPUT_CSV}")
