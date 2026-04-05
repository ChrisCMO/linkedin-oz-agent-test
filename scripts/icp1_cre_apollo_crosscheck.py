"""
Cross-check Commercial Real Estate companies (Google Places CSV)
against Apollo people search to find ICP1 decision-makers.

Search is FREE (0 credits). No enrichment.
"""

import csv
import json
import os
import sys
import time
import random
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.apollo import ApolloClient

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
INPUT_CSV = os.path.join(PROJECT_ROOT, "docs/ICP-Prospects/icp1_by_industry/real-estate/2_Commercial_Real_Estate.csv")
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "output/icp1_cre_apollo_prospects.csv")
OUTPUT_JSON = os.path.join(PROJECT_ROOT, "output/icp1_cre_apollo_prospects.json")

ICP_TITLES = [
    "CFO", "Chief Financial Officer",
    "Controller", "Comptroller",
    "VP Finance", "Vice President Finance",
    "Director of Finance",
    "Owner", "President", "CEO",
    "Managing Partner", "Principal",
]

DOMAINS_PER_BATCH = 10
DELAY = (1.0, 2.5)


def load_domains(csv_path):
    """Load unique domains from CSV."""
    domains = {}
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = row.get("domain", "").strip()
            if d and d not in domains:
                domains[d] = row.get("company_name", "").strip()
    return domains


def main():
    print("ICP1 Commercial Real Estate — Apollo Cross-Check (search only)")
    print("=" * 60)

    domains_map = load_domains(INPUT_CSV)
    domains = list(domains_map.keys())
    print(f"Loaded {len(domains)} unique domains")

    client = ApolloClient()
    health = client.get_health()
    if "error" in health:
        print(f"Apollo error: {health}")
        return
    print("Apollo API: OK\n")

    all_people = []
    total_batches = (len(domains) + DOMAINS_PER_BATCH - 1) // DOMAINS_PER_BATCH

    for i in range(0, len(domains), DOMAINS_PER_BATCH):
        batch_num = i // DOMAINS_PER_BATCH + 1
        batch = domains[i:i + DOMAINS_PER_BATCH]

        body = {
            "page": 1,
            "per_page": 100,
            "person_titles": ICP_TITLES,
            "q_organization_domains_list": batch,
        }
        result = client._request("POST", "/api/v1/mixed_people/api_search", json_body=body)

        if "error" in result:
            print(f"  Batch {batch_num}/{total_batches}: ERROR — {result['error']}")
            if "429" in str(result.get("error", "")):
                print("  Rate limited, waiting 60s...")
                time.sleep(60)
            continue

        people = result.get("people") or []
        total_pool = result.get("pagination", {}).get("total_entries", 0)

        for p in people:
            org = p.get("organization") or {}
            all_people.append({
                "name": p.get("name", ""),
                "title": p.get("title", ""),
                "seniority": p.get("seniority", ""),
                "city": p.get("city", ""),
                "state": p.get("state", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "company_name": org.get("name", ""),
                "company_domain": org.get("primary_domain", ""),
                "company_industry": org.get("industry", ""),
                "company_employees": org.get("estimated_num_employees", ""),
                "company_revenue_range": org.get("annual_revenue_printed", ""),
                "apollo_id": p.get("id", ""),
            })

        if people:
            print(f"  Batch {batch_num}/{total_batches}: {len(people)} people (pool: {total_pool})")
        elif batch_num % 25 == 0 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches}: ... ({len(all_people)} total so far)")

        time.sleep(random.uniform(*DELAY))

    # Dedupe
    seen = set()
    unique = []
    for p in all_people:
        if p["apollo_id"] and p["apollo_id"] not in seen:
            seen.add(p["apollo_id"])
            unique.append(p)

    unique.sort(
        key=lambda x: int(x["company_employees"]) if str(x.get("company_employees", "")).isdigit() else 0,
        reverse=True,
    )

    print(f"\n{'=' * 60}")
    print(f"Total matches: {len(unique)} unique prospects")

    if not unique:
        print("No matches. Most CRE firms may be too small for Apollo coverage.")
        return

    # Save
    os.makedirs(os.path.join(PROJECT_ROOT, "output"), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(unique[0].keys()))
        w.writeheader()
        w.writerows(unique)
    print(f"Saved: {OUTPUT_CSV}")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"generated": datetime.now().isoformat(), "count": len(unique), "prospects": unique}, f, indent=2, default=str)
    print(f"Saved: {OUTPUT_JSON}")

    # Top 20
    print(f"\nTOP PROSPECTS:")
    print("-" * 60)
    for i, p in enumerate(unique[:20], 1):
        print(f"{i}. {p['name']} — {p['title']}")
        print(f"   {p['company_name']} | {p['company_employees']} emp | {p['company_revenue_range'] or 'N/A'} rev")
        print(f"   {p['city']}, {p['state']} | {p['linkedin_url'] or 'no LinkedIn'}")
        print()


if __name__ == "__main__":
    main()
