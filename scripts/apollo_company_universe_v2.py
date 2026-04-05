#!/usr/bin/env python3
"""
Fresh Apollo Company Universe Search — All ICP 1 Industries with Full Pagination.
Uses POST /api/v1/mixed_companies/search (FREE - 0 credits).
Captures Apollo account IDs, org IDs, and all company data.

Output: docs/deliverables/week2/universe/untapped/new apollo universe/
"""

import os, sys, csv, json, time, requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

APOLLO_KEY = os.environ["APOLLO_API_KEY"]
HEADERS = {"X-Api-Key": APOLLO_KEY, "Content-Type": "application/json"}
API_URL = "https://api.apollo.io/api/v1/mixed_companies/search"

BASE = os.path.join(os.path.dirname(__file__), "..")
OUTPUT_DIR = os.path.join(BASE, "docs", "deliverables", "week2", "universe", "untapped", "new apollo universe")

# Seattle metro cities (from icp1_apollo_filter_seattle.py)
SEATTLE_METRO = [
    'seattle', 'bellevue', 'tacoma', 'redmond', 'kirkland', 'everett', 'renton',
    'kent', 'auburn', 'olympia', 'federal way', 'tukwila', 'shoreline', 'bothell',
    'issaquah', 'puyallup', 'lynnwood', 'woodinville', 'kenmore', 'burien', 'seatac',
    'sammamish', 'lakewood', 'sumner', 'bainbridge', 'mountlake terrace', 'mercer island',
    'des moines', 'maple valley', 'snoqualmie', 'duvall', 'snohomish', 'marysville',
    'lake stevens', 'arlington', 'monroe', 'enumclaw', 'bonney lake', 'buckley',
    'steilacoom', 'university place', 'fife', 'edgewood', 'milton', 'pacific', 'algona',
    'normandy park', 'clyde hill', 'medina', 'hunts point', 'yarrow point', 'newcastle',
    'covington', 'north bend', 'granite falls', 'sultan',
]

# ICP 1 Industries and search keywords — expanded for maximum coverage
INDUSTRIES = {
    "Manufacturing": [
        "manufacturing", "machinery", "aerospace", "industrial", "fabrication",
        "food production", "metal", "plastics", "packaging", "chemicals",
        "lumber", "wood products", "paper", "printing", "textiles",
        "electronics manufacturing", "semiconductor", "defense",
    ],
    "Commercial Real Estate": [
        "real estate", "property management", "commercial real estate",
        "real estate investment", "real estate development",
    ],
    "Professional Services": [
        "engineering", "architecture", "consulting", "environmental",
        "accounting", "legal services", "law firm", "design",
        "surveying", "geotechnical", "civil engineering",
    ],
    "Hospitality": [
        "hospitality", "hotels", "hotel management", "resort",
        "restaurant group", "catering", "food service",
    ],
    "Nonprofit": [
        "nonprofit", "non-profit", "foundation", "charity",
        "social services", "community organization",
    ],
    "Construction": [
        "construction", "general contractor", "building",
        "roofing", "plumbing", "electrical contractor", "HVAC",
        "excavation", "paving", "concrete",
    ],
}

# Employee ranges — send ALL at once since API returns same results per range on free tier
EMP_RANGES_ALL = ["11-20", "21-50", "51-100", "101-200", "201-500", "501-1000"]

# Use "Washington State" to avoid D.C. confusion
LOCATIONS = [
    "Seattle, Washington, United States",
    "Washington State, United States",
]

CSV_FIELDS = [
    "apollo_account_id", "apollo_org_id", "icp_industry", "search_keyword",
    "company_name", "domain", "website_url", "linkedin_url", "phone",
    "city", "state", "postal_code", "street_address", "country",
    "revenue", "revenue_printed", "founded_year",
    "sic_codes", "naics_codes",
    "publicly_traded_symbol", "headcount_growth_6m", "headcount_growth_12m",
    "geo_classification",
]


def classify_geo(city, state):
    c = (city or "").lower().strip()
    s = (state or "").lower().strip()
    if not s:
        return "unknown"
    if "washington" in s:
        if any(metro == c for metro in SEATTLE_METRO):
            return "seattle_confirmed"
        return "washington_confirmed"
    if s in ("oregon", "california", "nevada", "hawaii", "alaska"):
        return "west_coast"
    return "national_other"


def extract_company(account, icp_industry, keyword):
    """Extract structured company data from Apollo account object."""
    return {
        "apollo_account_id": account.get("id", ""),
        "apollo_org_id": account.get("organization_id", ""),
        "icp_industry": icp_industry,
        "search_keyword": keyword,
        "company_name": account.get("name", ""),
        "domain": account.get("domain") or account.get("primary_domain", ""),
        "website_url": account.get("website_url", ""),
        "linkedin_url": account.get("linkedin_url", ""),
        "phone": account.get("phone", ""),
        "city": account.get("city", ""),
        "state": account.get("state", ""),
        "postal_code": account.get("postal_code", ""),
        "street_address": account.get("street_address", ""),
        "country": account.get("country", ""),
        "revenue": account.get("organization_revenue", ""),
        "revenue_printed": account.get("organization_revenue_printed", ""),
        "founded_year": account.get("founded_year", ""),
        "sic_codes": ",".join(account.get("sic_codes", []) or []),
        "naics_codes": ",".join(account.get("naics_codes", []) or []),
        "publicly_traded_symbol": account.get("publicly_traded_symbol", ""),
        "headcount_growth_6m": account.get("organization_headcount_six_month_growth", ""),
        "headcount_growth_12m": account.get("organization_headcount_twelve_month_growth", ""),
        "geo_classification": classify_geo(account.get("city"), account.get("state")),
    }


def search_paginated(keyword, emp_ranges, location, max_pages=50):
    """Search Apollo with pagination, return list of account dicts."""
    results = []
    empty_streak = 0
    for page in range(1, max_pages + 1):
        payload = {
            "q_organization_keyword_tags": [keyword],
            "organization_num_employees_ranges": emp_ranges if isinstance(emp_ranges, list) else [emp_ranges],
            "organization_locations": [location],
            "per_page": 100,
            "page": page,
        }

        for attempt in range(3):
            try:
                resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
                if resp.status_code == 429:
                    wait = (attempt + 1) * 10
                    print(f"      Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                accounts = data.get("accounts", [])
                total = data.get("pagination", {}).get("total_entries", 0)

                results.extend(accounts)

                if len(accounts) == 0:
                    empty_streak += 1
                    if empty_streak >= 2:
                        return results, total
                else:
                    empty_streak = 0
                    if page > 1:
                        print(f"      page {page}: +{len(accounts)} (cumulative: {len(results)})")

                break  # success, go to next page
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f"      ERROR: {e}")
                    return results, 0

        time.sleep(0.5)

    return results, 0


def search_by_individual_ranges(keyword, location, max_pages=50):
    """Search each employee range separately to maximize results."""
    all_accounts = []
    total_available = 0
    for emp_range in EMP_RANGES_ALL:
        accounts, total = search_paginated(keyword, [emp_range], location, max_pages=max_pages)
        all_accounts.extend(accounts)
        total_available = max(total_available, total)
    return all_accounts, total_available


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check for checkpoint
    checkpoint_file = os.path.join(OUTPUT_DIR, "checkpoint.json")
    all_companies = {}  # keyed by apollo_org_id

    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            all_companies = json.load(f)
        print(f"Resuming from checkpoint: {len(all_companies)} companies loaded")

    completed_queries = set()
    if all_companies:
        # Track which queries are done based on checkpoint
        for c in all_companies.values():
            key = f"{c['search_keyword']}|{c.get('_emp_range', '')}|{c.get('_location', '')}"
            completed_queries.add(key)

    total_queries = sum(len(kws) * len(LOCATIONS) for kws in INDUSTRIES.values())
    query_num = 0

    for icp_industry, keywords in INDUSTRIES.items():
        print(f"\n{'='*60}")
        print(f"ICP INDUSTRY: {icp_industry}")
        print(f"{'='*60}")

        for keyword in keywords:
            for location in LOCATIONS:
                query_num += 1

                print(f"\n  [{query_num}/{total_queries}] {keyword} | {location}")

                # Search each employee range separately to maximize results
                accounts, total_available = search_by_individual_ranges(keyword, location, max_pages=50)

                new_count = 0
                for acct in accounts:
                    org_id = acct.get("organization_id", acct.get("id", ""))
                    if org_id and org_id not in all_companies:
                        company = extract_company(acct, icp_industry, keyword)
                        company["_location"] = location
                        all_companies[org_id] = company
                        new_count += 1

                print(f"    Found: {len(accounts)} results (total available: {total_available})")
                print(f"    New unique: {new_count} | Running total: {len(all_companies)}")

                # Checkpoint every query
                with open(checkpoint_file, "w") as f:
                    json.dump(all_companies, f)

    # Remove internal tracking fields
    for c in all_companies.values():
        c.pop("_location", None)

    # Classify into buckets
    buckets = {
        "seattle_confirmed": [],
        "washington_confirmed": [],
        "west_coast": [],
        "national_other": [],
        "unknown": [],
    }
    for c in all_companies.values():
        geo = c.get("geo_classification", "unknown")
        if geo in buckets:
            buckets[geo].append(c)
        else:
            buckets["unknown"].append(c)

    # Write CSVs
    print(f"\n{'='*60}")
    print("Writing output files...")

    for bucket_name, rows in buckets.items():
        if not rows:
            continue
        filename = f"{bucket_name}_{len(rows)}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {filename}")

    # Full audit file
    all_file = os.path.join(OUTPUT_DIR, "all_results_full.csv")
    with open(all_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for c in sorted(all_companies.values(), key=lambda x: (x["icp_industry"], x["company_name"])):
            writer.writerow(c)
    print(f"  all_results_full.csv ({len(all_companies)} total)")

    # Summary
    summary_file = os.path.join(OUTPUT_DIR, "summary.txt")
    with open(summary_file, "w") as f:
        f.write("Apollo Company Universe Search v2\n")
        f.write(f"Total unique companies: {len(all_companies)}\n")
        f.write(f"Credits used: 0\n\n")

        f.write("Geographic Distribution:\n")
        for bucket_name, rows in sorted(buckets.items(), key=lambda x: -len(x[1])):
            f.write(f"  {bucket_name}: {len(rows)}\n")

        f.write(f"\nBy ICP Industry:\n")
        ind_counts = {}
        for c in all_companies.values():
            ind = c["icp_industry"]
            ind_counts[ind] = ind_counts.get(ind, 0) + 1
        for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")

        f.write(f"\nSeattle Confirmed by Industry:\n")
        seattle_ind = {}
        for c in buckets["seattle_confirmed"]:
            ind = c["icp_industry"]
            seattle_ind[ind] = seattle_ind.get(ind, 0) + 1
        for ind, count in sorted(seattle_ind.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")

        # Data quality
        has_domain = sum(1 for c in all_companies.values() if c.get("domain"))
        has_linkedin = sum(1 for c in all_companies.values() if c.get("linkedin_url"))
        has_revenue = sum(1 for c in all_companies.values() if c.get("revenue"))
        has_org_id = sum(1 for c in all_companies.values() if c.get("apollo_org_id"))
        f.write(f"\nData Quality:\n")
        f.write(f"  Has apollo_org_id: {has_org_id}\n")
        f.write(f"  Has domain: {has_domain}\n")
        f.write(f"  Has LinkedIn URL: {has_linkedin}\n")
        f.write(f"  Has revenue: {has_revenue}\n")

    print(f"  summary.txt")

    # Console summary
    print(f"\n{'='*60}")
    print(f"DONE! Total unique companies: {len(all_companies)}")
    print(f"Credits used: 0")
    print(f"\nGeographic Distribution:")
    for bucket_name, rows in sorted(buckets.items(), key=lambda x: -len(x[1])):
        if rows:
            print(f"  {bucket_name}: {len(rows)}")
    print(f"\nBy ICP Industry:")
    for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
        print(f"  {ind}: {count}")
    print(f"\nOutput: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
