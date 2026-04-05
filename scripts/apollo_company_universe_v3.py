#!/usr/bin/env python3
"""
Apollo Company Universe Search v3 — Maximum Coverage
Searches every combination of keyword × city × employee range × revenue range
to get different result slices from Apollo's free tier.

Builds on v2 checkpoint data (loads existing, only adds new).
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

# Individual cities to search — each returns different result set
CITIES = [
    "Seattle, Washington, United States",
    "Bellevue, Washington, United States",
    "Tacoma, Washington, United States",
    "Redmond, Washington, United States",
    "Kirkland, Washington, United States",
    "Everett, Washington, United States",
    "Renton, Washington, United States",
    "Kent, Washington, United States",
    "Auburn, Washington, United States",
    "Federal Way, Washington, United States",
    "Bothell, Washington, United States",
    "Issaquah, Washington, United States",
    "Puyallup, Washington, United States",
    "Lynnwood, Washington, United States",
    "Olympia, Washington, United States",
    "Shoreline, Washington, United States",
    "Sammamish, Washington, United States",
    "Woodinville, Washington, United States",
    "Tukwila, Washington, United States",
    "Marysville, Washington, United States",
    # Broader WA catch-all
    "Washington State, United States",
]

INDUSTRIES = {
    "Manufacturing": [
        "manufacturing", "machinery", "aerospace", "industrial", "fabrication",
        "food production", "metal", "plastics", "packaging",
        "electronics manufacturing", "defense",
    ],
    "Commercial Real Estate": [
        "real estate", "property management", "commercial real estate",
        "real estate investment", "real estate development",
    ],
    "Professional Services": [
        "engineering", "architecture", "consulting", "environmental",
        "accounting", "law firm", "design", "surveying",
    ],
    "Hospitality": [
        "hospitality", "hotels", "hotel management", "resort",
        "restaurant group", "catering",
    ],
    "Nonprofit": [
        "nonprofit", "non-profit", "foundation", "charity",
        "social services",
    ],
    "Construction": [
        "construction", "general contractor", "building",
        "roofing", "plumbing", "electrical contractor", "HVAC",
        "excavation", "paving", "concrete",
    ],
}

EMP_RANGES = ["1-10", "11-20", "21-50", "51-100", "101-200", "201-500", "501-1000"]

# Revenue ranges to slice differently
REVENUE_RANGES = [
    None,  # no filter
    "0,1000000",
    "1000000,10000000",
    "10000000,50000000",
    "50000000,200000000",
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


def search_one(keyword, emp_ranges, location, revenue_range=None, max_pages=10):
    """Single search with pagination."""
    results = []
    total = 0
    for page in range(1, max_pages + 1):
        payload = {
            "q_organization_keyword_tags": [keyword],
            "organization_num_employees_ranges": emp_ranges,
            "organization_locations": [location],
            "per_page": 100,
            "page": page,
        }
        if revenue_range:
            payload["organization_revenue_ranges"] = [revenue_range]

        for attempt in range(3):
            try:
                resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
                if resp.status_code == 429:
                    time.sleep((attempt + 1) * 10)
                    continue
                resp.raise_for_status()
                data = resp.json()
                accounts = data.get("accounts", [])
                total = data.get("pagination", {}).get("total_entries", 0)
                results.extend(accounts)
                if len(accounts) == 0:
                    return results, total
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    return results, 0
        time.sleep(0.35)
    return results, total


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    checkpoint_file = os.path.join(OUTPUT_DIR, "checkpoint.json")

    # Load existing from v2
    all_companies = {}
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            all_companies = json.load(f)
        # Clean internal fields
        for c in all_companies.values():
            c.pop("_location", None)
        print(f"Loaded {len(all_companies)} existing companies from checkpoint")

    start_count = len(all_companies)
    query_num = 0

    for icp_industry, keywords in INDUSTRIES.items():
        print(f"\n{'='*60}")
        print(f"ICP INDUSTRY: {icp_industry}")
        print(f"{'='*60}")

        for keyword in keywords:
            # Strategy 1: Search each city individually with all emp ranges
            for city in CITIES:
                query_num += 1
                accounts, total = search_one(keyword, EMP_RANGES, city)

                new = 0
                for acct in accounts:
                    org_id = acct.get("organization_id", acct.get("id", ""))
                    if org_id and org_id not in all_companies:
                        all_companies[org_id] = extract_company(acct, icp_industry, keyword)
                        new += 1

                if new > 0:
                    print(f"  [{query_num}] {keyword} | {city.split(',')[0]:12s} → +{new} new (total: {len(all_companies)})")

            # Strategy 2: Search with revenue ranges (WA State only) to surface different results
            for rev in REVENUE_RANGES:
                if rev is None:
                    continue
                accounts, total = search_one(keyword, EMP_RANGES, "Washington State, United States", revenue_range=rev)

                new = 0
                for acct in accounts:
                    org_id = acct.get("organization_id", acct.get("id", ""))
                    if org_id and org_id not in all_companies:
                        all_companies[org_id] = extract_company(acct, icp_industry, keyword)
                        new += 1

                if new > 0:
                    print(f"  [{query_num}] {keyword} | WA rev:{rev:20s} → +{new} new (total: {len(all_companies)})")

            # Strategy 3: Search each emp range individually (WA State) to get different results per range
            for emp in EMP_RANGES:
                accounts, total = search_one(keyword, [emp], "Washington State, United States")

                new = 0
                for acct in accounts:
                    org_id = acct.get("organization_id", acct.get("id", ""))
                    if org_id and org_id not in all_companies:
                        all_companies[org_id] = extract_company(acct, icp_industry, keyword)
                        new += 1

                if new > 0:
                    print(f"  [{query_num}] {keyword} | WA emp:{emp:10s} → +{new} new (total: {len(all_companies)})")

            # Checkpoint after each keyword
            with open(checkpoint_file, "w") as f:
                json.dump(all_companies, f)

    new_added = len(all_companies) - start_count
    print(f"\n{'='*60}")
    print(f"Search complete. New companies added: {new_added}")
    print(f"Total unique companies: {len(all_companies)}")

    # Write output files
    buckets = {
        "seattle_confirmed": [],
        "washington_confirmed": [],
        "west_coast": [],
        "national_other": [],
        "unknown": [],
    }
    for c in all_companies.values():
        geo = c.get("geo_classification", "unknown")
        buckets.get(geo, buckets["unknown"]).append(c)

    print(f"\nWriting output files...")
    for bucket_name, rows in buckets.items():
        if not rows:
            continue
        filepath = os.path.join(OUTPUT_DIR, f"{bucket_name}_{len(rows)}.csv")
        # Remove old file with different count
        for old in os.listdir(OUTPUT_DIR):
            if old.startswith(bucket_name) and old.endswith(".csv"):
                os.remove(os.path.join(OUTPUT_DIR, old))
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {bucket_name}_{len(rows)}.csv")

    # Full file
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
        f.write(f"Apollo Company Universe Search v3\n")
        f.write(f"Total unique companies: {len(all_companies)}\n")
        f.write(f"New from v3: {new_added}\n")
        f.write(f"Credits used: 0\n\n")
        f.write("Geographic Distribution:\n")
        for b, rows in sorted(buckets.items(), key=lambda x: -len(x[1])):
            f.write(f"  {b}: {len(rows)}\n")
        f.write(f"\nBy ICP Industry:\n")
        ind_counts = {}
        for c in all_companies.values():
            ind = c["icp_industry"]
            ind_counts[ind] = ind_counts.get(ind, 0) + 1
        for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")
        f.write(f"\nSeattle by Industry:\n")
        for c in buckets["seattle_confirmed"]:
            ind = c["icp_industry"]
            ind_counts[ind] = ind_counts.get(ind, 0)
        seattle_ind = {}
        for c in buckets["seattle_confirmed"]:
            ind = c["icp_industry"]
            seattle_ind[ind] = seattle_ind.get(ind, 0) + 1
        for ind, count in sorted(seattle_ind.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")
        has_domain = sum(1 for c in all_companies.values() if c.get("domain"))
        has_linkedin = sum(1 for c in all_companies.values() if c.get("linkedin_url"))
        has_orgid = sum(1 for c in all_companies.values() if c.get("apollo_org_id"))
        f.write(f"\nData Quality:\n")
        f.write(f"  Has apollo_org_id: {has_orgid}\n")
        f.write(f"  Has domain: {has_domain}\n")
        f.write(f"  Has LinkedIn: {has_linkedin}\n")

    print(f"  summary.txt")
    print(f"\n{'='*60}")
    print(f"DONE! Total: {len(all_companies)} | Seattle: {len(buckets['seattle_confirmed'])} | WA: {len(buckets['washington_confirmed'])}")


if __name__ == "__main__":
    main()
