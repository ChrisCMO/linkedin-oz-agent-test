#!/usr/bin/env python3
"""
Apollo Deep Pagination — exhaust all pages for each keyword to maximize company count.
Builds on v3 checkpoint. 0 credits.
"""

import os, csv, json, time, requests
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

# High-volume keywords to deep-paginate
SEARCH_MATRIX = [
    # (icp_industry, keyword, location)
    # Manufacturing
    ("Manufacturing", "manufacturing", "Washington State, United States"),
    ("Manufacturing", "manufacturing", "Seattle, Washington, United States"),
    ("Manufacturing", "machinery", "Washington State, United States"),
    ("Manufacturing", "aerospace", "Washington State, United States"),
    ("Manufacturing", "industrial", "Washington State, United States"),
    ("Manufacturing", "industrial", "Seattle, Washington, United States"),
    ("Manufacturing", "fabrication", "Washington State, United States"),
    ("Manufacturing", "food production", "Washington State, United States"),
    ("Manufacturing", "metal", "Washington State, United States"),
    ("Manufacturing", "plastics", "Washington State, United States"),
    ("Manufacturing", "packaging", "Washington State, United States"),
    ("Manufacturing", "electronics manufacturing", "Washington State, United States"),
    ("Manufacturing", "defense", "Washington State, United States"),
    ("Manufacturing", "lumber", "Washington State, United States"),
    ("Manufacturing", "wood products", "Washington State, United States"),
    ("Manufacturing", "printing", "Washington State, United States"),
    # Commercial Real Estate
    ("Commercial Real Estate", "real estate", "Washington State, United States"),
    ("Commercial Real Estate", "real estate", "Seattle, Washington, United States"),
    ("Commercial Real Estate", "property management", "Washington State, United States"),
    ("Commercial Real Estate", "commercial real estate", "Washington State, United States"),
    ("Commercial Real Estate", "real estate investment", "Washington State, United States"),
    ("Commercial Real Estate", "real estate development", "Washington State, United States"),
    # Professional Services
    ("Professional Services", "engineering", "Washington State, United States"),
    ("Professional Services", "engineering", "Seattle, Washington, United States"),
    ("Professional Services", "architecture", "Washington State, United States"),
    ("Professional Services", "architecture", "Seattle, Washington, United States"),
    ("Professional Services", "consulting", "Washington State, United States"),
    ("Professional Services", "consulting", "Seattle, Washington, United States"),
    ("Professional Services", "environmental", "Washington State, United States"),
    ("Professional Services", "environmental", "Seattle, Washington, United States"),
    ("Professional Services", "accounting", "Washington State, United States"),
    ("Professional Services", "law firm", "Washington State, United States"),
    ("Professional Services", "design", "Washington State, United States"),
    ("Professional Services", "surveying", "Washington State, United States"),
    ("Professional Services", "geotechnical", "Washington State, United States"),
    # Hospitality
    ("Hospitality", "hospitality", "Washington State, United States"),
    ("Hospitality", "hotels", "Washington State, United States"),
    ("Hospitality", "hotel management", "Washington State, United States"),
    ("Hospitality", "resort", "Washington State, United States"),
    ("Hospitality", "restaurant group", "Washington State, United States"),
    ("Hospitality", "catering", "Washington State, United States"),
    ("Hospitality", "food service", "Washington State, United States"),
    # Nonprofit
    ("Nonprofit", "nonprofit", "Washington State, United States"),
    ("Nonprofit", "nonprofit", "Seattle, Washington, United States"),
    ("Nonprofit", "non-profit", "Washington State, United States"),
    ("Nonprofit", "foundation", "Washington State, United States"),
    ("Nonprofit", "charity", "Washington State, United States"),
    ("Nonprofit", "social services", "Washington State, United States"),
    # Construction
    ("Construction", "construction", "Washington State, United States"),
    ("Construction", "construction", "Seattle, Washington, United States"),
    ("Construction", "general contractor", "Washington State, United States"),
    ("Construction", "building", "Washington State, United States"),
    ("Construction", "building", "Seattle, Washington, United States"),
    ("Construction", "roofing", "Washington State, United States"),
    ("Construction", "plumbing", "Washington State, United States"),
    ("Construction", "electrical contractor", "Washington State, United States"),
    ("Construction", "HVAC", "Washington State, United States"),
    ("Construction", "excavation", "Washington State, United States"),
    ("Construction", "paving", "Washington State, United States"),
    ("Construction", "concrete", "Washington State, United States"),
]

EMP_RANGES = ["1-10", "11-20", "21-50", "51-100", "101-200", "201-500", "501-1000"]

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
        return "seattle_confirmed" if any(metro == c for metro in SEATTLE_METRO) else "washington_confirmed"
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


def deep_paginate(keyword, location, max_pages=200):
    """Paginate through ALL available pages for a query."""
    all_accounts = []
    empty_count = 0
    for page in range(1, max_pages + 1):
        payload = {
            "q_organization_keyword_tags": [keyword],
            "organization_num_employees_ranges": EMP_RANGES,
            "organization_locations": [location],
            "per_page": 100,
            "page": page,
        }
        for attempt in range(3):
            try:
                resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
                if resp.status_code == 429:
                    time.sleep((attempt + 1) * 10)
                    continue
                resp.raise_for_status()
                accounts = resp.json().get("accounts", [])
                all_accounts.extend(accounts)
                if len(accounts) == 0:
                    empty_count += 1
                    if empty_count >= 3:
                        return all_accounts, page
                else:
                    empty_count = 0
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    return all_accounts, page
        time.sleep(0.35)
    return all_accounts, max_pages


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    checkpoint_file = os.path.join(OUTPUT_DIR, "checkpoint.json")

    all_companies = {}
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            all_companies = json.load(f)
        for c in all_companies.values():
            c.pop("_location", None)
        print(f"Loaded {len(all_companies)} existing companies")

    start_count = len(all_companies)
    total_queries = len(SEARCH_MATRIX)

    for i, (icp_industry, keyword, location) in enumerate(SEARCH_MATRIX, 1):
        loc_short = location.split(",")[0]
        print(f"\n[{i}/{total_queries}] {icp_industry} | {keyword} | {loc_short}")

        accounts, pages_used = deep_paginate(keyword, location)

        new = 0
        for acct in accounts:
            org_id = acct.get("organization_id", acct.get("id", ""))
            if org_id and org_id not in all_companies:
                all_companies[org_id] = extract_company(acct, icp_industry, keyword)
                new += 1

        print(f"  → {len(accounts)} results from {pages_used} pages | +{new} new | total: {len(all_companies)}")

        with open(checkpoint_file, "w") as f:
            json.dump(all_companies, f)

    # Write outputs
    new_added = len(all_companies) - start_count
    print(f"\n{'='*60}")
    print(f"DEEP PAGINATION COMPLETE")
    print(f"New companies added: {new_added}")
    print(f"Total unique: {len(all_companies)}")

    buckets = {"seattle_confirmed": [], "washington_confirmed": [], "west_coast": [], "national_other": [], "unknown": []}
    for c in all_companies.values():
        geo = c.get("geo_classification", "unknown")
        buckets.get(geo, buckets["unknown"]).append(c)

    for bucket_name, rows in buckets.items():
        if not rows:
            continue
        for old in os.listdir(OUTPUT_DIR):
            if old.startswith(bucket_name) and old.endswith(".csv"):
                os.remove(os.path.join(OUTPUT_DIR, old))
        filepath = os.path.join(OUTPUT_DIR, f"{bucket_name}_{len(rows)}.csv")
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {bucket_name}_{len(rows)}.csv")

    all_file = os.path.join(OUTPUT_DIR, "all_results_full.csv")
    with open(all_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for c in sorted(all_companies.values(), key=lambda x: (x["icp_industry"], x["company_name"])):
            writer.writerow(c)
    print(f"  all_results_full.csv ({len(all_companies)})")

    # Summary
    ind_counts = {}
    seattle_ind = {}
    for c in all_companies.values():
        ind = c["icp_industry"]
        ind_counts[ind] = ind_counts.get(ind, 0) + 1
    for c in buckets["seattle_confirmed"]:
        ind = c["icp_industry"]
        seattle_ind[ind] = seattle_ind.get(ind, 0) + 1

    summary_file = os.path.join(OUTPUT_DIR, "summary.txt")
    with open(summary_file, "w") as f:
        f.write(f"Apollo Company Universe — Deep Pagination\n")
        f.write(f"Total unique companies: {len(all_companies)}\n")
        f.write(f"Credits used: 0\n\n")
        f.write("Geographic Distribution:\n")
        for b, rows in sorted(buckets.items(), key=lambda x: -len(x[1])):
            f.write(f"  {b}: {len(rows)}\n")
        f.write(f"\nAll Companies by Industry:\n")
        for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")
        f.write(f"\nSeattle by Industry:\n")
        for ind, count in sorted(seattle_ind.items(), key=lambda x: -x[1]):
            f.write(f"  {ind}: {count}\n")
        has_orgid = sum(1 for c in all_companies.values() if c.get("apollo_org_id"))
        has_domain = sum(1 for c in all_companies.values() if c.get("domain"))
        has_linkedin = sum(1 for c in all_companies.values() if c.get("linkedin_url"))
        f.write(f"\nData Quality:\n  Has org_id: {has_orgid}\n  Has domain: {has_domain}\n  Has LinkedIn: {has_linkedin}\n")

    print(f"\n{'='*60}")
    print(f"DONE! Total: {len(all_companies)} | Seattle: {len(buckets['seattle_confirmed'])} | WA: {len(buckets['washington_confirmed'])}")
    print(f"By Industry:")
    for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
        print(f"  {ind}: {count}")


if __name__ == "__main__":
    main()
