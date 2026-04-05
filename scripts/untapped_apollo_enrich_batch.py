#!/usr/bin/env python3
"""
Enrich untapped Apollo companies via free org search, classify by geography.
Phase 1: Free org search (0 credits) → get city/state/domain/employees
Phase 2 (optional): Paid org enrichment (1 credit) → revenue/ownership (Seattle/WA only)

Usage:
  python scripts/untapped_apollo_enrich_batch.py --start 0 --count 50
  python scripts/untapped_apollo_enrich_batch.py --start 0 --count 500 --skip-paid
"""

import os, sys, csv, json, time, argparse, re, difflib, requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
APOLLO_BASE = "https://api.apollo.io"
HEADERS = {"X-Api-Key": APOLLO_API_KEY, "Content-Type": "application/json"}

BASE = os.path.join(os.path.dirname(__file__), "..")
INPUT_FILE = os.path.join(BASE, "docs", "deliverables", "week2", "universe", "untapped", "apollo_raw_3944.csv")
OUTPUT_DIR = os.path.join(BASE, "docs", "deliverables", "week2", "universe", "untapped", "apollo")

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

WEST_COAST_STATES = ['oregon', 'california', 'nevada', 'hawaii', 'alaska']
EXCLUDED_INDUSTRIES = ['banking', 'financial services', 'government', 'government administration',
                       'government relations', 'political organization', 'public policy']


def normalize_name(name):
    """Normalize company name for fuzzy matching."""
    n = name.lower().strip()
    n = re.sub(r'\b(llc|inc|corp|corporation|co|ltd|company|group|the)\b', '', n)
    n = re.sub(r'[.,\-\'\"&]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def fuzzy_match(name1, name2):
    """Return similarity ratio 0-1."""
    return difflib.SequenceMatcher(None, normalize_name(name1), normalize_name(name2)).ratio()


def classify_geo(city, state):
    """Classify geographic location."""
    if not city and not state:
        return "not_found"
    c = (city or "").lower().strip()
    s = (state or "").lower().strip()
    if any(metro in c for metro in SEATTLE_METRO) and 'washington' in s:
        return "seattle_confirmed"
    if 'washington' in s:
        return "washington_confirmed"
    if any(st in s for st in WEST_COAST_STATES):
        return "west_coast_confirmed"
    if s:
        return "national_other"
    return "not_found"


def search_org(company_name, icp_industry, retries=3):
    """Search Apollo for a company by name (FREE - 0 credits)."""
    url = f"{APOLLO_BASE}/api/v1/mixed_companies/search"
    payload = {"q_organization_name": company_name, "per_page": 5, "page": 1}

    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code == 429:
                wait = (attempt + 1) * 5
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            orgs = data.get("organizations", [])
            if not orgs:
                return None, "none", {}

            # Find best match
            best_org = None
            best_score = 0
            best_confidence = "none"

            for org in orgs:
                org_name = org.get("name", "")
                score = fuzzy_match(company_name, org_name)

                # Boost if industry aligns
                org_industry = (org.get("industry", "") or "").lower()
                icp_lower = (icp_industry or "").lower()
                if icp_lower and icp_lower in org_industry or org_industry in icp_lower:
                    score += 0.1

                if score > best_score:
                    best_score = score
                    best_org = org

            if best_org:
                if best_score >= 0.8:
                    best_confidence = "high" if best_score < 1.0 else "exact"
                elif best_score >= 0.5:
                    best_confidence = "low"
                else:
                    best_confidence = "none"
                    best_org = None

            return best_org, best_confidence, {}

        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"    ERROR: {e}")
                return None, "error", {}

    return None, "error", {}


def enrich_org(domain, retries=3):
    """Paid org enrichment by domain (1 credit)."""
    url = f"{APOLLO_BASE}/api/v1/organizations/enrich"
    payload = {"domain": domain}

    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            if resp.status_code == 429:
                time.sleep((attempt + 1) * 5)
                continue
            resp.raise_for_status()
            return resp.json().get("organization", {})
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                print(f"    Enrich ERROR: {e}")
                return {}
    return {}


def extract_domain(url):
    """Extract domain from website URL."""
    if not url:
        return ""
    url = url.lower().replace("http://", "").replace("https://", "").replace("www.", "")
    return url.split("/")[0].strip()


def main():
    parser = argparse.ArgumentParser(description="Enrich untapped Apollo companies")
    parser.add_argument("--start", type=int, default=0, help="Start row (0-indexed)")
    parser.add_argument("--count", type=int, default=50, help="Number of companies to process")
    parser.add_argument("--skip-paid", action="store_true", help="Skip paid enrichment (free search only)")
    parser.add_argument("--input", default=INPUT_FILE, help="Input CSV file")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Load input
    with open(args.input) as f:
        all_rows = list(csv.DictReader(f))

    batch = all_rows[args.start : args.start + args.count]
    print(f"Processing {len(batch)} companies (rows {args.start}-{args.start + len(batch) - 1})")
    print(f"Output: {args.output_dir}")
    print()

    # Process each company
    results = []
    for i, row in enumerate(batch):
        company_name = (row.get("company_name") or "").strip()
        icp_industry = (row.get("icp_industry") or "").strip()
        industry_tag = (row.get("industry_tag") or "").strip()
        contact_name = (row.get("contact_name") or "").strip()
        contact_title = (row.get("contact_title") or "").strip()

        print(f"[{i+1}/{len(batch)}] {company_name} ({icp_industry})")

        # Phase 1: Free org search
        org, confidence, _ = search_org(company_name, icp_industry)

        result = {
            "icp_industry": icp_industry,
            "company_name": company_name,
            "industry_tag": industry_tag,
            "apollo_matched_name": "",
            "match_confidence": confidence,
            "apollo_industry": "",
            "employees": "",
            "domain": "",
            "linkedin_url": "",
            "city": "",
            "state": "",
            "contact_name": contact_name,
            "contact_title": contact_title,
            "geo_classification": "not_found",
            "industry_match": "",
            "size_fit": "",
            "excluded_industry": "",
            "has_domain": "no",
            # Paid enrichment fields
            "annual_revenue": "",
            "ownership": "",
            "finance_dept_size": "",
            "founded_year": "",
        }

        if org and confidence in ("exact", "high"):
            city = (org.get("city") or "").strip()
            state = (org.get("state") or "").strip()
            apollo_industry = (org.get("industry") or "").strip()
            employees = org.get("estimated_num_employees") or ""
            website = (org.get("website_url") or "").strip()
            domain = extract_domain(website)
            linkedin = (org.get("linkedin_url") or "").strip()

            geo = classify_geo(city, state)

            # Validation flags
            icp_lower = icp_industry.lower()
            ind_lower = apollo_industry.lower()
            industry_match = "yes" if (icp_lower in ind_lower or ind_lower in icp_lower) else "partial"

            emp_num = int(employees) if str(employees).isdigit() else 0
            size_fit = "yes" if 11 <= emp_num <= 1000 else ("unknown" if emp_num == 0 else "no")

            is_excluded = "yes" if ind_lower in EXCLUDED_INDUSTRIES else "no"

            result.update({
                "apollo_matched_name": org.get("name", ""),
                "apollo_industry": apollo_industry,
                "employees": str(employees),
                "domain": domain,
                "linkedin_url": linkedin,
                "city": city,
                "state": state,
                "geo_classification": geo,
                "industry_match": industry_match,
                "size_fit": size_fit,
                "excluded_industry": is_excluded,
                "has_domain": "yes" if domain else "no",
            })

            print(f"    → {org.get('name','')} | {city}, {state} | {employees} emp | {geo}")

            # Phase 2: Paid enrichment for Seattle/WA only
            if not args.skip_paid and geo in ("seattle_confirmed", "washington_confirmed") and domain and is_excluded == "no":
                print(f"    → Enriching {domain} (1 credit)...")
                enriched = enrich_org(domain)
                if enriched:
                    revenue = enriched.get("annual_revenue")
                    result["annual_revenue"] = str(revenue) if revenue else ""
                    result["ownership"] = enriched.get("owned_by_organization", {}).get("name", "") if enriched.get("owned_by_organization") else "independent"
                    dept = enriched.get("departmental_head_count", {}) or {}
                    result["finance_dept_size"] = str(dept.get("finance", "") or dept.get("accounting", ""))
                    result["founded_year"] = str(enriched.get("founded_year", ""))
                time.sleep(0.5)
        else:
            print(f"    → No match (confidence: {confidence})")

        results.append(result)
        time.sleep(0.35)  # Rate limit buffer

    # Write output files
    print(f"\n{'='*60}")
    print("Writing output files...")

    fieldnames = list(results[0].keys()) if results else []

    # Classify into buckets
    buckets = {
        "seattle_confirmed": [],
        "washington_confirmed": [],
        "west_coast_confirmed": [],
        "national_other": [],
        "not_found": [],
    }
    for r in results:
        geo = r["geo_classification"]
        if geo in buckets:
            buckets[geo].append(r)
        else:
            buckets["not_found"].append(r)

    # Write each bucket
    for bucket_name, rows in buckets.items():
        if not rows:
            continue
        filename = f"{bucket_name}_{len(rows)}.csv"
        filepath = os.path.join(args.output_dir, filename)
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {filename}: {len(rows)} companies")

    # Write full enrichment log
    log_file = os.path.join(args.output_dir, "enrichment_log.csv")
    with open(log_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"  enrichment_log.csv: {len(results)} companies (full audit)")

    # Write summary
    summary_file = os.path.join(args.output_dir, "batch_summary.txt")
    with open(summary_file, "w") as f:
        f.write(f"Batch: rows {args.start}-{args.start + len(batch) - 1}\n")
        f.write(f"Total processed: {len(results)}\n")
        f.write(f"Paid enrichment: {'skipped' if args.skip_paid else 'enabled'}\n\n")
        f.write("Geographic Distribution:\n")
        for bucket_name, rows in buckets.items():
            f.write(f"  {bucket_name}: {len(rows)}\n")
        f.write(f"\nMatch Quality:\n")
        for conf in ["exact", "high", "low", "none", "error"]:
            count = sum(1 for r in results if r["match_confidence"] == conf)
            if count:
                f.write(f"  {conf}: {count}\n")
        # Industry breakdown for Seattle
        seattle_rows = buckets["seattle_confirmed"]
        if seattle_rows:
            f.write(f"\nSeattle Companies by ICP Industry:\n")
            ind_counts = {}
            for r in seattle_rows:
                ind = r["icp_industry"]
                ind_counts[ind] = ind_counts.get(ind, 0) + 1
            for ind, count in sorted(ind_counts.items(), key=lambda x: -x[1]):
                f.write(f"  {ind}: {count}\n")

    print(f"  batch_summary.txt")
    print(f"\nDONE!")

    # Print summary to console
    print(f"\n--- SUMMARY ---")
    for bucket_name, rows in buckets.items():
        print(f"  {bucket_name}: {len(rows)}")
    credits_used = sum(1 for r in results if r["annual_revenue"] or r["founded_year"])
    print(f"  Credits used: {credits_used}")


if __name__ == "__main__":
    main()
