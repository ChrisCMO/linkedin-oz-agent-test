"""
Run Batch 1 (Seattle-only) Google X-ray LinkedIn searches via Apify Google Search Scraper.
Outputs one CSV per query + a merged master CSV in docs/TODO/.

Uses Apify actor: apify/google-search-scraper
"""

import os
import re
import csv
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

APIFY_API_KEY = os.getenv("APIFY_API_KEY")
ACTOR_ID = "apify/google-search-scraper"
BASE_URL = "https://api.apify.com/v2"

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "TODO")
INDIVIDUAL_DIR = os.path.join(OUTPUT_DIR, "xray_individual")
MASTER_CSV = os.path.join(OUTPUT_DIR, "xray_batch1_seattle_apify.csv")

# Batch 1 — Seattle only queries
QUERIES = [
    # Manufacturing (ICP Priority 1)
    ('site:linkedin.com/company "manufacturing" "Seattle"', "Manufacturing"),
    ('site:linkedin.com/company "aerospace" "Seattle"', "Aerospace"),
    ('site:linkedin.com/company "industrial" "Seattle"', "Industrial"),
    ('site:linkedin.com/company "fabrication" "Seattle"', "Fabrication"),
    ('site:linkedin.com/company "machining" "Seattle"', "Machining"),
    ('site:linkedin.com/company "precision" "Seattle"', "Precision Manufacturing"),
    # Commercial Real Estate (ICP Priority 2)
    ('site:linkedin.com/company "real estate" "Seattle"', "Real Estate"),
    ('site:linkedin.com/company "commercial real estate" "Seattle"', "Commercial Real Estate"),
    ('site:linkedin.com/company "property management" "Seattle"', "Property Management"),
    ('site:linkedin.com/company "real estate investment" "Seattle"', "Real Estate Investment"),
    # Professional Services (ICP Priority 3)
    ('site:linkedin.com/company "engineering" "Seattle"', "Engineering"),
    ('site:linkedin.com/company "consulting" "Seattle"', "Consulting"),
    ('site:linkedin.com/company "architecture" "Seattle"', "Architecture"),
    ('site:linkedin.com/company "environmental" "Seattle"', "Environmental"),
    ('site:linkedin.com/company "professional services" "Seattle"', "Professional Services"),
    # Hospitality (ICP Priority 4)
    ('site:linkedin.com/company "hotel" "Seattle"', "Hotel"),
    ('site:linkedin.com/company "hospitality" "Seattle"', "Hospitality"),
    ('site:linkedin.com/company "hotel management" "Seattle"', "Hotel Management"),
    ('site:linkedin.com/company "resort" "Seattle"', "Resort"),
    # Nonprofit (ICP Priority 5)
    ('site:linkedin.com/company "nonprofit" "Seattle"', "Nonprofit"),
    ('site:linkedin.com/company "foundation" "Seattle"', "Foundation"),
    ('site:linkedin.com/company "non-profit" "Seattle"', "Non-Profit"),
    ('site:linkedin.com/company "community" "Seattle"', "Community"),
    # Construction (ICP Priority 6)
    ('site:linkedin.com/company "construction" "Seattle"', "Construction"),
    ('site:linkedin.com/company "general contractor" "Seattle"', "General Contractor"),
    # Bonus
    ('site:linkedin.com/company "distribution" "Seattle"', "Distribution"),
    ('site:linkedin.com/company "logistics" "Seattle"', "Logistics"),
    ('site:linkedin.com/company "food processing" "Seattle"', "Food Processing"),
    ('site:linkedin.com/company "ESOP" "Seattle"', "ESOP"),
    ('site:linkedin.com/company "family-owned" "Seattle"', "Family-Owned"),
    ('site:linkedin.com/company "warehouse" "Seattle"', "Warehouse"),
]


def run_search(query: str, max_results: int = 100) -> list:
    """Run a single Google search via Apify and return results."""
    url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    headers = {"Authorization": f"Bearer {APIFY_API_KEY}", "Content-Type": "application/json"}

    payload = {
        "queries": query,
        "maxPagesPerQuery": 5,
        "resultsPerPage": 100,
        "languageCode": "en",
        "countryCode": "us",
        "mobileResults": False,
        "includeUnfilteredResults": False,
    }

    print(f"  Starting Apify run for: {query[:60]}...")
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    run_data = resp.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]

    # Poll for completion
    status_url = f"{BASE_URL}/actor-runs/{run_id}"
    for attempt in range(60):  # max 5 min
        time.sleep(5)
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        status = status_resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"  Run {status} for query: {query[:50]}")
            return []
    else:
        print(f"  Timeout waiting for: {query[:50]}")
        return []

    # Fetch results
    dataset_url = f"{BASE_URL}/datasets/{dataset_id}/items?format=json"
    dataset_resp = requests.get(dataset_url, headers=headers)
    dataset_resp.raise_for_status()
    return dataset_resp.json()


def extract_linkedin_companies(results: list) -> list:
    """Extract company name and LinkedIn URL from Apify Google Search results."""
    companies = []
    for item in results:
        organic = item.get("organicResults", [])
        for r in organic:
            url = r.get("url", "")
            title = r.get("title", "")
            # Only keep linkedin.com/company URLs
            if "linkedin.com/company/" in url:
                # Clean company name from title (remove " | LinkedIn" suffix)
                name = re.sub(r'\s*\|\s*LinkedIn\s*$', '', title).strip()
                name = re.sub(r'\s*-\s*LinkedIn\s*$', '', name).strip()
                # Normalize URL
                url = url.split("?")[0].rstrip("/")
                companies.append((name, url))
    return companies


def slugify(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')


def main():
    os.makedirs(INDIVIDUAL_DIR, exist_ok=True)

    all_companies = {}  # key: linkedin_url -> {name, url, industry}
    total_queries = len(QUERIES)

    for i, (query, industry) in enumerate(QUERIES, 1):
        print(f"\n[{i}/{total_queries}] {industry}: {query}")

        try:
            results = run_search(query)
            companies = extract_linkedin_companies(results)
            print(f"  Found {len(companies)} LinkedIn company pages")

            # Save individual CSV
            slug = slugify(industry)
            individual_file = os.path.join(INDIVIDUAL_DIR, f"seattle_{slug}.csv")
            with open(individual_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["Company Name", "LinkedIn Company URL", "Source"])
                for name, url in companies:
                    writer.writerow([name, url, f"Google X-ray: {industry}"])

            # Add to master (dedup by URL)
            for name, url in companies:
                url_key = url.lower()
                if url_key not in all_companies:
                    all_companies[url_key] = {
                        "company_name": name,
                        "linkedin_url": url,
                        "industry": industry,
                        "city": "Seattle",
                        "source": f"Google X-ray: {industry}",
                    }
                else:
                    # Append industry if found in multiple queries
                    existing = all_companies[url_key]
                    if industry not in existing["industry"]:
                        existing["industry"] += f" | {industry}"

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        # Small delay between queries to be nice to Apify
        if i < total_queries:
            time.sleep(2)

    # Write master CSV
    with open(MASTER_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["company_name", "linkedin_url", "industry", "city", "source"])
        writer.writeheader()
        for c in sorted(all_companies.values(), key=lambda x: x["industry"]):
            writer.writerow(c)

    print(f"\n{'='*60}")
    print(f"DONE! Total unique companies: {len(all_companies)}")
    print(f"Master CSV: {MASTER_CSV}")
    print(f"Individual CSVs: {INDIVIDUAL_DIR}/")

    # Breakdown
    industry_counts = {}
    for c in all_companies.values():
        for ind in c["industry"].split(" | "):
            industry_counts[ind] = industry_counts.get(ind, 0) + 1
    print(f"\nBreakdown:")
    for ind, count in sorted(industry_counts.items(), key=lambda x: -x[1]):
        print(f"  {ind}: {count}")


if __name__ == "__main__":
    main()
