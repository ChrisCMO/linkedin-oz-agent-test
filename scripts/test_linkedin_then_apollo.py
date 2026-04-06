#!/usr/bin/env python3
"""Test: LinkedIn company scrape → Apollo org enrichment → compare data.

LinkedIn is primary (Chad says Apollo data is unreliable).
Apollo adds revenue + ownership as supplementary data.

Usage:
    python3 -m scripts.test_linkedin_then_apollo --file /path/to/csv
"""
import argparse
import csv
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import requests

APIFY_TOKEN = os.environ["APIFY_API_KEY"]
APIFY_HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
COMPANY_SCRAPER = "UwSdACBp7ymaGUJjS"  # LinkedIn company page scraper

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")


def run_actor(actor_id, payload, max_wait=120):
    """Run an Apify actor and return results."""
    r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs",
                      headers=APIFY_HEADERS, json=payload, timeout=30)
    if r.status_code != 201:
        print(f"  Actor start failed: {r.status_code} {r.text[:200]}")
        return []
    run_id = r.json()["data"]["id"]
    ds = r.json()["data"]["defaultDatasetId"]
    for _ in range(max_wait // 5):
        time.sleep(5)
        status = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}",
                              headers=APIFY_HEADERS, timeout=15).json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED"):
            break
    try:
        return requests.get(f"https://api.apify.com/v2/datasets/{ds}/items",
                            headers=APIFY_HEADERS, timeout=15).json()
    except Exception:
        return []


def apollo_org_enrich(domain):
    """Apollo org enrichment — supplementary data (revenue, ownership)."""
    if not APOLLO_API_KEY or not domain:
        return None
    try:
        resp = requests.post(
            "https://api.apollo.io/api/v1/organizations/enrich",
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
            json={"api_key": APOLLO_API_KEY, "domain": domain},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("organization", {})
    except Exception as e:
        print(f"  Apollo error: {e}")
    return None


def extract_domain_from_website(website):
    """Extract domain from website URL."""
    if not website:
        return None
    domain = website.replace("https://", "").replace("http://", "").replace("www.", "")
    return domain.split("/")[0].strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="CSV with company_name, linkedin_url columns")
    args = parser.parse_args()

    with open(args.file) as f:
        companies = list(csv.DictReader(f))
    print(f"Loaded {len(companies)} companies\n")

    # Step 1: Scrape LinkedIn company pages (batch)
    linkedin_urls = [c["linkedin_url"] for c in companies if c.get("linkedin_url")]
    print(f"=== Step 1: LinkedIn Company Page Scrape ({len(linkedin_urls)} URLs) ===")
    li_results = run_actor(COMPANY_SCRAPER, {"companies": linkedin_urls})
    print(f"  Got {len(li_results)} results\n")

    # Index by URL
    li_by_url = {}
    for item in li_results:
        url = item.get("linkedinUrl", item.get("url", ""))
        if url:
            li_by_url[url.rstrip("/")] = item

    # Step 2: For each company, match LinkedIn data + Apollo enrichment
    results = []
    for c in companies:
        name = c["company_name"]
        li_url = c.get("linkedin_url", "").rstrip("/")
        print(f"--- {name} ---")

        # LinkedIn data (PRIMARY source)
        li = li_by_url.get(li_url, {})
        website = li.get("website") or li.get("websiteUrl") or ""
        domain = extract_domain_from_website(website)

        li_data = {
            "company_name": name,
            "linkedin_url": li_url,
            "li_employees": li.get("employeeCount", ""),
            "li_followers": li.get("followerCount", ""),
            "li_tagline": li.get("tagline", ""),
            "li_description": (li.get("description") or "")[:300],
            "li_founded": (li.get("foundedOn") or {}).get("year", ""),
            "li_industry": li.get("industry", ""),
            "li_specialties": ", ".join(li.get("specialities", []) or []),
            "li_headquarters": "",
            "li_has_logo": "Yes" if li.get("logo") else "No",
            "website": website,
            "domain": domain,
        }

        # Extract headquarters
        hq = li.get("headquarter") or {}
        if hq:
            parts = [hq.get("city", ""), hq.get("geographicArea", "")]
            li_data["li_headquarters"] = ", ".join(p for p in parts if p)

        print(f"  LinkedIn: {li_data['li_employees']} employees, {li_data['li_followers']} followers, HQ: {li_data['li_headquarters']}")
        print(f"  Website: {website}  Domain: {domain}")

        # Apollo enrichment (SUPPLEMENTARY — only if we have a domain)
        apollo_data = {}
        if domain:
            print(f"  Apollo enriching {domain}...")
            apollo = apollo_org_enrich(domain)
            if apollo:
                apollo_data = {
                    "apollo_employees": apollo.get("estimated_num_employees", ""),
                    "apollo_revenue": apollo.get("annual_revenue_printed", ""),
                    "apollo_annual_revenue": apollo.get("annual_revenue", ""),
                    "apollo_industry": apollo.get("industry", ""),
                    "apollo_ownership": apollo.get("ownership_type", ""),
                    "apollo_founded": apollo.get("founded_year", ""),
                    "apollo_city": apollo.get("city", ""),
                    "apollo_state": apollo.get("state", ""),
                    "apollo_description": (apollo.get("short_description") or "")[:200],
                }
                print(f"  Apollo: {apollo_data['apollo_employees']} employees, {apollo_data['apollo_revenue']} revenue, {apollo_data['apollo_ownership']} ownership")
            else:
                print(f"  Apollo: no data returned")
            time.sleep(1)
        else:
            print(f"  Apollo: skipped (no domain from LinkedIn)")

        # Merge — LinkedIn is primary, Apollo supplements
        merged = {**li_data, **apollo_data}
        merged["source_csv_industry"] = c.get("industry", "")
        merged["source_csv_city"] = c.get("city", "")
        results.append(merged)
        print()

    # Output
    print("\n=== COMPARISON TABLE ===\n")
    for r in results:
        print(f"Company: {r['company_name']}")
        print(f"  LinkedIn URL:     {r['linkedin_url']}")
        print(f"  Website/Domain:   {r.get('website', '--')} / {r.get('domain', '--')}")
        print(f"  Headquarters:     {r.get('li_headquarters', '--')}")
        print(f"  Industry:         LI={r.get('li_industry', '--')}  Apollo={r.get('apollo_industry', '--')}  CSV={r.get('source_csv_industry', '--')}")
        print(f"  Employees:        LI={r.get('li_employees', '--')}  Apollo={r.get('apollo_employees', '--')}")
        print(f"  Revenue:          Apollo={r.get('apollo_revenue', '--')}")
        print(f"  Ownership:        Apollo={r.get('apollo_ownership', '--')}")
        print(f"  Founded:          LI={r.get('li_founded', '--')}  Apollo={r.get('apollo_founded', '--')}")
        print(f"  Followers:        {r.get('li_followers', '--')}")
        print(f"  Tagline:          {r.get('li_tagline', '--')}")
        print(f"  Description:      {(r.get('li_description') or '--')[:100]}...")
        print(f"  Specialties:      {r.get('li_specialties', '--')}")
        print()

    # Save to CSV
    out_file = args.file.replace(".csv", "_enriched.csv")
    if results:
        fieldnames = list(results[0].keys())
        with open(out_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"Saved to {out_file}")


if __name__ == "__main__":
    main()
