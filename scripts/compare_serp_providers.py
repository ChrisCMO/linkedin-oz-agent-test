#!/usr/bin/env python3
"""Compare Apify SERP vs Serper.dev for X-ray LinkedIn discovery.

Runs the same queries through both providers on 5 companies and compares
speed, result count, and quality of LinkedIn profile URLs found.
"""

import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
load_dotenv(os.path.join(BASE_DIR, ".env"))

from lib.apify import run_actor, SERP_ACTOR
from lib.title_tiers import TIER_1_TITLES

SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SECRET_KEY"]


def get_test_companies(n=5):
    """Get n scored companies with domains for testing."""
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/raw_companies",
        params={
            "select": "id,name,domain,location,icp_score,pipeline_action",
            "tenant_id": "eq.00000000-0000-0000-0000-000000000001",
            "pipeline_status": "eq.scored",
            "domain": "not.is.null",
            "order": "icp_score.desc",
            "limit": n,
        },
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
    )
    return resp.json()


def build_queries(domain: str) -> list[str]:
    """Build X-ray SERP queries for a company domain."""
    return [
        f'site:linkedin.com/in "{domain}" CFO',
        f'site:linkedin.com/in "{domain}" Controller',
        f'site:linkedin.com/in "{domain}" "Director of Finance"',
    ]


def extract_linkedin_urls(results: list[dict], url_key: str = "link") -> list[str]:
    """Extract LinkedIn profile URLs from SERP results."""
    urls = []
    for r in results:
        url = r.get(url_key, "")
        if "linkedin.com/in/" in url:
            urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# Provider 1: Apify SERP
# ---------------------------------------------------------------------------

def search_apify(queries: list[str]) -> dict:
    """Run queries through Apify SERP actor."""
    t0 = time.time()
    results = run_actor(SERP_ACTOR, {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "countryCode": "us",
    })
    elapsed = time.time() - t0

    all_urls = []
    total_results = 0
    for batch in (results or []):
        organic = batch.get("organicResults", [])
        total_results += len(organic)
        all_urls.extend(extract_linkedin_urls(organic, url_key="url"))

    return {
        "provider": "Apify",
        "time_sec": round(elapsed, 1),
        "queries": len(queries),
        "total_results": total_results,
        "linkedin_urls": all_urls,
    }


# ---------------------------------------------------------------------------
# Provider 2: Serper.dev
# ---------------------------------------------------------------------------

def search_serper(queries: list[str]) -> dict:
    """Run queries through Serper.dev API."""
    if not SERPER_API_KEY:
        return {"provider": "Serper", "time_sec": 0, "error": "No API key"}

    t0 = time.time()
    all_urls = []
    total_results = 0

    for q in queries:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": q, "gl": "us", "num": 5},
            headers={"X-API-KEY": SERPER_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            organic = data.get("organic", [])
            total_results += len(organic)
            all_urls.extend(extract_linkedin_urls(organic, url_key="link"))
        else:
            print(f"    Serper error {resp.status_code}: {resp.text[:100]}")

    elapsed = time.time() - t0

    return {
        "provider": "Serper",
        "time_sec": round(elapsed, 1),
        "queries": len(queries),
        "total_results": total_results,
        "linkedin_urls": all_urls,
    }


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------

def main():
    companies = get_test_companies(5)
    print(f"Testing {len(companies)} companies\n")
    print(f"{'Company':<40} {'Provider':<10} {'Time':<8} {'Results':<10} {'LI URLs':<10}")
    print("-" * 90)

    totals = {"Apify": {"time": 0, "results": 0, "urls": 0},
              "Serper": {"time": 0, "results": 0, "urls": 0}}

    for company in companies:
        name = company["name"]
        domain = company["domain"]
        score = company.get("icp_score", "?")
        print(f"\n{name} (score: {score}, domain: {domain})")

        queries = build_queries(domain)

        # Run Serper first (faster)
        serper = search_serper(queries)
        print(f"  {'Serper':<40} {'':10} {serper['time_sec']:<8} {serper['total_results']:<10} {len(serper.get('linkedin_urls', [])):<10}")
        for url in serper.get("linkedin_urls", [])[:3]:
            slug = url.split("/in/")[1].rstrip("/") if "/in/" in url else url
            print(f"    → {slug}")

        # Run Apify
        apify = search_apify(queries)
        print(f"  {'Apify':<40} {'':10} {apify['time_sec']:<8} {apify['total_results']:<10} {len(apify.get('linkedin_urls', [])):<10}")
        for url in apify.get("linkedin_urls", [])[:3]:
            slug = url.split("/in/")[1].rstrip("/") if "/in/" in url else url
            print(f"    → {slug}")

        # Overlap
        serper_set = set(serper.get("linkedin_urls", []))
        apify_set = set(apify.get("linkedin_urls", []))
        overlap = serper_set & apify_set
        only_serper = serper_set - apify_set
        only_apify = apify_set - serper_set
        print(f"  Overlap: {len(overlap)} | Serper-only: {len(only_serper)} | Apify-only: {len(only_apify)}")

        totals["Serper"]["time"] += serper["time_sec"]
        totals["Serper"]["results"] += serper["total_results"]
        totals["Serper"]["urls"] += len(serper.get("linkedin_urls", []))
        totals["Apify"]["time"] += apify["time_sec"]
        totals["Apify"]["results"] += apify["total_results"]
        totals["Apify"]["urls"] += len(apify.get("linkedin_urls", []))

    print(f"\n{'='*90}")
    print(f"\nTOTALS across {len(companies)} companies:")
    print(f"  {'Provider':<10} {'Total Time':<15} {'Total Results':<15} {'LI URLs Found':<15}")
    print(f"  {'Serper':<10} {totals['Serper']['time']:<15.1f} {totals['Serper']['results']:<15} {totals['Serper']['urls']:<15}")
    print(f"  {'Apify':<10} {totals['Apify']['time']:<15.1f} {totals['Apify']['results']:<15} {totals['Apify']['urls']:<15}")

    speedup = totals["Apify"]["time"] / totals["Serper"]["time"] if totals["Serper"]["time"] > 0 else 0
    print(f"\n  Serper is {speedup:.1f}x faster than Apify")


if __name__ == "__main__":
    main()
