#!/usr/bin/env python3
"""
Discover LinkedIn profiles for ZoomInfo-only contacts via Unipile LinkedIn search.

For each contact without a LinkedIn URL, searches LinkedIn by name + company,
then fetches full profile data via Unipile GET /users/{slug}?linkedin_sections=*.

Updates the pipeline Excel with discovered LinkedIn URLs and profile data.
"""

import sys, os, time, json, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
import logging
import csv
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

BASE_URL = os.environ["UNIPILE_BASE_URL"]
API_KEY = os.environ["UNIPILE_API_KEY"]
ACCOUNT_ID = os.environ["UNIPILE_ACCOUNT_ID_LAIKAH"]
HEADERS = {"X-API-KEY": API_KEY, "accept": "application/json"}

# ZoomInfo-only contacts to find on LinkedIn
CONTACTS = [
    # Formost Fuji
    {"first_name": "Daniel", "last_name": "Semanskee", "title": "Chief Financial Officer & Board Member", "company": "Formost Fuji Corporation"},
    {"first_name": "Dennis", "last_name": "Gunnell", "title": "President", "company": "Formost Fuji Corporation"},
    # Shannon & Wilson
    {"first_name": "Riitta", "last_name": "O'Grady", "title": "Assistant Controller", "company": "Shannon & Wilson"},
    # Skills Inc.
    {"first_name": "Stacy", "last_name": "Garnett", "title": "Chief Financial Officer & President", "company": "Skills Inc."},
    {"first_name": "Christopher", "last_name": "Kuczek", "title": "Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Kathy", "last_name": "Frey", "title": "Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Mary", "last_name": "Lamanna", "title": "Chief Financial Officer, Gov Poc", "company": "Skills Inc."},
    {"first_name": "Aleks", "last_name": "Mousaian", "title": "Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Barry", "last_name": "Wilson", "title": "Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Tommi", "last_name": "Holloway", "title": "Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Sanjay", "last_name": "Amdekar", "title": "Global Chief Financial Officer", "company": "Skills Inc."},
    {"first_name": "Laura", "last_name": "Skillings", "title": "Chief Financial Officer & Principal", "company": "Skills Inc."},
]


def search_linkedin(keywords, retries=2):
    """Search LinkedIn via Unipile with retry on 503."""
    url = f"{BASE_URL}/api/v1/linkedin/search?account_id={ACCOUNT_ID}"
    body = {
        "api": "classic",
        "category": "people",
        "keywords": keywords,
    }
    for attempt in range(retries + 1):
        try:
            r = requests.post(url, headers={**HEADERS, "Content-Type": "application/json"}, json=body, timeout=30)
            if r.status_code == 503 and attempt < retries:
                log.warning(f"  503 session error, retrying in 30s... (attempt {attempt + 1})")
                time.sleep(30)
                continue
            if r.status_code != 200:
                log.warning(f"  Search failed: {r.status_code} {r.text[:200]}")
                return []
            data = r.json()
            return data.get("items", [])
        except Exception as e:
            log.warning(f"  Search error: {e}")
            if attempt < retries:
                time.sleep(15)
                continue
            return []
    return []


def get_profile(identifier):
    """Get full LinkedIn profile via Unipile."""
    url = f"{BASE_URL}/api/v1/users/{identifier}?account_id={ACCOUNT_ID}&linkedin_sections=*"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            log.warning(f"  Profile fetch failed: {r.status_code}")
            return None
        return r.json()
    except Exception as e:
        log.warning(f"  Profile error: {e}")
        return None


def name_matches(search_first, search_last, result_name):
    """Check if a search result name matches the contact.

    Flexible: last name must match (or near-match), first name can be
    a nickname (Dan/Daniel, Chris/Christopher, etc.)
    """
    result_lower = result_name.lower()
    search_first_l = search_first.lower()
    search_last_l = search_last.lower()

    # Last name must be present (primary match criterion)
    last_ok = search_last_l in result_lower
    if not last_ok:
        # Try first 5 chars of last name for partial matches
        last_ok = search_last_l[:5] in result_lower if len(search_last_l) >= 5 else False

    if not last_ok:
        return False

    # First name: flexible — check if either is a prefix of the other
    # Handles Dan/Daniel, Chris/Christopher, etc.
    result_parts = result_lower.split()
    if result_parts:
        result_first = result_parts[0]
        first_ok = (
            search_first_l.startswith(result_first) or
            result_first.startswith(search_first_l) or
            search_first_l[:3] == result_first[:3]  # first 3 chars match
        )
    else:
        first_ok = search_first_l in result_lower

    return first_ok and last_ok


# ── Main ──
results = []
not_found = []

for contact in CONTACTS:
    name = f"{contact['first_name']} {contact['last_name']}"
    company = contact["company"]
    log.info(f"\n{'─' * 50}")
    log.info(f"Searching: {name} @ {company}")
    log.info(f"{'─' * 50}")

    # Search strategy: name + company
    keywords = f"{name} {company}"
    items = search_linkedin(keywords)
    log.info(f"  Results: {len(items)}")

    match = None
    for item in items:
        item_name = item.get("name", "")
        item_headline = item.get("headline", "")
        item_provider_id = item.get("provider_id", "")
        item_slug = item.get("public_identifier", "") or item.get("slug", "")
        item_network = item.get("network_distance", "")

        log.info(f"  Candidate: {item_name} | {item_headline[:60]} | {item_network}")

        if name_matches(contact["first_name"], contact["last_name"], item_name):
            match = item
            log.info(f"  ✓ NAME MATCH: {item_name}")
            break

    if not match:
        # Fallback: try just the name without company (extra delay first)
        log.info(f"  No name match in results. Waiting before fallback search...")
        time.sleep(random.uniform(30, 45))
        log.info(f"  Trying name-only search...")
        items2 = search_linkedin(name)
        for item in items2:
            item_name = item.get("name", "")
            if name_matches(contact["first_name"], contact["last_name"], item_name):
                match = item
                log.info(f"  ✓ NAME MATCH (name-only): {item_name}")
                break

    if not match:
        log.info(f"  ✗ NOT FOUND on LinkedIn")
        not_found.append(contact)
        results.append({**contact, "found": False, "linkedin_url": "", "headline": "", "provider_id": "", "network_distance": ""})
        time.sleep(random.uniform(45, 60))
        continue

    # Extract data from search result only — NO profile fetch via Unipile
    # (profile fetches are rate-limited ~100/day and increase account risk)
    # We'll use Apify profile scraper later for full data — that doesn't touch the LI account
    slug = match.get("public_identifier", "") or match.get("slug", "")
    provider_id = match.get("provider_id", "")
    linkedin_url = f"https://www.linkedin.com/in/{slug}" if slug else ""
    network_distance = match.get("network_distance", "")
    headline = match.get("headline", "")
    location = match.get("location", "")

    log.info(f"  LinkedIn URL: {linkedin_url}")
    log.info(f"  Headline: {headline[:80]}")
    log.info(f"  Network: {network_distance}")

    results.append({
        **contact,
        "found": True,
        "linkedin_url": linkedin_url,
        "provider_id": provider_id,
        "slug": slug,
        "headline": headline,
        "location": location,
        "connections": "",
        "followers": "",
        "network_distance": network_distance,
        "current_role": "",
        "current_company": "",
        "role_match": "",
    })

    # Rate limit: 60-120s between searches (conservative — account safety)
    delay = random.uniform(60, 120)
    log.info(f"  Waiting {delay:.0f}s...")
    time.sleep(delay)


# ── Output ──
OUTFILE = "docs/ICP-Prospects/zoominfo_only_linkedin_discovery.csv"
csv_headers = [
    "First Name", "Last Name", "Title", "Company",
    "Found on LinkedIn", "LinkedIn URL", "Provider ID", "Slug",
    "LinkedIn Headline", "Location", "Connections", "Followers",
    "Network Distance", "Current Role", "Current Company", "Role Match",
]

with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=csv_headers)
    writer.writeheader()
    for r in results:
        writer.writerow({
            "First Name": r.get("first_name", ""),
            "Last Name": r.get("last_name", ""),
            "Title": r.get("title", ""),
            "Company": r.get("company", ""),
            "Found on LinkedIn": "Yes" if r.get("found") else "No",
            "LinkedIn URL": r.get("linkedin_url", ""),
            "Provider ID": r.get("provider_id", ""),
            "Slug": r.get("slug", ""),
            "LinkedIn Headline": r.get("headline", ""),
            "Location": r.get("location", ""),
            "Connections": r.get("connections", ""),
            "Followers": r.get("followers", ""),
            "Network Distance": r.get("network_distance", ""),
            "Current Role": r.get("current_role", ""),
            "Current Company": r.get("current_company", ""),
            "Role Match": r.get("role_match", ""),
        })

log.info(f"\n{'=' * 50}")
log.info(f"RESULTS: {sum(1 for r in results if r.get('found'))}/{len(results)} found on LinkedIn")
log.info(f"Output: {OUTFILE}")
log.info(f"{'=' * 50}")

if not_found:
    log.info(f"\nNOT FOUND ({len(not_found)}):")
    for c in not_found:
        log.info(f"  {c['first_name']} {c['last_name']} @ {c['company']}")
