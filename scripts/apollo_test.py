"""
Apollo.io API Test Script
Tests people search for VWC CPAs ICP:
- Titles: CFO, Controller, VP Finance, Chief Financial Officer
- Industries: Accounting, CPA-adjacent
- Company size: 11-200 employees
- Location: Pacific Northwest / West Coast (VWC is Seattle-based)
- Seniority: C-suite, VP, Director, Owner
"""

import requests
import json
import sys

APOLLO_API_KEY = "I0gWSglDrTxCJmiGw-J2aQ"
BASE_URL = "https://api.apollo.io"
HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": APOLLO_API_KEY,
}


def check_api_health():
    """Verify API key works."""
    print("=" * 60)
    print("TEST 1: API Health Check")
    print("=" * 60)
    resp = requests.get(f"{BASE_URL}/api/v1/auth/health", headers=HEADERS)
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        print(f"Response: {json.dumps(data, indent=2)[:500]}")
    else:
        print(f"Error: {resp.text[:500]}")
    return resp.status_code == 200


def search_cfo_prospects():
    """Search for CFO/Controller/VP Finance at small-mid accounting-adjacent firms."""
    print("\n" + "=" * 60)
    print("TEST 2: People Search — CFO/Finance Leaders")
    print("=" * 60)

    data = {
        "person_titles": [
            "CFO",
            "Chief Financial Officer",
            "Controller",
            "VP Finance",
            "Vice President Finance",
            "Finance Director",
            "Director of Finance",
        ],
        "person_seniorities": ["c_suite", "vp", "director", "owner"],
        "organization_num_employees_ranges": ["11-50", "51-200"],
        "person_locations": ["Washington, United States", "Oregon, United States", "California, United States"],
        "per_page": 10,
        "page": 1,
    }

    print(f"\nRequest body:\n{json.dumps(data, indent=2)}")
    resp = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json=data)
    print(f"\nStatus: {resp.status_code}")

    if resp.status_code == 200:
        result = resp.json()
        people = result.get("people", [])
        pagination = result.get("pagination", {})

        print(f"Total results: {pagination.get('total_entries', 'N/A')}")
        print(f"Pages: {pagination.get('total_pages', 'N/A')}")
        print(f"Showing: {len(people)} results\n")

        for i, person in enumerate(people, 1):
            org = person.get("organization", {}) or {}
            print(f"--- Prospect {i} ---")
            print(f"  Name:      {person.get('name', 'N/A')}")
            print(f"  Title:     {person.get('title', 'N/A')}")
            print(f"  Seniority: {person.get('seniority', 'N/A')}")
            print(f"  Location:  {person.get('city', '')}, {person.get('state', '')}")
            print(f"  LinkedIn:  {person.get('linkedin_url', 'N/A')}")
            print(f"  Company:   {org.get('name', 'N/A')}")
            print(f"  Industry:  {org.get('industry', 'N/A')}")
            print(f"  Employees: {org.get('estimated_num_employees', 'N/A')}")
            print(f"  Website:   {org.get('website_url', 'N/A')}")
            print(f"  Keywords:  {org.get('keywords', [])}")
            print()

        return result
    else:
        print(f"Error: {resp.text[:500]}")
        return None


def search_audit_industry_prospects():
    """Search specifically for finance leaders at accounting/audit firms."""
    print("\n" + "=" * 60)
    print("TEST 3: People Search — Finance Leaders at Accounting Firms")
    print("=" * 60)

    data = {
        "person_titles": [
            "CFO",
            "Chief Financial Officer",
            "Controller",
            "VP Finance",
            "Owner",
            "Managing Partner",
        ],
        "person_seniorities": ["c_suite", "owner", "founder", "vp", "director"],
        "q_organization_keyword_tags": ["accounting", "CPA", "audit", "tax services"],
        "organization_num_employees_ranges": ["11-50", "51-200", "201-500"],
        "person_locations": ["United States"],
        "per_page": 10,
        "page": 1,
    }

    print(f"\nRequest body:\n{json.dumps(data, indent=2)}")
    resp = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json=data)
    print(f"\nStatus: {resp.status_code}")

    if resp.status_code == 200:
        result = resp.json()
        people = result.get("people", [])
        pagination = result.get("pagination", {})

        print(f"Total results: {pagination.get('total_entries', 'N/A')}")
        print(f"Pages: {pagination.get('total_pages', 'N/A')}")
        print(f"Showing: {len(people)} results\n")

        for i, person in enumerate(people, 1):
            org = person.get("organization", {}) or {}
            print(f"--- Prospect {i} ---")
            print(f"  Name:      {person.get('name', 'N/A')}")
            print(f"  Title:     {person.get('title', 'N/A')}")
            print(f"  Seniority: {person.get('seniority', 'N/A')}")
            print(f"  Location:  {person.get('city', '')}, {person.get('state', '')}")
            print(f"  LinkedIn:  {person.get('linkedin_url', 'N/A')}")
            print(f"  Company:   {org.get('name', 'N/A')}")
            print(f"  Industry:  {org.get('industry', 'N/A')}")
            print(f"  Employees: {org.get('estimated_num_employees', 'N/A')}")
            print(f"  Website:   {org.get('website_url', 'N/A')}")
            print()

        return result
    else:
        print(f"Error: {resp.text[:500]}")
        return None


def search_vwc_target_companies():
    """Search for companies that match VWC's audit client profile."""
    print("\n" + "=" * 60)
    print("TEST 4: Organization Search — VWC Target Companies")
    print("=" * 60)

    data = {
        "organization_num_employees_ranges": ["51-200", "201-500"],
        "organization_locations": ["Washington, United States", "Oregon, United States"],
        "q_organization_keyword_tags": ["manufacturing", "construction", "real estate", "nonprofit", "healthcare"],
        "per_page": 10,
        "page": 1,
    }

    print(f"\nRequest body:\n{json.dumps(data, indent=2)}")
    resp = requests.post(f"{BASE_URL}/api/v1/mixed_companies/search", headers=HEADERS, json=data)
    print(f"\nStatus: {resp.status_code}")

    if resp.status_code == 200:
        result = resp.json()
        orgs = result.get("organizations", [])
        pagination = result.get("pagination", {})

        print(f"Total results: {pagination.get('total_entries', 'N/A')}")
        print(f"Showing: {len(orgs)} results\n")

        for i, org in enumerate(orgs, 1):
            print(f"--- Company {i} ---")
            print(f"  Name:      {org.get('name', 'N/A')}")
            print(f"  Industry:  {org.get('industry', 'N/A')}")
            print(f"  Employees: {org.get('estimated_num_employees', 'N/A')}")
            print(f"  Website:   {org.get('website_url', 'N/A')}")
            print(f"  Location:  {org.get('city', '')}, {org.get('state', '')}")
            print(f"  Keywords:  {org.get('keywords', [])[:5]}")
            print()

        return result
    else:
        print(f"Error: {resp.text[:500]}")
        return None


def check_rate_limits():
    """Check current API usage stats."""
    print("\n" + "=" * 60)
    print("TEST 5: Rate Limit / Usage Stats")
    print("=" * 60)

    resp = requests.post(f"{BASE_URL}/api/v1/usage_stats/api_usage_stats", headers=HEADERS, json={})
    print(f"Status: {resp.status_code}")
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2)[:1000])
    else:
        print(f"Response: {resp.text[:500]}")


if __name__ == "__main__":
    print("APOLLO.IO API TEST — VWC CPAs ICP Prospect Search")
    print("=" * 60)

    # Test 1: Health check
    if not check_api_health():
        print("\nAPI key invalid or API unreachable. Stopping.")
        sys.exit(1)

    # Test 2: Broad CFO search (West Coast)
    search_cfo_prospects()

    # Test 3: CFOs at accounting firms specifically
    search_audit_industry_prospects()

    # Test 4: Company search for VWC target industries
    search_vwc_target_companies()

    # Test 5: Check usage
    check_rate_limits()

    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETE")
    print("=" * 60)
