"""
Apollo API — Test all available search approaches and check what's accessible.
Also tests people/match (enrichment) to see if it reveals full data.
"""
import requests
import json

APOLLO_API_KEY = "I0gWSglDrTxCJmiGw-J2aQ"
BASE_URL = "https://api.apollo.io"
HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": APOLLO_API_KEY,
}


def test_people_search_count():
    """Check total available prospects for VWC ICP."""
    print("=" * 60)
    print("TEST 1: ICP Pool Size (CFOs at small/mid companies, West Coast)")
    print("=" * 60)

    searches = {
        "CFOs in WA/OR/CA (11-200 emp)": {
            "person_titles": ["CFO", "Chief Financial Officer"],
            "person_seniorities": ["c_suite", "owner"],
            "organization_num_employees_ranges": ["11-50", "51-200"],
            "person_locations": ["Washington, United States", "Oregon, United States", "California, United States"],
            "per_page": 1,
            "page": 1,
        },
        "Controllers in WA/OR/CA (11-500 emp)": {
            "person_titles": ["Controller", "Comptroller"],
            "person_seniorities": ["c_suite", "vp", "director", "manager"],
            "organization_num_employees_ranges": ["11-50", "51-200", "201-500"],
            "person_locations": ["Washington, United States", "Oregon, United States", "California, United States"],
            "per_page": 1,
            "page": 1,
        },
        "VP/Director Finance in WA/OR/CA (11-500 emp)": {
            "person_titles": ["VP Finance", "Vice President Finance", "Director of Finance", "Finance Director"],
            "person_seniorities": ["vp", "director"],
            "organization_num_employees_ranges": ["11-50", "51-200", "201-500"],
            "person_locations": ["Washington, United States", "Oregon, United States", "California, United States"],
            "per_page": 1,
            "page": 1,
        },
        "All finance titles, US-wide, accounting industry": {
            "person_titles": ["CFO", "Controller", "VP Finance", "Managing Partner", "Owner"],
            "person_seniorities": ["c_suite", "owner", "founder", "vp"],
            "q_organization_keyword_tags": ["accounting", "CPA", "audit"],
            "organization_num_employees_ranges": ["11-50", "51-200", "201-500"],
            "person_locations": ["United States"],
            "per_page": 1,
            "page": 1,
        },
        "Broad: decision makers at audit-needing companies (WA)": {
            "person_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Owner", "CEO"],
            "person_seniorities": ["c_suite", "owner", "founder"],
            "organization_num_employees_ranges": ["51-200", "201-500"],
            "person_locations": ["Washington, United States"],
            "per_page": 1,
            "page": 1,
        },
    }

    for label, data in searches.items():
        resp = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json=data)
        if resp.status_code == 200:
            total = resp.json().get("total_entries", "?")
            print(f"  {label}: {total:,} prospects" if isinstance(total, int) else f"  {label}: {total} prospects")
        else:
            print(f"  {label}: ERROR {resp.status_code}")


def test_enrichment_single():
    """Test if enrichment reveals full person data (costs 1 credit)."""
    print("\n" + "=" * 60)
    print("TEST 2: Single Person Enrichment (1 credit)")
    print("=" * 60)

    # First, get an Apollo ID from search
    search_data = {
        "person_titles": ["CFO"],
        "person_seniorities": ["c_suite"],
        "organization_num_employees_ranges": ["51-200"],
        "person_locations": ["Washington, United States"],
        "per_page": 1,
        "page": 1,
    }
    resp = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json=search_data)
    if resp.status_code != 200:
        print(f"Search failed: {resp.status_code}")
        return

    people = resp.json().get("people", [])
    if not people:
        print("No search results")
        return

    person = people[0]
    apollo_id = person.get("id")
    print(f"Found: {person.get('first_name', '?')} {person.get('last_name_obfuscated', '?')} — {person.get('title', '?')} at {person.get('organization', {}).get('name', '?')}")
    print(f"Apollo ID: {apollo_id}")

    # Now enrich by ID
    print("\nEnriching by Apollo ID...")
    enrich_data = {
        "id": apollo_id,
        "reveal_personal_emails": True,
        "reveal_phone_number": True,
    }
    resp = requests.post(f"{BASE_URL}/api/v1/people/match", headers=HEADERS, json=enrich_data)
    print(f"Status: {resp.status_code}")

    if resp.status_code == 200:
        result = resp.json()
        p = result.get("person", {})
        print(f"\n--- ENRICHED PERSON ---")
        print(f"  Name:       {p.get('first_name', 'N/A')} {p.get('last_name', 'N/A')}")
        print(f"  Title:      {p.get('title', 'N/A')}")
        print(f"  LinkedIn:   {p.get('linkedin_url', 'N/A')}")
        print(f"  Email:      {p.get('email', 'N/A')}")
        print(f"  Phone:      {p.get('phone_numbers', 'N/A')}")
        print(f"  City:       {p.get('city', 'N/A')}")
        print(f"  State:      {p.get('state', 'N/A')}")
        print(f"  Seniority:  {p.get('seniority', 'N/A')}")

        org = p.get("organization", {})
        if org:
            print(f"  Company:    {org.get('name', 'N/A')}")
            print(f"  Industry:   {org.get('industry', 'N/A')}")
            print(f"  Employees:  {org.get('estimated_num_employees', 'N/A')}")
            print(f"  Website:    {org.get('website_url', 'N/A')}")
            print(f"  Revenue:    {org.get('annual_revenue', 'N/A')}")
            print(f"  Founded:    {org.get('founded_year', 'N/A')}")
            print(f"  Keywords:   {org.get('keywords', [])[:5]}")

        # Dump full for reference
        print(f"\n--- FULL RAW ENRICHED RESPONSE ---")
        print(json.dumps(result, indent=2, default=str)[:3000])
    else:
        print(f"Error: {resp.text[:500]}")


def test_credits_available():
    """Check how many credits we have."""
    print("\n" + "=" * 60)
    print("TEST 3: Credit Balance Check")
    print("=" * 60)

    # Try the usage endpoint
    resp = requests.get(f"{BASE_URL}/api/v1/auth/health", headers=HEADERS)
    if resp.status_code == 200:
        print(f"Auth health: {json.dumps(resp.json(), indent=2)}")

    # Rate limits from headers
    resp2 = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json={
        "person_titles": ["CFO"], "per_page": 1, "page": 1
    })
    print(f"\nRate limit headers:")
    for h in sorted(resp2.headers):
        if any(x in h.lower() for x in ["rate", "usage", "request", "credit", "limit"]):
            print(f"  {h}: {resp2.headers[h]}")


if __name__ == "__main__":
    test_people_search_count()
    test_enrichment_single()
    test_credits_available()
