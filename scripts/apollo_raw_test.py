"""Quick raw dump to see actual Apollo response structure."""
import requests
import json

APOLLO_API_KEY = "I0gWSglDrTxCJmiGw-J2aQ"
BASE_URL = "https://api.apollo.io"
HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": APOLLO_API_KEY,
}

# Simple search: CFOs in Washington state
data = {
    "person_titles": ["CFO", "Chief Financial Officer"],
    "person_seniorities": ["c_suite", "owner"],
    "organization_num_employees_ranges": ["11-50", "51-200"],
    "person_locations": ["Washington, United States"],
    "per_page": 3,
    "page": 1,
}

resp = requests.post(f"{BASE_URL}/api/v1/mixed_people/api_search", headers=HEADERS, json=data)
print(f"Status: {resp.status_code}")
print(f"\nResponse headers (rate limits):")
for h in resp.headers:
    if "rate" in h.lower() or "usage" in h.lower() or "request" in h.lower():
        print(f"  {h}: {resp.headers[h]}")

result = resp.json()

# Show top-level keys
print(f"\nTop-level keys: {list(result.keys())}")

# Pagination
print(f"\nPagination: {json.dumps(result.get('pagination', {}), indent=2)}")

# Raw first person
people = result.get("people", [])
if people:
    print(f"\n--- RAW FIRST PERSON (all fields) ---")
    print(json.dumps(people[0], indent=2, default=str))
else:
    print("\nNo people found")

# Raw first org if present
orgs = result.get("organizations", [])
if orgs:
    print(f"\n--- RAW FIRST ORGANIZATION ---")
    print(json.dumps(orgs[0], indent=2, default=str)[:2000])
