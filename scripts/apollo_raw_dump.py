"""Dump full raw Apollo API responses — search, enrich person, enrich org."""
import requests
import json
import os

APOLLO_API_KEY = "I0gWSglDrTxCJmiGw-J2aQ"
BASE = "https://api.apollo.io"
HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": APOLLO_API_KEY,
}

os.makedirs("output", exist_ok=True)

# ── 1. People Search ───────────────────────────────────
search_payload = {
    "person_titles": [
        "CFO",
        "Chief Financial Officer",
        "Controller",
        "VP Finance",
        "Vice President Finance",
        "Director of Finance",
    ],
    "person_seniorities": ["c_suite", "vp", "director", "owner"],
    "organization_num_employees_ranges": ["11-50", "51-200", "201-500"],
    "person_locations": ["Washington, United States", "Oregon, United States"],
    "q_organization_keyword_tags": ["manufacturing", "construction", "real estate", "nonprofit", "healthcare"],
    "per_page": 5,
    "page": 1,
}

print("=" * 70)
print("1. PEOPLE SEARCH — REQUEST PAYLOAD")
print("=" * 70)
print(f"POST {BASE}/api/v1/mixed_people/api_search")
print(json.dumps(search_payload, indent=2))

resp = requests.post(f"{BASE}/api/v1/mixed_people/api_search", headers=HEADERS, json=search_payload)

print(f"\n{'=' * 70}")
print(f"1. PEOPLE SEARCH — RESPONSE (status {resp.status_code})")
print("=" * 70)
search_result = resp.json()
print(json.dumps(search_result, indent=2, default=str))

# Save
with open("output/01_search_payload.json", "w") as f:
    json.dump(search_payload, f, indent=2)
with open("output/01_search_response.json", "w") as f:
    json.dump(search_result, f, indent=2, default=str)

# Response headers
print(f"\nResponse Headers:")
for h in sorted(resp.headers):
    if any(x in h.lower() for x in ["rate", "usage", "request", "limit", "content-type"]):
        print(f"  {h}: {resp.headers[h]}")

# ── 2. Person Enrichment ───────────────────────────────
people = search_result.get("people", [])
if people:
    apollo_id = people[0]["id"]
    enrich_payload = {
        "id": apollo_id,
        "reveal_personal_emails": True,
    }

    print(f"\n{'=' * 70}")
    print("2. PERSON ENRICHMENT — REQUEST PAYLOAD")
    print("=" * 70)
    print(f"POST {BASE}/api/v1/people/match")
    print(json.dumps(enrich_payload, indent=2))

    resp2 = requests.post(f"{BASE}/api/v1/people/match", headers=HEADERS, json=enrich_payload)

    print(f"\n{'=' * 70}")
    print(f"2. PERSON ENRICHMENT — RESPONSE (status {resp2.status_code})")
    print("=" * 70)
    enrich_result = resp2.json()
    print(json.dumps(enrich_result, indent=2, default=str))

    with open("output/02_enrich_payload.json", "w") as f:
        json.dump(enrich_payload, f, indent=2)
    with open("output/02_enrich_response.json", "w") as f:
        json.dump(enrich_result, f, indent=2, default=str)

    # ── 3. Org Enrichment ──────────────────────────────
    person = enrich_result.get("person", {})
    org = person.get("organization", {}) or {}
    domain = org.get("primary_domain") or org.get("website_url", "")
    # Clean domain
    domain = domain.replace("http://", "").replace("https://", "").rstrip("/")

    if domain:
        org_payload = {"domain": domain}

        print(f"\n{'=' * 70}")
        print("3. ORG ENRICHMENT — REQUEST PAYLOAD")
        print("=" * 70)
        print(f"POST {BASE}/api/v1/organizations/enrich")
        print(json.dumps(org_payload, indent=2))

        resp3 = requests.post(f"{BASE}/api/v1/organizations/enrich", headers=HEADERS, json=org_payload)

        print(f"\n{'=' * 70}")
        print(f"3. ORG ENRICHMENT — RESPONSE (status {resp3.status_code})")
        print("=" * 70)
        org_result = resp3.json()
        print(json.dumps(org_result, indent=2, default=str))

        with open("output/03_org_enrich_payload.json", "w") as f:
            json.dump(org_payload, f, indent=2)
        with open("output/03_org_enrich_response.json", "w") as f:
            json.dump(org_result, f, indent=2, default=str)
    else:
        print("\nNo domain found for org enrichment")

print(f"\n{'=' * 70}")
print("All raw payloads/responses saved to output/ folder")
print("=" * 70)
