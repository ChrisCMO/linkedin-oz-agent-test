"""
Apollo → Unipile Pipeline Example
VWC CPAs ICP: CFOs/Controllers at mid-size companies in the Pacific Northwest

Flow:
1. Search Apollo for ICP matches (FREE - no credits)
2. Enrich top prospects to get LinkedIn URLs (1 credit each)
3. Output structured data ready for Unipile connection requests
"""

import requests
import json
import time
import random

# ── Config ──────────────────────────────────────────────
APOLLO_API_KEY = "I0gWSglDrTxCJmiGw-J2aQ"
APOLLO_BASE = "https://api.apollo.io"
APOLLO_HEADERS = {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "X-Api-Key": APOLLO_API_KEY,
}

UNIPILE_API_KEY = "your_unipile_key"  # placeholder
UNIPILE_BASE = "https://api22.unipile.com:15258"
UNIPILE_HEADERS = {
    "Content-Type": "application/json",
    "X-API-KEY": UNIPILE_API_KEY,
}

# VWC ICP definition
ICP = {
    "titles": [
        "CFO",
        "Chief Financial Officer",
        "Controller",
        "VP Finance",
        "Vice President Finance",
        "Director of Finance",
    ],
    "seniorities": ["c_suite", "vp", "director", "owner"],
    "company_sizes": ["11-50", "51-200", "201-500"],
    "locations": [
        "Washington, United States",
        "Oregon, United States",
    ],
    # Industries where companies NEED audits
    "target_keywords": [
        "manufacturing",
        "construction",
        "real estate",
        "nonprofit",
        "healthcare",
        "government contracting",
    ],
}


# ── Step 1: Search (FREE) ──────────────────────────────
def search_prospects(page=1, per_page=10):
    """Search Apollo for ICP matches. Costs 0 credits."""
    data = {
        "person_titles": ICP["titles"],
        "person_seniorities": ICP["seniorities"],
        "organization_num_employees_ranges": ICP["company_sizes"],
        "person_locations": ICP["locations"],
        "q_organization_keyword_tags": ICP["target_keywords"],
        "per_page": per_page,
        "page": page,
    }

    resp = requests.post(
        f"{APOLLO_BASE}/api/v1/mixed_people/api_search",
        headers=APOLLO_HEADERS,
        json=data,
    )

    if resp.status_code != 200:
        print(f"Search failed: {resp.status_code} — {resp.text[:200]}")
        return None

    result = resp.json()
    total = result.get("total_entries", 0)
    people = result.get("people", [])

    print(f"Found {total:,} total prospects matching ICP")
    print(f"Page {page}: {len(people)} results\n")

    return result


# ── Step 2: Enrich (1 credit each) ─────────────────────
def enrich_prospect(apollo_id):
    """Get full person data including LinkedIn URL. Costs 1 credit."""
    resp = requests.post(
        f"{APOLLO_BASE}/api/v1/people/match",
        headers=APOLLO_HEADERS,
        json={"id": apollo_id, "reveal_personal_emails": True},
    )

    if resp.status_code != 200:
        print(f"  Enrich failed for {apollo_id}: {resp.status_code}")
        return None

    person = resp.json().get("person", {})
    org = person.get("organization", {}) or {}

    return {
        "apollo_id": person.get("id"),
        "name": person.get("name"),
        "first_name": person.get("first_name"),
        "last_name": person.get("last_name"),
        "title": person.get("title"),
        "seniority": person.get("seniority"),
        "linkedin_url": person.get("linkedin_url"),
        "email": person.get("email"),
        "city": person.get("city"),
        "state": person.get("state"),
        "photo_url": person.get("photo_url"),
        "company_name": org.get("name"),
        "company_industry": org.get("industry"),
        "company_employees": org.get("estimated_num_employees"),
        "company_website": org.get("website_url"),
        "company_linkedin": org.get("linkedin_url"),
        "company_revenue": org.get("annual_revenue"),
        "company_founded": org.get("founded_year"),
        "company_keywords": org.get("keywords", []),
        "employment_history": [
            {
                "company": e.get("organization_name"),
                "title": e.get("title"),
                "start": e.get("start_date"),
                "end": e.get("end_date"),
                "current": e.get("current"),
            }
            for e in person.get("employment_history", [])[:5]
        ],
    }


# ── Step 3: Score against ICP ───────────────────────────
def score_prospect(prospect):
    """Simple ICP scoring (0-100). In production, use AI for reasoning."""
    score = 0
    reasons = []

    # Title match (0-30)
    title = (prospect.get("title") or "").lower()
    if "cfo" in title or "chief financial" in title:
        score += 30
        reasons.append("CFO title (highest value)")
    elif "controller" in title or "comptroller" in title:
        score += 25
        reasons.append("Controller title (strong)")
    elif "vp" in title or "vice president" in title:
        score += 20
        reasons.append("VP Finance title")
    elif "director" in title:
        score += 15
        reasons.append("Director level")

    # Company size (0-25)
    employees = prospect.get("company_employees") or 0
    if 51 <= employees <= 500:
        score += 25
        reasons.append(f"Ideal company size ({employees} employees)")
    elif 11 <= employees <= 50:
        score += 15
        reasons.append(f"Small company ({employees} employees)")
    elif employees > 500:
        score += 10
        reasons.append(f"Large company ({employees} employees)")

    # Industry (0-20)
    industry = (prospect.get("company_industry") or "").lower()
    keywords = [k.lower() for k in prospect.get("company_keywords", [])]
    high_value_industries = ["manufacturing", "construction", "real estate", "nonprofit", "healthcare"]
    for ind in high_value_industries:
        if ind in industry or any(ind in k for k in keywords):
            score += 20
            reasons.append(f"Target industry: {ind}")
            break

    # Location (0-15)
    state = (prospect.get("state") or "").lower()
    if "washington" in state:
        score += 15
        reasons.append("Located in Washington (home state)")
    elif "oregon" in state:
        score += 12
        reasons.append("Located in Oregon (nearby)")
    elif state:
        score += 5
        reasons.append(f"Located in {prospect.get('state')}")

    # Has LinkedIn (0-10) — required for Unipile outreach
    if prospect.get("linkedin_url"):
        score += 10
        reasons.append("LinkedIn profile available")

    prospect["icp_score"] = score
    prospect["icp_reasons"] = reasons
    return prospect


# ── Step 4: Format for Unipile ──────────────────────────
def format_for_unipile(prospect):
    """Extract the LinkedIn provider_id from URL for Unipile API calls."""
    linkedin_url = prospect.get("linkedin_url", "")

    # Extract slug from LinkedIn URL
    # e.g., "http://www.linkedin.com/in/chipgaskins" → "chipgaskins"
    slug = ""
    if linkedin_url:
        slug = linkedin_url.rstrip("/").split("/")[-1]

    return {
        "name": prospect.get("name"),
        "title": prospect.get("title"),
        "company": prospect.get("company_name"),
        "linkedin_slug": slug,
        "linkedin_url": linkedin_url,
        "icp_score": prospect.get("icp_score"),
        "icp_reasons": prospect.get("icp_reasons"),
        # For connection request note (300 char max)
        "suggested_note": generate_connection_note(prospect),
    }


def generate_connection_note(prospect):
    """Draft a connection request note. In production, use AI for this."""
    first = prospect.get("first_name", "there")
    company = prospect.get("company_name", "your company")
    industry = prospect.get("company_industry", "")

    # Keep under 300 chars
    note = (
        f"Hi {first}, I noticed your work as {prospect.get('title', 'a finance leader')} "
        f"at {company}. I work with companies in the {industry or 'your'} space on financial "
        f"strategy. Would love to connect and exchange ideas."
    )
    return note[:300]


# ── Run the pipeline ────────────────────────────────────
def main():
    print("=" * 70)
    print("APOLLO → UNIPILE PIPELINE EXAMPLE")
    print("VWC CPAs — ICP: Finance Leaders at Mid-Size Companies (PNW)")
    print("=" * 70)

    # Step 1: Search (FREE)
    print("\n📋 STEP 1: Search Apollo for ICP matches (free, no credits)\n")
    result = search_prospects(page=1, per_page=5)
    if not result:
        return

    people = result.get("people", [])
    print(f"Preview of search results (redacted):")
    for i, p in enumerate(people, 1):
        org = p.get("organization", {}) or {}
        print(f"  {i}. {p.get('first_name', '?')} {p.get('last_name_obfuscated', '***')} — "
              f"{p.get('title', 'N/A')} at {org.get('name', 'N/A')}")

    # Step 2: Enrich first 3 (costs 1 credit each)
    print(f"\n🔍 STEP 2: Enrich top {min(3, len(people))} prospects (1 credit each)\n")
    enriched = []
    for i, p in enumerate(people[:3]):
        apollo_id = p.get("id")
        print(f"  Enriching {i+1}/3: {p.get('first_name', '?')} at {p.get('organization', {}).get('name', '?')}...")
        person = enrich_prospect(apollo_id)
        if person:
            enriched.append(person)
            print(f"    ✓ {person['name']} — {person['linkedin_url'] or 'No LinkedIn'}")
        time.sleep(random.uniform(0.5, 1.5))  # be polite to API

    # Step 3: Score
    print(f"\n📊 STEP 3: ICP Scoring\n")
    scored = [score_prospect(p) for p in enriched]
    scored.sort(key=lambda x: x.get("icp_score", 0), reverse=True)

    for p in scored:
        print(f"  {p['name']} — Score: {p['icp_score']}/100")
        print(f"    Title:    {p['title']}")
        print(f"    Company:  {p['company_name']} ({p.get('company_employees', '?')} emp)")
        print(f"    Industry: {p.get('company_industry', 'N/A')}")
        print(f"    Location: {p.get('city', '?')}, {p.get('state', '?')}")
        print(f"    LinkedIn: {p.get('linkedin_url', 'N/A')}")
        print(f"    Reasons:  {', '.join(p.get('icp_reasons', []))}")
        print()

    # Step 4: Format for Unipile
    print(f"🚀 STEP 4: Ready for Unipile outreach\n")
    unipile_ready = [format_for_unipile(p) for p in scored if p.get("linkedin_url")]

    for u in unipile_ready:
        print(f"  {u['name']} (Score: {u['icp_score']})")
        print(f"    LinkedIn slug: {u['linkedin_slug']}")
        print(f"    Connection note: {u['suggested_note'][:100]}...")
        print()

    # Save output
    output = {
        "search_total": result.get("total_entries", 0),
        "enriched_count": len(enriched),
        "unipile_ready": unipile_ready,
    }
    with open("output/apollo_pipeline_results.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"💾 Results saved to output/apollo_pipeline_results.json")

    # What Unipile calls would look like
    print(f"\n{'=' * 70}")
    print("NEXT STEP: Unipile API calls (not executed — just showing format)")
    print("=" * 70)
    for u in unipile_ready:
        print(f"""
    # Look up full profile via Unipile
    GET {UNIPILE_BASE}/api/v1/users/{u['linkedin_slug']}?account_id=DN3tskyWTS-zt9EsBELurQ&linkedin_sections=*

    # Send connection request
    POST {UNIPILE_BASE}/api/v1/users/invite
    Body: {{
        "account_id": "DN3tskyWTS-zt9EsBELurQ",
        "provider_id": "<provider_id from profile lookup>",
        "message": "{u['suggested_note'][:200]}..."
    }}
""")


if __name__ == "__main__":
    import os
    os.makedirs("output", exist_ok=True)
    main()
