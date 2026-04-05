#!/usr/bin/env python3
"""Test the revamped v2 company scoring on flagged companies.

Loads companies_for_adrienne_review.csv, runs finance title scan (Apollo + X-ray),
cross-references PSBJ data, detects revenue mismatches, scores with v2,
and outputs a comparison CSV.
"""
import sys, os, csv, json, time, random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import requests
from lib.apollo import ApolloClient
from mvp.backend.services.scoring import score_companies_v2, detect_revenue_mismatch

# --- Config ---
BASE = os.path.join(os.path.dirname(__file__), "..")
APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}
SERP_ACTOR = 'nFJndFXA5zjCTuudP'
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'

INPUT_FILE = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'companies_for_adrienne_review.csv')
PSBJ_FILE = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'universe', 'private', 'psbj_family_owned_wa_2026_86.csv')
BLACKLIST_FILE = os.path.join(BASE, 'data', 'blacklist.csv')
OUTPUT_FILE = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'v2_scoring_comparison.csv')

FINANCE_TITLES = ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance"]
FINANCE_TITLE_PRIORITY = {
    "cfo": 1, "chief financial officer": 1,
    "controller": 2, "financial controller": 2,
    "vp finance": 3, "vice president of finance": 3, "vp of finance": 3,
    "director of finance": 4, "director of finance and administration": 4,
}


def run_actor(actor_id, payload, max_wait=180):
    """Run an Apify actor and return results."""
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                      headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201:
        print(f"    Actor start failed: {r.status_code}")
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(max_wait // 5):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                         headers=apify_h, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
            break
    try:
        return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                            headers=apify_h, timeout=15).json()
    except Exception:
        return []


def load_blacklist():
    """Load blacklisted company names and domains."""
    blacklist = {"names": set(), "domains": set()}
    if not os.path.exists(BLACKLIST_FILE):
        return blacklist
    with open(BLACKLIST_FILE) as f:
        for row in csv.DictReader(f):
            name = row.get("company_name", "").strip().lower()
            domain = row.get("domain", "").strip().lower()
            if name:
                blacklist["names"].add(name)
            if domain:
                blacklist["domains"].add(domain)
    return blacklist


def is_blacklisted(company_name, domain, blacklist):
    """Check if a company is blacklisted (fuzzy name match or exact domain)."""
    name_lower = company_name.strip().lower()
    domain_lower = (domain or "").strip().lower()
    # Exact domain match
    if domain_lower and domain_lower in blacklist["domains"]:
        return True
    # Substring name match (catches "Anthony's HomePort Everett" matching "Anthony's HomePort")
    for bl_name in blacklist["names"]:
        if bl_name in name_lower or name_lower in bl_name:
            return True
    return False


def load_psbj():
    """Load PSBJ family-owned companies list for revenue cross-reference."""
    psbj = {}
    if not os.path.exists(PSBJ_FILE):
        print("  PSBJ file not found, skipping cross-reference")
        return psbj
    with open(PSBJ_FILE) as f:
        for row in csv.DictReader(f):
            name = row.get("Company", row.get("company_name", "")).strip().lower()
            if name:
                psbj[name] = {
                    "revenue": row.get("Revenue", row.get("revenue", "")),
                    "employees": row.get("Employees", row.get("employees", "")),
                    "ceo": row.get("CEO", row.get("ceo", "")),
                }
    return psbj


def psbj_match(company_name, psbj_data):
    """Check if company name matches PSBJ list (substring match)."""
    name_lower = company_name.strip().lower()
    for psbj_name, data in psbj_data.items():
        if psbj_name in name_lower or name_lower in psbj_name:
            return data
    return None


def apollo_finance_scan(domain):
    """Free Apollo people search for finance titles at a company domain."""
    apollo = ApolloClient()
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_organization_domains_list": [domain],
            "person_titles": FINANCE_TITLES,
            "per_page": 5,
        })
        people = result.get("people", [])
        contacts = []
        for p in people:
            title = p.get("title", "")
            contacts.append({
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "title": title,
                "linkedin_url": p.get("linkedin_url", ""),
                "company": p.get("organization", {}).get("name", ""),
            })
        return contacts
    except Exception as e:
        print(f"    Apollo scan error: {e}")
        return []


def xray_finance_search(company_name):
    """Google X-ray search for CFO/Controller LinkedIn profiles."""
    queries = [
        f'site:linkedin.com/in "{company_name}" CFO',
        f'site:linkedin.com/in "{company_name}" "chief financial officer"',
        f'site:linkedin.com/in "{company_name}" controller',
        f'site:linkedin.com/in "{company_name}" "director of finance"',
    ]
    combined_query = "\n".join(queries)

    results = run_actor(SERP_ACTOR, {
        "queries": combined_query,
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "countryCode": "us",
    })

    contacts = []
    seen_urls = set()
    for r in results:
        for item in r.get("organicResults", []):
            url = item.get("url", "")
            title_text = item.get("title", "")
            snippet = item.get("description", "")
            if "linkedin.com/in/" not in url or url in seen_urls:
                continue
            seen_urls.add(url)
            # Check if snippet/title mentions a finance role
            combined = (title_text + " " + snippet).lower()
            detected_title = ""
            for ft in ["cfo", "chief financial officer", "controller", "financial controller",
                        "director of finance", "vp finance", "vice president of finance"]:
                if ft in combined:
                    detected_title = ft.title()
                    break
            if detected_title:
                # Extract name from LinkedIn title (usually "FirstName LastName - ...")
                name_parts = title_text.split(" - ")[0].split(" | ")[0].strip().split(" ", 1)
                contacts.append({
                    "first_name": name_parts[0] if name_parts else "",
                    "last_name": name_parts[1] if len(name_parts) > 1 else "",
                    "title": detected_title,
                    "linkedin_url": url,
                    "source": "xray",
                })
    return contacts


def pick_best_finance_contact(contacts):
    """Pick the highest-priority finance contact from a list."""
    if not contacts:
        return None
    # Sort by title priority (CFO > Controller > VP Finance > Director)
    def priority(c):
        t = c.get("title", "").lower()
        for key, pri in FINANCE_TITLE_PRIORITY.items():
            if key in t:
                return pri
        return 99
    contacts.sort(key=priority)
    return contacts[0]


def main():
    # Load input data
    print("Loading input data...")
    with open(INPUT_FILE) as f:
        all_companies = list(csv.DictReader(f))
    print(f"  Total companies: {len(all_companies)}")

    # Filter to FLAG only for testing
    flagged = [c for c in all_companies if c.get("Pipeline Action") == "FLAG"]
    print(f"  Flagged companies: {len(flagged)}")

    # Load blacklist
    blacklist = load_blacklist()
    print(f"  Blacklist entries: {len(blacklist['names'])} names, {len(blacklist['domains'])} domains")

    # Load PSBJ cross-reference
    psbj_data = load_psbj()
    print(f"  PSBJ companies: {len(psbj_data)}")

    # --- Step 1b: Blacklist check ---
    print("\n--- Step 1b: Blacklist check ---")
    companies = []
    blacklisted = []
    for c in flagged:
        name = c.get("Company", "")
        domain = c.get("Domain", "")
        if is_blacklisted(name, domain, blacklist):
            print(f"  BLACKLISTED: {name}")
            blacklisted.append(c)
        else:
            companies.append(c)
    print(f"  Removed {len(blacklisted)} blacklisted, {len(companies)} remaining")

    # --- Step 3b: Finance title scan ---
    print(f"\n--- Step 3b: Finance title scan ({len(companies)} companies) ---")
    for i, c in enumerate(companies):
        name = c.get("Company", "")
        domain = c.get("Domain", "")
        print(f"  [{i+1}/{len(companies)}] {name} (domain: {domain})")

        finance_contacts = []

        # Tier 1: Apollo free search
        if domain:
            apollo_contacts = apollo_finance_scan(domain)
            if apollo_contacts:
                print(f"    Apollo: {len(apollo_contacts)} finance contacts found")
                finance_contacts.extend(apollo_contacts)
            else:
                print(f"    Apollo: no finance contacts")
            time.sleep(random.uniform(0.5, 1.0))

        # Tier 2: X-ray fallback (only if Apollo found nothing)
        if not finance_contacts and name:
            print(f"    Running X-ray search...")
            xray_contacts = xray_finance_search(name)
            if xray_contacts:
                print(f"    X-ray: {len(xray_contacts)} finance contacts found")
                finance_contacts.extend(xray_contacts)
            else:
                print(f"    X-ray: no finance contacts")
            time.sleep(1)

        # Pick best contact and set flags
        best = pick_best_finance_contact(finance_contacts)
        if best:
            c["finance_contact_first_name"] = best.get("first_name", "")
            c["finance_contact_last_name"] = best.get("last_name", "")
            c["finance_contact_title"] = best.get("title", "")
            c["finance_contact_linkedin_url"] = best.get("linkedin_url", "")
            title_lower = best.get("title", "").lower()
            c["has_cfo"] = "cfo" in title_lower or "chief financial" in title_lower
            c["has_controller"] = "controller" in title_lower
        else:
            c["finance_contact_first_name"] = ""
            c["finance_contact_last_name"] = ""
            c["finance_contact_title"] = ""
            c["finance_contact_linkedin_url"] = ""
            c["has_cfo"] = False
            c["has_controller"] = False

        # All finance titles found
        all_titles = list(set(fc.get("title", "") for fc in finance_contacts if fc.get("title")))
        c["finance_titles_found"] = ", ".join(all_titles)

    # --- Step 3c: PSBJ cross-reference ---
    print("\n--- Step 3c: PSBJ cross-reference ---")
    psbj_matches = 0
    for c in companies:
        match = psbj_match(c.get("Company", ""), psbj_data)
        if match:
            psbj_matches += 1
            psbj_rev = match.get("revenue", "")
            current_rev = c.get("Revenue", "")
            print(f"  PSBJ match: {c['Company']} — PSBJ rev: {psbj_rev}, current rev: {current_rev}")
            if psbj_rev and not current_rev:
                c["Revenue"] = psbj_rev
                c["notes"] = c.get("notes", "") + f" Revenue from PSBJ: {psbj_rev}."
            # Confirm family-owned
            c["Ownership"] = "Private (family-owned, confirmed via PSBJ)"
    print(f"  {psbj_matches} PSBJ matches found")

    # --- Step 3d: Revenue mismatch detection ---
    print("\n--- Step 3d: Revenue mismatch detection ---")
    mismatches = 0
    for c in companies:
        rev = c.get("Revenue", "")
        emp = c.get("Employees (LinkedIn)", "")
        if detect_revenue_mismatch(rev, emp):
            mismatches += 1
            print(f"  SUSPECT: {c['Company']} — {rev} rev with {emp} employees")
            c["revenue_flagged_suspect"] = True
        else:
            c["revenue_flagged_suspect"] = False
    print(f"  {mismatches} revenue mismatches detected")

    # --- Step 4: Score with v2 ---
    print(f"\n--- Step 4: Scoring {len(companies)} companies with v2 ---")

    # Build company dicts for scorer
    score_input = []
    for c in companies:
        score_input.append({
            "company_id": c.get("Company", ""),
            "company_name": c.get("Company", ""),
            "industry": c.get("Industry", ""),
            "linkedin_employees": c.get("Employees (LinkedIn)", ""),
            "revenue": c.get("Revenue", ""),
            "location": c.get("Location", ""),
            "ownership": c.get("Ownership", ""),
            "linkedin_page": c.get("Company LinkedIn URL", ""),
            "website": c.get("Domain", ""),
            "finance_titles": c.get("finance_titles_found", ""),
            "has_cfo": c.get("has_cfo", False),
            "has_controller": c.get("has_controller", False),
            "finance_contact_name": f"{c.get('finance_contact_first_name', '')} {c.get('finance_contact_last_name', '')}".strip(),
            "finance_contact_linkedin": c.get("finance_contact_linkedin_url", ""),
            "notes": c.get("notes", ""),
        })

    # Score in batches of 15
    all_scores = []
    batch_size = 15
    for i in range(0, len(score_input), batch_size):
        batch = score_input[i:i+batch_size]
        print(f"  Scoring batch {i//batch_size + 1} ({len(batch)} companies)...")
        try:
            scores = score_companies_v2(batch)
            all_scores.extend(scores)
            print(f"    Got {len(scores)} scores")
        except Exception as e:
            print(f"    ERROR: {e}")
            # Fill with empty scores for failed batch
            for b in batch:
                all_scores.append({
                    "company_id": b["company_id"],
                    "company_name": b["company_name"],
                    "score": -1,
                    "breakdown": {},
                    "reasoning": f"Scoring failed: {e}",
                })
        time.sleep(1)

    # Build score lookup
    score_lookup = {}
    for s in all_scores:
        key = s.get("company_name", s.get("company_id", ""))
        score_lookup[key] = s

    # --- Output comparison CSV ---
    print(f"\n--- Writing output to {OUTPUT_FILE} ---")
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    output_rows = []
    promoted = 0
    for c in companies:
        name = c.get("Company", "")
        old_score = int(c.get("Company ICP Score", 0))
        old_action = c.get("Pipeline Action", "")

        v2 = score_lookup.get(name, {})
        new_score = v2.get("score", -1)
        if new_score >= 80:
            new_action = "PROCEED"
        elif new_score >= 60:
            new_action = "FLAG"
        elif new_score == 0:
            new_action = "HARD EXCLUDE"
        elif new_score > 0:
            new_action = "SKIP"
        else:
            new_action = "ERROR"

        if old_action != "PROCEED" and new_action == "PROCEED":
            promoted += 1

        breakdown = v2.get("breakdown", {})
        breakdown_str = " | ".join(f"{k}: {v}" for k, v in breakdown.items()) if breakdown else ""

        output_rows.append({
            "Company": name,
            "Category": c.get("Category", ""),
            "Industry": c.get("Industry", ""),
            "Location": c.get("Location", ""),
            "Employees (LinkedIn)": c.get("Employees (LinkedIn)", ""),
            "Revenue": c.get("Revenue", ""),
            "Ownership": c.get("Ownership", ""),
            "Old Score (v1)": old_score,
            "Old Action": old_action,
            "New Score (v2)": new_score,
            "New Action": new_action,
            "Score Change": new_score - old_score if new_score >= 0 else "",
            "Promoted": "YES" if old_action != "PROCEED" and new_action == "PROCEED" else "",
            "V2 Breakdown": breakdown_str,
            "V2 Reasoning": v2.get("reasoning", ""),
            "Finance Contact First Name": c.get("finance_contact_first_name", ""),
            "Finance Contact Last Name": c.get("finance_contact_last_name", ""),
            "Finance Contact Title": c.get("finance_contact_title", ""),
            "Finance Contact LinkedIn URL": c.get("finance_contact_linkedin_url", ""),
            "Has CFO": c.get("has_cfo", ""),
            "Has Controller": c.get("has_controller", ""),
            "Finance Titles Found": c.get("finance_titles_found", ""),
            "Revenue Flagged Suspect": c.get("revenue_flagged_suspect", ""),
            "Company LinkedIn URL": c.get("Company LinkedIn URL", ""),
            "Domain": c.get("Domain", ""),
        })

    fieldnames = list(output_rows[0].keys()) if output_rows else []
    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\n=== SUMMARY ===")
    print(f"  Companies scored: {len(companies)}")
    print(f"  Blacklisted (removed): {len(blacklisted)}")
    print(f"  Revenue mismatches: {mismatches}")
    print(f"  PSBJ matches: {psbj_matches}")
    print(f"  Companies with finance contacts: {sum(1 for c in companies if c.get('finance_titles_found'))}")
    print(f"  Promoted to PROCEED: {promoted}")
    print(f"  Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
