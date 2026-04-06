#!/usr/bin/env python3
"""Full v2 pipeline test: LinkedIn scrape → Apollo enrich → finance scan → v2 score.

Usage:
    .venv/bin/python3 -m scripts.test_full_v2_pipeline --file /path/to/csv
"""
import argparse, csv, json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import requests
from lib.apollo import ApolloClient
from mvp.backend.services.scoring import score_companies_v2, detect_revenue_mismatch

APIFY_TOKEN = os.environ["APIFY_API_KEY"]
APIFY_H = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
COMPANY_SCRAPER = "UwSdACBp7ymaGUJjS"
PROFILE_SCRAPER = "LpVuK3Zozwuipa5bp"  # LinkedIn profile scraper for verification
SERP_ACTOR = "nFJndFXA5zjCTuudP"  # Google SERP for X-ray
FINANCE_TITLES = ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance"]


def run_actor(actor_id, payload, max_wait=120):
    r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs",
                      headers=APIFY_H, json=payload, timeout=30)
    if r.status_code != 201:
        print(f"  Actor start failed: {r.status_code}")
        return []
    run_id = r.json()["data"]["id"]
    ds = r.json()["data"]["defaultDatasetId"]
    for _ in range(max_wait // 5):
        time.sleep(5)
        s = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}",
                         headers=APIFY_H, timeout=15).json()["data"]["status"]
        if s in ("SUCCEEDED", "FAILED", "ABORTED"):
            break
    try:
        return requests.get(f"https://api.apify.com/v2/datasets/{ds}/items",
                            headers=APIFY_H, timeout=15).json()
    except:
        return []


JUNK_DOMAINS = {
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com", "snapchat.com",
    "amazon.com", "lazada.com", "shopee.com", "ebay.com", "etsy.com",
    "alibaba.com", "aliexpress.com",
    "yelp.com", "yellowpages.com", "bbb.org", "mapquest.com",
    "google.com", "goo.gl", "bit.ly", "linktr.ee",
    "wix.com", "squarespace.com", "godaddy.com", "wordpress.com",
}


def extract_domain(website):
    if not website:
        return None
    raw = website.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0].strip()
    if raw.lower() in JUNK_DOMAINS:
        return None
    return raw


def xray_find_contact_linkedin(contacts, company_name):
    """Google X-ray via Apify SERP actor to find LinkedIn URLs for contacts."""
    needs_lookup = [c for c in contacts if not c.get("linkedin_url")]
    if not needs_lookup:
        return contacts

    queries = []
    for c in needs_lookup:
        name = c.get("first_name", c.get("name", ""))
        queries.append(f'site:linkedin.com/in "{name}" "{company_name}"')

    print(f"    X-ray: searching {len(queries)} contacts via Apify SERP...")
    results = run_actor(SERP_ACTOR, {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 3,
        "countryCode": "us",
    })

    for i, c in enumerate(needs_lookup):
        if i >= len(results):
            break
        name_lower = c.get("first_name", "").lower()
        for item in results[i].get("organicResults", []):
            url = item.get("url", "")
            title_text = (item.get("title", "") or "").lower()
            if "linkedin.com/in/" in url and name_lower in title_text:
                c["linkedin_url"] = url
                full_name = item.get("title", "").split(" - ")[0].split(" | ")[0].strip()
                if full_name and len(full_name) > len(c.get("name", "")):
                    c["name"] = full_name
                    parts = full_name.split(" ", 1)
                    c["first_name"] = parts[0]
                    c["last_name"] = parts[1] if len(parts) > 1 else ""
                print(f"      {c['name']} → {url}")
                break

    return contacts


def _build_company_match_terms(company_name):
    """Build a list of match terms to verify X-ray results belong to the right company.

    For "SMC - Seattle Manufacturing Corporation":
      → ["seattle manufacturing corporation", "seattle manufacturing", "smcgear"]
    For "TASC - Technical & Assembly Services Corporation":
      → ["technical & assembly services corporation", "technical assembly"]
    """
    terms = []
    name = company_name.strip()

    # If name has a separator like " - ", use the longer part as the real name
    for sep in [" - ", " – ", " — "]:
        if sep in name:
            parts = name.split(sep)
            # Use the longer part (usually the full name, not the abbreviation)
            long_part = max(parts, key=len).strip()
            short_part = min(parts, key=len).strip()
            terms.append(long_part.lower())
            # Also add first 2+ significant words of the long part
            words = [w for w in long_part.split() if len(w) > 2
                     and w.lower() not in ("the", "inc", "inc.", "llc", "corp", "corp.",
                                           "corporation", "company", "co", "co.", "ltd",
                                           "group", "services", "management")]
            if len(words) >= 2:
                terms.append(" ".join(words[:2]).lower())
            break

    # Fallback: filter out generic words, take first 2+ meaningful words
    if not terms:
        words = [w for w in name.split() if len(w) > 2
                 and w.lower() not in ("the", "inc", "inc.", "llc", "corp", "corp.",
                                       "corporation", "company", "co", "co.", "ltd",
                                       "group", "services", "management")]
        if len(words) >= 2:
            terms.append(" ".join(words[:2]).lower())
        elif words:
            terms.append(words[0].lower())

    return terms


def xray_discover_finance_contacts(company_name, domain=None):
    """Tier 2: Google X-ray search for finance titles at a company when Apollo returns 0.

    Uses the most distinctive part of the company name (not abbreviations) plus domain
    for more accurate matching.
    """
    title_keywords = [
        ("CFO", "CFO"),
        ('"chief financial officer"', "Chief Financial Officer"),
        ("controller", "Controller"),
        ('"director of finance"', "Director of Finance"),
    ]

    # Build search name: prefer the distinctive part, not abbreviations
    match_terms = _build_company_match_terms(company_name)
    # Use the most specific match term for the search query
    search_name = match_terms[0] if match_terms else company_name

    # Build queries: use domain-based search if available, name-based as fallback
    queries = []
    for kw, _ in title_keywords:
        if domain:
            queries.append(f'site:linkedin.com/in "{domain}" {kw}')
        else:
            queries.append(f'site:linkedin.com/in "{search_name}" {kw}')

    print(f"    X-ray Tier 2: searching finance titles...")
    print(f"      Search term: \"{domain or search_name}\"")
    print(f"      Match terms: {match_terms}")
    results = run_actor(SERP_ACTOR, {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "countryCode": "us",
    })

    contacts = []
    seen_urls = set()

    for i, batch in enumerate(results or []):
        title_label = title_keywords[i][1] if i < len(title_keywords) else "Finance"
        for item in batch.get("organicResults", []):
            url = item.get("url", "")
            if "linkedin.com/in/" not in url or url in seen_urls:
                continue
            title_text = item.get("title", "")
            desc = item.get("description", "")
            combined = (title_text + " " + desc).lower()

            # Verify the result matches at least one company match term
            matched = any(term in combined for term in match_terms)
            if not matched:
                print(f"      SKIP (no match): {title_text[:60]} — {url}")
                continue

            seen_urls.add(url)
            name_part = title_text.split(" - ")[0].split(" | ")[0].strip()
            parts = name_part.split(" ", 1)
            first = parts[0] if parts else ""
            last = parts[1] if len(parts) > 1 else ""
            contacts.append({
                "name": name_part,
                "first_name": first,
                "last_name": last,
                "title": title_label,
                "linkedin_url": url,
                "apollo_id": "",
            })
            print(f"      MATCH: {name_part} ({title_label}) → {url}")

    # --- Tier 3: Profile scrape verification ---
    if contacts:
        print(f"    Tier 3: Verifying {len(contacts)} X-ray contacts via profile scrape...")
        urls_to_scrape = [c["linkedin_url"] for c in contacts if c.get("linkedin_url")]
        if urls_to_scrape:
            # Normalize URLs to https
            urls_to_scrape = [u if u.startswith("https://") else u.replace("http://", "https://")
                              for u in urls_to_scrape]
            profiles = run_actor(PROFILE_SCRAPER, {"urls": urls_to_scrape})
            profile_by_url = {}
            for p in profiles:
                p_url = p.get("url", p.get("linkedinUrl", ""))
                if p_url:
                    profile_by_url[p_url.rstrip("/")] = p

            verified = []
            for c in contacts:
                li = c["linkedin_url"].rstrip("/")
                li_https = li if li.startswith("https://") else li.replace("http://", "https://")
                profile = profile_by_url.get(li_https) or profile_by_url.get(li, {})
                if not profile:
                    print(f"      UNVERIFIED (no profile data): {c['name']}")
                    continue

                headline = (profile.get("headline") or "").lower()
                current_positions = profile.get("currentPosition") or []
                current_companies = [pos.get("companyName", "").lower() for pos in current_positions]
                current_titles = [pos.get("title", "").lower() for pos in current_positions]

                # Check if person works at this company
                company_matched = any(term in " ".join(current_companies + [headline])
                                      for term in match_terms)
                if not company_matched:
                    actual = current_companies[0] if current_companies else headline[:50]
                    print(f"      REJECTED (wrong company): {c['name']} — actually at \"{actual}\"")
                    continue

                # Update title from live profile data
                live_title = current_titles[0] if current_titles else profile.get("headline", "")
                c["title"] = live_title.title() if live_title else c["title"]
                c["connections"] = profile.get("connectionsCount", "")
                verified.append(c)
                print(f"      VERIFIED: {c['name']} — {c['title']} ({c.get('connections', '?')} connections)")

            contacts = verified

    return contacts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--output", required=False, help="Output CSV path")
    args = parser.parse_args()

    with open(args.file) as f:
        companies = list(csv.DictReader(f))
    print(f"Loaded {len(companies)} companies\n")

    # Normalize column names — handle different CSV formats
    for c in companies:
        if not c.get("linkedin_url"):
            c["linkedin_url"] = c.get("serper_linkedin_url", "")
        if not c.get("domain") and c.get("website"):
            c["domain"] = extract_domain(c["website"])
        if not c.get("location"):
            parts = [c.get("city", c.get("serper_city", "")), c.get("state", c.get("serper_state", ""))]
            c["location"] = ", ".join(p for p in parts if p)
        if not c.get("industry"):
            c["industry"] = c.get("icp_industry", c.get("zi_industry", ""))

    # --- Step 2: LinkedIn company page scrape (only if we have LinkedIn URLs) ---
    urls = [c["linkedin_url"] for c in companies if c.get("linkedin_url")]
    print(f"Step 2: LinkedIn Company Page Scrape ({len(urls)} URLs)...")
    li_results = run_actor(COMPANY_SCRAPER, {"companies": urls})
    print(f"  Got {len(li_results)} results\n")

    li_by_url = {}
    for item in li_results:
        url = item.get("linkedinUrl", item.get("url", ""))
        if url:
            li_by_url[url.rstrip("/")] = item

    apollo = ApolloClient()
    results = []

    for c in companies:
        name = c["company_name"]
        li_url = c.get("linkedin_url", "").rstrip("/")
        li = li_by_url.get(li_url, {})
        website = li.get("website") or li.get("websiteUrl") or c.get("website", "") or ""
        domain = extract_domain(website) or c.get("domain", "")
        hq = li.get("headquarter") or {}
        location = ", ".join(p for p in [hq.get("city", ""), hq.get("geographicArea", "")] if p)
        if not location:
            location = c.get("location", "") or ", ".join(
                p for p in [c.get("city", c.get("serper_city", "")),
                            c.get("state", c.get("serper_state", ""))] if p)

        row = {
            "company_name": name,
            "linkedin_url": li_url,
            "domain": domain or "",
            "website": website,
            "location": location,
            "li_employees": li.get("employeeCount", ""),
            "li_followers": li.get("followerCount", ""),
            "li_tagline": li.get("tagline", ""),
            "li_description": li.get("description") or "",
            "li_founded": (li.get("foundedOn") or {}).get("year", ""),
            "li_specialties": ", ".join(li.get("specialities", []) or []),
        }

        print(f"--- {name} ---")
        print(f"  LinkedIn: {row['li_employees']} emp, {row['li_followers']} followers, domain: {domain}")

        # --- Step 3: Apollo org enrichment (uses ApolloClient with proper auth) ---
        if domain:
            print(f"  Step 3: Apollo enrich ({domain})...")
            ar = apollo._request("POST", "/api/v1/organizations/enrich",
                                 json_body={"api_key": apollo.api_key, "domain": domain})
            org = ar.get("organization", {})
            if org:
                row["apollo_employees"] = org.get("estimated_num_employees", "")
                row["apollo_revenue"] = org.get("annual_revenue_printed", "")
                row["apollo_industry"] = org.get("industry", "")
                row["apollo_description"] = org.get("short_description") or org.get("seo_description") or ""

                # Ownership detection: stock symbol → PUBLIC, funding stage → VC/PE, else → Private
                symbol = org.get("publicly_traded_symbol")
                funding_stage = org.get("latest_funding_stage")
                if symbol:
                    row["ownership"] = f"Public ({symbol})"
                elif funding_stage and any(s in str(funding_stage).lower() for s in ["series", "private equity", "venture"]):
                    row["ownership"] = f"VC/PE-backed ({funding_stage})"
                else:
                    row["ownership"] = "Private (confirmed via Apollo)"

                print(f"    {row['apollo_employees']} emp, {row['apollo_revenue']} rev, {row['ownership']}")
            else:
                print(f"    No Apollo data")
            time.sleep(0.5)

        # Best description: prefer LinkedIn (longer), fallback to Apollo
        li_desc = row.get("li_description", "")
        apollo_desc = row.get("apollo_description", "")
        row["description"] = li_desc if len(li_desc) > len(apollo_desc) else apollo_desc or li_desc

        # --- Step 3b: Finance title scan ---
        if domain:
            print(f"  Step 3b: Finance title scan...")
            fr = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
                "q_organization_domains_list": [domain],
                "person_titles": FINANCE_TITLES,
                "per_page": 5,
            })
            contacts = []
            for p in fr.get("people", []):
                first = p.get("first_name", "")
                last = p.get("last_name", "")
                full_name = f"{first} {last}".strip()
                li_url = p.get("linkedin_url", "")
                apollo_id = p.get("id", "")
                contacts.append({
                    "name": full_name,
                    "first_name": first,
                    "last_name": last,
                    "title": p.get("title", ""),
                    "linkedin_url": li_url,
                    "apollo_id": apollo_id,
                })

            # X-ray to find LinkedIn URLs for contacts Apollo didn't provide
            if contacts:
                contacts = xray_find_contact_linkedin(contacts, name)

            if contacts:
                row["_contacts"] = contacts
                row["has_cfo"] = any("cfo" in fc["title"].lower() or "chief financial" in fc["title"].lower() for fc in contacts)
                row["has_controller"] = any("controller" in fc["title"].lower() for fc in contacts)
                row["all_finance_titles"] = "; ".join(f"{fc['name']} ({fc['title']})" for fc in contacts)
                row["finance_contact_name"] = contacts[0]["name"]
                row["finance_contact_linkedin"] = contacts[0].get("linkedin_url", "")
                print(f"    Found {len(contacts)}: {row['all_finance_titles']}")
            else:
                row.update({"_contacts": [], "has_cfo": False,
                            "has_controller": False, "all_finance_titles": "",
                            "finance_contact_name": "", "finance_contact_linkedin": ""})
                print(f"    No finance contacts")
            time.sleep(0.5)
        else:
            row.update({"_contacts": [], "has_cfo": False,
                        "has_controller": False, "all_finance_titles": "",
                        "finance_contact_name": "", "finance_contact_linkedin": ""})

        # --- Step 3d: Revenue mismatch ---
        row["revenue_suspect"] = detect_revenue_mismatch(
            row.get("apollo_revenue", ""), row.get("li_employees", ""))

        results.append(row)

    # --- Step 4: Score with v2 ---
    print(f"\nStep 4: Scoring {len(results)} companies with v2...")
    score_input = [{
        "company_id": r["company_name"],
        "company_name": r["company_name"],
        "industry": r.get("apollo_industry", ""),
        "linkedin_employees": r.get("li_employees", ""),
        "apollo_employees": r.get("apollo_employees", ""),
        "revenue": r.get("apollo_revenue", ""),
        "location": r.get("location", ""),
        "ownership": r.get("ownership", ""),
        "linkedin_page": r["linkedin_url"],
        "linkedin_followers": r.get("li_followers", ""),
        "website": r.get("website", ""),
        "finance_titles": r.get("all_finance_titles", ""),
        "has_cfo": r.get("has_cfo", False),
        "has_controller": r.get("has_controller", False),
        "finance_contact_name": r.get("finance_contact_name", ""),
        "finance_contact_linkedin": r.get("finance_contact_linkedin", ""),
        "notes": f"Revenue suspect" if r.get("revenue_suspect") else "",
    } for r in results]

    scores = score_companies_v2(score_input)
    score_map = {s.get("company_name", s.get("company_id", "")): s for s in scores}

    for r in results:
        s = score_map.get(r["company_name"], {})
        score = s.get("score", 0) or 0
        r["icp_score"] = score
        r["pipeline_action"] = "PROCEED" if score >= 80 else "REVIEW" if score >= 60 else "HARD EXCLUDE" if score == 0 else "SKIP"
        bd = s.get("breakdown", {})
        r["score_breakdown"] = " | ".join(f"{k}: {v}" for k, v in bd.items())
        r["reasoning"] = s.get("reasoning", "")

    # --- Step 3b Tier 2: X-ray fallback for REVIEW companies with 0 contacts ---
    review_no_contacts = [r for r in results
                          if r["pipeline_action"] == "REVIEW" and not r.get("_contacts")]
    if review_no_contacts:
        print(f"\nStep 3b Tier 2: X-ray finance search for {len(review_no_contacts)} REVIEW companies with 0 contacts...")
        rescore_needed = []
        for r in review_no_contacts:
            xray_contacts = xray_discover_finance_contacts(r["company_name"], domain=r.get("domain"))
            if xray_contacts:
                r["_contacts"] = xray_contacts
                r["has_cfo"] = any("cfo" in fc["title"].lower() or "chief financial" in fc["title"].lower() for fc in xray_contacts)
                r["has_controller"] = any("controller" in fc["title"].lower() for fc in xray_contacts)
                r["all_finance_titles"] = "; ".join(f"{fc['name']} ({fc['title']})" for fc in xray_contacts)
                r["finance_contact_name"] = xray_contacts[0]["name"]
                r["finance_contact_linkedin"] = xray_contacts[0].get("linkedin_url", "")
                rescore_needed.append(r)
                print(f"    {r['company_name']}: found {len(xray_contacts)} via X-ray → will rescore")
            else:
                print(f"    {r['company_name']}: no X-ray results either")
            time.sleep(1)

        # Rescore companies that got new finance contacts
        if rescore_needed:
            print(f"\nRescoring {len(rescore_needed)} companies with new finance data...")
            rescore_input = [{
                "company_id": r["company_name"],
                "company_name": r["company_name"],
                "industry": r.get("apollo_industry", ""),
                "linkedin_employees": r.get("li_employees", ""),
                "apollo_employees": r.get("apollo_employees", ""),
                "revenue": r.get("apollo_revenue", ""),
                "location": r.get("location", ""),
                "ownership": r.get("ownership", ""),
                "linkedin_page": r["linkedin_url"],
                "linkedin_followers": r.get("li_followers", ""),
                "website": r.get("website", ""),
                "finance_titles": r.get("all_finance_titles", ""),
                "has_cfo": r.get("has_cfo", False),
                "has_controller": r.get("has_controller", False),
                "finance_contact_name": r.get("finance_contact_name", ""),
                "finance_contact_linkedin": r.get("finance_contact_linkedin", ""),
                "notes": f"Revenue suspect" if r.get("revenue_suspect") else "",
            } for r in rescore_needed]

            new_scores = score_companies_v2(rescore_input)
            new_score_map = {s.get("company_name", s.get("company_id", "")): s for s in new_scores}

            for r in rescore_needed:
                s = new_score_map.get(r["company_name"], {})
                old_score = r["icp_score"]
                new_score = s.get("score", 0) or 0
                r["icp_score"] = new_score
                r["pipeline_action"] = "PROCEED" if new_score >= 80 else "REVIEW" if new_score >= 60 else "HARD EXCLUDE" if new_score == 0 else "SKIP"
                bd = s.get("breakdown", {})
                r["score_breakdown"] = " | ".join(f"{k}: {v}" for k, v in bd.items())
                r["reasoning"] = s.get("reasoning", "")
                print(f"    {r['company_name']}: {old_score} → {new_score} ({r['pipeline_action']})")

    # --- Build output rows matching all_proceed_companies.csv format + v2 columns ---
    OUTPUT_FIELDS = [
        "Category",
        "Company",
        "Company ICP Score",
        "Pipeline Action",
        "Industry",
        "Employees (LinkedIn)",
        "Employees (Apollo)",
        "Revenue",
        "Location",
        "Ownership",
        "Company LinkedIn URL",
        "LI Followers",
        "LI Description",
        "LI Tagline",
        "LI Founded",
        "LI Has Logo",
        "Domain",
        "Website",
        "Contacts Found",
        "Score Breakdown",
        "Reasoning",
        "Why This Score",
        # v2 columns
        "Has CFO",
        "Has Controller",
        "Revenue Suspect",
        "Organizational Complexity",
        # Finance contacts (up to 5)
        "Finance Contact 1 Name",
        "Finance Contact 1 Title",
        "Finance Contact 1 LinkedIn URL",
        "Finance Contact 2 Name",
        "Finance Contact 2 Title",
        "Finance Contact 2 LinkedIn URL",
        "Finance Contact 3 Name",
        "Finance Contact 3 Title",
        "Finance Contact 3 LinkedIn URL",
        "Finance Contact 4 Name",
        "Finance Contact 4 Title",
        "Finance Contact 4 LinkedIn URL",
        "Finance Contact 5 Name",
        "Finance Contact 5 Title",
        "Finance Contact 5 LinkedIn URL",
    ]

    output_rows = []
    for r in results:
        bd = r.get("score_breakdown", "")
        # Extract organizational_complexity from breakdown
        org_complexity = ""
        for part in bd.split(" | "):
            if "organizational_complexity" in part:
                org_complexity = part.split(": ", 1)[-1]

        # Build why_this_score summary
        score = r.get("icp_score", 0)
        notes = []
        if score >= 80:
            notes.append("Strong match across all dimensions.")
        elif score >= 76:
            diff = 80 - score
            notes.append(f"{diff} point{'s' if diff != 1 else ''} from PROCEED.")
        li_emp = r.get("li_employees", "")
        if li_emp and str(li_emp).isdigit() and int(li_emp) < 50:
            notes.append(f"Small company ({li_emp} employees)")
        rev = r.get("apollo_revenue", "")
        if not rev:
            notes.append("Revenue unknown")
        elif r.get("revenue_suspect"):
            notes.append(f"Revenue suspect ({rev})")
        if r.get("has_cfo"):
            notes.append("CFO found (org complexity signal)")
        elif r.get("has_controller"):
            notes.append("Controller found (org complexity signal)")
        why = "; ".join(notes) if notes else ""

        row = {
            "Category": r.get("apollo_industry", r.get("industry", "")).title() if r.get("apollo_industry") or r.get("industry") else "",
            "Company": r["company_name"],
            "Company ICP Score": score,
            "Pipeline Action": r.get("pipeline_action", ""),
            "Industry": r.get("apollo_industry", r.get("industry", "")),
            "Employees (LinkedIn)": r.get("li_employees", ""),
            "Employees (Apollo)": r.get("apollo_employees", ""),
            "Revenue": r.get("apollo_revenue", ""),
            "Location": r.get("location", ""),
            "Ownership": r.get("ownership", ""),
            "Company LinkedIn URL": r.get("linkedin_url", ""),
            "LI Followers": r.get("li_followers", ""),
            "LI Description": r.get("li_description", ""),
            "LI Tagline": r.get("li_tagline", ""),
            "LI Founded": r.get("li_founded", ""),
            "LI Has Logo": "Yes" if r.get("li_tagline") or r.get("li_description") else "",
            "Domain": r.get("domain", ""),
            "Website": r.get("website", ""),
            "Contacts Found": len(r.get("_contacts", [])),
            "Score Breakdown": bd,
            "Reasoning": r.get("reasoning", ""),
            "Why This Score": why,
            "Has CFO": "Yes" if r.get("has_cfo") else "No",
            "Has Controller": "Yes" if r.get("has_controller") else "No",
            "Revenue Suspect": "Yes" if r.get("revenue_suspect") else "No",
            "Organizational Complexity": org_complexity,
        }

        # Add finance contacts as separate columns
        contacts = r.get("_contacts", [])
        for i in range(5):
            prefix = f"Finance Contact {i+1}"
            if i < len(contacts):
                fc = contacts[i]
                row[f"{prefix} Name"] = fc.get("name", "")
                row[f"{prefix} Title"] = fc.get("title", "")
                row[f"{prefix} LinkedIn URL"] = fc.get("linkedin_url", "")
            else:
                row[f"{prefix} Name"] = ""
                row[f"{prefix} Title"] = ""
                row[f"{prefix} LinkedIn URL"] = ""

        output_rows.append(row)

    # --- Print summary ---
    print(f"\n{'='*80}")
    print(f"FULL V2 PIPELINE RESULTS")
    print(f"{'='*80}")
    for row in output_rows:
        contacts_str = ""
        for i in range(1, 6):
            name = row.get(f"Finance Contact {i} Name", "")
            if name:
                title = row.get(f"Finance Contact {i} Title", "")
                li = row.get(f"Finance Contact {i} LinkedIn URL", "")
                contacts_str += f"\n    {i}. {name} ({title}) — {li or 'no LinkedIn URL'}"
        print(f"""
{row['Company']} — Score: {row['Company ICP Score']} ({row['Pipeline Action']})
  Domain: {row['Domain'] or '-'}  Website: {row['Website'] or '-'}
  Employees: LI={row['Employees (LinkedIn)'] or '-'}  Apollo={row['Employees (Apollo)'] or '-'}
  Revenue: {row['Revenue'] or '-'}  Ownership: {row['Ownership'] or '-'}
  Location: {row['Location'] or '-'}  Founded: {row['LI Founded'] or '-'}
  Has CFO: {row['Has CFO']}  Has Controller: {row['Has Controller']}
  Revenue Suspect: {row['Revenue Suspect']}  Org Complexity: {row['Organizational Complexity'] or '-'}
  Finance contacts ({row['Contacts Found']}): {contacts_str or 'None found'}
  Why: {row['Why This Score']}
  Breakdown: {row['Score Breakdown']}
  Reasoning: {row['Reasoning']}""")

    out = args.file.replace(".csv", "_full_v2_results.csv")
    if args.output:
        out = args.output
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(output_rows)
    print(f"\nSaved to {out}")


if __name__ == "__main__":
    main()
