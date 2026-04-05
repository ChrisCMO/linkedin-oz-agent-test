#!/usr/bin/env python3
"""Full v2 pipeline: 5 Seattle manufacturing companies — all 8 steps including company LinkedIn."""

import sys, os, csv, time, random, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timedelta
from lib.apollo import ApolloClient
from mvp.backend.services.scoring import score_prospects
from mvp.backend.services.message_gen_svc import generate_connection_note, generate_messages

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

apollo = ApolloClient()
APIFY_TOKEN = os.environ["APIFY_API_KEY"]

# Apify actors
POSTS_ACTOR = "RE0MriXnFhR3IgVnJ"
PROFILE_ACTOR = "LpVuK3Zozwuipa5bp"
COMPANY_PAGE_ACTOR = "UwSdACBp7ymaGUJjS"

BASE = "docs/ICP-Prospects/icp1_by_industry/manufacturing"


def run_apify(actor_id, payload):
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    try:
        r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs", headers=headers, json=payload, timeout=30)
        if r.status_code != 201:
            return []
        run_id = r.json()["data"]["id"]
        dataset_id = r.json()["data"]["defaultDatasetId"]
        for _ in range(24):
            time.sleep(5)
            sr = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers, timeout=15)
            if sr.json()["data"]["status"] in ("SUCCEEDED", "FAILED", "ABORTED"):
                break
        return requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers, timeout=15).json()
    except Exception as e:
        log.warning(f"  Apify error: {e}")
        return []


def parse_date(ds):
    if not ds:
        return None
    try:
        return datetime.fromisoformat(ds.replace("Z", "+00:00")).replace(tzinfo=None)
    except:
        try:
            return datetime.strptime(ds, "%Y-%m-%d %H:%M:%S")
        except:
            return None


def main():
    # Read existing enriched data to get our 5 candidates
    with open(f"{BASE}/seattle_manufacturing_enriched.csv") as f:
        rows = list(csv.DictReader(f))

    candidates = [r for r in rows if r.get("Apollo ID", "").strip() and r.get("Company Domain", "").strip() and int(r.get("ICP Score", "0") or "0") > 0]
    candidates.sort(key=lambda x: int(x.get("ICP Score", "0") or "0"), reverse=True)
    candidates = candidates[:5]

    log.info(f"Running full v2 pipeline on {len(candidates)} Seattle manufacturing companies")

    results = []

    for i, c in enumerate(candidates):
        company = c.get("Company", "")
        domain = c.get("Company Domain", "")
        apollo_id = c.get("Apollo ID", "")
        address = c.get("Google Places Address", "")

        log.info(f"\n{'=' * 60}")
        log.info(f"[{i+1}/5] {company} (domain: {domain})")
        log.info(f"{'=' * 60}")

        result = {
            "google_company": company,
            "google_address": address,
            "google_phone": c.get("Google Phone", ""),
            "google_rating": c.get("Google Rating", ""),
            "google_reviews": c.get("Google Reviews", ""),
        }

        # Step 2: Apollo Org Enrichment (company data + LinkedIn URL)
        log.info("  Step 2: Apollo Org Enrichment...")
        org_data = apollo._request("POST", "/api/v1/organizations/enrich", json_body={"domain": domain})
        org = org_data.get("organization", {})
        result["company_name"] = org.get("name", company)
        result["company_industry"] = org.get("industry", "")
        result["company_employees"] = org.get("estimated_num_employees", "")
        result["company_revenue"] = org.get("annual_revenue")
        result["company_founded"] = org.get("founded_year", "")
        result["company_city"] = f"{org.get('city', '')}, {org.get('state', '')}".strip(", ")
        result["company_linkedin_url"] = org.get("linkedin_url", "")
        result["company_website"] = org.get("website_url", "")

        rev = result["company_revenue"]
        rev_fmt = f"${rev / 1e6:.0f}M" if rev and rev >= 1e6 else ""
        log.info(f"    {result['company_name']} | {result['company_industry']} | emp={result['company_employees']} | rev={rev_fmt} | LI: {result['company_linkedin_url']}")
        time.sleep(1)

        # Step 4: Apollo Person Enrichment
        log.info("  Step 4: Apollo Person Enrichment...")
        person_data = apollo.enrich_person(apollo_id)
        person = person_data.get("person")
        if person:
            e = apollo._extract_person(person)
            result["first_name"] = e.get("first_name", "")
            result["last_name"] = e.get("last_name", "")
            result["title"] = e.get("title", "")
            result["seniority"] = e.get("seniority", "")
            result["email"] = e.get("email", "")
            result["email_status"] = (e.get("raw_person") or {}).get("email_status", "")
            result["linkedin_url"] = e.get("linkedin_url", "")
            result["headline"] = e.get("headline", "")
            result["person_city"] = f"{e.get('city', '')}, {e.get('state', '')}".strip(", ")
            log.info(f"    {result['first_name']} {result['last_name']} | {result['title']} | {result['linkedin_url']}")
        else:
            log.warning("    No person data returned")
            result.update({"first_name": "", "last_name": "", "title": "", "seniority": "", "email": "", "email_status": "", "linkedin_url": "", "headline": "", "person_city": ""})
        time.sleep(1)

        # Step 6a: Apify Company Page Scraper
        if result.get("company_linkedin_url"):
            log.info("  Step 6a: Company LinkedIn Page...")
            company_items = run_apify(COMPANY_PAGE_ACTOR, {"companies": [result["company_linkedin_url"]]})
            if company_items:
                cp = company_items[0]
                result["company_li_followers"] = cp.get("followerCount", "")
                result["company_li_employees"] = cp.get("employeeCount", "")
                result["company_li_tagline"] = cp.get("tagline", "")
                result["company_li_description"] = (cp.get("description") or "")[:200]
                result["company_li_founded"] = (cp.get("foundedOn") or {}).get("year", "")
                result["company_li_has_logo"] = "Yes" if cp.get("logo") else "No"
                log.info(f"    Followers: {result['company_li_followers']} | Tagline: {(result['company_li_tagline'] or '')[:50]}")
            else:
                result.update({"company_li_followers": "", "company_li_employees": "", "company_li_tagline": "", "company_li_description": "", "company_li_founded": "", "company_li_has_logo": ""})
        else:
            log.info("  Step 6a: No company LinkedIn URL — skipping")
            result.update({"company_li_followers": "", "company_li_employees": "", "company_li_tagline": "", "company_li_description": "", "company_li_founded": "", "company_li_has_logo": ""})

        # Step 6b: Apify Profile Scraper (role verification)
        if result.get("linkedin_url"):
            log.info("  Step 6b: Profile Scraper (role verification)...")
            profile_items = run_apify(PROFILE_ACTOR, {"urls": [result["linkedin_url"]]})
            if profile_items:
                prof = profile_items[0]
                li_headline = prof.get("headline", "")
                li_connections = prof.get("connectionsCount", "")
                li_followers = prof.get("followerCount", "")
                li_open_to_work = prof.get("openToWork", False)
                current_positions = prof.get("currentPosition", [])
                li_current_company = current_positions[0].get("companyName", "") if current_positions else ""

                result["li_headline"] = li_headline
                result["li_connections"] = li_connections
                result["li_followers"] = li_followers
                result["li_open_to_work"] = "Yes" if li_open_to_work else "No"
                result["li_current_company"] = li_current_company

                # Role verification
                apollo_title = result.get("title", "").lower()
                li_headline_lower = li_headline.lower()
                if apollo_title and li_headline_lower:
                    title_words = set(apollo_title.split())
                    headline_words = set(li_headline_lower.split())
                    overlap = title_words & headline_words
                    if len(overlap) >= 2 or "cfo" in headline_words or "controller" in headline_words:
                        result["role_verified"] = "Yes"
                    else:
                        result["role_verified"] = f"MISMATCH — Apollo: '{result['title']}' vs LinkedIn: '{li_headline}'"
                else:
                    result["role_verified"] = "Unable to verify"

                log.info(f"    LinkedIn headline: {li_headline}")
                log.info(f"    Connections: {li_connections} | Role verified: {result['role_verified']}")
            else:
                result.update({"li_headline": "", "li_connections": "", "li_followers": "", "li_open_to_work": "", "li_current_company": "", "role_verified": "No profile data"})
        else:
            result.update({"li_headline": "", "li_connections": "", "li_followers": "", "li_open_to_work": "", "li_current_company": "", "role_verified": "No LinkedIn URL"})

        # Step 6c: Apify Posts Scraper (activity)
        if result.get("linkedin_url"):
            log.info("  Step 6c: Posts Scraper (activity)...")
            posts_items = run_apify(POSTS_ACTOR, {"max_posts": 10, "profiles": [result["linkedin_url"]]})
            slug = result["linkedin_url"].rstrip("/").split("/")[-1].lower()

            now = datetime.now()
            activities = []
            for item in posts_items:
                author_url = (item.get("author", {}).get("profile_url") or "").lower()
                post_type = item.get("post_type", "regular")
                text = (item.get("text") or "")[:150]
                date_str = (item.get("posted_at") or {}).get("date", "")
                pd = parse_date(date_str)

                if slug in author_url:
                    activities.append({"date": pd, "type": "Post" if post_type == "regular" else post_type.title(), "detail": text})

            activities.sort(key=lambda x: x["date"] or datetime.min, reverse=True)
            posts_count = sum(1 for a in activities if a["type"] == "Post")
            reposts_count = sum(1 for a in activities if a["type"] in ("Repost", "Quote"))

            if activities:
                latest = activities[0]
                ld = latest["date"]
                thirty_d = now - timedelta(days=30)
                ninety_d = now - timedelta(days=90)
                if ld and ld >= thirty_d:
                    level = "Active"
                elif ld and ld >= ninety_d:
                    level = "Moderate"
                else:
                    level = "Inactive"
                result["activity_level"] = f"{level} — last {latest['type'].lower()} {ld.strftime('%Y-%m-%d') if ld else '?'}"
                result["recent_post_date"] = ld.strftime("%Y-%m-%d") if ld else ""
                result["recent_post_text"] = latest["detail"][:150]
            else:
                result["activity_level"] = "No posts found"
                result["recent_post_date"] = ""
                result["recent_post_text"] = ""

            result["posts_count"] = posts_count
            result["reposts_count"] = reposts_count
            result["total_feed_items"] = len(posts_items)
            log.info(f"    {result['activity_level']} | Posts: {posts_count} | Reposts: {reposts_count}")
        else:
            result.update({"activity_level": "No LinkedIn URL", "recent_post_date": "", "recent_post_text": "", "posts_count": 0, "reposts_count": 0, "total_feed_items": 0})

        # Step 5: AI Scoring
        log.info("  Step 5: AI Scoring...")
        prospect_for_scoring = {
            "apollo_id": apollo_id,
            "name": f"{result.get('first_name', '')} {result.get('last_name', '')}".strip(),
            "title": result.get("title", ""),
            "seniority": result.get("seniority", ""),
            "company_name": result.get("company_name", ""),
            "company_industry": result.get("company_industry", ""),
            "company_employees": result.get("company_employees"),
            "company_revenue": result.get("company_revenue"),
            "company_location": address,
            "city": "", "state": "",
            "linkedin_url": result.get("linkedin_url", ""),
        }

        ICP = {
            "target_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance", "Owner", "President"],
            "target_seniorities": ["c_suite", "vp", "director", "owner"],
            "target_industries": [
                "manufacturing", "machinery", "mechanical or industrial engineering",
                "electrical/electronic manufacturing", "building materials", "chemicals",
                "wholesale", "industrial automation", "aviation & aerospace", "automotive",
                "food production", "consumer goods", "construction", "renewables & environment",
            ],
            "target_locations": ["Seattle, Washington", "Washington, United States"],
            "employee_count_ranges": ["11,50", "51,200", "201,500", "501,1000"],
            "revenue_ranges": ["5000000,150000000"],
            "scoring_config": {
                "custom_notes": f"Google Places VERIFIED manufacturing company at {address}. Manufacturing is ICP priority #1. Score industry generously.",
                "hard_exclusions": ["Revenue > $150M", "Public Fortune 500 (>10000 employees)"],
            },
        }

        scores = score_prospects([prospect_for_scoring], ICP)
        if scores:
            s = scores[0]
            result["icp_score"] = s.get("score", 0)
            result["icp_reasoning"] = s.get("reasoning", "")
            result["icp_breakdown"] = " | ".join(f"{k}: {v}" for k, v in s.get("breakdown", {}).items())
        else:
            result["icp_score"] = 0
            result["icp_reasoning"] = "Scoring failed"
            result["icp_breakdown"] = ""
        log.info(f"    Score: {result['icp_score']} | {result['icp_reasoning'][:80]}")

        # Steps 7-8: Messages
        log.info("  Steps 7-8: Connection Notes + Messages...")
        pd = {"first_name": result.get("first_name", ""), "last_name": result.get("last_name", ""), "title": result.get("title", ""), "company_name": result.get("company_name", ""), "location": address, "headline": result.get("headline", ""), "icp_reasoning": result.get("icp_reasoning", "")}
        cd = {"industry": result.get("company_industry", "")}

        try:
            result["note_adrienne"] = generate_connection_note(pd, cd, "Adrienne Nordland")
            result["note_melinda"] = generate_connection_note(pd, cd, "Melinda Johnson")
            msgs = generate_messages(pd, cd, None)
            result["msg1"] = msgs[0]["text"] if len(msgs) > 0 else ""
            result["msg2"] = msgs[1]["text"] if len(msgs) > 1 else ""
            result["msg3"] = msgs[2]["text"] if len(msgs) > 2 else ""
            log.info(f"    Adrienne: {result['note_adrienne'][:60]}...")
            log.info(f"    Melinda:  {result['note_melinda'][:60]}...")
        except Exception as e:
            log.warning(f"    Message gen error: {e}")
            result.update({"note_adrienne": "", "note_melinda": "", "msg1": "", "msg2": "", "msg3": ""})

        results.append(result)

    # Export CSV
    log.info(f"\n{'=' * 60}")
    log.info("EXPORTING CSV")
    log.info(f"{'=' * 60}")

    outfile = f"{BASE}/seattle_mfg_pipeline_v2.csv"
    headers = [
        "ICP Score", "First Name", "Last Name", "Title", "Company",
        "Industry", "Employees", "Revenue", "Company City", "Company Domain",
        "Email", "Email Status", "LinkedIn URL", "Seniority", "Headline",
        # Role verification
        "LinkedIn Headline", "LinkedIn Connections", "LinkedIn Followers", "Open to Work", "LinkedIn Current Company", "Role Verified",
        # Company LinkedIn page
        "Company LinkedIn URL", "Company LI Followers", "Company LI Employees", "Company LI Tagline", "Company LI Description", "Company LI Founded", "Company LI Has Logo",
        # Activity
        "Activity Level", "Recent Post Date", "Recent Post Text", "Posts Count", "Reposts Count", "Total Feed Items",
        # Scoring
        "ICP Reasoning", "ICP Score Breakdown",
        # Messages
        "Melinda's Connection Note", "Adrienne's Connection Note",
        "Message 1 (after connect)", "Message 2 (2 weeks)", "Message 3 (4 weeks)",
        # Google Places
        "Google Places Address", "Google Phone", "Google Rating", "Google Reviews",
        # Meta
        "Apollo ID", "Data Source Pipeline",
    ]

    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for r in results:
            rev = r.get("company_revenue")
            rev_fmt = f"${rev / 1e6:.0f}M" if rev and rev >= 1e6 else ""
            writer.writerow([
                r.get("icp_score", 0), r.get("first_name", ""), r.get("last_name", ""),
                r.get("title", ""), r.get("company_name", ""), r.get("company_industry", ""),
                r.get("company_employees", ""), rev_fmt, r.get("company_city", ""), domain,
                r.get("email", ""), r.get("email_status", ""), r.get("linkedin_url", ""),
                r.get("seniority", ""), r.get("headline", ""),
                r.get("li_headline", ""), r.get("li_connections", ""), r.get("li_followers", ""),
                r.get("li_open_to_work", ""), r.get("li_current_company", ""), r.get("role_verified", ""),
                r.get("company_linkedin_url", ""), r.get("company_li_followers", ""),
                r.get("company_li_employees", ""), r.get("company_li_tagline", ""),
                r.get("company_li_description", ""), r.get("company_li_founded", ""), r.get("company_li_has_logo", ""),
                r.get("activity_level", ""), r.get("recent_post_date", ""), r.get("recent_post_text", ""),
                r.get("posts_count", 0), r.get("reposts_count", 0), r.get("total_feed_items", 0),
                r.get("icp_reasoning", ""), r.get("icp_breakdown", ""),
                r.get("note_melinda", ""), r.get("note_adrienne", ""),
                r.get("msg1", ""), r.get("msg2", ""), r.get("msg3", ""),
                r.get("google_address", address), r.get("google_phone", ""),
                r.get("google_rating", ""), r.get("google_reviews", ""),
                apollo_id, "Google Places → Apollo Org Enrich → Apollo Person Enrich → Apify Company Page + Profile + Posts → GPT-5.4 Score → Messages",
            ])

    log.info(f"Exported: {outfile}")

    # Summary
    log.info(f"\n{'=' * 60}")
    log.info("SUMMARY")
    log.info(f"{'=' * 60}")
    for r in results:
        rev = r.get("company_revenue")
        rev_fmt = f"${rev / 1e6:.0f}M" if rev and rev >= 1e6 else "N/A"
        log.info(f"  {r.get('icp_score', '?'):>3} | {r.get('first_name', '')} {r.get('last_name', '')} | {r.get('title', '')} | {r.get('company_name', '')} | {rev_fmt} | {r.get('role_verified', '')} | {r.get('activity_level', '')[:30]}")


if __name__ == "__main__":
    main()
