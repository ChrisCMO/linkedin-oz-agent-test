#!/usr/bin/env python3
"""Enriched ICP1 sample v2: 13 WA + 12 OR, fixed Apify authored-post detection."""

import os, sys, csv, json, time, random, logging
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
POSTS_ACTOR_ID = "LQQIXN9Othf8f7R5n"

ICP_1_CONFIG = {
    "name": "VWC ICP 1 — Audit & Tax (PNW)",
    "target_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance",
                      "Director of Finance", "Owner", "President"],
    "target_seniorities": ["c_suite", "vp", "director", "owner", "founder"],
    "target_industries": ["manufacturing", "construction", "real estate",
                          "professional services", "hospitality", "nonprofit"],
    "target_locations": ["Washington, United States", "Oregon, United States"],
    "employee_count_ranges": ["11,50", "51,200", "201,500", "501,1000"],
    "revenue_ranges": ["25000000,150000000"],
    "keywords": ["manufacturing", "construction", "real estate", "professional services",
                 "hospitality", "nonprofit"],
    "scoring_config": {
        "custom_notes": (
            "VWC CPAs Audit & Tax. Sweet spot: $50M-$100M revenue, 100-300 employees "
            "in Seattle/PNW (Washington State and Oregon only). "
            "Hard ceiling: >$150M revenue = exclude. Priority industries: "
            "manufacturing (#1), commercial RE (#2), professional services (#3), "
            "hospitality (#4), nonprofit (#5), construction (#6). "
            "Exclude: public companies, PE-backed, government, banking/financial institutions, "
            "insurance companies, credit unions."
        ),
        "hard_exclusions": [
            "Revenue > $150M", "Public companies", "PE-backed firms",
            "Government agencies", "Banking/financial institutions",
            "Insurance companies", "Credit unions",
        ],
    },
}

ICP_TITLES = ["cfo", "chief financial officer", "controller", "vp finance",
              "vice president finance", "director of finance", "vp of finance"]

# Industries to AVOID (hard exclusions)
BAD_INDUSTRIES = ["bank", "credit union", "insurance", "government", "public"]
BAD_COMPANIES = ["bank", "credit union", "insurance", "government"]


def select_candidates(rows, state, count, already_used_apollo_ids):
    """Select ICP-matching candidates from a state, avoiding previously enriched ones."""
    candidates = [
        r for r in rows
        if r["state"] == state
        and r.get("apollo_id", "").strip()
        and r["apollo_id"] not in already_used_apollo_ids
        and any(t in r["title"].lower() for t in ICP_TITLES)
        # Exclude obvious bad matches by company name
        and not any(bad in r["company"].lower() for bad in BAD_COMPANIES)
    ]

    # Score candidates for selection priority
    def rank(r):
        score = 0
        co = r.get("company", "").lower()
        title = r.get("title", "").lower()
        # Prefer construction, manufacturing, real estate
        for kw in ["construct", "manufactur", "real estate", "hospitality", "hotel"]:
            if kw in co:
                score += 5
        # Prefer CFO/Controller over Owner/President
        if "cfo" in title or "chief financial" in title:
            score += 3
        elif "controller" in title:
            score += 2
        # Prefer those with revenue/employee data
        if r.get("has_revenue") in ("True", True):
            score += 1
        if r.get("has_employees") in ("True", True):
            score += 1
        # Prefer ZoomInfo-matched (cross-validated)
        if r["source"] == "ZoomInfo":
            score += 2
        return score

    candidates.sort(key=rank, reverse=True)
    return candidates[:count]


def check_linkedin_activity(linkedin_url, linkedin_slug):
    """Run Apify posts scraper and analyze AUTHORED posts only."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        f"https://api.apify.com/v2/acts/{POSTS_ACTOR_ID}/runs",
        headers=headers,
        json={"profileUrl": linkedin_url, "maxPosts": 10},
    )
    if r.status_code != 201:
        return {
            "is_active": False, "level": "Unknown", "recent_post_date": None,
            "recent_post_text": None, "authored_posts": 0, "feed_posts": 0,
        }

    run_data = r.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]

    for _ in range(30):
        time.sleep(5)
        sr = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED"):
            break

    if status != "SUCCEEDED":
        return {
            "is_active": False, "level": "Unknown", "recent_post_date": None,
            "recent_post_text": None, "authored_posts": 0, "feed_posts": 0,
        }

    items = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers).json()

    now = datetime.now()
    thirty_days = now - timedelta(days=30)
    ninety_days = now - timedelta(days=90)

    # Separate authored posts from feed activity
    authored = []
    feed_activity_date = None

    slug_lower = (linkedin_slug or "").lower().strip("/")

    for item in items:
        author = item.get("author", {})
        author_username = (author.get("username") or "").lower().strip("/")
        author_profile = (author.get("profile_url") or "").lower().rstrip("/")

        posted = item.get("posted_at", {}).get("date")
        post_date = None
        if posted:
            try:
                post_date = datetime.strptime(posted, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        # Track latest feed activity regardless
        if post_date:
            if feed_activity_date is None or post_date > feed_activity_date:
                feed_activity_date = post_date

        # Check if this is an authored post (by the prospect, not someone they follow)
        is_authored = False
        if slug_lower and (slug_lower in author_username or slug_lower in author_profile):
            is_authored = True

        if is_authored and post_date:
            authored.append({
                "date": post_date,
                "text": (item.get("text") or "")[:200],
                "type": item.get("post_type", "regular"),
                "reactions": (item.get("stats") or {}).get("total_reactions", 0),
            })

    authored.sort(key=lambda x: x["date"], reverse=True)

    # Determine activity level based on AUTHORED posts
    recent_authored = None
    if authored:
        recent_authored = authored[0]

    if recent_authored and recent_authored["date"] >= thirty_days:
        level = "Active (posted <30d)"
    elif recent_authored and recent_authored["date"] >= ninety_days:
        level = "Moderate (posted <90d)"
    elif authored:
        level = f"Inactive (last post {authored[0]['date'].strftime('%Y-%m-%d')})"
    elif feed_activity_date and feed_activity_date >= thirty_days:
        level = "Feed active (no authored posts)"
    else:
        level = "Inactive (no posts found)"

    return {
        "is_active": bool(recent_authored and recent_authored["date"] >= ninety_days),
        "level": level,
        "recent_post_date": recent_authored["date"].strftime("%Y-%m-%d") if recent_authored else None,
        "recent_post_text": recent_authored["text"] if recent_authored else None,
        "recent_post_reactions": recent_authored["reactions"] if recent_authored else 0,
        "authored_posts": len(authored),
        "feed_posts": len(items),
        "feed_last_date": feed_activity_date.strftime("%Y-%m-%d") if feed_activity_date else None,
    }


def main():
    # Read pool CSV
    with open("output/icp1_full_prospect_pool.csv") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.info(f"Pool: {len(rows)} total contacts")

    # Get previously enriched apollo_ids to avoid duplicates
    prev_enriched = set()
    if os.path.exists("output/icp1_enriched_prospect_sample.csv"):
        with open("output/icp1_enriched_prospect_sample.csv") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get("Apollo ID"):
                    prev_enriched.add(r["Apollo ID"])
    log.info(f"Previously enriched: {len(prev_enriched)} (will skip)")

    # Select: 13 WA + 12 OR (no Idaho)
    wa_picks = select_candidates(rows, "Washington", 13, prev_enriched)
    or_picks = select_candidates(rows, "Oregon", 12, prev_enriched)
    candidates = wa_picks + or_picks

    log.info(f"Selected {len(candidates)} candidates: WA={len(wa_picks)}, OR={len(or_picks)}")
    for c in candidates:
        log.info(f"  {c['state'][:2]} | {c['first_name']} {c['last_name']} | {c['title'][:50]} | {c['company']} | src={c['source']}")

    # Step 1: Enrich
    log.info("\n" + "=" * 60)
    log.info("STEP 1: APOLLO ENRICHMENT (25 credits)")
    log.info("=" * 60)

    enriched = []
    for i, c in enumerate(candidates):
        log.info(f"  Enriching {i+1}/{len(candidates)}: {c['first_name']} {c['last_name']} @ {c['company']}...")
        result = apollo.enrich_person(c["apollo_id"])
        person = result.get("person")
        if person:
            extracted = apollo._extract_person(person)
            extracted["_source"] = c["source"]
            extracted["_zi_score"] = c.get("accuracy_score", "")
            extracted["_zi_id"] = c.get("zoominfo_id", "")
            extracted["_state"] = c["state"]
            # Get LinkedIn slug for authored post matching
            li_url = extracted.get("linkedin_url", "")
            extracted["_li_slug"] = li_url.rstrip("/").split("/")[-1] if li_url else ""
            enriched.append(extracted)
            log.info(f"    -> {extracted.get('name')} | {extracted.get('company_industry')} | emp={extracted.get('company_employees')} | rev={extracted.get('company_revenue')} | slug={extracted['_li_slug']}")
        else:
            log.warning(f"    -> No data returned")
        time.sleep(random.uniform(1.0, 2.0))

    log.info(f"\nEnriched {len(enriched)} / {len(candidates)}")

    # Step 2: Score
    log.info("\n" + "=" * 60)
    log.info("STEP 2: AI SCORING (GPT-4o-mini)")
    log.info("=" * 60)

    scores = score_prospects(enriched, ICP_1_CONFIG, model="gpt-4o-mini")
    score_map = {s["apollo_id"]: s for s in scores}
    enriched.sort(key=lambda p: score_map.get(p.get("apollo_id"), {}).get("score", 0), reverse=True)

    above_60 = sum(1 for p in enriched if score_map.get(p.get("apollo_id"), {}).get("score", 0) >= 60)
    log.info(f"Scored {len(scores)} | Above 60: {above_60} | Above 50: {sum(1 for p in enriched if score_map.get(p.get('apollo_id'), {}).get('score', 0) >= 50)}")
    for p in enriched[:8]:
        s = score_map.get(p.get("apollo_id"), {})
        log.info(f"  {s.get('score', 0):>3} | {p.get('name')} | {p.get('title')} | {p.get('company_name')} | rev={p.get('company_revenue')}")

    # Step 3: Apify — FIXED to detect authored posts vs feed
    log.info("\n" + "=" * 60)
    log.info("STEP 3: LINKEDIN ACTIVITY (Apify — authored posts only)")
    log.info("=" * 60)

    activity_map = {}
    for i, p in enumerate(enriched):
        name = p.get("name", "?")
        li_url = p.get("linkedin_url")
        li_slug = p.get("_li_slug", "")

        if not li_url:
            log.info(f"  {i+1}/{len(enriched)}: {name} — no LinkedIn URL")
            activity_map[p["apollo_id"]] = {
                "is_active": False, "level": "No LinkedIn URL",
                "recent_post_date": None, "recent_post_text": None,
                "authored_posts": 0, "feed_posts": 0,
            }
            continue

        log.info(f"  {i+1}/{len(enriched)}: {name} (slug: {li_slug})...")
        activity = check_linkedin_activity(li_url, li_slug)
        activity_map[p["apollo_id"]] = activity
        log.info(f"    -> {activity['level']} | authored: {activity['authored_posts']} | feed: {activity['feed_posts']} | recent: {activity.get('recent_post_date', 'N/A')}")
        if activity.get("recent_post_text"):
            log.info(f"    -> Post: {activity['recent_post_text'][:100]}...")

    # Step 4: Messages
    log.info("\n" + "=" * 60)
    log.info("STEP 4: CONNECTION NOTES + MESSAGES (Adrienne/Melinda → prospect)")
    log.info("=" * 60)

    notes_map = {}
    messages_map = {}
    for i, p in enumerate(enriched):
        name = p.get("name", "?")
        log.info(f"  {i+1}/{len(enriched)}: {name}")

        prospect_dict = {
            "first_name": p.get("first_name", ""),
            "last_name": p.get("last_name", ""),
            "title": p.get("title", ""),
            "company_name": p.get("company_name", ""),
            "location": f"{p.get('city', '')}, {p.get('state', '')}".strip(", "),
            "headline": p.get("headline", ""),
            "icp_reasoning": score_map.get(p.get("apollo_id"), {}).get("reasoning", ""),
        }
        company_dict = {
            "industry": p.get("company_industry"),
            "employee_count_range": p.get("company_employee_range"),
        }

        note_a = generate_connection_note(prospect_dict, company_dict, "Adrienne Nordland")
        note_m = generate_connection_note(prospect_dict, company_dict, "Melinda Johnson")
        msgs = generate_messages(prospect_dict, company_dict, None)

        notes_map[p["apollo_id"]] = {"adrienne": note_a, "melinda": note_m}
        messages_map[p["apollo_id"]] = msgs
        time.sleep(0.3)

    # Step 5: Export
    log.info("\n" + "=" * 60)
    log.info("STEP 5: EXPORTING CSV")
    log.info("=" * 60)

    os.makedirs("output", exist_ok=True)
    outfile = "output/icp1_enriched_prospect_sample.csv"

    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ICP Score", "First Name", "Last Name", "Title", "Company",
            "Industry", "Employees", "Revenue", "Location", "State",
            "Email", "Email Status", "LinkedIn URL",
            "Source", "ZoomInfo ID", "Apollo ID",
            "Is Active on LinkedIn", "Activity Level", "Authored Posts Found",
            "Recent Post Date", "Recent Post Preview", "Recent Post Reactions",
            "Feed Activity Date", "Total Feed Items",
            "ICP Reasoning", "ICP Score Breakdown",
            "Melinda's Connection Note", "Adrienne's Connection Note",
            "Message 1 (after connect)", "Message 2 (2 weeks)", "Message 3 (4 weeks)",
            "Seniority", "Headline", "Company Website", "Company Domain",
        ])

        for p in enriched:
            aid = p.get("apollo_id", "")
            s = score_map.get(aid, {})
            act = activity_map.get(aid, {})
            notes = notes_map.get(aid, {})
            msgs = messages_map.get(aid, [])

            rev = p.get("company_revenue")
            if rev and rev >= 1_000_000_000:
                rev_fmt = f"${rev / 1_000_000_000:.1f}B"
            elif rev and rev >= 1_000_000:
                rev_fmt = f"${rev / 1_000_000:.0f}M"
            elif rev and rev >= 1_000:
                rev_fmt = f"${rev / 1_000:.0f}K"
            elif rev:
                rev_fmt = f"${rev:.0f}"
            else:
                rev_fmt = ""

            src = p.get("_source", "apollo")
            source_label = "ZoomInfo (Apollo matched)" if src == "zoominfo" else "Apollo"

            breakdown = s.get("breakdown", {})
            breakdown_str = " | ".join(f"{k}: {v}" for k, v in breakdown.items()) if breakdown else ""

            writer.writerow([
                s.get("score", 0),
                p.get("first_name", ""),
                p.get("last_name", ""),
                p.get("title", ""),
                p.get("company_name", ""),
                p.get("company_industry", ""),
                p.get("company_employees", ""),
                rev_fmt,
                f"{p.get('city', '')}, {p.get('state', '')}".strip(", "),
                p.get("_state", ""),
                p.get("email", ""),
                p.get("raw_person", {}).get("email_status", "") if p.get("raw_person") else "",
                p.get("linkedin_url", ""),
                source_label,
                p.get("_zi_id", ""),
                aid,
                "Yes" if act.get("is_active") else "No",
                act.get("level", ""),
                act.get("authored_posts", 0),
                act.get("recent_post_date", ""),
                act.get("recent_post_text", ""),
                act.get("recent_post_reactions", 0),
                act.get("feed_last_date", ""),
                act.get("feed_posts", 0),
                s.get("reasoning", ""),
                breakdown_str,
                notes.get("melinda", ""),
                notes.get("adrienne", ""),
                msgs[0]["text"] if len(msgs) > 0 else "",
                msgs[1]["text"] if len(msgs) > 1 else "",
                msgs[2]["text"] if len(msgs) > 2 else "",
                p.get("seniority", ""),
                p.get("headline", ""),
                p.get("company_website", ""),
                p.get("company_domain", ""),
            ])

    log.info(f"Exported to: {outfile}")

    # Summary
    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info(f"  Total enriched: {len(enriched)}")
    log.info(f"  Credits used: {len(candidates)} Apollo enrichment")
    active = sum(1 for a in activity_map.values() if a.get("is_active"))
    authored = sum(1 for a in activity_map.values() if a.get("authored_posts", 0) > 0)
    log.info(f"  LinkedIn active (authored posts <90d): {active}/{len(enriched)}")
    log.info(f"  Has authored posts: {authored}/{len(enriched)}")
    above_60 = sum(1 for p in enriched if score_map.get(p["apollo_id"], {}).get("score", 0) >= 60)
    above_50 = sum(1 for p in enriched if score_map.get(p["apollo_id"], {}).get("score", 0) >= 50)
    avg = sum(score_map.get(p["apollo_id"], {}).get("score", 0) for p in enriched) / len(enriched) if enriched else 0
    log.info(f"  ICP Score >= 60: {above_60}/{len(enriched)}")
    log.info(f"  ICP Score >= 50: {above_50}/{len(enriched)}")
    log.info(f"  Avg ICP Score: {avg:.1f}")
    log.info(f"  WA: {sum(1 for p in enriched if p.get('_state') == 'Washington')}")
    log.info(f"  OR: {sum(1 for p in enriched if p.get('_state') == 'Oregon')}")


if __name__ == "__main__":
    main()
