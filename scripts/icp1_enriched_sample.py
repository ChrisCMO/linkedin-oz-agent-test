#!/usr/bin/env python3
"""Enrich 25 ICP1 prospects (9 WA, 8 OR, 8 ID), score, check activity, generate messages.
Output: output/icp1_enriched_prospect_sample.csv"""

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

# ICP config for scoring
ICP_1_CONFIG = {
    "name": "VWC ICP 1 — Audit & Tax (PNW)",
    "target_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance",
                      "Director of Finance", "Owner", "President"],
    "target_seniorities": ["c_suite", "vp", "director", "owner", "founder"],
    "target_industries": ["manufacturing", "construction", "real estate",
                          "professional services", "hospitality", "nonprofit"],
    "target_locations": ["Washington, United States", "Oregon, United States", "Idaho, United States"],
    "employee_count_ranges": ["11,50", "51,200", "201,500", "501,1000"],
    "revenue_ranges": ["25000000,150000000"],
    "keywords": ["manufacturing", "construction", "real estate", "professional services",
                 "hospitality", "nonprofit"],
    "scoring_config": {
        "custom_notes": (
            "VWC CPAs Audit & Tax. Sweet spot: $50M-$100M revenue, 100-300 employees "
            "in Seattle/PNW. Hard ceiling: >$150M revenue = exclude. Priority industries: "
            "manufacturing (#1), commercial RE (#2), professional services (#3), "
            "hospitality (#4), nonprofit (#5), construction (#6). "
            "Exclude: public companies, PE-backed, government, banking/financial institutions."
        ),
        "hard_exclusions": [
            "Revenue > $150M", "Public companies", "PE-backed firms",
            "Government agencies", "Banking/financial institutions",
        ],
    },
}

# ICP title keywords for selection
ICP_TITLES = ["cfo", "chief financial officer", "controller", "vp finance",
              "vice president finance", "director of finance", "vp of finance"]

# ICP industry keywords to prefer
ICP_INDUSTRIES = ["manufactur", "construct", "real estate", "professional service",
                  "hospitality", "hotel", "nonprofit", "non-profit"]


def select_best_candidates(rows, state, count):
    """Select the best ICP-matching candidates from a state."""
    # Filter: must have apollo_id and ICP-matching title
    candidates = [
        r for r in rows
        if r["state"] == state
        and r.get("apollo_id", "").strip()
        and any(t in r["title"].lower() for t in ICP_TITLES)
    ]

    # Prefer ZoomInfo-matched (cross-source) first, then Apollo-only
    zi_matched = [r for r in candidates if r["source"] == "ZoomInfo"]
    apollo_only = [r for r in candidates if r["source"] == "Apollo"]

    # Prefer candidates with has_revenue or has_employees
    def rank(r):
        score = 0
        if r.get("has_revenue") in ("True", True):
            score += 2
        if r.get("has_employees") in ("True", True):
            score += 1
        # Prefer ICP industry keywords in company name
        co = r.get("company", "").lower()
        if any(ind in co for ind in ICP_INDUSTRIES):
            score += 3
        return score

    zi_matched.sort(key=rank, reverse=True)
    apollo_only.sort(key=rank, reverse=True)

    # Mix: prefer ZoomInfo-matched (better data validation), fill with Apollo
    selected = []
    zi_take = min(len(zi_matched), count // 2 + 1)
    selected.extend(zi_matched[:zi_take])
    remaining = count - len(selected)
    selected.extend(apollo_only[:remaining])

    return selected[:count]


def check_linkedin_activity(linkedin_url):
    """Run Apify posts scraper and return activity summary."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        f"https://api.apify.com/v2/acts/{POSTS_ACTOR_ID}/runs",
        headers=headers,
        json={"profileUrl": linkedin_url, "maxPosts": 10},
    )
    if r.status_code != 201:
        return {"is_active": False, "level": "unknown", "recent_post": None, "total_posts": 0}

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
        return {"is_active": False, "level": "unknown", "recent_post": None, "total_posts": 0}

    items = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers).json()

    now = datetime.now()
    thirty_days = now - timedelta(days=30)
    ninety_days = now - timedelta(days=90)
    last_date = None
    recent_30 = 0
    recent_90 = 0

    for item in items:
        posted = item.get("posted_at", {}).get("date")
        if posted:
            try:
                dt = datetime.strptime(posted, "%Y-%m-%d %H:%M:%S")
                if last_date is None or dt > last_date:
                    last_date = dt
                if dt >= thirty_days:
                    recent_30 += 1
                if dt >= ninety_days:
                    recent_90 += 1
            except ValueError:
                pass

    if recent_30 > 0:
        level = "Active"
    elif recent_90 > 0:
        level = "Moderate"
    else:
        level = "Inactive"

    return {
        "is_active": level in ("Active", "Moderate"),
        "level": level,
        "recent_post": last_date.strftime("%Y-%m-%d") if last_date else None,
        "total_posts": len(items),
        "posts_30d": recent_30,
        "posts_90d": recent_90,
    }


def main():
    # Read pool CSV
    with open("output/icp1_full_prospect_pool.csv") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.info(f"Pool: {len(rows)} total contacts")

    # Select candidates: 9 WA, 8 OR, 8 ID
    wa_picks = select_best_candidates(rows, "Washington", 9)
    or_picks = select_best_candidates(rows, "Oregon", 8)
    id_picks = select_best_candidates(rows, "Idaho", 8)
    candidates = wa_picks + or_picks + id_picks

    log.info(f"Selected {len(candidates)} candidates: WA={len(wa_picks)}, OR={len(or_picks)}, ID={len(id_picks)}")
    for c in candidates:
        log.info(f"  {c['state'][:2]} | {c['first_name']} {c['last_name']} | {c['title'][:40]} | {c['company']} | src={c['source']}")

    # Step 1: Enrich via Apollo (25 credits)
    log.info("")
    log.info("=" * 60)
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
            enriched.append(extracted)
            log.info(f"    -> {extracted.get('name')} | {extracted.get('company_industry')} | emp={extracted.get('company_employees')} | rev={extracted.get('company_revenue')}")
        else:
            log.warning(f"    -> No enrichment data returned")
        time.sleep(random.uniform(1.0, 2.0))

    log.info(f"\nEnriched {len(enriched)} / {len(candidates)} prospects")

    # Step 2: AI Scoring
    log.info("")
    log.info("=" * 60)
    log.info("STEP 2: AI SCORING")
    log.info("=" * 60)

    scores = score_prospects(enriched, ICP_1_CONFIG, model="gpt-4o-mini")
    score_map = {s["apollo_id"]: s for s in scores}
    log.info(f"Scored {len(scores)} prospects")

    # Sort by score desc
    enriched.sort(key=lambda p: score_map.get(p.get("apollo_id"), {}).get("score", 0), reverse=True)

    for p in enriched[:5]:
        s = score_map.get(p.get("apollo_id"), {})
        log.info(f"  {s.get('score', 0):>3} | {p.get('name')} | {p.get('title')} | {p.get('company_name')}")

    # Step 3: Apify LinkedIn Activity
    log.info("")
    log.info("=" * 60)
    log.info("STEP 3: LINKEDIN ACTIVITY CHECK (Apify)")
    log.info("=" * 60)

    activity_map = {}
    for i, p in enumerate(enriched):
        name = p.get("name", "?")
        li_url = p.get("linkedin_url")
        if not li_url:
            log.info(f"  {i+1}/{len(enriched)}: {name} — no LinkedIn URL, skipping")
            activity_map[p["apollo_id"]] = {"is_active": False, "level": "No URL", "recent_post": None, "total_posts": 0}
            continue

        log.info(f"  {i+1}/{len(enriched)}: {name} — checking...")
        activity = check_linkedin_activity(li_url)
        activity_map[p["apollo_id"]] = activity
        log.info(f"    -> {activity['level']} | {activity['total_posts']} posts | last: {activity.get('recent_post', 'N/A')}")

    # Step 4: Generate Messages
    log.info("")
    log.info("=" * 60)
    log.info("STEP 4: GENERATING CONNECTION NOTES + MESSAGES")
    log.info("=" * 60)

    messages_map = {}
    notes_map = {}

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

        log.info(f"    Adrienne: {note_a[:80]}...")
        log.info(f"    Melinda:  {note_m[:80]}...")
        time.sleep(0.3)

    # Step 5: Export CSV
    log.info("")
    log.info("=" * 60)
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
            "Is Active", "Activity Level", "Recent Post", "Total Posts",
            "ICP Reasoning", "ICP Score Breakdown",
            "Melinda Connection Note", "Adrienne Connection Note",
            "Message 1", "Message 2", "Message 3",
            "Seniority", "Headline", "Company Website", "Company Domain",
        ])

        for p in enriched:
            aid = p.get("apollo_id", "")
            s = score_map.get(aid, {})
            act = activity_map.get(aid, {})
            notes = notes_map.get(aid, {})
            msgs = messages_map.get(aid, [])

            # Format revenue
            rev = p.get("company_revenue")
            if rev and rev >= 1_000_000:
                rev_fmt = f"${rev / 1_000_000:.0f}M"
            elif rev and rev >= 1_000:
                rev_fmt = f"${rev / 1_000:.0f}K"
            elif rev:
                rev_fmt = f"${rev:.0f}"
            else:
                rev_fmt = ""

            # Source label
            src = p.get("_source", "apollo")
            if src == "zoominfo":
                source_label = "ZoomInfo (Apollo matched)"
            else:
                source_label = "Apollo"

            # Score breakdown as readable string
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
                act.get("recent_post", ""),
                act.get("total_posts", 0),
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
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info(f"  Total enriched: {len(enriched)}")
    log.info(f"  Credits used: {len(candidates)} (Apollo enrichment)")
    active_count = sum(1 for a in activity_map.values() if a.get("is_active"))
    log.info(f"  LinkedIn active: {active_count}/{len(enriched)}")
    avg_score = sum(score_map.get(p["apollo_id"], {}).get("score", 0) for p in enriched) / len(enriched) if enriched else 0
    log.info(f"  Avg ICP score: {avg_score:.1f}")
    log.info(f"  WA: {sum(1 for p in enriched if p.get('_state') == 'Washington')}")
    log.info(f"  OR: {sum(1 for p in enriched if p.get('_state') == 'Oregon')}")
    log.info(f"  ID: {sum(1 for p in enriched if p.get('_state') == 'Idaho')}")


if __name__ == "__main__":
    main()
