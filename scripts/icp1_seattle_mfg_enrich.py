#!/usr/bin/env python3
"""Full enrichment pipeline for Seattle manufacturing companies (company-first approach)."""

import os, sys, csv, time, random, json, logging
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
POSTS_ACTOR = "A3cAPGpwBEG8RJwse"
COMMENTS_ACTOR = "FiHYLewnJwS6GnRpo"

BASE = "docs/ICP-Prospects/icp1_by_industry/manufacturing"

ICP_1 = {
    "name": "VWC ICP 1 — Audit & Tax (PNW)",
    "target_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance",
                      "Director of Finance", "Owner", "President"],
    "target_seniorities": ["c_suite", "vp", "director", "owner", "founder"],
    "target_industries": ["manufacturing", "construction", "real estate",
                          "professional services", "hospitality", "nonprofit"],
    "target_locations": ["Seattle, Washington", "Washington, United States"],
    "employee_count_ranges": ["11,50", "51,200", "201,500", "501,1000"],
    "revenue_ranges": ["25000000,150000000"],
    "keywords": ["manufacturing"],
    "scoring_config": {
        "custom_notes": (
            "VWC CPAs Audit & Tax. Company-first pipeline: these companies were discovered "
            "via Google Places as real, operating manufacturing businesses in Seattle metro. "
            "Sweet spot: $50M-$100M revenue, 100-300 employees. Hard ceiling: >$150M = exclude. "
            "Exclude: public companies, PE-backed, government, banking. "
            "Priority: manufacturing is #1 industry for VWC."
        ),
        "hard_exclusions": [
            "Revenue > $150M", "Public companies", "PE-backed firms",
            "Government agencies", "Banking/financial institutions",
        ],
    },
}


def run_actor(actor_id, payload):
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
        log.warning(f"  Apify error: {str(e)[:80]}")
        return []


def parse_date(ds):
    if not ds:
        return None
    try:
        return datetime.fromisoformat(ds.replace("Z", "+00:00")).replace(tzinfo=None)
    except:
        return None


def check_activity(linkedin_url):
    slug = linkedin_url.rstrip("/").split("/")[-1] if linkedin_url else ""
    if not slug:
        return {"is_active": "Unknown", "level": "No LinkedIn URL", "posts": 0, "reposts": 0,
                "comments": 0, "recent_type": "", "recent_detail": "", "recent_date": "",
                "breakdown": "", "summary": ""}

    posts_items = run_actor(POSTS_ACTOR, {"usernames": [linkedin_url], "limit": 20})
    comments_items = run_actor(COMMENTS_ACTOR, {"maxItems": 20, "profiles": [linkedin_url]})

    now = datetime.now()
    ninety_d = now - timedelta(days=90)
    thirty_d = now - timedelta(days=30)
    six_m = now - timedelta(days=180)
    activities = []
    slug_lower = slug.lower()

    for item in posts_items:
        target = (item.get("query") or {}).get("targetUrl", "")
        if slug_lower not in target.lower():
            continue
        author_id = (item.get("author", {}).get("publicIdentifier") or "").lower()
        rb = item.get("repostedBy")
        content = (item.get("content") or "")[:200]
        pd = parse_date((item.get("postedAt") or {}).get("date", ""))
        rd = parse_date((item.get("repostedAt") or {}).get("date", "")) if item.get("repostedAt") else None
        if rb and slug_lower in (rb.get("publicIdentifier") or "").lower():
            activities.append({"date": rd or pd, "type": "Repost", "detail": f"Reposted: {content[:100]}"})
        elif slug_lower in author_id:
            activities.append({"date": pd, "type": "Original Post", "detail": content[:150] or "(image/video)"})

    for item in comments_items:
        cd = parse_date(item.get("createdAt", ""))
        if cd:
            activities.append({"date": cd, "type": "Comment", "detail": (item.get("commentary") or "")[:150]})

    activities.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

    if not activities:
        return {"is_active": "Unknown", "level": "No posts or comments found", "posts": 0,
                "reposts": 0, "comments": 0, "recent_type": "", "recent_detail": "",
                "recent_date": "", "breakdown": "", "summary": ""}

    latest = activities[0]
    ld = latest["date"]
    pc = sum(1 for a in activities if a["type"] == "Original Post")
    rc = sum(1 for a in activities if a["type"] == "Repost")
    cc = sum(1 for a in activities if a["type"] == "Comment")
    parts = []
    if cc: parts.append(f"{cc} comment{'s' if cc > 1 else ''}")
    if rc: parts.append(f"{rc} repost{'s' if rc > 1 else ''}")
    if pc: parts.append(f"{pc} post{'s' if pc > 1 else ''}")

    if ld and ld >= thirty_d:
        level = f"Active — {latest['type'].lower()} {ld.strftime('%Y-%m-%d')} ({', '.join(parts)})"
        is_active = "Yes"
    elif ld and ld >= ninety_d:
        level = f"Moderate — last {latest['type'].lower()} {ld.strftime('%Y-%m-%d')} ({', '.join(parts)})"
        is_active = "Yes"
    elif ld and ld >= six_m:
        level = f"Low — last {latest['type'].lower()} {ld.strftime('%Y-%m-%d')} ({', '.join(parts)})"
        is_active = "Somewhat"
    else:
        days = (now - ld).days if ld else 999
        level = f"Inactive — last {latest['type'].lower()} {ld.strftime('%Y-%m-%d') if ld else '?'} ({days}d ago) ({', '.join(parts)})"
        is_active = "Inactive"

    top3 = " | ".join(f"[{a['date'].strftime('%Y-%m-%d') if a['date'] else '?'}] {a['type']}: {a['detail'][:60]}" for a in activities[:3])

    return {"is_active": is_active, "level": level, "posts": pc, "reposts": rc, "comments": cc,
            "recent_type": latest["type"], "recent_detail": latest["detail"][:200],
            "recent_date": ld.strftime("%Y-%m-%d") if ld else "", "breakdown": ", ".join(parts), "summary": top3}


def main():
    with open(f"{BASE}/1_Manufacturing_merged.csv") as f:
        all_rows = list(csv.DictReader(f))

    # Seattle + has Apollo ID
    candidates = [r for r in all_rows if r.get("search_city") == "Seattle" and r.get("apollo_contact_id", "").strip()]
    log.info(f"Seattle manufacturing: {len(candidates)} enrichable companies")

    # Step 1: Apollo Enrichment
    log.info("\n" + "=" * 60)
    log.info(f"STEP 1: APOLLO ENRICHMENT ({len(candidates)} contacts)")
    log.info("=" * 60)

    enriched = []
    for i, c in enumerate(candidates):
        aid = c["apollo_contact_id"].strip()
        log.info(f"  {i+1}/{len(candidates)}: {c['company_name']} — {c.get('best_contact_name', '')}...")
        result = apollo.enrich_person(aid)
        person = result.get("person")
        if person:
            e = apollo._extract_person(person)
            e["_company_google"] = c.get("company_name", "")
            e["_address"] = c.get("address", "")
            e["_search_city"] = c.get("search_city", "")
            e["_google_domain"] = c.get("domain", "")
            e["_google_phone"] = c.get("phone", "")
            e["_google_rating"] = c.get("rating", "")
            e["_google_reviews"] = c.get("review_count", "")
            e["_zi_cfo_name"] = c.get("zi_cfo_name", "")
            e["_best_source"] = c.get("best_source", "")
            enriched.append(e)
            rev = e.get("company_revenue")
            rev_fmt = f"${rev/1e6:.0f}M" if rev and rev >= 1e6 else str(rev) if rev else "N/A"
            log.info(f"    -> {e.get('name')} | {e.get('title')} | {e.get('company_name')} | {e.get('company_industry')} | emp={e.get('company_employees')} | rev={rev_fmt}")
        else:
            log.warning(f"    -> No data returned")
        time.sleep(random.uniform(1.0, 2.0))

    log.info(f"\nEnriched: {len(enriched)}/{len(candidates)}")

    # Step 2: AI Scoring
    log.info("\n" + "=" * 60)
    log.info("STEP 2: AI SCORING")
    log.info("=" * 60)
    scores = score_prospects(enriched, ICP_1, model="gpt-4o-mini") if enriched else []
    score_map = {s["apollo_id"]: s for s in scores}
    enriched.sort(key=lambda p: score_map.get(p.get("apollo_id"), {}).get("score", 0), reverse=True)
    above_60 = sum(1 for p in enriched if score_map.get(p.get("apollo_id"), {}).get("score", 0) >= 60)
    log.info(f"Scored: {len(scores)} | Above 60: {above_60}")

    # Step 3: LinkedIn Activity
    log.info("\n" + "=" * 60)
    log.info("STEP 3: LINKEDIN ACTIVITY (posts + comments)")
    log.info("=" * 60)
    activity_map = {}
    for i, p in enumerate(enriched):
        name = p.get("name", "?")
        li_url = p.get("linkedin_url", "")
        log.info(f"  {i+1}/{len(enriched)}: {name}...")
        act = check_activity(li_url)
        activity_map[p["apollo_id"]] = act
        log.info(f"    {act['is_active']} | {act['level'][:80]}")

    # Step 4: Messages
    log.info("\n" + "=" * 60)
    log.info("STEP 4: CONNECTION NOTES + MESSAGES")
    log.info("=" * 60)
    notes_map = {}
    msgs_map = {}
    for i, p in enumerate(enriched):
        pd = {"first_name": p.get("first_name", ""), "last_name": p.get("last_name", ""),
              "title": p.get("title", ""), "company_name": p.get("company_name", ""),
              "location": f"{p.get('city', '')}, {p.get('state', '')}".strip(", "),
              "headline": p.get("headline", ""),
              "icp_reasoning": score_map.get(p.get("apollo_id"), {}).get("reasoning", "")}
        cd = {"industry": p.get("company_industry"), "employee_count_range": p.get("company_employee_range")}
        na = generate_connection_note(pd, cd, "Adrienne Nordland")
        nm = generate_connection_note(pd, cd, "Melinda Johnson")
        msgs = generate_messages(pd, cd, None)
        notes_map[p["apollo_id"]] = {"adrienne": na, "melinda": nm}
        msgs_map[p["apollo_id"]] = msgs
        log.info(f"  {i+1}/{len(enriched)}: {p.get('name')} — done")
        time.sleep(0.3)

    # Step 5: Export
    log.info("\n" + "=" * 60)
    log.info("STEP 5: EXPORT")
    log.info("=" * 60)

    outfile = f"{BASE}/seattle_manufacturing_enriched.csv"
    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ICP Score", "First Name", "Last Name", "Title", "Company",
            "Industry", "Employees", "Revenue", "Location", "State",
            "Email", "Email Status", "LinkedIn URL",
            "Source", "ZoomInfo ID", "Apollo ID",
            "Is Active on LinkedIn", "Activity Level", "Authored Posts",
            "Last Authored Post Date", "Last Authored Post Text", "Post Reactions",
            "Latest Feed Date", "Feed Items Count",
            "ICP Reasoning", "ICP Score Breakdown",
            "Melinda's Connection Note", "Adrienne's Connection Note",
            "Message 1 (after connect)", "Message 2 (2 weeks)", "Message 3 (4 weeks)",
            "Seniority", "Headline", "Company Website", "Company Domain",
            "Recent Activity Type", "Recent Activity Detail",
            "Posts Count", "Reposts Count", "Comments Count",
            "Activity Type Breakdown", "Activity Summary (Top 3)",
            # Company-first extras
            "Google Places Address", "Google Phone", "Google Rating", "Google Reviews",
            "ZoomInfo CFO Name", "Data Source Pipeline",
        ])

        for p in enriched:
            aid = p.get("apollo_id", "")
            s = score_map.get(aid, {})
            act = activity_map.get(aid, {})
            notes = notes_map.get(aid, {})
            msgs = msgs_map.get(aid, [])
            rev = p.get("company_revenue")
            rev_fmt = f"${rev/1e9:.1f}B" if rev and rev >= 1e9 else (f"${rev/1e6:.0f}M" if rev and rev >= 1e6 else "")
            bd = " | ".join(f"{k}: {v}" for k, v in s.get("breakdown", {}).items())

            writer.writerow([
                s.get("score", 0), p.get("first_name", ""), p.get("last_name", ""),
                p.get("title", ""), p.get("company_name", ""), p.get("company_industry", ""),
                p.get("company_employees", ""), rev_fmt,
                f"{p.get('city', '')}, {p.get('state', '')}".strip(", "), p.get("state", ""),
                p.get("email", ""), (p.get("raw_person") or {}).get("email_status", ""),
                p.get("linkedin_url", ""), p.get("_best_source", ""), "", aid,
                act.get("is_active", ""), act.get("level", ""),
                act.get("posts", 0) + act.get("reposts", 0),
                act.get("recent_date", ""), act.get("recent_detail", ""), "",
                act.get("recent_date", ""),
                act.get("posts", 0) + act.get("reposts", 0) + act.get("comments", 0),
                s.get("reasoning", ""), bd,
                notes.get("melinda", ""), notes.get("adrienne", ""),
                msgs[0]["text"] if len(msgs) > 0 else "",
                msgs[1]["text"] if len(msgs) > 1 else "",
                msgs[2]["text"] if len(msgs) > 2 else "",
                p.get("seniority", ""), p.get("headline", ""),
                p.get("company_website", ""), p.get("company_domain", ""),
                act.get("recent_type", ""), act.get("recent_detail", ""),
                act.get("posts", 0), act.get("reposts", 0), act.get("comments", 0),
                act.get("breakdown", ""), act.get("summary", ""),
                p.get("_address", ""), p.get("_google_phone", ""),
                p.get("_google_rating", ""), p.get("_google_reviews", ""),
                p.get("_zi_cfo_name", ""), "Google Places → ZoomInfo/Apollo → Enriched → Scored",
            ])

    log.info(f"Exported: {outfile}")

    # Summary
    log.info("\n" + "=" * 60)
    log.info("SUMMARY — Seattle Manufacturing")
    log.info("=" * 60)
    log.info(f"  Companies enriched: {len(enriched)}")
    log.info(f"  Credits used: {len(candidates)} Apollo")
    active = sum(1 for a in activity_map.values() if a.get("is_active") in ("Yes", "Somewhat"))
    log.info(f"  LinkedIn active: {active}/{len(enriched)}")
    log.info(f"  ICP Score >= 60: {above_60}/{len(enriched)}")
    avg = sum(score_map.get(p["apollo_id"], {}).get("score", 0) for p in enriched) / len(enriched) if enriched else 0
    log.info(f"  Avg ICP Score: {avg:.1f}")


if __name__ == "__main__":
    main()
