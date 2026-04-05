#!/usr/bin/env python3
"""Run Apify activity checks + generate connection notes & messages for ICP 1 top prospects."""

import sys, os, json, time, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timedelta
from db.connect import get_supabase
from mvp.backend.services.message_gen_svc import generate_connection_note, generate_messages

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

sb = get_supabase()
APIFY_TOKEN = os.environ["APIFY_API_KEY"]
POSTS_ACTOR_ID = "LQQIXN9Othf8f7R5n"
CAMPAIGN_ID = "75d1b4ad-e1c9-48e7-a1bd-91c8e29f4337"


def check_activity(linkedin_url):
    """Run Apify posts scraper and return activity summary."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(
        f"https://api.apify.com/v2/acts/{POSTS_ACTOR_ID}/runs",
        headers=headers,
        json={"profileUrl": linkedin_url, "maxPosts": 10},
    )
    if r.status_code != 201:
        return {"level": "unknown", "total_posts": 0, "last_activity_date": None, "authored_posts": 0}

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
        return {"level": "unknown", "total_posts": 0, "last_activity_date": None, "authored_posts": 0}

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
        level = "active"
    elif recent_90 > 0:
        level = "moderate"
    else:
        level = "inactive"

    return {
        "level": level,
        "total_posts": len(items),
        "recent_30_days": recent_30,
        "recent_90_days": recent_90,
        "last_activity_date": last_date.strftime("%Y-%m-%d") if last_date else None,
        "authored_posts": len(items),
    }


def main():
    # Get top 10 ICP 1 prospects with score >= 70
    resp = (
        sb.table("prospects")
        .select("id, first_name, last_name, title, company_name, company_domain, location, linkedin_url, icp_score, icp_reasoning, raw_apollo_data")
        .eq("campaign_id", CAMPAIGN_ID)
        .gte("icp_score", 70)
        .order("icp_score", desc=True)
        .limit(10)
        .execute()
    )
    prospects = resp.data
    log.info(f"Processing {len(prospects)} ICP 1 prospects (score >= 70)")

    # --- APIFY ACTIVITY CHECKS ---
    log.info("=" * 60)
    log.info("APIFY LINKEDIN ACTIVITY CHECKS")
    log.info("=" * 60)

    for i, p in enumerate(prospects):
        name = f"{p['first_name']} {p['last_name']}"
        log.info(f"  {i+1}/{len(prospects)}: {name} — checking LinkedIn activity...")

        activity = check_activity(p["linkedin_url"])
        log.info(f"    -> {activity['level']} | {activity['total_posts']} posts | last: {activity.get('last_activity_date', 'N/A')}")

        raw = p.get("raw_apollo_data") or {}
        if isinstance(raw, str):
            raw = json.loads(raw)
        raw["linkedin_activity"] = activity
        sb.table("prospects").update({"raw_apollo_data": raw}).eq("id", p["id"]).execute()

    # Reload prospects with updated raw_apollo_data
    resp = (
        sb.table("prospects")
        .select("id, first_name, last_name, title, company_name, company_domain, location, linkedin_url, icp_score, icp_reasoning, raw_apollo_data")
        .eq("campaign_id", CAMPAIGN_ID)
        .gte("icp_score", 70)
        .order("icp_score", desc=True)
        .limit(10)
        .execute()
    )
    prospects = resp.data

    # --- CONNECTION NOTES + MESSAGES ---
    log.info("")
    log.info("=" * 60)
    log.info("GENERATING CONNECTION NOTES + 3-MESSAGE SEQUENCES")
    log.info("(From Adrienne/Melinda TO each prospect)")
    log.info("=" * 60)

    for i, p in enumerate(prospects):
        name = f"{p['first_name']} {p['last_name']}"
        log.info(f"\n  {i+1}/{len(prospects)}: {name} @ {p['company_name']}")

        raw = p.get("raw_apollo_data") or {}
        if isinstance(raw, str):
            raw = json.loads(raw)

        prospect_dict = {
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "title": p["title"],
            "company_name": p["company_name"],
            "location": p["location"],
            "headline": p.get("title", ""),
            "icp_reasoning": p.get("icp_reasoning", ""),
        }
        company_dict = {
            "industry": raw.get("company_industry"),
            "employee_count_range": raw.get("company_employee_range"),
        }

        # Connection notes — Adrienne and Melinda writing TO the prospect
        note_adrienne = generate_connection_note(prospect_dict, company_dict, "Adrienne Nordland")
        note_melinda = generate_connection_note(prospect_dict, company_dict, "Melinda Johnson")

        log.info(f"    [Adrienne -> {p['first_name']}]: {note_adrienne}")
        log.info(f"    [Melinda  -> {p['first_name']}]: {note_melinda}")

        # 3-message follow-up sequence
        msgs = generate_messages(prospect_dict, company_dict, None)
        for m in msgs:
            log.info(f"    Msg {m['step']}: {m['text'][:100]}...")

        # Save to raw_apollo_data
        raw["connection_notes"] = {
            "adrienne": note_adrienne,
            "melinda": note_melinda,
        }
        raw["messages"] = msgs

        sb.table("prospects").update({"raw_apollo_data": raw}).eq("id", p["id"]).execute()
        time.sleep(0.5)

    log.info("\n" + "=" * 60)
    log.info("DONE! Refresh /icp-prospects to see activity + messages.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
