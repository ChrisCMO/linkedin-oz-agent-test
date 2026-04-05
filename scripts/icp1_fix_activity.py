#!/usr/bin/env python3
"""Fix Apify activity check — pass username explicitly to get real prospect data."""

import os, sys, csv, time, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timedelta
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

APIFY_TOKEN = os.environ["APIFY_API_KEY"]
POSTS_ACTOR_ID = "LQQIXN9Othf8f7R5n"


def check_activity_fixed(linkedin_url):
    """Run Apify with explicit username to get actual prospect data."""
    slug = linkedin_url.rstrip("/").split("/")[-1] if linkedin_url else ""
    if not slug:
        return {
            "is_active": "Unknown", "activity_level": "No LinkedIn URL",
            "authored_posts": 0, "recent_post_date": None,
            "recent_post_text": None, "recent_post_reactions": 0,
            "feed_items": 0, "latest_feed_date": None,
        }

    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}

    r = requests.post(
        f"https://api.apify.com/v2/acts/{POSTS_ACTOR_ID}/runs",
        headers=headers,
        json={
            "profileUrl": linkedin_url,
            "username": slug,  # KEY FIX: override the default satyanadella
            "maxPosts": 10,
        },
    )
    if r.status_code != 201:
        return {
            "is_active": "Unknown", "activity_level": "Apify error",
            "authored_posts": 0, "recent_post_date": None,
            "recent_post_text": None, "recent_post_reactions": 0,
            "feed_items": 0, "latest_feed_date": None,
        }

    run_data = r.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]

    for _ in range(24):
        time.sleep(5)
        sr = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers)
        status = sr.json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED"):
            break

    if status != "SUCCEEDED":
        return {
            "is_active": "Unknown", "activity_level": f"Run {status}",
            "authored_posts": 0, "recent_post_date": None,
            "recent_post_text": None, "recent_post_reactions": 0,
            "feed_items": 0, "latest_feed_date": None,
        }

    items = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers).json()

    now = datetime.now()
    thirty_days = now - timedelta(days=30)
    ninety_days = now - timedelta(days=90)
    six_months = now - timedelta(days=180)

    slug_lower = slug.lower()

    authored = []
    other_activity = []  # reposts, quotes, reactions visible in feed

    for item in items:
        author = item.get("author", {})
        author_username = (author.get("username") or "").lower()
        post_type = item.get("post_type", "regular")

        posted = item.get("posted_at", {}).get("date")
        post_date = None
        if posted:
            try:
                post_date = datetime.strptime(posted, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

        # Check if authored by the prospect
        is_by_prospect = slug_lower in author_username

        if is_by_prospect and post_date:
            authored.append({
                "date": post_date,
                "text": (item.get("text") or "")[:200],
                "type": post_type,
                "reactions": (item.get("stats") or {}).get("total_reactions", 0),
                "comments": (item.get("stats") or {}).get("comments", 0),
            })
        elif post_date:
            # Feed activity — prospect is engaging with this content
            other_activity.append({
                "date": post_date,
                "type": post_type,
                "author": f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
            })

    authored.sort(key=lambda x: x["date"], reverse=True)
    other_activity.sort(key=lambda x: x["date"], reverse=True)

    # Determine activity level
    latest_authored = authored[0] if authored else None
    latest_feed = other_activity[0] if other_activity else None

    if latest_authored and latest_authored["date"] >= thirty_days:
        is_active = "Yes"
        level = f"Active — posted {latest_authored['date'].strftime('%Y-%m-%d')} ({latest_authored['type']})"
    elif latest_authored and latest_authored["date"] >= ninety_days:
        is_active = "Yes"
        level = f"Moderate — last post {latest_authored['date'].strftime('%Y-%m-%d')}"
    elif latest_authored and latest_authored["date"] >= six_months:
        is_active = "Somewhat"
        level = f"Low — last post {latest_authored['date'].strftime('%Y-%m-%d')}"
    elif authored:
        is_active = "Inactive"
        level = f"Inactive — last post {authored[0]['date'].strftime('%Y-%m-%d')} (>{(now - authored[0]['date']).days}d ago)"
    elif other_activity:
        # No authored posts but feed has content — they follow people but don't post
        is_active = "Feed only"
        level = f"Feed active (no posts) — follows {len(set(a['author'] for a in other_activity))} people, latest feed {latest_feed['date'].strftime('%Y-%m-%d')}"
    elif items:
        is_active = "Minimal"
        level = "Has LinkedIn profile but minimal activity"
    else:
        is_active = "Unknown"
        level = "No data returned"

    return {
        "is_active": is_active,
        "activity_level": level,
        "authored_posts": len(authored),
        "recent_post_date": latest_authored["date"].strftime("%Y-%m-%d") if latest_authored else "",
        "recent_post_text": latest_authored["text"] if latest_authored else "",
        "recent_post_reactions": latest_authored["reactions"] if latest_authored else 0,
        "feed_items": len(items),
        "latest_feed_date": latest_feed["date"].strftime("%Y-%m-%d") if latest_feed else "",
    }


def main():
    # Read the enriched CSV
    infile = "output/icp1_enriched_prospect_sample.csv"
    with open(infile) as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    log.info(f"Re-checking LinkedIn activity for {len(rows)} prospects (with username fix)")

    for i, row in enumerate(rows):
        name = f"{row['First Name']} {row['Last Name']}"
        li_url = row.get("LinkedIn URL", "")

        log.info(f"  {i+1}/{len(rows)}: {name}...")

        activity = check_activity_fixed(li_url)

        row["Is Active on LinkedIn"] = activity["is_active"]
        row["Activity Level"] = activity["activity_level"]
        row["Authored Posts"] = str(activity["authored_posts"])
        row["Last Authored Post Date"] = activity["recent_post_date"]
        row["Last Authored Post Text"] = activity["recent_post_text"]
        row["Post Reactions"] = str(activity["recent_post_reactions"])
        row["Feed Items Count"] = str(activity["feed_items"])
        row["Latest Feed Date"] = activity["latest_feed_date"]

        log.info(f"    -> {activity['is_active']} | {activity['activity_level'][:80]}")
        if activity["recent_post_text"]:
            log.info(f"    -> Recent post: {activity['recent_post_text'][:100]}...")

    # Write back
    with open(infile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"\nUpdated: {infile}")

    # Summary
    active_counts = Counter(row["Is Active on LinkedIn"] for row in rows)
    log.info("\nActivity breakdown:")
    for status, count in active_counts.most_common():
        log.info(f"  {status}: {count}")

    has_posts = sum(1 for r in rows if int(r.get("Authored Posts", 0) or 0) > 0)
    log.info(f"Has authored posts: {has_posts}/{len(rows)}")


if __name__ == "__main__":
    main()
