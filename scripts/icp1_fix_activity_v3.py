#!/usr/bin/env python3
"""Fix activity v3: Run posts + comments scrapers ONE PROFILE AT A TIME (reliable)."""

import os, sys, csv, time, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

APIFY_TOKEN = os.environ["APIFY_API_KEY"]
POSTS_ACTOR = "A3cAPGpwBEG8RJwse"     # harvestapi posts (has repostedBy)
COMMENTS_ACTOR = "FiHYLewnJwS6GnRpo"  # harvestapi comments


def run_actor(actor_id, payload, label=""):
    """Run Apify actor, poll, return items."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs", headers=headers, json=payload)
    if r.status_code != 201:
        return []
    run_id = r.json()["data"]["id"]
    dataset_id = r.json()["data"]["defaultDatasetId"]
    for _ in range(24):
        time.sleep(5)
        sr = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers)
        if sr.json()["data"]["status"] in ("SUCCEEDED", "FAILED", "ABORTED"):
            break
    return requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers).json()


def check_full_activity(linkedin_url):
    """Run both posts + comments scrapers for one profile, return combined analysis."""
    slug = linkedin_url.rstrip("/").split("/")[-1] if linkedin_url else ""
    if not slug:
        return empty_result("No LinkedIn URL")

    slug_lower = slug.lower()

    # Run posts scraper
    posts_items = run_actor(POSTS_ACTOR, {
        "usernames": [linkedin_url],
        "limit": 20,
    })

    # Run comments scraper
    comments_items = run_actor(COMMENTS_ACTOR, {
        "maxItems": 20,
        "profiles": [linkedin_url],
    })

    # Build activity list
    now = datetime.now()
    activities = []

    # Parse posts
    for item in posts_items:
        author = item.get("author", {})
        author_id = (author.get("publicIdentifier") or "").lower()
        reposted_by = item.get("repostedBy")
        content = (item.get("content") or "")[:200]
        posted_at = item.get("postedAt", {})
        reposted_at = item.get("repostedAt", {})

        post_date = parse_date(posted_at.get("date", ""))
        repost_date = parse_date(reposted_at.get("date", "")) if reposted_at else None

        if reposted_by and slug_lower in (reposted_by.get("publicIdentifier") or "").lower():
            author_name = author.get("name", "someone")
            activities.append({
                "date": repost_date or post_date,
                "type": "Repost",
                "detail": f"Reposted {author_name}'s post: {content[:100]}",
            })
        elif slug_lower in author_id:
            activities.append({
                "date": post_date,
                "type": "Original Post",
                "detail": content[:150] if content else "(image/video post)",
            })

    # Parse comments
    for item in comments_items:
        commentary = item.get("commentary", "")
        created_at = item.get("createdAt", "")
        comment_date = parse_date(created_at)
        if comment_date:
            activities.append({
                "date": comment_date,
                "type": "Comment",
                "detail": commentary[:150],
            })

    # Sort by date desc
    activities.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

    if not activities:
        return empty_result("No posts, reposts, or comments found")

    # Classify
    latest = activities[0]
    latest_date = latest["date"]
    thirty_days = now - timedelta(days=30)
    ninety_days = now - timedelta(days=90)
    six_months = now - timedelta(days=180)

    # Counts
    posts_count = sum(1 for a in activities if a["type"] == "Original Post")
    reposts_count = sum(1 for a in activities if a["type"] == "Repost")
    comments_count = sum(1 for a in activities if a["type"] == "Comment")

    type_parts = []
    if comments_count:
        type_parts.append(f"{comments_count} comment{'s' if comments_count > 1 else ''}")
    if reposts_count:
        type_parts.append(f"{reposts_count} repost{'s' if reposts_count > 1 else ''}")
    if posts_count:
        type_parts.append(f"{posts_count} post{'s' if posts_count > 1 else ''}")
    type_breakdown = ", ".join(type_parts) if type_parts else "none"

    if latest_date and latest_date >= thirty_days:
        is_active = "Yes"
        level = f"Active — {latest['type'].lower()} on {latest_date.strftime('%Y-%m-%d')} ({type_breakdown})"
    elif latest_date and latest_date >= ninety_days:
        is_active = "Yes"
        level = f"Moderate — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d')} ({type_breakdown})"
    elif latest_date and latest_date >= six_months:
        is_active = "Somewhat"
        level = f"Low — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d')} ({type_breakdown})"
    else:
        days_ago = (now - latest_date).days if latest_date else 999
        is_active = "Inactive"
        level = f"Inactive — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d') if latest_date else '?'} ({days_ago}d ago) ({type_breakdown})"

    # Top 3 summary
    summary_parts = []
    for a in activities[:3]:
        d = a["date"].strftime("%Y-%m-%d") if a["date"] else "?"
        summary_parts.append(f"[{d}] {a['type']}: {a['detail'][:80]}")

    return {
        "is_active": is_active,
        "activity_level": level,
        "recent_activity_date": latest_date.strftime("%Y-%m-%d") if latest_date else "",
        "recent_activity_type": latest["type"],
        "recent_activity_detail": latest["detail"][:200],
        "posts_count": posts_count,
        "reposts_count": reposts_count,
        "comments_count": comments_count,
        "total_activities": len(activities),
        "type_breakdown": type_breakdown,
        "activity_summary": " | ".join(summary_parts),
    }


def parse_date(date_str):
    if not date_str:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f+00:00", "%Y-%m-%dT%H:%M:%S+00:00"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def empty_result(reason):
    return {
        "is_active": "Unknown", "activity_level": reason,
        "recent_activity_date": "", "recent_activity_type": "",
        "recent_activity_detail": "", "posts_count": 0,
        "reposts_count": 0, "comments_count": 0,
        "total_activities": 0, "type_breakdown": "none",
        "activity_summary": reason,
    }


def main():
    infile = "output/icp1_enriched_prospect_sample.csv"
    with open(infile) as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames)
        rows = list(reader)

    # Ensure new columns exist
    new_cols = ["Recent Activity Type", "Recent Activity Detail",
                "Posts Count", "Reposts Count", "Comments Count",
                "Activity Type Breakdown", "Activity Summary (Top 3)"]
    for col in new_cols:
        if col not in headers:
            headers.append(col)

    log.info(f"Checking {len(rows)} prospects (posts + comments, one at a time)")
    log.info("This takes ~20s per prospect (2 actor runs each)")

    for i, row in enumerate(rows):
        name = f"{row['First Name']} {row['Last Name']}"
        li_url = row.get("LinkedIn URL", "")
        log.info(f"\n  {i+1}/{len(rows)}: {name}...")

        activity = check_full_activity(li_url)

        row["Is Active on LinkedIn"] = activity["is_active"]
        row["Activity Level"] = activity["activity_level"]
        row["Last Authored Post Date"] = activity["recent_activity_date"]
        row["Last Authored Post Text"] = activity["recent_activity_detail"]
        row["Recent Activity Type"] = activity["recent_activity_type"]
        row["Recent Activity Detail"] = activity["recent_activity_detail"]
        row["Posts Count"] = str(activity["posts_count"])
        row["Reposts Count"] = str(activity["reposts_count"])
        row["Comments Count"] = str(activity["comments_count"])
        row["Authored Posts"] = str(activity["total_activities"])
        row["Activity Type Breakdown"] = activity["type_breakdown"]
        row["Activity Summary (Top 3)"] = activity["activity_summary"]
        row["Feed Items Count"] = str(activity["total_activities"])
        row["Latest Feed Date"] = activity["recent_activity_date"]

        log.info(f"    {activity['is_active']} | {activity['activity_level'][:90]}")
        if activity["activity_summary"] and activity["activity_summary"] != activity["activity_level"]:
            log.info(f"    Summary: {activity['activity_summary'][:120]}")

    with open(infile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"\nUpdated: {infile}")

    from collections import Counter
    statuses = Counter(row["Is Active on LinkedIn"] for row in rows)
    log.info("\nFinal Activity Breakdown:")
    for s, c in statuses.most_common():
        log.info(f"  {s}: {c}")
    log.info(f"  Has comments: {sum(1 for r in rows if int(r.get('Comments Count', 0) or 0) > 0)}/25")
    log.info(f"  Has posts: {sum(1 for r in rows if int(r.get('Posts Count', 0) or 0) > 0)}/25")
    log.info(f"  Has reposts: {sum(1 for r in rows if int(r.get('Reposts Count', 0) or 0) > 0)}/25")


if __name__ == "__main__":
    main()
