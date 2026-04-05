#!/usr/bin/env python3
"""Fix activity check v2: Use better posts actor + comments actor for complete picture."""

import os, sys, csv, time, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

APIFY_TOKEN = os.environ["APIFY_API_KEY"]
POSTS_ACTOR_ID = "A3cAPGpwBEG8RJwse"      # harvestapi/linkedin-profile-posts (better version)
COMMENTS_ACTOR_ID = "FiHYLewnJwS6GnRpo"    # harvestapi/linkedin-profile-comments


def run_apify_actor(actor_id, payload):
    """Run an Apify actor and return results."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs", headers=headers, json=payload)
    if r.status_code != 201:
        log.warning(f"  Actor start failed: {r.status_code} {r.text[:100]}")
        return []

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
        log.warning(f"  Actor run {status}")
        return []

    items = requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers).json()
    return items


def analyze_activity(linkedin_url, slug, posts_items, comments_items):
    """Combine posts + comments into a complete activity picture."""
    now = datetime.now()
    thirty_days = now - timedelta(days=30)
    ninety_days = now - timedelta(days=90)
    six_months = now - timedelta(days=180)

    slug_lower = slug.lower()
    activities = []  # list of {date, type, detail}

    # --- Analyze posts ---
    for item in posts_items:
        target = (item.get("query") or {}).get("targetUrl", "")
        if slug_lower and slug_lower not in target.lower():
            continue  # not for this profile

        author = item.get("author", {})
        author_id = (author.get("publicIdentifier") or "").lower()
        reposted_by = item.get("repostedBy")
        posted_at = item.get("postedAt", {})
        content = (item.get("content") or "")[:200]
        engagement = item.get("engagement", {})

        # Parse date
        date_str = posted_at.get("date", "")
        post_date = None
        if date_str:
            try:
                post_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        reposted_at = item.get("repostedAt", {})
        repost_date = None
        if reposted_at:
            rd = reposted_at.get("date", "")
            if rd:
                try:
                    repost_date = datetime.fromisoformat(rd.replace("Z", "+00:00")).replace(tzinfo=None)
                except (ValueError, TypeError):
                    pass

        # Determine activity type
        if reposted_by and slug_lower in (reposted_by.get("publicIdentifier") or "").lower():
            # Prospect reposted someone else's content
            date_used = repost_date or post_date
            author_name = author.get("name", "someone")
            activities.append({
                "date": date_used,
                "type": "Repost",
                "detail": f"Reposted {author_name}'s post: {content[:100]}",
                "engagement": engagement,
            })
        elif slug_lower in author_id:
            # Prospect's own original post
            activities.append({
                "date": post_date,
                "type": "Original Post",
                "detail": content[:150] if content else "(no text — may be image/video)",
                "engagement": engagement,
            })

    # --- Analyze comments ---
    for item in comments_items:
        commentary = item.get("commentary", "")
        created_at = item.get("createdAt", "")
        comment_date = None
        if created_at:
            try:
                comment_date = datetime.fromisoformat(created_at.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        if comment_date:
            activities.append({
                "date": comment_date,
                "type": "Comment",
                "detail": commentary[:150],
                "engagement": {},
            })

    # Sort by date descending
    activities.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

    # Determine overall activity level
    if not activities:
        return {
            "is_active": "Unknown",
            "activity_level": "No posts or comments found",
            "total_activities": 0,
            "recent_activity_date": "",
            "recent_activity_type": "",
            "recent_activity_detail": "",
            "activity_summary": "No detectable LinkedIn activity",
        }

    latest = activities[0]
    latest_date = latest["date"]

    # Count by recency
    last_30d = [a for a in activities if a["date"] and a["date"] >= thirty_days]
    last_90d = [a for a in activities if a["date"] and a["date"] >= ninety_days]
    last_6m = [a for a in activities if a["date"] and a["date"] >= six_months]

    # Count by type
    type_counts = {}
    for a in activities:
        t = a["type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    type_summary = ", ".join(f"{count} {t.lower()}{'s' if count > 1 else ''}" for t, count in type_counts.items())

    if latest_date and latest_date >= thirty_days:
        is_active = "Yes"
        level = f"Active — {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d')} ({len(last_30d)} activities in 30d)"
    elif latest_date and latest_date >= ninety_days:
        is_active = "Yes"
        level = f"Moderate — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d')} ({len(last_90d)} in 90d)"
    elif latest_date and latest_date >= six_months:
        is_active = "Somewhat"
        level = f"Low — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d')}"
    else:
        days_ago = (now - latest_date).days if latest_date else 999
        is_active = "Inactive"
        level = f"Inactive — last {latest['type'].lower()} {latest_date.strftime('%Y-%m-%d') if latest_date else '?'} ({days_ago}d ago)"

    # Build activity summary showing the 3 most recent activities
    summary_parts = []
    for a in activities[:3]:
        d = a["date"].strftime("%Y-%m-%d") if a["date"] else "?"
        summary_parts.append(f"[{d}] {a['type']}: {a['detail'][:80]}")
    activity_summary = " | ".join(summary_parts)

    return {
        "is_active": is_active,
        "activity_level": level,
        "total_activities": len(activities),
        "recent_activity_date": latest_date.strftime("%Y-%m-%d") if latest_date else "",
        "recent_activity_type": latest["type"],
        "recent_activity_detail": latest["detail"][:200],
        "posts_count": type_counts.get("Original Post", 0),
        "reposts_count": type_counts.get("Repost", 0),
        "comments_count": type_counts.get("Comment", 0),
        "activity_summary": activity_summary,
        "type_breakdown": type_summary,
    }


def main():
    infile = "output/icp1_enriched_prospect_sample.csv"
    with open(infile) as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames)
        rows = list(reader)

    # Collect all LinkedIn URLs
    profiles = []
    for row in rows:
        li_url = row.get("LinkedIn URL", "")
        if li_url:
            profiles.append(li_url)

    log.info(f"Checking activity for {len(profiles)} prospects using Posts + Comments scrapers")

    # --- Step 1: Run Posts scraper (batch — all profiles at once) ---
    log.info("\nStep 1: Posts scraper (A3cAPGpwBEG8RJwse)...")
    posts_items = run_apify_actor(POSTS_ACTOR_ID, {
        "usernames": profiles,
        "limit": 100,
    })
    log.info(f"  Got {len(posts_items)} total post items")

    # --- Step 2: Run Comments scraper (batch) ---
    log.info("\nStep 2: Comments scraper (FiHYLewnJwS6GnRpo)...")
    comments_items = run_apify_actor(COMMENTS_ACTOR_ID, {
        "maxItems": 100,
        "profiles": profiles,
    })
    log.info(f"  Got {len(comments_items)} total comment items")

    # --- Step 3: Group by profile and analyze ---
    log.info("\nStep 3: Analyzing activity per prospect...")

    # Group posts by target URL
    from collections import defaultdict
    posts_by_profile = defaultdict(list)
    for item in posts_items:
        target = (item.get("query") or {}).get("targetUrl", "")
        # Normalize URL
        target_slug = target.rstrip("/").split("/")[-1].lower() if target else ""
        if target_slug:
            posts_by_profile[target_slug].append(item)
        # Also try profile_input
        pi = (item.get("profile_input") or "").rstrip("/").split("/")[-1].lower()
        if pi and pi != target_slug:
            posts_by_profile[pi].append(item)

    # Group comments by profile URL
    comments_by_profile = defaultdict(list)
    for item in comments_items:
        # Comments actor may use different field names
        profile_url = item.get("profileUrl", item.get("profile_url", ""))
        slug = profile_url.rstrip("/").split("/")[-1].lower() if profile_url else ""
        if slug:
            comments_by_profile[slug].append(item)

    # Update columns — add new ones if needed
    new_cols = [
        "Recent Activity Type", "Recent Activity Detail",
        "Posts Count", "Reposts Count", "Comments Count",
        "Activity Type Breakdown", "Activity Summary (Top 3)",
    ]
    for col in new_cols:
        if col not in headers:
            headers.append(col)

    for row in rows:
        li_url = row.get("LinkedIn URL", "")
        slug = li_url.rstrip("/").split("/")[-1].lower() if li_url else ""
        name = f"{row['First Name']} {row['Last Name']}"

        p_items = posts_by_profile.get(slug, [])
        c_items = comments_by_profile.get(slug, [])

        activity = analyze_activity(li_url, slug, p_items, c_items)

        row["Is Active on LinkedIn"] = activity["is_active"]
        row["Activity Level"] = activity["activity_level"]
        row["Authored Posts"] = str(activity.get("posts_count", 0))
        row["Last Authored Post Date"] = activity["recent_activity_date"]
        row["Last Authored Post Text"] = activity["recent_activity_detail"]
        row["Feed Items Count"] = str(activity["total_activities"])
        row["Latest Feed Date"] = activity["recent_activity_date"]
        row["Recent Activity Type"] = activity.get("recent_activity_type", "")
        row["Recent Activity Detail"] = activity.get("recent_activity_detail", "")
        row["Posts Count"] = str(activity.get("posts_count", 0))
        row["Reposts Count"] = str(activity.get("reposts_count", 0))
        row["Comments Count"] = str(activity.get("comments_count", 0))
        row["Activity Type Breakdown"] = activity.get("type_breakdown", "")
        row["Activity Summary (Top 3)"] = activity.get("activity_summary", "")

        log.info(f"  {name}: {activity['is_active']} | {activity['activity_level'][:80]}")
        if activity.get("type_breakdown"):
            log.info(f"    Breakdown: {activity['type_breakdown']}")
        if activity.get("recent_activity_detail"):
            log.info(f"    Latest: [{activity['recent_activity_type']}] {activity['recent_activity_detail'][:100]}")

    # Write back
    with open(infile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"\nUpdated: {infile}")

    # Summary
    from collections import Counter
    statuses = Counter(row["Is Active on LinkedIn"] for row in rows)
    log.info("\nActivity Summary:")
    for status, count in statuses.most_common():
        log.info(f"  {status}: {count}")

    has_comments = sum(1 for r in rows if int(r.get("Comments Count", 0) or 0) > 0)
    has_posts = sum(1 for r in rows if int(r.get("Posts Count", 0) or 0) > 0)
    has_reposts = sum(1 for r in rows if int(r.get("Reposts Count", 0) or 0) > 0)
    log.info(f"\n  Has original posts: {has_posts}/25")
    log.info(f"  Has reposts: {has_reposts}/25")
    log.info(f"  Has comments: {has_comments}/25")


if __name__ == "__main__":
    main()
