"""
Test: Apify LinkedIn Profile Posts Scraper (No Cookies)
Actor: apimaestro/linkedin-profile-posts (ID: LQQIXN9Othf8f7R5n)

Tests the freshness validation layer:
- Scrape recent posts from a LinkedIn profile
- Assess activity recency and engagement levels
- No LinkedIn account/cookies required
"""

import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_KEY")
ACTOR_ID = "LQQIXN9Othf8f7R5n"
BASE_URL = "https://api.apify.com/v2"

# Test profile
TEST_PROFILE_URL = "https://www.linkedin.com/in/christopher-c-083561124/"


def run_actor(profile_url: str, max_posts: int = 10) -> dict:
    """Run the Apify actor and wait for results."""
    url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_API_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "profileUrl": profile_url,
        "maxPosts": max_posts,
    }

    print(f"Starting actor run for: {profile_url}")
    print(f"Max posts: {max_posts}")
    print("-" * 60)

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 201:
        print(f"ERROR: Failed to start actor. Status: {resp.status_code}")
        print(resp.text)
        return {}

    run_data = resp.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]
    print(f"Run started: {run_id}")
    print(f"Dataset ID: {dataset_id}")
    print(f"Status: {run_data['status']}")

    # Poll for completion
    print("\nWaiting for results", end="")
    for _ in range(60):  # max 5 minutes
        time.sleep(5)
        print(".", end="", flush=True)

        status_resp = requests.get(
            f"{BASE_URL}/actor-runs/{run_id}",
            headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
        )
        status = status_resp.json()["data"]["status"]

        if status == "SUCCEEDED":
            print(f"\nRun completed successfully!")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"\nRun ended with status: {status}")
            print(json.dumps(status_resp.json()["data"], indent=2))
            return {}
    else:
        print("\nTimed out waiting for results")
        return {}

    # Fetch results from dataset
    results_resp = requests.get(
        f"{BASE_URL}/datasets/{dataset_id}/items",
        headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
        params={"format": "json"},
    )
    items = results_resp.json()
    return items


def analyze_freshness(posts: list) -> dict:
    """Analyze LinkedIn activity freshness from scraped posts."""
    if not posts:
        return {
            "post_count": 0,
            "freshness_score": 0,
            "assessment": "NO DATA - Profile may be private or inactive",
        }

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    analysis = {
        "post_count": len(posts),
        "posts": [],
    }

    for i, post in enumerate(posts):
        post_summary = {
            "index": i + 1,
            "text_preview": (post.get("text") or post.get("postText") or "")[:150],
            "date": post.get("postedAt") or post.get("date") or post.get("postedDate"),
            "reactions": post.get("totalReactions") or post.get("numLikes") or post.get("reactionCount", 0),
            "comments": post.get("totalComments") or post.get("numComments") or post.get("commentCount", 0),
            "reposts": post.get("totalReposts") or post.get("numShares") or post.get("repostCount", 0),
        }
        analysis["posts"].append(post_summary)

    # Calculate freshness score
    score = 0
    if len(posts) >= 1:
        first_post_date = posts[0].get("postedAt") or posts[0].get("date") or posts[0].get("postedDate")
        if first_post_date:
            score += 15  # Has at least one post with a date
            analysis["most_recent_post_date"] = first_post_date

    if len(posts) >= 3:
        score += 5  # Multiple posts = active poster
    if len(posts) >= 5:
        score += 5  # Frequent poster

    # Check engagement
    total_reactions = sum(
        p.get("totalReactions") or p.get("numLikes") or p.get("reactionCount", 0)
        for p in posts
    )
    avg_reactions = total_reactions / len(posts) if posts else 0
    if avg_reactions >= 50:
        score += 5
    elif avg_reactions >= 10:
        score += 3

    analysis["freshness_score"] = score
    analysis["avg_reactions"] = round(avg_reactions, 1)
    analysis["total_reactions"] = total_reactions

    if score >= 20:
        analysis["assessment"] = "ACTIVE - Strong LinkedIn presence, good outreach candidate"
    elif score >= 10:
        analysis["assessment"] = "MODERATE - Some activity, reasonable outreach candidate"
    elif score > 0:
        analysis["assessment"] = "LOW - Minimal activity, may not respond on LinkedIn"
    else:
        analysis["assessment"] = "INACTIVE - No meaningful activity detected"

    return analysis


def main():
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_TOKEN not set in .env")
        print("Get your token from: https://console.apify.com/account#/integrations")
        return

    print("=" * 60)
    print("APIFY LinkedIn Profile Posts Scraper - Test Run")
    print(f"Actor: apimaestro/linkedin-profile-posts ({ACTOR_ID})")
    print(f"Target: {TEST_PROFILE_URL}")
    print("=" * 60)

    # Run the actor
    results = run_actor(TEST_PROFILE_URL, max_posts=10)

    # Save raw results
    output_path = os.path.join(os.path.dirname(__file__), "..", "output", "apify_profile_posts_test.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nRaw results saved to: {output_path}")

    # Analyze freshness
    print("\n" + "=" * 60)
    print("FRESHNESS ANALYSIS")
    print("=" * 60)

    analysis = analyze_freshness(results if isinstance(results, list) else [])
    print(json.dumps(analysis, indent=2, default=str))

    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
