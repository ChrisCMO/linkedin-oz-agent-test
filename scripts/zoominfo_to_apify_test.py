"""
Test: ZoomInfo → Apify LinkedIn Profile Search Pipeline
Actor: harvestapi/linkedin-profile-search (ID: M2FMdjRVeF1HPGFcc)

Pipeline test: Take ZoomInfo contact search results, find their LinkedIn
profiles via Apify, and extract detailed profile data for ICP scoring.

No cookies or LinkedIn accounts required.
"""

import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

APIFY_API_TOKEN = os.getenv("APIFY_API_KEY")
ACTOR_ID = "M2FMdjRVeF1HPGFcc"
BASE_URL = "https://api.apify.com/v2"

# Top 5 ZoomInfo contacts by contactAccuracyScore
ZOOMINFO_CONTACTS = [
    {
        "zoominfo_id": 1754176005,
        "firstName": "Marquis",
        "lastName": "Worthy",
        "jobTitle": "Manager, Clinical Imaging & Radiation Safety",
        "company": "Piedmont Healthcare",
        "contactAccuracyScore": 98,
    },
    {
        "zoominfo_id": 3214889227,
        "firstName": "Tracy",
        "lastName": "Phillips",
        "jobTitle": "Director, Quality & Safety",
        "company": "Piedmont Healthcare",
        "contactAccuracyScore": 94,
    },
    {
        "zoominfo_id": -912276208,
        "firstName": "Poppy",
        "lastName": "Howard",
        "jobTitle": "Radiation Safety Officer",
        "company": "Piedmont Healthcare",
        "contactAccuracyScore": 93,
    },
    {
        "zoominfo_id": 2050240207,
        "firstName": "Bennie",
        "lastName": "Paige",
        "jobTitle": "Emergency Department Technology & Safety Officer",
        "company": "Piedmont Healthcare",
        "contactAccuracyScore": 82,
    },
    {
        "zoominfo_id": -1265943844,
        "firstName": "Katherine",
        "lastName": "Collins",
        "jobTitle": "Director, Quality & Safety Phh",
        "company": "Piedmont Healthcare",
        "contactAccuracyScore": 81,
    },
]


def run_actor(contact: dict) -> dict:
    """Run the Apify LinkedIn profile search for a single contact."""
    url = f"{BASE_URL}/acts/{ACTOR_ID}/runs"
    headers = {
        "Authorization": f"Bearer {APIFY_API_TOKEN}",
        "Content-Type": "application/json",
    }

    # Search by full name + company keyword
    search_query = f"{contact['firstName']} {contact['lastName']}"
    payload = {
        "search": search_query,
        "keywordsCompany": contact["company"],
        "scrapeProfiles": True,
        "startPage": 1,
        "takePages": 1,
    }

    name = f"{contact['firstName']} {contact['lastName']}"
    print(f"\n{'='*60}")
    print(f"Searching: {name}")
    print(f"  Title: {contact['jobTitle']}")
    print(f"  Company: {contact['company']}")
    print(f"  ZoomInfo Accuracy: {contact['contactAccuracyScore']}")
    print(f"  Query: search=\"{search_query}\", keywordsCompany=\"{contact['company']}\"")
    print(f"  Mode: scrapeProfiles=True (full profile details)")
    print(f"-" * 60)

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code != 201:
        print(f"  ERROR: Failed to start actor. Status: {resp.status_code}")
        print(f"  {resp.text[:500]}")
        return {"contact": name, "error": resp.text, "results": []}

    run_data = resp.json()["data"]
    run_id = run_data["id"]
    dataset_id = run_data["defaultDatasetId"]
    print(f"  Run ID: {run_id}")
    print(f"  Dataset: {dataset_id}")

    # Poll for completion
    print(f"  Waiting", end="")
    for _ in range(120):  # max 10 minutes
        time.sleep(5)
        print(".", end="", flush=True)

        status_resp = requests.get(
            f"{BASE_URL}/actor-runs/{run_id}",
            headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
        )
        run_info = status_resp.json()["data"]
        status = run_info["status"]

        if status == "SUCCEEDED":
            duration = round(
                (run_info.get("finishedAt", "") or ""), 2
            ) if isinstance(run_info.get("finishedAt"), (int, float)) else "N/A"
            print(f"\n  Completed! Status: {status}")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"\n  FAILED with status: {status}")
            return {"contact": name, "error": status, "results": []}
    else:
        print(f"\n  Timed out after 10 minutes")
        return {"contact": name, "error": "TIMEOUT", "results": []}

    # Fetch results
    results_resp = requests.get(
        f"{BASE_URL}/datasets/{dataset_id}/items",
        headers={"Authorization": f"Bearer {APIFY_API_TOKEN}"},
        params={"format": "json"},
    )
    items = results_resp.json()

    print(f"  Results: {len(items)} profiles found")

    # Show top result
    if items:
        top = items[0]
        print(f"\n  TOP MATCH:")
        print(f"    Name: {top.get('name', top.get('firstName', '?'))} {top.get('lastName', '')}")
        print(f"    Headline: {top.get('headline', top.get('position', 'N/A'))}")
        print(f"    Location: {top.get('location', 'N/A')}")
        print(f"    LinkedIn: {top.get('linkedinUrl', top.get('url', 'N/A'))}")
        print(f"    Profile ID: {top.get('publicIdentifier', top.get('id', 'N/A'))}")

        # Show all available fields
        print(f"    All fields: {list(top.keys())}")

    return {
        "contact": name,
        "zoominfo_title": contact["jobTitle"],
        "zoominfo_company": contact["company"],
        "zoominfo_accuracy": contact["contactAccuracyScore"],
        "search_query": search_query,
        "results_count": len(items),
        "results": items,
    }


def main():
    if not APIFY_API_TOKEN:
        print("ERROR: APIFY_API_KEY not set in .env")
        return

    print("=" * 60)
    print("ZoomInfo → Apify LinkedIn Profile Search Pipeline Test")
    print(f"Actor: harvestapi/linkedin-profile-search ({ACTOR_ID})")
    print(f"Contacts: {len(ZOOMINFO_CONTACTS)} from ZoomInfo (Piedmont Healthcare)")
    print(f"Mode: Full profile scrape (scrapeProfiles=True)")
    print("=" * 60)

    all_results = []

    for i, contact in enumerate(ZOOMINFO_CONTACTS):
        result = run_actor(contact)
        all_results.append(result)

        # Delay between runs (not the last one)
        if i < len(ZOOMINFO_CONTACTS) - 1:
            delay = 10
            print(f"\n  Waiting {delay}s before next search...")
            time.sleep(delay)

    # Save all results
    output_path = os.path.join(
        os.path.dirname(__file__), "..", "output", "zoominfo_to_apify_test.json"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n\nResults saved to: {output_path}")

    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"{'Contact':<25} {'ZI Score':>8} {'LinkedIn Matches':>16} {'Status':<10}")
    print("-" * 65)
    for r in all_results:
        status = "FOUND" if r["results_count"] > 0 else "NOT FOUND"
        if r.get("error"):
            status = "ERROR"
        print(f"{r['contact']:<25} {r.get('zoominfo_accuracy', 'N/A'):>8} {r['results_count']:>16} {status:<10}")

    found = sum(1 for r in all_results if r["results_count"] > 0)
    print(f"\nMatch rate: {found}/{len(all_results)} ({found/len(all_results)*100:.0f}%)")


if __name__ == "__main__":
    main()
