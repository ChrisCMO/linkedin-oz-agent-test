#!/usr/bin/env python3
"""Fix missing profile data and re-check posts for all contacts."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests
from datetime import datetime

APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'
BASE = os.path.join(os.path.dirname(__file__), "..")

def run_actor(actor_id, payload):
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                      headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201:
        print(f"  Actor failed: {r.status_code}")
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(30):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                         headers=apify_h, timeout=30).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'): break
    try:
        return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                            headers=apify_h, timeout=15).json()
    except: return []

# Load
f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'proceed_contacts_enriched_v2.csv')
with open(f) as fh:
    reader = csv.DictReader(fh)
    headers = list(reader.fieldnames)
    rows = list(reader)

# Add missing columns
for col in ['Activity Level', 'Recent Post Date', 'Recent Post Text', 'Posts Count', 'Reposts Count', 'Total Feed Items', 'LinkedIn Active Status']:
    if col not in headers:
        headers.append(col)

print(f"Total contacts: {len(rows)}")

# ── FIX 1: Re-scrape 40 profiles with no data - ONE AT A TIME for reliability ──
no_profile = [r for r in rows if r.get('Role Verified', '') == 'No profile data']
print(f"\nFix 1: {len(no_profile)} profiles to re-scrape")

for i, r in enumerate(no_profile):
    li_url = (r.get('LinkedIn URL') or '').strip()
    if not li_url:
        continue

    try:
        items = run_actor(PROFILE_SCRAPER, {'urls': [li_url]})
        if items:
            prof = items[0]
            r['LinkedIn Headline'] = prof.get('headline', '') or r.get('LinkedIn Headline', '')
            r['LinkedIn Connections'] = str(prof.get('connectionsCount', ''))
            r['LinkedIn Followers'] = str(prof.get('followerCount', ''))
            r['Open to Work'] = 'Yes' if prof.get('openToWork') else 'No'

            current = prof.get('currentPosition', [])
            current_co = current[0].get('companyName', '') if current else ''
            company_name = r.get('Company', '')
            if current_co and company_name.lower()[:6] in current_co.lower():
                r['Role Verified'] = 'Yes'
            elif current_co:
                r['Role Verified'] = f"Check - LI: {prof.get('headline', '')[:40]}"
            else:
                r['Role Verified'] = 'Profile scraped - no current position'
            print(f"  {i+1}/{len(no_profile)} Fixed: {r.get('First Name', '')} {r.get('Last Name', '')} | {r['LinkedIn Headline'][:40]} | Conn: {r['LinkedIn Connections']}")
        else:
            print(f"  {i+1}/{len(no_profile)} No data: {r.get('First Name', '')} {r.get('Last Name', '')}")
    except Exception as e:
        print(f"  {i+1}/{len(no_profile)} Error: {r.get('First Name', '')} - {e}")

    # Save every 5
    if (i + 1) % 5 == 0:
        with open(f, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Progress saved ({i+1}/{len(no_profile)})")
    time.sleep(2)

# Save after all profiles
with open(f, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
print("Profiles done.")

# NOTE: Posts/activity checking is now done via scripts/icp1_fix_posts_only.py
# using the Activity Index actor (kog75ERz9lcVNujbQ) which provides a complete
# 1-10 activity score including posts, reposts, reactions, and comments.

# Final save
with open(f, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Stats
still_no_profile = sum(1 for r in rows if r.get('Role Verified', '') == 'No profile data')
has_conn = sum(1 for r in rows if str(r.get('LinkedIn Connections') or '').strip())
has_activity = sum(1 for r in rows if r.get('Activity Level', '') not in ('', 'Not checked yet', 'No posts found'))
has_checked = sum(1 for r in rows if r.get('Activity Level', '') != 'Not checked yet' and r.get('Activity Level', ''))

print(f"\n{'='*60}")
print(f"FINAL: {len(rows)} contacts")
print(f"  Still no profile data: {still_no_profile}")
print(f"  With connections: {has_conn}")
print(f"  Activity checked: {has_checked}")
print(f"  With actual posts: {has_activity}")
print(f"Saved: {f}")
