#!/usr/bin/env python3
"""Check LinkedIn activity for top 50 contacts using Activity Index actor.
Captures posts, reposts, reactions, comments — full activity picture.

Usage:
    python scripts/icp1_fix_posts_only.py              # Process all unscored contacts
    python scripts/icp1_fix_posts_only.py --retry-errors  # Retry only Error/0-score contacts
"""
import sys, os, csv, time, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests
from datetime import datetime

APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}
ACTIVITY_INDEX = 'kog75ERz9lcVNujbQ'  # LinkedIn Activity Index (LinkedScore)
BASE = os.path.join(os.path.dirname(__file__), "..")

MAX_RETRIES = 3          # 1 initial + 2 retries
RETRY_DELAY = 10         # seconds between retries
RATE_LIMIT_DELAY = 30    # seconds to wait on 429

# Track all errors for summary report
error_log = []  # list of (name, company, error_reason)


def normalize_linkedin_url(url):
    """Normalize a LinkedIn URL to https://www.linkedin.com/in/... format."""
    url = url.strip()
    if not url:
        return url
    url = url.replace('http://', 'https://')
    if 'https://linkedin.com' in url:
        url = url.replace('https://linkedin.com', 'https://www.linkedin.com')
    if not url.startswith('https://www.linkedin.com'):
        url = 'https://www.linkedin.com/in/' + url.rstrip('/').split('/')[-1]
    return url


def run_actor(actor_id, payload, contact_name="", li_url=""):
    """Run an Apify actor with retry logic.

    Retries up to MAX_RETRIES times on failure/empty response.
    Handles 429 (rate limit) with a 30s wait.
    Raises SystemExit on 402 (out of credits).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                print(f"\n    Retry {attempt-1}/{MAX_RETRIES-1} for {contact_name} ({li_url})...", end='', flush=True)
                time.sleep(RETRY_DELAY)

            r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                              headers=apify_h, json=payload, timeout=30)

            # Handle rate limits
            if r.status_code == 429:
                print(f"\n    [RATE LIMIT] 429 received for {contact_name}. Waiting {RATE_LIMIT_DELAY}s...", flush=True)
                time.sleep(RATE_LIMIT_DELAY)
                continue  # counts as a retry attempt

            # Handle out of credits — stop the entire script
            if r.status_code == 402:
                print(f"\n    [OUT OF CREDITS] 402 received. Stopping script.")
                return None  # sentinel value for "stop script"

            if r.status_code != 201:
                print(f"\n    Actor start failed (attempt {attempt}): {r.status_code} - {r.text[:100]}")
                continue

            run_id = r.json()['data']['id']
            ds = r.json()['data']['defaultDatasetId']
            for _ in range(30):
                time.sleep(5)
                status_resp = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                                           headers=apify_h, timeout=30)
                # Handle rate limit on status polling too
                if status_resp.status_code == 429:
                    print(f"\n    [RATE LIMIT] 429 on status poll. Waiting {RATE_LIMIT_DELAY}s...", flush=True)
                    time.sleep(RATE_LIMIT_DELAY)
                    continue
                s = status_resp.json()['data']['status']
                if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
                    break

            if s != 'SUCCEEDED':
                print(f"\n    Actor run {s} (attempt {attempt}) for {contact_name}")
                continue

            items = requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                                 headers=apify_h, timeout=15).json()

            if items:
                return items

            # Empty response — retry
            print(f"\n    Empty response (attempt {attempt}) for {contact_name}", flush=True)
            continue

        except Exception as e:
            print(f"\n    Actor error (attempt {attempt}) for {contact_name}: {e}")
            continue

    # All retries exhausted
    return []


# Parse arguments
parser = argparse.ArgumentParser(description='Check LinkedIn activity for contacts')
parser.add_argument('--retry-errors', action='store_true',
                    help='Only process contacts with Activity Level = "Error" or Activity Score = "0"')
args = parser.parse_args()

# Load
f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'proceed_contacts_enriched_v2.csv')
with open(f) as fh:
    reader = csv.DictReader(fh)
    headers = list(reader.fieldnames)
    rows = list(reader)

# Ensure columns exist
new_cols = [
    'Activity Score', 'Activity Level', 'Activity Recommendation',
    'Activity Insights',
    'Posts Last 30 Days', 'Reactions Last 30 Days',
    'Last Activity Date', 'Days Since Last Activity',
    'LinkedIn Active Status',
]
for col in new_cols:
    if col not in headers:
        headers.append(col)

# Pre-processing: normalize ALL LinkedIn URLs before the loop
normalized_count = 0
for r in rows:
    raw_url = r.get('LinkedIn URL', '').strip()
    if raw_url:
        normalized = normalize_linkedin_url(raw_url)
        if normalized != raw_url:
            r['LinkedIn URL'] = normalized
            normalized_count += 1

if normalized_count > 0:
    print(f"Pre-processing: normalized {normalized_count} LinkedIn URLs")

print(f"Total contacts: {len(rows)}")

# Sort by ICP score, run ALL contacts with LinkedIn URLs
has_li = [r for r in rows if (r.get('LinkedIn URL') or '').strip()]
has_li_sorted = sorted(has_li, key=lambda x: -(int(x.get('Company ICP Score', '0')) if str(x.get('Company ICP Score', '')).lstrip('-').isdigit() else 0))
top_contacts = has_li_sorted  # Run all, not just top 50

# If --retry-errors, filter to only Error/0-score contacts
if args.retry_errors:
    top_contacts = [r for r in top_contacts
                    if r.get('Activity Level') == 'Error'
                    or r.get('Activity Score') == '0']
    print(f"--retry-errors mode: {len(top_contacts)} contacts to retry")

now = datetime.now()
completed_count = 0
print(f"\nChecking activity for {len(top_contacts)} contacts using Activity Index actor...")

for i, r in enumerate(top_contacts):
    li_url = r.get('LinkedIn URL', '').strip()
    if not li_url:
        continue

    contact_name = f"{r.get('First Name', '')} {r.get('Last Name', '')}".strip()
    company = r.get('Company', '')

    # Skip if already has a valid Activity Score from a previous run (unless retrying errors)
    if not args.retry_errors:
        existing_score = r.get('Activity Score', '')
        if existing_score and existing_score != '0' and r.get('Activity Level', '') != 'Error':
            print(f"  {i+1}/{len(top_contacts)}: {contact_name} - already scored ({existing_score}/10), skipping")
            continue

    print(f"  {i+1}/{len(top_contacts)}: {contact_name} ({company})...", end='', flush=True)

    items = run_actor(ACTIVITY_INDEX, {'linkedinUrl': li_url}, contact_name=contact_name, li_url=li_url)

    # None = out of credits sentinel — save progress and exit
    if items is None:
        error_reason = "Out of Apify credits (402)"
        r['Activity Score'] = '0'
        r['Activity Level'] = 'Error'
        r['LinkedIn Active Status'] = f'Activity check failed: {error_reason}'
        error_log.append((contact_name, company, error_reason))
        print(f"\n\n[FATAL] Out of Apify credits. Completed {completed_count} contacts before stopping.")
        break

    if items and isinstance(items, list) and len(items) > 0:
        data = items[0]

        # Validate actor response structure
        if not data.get('success'):
            err = data.get('error', 'Actor returned success=false')
            r['Activity Score'] = '0'
            r['Activity Level'] = 'Error'
            r['LinkedIn Active Status'] = f'Activity check failed: {str(err)[:60]}'
            error_log.append((contact_name, company, f"success=false: {str(err)[:80]}"))
            print(f" ERROR: {str(err)[:60]}")
        elif 'activity_metrics' not in data:
            reason = "Missing activity_metrics in response"
            r['Activity Score'] = '0'
            r['Activity Level'] = 'Error'
            r['LinkedIn Active Status'] = f'Activity check failed: {reason}'
            error_log.append((contact_name, company, reason))
            print(f" ERROR: {reason}")
        else:
            # Valid response — extract data
            score = data.get('activity_score', 0)
            rec = data.get('recommendation', '')
            metrics = data.get('activity_metrics', {})

            last_date = metrics.get('last_activity_date', '')
            days_since = metrics.get('days_since_last_activity', '')
            posts_30d = metrics.get('posts_last_30_days', 0)
            reactions_30d = metrics.get('reactions_last_30_days', 0)
            total_posts = metrics.get('total_posts_scraped', 0)
            total_reactions = metrics.get('total_reactions_scraped', 0)

            r['Activity Score'] = str(score)
            r['Activity Recommendation'] = rec
            r['Posts Last 30 Days'] = str(posts_30d)
            r['Reactions Last 30 Days'] = str(reactions_30d)
            r['Last Activity Date'] = last_date
            r['Days Since Last Activity'] = str(days_since) if days_since != '' else ''

            # Map score to Activity Level
            if score >= 7:
                r['Activity Level'] = 'Very Active'
            elif score >= 5:
                r['Activity Level'] = 'Active'
            elif score >= 3:
                r['Activity Level'] = 'Moderate'
            elif score >= 1:
                r['Activity Level'] = 'Low'
            else:
                r['Activity Level'] = 'Inactive'

            # Insights from actor
            insights = data.get('insights', [])
            r['Activity Insights'] = ' | '.join(insights) if insights else ''

            # LinkedIn Active Status — short format
            r['LinkedIn Active Status'] = f'{score}/10 {r["Activity Level"]}'

            completed_count += 1
            print(f" Score:{score}/10 | {r['Activity Level']} | {posts_30d}p/{reactions_30d}r in 30d | last: {last_date}")
    else:
        error_reason = 'No response after all retries'
        r['Activity Score'] = '0'
        r['Activity Level'] = 'Error'
        r['LinkedIn Active Status'] = f'Activity check failed: {error_reason}'
        error_log.append((contact_name, company, error_reason))
        print(f" ERROR: {error_reason}")

    # Save every 10
    if (i + 1) % 10 == 0:
        with open(f, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  ** Progress saved ({i+1}/{len(top_contacts)}) **")

    time.sleep(1)

# Final save
with open(f, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Save error log to CSV
if error_log:
    error_file = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'activity_check_errors.csv')
    with open(error_file, 'w', newline='', encoding='utf-8') as efh:
        ew = csv.writer(efh)
        ew.writerow(['Name', 'Company', 'Error Reason'])
        for name, company, reason in error_log:
            ew.writerow([name, company, reason])
    print(f"\nError log saved: {error_file}")

# Stats
very_active = sum(1 for r in rows if r.get('Activity Level') == 'Very Active')
active = sum(1 for r in rows if r.get('Activity Level') == 'Active')
moderate = sum(1 for r in rows if r.get('Activity Level') == 'Moderate')
low = sum(1 for r in rows if r.get('Activity Level') == 'Low')
inactive = sum(1 for r in rows if r.get('Activity Level') == 'Inactive')
error = sum(1 for r in rows if r.get('Activity Level') == 'Error')
not_checked = sum(1 for r in rows if r.get('Activity Level') == 'Not checked yet')

print(f"\n{'='*60}")
print(f"ACTIVITY RESULTS: {len(rows)} contacts ({completed_count} successfully scored this run)")
print(f"  Very Active (7-10):  {very_active}")
print(f"  Active (5-6):        {active}")
print(f"  Moderate (3-4):      {moderate}")
print(f"  Low (1-2):           {low}")
print(f"  Inactive (0):        {inactive}")
print(f"  Error:               {error}")
print(f"  Not checked yet:     {not_checked}")

# Print error summary
if error_log:
    print(f"\n{'='*60}")
    print(f"ERROR SUMMARY: {len(error_log)} contacts failed")
    print(f"{'─'*60}")
    for name, company, reason in error_log:
        print(f"  {name} ({company}): {reason}")

# Show top active contacts
print(f"\nTop active contacts (score 5+):")
scored = [(r, int(r.get('Activity Score', '0'))) for r in rows if r.get('Activity Score', '').isdigit() and int(r.get('Activity Score', '0')) >= 5]
scored.sort(key=lambda x: -x[1])
for r, sc in scored[:15]:
    print(f"  {sc}/10 | {r['First Name']} {r['Last Name']} | {r['Company']} | {r.get('Posts Last 30 Days','0')}p/{r.get('Reactions Last 30 Days','0')}r in 30d")

print(f"\nSaved: {f}")
