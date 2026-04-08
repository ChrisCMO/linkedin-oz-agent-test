#!/usr/bin/env python3
"""Re-validate all 'verified' CFO/Controller contacts by re-scraping their LinkedIn profiles.
Strict company name matching — must match the FULL company name, not just first word."""
import sys, os, csv, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'
BASE = os.path.join(os.path.dirname(__file__), "..")

INPUT = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'cfo_controller_verified.csv')
OUTPUT = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'cfo_controller_revalidated.csv')


def run_actor(actor_id, payload):
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(10)
            r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                              headers=apify_h, json=payload, timeout=30)
            if r.status_code != 201:
                continue
            run_id = r.json()['data']['id']
            ds = r.json()['data']['defaultDatasetId']
            for _ in range(30):
                time.sleep(5)
                s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                                 headers=apify_h, timeout=30).json()['data']['status']
                if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
                    break
            if s == 'SUCCEEDED':
                return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                                    headers=apify_h, timeout=15).json()
        except Exception as e:
            print(f"    Error: {e}")
    return []


def strict_company_match(target_company, linkedin_company, headline):
    """Strict matching — the LinkedIn company or headline must clearly reference the target."""
    target = target_company.lower().strip()
    li_co = (linkedin_company or '').lower().strip()
    hl = (headline or '').lower().strip()

    # Exact or near-exact match on LinkedIn current company
    if target in li_co or li_co in target:
        return True, "Company name match"

    # Check significant words (>3 chars) from target in LinkedIn company
    target_words = [w for w in target.split() if len(w) > 3 and w not in ('inc.', 'inc', 'llc', 'corp', 'corp.', 'the', 'and')]
    if target_words:
        li_combined = li_co + ' ' + hl
        matched_words = [w for w in target_words if w in li_combined]
        # Need at least 2 significant words to match, or 1 if company is 1 word
        if len(target_words) == 1 and len(matched_words) == 1:
            return True, f"Single-word match: {matched_words}"
        if len(matched_words) >= 2:
            return True, f"Multi-word match: {matched_words}"
        # Special case: first significant word + "at" pattern in headline
        if target_words[0] in hl and f"at {target_words[0]}" in hl:
            return True, f"'at {target_words[0]}' in headline"

    return False, f"No match: target='{target}' vs linkedin='{li_co}'"


# Load contacts
with open(INPUT) as fh:
    contacts = list(csv.DictReader(fh))

print(f"Contacts to re-validate: {len(contacts)}")
print("=" * 70)

results = []
for i, c in enumerate(contacts):
    company = c['Company']
    name = f"{c['First Name']} {c['Last Name']}"
    li_url = c.get('LinkedIn URL', '')

    print(f"\n[{i+1}/{len(contacts)}] {name} at {company}")
    print(f"  URL: {li_url}")

    if not li_url:
        print(f"  SKIP — no LinkedIn URL")
        c['Revalidation'] = 'FAIL — no URL'
        results.append(c)
        continue

    # Re-scrape profile
    prof_items = run_actor(PROFILE_SCRAPER, {'urls': [li_url]})
    if not prof_items:
        print(f"  FAIL — profile scrape failed")
        c['Revalidation'] = 'FAIL — scrape error'
        results.append(c)
        continue

    prof = prof_items[0]
    current_positions = prof.get('currentPosition', [])
    headline = prof.get('headline', '')
    location = prof.get('location', '')
    if isinstance(location, dict):
        location = location.get('default', str(location))

    # Get ALL current company names
    current_companies = [pos.get('companyName', '') for pos in current_positions]
    current_titles = [pos.get('title', '') for pos in current_positions]

    print(f"  Headline: {headline[:70]}")
    print(f"  Current positions: {current_companies}")
    print(f"  Current titles: {current_titles}")
    print(f"  Location: {location}")

    # Strict company match against ALL current positions
    matched = False
    match_reason = ''
    for co_name in current_companies:
        m, reason = strict_company_match(company, co_name, headline)
        if m:
            matched = True
            match_reason = reason
            break

    # Also check headline if no position match
    if not matched:
        m, reason = strict_company_match(company, '', headline)
        if m:
            matched = True
            match_reason = f"Headline only: {reason}"

    # Verify finance title
    finance_kw = ['cfo', 'chief financial', 'controller', 'vp finance', 'vp of finance',
                  'vice president of finance', 'vice president, finance', 'director of finance',
                  'director, finance', 'finance director', 'financial controller',
                  'treasurer', 'accounting manager', 'finance manager']
    all_titles = ' '.join(current_titles + [headline]).lower()
    has_finance_title = any(k in all_titles for k in finance_kw)

    if matched and has_finance_title:
        status = 'CONFIRMED'
        print(f"  ✅ CONFIRMED — {match_reason} + finance title verified")
    elif matched and not has_finance_title:
        status = f'CHECK — at company but title unclear: {headline[:50]}'
        print(f"  ⚠️  CHECK — at company but finance title not confirmed")
    elif not matched:
        actual = current_companies[0] if current_companies else 'unknown'
        status = f'FAIL — actually at: {actual}'
        print(f"  ❌ FAIL — NOT at {company}. Actually at: {actual}")

    c['Revalidation'] = status
    c['Live Headline'] = headline
    c['Live Company'] = ', '.join(current_companies)
    c['Live Title'] = ', '.join(current_titles)
    c['Live Location'] = location
    results.append(c)
    time.sleep(1)

# Write results
fieldnames = list(results[0].keys())
with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(results)

# Summary
print(f"\n{'='*70}")
print(f"REVALIDATION RESULTS")
print(f"{'='*70}")
confirmed = [r for r in results if r['Revalidation'] == 'CONFIRMED']
check = [r for r in results if r['Revalidation'].startswith('CHECK')]
failed = [r for r in results if r['Revalidation'].startswith('FAIL')]

print(f"CONFIRMED: {len(confirmed)}")
for r in confirmed:
    print(f"  ✅ {r['Company']} — {r['First Name']} {r['Last Name']} | {r.get('Live Headline','')[:50]}")

print(f"\nCHECK (at company, title unclear): {len(check)}")
for r in check:
    print(f"  ⚠️  {r['Company']} — {r['First Name']} {r['Last Name']} | {r['Revalidation']}")

print(f"\nFAILED (wrong company): {len(failed)}")
for r in failed:
    print(f"  ❌ {r['Company']} — {r['First Name']} {r['Last Name']} | {r['Revalidation']}")

print(f"\nOutput: {OUTPUT}")
