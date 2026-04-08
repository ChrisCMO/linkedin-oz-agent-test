#!/usr/bin/env python3
"""Check 46 companies with no CFO/Controller via ZoomInfo + Google X-ray search.
Finds finance contacts, verifies via LinkedIn profile scrape."""
import sys, os, csv, json, time, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
APIFY_TOKEN = os.environ['APIFY_API_KEY']

apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

BASE = os.path.join(os.path.dirname(__file__), "..")
GOOGLE_SERP = 'nFJndFXA5zjCTuudP'
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'

INPUT_CSV = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'companies_no_cfo_controller.csv')
OUTPUT_CSV = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'cfo_controller_search_results.csv')

CITY_ZIP = {
    'seattle': '98101', 'bellevue': '98004', 'tacoma': '98402',
    'redmond': '98052', 'kirkland': '98033', 'everett': '98201',
    'renton': '98057', 'kent': '98032', 'auburn': '98002',
    'olympia': '98501', 'lynnwood': '98036', 'lakewood': '98499',
    'federal way': '98003', 'vancouver': '98660', 'ferndale': '98248',
}

def get_zip(location):
    loc = (location or '').lower().strip()
    for city, zipcode in CITY_ZIP.items():
        if city in loc:
            return zipcode
    return '98101'

def normalize_url(url):
    url = (url or '').strip().replace('http://', 'https://')
    if 'https://linkedin.com' in url:
        url = url.replace('https://linkedin.com', 'https://www.linkedin.com')
    return url

def run_actor(actor_id, payload, label=""):
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(10)
            r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                              headers=apify_h, json=payload, timeout=30)
            if r.status_code == 402:
                print(f"    [OUT OF CREDITS]")
                return None
            if r.status_code != 201:
                continue
            run_id = r.json()['data']['id']
            ds = r.json()['data']['defaultDatasetId']
            for _ in range(30):
                time.sleep(5)
                s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                                 headers=apify_h, timeout=30).json()['data']['status']
                if s in ('SUCCEEDED', 'FAILED', 'ABORTED'): break
            if s == 'SUCCEEDED':
                return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                                    headers=apify_h, timeout=15).json()
        except Exception as e:
            print(f"    Error: {e}")
    return []

# ═══════════════════════════════════════
# Load companies
# ═══════════════════════════════════════
with open(INPUT_CSV) as fh:
    rows = list(csv.DictReader(fh))

companies = {}
for r in rows:
    co = r['Company']
    if co not in companies:
        companies[co] = {
            'location': r.get('Company Location', ''),
            'icp_score': r.get('Company ICP Score', ''),
            'category': r.get('Category', ''),
            'industry': r.get('Industry', ''),
            'li_url': r.get('Company LinkedIn URL', ''),
            'li_followers': r.get('Company LI Followers', ''),
        }

SKIP = int(os.environ.get('CHECK_SKIP', '0'))
MAX_COMPANIES = int(os.environ.get('CHECK_LIMIT', '0')) or len(companies)
companies = dict(list(sorted(companies.items()))[SKIP:SKIP + MAX_COMPANIES])
print(f"Companies to check: {len(companies)} (skip={SKIP})")

# ═══════════════════════════════════════
# ZoomInfo auth
# ═══════════════════════════════════════
zi_user = os.environ.get('ZOOMINFO_USERNAME', '')
zi_pass = os.environ.get('ZOOMINFO_PASSWORD', '')
try:
    auth = requests.post('https://api.zoominfo.com/authenticate',
                         json={'username': zi_user, 'password': zi_pass}, timeout=15)
    token = auth.json().get('jwt', '')
    zi_h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    print(f"ZoomInfo auth: {'OK' if token else 'FAILED'}")
except Exception as e:
    print(f"ZoomInfo auth error: {e}")
    token = ''
    zi_h = {}

# ═══════════════════════════════════════
# Search pipeline
# ═══════════════════════════════════════
FINANCE_TITLES = 'CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance'

# Load existing results to append
all_results = []
if os.path.exists(OUTPUT_CSV):
    with open(OUTPUT_CSV) as fh:
        all_results = list(csv.DictReader(fh))
    existing_companies = set(r['Company'] for r in all_results)
    print(f"Loaded {len(all_results)} existing results from {len(existing_companies)} companies")
else:
    existing_companies = set()
out_of_credits = False

for i, (co_name, co_info) in enumerate(sorted(companies.items())):
    if out_of_credits:
        break

    zipcode = get_zip(co_info['location'])
    print(f"\n[{i+1}/{len(companies)}] {co_name} ({co_info['location']}, zip: {zipcode})")
    print("-" * 60)

    # Skip already-processed companies
    if co_name in existing_companies:
        print(f"  Already processed, skipping")
        continue

    found_contacts = []

    # ── Step 1: ZoomInfo contact search ──
    zi_contacts = []
    if token:
        try:
            resp = requests.post('https://api.zoominfo.com/search/contact', headers=zi_h, json={
                'companyName': co_name,
                'jobTitle': FINANCE_TITLES,
                'zipCode': zipcode,
                'zipCodeRadiusMiles': '50',
                'rpp': 5,
            }, timeout=15)
            zi_contacts = resp.json().get('data', []) or []
        except Exception as e:
            print(f"  ZoomInfo error: {e}")

    if zi_contacts:
        print(f"  ZoomInfo: {len(zi_contacts)} finance contacts")
        for zc in zi_contacts:
            found_contacts.append({
                'first': zc.get('firstName', ''),
                'last': zc.get('lastName', ''),
                'title': zc.get('jobTitle', ''),
                'source': 'ZoomInfo',
                'zi_id': str(zc.get('id', '')),
                'accuracy': str(zc.get('contactAccuracyScore', '')),
            })
            print(f"    → {zc.get('firstName','')} {zc.get('lastName','')} | {zc.get('jobTitle','')} | Acc: {zc.get('contactAccuracyScore','')}")
    else:
        print(f"  ZoomInfo: 0 results")

    time.sleep(random.uniform(0.5, 0.8))

    # ── Step 2: Google X-ray search ──
    co_short = co_name.split('(')[0].split(',')[0].strip()
    xray_queries = [
        f'site:linkedin.com/in "{co_short}" CFO',
        f'site:linkedin.com/in "{co_short}" "chief financial"',
        f'site:linkedin.com/in "{co_short}" controller',
        f'site:linkedin.com/in "{co_short}" "director of finance"',
    ]

    xray_found = {}  # url -> info
    for q in xray_queries:
        items = run_actor(GOOGLE_SERP, {
            'queries': q,
            'maxPagesPerQuery': 1,
            'resultsPerPage': 5,
            'countryCode': 'us',
        })
        if items is None:
            print("  [OUT OF CREDITS] Stopping.")
            out_of_credits = True
            break
        if items:
            for item in items:
                for result in item.get('organicResults', [item]):
                    url = result.get('url', result.get('link', ''))
                    title = result.get('title', '')
                    desc = result.get('description', '')
                    if 'linkedin.com/in/' in url and url not in xray_found:
                        # Check the result mentions the company
                        combined = (title + ' ' + desc).lower()
                        co_words = co_short.lower().split()
                        if any(w in combined for w in co_words if len(w) > 3):
                            xray_found[url] = {'title': title, 'desc': desc}
        time.sleep(random.uniform(2, 3))

    if out_of_credits:
        break

    if xray_found:
        print(f"  X-ray: {len(xray_found)} LinkedIn profiles found")
        for url, info in xray_found.items():
            print(f"    → {url[:60]} | {info['title'][:50]}")
    else:
        print(f"  X-ray: 0 results")

    # ── Step 3: Verify via profile scrape (ZoomInfo contacts + X-ray contacts) ──
    # First, try to get LinkedIn URLs for ZoomInfo contacts via Apollo or X-ray
    verified = []

    # Verify ZoomInfo contacts
    for fc in found_contacts:
        first, last, title = fc['first'], fc['last'], fc['title']
        li_url = ''

        # Try Apollo cross-match
        try:
            apollo_resp = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                headers=apollo_h, json={
                    'q_keywords': f'{first} {last} {co_name}',
                    'per_page': 1,
                }, timeout=30)
            people = apollo_resp.json().get('people', [])
            if people:
                p = people[0]
                li_url = normalize_url(p.get('linkedin_url', ''))
                fc['apollo_id'] = p.get('id', '')
                fc['email'] = p.get('email', '')
        except Exception as e:
            print(f"    Apollo error: {e}")

        time.sleep(0.5)

        # Fallback: check X-ray results for name match
        if not li_url:
            for url, info in xray_found.items():
                combined = (info['title'] + ' ' + info['desc']).lower()
                if last.lower() in combined:
                    li_url = normalize_url(url.split('?')[0])
                    break

        if not li_url:
            # Try direct X-ray for this person
            items = run_actor(GOOGLE_SERP, {
                'queries': f'site:linkedin.com/in "{first} {last}" "{co_short.split()[0]}"',
                'maxPagesPerQuery': 1,
                'resultsPerPage': 3,
                'countryCode': 'us',
            })
            if items is None:
                out_of_credits = True
                break
            if items:
                for item in items:
                    for result in item.get('organicResults', [item]):
                        url = result.get('url', result.get('link', ''))
                        if 'linkedin.com/in/' in url:
                            li_url = normalize_url(url.split('?')[0])
                            break
                    if li_url: break
            time.sleep(random.uniform(2, 3))

        if not li_url:
            # Last resort: broader X-ray with just the name
            print(f"    X-ray name search for {first} {last}...")
            items = run_actor(GOOGLE_SERP, {
                'queries': f'site:linkedin.com/in "{first} {last}"',
                'maxPagesPerQuery': 1,
                'resultsPerPage': 5,
                'countryCode': 'us',
            })
            if items is None:
                out_of_credits = True
                break
            if items:
                for item in items:
                    for result in item.get('organicResults', [item]):
                        url = result.get('url', result.get('link', ''))
                        snippet = (result.get('title', '') + ' ' + result.get('description', '')).lower()
                        co_words = [w for w in co_short.lower().split() if len(w) > 3]
                        if 'linkedin.com/in/' in url and any(w in snippet for w in co_words):
                            li_url = normalize_url(url.split('?')[0])
                            break
                    if li_url: break
            time.sleep(random.uniform(2, 3))

        if not li_url:
            print(f"    ⚠️  {first} {last} - no LinkedIn URL found anywhere, skipping")
            continue

        # Scrape profile
        print(f"    Verifying {first} {last} at {li_url[:50]}...")
        prof_items = run_actor(PROFILE_SCRAPER, {'urls': [li_url]})
        if not prof_items:
            print(f"    ❌ Profile scrape failed")
            verified.append({
                'Company': co_name,
                'Company Location': co_info['location'],
                'Company ICP Score': co_info['icp_score'],
                'Industry': co_info['industry'],
                'First Name': first,
                'Last Name': last,
                'Title (ZoomInfo)': title,
                'LinkedIn URL': li_url,
                'LinkedIn Headline': '',
                'Current Company (LinkedIn)': '',
                'Verified at Company': 'Scrape failed',
                'Source': fc['source'] + ' + Apollo',
                'ZoomInfo Contact ID': fc.get('zi_id', ''),
                'ZoomInfo Accuracy': fc.get('accuracy', ''),
                'Apollo Person ID': fc.get('apollo_id', ''),
                'Email': fc.get('email', ''),
            })
            continue

        prof = prof_items[0]
        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''
        headline = prof.get('headline', '')

        co_check = co_name.lower()[:8]
        at_company = co_check in (current_co or '').lower() or co_check in (headline or '').lower()

        status = 'YES - Verified' if at_company else f'NO - At: {current_co}'
        print(f"    {first} {last} | {headline[:50]} | At company: {status}")

        verified.append({
            'Company': co_name,
            'Company Location': co_info['location'],
            'Company ICP Score': co_info['icp_score'],
            'Industry': co_info['industry'],
            'First Name': prof.get('firstName', first),
            'Last Name': prof.get('lastName', last),
            'Title (ZoomInfo)': title,
            'LinkedIn URL': li_url,
            'LinkedIn Headline': headline,
            'Current Company (LinkedIn)': current_co,
            'Verified at Company': status,
            'Source': fc['source'] + (' + Apollo' if fc.get('apollo_id') else ' + X-ray'),
            'ZoomInfo Contact ID': fc.get('zi_id', ''),
            'ZoomInfo Accuracy': fc.get('accuracy', ''),
            'Apollo Person ID': fc.get('apollo_id', ''),
            'Email': fc.get('email', ''),
        })
        time.sleep(1)

    if out_of_credits:
        all_results.extend(verified)
        break

    # Also verify X-ray-only contacts (not already found via ZoomInfo)
    finance_snippet_kw = ['cfo', 'chief financial', 'controller', 'vp finance', 'vp of finance',
                          'vice president of finance', 'vice president, finance', 'director of finance',
                          'director, finance', 'finance director', 'financial controller',
                          'treasurer', 'accounting manager', 'finance manager']
    zi_names = set(f"{fc['first'].lower()} {fc['last'].lower()}" for fc in found_contacts)
    for url, info in xray_found.items():
        norm_url = normalize_url(url.split('?')[0])
        # Skip if already verified from ZoomInfo flow
        if any(v.get('LinkedIn URL') == norm_url for v in verified):
            continue

        # Pre-filter: check snippet for finance keywords BEFORE scraping
        snippet = (info.get('title', '') + ' ' + info.get('desc', '')).lower()
        if not any(k in snippet for k in finance_snippet_kw):
            snippet_title = info.get('title', '')[:50]
            print(f"    Skipping X-ray (non-finance snippet): {snippet_title}")
            continue

        print(f"    Verifying X-ray find: {norm_url[:50]}...")
        prof_items = run_actor(PROFILE_SCRAPER, {'urls': [norm_url]})
        if not prof_items:
            continue

        prof = prof_items[0]
        first = prof.get('firstName', '')
        last = prof.get('lastName', '')
        headline = prof.get('headline', '')
        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''

        # Skip if same person already from ZoomInfo
        if f"{first.lower()} {last.lower()}" in zi_names:
            continue

        # Check if finance title
        finance_kw = ['cfo', 'chief financial', 'controller', 'vp finance', 'vp of finance',
                      'vice president of finance', 'vice president, finance', 'director of finance',
                      'director, finance', 'finance director', 'financial controller',
                      'treasurer', 'accounting manager', 'finance manager']
        is_finance = any(k in headline.lower() for k in finance_kw)
        if not is_finance:
            print(f"    Skipping {first} {last} - not finance title: {headline[:50]}")
            continue

        co_check = co_name.lower()[:8]
        at_company = co_check in (current_co or '').lower() or co_check in (headline or '').lower()
        status = 'YES - Verified' if at_company else f'NO - At: {current_co}'
        print(f"    {first} {last} | {headline[:50]} | At company: {status}")

        verified.append({
            'Company': co_name,
            'Company Location': co_info['location'],
            'Company ICP Score': co_info['icp_score'],
            'Industry': co_info['industry'],
            'First Name': first,
            'Last Name': last,
            'Title (ZoomInfo)': '',
            'LinkedIn URL': norm_url,
            'LinkedIn Headline': headline,
            'Current Company (LinkedIn)': current_co,
            'Verified at Company': status,
            'Source': 'Google X-ray + Profile verified',
            'ZoomInfo Contact ID': '',
            'ZoomInfo Accuracy': '',
            'Apollo Person ID': '',
            'Email': '',
        })
        time.sleep(1)

    all_results.extend(verified)

    # Save progress after each company
    if all_results:
        out_headers = list(all_results[0].keys())
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=out_headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_results)

# ═══════════════════════════════════════
# Summary
# ═══════════════════════════════════════
print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"Companies searched: {len(companies)}")
print(f"Total contacts found: {len(all_results)}")
verified_yes = sum(1 for r in all_results if r.get('Verified at Company', '').startswith('YES'))
verified_no = sum(1 for r in all_results if r.get('Verified at Company', '').startswith('NO'))
unverified = len(all_results) - verified_yes - verified_no
print(f"  Verified at company: {verified_yes}")
print(f"  NOT at company (stale): {verified_no}")
print(f"  Unverified: {unverified}")

cos_with_results = len(set(r['Company'] for r in all_results))
cos_with_verified = len(set(r['Company'] for r in all_results if r.get('Verified at Company', '').startswith('YES')))
print(f"\nCompanies with any results: {cos_with_results}")
print(f"Companies with VERIFIED CFO/Controller: {cos_with_verified}")
print(f"Still no CFO/Controller: {len(companies) - cos_with_verified}")
print(f"\nOutput: {OUTPUT_CSV}")
