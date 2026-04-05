#!/usr/bin/env python3
"""Find finance contacts via ZoomInfo for 54 companies where Apollo only returned executives.
Uses zip code validation, then cross-matches with Apollo/X-ray for LinkedIn URLs."""
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
ACTIVITY_INDEX = 'kog75ERz9lcVNujbQ'

# City → zip code mapping for PNW
CITY_ZIP = {
    'seattle': '98101', 'bellevue': '98004', 'tacoma': '98402',
    'redmond': '98052', 'kirkland': '98033', 'everett': '98201',
    'renton': '98057', 'kent': '98032', 'auburn': '98002',
    'olympia': '98501', 'lynnwood': '98036', 'lakewood': '98499',
    'federal way': '98003', 'vancouver': '98660', 'ferndale': '98248',
    'puyallup': '98371', 'spokane': '98201', 'portland': '97201',
}

def get_zip(location):
    loc = (location or '').lower().strip()
    for city, zipcode in CITY_ZIP.items():
        if city in loc:
            return zipcode
    return '98101'  # Default Seattle


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


def normalize_url(url):
    url = (url or '').strip().replace('http://', 'https://')
    if 'https://linkedin.com' in url:
        url = url.replace('https://linkedin.com', 'https://www.linkedin.com')
    return url


# ═══════════════════════════════════════
# Load data
# ═══════════════════════════════════════

contacts_f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'proceed_contacts_enriched_v2.csv')
with open(contacts_f) as fh:
    contacts = list(csv.DictReader(fh))

companies_f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'all_proceed_companies.csv')
with open(companies_f) as fh:
    all_cos = {c['Company']: c for c in csv.DictReader(fh)}

# Find 54 companies without finance contacts
finance_keywords = ['cfo', 'chief financial', 'controller', 'vp finance', 'director of finance',
                    'accounting', 'treasurer', 'finance director']
companies_with_finance = set()
for r in contacts:
    if any(k in (r.get('Title', '') or '').lower() for k in finance_keywords):
        companies_with_finance.add(r.get('Company', ''))

no_finance_companies = []
for co_name in sorted(set(r.get('Company', '') for r in contacts) - companies_with_finance):
    co = all_cos.get(co_name, {})
    no_finance_companies.append({
        'name': co_name,
        'location': co.get('Location', ''),
        'domain': co.get('Domain', ''),
        'icp_score': co.get('Company ICP Score', ''),
        'category': co.get('Category', ''),
        'industry': co.get('Industry', ''),
        'li_url': co.get('Company LinkedIn URL', ''),
        'li_followers': co.get('LI Followers', ''),
    })

print(f"Companies without finance contacts: {len(no_finance_companies)}")

# ZoomInfo auth
zi_user = os.environ.get('ZOOMINFO_USERNAME', '')
zi_pass = os.environ.get('ZOOMINFO_PASSWORD', '')
auth = requests.post('https://api.zoominfo.com/authenticate',
                     json={'username': zi_user, 'password': zi_pass}, timeout=15)
token = auth.json().get('jwt', '')
zi_h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
print(f"ZoomInfo auth: {'OK' if token else 'FAILED'}")

# ═══════════════════════════════════════
# Pipeline
# ═══════════════════════════════════════

all_verified = []
MAX_PER_COMPANY = 3

for i, co in enumerate(no_finance_companies):
    name = co['name']
    zipcode = get_zip(co['location'])
    domain = co['domain']

    print(f"\n[{i+1}/{len(no_finance_companies)}] {name} (zip: {zipcode})")
    print("-" * 50)

    # ── Step 1: ZoomInfo contact search with zip code ──
    try:
        resp = requests.post('https://api.zoominfo.com/search/contact', headers=zi_h, json={
            'companyName': name,
            'jobTitle': 'CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance',
            'zipCode': zipcode,
            'zipCodeRadiusMiles': '50',
            'rpp': 5,
        }, timeout=15)
        zi_contacts = resp.json().get('data', [])
    except Exception as e:
        print(f"  ZoomInfo error: {e}")
        zi_contacts = []

    print(f"  Step 1 - ZoomInfo: {len(zi_contacts)} finance contacts")

    if not zi_contacts:
        continue

    verified_this_company = 0
    for zc in zi_contacts:
        if verified_this_company >= MAX_PER_COMPANY:
            break

        first = zc.get('firstName', '')
        last = zc.get('lastName', '')
        zi_title = zc.get('jobTitle', '')
        zi_contact_id = str(zc.get('id', ''))
        zi_accuracy = zc.get('contactAccuracyScore', '')

        print(f"  → {first} {last} | {zi_title} | Accuracy: {zi_accuracy}")

        # ── Step 2: Apollo cross-match for LinkedIn URL + Apollo ID ──
        li_url = ''
        apollo_id = ''
        email = ''

        try:
            apollo_resp = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                headers=apollo_h, json={
                    'q_keywords': f'{first} {last} {name}',
                    'per_page': 1,
                }, timeout=30)
            people = apollo_resp.json().get('people', [])
            if people:
                p = people[0]
                apollo_id = p.get('id', '')
                li_url = normalize_url(p.get('linkedin_url', ''))
                email = p.get('email', '')
                print(f"    Apollo: ✅ ID={apollo_id[:12]}... | LI={li_url[:50]}... | Email={email or 'none'}")
            else:
                print(f"    Apollo: ❌ No match")
        except Exception as e:
            print(f"    Apollo error: {e}")

        time.sleep(0.5)

        # ── Step 3: Google X-ray fallback (if no LinkedIn from Apollo) ──
        if not li_url:
            print(f"    X-ray fallback...")
            co_short = name.split()[0] if name else ''
            items = run_actor(GOOGLE_SERP, {
                'queries': f'site:linkedin.com/in "{first} {last}" "{co_short}"',
                'maxPagesPerQuery': 1,
                'resultsPerPage': 3,
                'countryCode': 'us',
            })
            if items is None:
                print("    [OUT OF CREDITS] Stopping.")
                break
            if items:
                for item in items:
                    for result in item.get('organicResults', [item]):
                        url = result.get('url', result.get('link', ''))
                        if 'linkedin.com/in/' in url:
                            li_url = normalize_url(url.split('?')[0])
                            break
                    if li_url: break
            if li_url:
                print(f"    X-ray: ✅ {li_url[:60]}")
            else:
                print(f"    X-ray: ❌ No LinkedIn found, skipping")
                continue

        # ── Step 4: Apify profile scraper — verify company + location ──
        print(f"    Verifying profile...")
        prof_items = run_actor(PROFILE_SCRAPER, {'urls': [li_url]})
        if not prof_items:
            print(f"    Profile: ❌ Scrape failed")
            continue

        prof = prof_items[0]
        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''
        headline = prof.get('headline', '')
        prof_location = prof.get('location', '')

        # Verify company match
        co_short = name.lower()[:8]
        if current_co and co_short in current_co.lower():
            role_verified = 'Yes'
        elif current_co:
            role_verified = f'Check - LI: {headline[:40]}'
        else:
            role_verified = 'Profile found - no current position'

        # Verify location (PNW area)
        pnw_keywords = ['washington', 'seattle', 'tacoma', 'everett', 'bellevue', 'renton',
                         'kirkland', 'redmond', 'kent', 'auburn', 'olympia', 'lynnwood',
                         'portland', 'oregon', 'pacific northwest', 'greater seattle']
        # prof_location can be a string or dict
        if isinstance(prof_location, dict):
            prof_location = prof_location.get('default', prof_location.get('city', str(prof_location)))
        loc_match = any(k in (str(prof_location) or '').lower() for k in pnw_keywords)

        print(f"    Profile: {prof.get('firstName','')} {prof.get('lastName','')} | {headline[:50]}")
        print(f"    Company: {current_co} | Location: {prof_location}")
        print(f"    Verified: {role_verified} | PNW: {'✅' if loc_match else '⚠️ ' + prof_location}")

        if role_verified != 'Yes':
            print(f"    ❌ Skipping — company mismatch")
            continue

        # ── Step 5: Activity Index ──
        print(f"    Activity check...")
        act_items = run_actor(ACTIVITY_INDEX, {'linkedinUrl': li_url})
        activity = {}
        if act_items and act_items[0].get('success'):
            d = act_items[0]
            score = d.get('activity_score', 0)
            metrics = d.get('activity_metrics', {})
            if score >= 7: level = 'Very Active'
            elif score >= 5: level = 'Active'
            elif score >= 3: level = 'Moderate'
            elif score >= 1: level = 'Low'
            else: level = 'Inactive'
            activity = {
                'Activity Score': str(score),
                'Activity Level': level,
                'Activity Recommendation': d.get('recommendation', ''),
                'Activity Insights': ' | '.join(d.get('insights', [])),
                'Posts Last 30 Days': str(metrics.get('posts_last_30_days', 0)),
                'Reactions Last 30 Days': str(metrics.get('reactions_last_30_days', 0)),
                'Last Activity Date': metrics.get('last_activity_date', ''),
                'Days Since Last Activity': str(metrics.get('days_since_last_activity', '')),
                'LinkedIn Active Status': f'{score}/10 {level}',
            }
            print(f"    Activity: {score}/10 {level}")
        else:
            activity = {'Activity Score': '0', 'Activity Level': 'Error', 'LinkedIn Active Status': 'Check failed'}
            print(f"    Activity: Error")

        # Build contact row
        contact = {
            'Company ICP Score': co['icp_score'],
            'Pipeline Action': 'PROCEED',
            'Category': co['category'],
            'Company': name,
            'Industry': co['industry'],
            'Company Location': co['location'],
            'Company LinkedIn URL': co['li_url'],
            'Company LI Followers': co['li_followers'],
            'First Name': prof.get('firstName', first),
            'Last Name': prof.get('lastName', last),
            'Title': headline or zi_title,
            'Seniority': '',
            'LinkedIn URL': li_url,
            'LinkedIn Headline': headline,
            'Role Verified': role_verified,
            'LinkedIn Connections': str(prof.get('connectionsCount', '')),
            'LinkedIn Followers': str(prof.get('followerCount', '')),
            'Open to Work': 'Yes' if prof.get('openToWork') else 'No',
            'Email': email,
            'Email Status': '',
            'Apollo Person ID': apollo_id,
            'Apollo Company ID': '',
            'ZoomInfo Company ID': '',  # Free tier doesn't reliably return this
            'ZoomInfo Contact ID': zi_contact_id,
            'Data Source': f'ZoomInfo (zip:{zipcode}) + {"Apollo" if apollo_id else "X-ray"} + Profile verified',
            **activity,
        }
        all_verified.append(contact)
        verified_this_company += 1
        print(f"    ✅ ADDED — {first} {last} | {zi_title}")
        time.sleep(1)

    # Save progress after each company
    if all_verified:
        progress_f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'zoominfo_finance_contacts.csv')
        out_headers = list(all_verified[0].keys())
        with open(progress_f, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=out_headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_verified)

# ═══════════════════════════════════════
# Summary
# ═══════════════════════════════════════

print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"Companies searched: {len(no_finance_companies)}")
print(f"Verified finance contacts found: {len(all_verified)}")
cos_covered = len(set(c['Company'] for c in all_verified))
print(f"Companies with new finance contacts: {cos_covered}")
print(f"Still missing: {len(no_finance_companies) - cos_covered}")

if all_verified:
    from collections import Counter
    titles = Counter(c.get('Title', '') for c in all_verified)
    print(f"\nTitles found:")
    for t, cnt in titles.most_common():
        print(f"  {t}: {cnt}")

    has_apollo = sum(1 for c in all_verified if c.get('Apollo Person ID'))
    has_email = sum(1 for c in all_verified if c.get('Email'))
    print(f"\nWith Apollo ID: {has_apollo}/{len(all_verified)}")
    print(f"With email: {has_email}/{len(all_verified)}")

    print(f"\nSaved: {progress_f}")
