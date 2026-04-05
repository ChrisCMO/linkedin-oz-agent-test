#!/usr/bin/env python3
"""ICP 2 sample enrichment - 10 companies + 2 benchmarks."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import requests, time, json, csv

APOLLO_KEY = os.environ['APOLLO_API_KEY']
token = os.environ['APIFY_API_KEY']
apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}
apify_h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
COMPANY_SCRAPER = 'UwSdACBp7ymaGUJjS'
SERP = 'nFJndFXA5zjCTuudP'

def run_actor(actor_id, payload):
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs', headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201:
        print(f'  Actor failed: {r.status_code}')
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(24):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}', headers=apify_h, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
            break
    return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items', headers=apify_h, timeout=15).json()

companies = [
    ('Precision Machine & Manufacturing', 'Manufacturing'),
    ('Swanson Group', 'Manufacturing/Timber'),
    ('Collins Companies', 'Manufacturing'),
    ('Springbrook Software', 'Technology'),
    ('DrFirst', 'Technology'),
    ('Concord Hospitality', 'Hospitality'),
    ('Columbia Memorial Hospital', 'Healthcare'),
    ('Willamette University', 'Education'),
    ('Trailhead Credit Union', 'Banking/Credit Union'),
    ('NOVA Parks', 'Government'),
]

# Step 1: Find LinkedIn pages
print('Finding LinkedIn company pages...')
li_urls = {}
for co, cat in companies:
    words = co.split()
    q = 'site:linkedin.com/company "' + ' '.join(words[:2]) + '"'
    items = run_actor(SERP, {'queries': q, 'maxPagesPerQuery': 1, 'resultsPerPage': 3, 'countryCode': 'us'})
    for item in items:
        for r2 in item.get('organicResults', []):
            url = r2.get('url', '')
            title = r2.get('title', '')
            if 'linkedin.com/company' in url and words[0].lower() in title.lower():
                li_urls[co] = url.split('?')[0]
                print(f'  {co}: {li_urls[co]}')
                break
        if co in li_urls:
            break
    if co not in li_urls:
        print(f'  {co}: NOT FOUND')
    time.sleep(3)

# Step 2: Scrape pages
found_urls = list(set(li_urls.values()))
print(f'\nScraping {len(found_urls)} pages...')
pages = run_actor(COMPANY_SCRAPER, {'companies': found_urls})
print(f'Got {len(pages)} pages')

page_map = {}
for p in pages:
    name = (p.get('name') or '').strip()
    page_map[name.lower()] = p

# Step 3: Build results
all_results = [
    {'name': 'Shannon & Wilson', 'category': 'BENCHMARK', 'li_employees': 368, 'apollo_employees': 320,
     'revenue': '$28M', 'location': 'Seattle, Washington', 'industry': 'Professional services (civil engineering)',
     'ownership': 'Employee-owned (ESOP)', 'li_url': 'http://www.linkedin.com/company/shannon-&-wilson-inc-',
     'li_followers': 6836, 'website': 'shannonwilson.com', 'contacts': '10 via Apollo, 3 via ZoomInfo',
     'notes': 'VWC BENCHMARK. ESOP. 2 benefit plan audits.'},
    {'name': 'Skills Inc.', 'category': 'BENCHMARK', 'li_employees': 361, 'apollo_employees': 430,
     'revenue': '$25M', 'location': 'Auburn, Washington', 'industry': 'Nonprofit / Aerospace manufacturing',
     'ownership': 'Nonprofit', 'li_url': 'http://www.linkedin.com/company/skills-inc-',
     'li_followers': 2083, 'website': 'skillsinc.com', 'contacts': '3 via Apollo, 3 via X-ray',
     'notes': 'VWC BENCHMARK. Benefit plan audit.'},
]

for co, cat in companies:
    pd = None
    for key, val in page_map.items():
        if co.split()[0].lower() in key:
            pd = val
            break

    li_emp = pd.get('employeeCount', '') if pd else ''
    li_fol = pd.get('followerCount', '') if pd else ''
    website = (pd.get('website', '') or '') if pd else ''
    domain = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if website else ''

    hq = ''
    if pd:
        for loc in (pd.get('locations') or []):
            city = loc.get('city', '')
            state = loc.get('geographicArea', '')
            if city:
                hq = f'{city}, {state}'
                break

    apollo_org = {}
    if domain:
        r = requests.post('https://api.apollo.io/api/v1/organizations/enrich', headers=apollo_h,
                          json={'domain': domain}, timeout=30)
        apollo_org = r.json().get('organization', {}) or {}
        time.sleep(1)

    rev = apollo_org.get('annual_revenue')
    rev_str = f'${int(rev / 1e6)}M' if rev and rev >= 1e6 else ''
    apo_loc = f"{apollo_org.get('city', '')}, {apollo_org.get('state', '')}".strip(', ')

    all_results.append({
        'name': co, 'category': cat,
        'li_employees': li_emp,
        'apollo_employees': apollo_org.get('estimated_num_employees', ''),
        'revenue': rev_str,
        'location': hq or apo_loc,
        'industry': apollo_org.get('industry', cat),
        'ownership': '',
        'li_url': li_urls.get(co, ''),
        'li_followers': li_fol,
        'website': domain,
        'contacts': '',
        'notes': 'Technology - included in ICP 2' if cat == 'Technology' else (
            'TEST: Should be excluded' if cat in ('Banking/Credit Union', 'Government') else ''),
    })
    print(f'{co}: LI {li_emp} emp, Apollo {apollo_org.get("estimated_num_employees", "?")}, {rev_str}, {hq}')

# Save
output_headers = ['Category', 'Company', 'Company ICP Score', 'Pipeline Action',
                   'Industry', 'Employees (LinkedIn)', 'Employees (Apollo)', 'Revenue',
                   'Location', 'Ownership', 'Company LinkedIn URL', 'LI Followers',
                   'Website', 'Contacts Found', 'Score Breakdown', 'Reasoning', 'Notes']

outdir = os.path.join(os.path.dirname(__file__), '..', 'docs', 'ICP 2 Prospects')
os.makedirs(outdir, exist_ok=True)
outfile = os.path.join(outdir, 'company_level_icp2_test_results.csv')

rows = []
for r in all_results:
    rows.append({
        'Category': r['category'], 'Company': r['name'],
        'Company ICP Score': '', 'Pipeline Action': '',
        'Industry': r['industry'],
        'Employees (LinkedIn)': r['li_employees'],
        'Employees (Apollo)': r['apollo_employees'],
        'Revenue': r['revenue'], 'Location': r['location'],
        'Ownership': r['ownership'],
        'Company LinkedIn URL': r['li_url'],
        'LI Followers': r['li_followers'],
        'Website': r['website'],
        'Contacts Found': r['contacts'],
        'Score Breakdown': '', 'Reasoning': '', 'Notes': r['notes'],
    })

with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(rows)

print(f'\nSaved: {outfile} ({len(rows)} companies)')
for r in rows:
    li = str(r['Employees (LinkedIn)']).rjust(7)
    ap = str(r['Employees (Apollo)']).rjust(8)
    rv = (r['Revenue'] or '?').rjust(8)
    print(f'  {r["Category"]:<18}{r["Company"]:<35}{li}{ap}{rv}  {r["Location"]}')
