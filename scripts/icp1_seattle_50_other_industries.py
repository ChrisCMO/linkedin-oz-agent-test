#!/usr/bin/env python3
"""Run company-level pipeline on 50 Seattle companies from Prof Services, Hospitality, Nonprofit, CRE."""
import sys, os, time, json, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
APIFY_TOKEN = os.environ['APIFY_API_KEY']
OPENAI_KEY = os.environ['OPENAI_API_KEY']

apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

COMPANY_SCRAPER = 'UwSdACBp7ymaGUJjS'
SERP = 'nFJndFXA5zjCTuudP'

seattle_metro = ['seattle', 'bellevue', 'tacoma', 'redmond', 'kirkland', 'everett', 'renton',
    'kent', 'auburn', 'olympia', 'federal way', 'tukwila', 'shoreline', 'bothell',
    'issaquah', 'puyallup', 'lynnwood', 'woodinville', 'kenmore', 'burien', 'seatac',
    'sammamish', 'lakewood', 'sumner', 'bainbridge']


def run_actor(actor_id, payload):
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                      headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201:
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(30):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                         headers=apify_h, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
            break
    return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                        headers=apify_h, timeout=15).json()


# ── Load companies from each industry (Google Places format) ──
all_companies = []
seen = set()

for fname, category in [
    ('3_Professional_Services.csv', 'Professional Services'),
    ('4_Hospitality.csv', 'Hospitality'),
    ('5_Nonprofit.csv', 'Nonprofit'),
]:
    count = 0
    target = 17 if category != 'Nonprofit' else 16  # 17+17+16 = 50
    with open(f'docs/ICP-Prospects/icp1_by_industry/{fname}') as f:
        for row in csv.DictReader(f):
            if count >= target:
                break
            co = (row.get('company_name') or '').strip()
            city = (row.get('city') or '').lower().strip()
            domain = (row.get('domain') or '').strip()
            address = (row.get('address') or '').strip()
            phone = (row.get('phone') or '').strip()
            rating = (row.get('rating') or '').strip()
            reviews = (row.get('review_count') or '').strip()

            if co and co not in seen and any(c in city for c in seattle_metro):
                # Skip generic/chain businesses
                if any(skip in co.lower() for skip in ['starbucks', 'mcdonald', 'subway', 'walmart', 'target', 'home depot']):
                    continue
                seen.add(co)
                all_companies.append({
                    'name': co,
                    'category': category,
                    'industry': category.lower(),
                    'domain': domain,
                    'address': address,
                    'phone': phone,
                    'rating': rating,
                    'reviews': reviews,
                    'location': f'{city.title()}, WA',
                    'li_url': '',
                    'li_employees': '',
                    'li_followers': '',
                    'apollo_employees': '',
                    'revenue': '',
                })
                count += 1

print(f"Selected companies:")
for cat in ['Professional Services', 'Hospitality', 'Nonprofit']:
    n = sum(1 for c in all_companies if c['category'] == cat)
    print(f"  {cat}: {n}")
print(f"  Total: {len(all_companies)}")

# ── Step 1: Find LinkedIn company pages via Google X-ray ──
print("\nFinding LinkedIn company pages...")
for c in all_companies:
    words = c['name'].split()
    q = 'site:linkedin.com/company "' + ' '.join(words[:3]) + '"'
    items = run_actor(SERP, {'queries': q, 'maxPagesPerQuery': 1, 'resultsPerPage': 3, 'countryCode': 'us'})
    for item in items:
        for r2 in item.get('organicResults', []):
            url = r2.get('url', '')
            title = r2.get('title', '')
            if 'linkedin.com/company' in url and words[0].lower() in title.lower():
                c['li_url'] = url.split('?')[0]
                print(f"  {c['name']}: {c['li_url']}")
                break
        if c['li_url']:
            break
    if not c['li_url']:
        print(f"  {c['name']}: NOT FOUND")
    time.sleep(3)

# ── Step 2: Scrape LinkedIn company pages ──
li_urls = [c['li_url'] for c in all_companies if c['li_url']]
print(f"\nScraping {len(li_urls)} LinkedIn company pages...")

all_pages = []
for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20 + 1}: {len(batch)} URLs")
    pages = run_actor(COMPANY_SCRAPER, {'companies': batch})
    all_pages.extend(pages)
    time.sleep(2)

print(f"  Got {len(all_pages)} pages")

page_map = {}
for p in all_pages:
    name = (p.get('name') or '').strip()
    if name:
        page_map[name.lower()] = p

# Match pages to companies
for c in all_companies:
    pd = None
    for key, val in page_map.items():
        if c['name'].split()[0].lower() in key:
            pd = val
            break
    if pd:
        c['li_employees'] = pd.get('employeeCount', '')
        c['li_followers'] = pd.get('followerCount', '')
        # Update location from LinkedIn if available
        for loc in (pd.get('locations') or []):
            city = loc.get('city', '')
            state = loc.get('geographicArea', '')
            if city:
                c['location'] = f'{city}, {state}'
                break
        website = pd.get('website', '')
        if website and not c['domain']:
            c['domain'] = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]

# ── Step 3: Apollo org enrichment ──
print("\nApollo org enrichment...")
for c in all_companies:
    if c.get('domain'):
        r = requests.post('https://api.apollo.io/api/v1/organizations/enrich',
                          headers=apollo_h, json={'domain': c['domain']}, timeout=30)
        org = r.json().get('organization', {}) or {}
        if org.get('name'):
            c['apollo_employees'] = org.get('estimated_num_employees', '')
            rev = org.get('annual_revenue')
            c['revenue'] = f'${int(rev/1e6)}M' if rev and rev >= 1e6 else ''
            if org.get('industry'):
                c['industry'] = org.get('industry')
        time.sleep(0.5)

# ── Step 4: Score ──
print("\nScoring...")
prompt = open('mvp/backend/services/scoring.py').read().split('COMPANY_SYSTEM_PROMPT = """')[1].split('"""')[0]

all_scores = []
for i in range(0, len(all_companies), 15):
    batch = all_companies[i:i+15]
    score_input = []
    for c in batch:
        score_input.append({
            'company_id': c['name'].lower().replace(' ', '_')[:25],
            'company_name': c['name'],
            'industry': c.get('industry', ''),
            'linkedin_employees': c.get('li_employees', ''),
            'apollo_employees': c.get('apollo_employees', ''),
            'revenue': c.get('revenue', ''),
            'location': c.get('location', ''),
            'ownership': 'Unknown - appears private',
            'linkedin_page': c.get('li_url', ''),
            'linkedin_followers': c.get('li_followers', ''),
            'website': c.get('domain', ''),
            'finance_contacts_found': '',
        })

    try:
        resp = requests.post('https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
            json={'model': 'gpt-5.4', 'messages': [
                {'role': 'system', 'content': prompt},
                {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
            ], 'temperature': 0.3, 'max_completion_tokens': 8000}, timeout=90)

        resp_json = resp.json()
        if 'choices' not in resp_json:
            print(f"  Batch {i//15 + 1}: API error - {str(resp_json.get('error', resp_json))[:100]}")
            time.sleep(5)
            # Retry once
            resp = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-5.4', 'messages': [
                    {'role': 'system', 'content': prompt},
                    {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
                ], 'temperature': 0.3, 'max_completion_tokens': 8000}, timeout=90)
            resp_json = resp.json()

        raw = resp_json['choices'][0]['message']['content'].strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        scored = json.loads(raw)
        scores = scored.get('companies', scored if isinstance(scored, list) else [])
        all_scores.extend(scores)
        print(f"  Batch {i//15 + 1}: scored {len(scores)}")
    except Exception as e:
        print(f"  Batch {i//15 + 1}: FAILED - {e}")
    time.sleep(2)

score_map = {s.get('company_name', ''): s for s in all_scores}

# ── Build output ──
output_headers = ['Category', 'Company', 'Company ICP Score', 'Pipeline Action',
    'Industry', 'Employees (LinkedIn)', 'Employees (Apollo)', 'Revenue',
    'Location', 'Ownership', 'Company LinkedIn URL', 'LI Followers',
    'Website', 'Contacts Found', 'Score Breakdown', 'Reasoning', 'Notes']

rows = []
for c in all_companies:
    sc = score_map.get(c['name'], {})
    score = sc.get('score', '')
    bd = sc.get('breakdown', {})

    if score == 0: action = 'HARD EXCLUDE'
    elif score and score >= 80: action = 'PROCEED'
    elif score and score >= 60: action = 'FLAG'
    elif score and score < 60: action = 'SKIP'
    else: action = ''

    rows.append({
        'Category': c['category'],
        'Company': c['name'],
        'Company ICP Score': score,
        'Pipeline Action': action,
        'Industry': c.get('industry', ''),
        'Employees (LinkedIn)': c.get('li_employees', ''),
        'Employees (Apollo)': c.get('apollo_employees', ''),
        'Revenue': c.get('revenue', ''),
        'Location': c.get('location', ''),
        'Ownership': 'Unknown - appears private',
        'Company LinkedIn URL': c.get('li_url', ''),
        'LI Followers': c.get('li_followers', ''),
        'Website': c.get('domain', ''),
        'Contacts Found': '',
        'Score Breakdown': ' | '.join(f'{k}: {v}' for k, v in bd.items()),
        'Reasoning': sc.get('reasoning', ''),
        'Notes': '',
    })

rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if isinstance(x['Company ICP Score'], int) else 0)))

# Append to existing file
existing = []
existing_file = 'docs/TODO/seattle_100_company_pipeline_results.csv'
with open(existing_file) as f:
    reader = csv.DictReader(f)
    existing = list(reader)

combined = existing + rows
with open(existing_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    # Sort combined by score
    combined.sort(key=lambda x: (-(int(x['Company ICP Score']) if str(x['Company ICP Score']).isdigit() else 0)))
    writer.writerows(combined)

proceed = sum(1 for r in rows if r['Pipeline Action'] == 'PROCEED')
flag = sum(1 for r in rows if r['Pipeline Action'] == 'FLAG')
skip = sum(1 for r in rows if r['Pipeline Action'] == 'SKIP')
exclude = sum(1 for r in rows if r['Pipeline Action'] == 'HARD EXCLUDE')

print(f"\n{'='*80}")
print(f"NEW RESULTS: {len(rows)} companies scored")
print(f"  PROCEED (80+): {proceed}")
print(f"  FLAG (60-79):  {flag}")
print(f"  SKIP (<60):    {skip}")
print(f"  HARD EXCLUDE:  {exclude}")
print(f"\nCOMBINED TOTAL: {len(combined)} companies in {existing_file}")
print(f"  PROCEED: {sum(1 for r in combined if r['Pipeline Action'] == 'PROCEED')}")
print(f"  FLAG:    {sum(1 for r in combined if r['Pipeline Action'] == 'FLAG')}")
print(f"  SKIP:    {sum(1 for r in combined if r['Pipeline Action'] == 'SKIP')}")
print(f"  EXCLUDE: {sum(1 for r in combined if r['Pipeline Action'] == 'HARD EXCLUDE')}")

print(f"\nTop 15 new companies:")
for r in rows[:15]:
    print(f"  {r['Company ICP Score']:>3} {r['Pipeline Action']:>12}  {r['Category']:<22} {r['Company']:<40} {str(r['Employees (LinkedIn)']):>5} LI emp  {r['Location']}")
