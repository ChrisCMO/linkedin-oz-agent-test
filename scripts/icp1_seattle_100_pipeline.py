#!/usr/bin/env python3
"""Run company-level pipeline on ~100 Seattle construction + manufacturing companies."""
import sys, os, time, json, csv, random
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

seattle_metro = ['seattle', 'bellevue', 'tacoma', 'redmond', 'kirkland', 'everett', 'renton',
    'kent', 'auburn', 'olympia', 'federal way', 'tukwila', 'shoreline', 'bothell',
    'issaquah', 'puyallup', 'lynnwood', 'woodinville', 'kenmore', 'mountlake terrace',
    'burien', 'mercer island', 'seatac', 'sammamish', 'bainbridge', 'lakewood',
    'sumner', 'orting', 'enumclaw', 'covington']


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


def is_seattle_metro(location):
    loc = location.lower()
    return any(city in loc for city in seattle_metro)


# ── Step 1: Load manufacturing (already enriched) ──
print("Loading manufacturing companies...")
mfg_companies = []
seen = set()
with open('docs/ICP-Prospects/icp1_by_industry/manufacturing/seattle_manufacturing_enriched.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        co = (row.get('Company') or '').strip()
        loc = (row.get('Location') or row.get('Company City', '') or '').strip()
        if co and co not in seen and is_seattle_metro(loc):
            seen.add(co)
            emp = row.get('Employees', '')
            rev = row.get('Revenue', '')
            mfg_companies.append({
                'name': co,
                'category': 'Manufacturing',
                'industry': row.get('Industry', 'manufacturing'),
                'apollo_employees': emp,
                'revenue': rev,
                'location': loc,
                'domain': row.get('Company Domain', row.get('Company Website', '')),
                'li_url': '',  # Will get from Apollo or scrape
                'li_employees': '',
                'li_followers': '',
            })

print(f"  Manufacturing Seattle: {len(mfg_companies)}")

# ── Step 2: Load construction (need LinkedIn scrape) ──
print("Loading construction companies...")
construction = []
with open('docs/ICP-Prospects/icp1_by_industry/construction/seattle_construction_linkedin_xray.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        construction.append({
            'name': row.get('Company Name', ''),
            'category': 'Construction',
            'industry': 'construction',
            'li_company_url': row.get('LinkedIn Company URL', ''),
        })

print(f"  Construction: {len(construction)}")

# ── Step 3: Scrape construction LinkedIn company pages (batch) ──
print("\nScraping construction LinkedIn company pages...")
li_urls = [c['li_company_url'] for c in construction if c['li_company_url']]

# Batch in groups of 20
all_pages = []
for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20 + 1}: {len(batch)} URLs")
    pages = run_actor(COMPANY_SCRAPER, {'companies': batch})
    all_pages.extend(pages)
    time.sleep(2)

print(f"  Got {len(all_pages)} company pages")

# Map pages by URL slug or name
page_by_name = {}
for p in all_pages:
    name = (p.get('name') or '').strip()
    if name:
        page_by_name[name.lower()] = p

# Enrich construction companies with LinkedIn data and filter Seattle
construction_seattle = []
for c in construction:
    # Find matching page
    pd = None
    for key, val in page_by_name.items():
        if c['name'].split()[0].lower() in key:
            pd = val
            break

    if not pd:
        continue

    # Get HQ location
    hq = ''
    for loc in (pd.get('locations') or []):
        city = loc.get('city', '')
        state = loc.get('geographicArea', '')
        if city:
            hq = f'{city}, {state}'
            break

    # Filter Seattle metro
    if not is_seattle_metro(hq):
        print(f"  SKIP (not Seattle): {c['name']} -> {hq}")
        continue

    li_emp = pd.get('employeeCount', '')
    li_fol = pd.get('followerCount', '')
    website = (pd.get('website') or '')
    domain = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if website else ''

    c.update({
        'li_employees': li_emp,
        'li_followers': li_fol,
        'li_url': c['li_company_url'],
        'location': hq,
        'domain': domain,
        'apollo_employees': '',
        'revenue': '',
    })
    construction_seattle.append(c)

print(f"  Construction Seattle (verified): {len(construction_seattle)}")

# ── Step 4: Apollo org enrichment for construction companies ──
print("\nApollo org enrichment for construction...")
for c in construction_seattle:
    if c.get('domain'):
        r = requests.post('https://api.apollo.io/api/v1/organizations/enrich',
                          headers=apollo_h, json={'domain': c['domain']}, timeout=30)
        org = r.json().get('organization', {}) or {}
        if org.get('name'):
            c['apollo_employees'] = org.get('estimated_num_employees', '')
            rev = org.get('annual_revenue')
            c['revenue'] = f'${int(rev/1e6)}M' if rev and rev >= 1e6 else ''
            c['industry'] = org.get('industry', 'construction')
        time.sleep(0.5)

# ── Step 5: Apollo org enrichment for manufacturing (get LinkedIn URLs) ──
print("\nApollo org enrichment for manufacturing (LinkedIn URLs)...")
for c in mfg_companies:
    if c.get('domain'):
        r = requests.post('https://api.apollo.io/api/v1/organizations/enrich',
                          headers=apollo_h, json={'domain': c['domain']}, timeout=30)
        org = r.json().get('organization', {}) or {}
        if org.get('linkedin_url'):
            c['li_url'] = org.get('linkedin_url', '')
        time.sleep(0.5)

# Scrape manufacturing LinkedIn pages for employee count
mfg_li_urls = [c['li_url'] for c in mfg_companies if c['li_url'] and 'linkedin.com' in c['li_url']]
if mfg_li_urls:
    print(f"\nScraping {len(mfg_li_urls)} manufacturing LinkedIn pages...")
    mfg_pages = run_actor(COMPANY_SCRAPER, {'companies': mfg_li_urls})
    mfg_page_map = {}
    for p in mfg_pages:
        name = (p.get('name') or '').strip()
        if name:
            mfg_page_map[name.lower()] = p

    for c in mfg_companies:
        pd = None
        for key, val in mfg_page_map.items():
            if c['name'].split()[0].lower() in key:
                pd = val
                break
        if pd:
            c['li_employees'] = pd.get('employeeCount', '')
            c['li_followers'] = pd.get('followerCount', '')

# ── Combine all companies ──
all_companies = mfg_companies + construction_seattle
print(f"\nTotal Seattle companies: {len(all_companies)} ({len(mfg_companies)} mfg + {len(construction_seattle)} construction)")

# ── Step 6: Score in batches of 15 ──
print("\nScoring companies...")
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

    resp = requests.post('https://api.openai.com/v1/chat/completions',
        headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
        json={'model': 'gpt-5.4', 'messages': [
            {'role': 'system', 'content': prompt},
            {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
        ], 'temperature': 0.3, 'max_completion_tokens': 8000}, timeout=90)

    raw = resp.json()['choices'][0]['message']['content'].strip()
    if raw.startswith('```'):
        raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
    scored = json.loads(raw)
    scores = scored.get('companies', scored if isinstance(scored, list) else [])
    all_scores.extend(scores)
    print(f"  Batch {i//15 + 1}: scored {len(scores)} companies")
    time.sleep(1)

score_map = {s.get('company_name', ''): s for s in all_scores}

# ── Build output CSV ──
output_headers = ['Category', 'Company', 'Company ICP Score', 'Pipeline Action',
    'Industry', 'Employees (LinkedIn)', 'Employees (Apollo)', 'Revenue',
    'Location', 'Ownership', 'Company LinkedIn URL', 'LI Followers',
    'Website', 'Contacts Found', 'Score Breakdown', 'Reasoning', 'Notes']

rows = []
for c in all_companies:
    sc = score_map.get(c['name'], {})
    score = sc.get('score', '')
    bd = sc.get('breakdown', {})

    if score == 0:
        action = 'HARD EXCLUDE'
    elif score and score >= 80:
        action = 'PROCEED'
    elif score and score >= 60:
        action = 'FLAG'
    elif score and score < 60:
        action = 'SKIP'
    else:
        action = ''

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

# Sort by score descending
rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if isinstance(x['Company ICP Score'], int) else 0)))

outdir = 'docs/TODO'
os.makedirs(outdir, exist_ok=True)
outfile = f'{outdir}/seattle_100_company_pipeline_results.csv'

with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(rows)

# Summary
proceed = sum(1 for r in rows if r['Pipeline Action'] == 'PROCEED')
flag = sum(1 for r in rows if r['Pipeline Action'] == 'FLAG')
skip = sum(1 for r in rows if r['Pipeline Action'] == 'SKIP')
exclude = sum(1 for r in rows if r['Pipeline Action'] == 'HARD EXCLUDE')

print(f"\n{'='*80}")
print(f"RESULTS: {len(rows)} Seattle companies scored")
print(f"  PROCEED (80+): {proceed}")
print(f"  FLAG (60-79):  {flag}")
print(f"  SKIP (<60):    {skip}")
print(f"  HARD EXCLUDE:  {exclude}")
print(f"\nSaved: {outfile}")
print(f"\nTop 20 by score:")
for r in rows[:20]:
    print(f"  {r['Company ICP Score']:>3} {r['Pipeline Action']:>12}  {r['Company']:<40} {str(r['Employees (LinkedIn)']):>5} LI emp  {r['Revenue'] or '?':>8}  {r['Location']}")
