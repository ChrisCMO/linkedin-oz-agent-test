#!/usr/bin/env python3
"""Run company-level pipeline on 193 Seattle manufacturing companies."""
import sys, os, csv, json, time, random
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

BASE = os.path.join(os.path.dirname(__file__), "..")

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
    try:
        return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                            headers=apify_h, timeout=15).json()
    except:
        return []


# Step 1: Filter to Manufacturing only
print("Step 1: Loading manufacturing companies...")
xray_file = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'universe', 'seattle', 'xray_linkedin_seattle_924.csv')
mfg = []
with open(xray_file) as f:
    for row in csv.DictReader(f):
        if (row.get('Industry') or '').strip() == 'Manufacturing':
            mfg.append({
                'name': row.get('Company Name', ''),
                'li_url': row.get('LinkedIn Company URL', ''),
                'location': row.get('Location', ''),
            })

print(f"  Manufacturing companies: {len(mfg)}")

# Step 2: Apify LinkedIn company page scrape (batches of 20)
print("\nStep 2: Scraping LinkedIn company pages...")
all_pages = []
li_urls = [c['li_url'] for c in mfg if c['li_url']]

for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20 + 1}/{(len(li_urls)+19)//20}: {len(batch)} URLs")
    pages = run_actor(COMPANY_SCRAPER, {'companies': batch})
    all_pages.extend(pages)
    time.sleep(2)

print(f"  Got {len(all_pages)} company pages")

# Map pages by name
page_map = {}
for p in all_pages:
    name = (p.get('name') or '').strip()
    if name:
        page_map[name.lower()] = p

# Enrich manufacturing companies with LinkedIn data
for c in mfg:
    pd = None
    for key, val in page_map.items():
        if c['name'].split()[0].lower() in key:
            pd = val
            break

    c['li_employees'] = pd.get('employeeCount', '') if pd else ''
    c['li_followers'] = pd.get('followerCount', '') if pd else ''
    c['li_description'] = ((pd.get('description') or '')[:300]) if pd else ''
    c['li_tagline'] = (pd.get('tagline') or '') if pd else ''
    founded = pd.get('foundedOn', '') if pd else ''
    if isinstance(founded, dict):
        founded = str(founded.get('year', ''))
    c['li_founded'] = str(founded) if founded else ''
    c['li_has_logo'] = ('Yes' if pd.get('logo') else 'No') if pd else ''

    website = (pd.get('website') or '') if pd else ''
    c['website'] = website
    c['domain'] = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0] if website else ''

    hq = ''
    if pd:
        for loc in (pd.get('locations') or []):
            city = loc.get('city', '')
            state = loc.get('geographicArea', '')
            if city:
                hq = f'{city}, {state}'
                break
    if hq:
        c['location'] = hq

# Step 3: Apollo org enrichment
print("\nStep 3: Apollo org enrichment...")
for i, c in enumerate(mfg):
    if c.get('domain'):
        try:
            r = requests.post('https://api.apollo.io/api/v1/organizations/enrich',
                              headers=apollo_h, json={'domain': c['domain']}, timeout=30)
            org = r.json().get('organization', {}) or {}
            if org.get('name'):
                c['apollo_employees'] = org.get('estimated_num_employees', '')
                rev = org.get('annual_revenue')
                c['revenue'] = f'${int(rev/1e6)}M' if rev and rev >= 1e6 else ''
                c['apollo_industry'] = org.get('industry', '')
        except:
            pass
        time.sleep(0.5)

    if (i + 1) % 50 == 0:
        print(f"  Enriched {i+1}/{len(mfg)}")

# Step 4: Score in batches
print("\nStep 4: Scoring...")
prompt = open(os.path.join(BASE, 'mvp', 'backend', 'services', 'scoring.py')).read().split('COMPANY_SYSTEM_PROMPT = """')[1].split('"""')[0]

all_scores = []
for i in range(0, len(mfg), 15):
    batch = mfg[i:i+15]
    score_input = [{
        'company_id': c['name'].lower().replace(' ', '_')[:25],
        'company_name': c['name'],
        'industry': c.get('apollo_industry', 'manufacturing'),
        'linkedin_employees': c.get('li_employees', ''),
        'apollo_employees': c.get('apollo_employees', ''),
        'revenue': c.get('revenue', ''),
        'location': c.get('location', ''),
        'ownership': 'Unknown - appears private',
        'linkedin_page': c.get('li_url', ''),
        'linkedin_followers': c.get('li_followers', ''),
        'website': c.get('domain', ''),
    } for c in batch]

    try:
        resp = requests.post('https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
            json={'model': 'gpt-5.4', 'messages': [
                {'role': 'system', 'content': prompt},
                {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
            ], 'temperature': 0.3, 'max_completion_tokens': 8000}, timeout=90)

        rj = resp.json()
        if 'choices' not in rj:
            print(f"  Batch {i//15+1}: API error - {str(rj.get('error', ''))[:80]}")
            time.sleep(5)
            continue

        raw = rj['choices'][0]['message']['content'].strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        scored = json.loads(raw)
        scores = scored.get('companies', scored if isinstance(scored, list) else [])
        all_scores.extend(scores)
        print(f"  Batch {i//15+1}: scored {len(scores)}")
    except Exception as e:
        print(f"  Batch {i//15+1}: FAILED - {e}")
    time.sleep(2)

score_map = {s.get('company_name', ''): s for s in all_scores}

# Build output
output_headers = ['Category', 'Company', 'Company ICP Score', 'Pipeline Action',
    'Industry', 'Employees (LinkedIn)', 'Employees (Apollo)', 'Revenue',
    'Location', 'Ownership', 'Company LinkedIn URL', 'LI Followers',
    'LI Description', 'LI Tagline', 'LI Founded', 'LI Has Logo',
    'Domain', 'Website', 'Contacts Found', 'Score Breakdown', 'Reasoning', 'Notes']

rows = []
for c in mfg:
    sc = score_map.get(c['name'], {})
    score = sc.get('score', '')
    bd = sc.get('breakdown', {})

    if score == 0: action = 'HARD EXCLUDE'
    elif isinstance(score, int) and score >= 80: action = 'PROCEED'
    elif isinstance(score, int) and score >= 60: action = 'FLAG'
    elif isinstance(score, int): action = 'SKIP'
    else: action = ''

    rows.append({
        'Category': 'Manufacturing',
        'Company': c['name'],
        'Company ICP Score': score,
        'Pipeline Action': action,
        'Industry': c.get('apollo_industry', 'manufacturing'),
        'Employees (LinkedIn)': c.get('li_employees', ''),
        'Employees (Apollo)': c.get('apollo_employees', ''),
        'Revenue': c.get('revenue', ''),
        'Location': c.get('location', ''),
        'Ownership': 'Unknown - appears private',
        'Company LinkedIn URL': c.get('li_url', ''),
        'LI Followers': c.get('li_followers', ''),
        'LI Description': c.get('li_description', ''),
        'LI Tagline': c.get('li_tagline', ''),
        'LI Founded': c.get('li_founded', ''),
        'LI Has Logo': c.get('li_has_logo', ''),
        'Domain': c.get('domain', ''),
        'Website': c.get('website', ''),
        'Contacts Found': '',
        'Score Breakdown': ' | '.join(f'{k}: {v}' for k, v in bd.items()),
        'Reasoning': sc.get('reasoning', ''),
        'Notes': '',
    })

rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if isinstance(x['Company ICP Score'], int) else 0)))

# Save
outfile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'seattle_manufacturing_scored.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(rows)

proceed = sum(1 for r in rows if r['Pipeline Action'] == 'PROCEED')
flag = sum(1 for r in rows if r['Pipeline Action'] == 'FLAG')
skip = sum(1 for r in rows if r['Pipeline Action'] == 'SKIP')
exclude = sum(1 for r in rows if r['Pipeline Action'] == 'HARD EXCLUDE')

print(f"\n{'='*60}")
print(f"MANUFACTURING PIPELINE RESULTS: {len(rows)} companies")
print(f"  PROCEED (80+): {proceed}")
print(f"  FLAG (60-79):  {flag}")
print(f"  SKIP (<60):    {skip}")
print(f"  HARD EXCLUDE:  {exclude}")
print(f"\nSaved: {outfile}")

print(f"\nTop 20:")
for r in rows[:20]:
    print(f"  {r['Company ICP Score']:>3} {r['Pipeline Action']:>12}  {r['Company']:<40} {str(r['Employees (LinkedIn)']):>5} LI emp  {r['Revenue'] or '?':>8}  {r['Location']}")
