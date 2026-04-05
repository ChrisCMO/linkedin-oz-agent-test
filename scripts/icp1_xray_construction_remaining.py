#!/usr/bin/env python3
"""Finish construction X-ray queries that crashed + merge with existing."""
import sys, os, time, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

token = os.environ['APIFY_API_KEY']
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
SERP = 'nFJndFXA5zjCTuudP'

def run_serp(query, max_pages=3):
    payload = {
        'queries': query,
        'maxPagesPerQuery': max_pages,
        'resultsPerPage': 100,
        'countryCode': 'us',
    }
    r = requests.post(f'https://api.apify.com/v2/acts/{SERP}/runs', headers=headers, json=payload, timeout=30)
    if r.status_code != 201:
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(20):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}', headers=headers, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
            break
    try:
        items = requests.get(f'https://api.apify.com/v2/datasets/{ds}/items', headers=headers, timeout=15).json()
    except Exception:
        return []
    results = []
    for item in items:
        for r2 in item.get('organicResults', []):
            url = r2.get('url', '')
            title = r2.get('title', '')
            desc = r2.get('description', '')[:200]
            if 'linkedin.com/company' in url and url not in [x['url'] for x in results]:
                results.append({'title': title, 'url': url.split('?')[0], 'desc': desc})
    return results

# Load existing construction results (the 100 that were saved before crash)
existing = {}
partial_file = 'docs/google search client ICP/construction/seattle_construction_linkedin_xray.csv'
if os.path.exists(partial_file):
    with open(partial_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get('LinkedIn Company URL', '')
            if url:
                existing[url] = row

print(f"Existing construction companies: {len(existing)}")

# Run remaining queries
remaining = [
    'site:linkedin.com/company "building contractor" "Seattle"',
    'site:linkedin.com/company "construction" "Redmond" OR "Kirkland" OR "Renton" Washington',
    'site:linkedin.com/company "plumbing" OR "electrical" OR "HVAC" "Seattle" contractor',
    'site:linkedin.com/company "roofing" OR "concrete" OR "excavation" "Seattle" Washington',
]

for q in remaining:
    print(f"Q: {q[:70]}...")
    results = run_serp(q, max_pages=3)
    new = 0
    for r in results:
        if r['url'] not in existing:
            name = r['title'].split(' | ')[0].split(' - LinkedIn')[0].strip()
            existing[r['url']] = {
                'Company Name': name,
                'LinkedIn Company URL': r['url'],
                'Description': r['desc'][:150],
                'Source': 'Google X-ray LinkedIn',
            }
            new += 1
    print(f"  Got {len(results)}, {new} new (total: {len(existing)})")
    time.sleep(3)

# Save
outfile = partial_file
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Company Name', 'LinkedIn Company URL', 'Description', 'Source'])
    for url, r in sorted(existing.items(), key=lambda x: x[1].get('Company Name', '')):
        writer.writerow([
            r.get('Company Name', ''),
            r.get('LinkedIn Company URL', url),
            r.get('Description', ''),
            r.get('Source', 'Google X-ray LinkedIn'),
        ])

print(f"\nSaved: {outfile}")
print(f"Total construction companies: {len(existing)}")

# Grand total
print(f"\n{'='*60}")
print("GRAND TOTAL ACROSS ALL INDUSTRIES:")
base = 'docs/google search client ICP'
total = 0
for ind in ['manufacturing', 'commercial_real_estate', 'professional_services', 'hospitality', 'nonprofit', 'construction']:
    f = os.path.join(base, ind, f'seattle_{ind}_linkedin_xray.csv')
    if os.path.exists(f):
        with open(f) as fh:
            count = sum(1 for _ in fh) - 1
        print(f"  {ind:<25} {count:>5}")
        total += count
print(f"  {'TOTAL':<25} {total:>5}")
