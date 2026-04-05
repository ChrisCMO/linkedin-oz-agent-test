#!/usr/bin/env python3
"""Google X-ray search across all 6 ICP industries for Seattle metro."""
import sys, os, time, json, csv
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
        print(f'  FAIL: {r.status_code}')
        return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(20):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}', headers=headers, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
            break
    items = requests.get(f'https://api.apify.com/v2/datasets/{ds}/items', headers=headers, timeout=15).json()
    results = []
    for item in items:
        for r2 in item.get('organicResults', []):
            url = r2.get('url', '')
            title = r2.get('title', '')
            desc = r2.get('description', '')[:200]
            if 'linkedin.com/company' in url and url not in [x['url'] for x in results]:
                results.append({'title': title, 'url': url.split('?')[0], 'desc': desc})
    return results

searches = {
    'manufacturing': [
        'site:linkedin.com/company "manufacturing" "Seattle, Washington"',
        'site:linkedin.com/company "manufacturing" "Bellevue" OR "Tacoma" OR "Everett" Washington',
        'site:linkedin.com/company "manufacturer" "Seattle"',
        'site:linkedin.com/company "industrial" "Seattle, Washington"',
        'site:linkedin.com/company "manufacturing" "Redmond" OR "Kirkland" OR "Renton" Washington',
        'site:linkedin.com/company "aerospace" "Seattle" OR "Everett" Washington',
        'site:linkedin.com/company "machine shop" OR "fabrication" "Seattle" Washington',
        'site:linkedin.com/company "food production" OR "food manufacturing" "Seattle" Washington',
    ],
    'commercial_real_estate': [
        'site:linkedin.com/company "commercial real estate" "Seattle, Washington"',
        'site:linkedin.com/company "commercial real estate" "Bellevue" OR "Tacoma" Washington',
        'site:linkedin.com/company "property management" "Seattle, Washington"',
        'site:linkedin.com/company "real estate investment" "Seattle" Washington',
        'site:linkedin.com/company "commercial property" "Seattle" OR "Bellevue" Washington',
        'site:linkedin.com/company "real estate development" "Seattle" Washington',
        'site:linkedin.com/company "property management" "Bellevue" OR "Kirkland" Washington',
    ],
    'professional_services': [
        'site:linkedin.com/company "engineering" "Seattle, Washington"',
        'site:linkedin.com/company "engineering firm" "Bellevue" OR "Tacoma" Washington',
        'site:linkedin.com/company "architecture" "Seattle, Washington"',
        'site:linkedin.com/company "consulting" "Seattle, Washington" "engineering"',
        'site:linkedin.com/company "accounting" OR "CPA" "Seattle" Washington',
        'site:linkedin.com/company "law firm" "Seattle, Washington"',
        'site:linkedin.com/company "environmental consulting" "Seattle" Washington',
        'site:linkedin.com/company "civil engineering" "Seattle" OR "Bellevue" Washington',
    ],
    'hospitality': [
        'site:linkedin.com/company "hospitality" "Seattle, Washington"',
        'site:linkedin.com/company "hotel" "Seattle" OR "Bellevue" Washington',
        'site:linkedin.com/company "restaurant group" "Seattle" Washington',
        'site:linkedin.com/company "hospitality" "Bellevue" OR "Tacoma" OR "Everett" Washington',
        'site:linkedin.com/company "hotel management" "Seattle" Washington',
        'site:linkedin.com/company "catering" OR "event" "Seattle" Washington hospitality',
    ],
    'nonprofit': [
        'site:linkedin.com/company "nonprofit" "Seattle, Washington"',
        'site:linkedin.com/company "non-profit" "Seattle" Washington',
        'site:linkedin.com/company "foundation" "Seattle, Washington"',
        'site:linkedin.com/company "nonprofit" "Bellevue" OR "Tacoma" OR "Everett" Washington',
        'site:linkedin.com/company "charity" OR "social services" "Seattle" Washington',
        'site:linkedin.com/company "community" "Seattle" Washington nonprofit',
    ],
    'construction': [
        'site:linkedin.com/company "construction" "Seattle, Washington"',
        'site:linkedin.com/company "general contractor" "Seattle"',
        'site:linkedin.com/company "construction" "Bellevue" OR "Tacoma" OR "Everett" Washington',
        'site:linkedin.com/company "construction company" "Seattle" OR "Washington"',
        'site:linkedin.com/company "building contractor" "Seattle"',
        'site:linkedin.com/company "construction" "Redmond" OR "Kirkland" OR "Renton" Washington',
        'site:linkedin.com/company "plumbing" OR "electrical" OR "HVAC" "Seattle" contractor',
        'site:linkedin.com/company "roofing" OR "concrete" OR "excavation" "Seattle" Washington',
    ],
}

outdir = 'docs/google search client ICP'
os.makedirs(outdir, exist_ok=True)

grand_total = 0

for industry, queries in searches.items():
    print(f"\n{'='*60}")
    print(f"INDUSTRY: {industry}")
    print(f"{'='*60}")

    all_results = {}

    for q in queries:
        print(f"  Q: {q[:70]}...")
        results = run_serp(q, max_pages=3)
        new = 0
        for r in results:
            if r['url'] not in all_results:
                all_results[r['url']] = r
                new += 1
        print(f"    Got {len(results)} results, {new} new (total: {len(all_results)})")
        time.sleep(3)

    ind_dir = os.path.join(outdir, industry)
    os.makedirs(ind_dir, exist_ok=True)
    outfile = os.path.join(ind_dir, f'seattle_{industry}_linkedin_xray.csv')

    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Company Name', 'LinkedIn Company URL', 'Description', 'Source'])
        for url, r in sorted(all_results.items(), key=lambda x: x[1]['title']):
            name = r['title'].split(' | ')[0].split(' - LinkedIn')[0].strip()
            writer.writerow([name, r['url'], r['desc'][:150], 'Google X-ray LinkedIn'])

    print(f"\n  SAVED: {outfile}")
    print(f"  Total unique companies: {len(all_results)}")
    grand_total += len(all_results)

print(f"\n{'='*60}")
print(f"GRAND TOTAL: {grand_total} unique companies across all industries")
print(f"{'='*60}")

for industry in searches:
    ind_dir = os.path.join(outdir, industry)
    outfile = os.path.join(ind_dir, f'seattle_{industry}_linkedin_xray.csv')
    if os.path.exists(outfile):
        with open(outfile) as f:
            count = sum(1 for _ in f) - 1
        print(f"  {industry:<25} {count:>5} companies")
