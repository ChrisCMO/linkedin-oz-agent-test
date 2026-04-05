#!/usr/bin/env python3
"""ZoomInfo COMPANY search (not contact search) for Seattle universe."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

# Auth
print("Authenticating ZoomInfo...")
zi_resp = requests.post("https://api.zoominfo.com/authenticate", json={
    "username": os.environ["ZOOMINFO_USERNAME"],
    "password": os.environ["ZOOMINFO_PASSWORD"],
})
zi_jwt = zi_resp.json()["jwt"]
zi_h = {"Authorization": f"Bearer {zi_jwt}", "Content-Type": "application/json"}
print("Authenticated.")

# First check what input fields are available
print("\nChecking available company search fields...")
r = requests.get("https://api.zoominfo.com/lookup/inputfields/company/search", headers=zi_h, timeout=15)
if r.status_code == 200:
    fields = r.json()
    print(f"Available fields: {json.dumps(fields, indent=2)[:1000]}")
else:
    print(f"Lookup failed: {r.status_code}")

# Search by industry + metro region
# Metro region format from the n8n workflow: "WA - Seattle"
searches = {
    'Manufacturing': [
        {'industryKeywords': 'manufacturing', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'aerospace manufacturing', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'industrial manufacturing', 'metroRegion': 'WA - Seattle'},
    ],
    'Commercial Real Estate': [
        {'industryKeywords': 'commercial real estate', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'property management', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'real estate development', 'metroRegion': 'WA - Seattle'},
    ],
    'Professional Services': [
        {'industryKeywords': 'engineering', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'architecture', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'accounting', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'consulting', 'metroRegion': 'WA - Seattle'},
    ],
    'Hospitality': [
        {'industryKeywords': 'hospitality', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'hotel', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'restaurant', 'metroRegion': 'WA - Seattle'},
    ],
    'Nonprofit': [
        {'industryKeywords': 'nonprofit', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'foundation', 'metroRegion': 'WA - Seattle'},
    ],
    'Construction': [
        {'industryKeywords': 'construction', 'metroRegion': 'WA - Seattle'},
        {'industryKeywords': 'general contractor', 'metroRegion': 'WA - Seattle'},
    ],
}

all_companies = {}

for industry, queries in searches.items():
    print(f"\n{'='*60}")
    print(f"INDUSTRY: {industry}")
    print(f"{'='*60}")

    industry_companies = {}

    for query in queries:
        keyword = query['industryKeywords']
        for page in range(1, 11):
            body = {
                **query,
                'companyType': 'private',
                'rpp': 100,
                'page': page,
            }

            try:
                r = requests.post("https://api.zoominfo.com/search/company",
                    headers=zi_h, json=body, timeout=30)

                if r.status_code != 200:
                    print(f"  {keyword} page {page}: HTTP {r.status_code} - {r.text[:100]}")
                    break

                data = r.json()
                companies = data.get('data', [])

                if not companies:
                    break

                new = 0
                for c in companies:
                    name = c.get('name', '') or c.get('companyName', '')
                    if name and name not in industry_companies:
                        industry_companies[name] = {
                            'company_name': name,
                            'industry_keyword': keyword,
                            'zi_id': c.get('id', ''),
                            'zi_industry': c.get('industry', ''),
                            'employees': c.get('employeeCount', ''),
                            'revenue': c.get('revenue', ''),
                            'city': c.get('city', ''),
                            'state': c.get('state', ''),
                            'website': c.get('website', ''),
                            'phone': c.get('phone', ''),
                        }
                        new += 1

                if new > 0:
                    print(f"  {keyword} page {page}: {new} new (total: {len(industry_companies)})")

                if len(companies) < 100:
                    break

            except Exception as e:
                print(f"  {keyword} page {page}: ERROR - {e}")
                break

            time.sleep(0.8)

    outdir = os.path.join(os.path.dirname(__file__), '..', 'docs', 'TODO')
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f'zoominfo_{industry.lower().replace(" ", "_")}_seattle.csv')

    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'company_name', 'industry_keyword', 'zi_id', 'zi_industry',
            'employees', 'revenue', 'city', 'state', 'website', 'phone'
        ])
        writer.writeheader()
        writer.writerows(industry_companies.values())

    print(f"  SAVED: {len(industry_companies)} companies")

    for name, data in industry_companies.items():
        if name not in all_companies:
            all_companies[name] = {**data, 'icp_industry': industry}

# Combined
combined = os.path.join(outdir, 'zoominfo_seattle_all_industries.csv')
with open(combined, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'icp_industry', 'company_name', 'industry_keyword', 'zi_id', 'zi_industry',
        'employees', 'revenue', 'city', 'state', 'website', 'phone'
    ])
    writer.writeheader()
    for row in sorted(all_companies.values(), key=lambda x: (x.get('icp_industry', ''), x['company_name'])):
        writer.writerow(row)

print(f"\n{'='*60}")
print(f"ZOOMINFO COMPANY SEARCH SUMMARY")
print(f"{'='*60}")
print(f"Total unique companies: {len(all_companies)}")
for industry in searches:
    count = sum(1 for v in all_companies.values() if v.get('icp_industry') == industry)
    print(f"  {industry:<25} {count:>5}")
print(f"\nSaved: {combined}")
