#!/usr/bin/env python3
"""Search Apollo for the full Seattle company universe across all ICP industries."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}

seattle_locations = ['Seattle, Washington, United States', 'Washington, United States']

industries = {
    'Manufacturing': ['manufacturing', 'machinery', 'aerospace', 'industrial'],
    'Commercial Real Estate': ['real estate', 'property management', 'commercial real estate'],
    'Professional Services': ['engineering', 'architecture', 'consulting', 'accounting'],
    'Hospitality': ['hospitality', 'hotels', 'restaurants'],
    'Nonprofit': ['nonprofit', 'non-profit', 'foundation'],
    'Construction': ['construction', 'general contractor', 'building'],
}

emp_ranges = ['11-20', '21-50', '51-100', '101-200', '201-500', '501-1000']

all_companies = {}

for industry, keywords in industries.items():
    print(f"\n{'='*60}")
    print(f"INDUSTRY: {industry}")
    print(f"{'='*60}")

    industry_companies = {}

    for keyword in keywords:
        for emp_range in emp_ranges:
            body = {
                'q_organization_keyword_tags': [keyword],
                'organization_num_employees_ranges': [emp_range],
                'person_locations': seattle_locations,
                'person_titles': ['CFO', 'Chief Financial Officer', 'Controller',
                                  'VP Finance', 'Director of Finance', 'Owner', 'President', 'CEO'],
                'person_seniorities': ['c_suite', 'vp', 'director', 'owner'],
                'per_page': 100,
                'page': 1,
            }

            try:
                r = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                    headers=apollo_h, json=body, timeout=30)
                if r.status_code != 200:
                    continue

                data = r.json()
                people = data.get('people', [])
                total_available = data.get('pagination', {}).get('total_entries', 0)

                new = 0
                for p in people:
                    org = p.get('organization', {}) or {}
                    org_name = org.get('name', '')
                    if org_name and org_name not in industry_companies:
                        rev = org.get('annual_revenue')
                        rev_str = f'${int(rev/1e6)}M' if rev and rev >= 1e6 else ''
                        industry_companies[org_name] = {
                            'company_name': org_name,
                            'industry_tag': keyword,
                            'apollo_industry': org.get('industry', ''),
                            'employees': org.get('estimated_num_employees', ''),
                            'revenue': rev_str,
                            'domain': org.get('primary_domain', ''),
                            'linkedin_url': org.get('linkedin_url', ''),
                            'city': p.get('city', ''),
                            'state': p.get('state', ''),
                            'contact_name': f"{p.get('first_name', '')} {p.get('last_name', '')}",
                            'contact_title': p.get('title', ''),
                        }
                        new += 1

                if new > 0:
                    print(f"  {keyword} | {emp_range}: {new} new (avail: {total_available}, total: {len(industry_companies)})")
            except Exception as e:
                print(f"  {keyword} | {emp_range}: ERROR - {e}")

            time.sleep(0.5)

    # Save per industry
    outdir = os.path.join(os.path.dirname(__file__), '..', 'docs', 'google search client ICP')
    outfile = os.path.join(outdir, f'apollo_{industry.lower().replace(" ", "_")}_seattle.csv')

    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'company_name', 'industry_tag', 'apollo_industry', 'employees', 'revenue',
            'domain', 'linkedin_url', 'city', 'state', 'contact_name', 'contact_title'
        ])
        writer.writeheader()
        writer.writerows(industry_companies.values())

    print(f"  SAVED: {len(industry_companies)} companies")

    for name, data in industry_companies.items():
        if name not in all_companies:
            all_companies[name] = {**data, 'icp_industry': industry}

# Save combined
combined_file = os.path.join(outdir, 'apollo_seattle_all_industries.csv')
with open(combined_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'icp_industry', 'company_name', 'industry_tag', 'apollo_industry', 'employees',
        'revenue', 'domain', 'linkedin_url', 'city', 'state', 'contact_name', 'contact_title'
    ])
    writer.writeheader()
    for row in sorted(all_companies.values(), key=lambda x: (x['icp_industry'], x['company_name'])):
        writer.writerow(row)

print(f"\n{'='*60}")
print(f"APOLLO UNIVERSE SUMMARY")
print(f"{'='*60}")
print(f"Total unique companies: {len(all_companies)}")
for industry in industries:
    count = sum(1 for v in all_companies.values() if v['icp_industry'] == industry)
    print(f"  {industry:<25} {count:>5}")
print(f"\nSaved: {combined_file}")
