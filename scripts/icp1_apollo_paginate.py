#!/usr/bin/env python3
"""Re-run Apollo searches with pagination for queries that hit 100 results."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}

seattle_locations = ['Seattle, Washington, United States', 'Washington, United States']
titles = ['CFO', 'Chief Financial Officer', 'Controller', 'VP Finance', 'Director of Finance', 'Owner', 'President', 'CEO']
seniorities = ['c_suite', 'vp', 'director', 'owner']

# Load existing results
existing = {}
combined_file = os.path.join(os.path.dirname(__file__), '..', 'docs', 'google search client ICP', 'apollo_seattle_all_industries.csv')
with open(combined_file) as f:
    for row in csv.DictReader(f):
        existing[row['company_name']] = row

print(f"Existing: {len(existing)} companies")

# Queries that likely have more results (hit 90+ on page 1)
paginate_queries = [
    ('Manufacturing', 'manufacturing', '11-20'),
    ('Manufacturing', 'machinery', '11-20'),
    ('Manufacturing', 'aerospace', '11-20'),
    ('Commercial Real Estate', 'real estate', '11-20'),
    ('Professional Services', 'engineering', '11-20'),
    ('Professional Services', 'architecture', '11-20'),
    ('Professional Services', 'consulting', '11-20'),
    ('Professional Services', 'accounting', '11-20'),
    ('Hospitality', 'hospitality', '11-20'),
    ('Hospitality', 'hotels', '11-20'),
    ('Hospitality', 'restaurants', '11-20'),
    ('Nonprofit', 'nonprofit', '11-20'),
    ('Nonprofit', 'non-profit', '11-20'),
    ('Construction', 'construction', '11-20'),
    ('Construction', 'general contractor', '11-20'),
    # Also try larger size ranges
    ('Manufacturing', 'manufacturing', '21-50'),
    ('Professional Services', 'engineering', '21-50'),
    ('Construction', 'construction', '21-50'),
]

new_total = 0

for industry, keyword, emp_range in paginate_queries:
    for page in range(2, 6):  # Pages 2-5
        body = {
            'q_organization_keyword_tags': [keyword],
            'organization_num_employees_ranges': [emp_range],
            'person_locations': seattle_locations,
            'person_titles': titles,
            'person_seniorities': seniorities,
            'per_page': 100,
            'page': page,
        }

        try:
            r = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                headers=apollo_h, json=body, timeout=30)
            if r.status_code != 200:
                break

            people = r.json().get('people', [])
            if not people:
                break  # No more results

            new = 0
            for p in people:
                org = p.get('organization', {}) or {}
                org_name = org.get('name', '')
                if org_name and org_name not in existing:
                    rev = org.get('annual_revenue')
                    rev_str = f'${int(rev/1e6)}M' if rev and rev >= 1e6 else ''
                    existing[org_name] = {
                        'icp_industry': industry,
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
                new_total += new
                print(f"  {industry} | {keyword} | {emp_range} | page {page}: {new} new (running total: {len(existing)})")

            if len(people) < 100:
                break  # Last page

        except Exception as e:
            print(f"  ERROR: {e}")
            break

        time.sleep(0.5)

# Save updated combined
with open(combined_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'icp_industry', 'company_name', 'industry_tag', 'apollo_industry', 'employees',
        'revenue', 'domain', 'linkedin_url', 'city', 'state', 'contact_name', 'contact_title'
    ])
    writer.writeheader()
    for row in sorted(existing.values(), key=lambda x: (x.get('icp_industry', ''), x['company_name'])):
        writer.writerow(row)

print(f"\n{'='*60}")
print(f"PAGINATION RESULTS")
print(f"{'='*60}")
print(f"New companies from pagination: {new_total}")
print(f"Total unique companies: {len(existing)}")
print()
for industry in ['Manufacturing', 'Commercial Real Estate', 'Professional Services', 'Hospitality', 'Nonprofit', 'Construction']:
    count = sum(1 for v in existing.values() if v.get('icp_industry') == industry)
    print(f"  {industry:<25} {count:>5}")
print(f"\nSaved: {combined_file}")
