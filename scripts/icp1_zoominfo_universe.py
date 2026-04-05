#!/usr/bin/env python3
"""Search ZoomInfo for Seattle company universe across all ICP industries with pagination."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

# ZoomInfo auth
print("Authenticating ZoomInfo...")
zi_resp = requests.post("https://api.zoominfo.com/authenticate", json={
    "username": os.environ["ZOOMINFO_USERNAME"],
    "password": os.environ["ZOOMINFO_PASSWORD"],
})
zi_jwt = zi_resp.json()["jwt"]
zi_headers = {"Authorization": f"Bearer {zi_jwt}", "Content-Type": "application/json"}
print("Authenticated.")

# ICP industries and title searches
searches = {
    'Manufacturing': ['manufacturing', 'machinery', 'aerospace', 'industrial'],
    'Commercial Real Estate': ['commercial real estate', 'property management', 'real estate investment'],
    'Professional Services': ['engineering', 'architecture', 'consulting firm', 'accounting firm'],
    'Hospitality': ['hospitality', 'hotel', 'restaurant group'],
    'Nonprofit': ['nonprofit', 'foundation', 'charity'],
    'Construction': ['construction', 'general contractor', 'building contractor'],
}

finance_titles = "CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance OR Owner OR President OR CEO"

seattle_metro = [
    'Seattle', 'Bellevue', 'Tacoma', 'Redmond', 'Kirkland', 'Everett',
    'Renton', 'Kent', 'Auburn', 'Puyallup', 'Lynnwood', 'Bothell',
    'Issaquah', 'Woodinville', 'Tukwila', 'Federal Way', 'Shoreline',
]

all_companies = {}

for industry, keywords in searches.items():
    print(f"\n{'='*60}")
    print(f"INDUSTRY: {industry}")
    print(f"{'='*60}")

    industry_companies = {}

    for keyword in keywords:
        for page in range(1, 11):  # Up to 10 pages
            body = {
                "companyName": keyword,
                "locationSearchType": "city",
                "locationCity": seattle_metro,
                "locationState": ["Washington"],
                "jobTitle": finance_titles,
                "rpp": 25,
                "page": page,
            }

            try:
                r = requests.post("https://api.zoominfo.com/search/contact",
                    headers=zi_headers, json=body, timeout=30)

                if r.status_code != 200:
                    print(f"  {keyword} page {page}: HTTP {r.status_code}")
                    break

                data = r.json()
                contacts = data.get("data", [])

                if not contacts:
                    break

                new = 0
                for c in contacts:
                    co = c.get("companyName", "")
                    if co and co not in industry_companies:
                        industry_companies[co] = {
                            'company_name': co,
                            'industry_keyword': keyword,
                            'contact_name': f"{c.get('firstName', '')} {c.get('lastName', '')}",
                            'contact_title': c.get('jobTitle', ''),
                            'city': c.get('city', ''),
                            'state': c.get('state', ''),
                            'has_email': 'Yes' if c.get('hasEmail') else 'No',
                            'has_phone': 'Yes' if c.get('hasDirectPhone') else 'No',
                            'accuracy_score': c.get('contactAccuracyScore', ''),
                        }
                        new += 1

                if new > 0:
                    print(f"  {keyword} page {page}: {new} new (total: {len(industry_companies)})")

                if len(contacts) < 25:
                    break  # Last page

            except Exception as e:
                print(f"  {keyword} page {page}: ERROR - {e}")
                break

            time.sleep(0.8)

    # Save per industry
    outdir = os.path.join(os.path.dirname(__file__), '..', 'docs', 'TODO')
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f'zoominfo_{industry.lower().replace(" ", "_")}_seattle.csv')

    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'company_name', 'industry_keyword', 'contact_name', 'contact_title',
            'city', 'state', 'has_email', 'has_phone', 'accuracy_score'
        ])
        writer.writeheader()
        writer.writerows(industry_companies.values())

    print(f"  SAVED: {outfile} ({len(industry_companies)} companies)")

    for name, data in industry_companies.items():
        if name not in all_companies:
            all_companies[name] = {**data, 'icp_industry': industry}

# Save combined
combined_file = os.path.join(outdir, 'zoominfo_seattle_all_industries.csv')
with open(combined_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        'icp_industry', 'company_name', 'industry_keyword', 'contact_name', 'contact_title',
        'city', 'state', 'has_email', 'has_phone', 'accuracy_score'
    ])
    writer.writeheader()
    for row in sorted(all_companies.values(), key=lambda x: (x.get('icp_industry', ''), x['company_name'])):
        writer.writerow(row)

print(f"\n{'='*60}")
print(f"ZOOMINFO UNIVERSE SUMMARY")
print(f"{'='*60}")
print(f"Total unique companies: {len(all_companies)}")
for industry in searches:
    count = sum(1 for v in all_companies.values() if v.get('icp_industry') == industry)
    print(f"  {industry:<25} {count:>5}")
print(f"\nSaved: {combined_file}")
