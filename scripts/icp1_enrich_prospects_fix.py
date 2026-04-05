#!/usr/bin/env python3
"""Fix prospect enrichment: add LinkedIn profile data + multiple contacts per company."""
import sys, os, csv, json, time, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
APIFY_TOKEN = os.environ['APIFY_API_KEY']

apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

BASE = os.path.join(os.path.dirname(__file__), "..")
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'

def run_actor(actor_id, payload):
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                      headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201: return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(24):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                         headers=apify_h, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'): break
    try:
        return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                            headers=apify_h, timeout=15).json()
    except: return []

# Load existing prospect file
infile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_90_prospect_enrichment.csv')
with open(infile) as f:
    reader = csv.DictReader(f)
    old_headers = reader.fieldnames
    existing = list(reader)

print(f"Existing prospects: {len(existing)}")

# Load the PROCEED companies
proceed_file = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'all_proceed_companies.csv')
with open(proceed_file) as f:
    companies = list(csv.DictReader(f))

print(f"PROCEED companies: {len(companies)}")

# Step 1: Get ALL contacts from Apollo (not just top 1)
print("\nStep 1: Apollo search for ALL finance contacts...")
all_prospects = []
existing_by_company = {r.get('Company', ''): r for r in existing}

for i, co in enumerate(companies):
    domain = (co.get('Domain') or co.get('Website') or '').strip()
    domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
    company_name = co.get('Company', '')

    search_body = {
        'person_titles': ['CFO', 'Chief Financial Officer', 'Controller',
                          'VP Finance', 'Director of Finance', 'Owner', 'President', 'CEO'],
        'person_seniorities': ['c_suite', 'vp', 'director', 'owner'],
        'per_page': 10,  # Get up to 10 per company
    }
    if domain:
        search_body['q_organization_domains_list'] = [domain]
    else:
        search_body['q_keywords'] = company_name

    try:
        r = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                          headers=apollo_h, json=search_body, timeout=30)
        people = r.json().get('people', [])
    except:
        people = []

    # Get existing messages for this company (from the first run)
    ex = existing_by_company.get(company_name, {})

    for p in people:
        all_prospects.append({
            'company': co,
            'apollo_id': p.get('id', ''),
            'first_name': p.get('first_name', ''),
            'last_name': p.get('last_name', ''),
            'title': p.get('title', ''),
            'seniority': p.get('seniority', ''),
            'linkedin_url': p.get('linkedin_url', ''),
            'email': p.get('email', ''),
            'email_status': '',
            # Carry over messages from existing if same person
            'note_melinda': ex.get("Melinda's Connection Note", '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'note_adrienne': ex.get("Adrienne's Connection Note", '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_melinda_1': ex.get('Message 1 - Melinda', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_melinda_2': ex.get('Message 2 - Melinda', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_melinda_3': ex.get('Message 3 - Melinda', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_adrienne_1': ex.get('Message 1 - Adrienne', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_adrienne_2': ex.get('Message 2 - Adrienne', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
            'msg_adrienne_3': ex.get('Message 3 - Adrienne', '') if p.get('first_name', '').lower() == ex.get('First Name', '').lower() else '',
        })

    time.sleep(0.3)
    if (i + 1) % 20 == 0:
        print(f"  {i+1}/{len(companies)}: {len(all_prospects)} total contacts")

print(f"Total contacts found: {len(all_prospects)}")
contacts_per_co = {}
for p in all_prospects:
    co = p['company'].get('Company', '')
    contacts_per_co[co] = contacts_per_co.get(co, 0) + 1

print(f"Companies with contacts: {len(contacts_per_co)}")
print(f"Avg contacts per company: {len(all_prospects)/max(len(contacts_per_co),1):.1f}")

# Step 2: Apify profile scraper for ALL contacts with LinkedIn URLs
print("\nStep 2: Apify profile verification...")
li_urls = list(set(p['linkedin_url'] for p in all_prospects if p.get('linkedin_url')))
print(f"Unique LinkedIn URLs to scrape: {len(li_urls)}")

all_profiles = []
for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20+1}/{(len(li_urls)+19)//20}: {len(batch)} profiles")
    items = run_actor(PROFILE_SCRAPER, {'urls': batch})
    all_profiles.extend(items)
    time.sleep(2)

print(f"Profiles scraped: {len(all_profiles)}")

# Map by LinkedIn URL
profile_map = {}
for prof in all_profiles:
    # Try matching by URL in the input
    for url in li_urls:
        slug = url.rstrip('/').split('/')[-1].lower()
        prof_name = f"{prof.get('firstName', '')} {prof.get('lastName', '')}".lower()
        # Match by slug in any URL-like field
        if slug and (slug in str(prof).lower()):
            profile_map[url.lower()] = prof
            break

# Enrich each prospect with profile data
for p in all_prospects:
    li = (p.get('linkedin_url') or '').lower()
    prof = profile_map.get(li, {})

    if prof:
        p['li_headline'] = prof.get('headline', '')
        p['li_connections'] = prof.get('connectionsCount', '')
        p['li_followers_person'] = prof.get('followerCount', '')
        p['open_to_work'] = 'Yes' if prof.get('openToWork') else 'No'

        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''
        company_name = p['company'].get('Company', '')
        if current_co and company_name.lower()[:6] in current_co.lower():
            p['role_verified'] = 'Yes'
        elif current_co:
            p['role_verified'] = f'MISMATCH - LI: {p["li_headline"][:40]}'
        else:
            p['role_verified'] = 'Profile found'
    else:
        p['li_headline'] = p.get('title', '')
        p['li_connections'] = ''
        p['li_followers_person'] = ''
        p['open_to_work'] = ''
        p['role_verified'] = 'No profile data'

matched_profiles = sum(1 for p in all_prospects if p.get('li_connections'))
print(f"Profiles matched: {matched_profiles}/{len(all_prospects)}")

# Build output
output_headers = [
    'Company ICP Score', 'Pipeline Action', 'Company', 'Industry', 'Company Location',
    'Company LinkedIn URL', 'Company LI Followers',
    'First Name', 'Last Name', 'Title', 'Seniority',
    'LinkedIn URL', 'LinkedIn Headline', 'Role Verified',
    'LinkedIn Connections', 'LinkedIn Followers', 'Open to Work',
    'Email', 'Email Status',
    "Melinda's Connection Note", "Adrienne's Connection Note",
    'Message 1 - Melinda', 'Message 2 - Melinda', 'Message 3 - Melinda',
    'Message 1 - Adrienne', 'Message 2 - Adrienne', 'Message 3 - Adrienne',
    'Data Source',
]

rows = []
for p in all_prospects:
    co = p['company']
    rows.append({
        'Company ICP Score': co.get('Company ICP Score', ''),
        'Pipeline Action': 'PROCEED',
        'Company': co.get('Company', ''),
        'Industry': co.get('Industry', ''),
        'Company Location': co.get('Location', ''),
        'Company LinkedIn URL': co.get('Company LinkedIn URL', ''),
        'Company LI Followers': co.get('LI Followers', ''),
        'First Name': p.get('first_name', ''),
        'Last Name': p.get('last_name', ''),
        'Title': p.get('title', ''),
        'Seniority': p.get('seniority', ''),
        'LinkedIn URL': p.get('linkedin_url', ''),
        'LinkedIn Headline': p.get('li_headline', ''),
        'Role Verified': p.get('role_verified', ''),
        'LinkedIn Connections': p.get('li_connections', ''),
        'LinkedIn Followers': p.get('li_followers_person', ''),
        'Open to Work': p.get('open_to_work', ''),
        'Email': p.get('email', ''),
        'Email Status': p.get('email_status', ''),
        "Melinda's Connection Note": p.get('note_melinda', ''),
        "Adrienne's Connection Note": p.get('note_adrienne', ''),
        'Message 1 - Melinda': p.get('msg_melinda_1', ''),
        'Message 2 - Melinda': p.get('msg_melinda_2', ''),
        'Message 3 - Melinda': p.get('msg_melinda_3', ''),
        'Message 1 - Adrienne': p.get('msg_adrienne_1', ''),
        'Message 2 - Adrienne': p.get('msg_adrienne_2', ''),
        'Message 3 - Adrienne': p.get('msg_adrienne_3', ''),
        'Data Source': 'Apollo search + Apify profile scrape',
    })

rows.sort(key=lambda x: (
    -(int(x['Company ICP Score']) if str(x.get('Company ICP Score', '')).lstrip('-').isdigit() else 0),
    x['Company'],
))

outfile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_90_prospect_enrichment.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n{'='*60}")
print(f"FINAL RESULTS")
print(f"{'='*60}")
print(f"Companies: {len(companies)}")
print(f"Companies with contacts: {len(contacts_per_co)}")
print(f"Total contacts: {len(all_prospects)}")
print(f"Avg per company: {len(all_prospects)/max(len(contacts_per_co),1):.1f}")
print(f"With LinkedIn URL: {sum(1 for p in all_prospects if p.get('linkedin_url'))}")
print(f"With email: {sum(1 for p in all_prospects if p.get('email'))}")
print(f"Profile verified: {matched_profiles}")
print(f"\nTitle breakdown:")
titles = {}
for p in all_prospects:
    t = p.get('title', '')
    if t: titles[t] = titles.get(t, 0) + 1
for t, c in sorted(titles.items(), key=lambda x: -x[1])[:15]:
    print(f"  {t:<45} {c:>3}")

print(f"\nSaved: {outfile}")
