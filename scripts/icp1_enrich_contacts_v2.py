#!/usr/bin/env python3
"""Re-enrich 239 contacts to get LinkedIn URLs, emails, and profile data. Save to new file."""
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

# Read current contacts
infile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_90_prospect_enrichment.csv')
with open(infile) as f:
    reader = csv.DictReader(f)
    old_headers = reader.fieldnames
    rows = list(reader)

print(f"Contacts to enrich: {len(rows)}")

# Step 1: Apollo enrich each contact
print("\nStep 1: Apollo enrichment...")
enriched = 0
for i, r in enumerate(rows):
    first = (r.get('First Name') or '').strip()
    last = (r.get('Last Name') or '').strip()
    company = (r.get('Company') or '').strip()

    if not first or not company:
        continue

    try:
        resp = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
            headers=apollo_h, json={
                'q_keywords': f'{first} {last} {company}',
                'per_page': 1,
            }, timeout=30)
        people = resp.json().get('people', [])

        if people:
            apollo_id = people[0].get('id', '')
            if apollo_id:
                enrich_resp = requests.post('https://api.apollo.io/api/v1/people/match',
                    headers=apollo_h,
                    json={'id': apollo_id, 'reveal_personal_emails': True},
                    timeout=30)
                person = enrich_resp.json().get('person', {})

                if person:
                    r['LinkedIn URL'] = person.get('linkedin_url', '')
                    r['Email'] = person.get('email', '')
                    r['Email Status'] = person.get('email_status', '')
                    r['First Name'] = person.get('first_name', r['First Name'])
                    r['Last Name'] = person.get('last_name', r['Last Name'])
                    r['Title'] = person.get('title', r['Title'])
                    r['Seniority'] = person.get('seniority', r['Seniority'])
                    r['LinkedIn Headline'] = person.get('headline', '')
                    enriched += 1
    except:
        pass

    time.sleep(random.uniform(0.5, 1.0))
    if (i + 1) % 30 == 0:
        print(f"  {i+1}/{len(rows)}: {enriched} enriched")

print(f"Apollo enriched: {enriched}/{len(rows)}")

# Step 2: Apify profile scraper for those with LinkedIn URLs
li_urls = list(set(r.get('LinkedIn URL', '') for r in rows if (r.get('LinkedIn URL') or '').strip()))
print(f"\nStep 2: Apify profile scrape for {len(li_urls)} URLs...")

all_profiles = []
for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20+1}/{(len(li_urls)+19)//20}: {len(batch)} profiles")
    items = run_actor(PROFILE_SCRAPER, {'urls': batch})
    all_profiles.extend(items)
    time.sleep(2)

print(f"Profiles scraped: {len(all_profiles)}")

# Map profiles
profile_map = {}
for prof in all_profiles:
    fn = (prof.get('firstName') or '').lower()
    ln = (prof.get('lastName') or '').lower()
    key = f"{fn} {ln}"
    if key.strip():
        profile_map[key] = prof

# Enrich rows with profile data
profile_matched = 0
for r in rows:
    fn = (r.get('First Name') or '').lower()
    ln = (r.get('Last Name') or '').lower()
    key = f"{fn} {ln}"
    prof = profile_map.get(key, {})

    if prof:
        r['LinkedIn Headline'] = prof.get('headline', '') or r.get('LinkedIn Headline', '')
        r['LinkedIn Connections'] = prof.get('connectionsCount', '')
        r['LinkedIn Followers'] = prof.get('followerCount', '')
        r['Open to Work'] = 'Yes' if prof.get('openToWork') else 'No'

        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''
        company_name = r.get('Company', '')
        if current_co and company_name.lower()[:6] in current_co.lower():
            r['Role Verified'] = 'Yes'
        elif current_co:
            r['Role Verified'] = f'MISMATCH - LI: {r.get("LinkedIn Headline", "")[:40]}'
        else:
            r['Role Verified'] = 'Profile found'
        profile_matched += 1

print(f"Profile matched: {profile_matched}")

# Save to NEW file
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

outfile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_contacts_enriched_v2.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

has_li = sum(1 for r in rows if (r.get('LinkedIn URL') or '').strip())
has_email = sum(1 for r in rows if (r.get('Email') or '').strip())
has_headline = sum(1 for r in rows if (r.get('LinkedIn Headline') or '').strip())
has_connections = sum(1 for r in rows if (r.get('LinkedIn Connections') or '').strip())
has_note = sum(1 for r in rows if (r.get("Melinda's Connection Note") or '').strip())

print(f"\n{'='*60}")
print(f"FINAL: {len(rows)} contacts")
print(f"  With LinkedIn URL: {has_li}")
print(f"  With Email: {has_email}")
print(f"  With Headline: {has_headline}")
print(f"  With Connections: {has_connections}")
print(f"  With Messages: {has_note}")
print(f"\nSaved: {outfile}")

# Title breakdown
titles = {}
for r in rows:
    t = (r.get('Title') or '').strip()
    if t: titles[t] = titles.get(t, 0) + 1
print(f"\nTitle breakdown (top 15):")
for t, c in sorted(titles.items(), key=lambda x: -x[1])[:15]:
    print(f"  {t:<45} {c:>3}")
