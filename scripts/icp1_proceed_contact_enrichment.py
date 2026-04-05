#!/usr/bin/env python3
"""Find contacts at 90 PROCEED companies - Steps 5-11 of the pipeline."""
import sys, os, csv, json, time, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
OPENAI_KEY = os.environ['OPENAI_API_KEY']
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

# Load PROCEED companies
infile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'all_proceed_companies.csv')
with open(infile) as f:
    companies = list(csv.DictReader(f))

print(f"PROCEED companies: {len(companies)}")

prospects = []
no_contacts = []

# Step 5: Apollo contact search for each company
print("\nStep 5: Apollo contact search...")
for i, co in enumerate(companies):
    domain = (co.get('Domain') or co.get('Website') or '').strip()
    domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
    company_name = co.get('Company', '')

    search_body = {
        'person_titles': ['CFO', 'Chief Financial Officer', 'Controller',
                          'VP Finance', 'Director of Finance', 'Owner', 'President', 'CEO'],
        'person_seniorities': ['c_suite', 'vp', 'director', 'owner'],
        'per_page': 3,
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

    if people:
        # Take top contact (first result)
        p = people[0]
        prospects.append({
            'company': co,
            'apollo_id': p.get('id', ''),
            'first_name': p.get('first_name', ''),
            'last_name': p.get('last_name', ''),
            'title': p.get('title', ''),
            'seniority': p.get('seniority', ''),
            'linkedin_url': p.get('linkedin_url', ''),
            'email': p.get('email', ''),
            'email_status': '',
            'all_contacts_count': len(people),
        })
    else:
        no_contacts.append(co)

    time.sleep(0.5)
    if (i + 1) % 20 == 0:
        print(f"  {i+1}/{len(companies)}: {len(prospects)} contacts found, {len(no_contacts)} with 0")

    # Save progress every 30 companies
    if (i + 1) % 30 == 0:
        print(f"  Saving progress...")
        with open(os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', '_progress.json'), 'w') as pf:
            json.dump({'found': len(prospects), 'no_contacts': len(no_contacts), 'processed': i+1}, pf)

print(f"\nApollo results: {len(prospects)} contacts found, {len(no_contacts)} companies with 0 contacts")

# Step 7: Apollo person enrichment for top contacts
print("\nStep 7: Apollo person enrichment...")
for i, p in enumerate(prospects):
    if p.get('apollo_id'):
        try:
            r = requests.post('https://api.apollo.io/api/v1/people/match',
                              headers=apollo_h,
                              json={'id': p['apollo_id'], 'reveal_personal_emails': True},
                              timeout=30)
            person = r.json().get('person', {})
            if person:
                p['first_name'] = person.get('first_name', p['first_name'])
                p['last_name'] = person.get('last_name', p['last_name'])
                p['title'] = person.get('title', p['title'])
                p['linkedin_url'] = person.get('linkedin_url', p['linkedin_url'])
                p['email'] = person.get('email', p['email'])
                p['email_status'] = person.get('email_status', '')
                p['seniority'] = person.get('seniority', p['seniority'])
                p['headline'] = person.get('headline', '')
        except:
            pass
        time.sleep(random.uniform(0.5, 1.5))
        if (i + 1) % 20 == 0:
            print(f"  Enriched {i+1}/{len(prospects)}")

# Step 8: Apify profile scraper for role verification (batch)
print("\nStep 8: Profile verification...")
li_urls = [p['linkedin_url'] for p in prospects if p.get('linkedin_url')]
print(f"  Profiles to verify: {len(li_urls)}")

all_profiles = []
for i in range(0, len(li_urls), 20):
    batch = li_urls[i:i+20]
    print(f"  Batch {i//20+1}: {len(batch)} profiles")
    items = run_actor(PROFILE_SCRAPER, {'urls': batch})
    all_profiles.extend(items)
    time.sleep(2)

# Map profiles by URL slug
profile_map = {}
for prof in all_profiles:
    url = (prof.get('url') or '').lower()
    slug = url.rstrip('/').split('/')[-1] if url else ''
    if slug:
        profile_map[slug] = prof

# Enrich prospects with profile data
for p in prospects:
    li = p.get('linkedin_url', '')
    slug = li.rstrip('/').split('/')[-1].lower() if li else ''
    prof = profile_map.get(slug, {})
    if prof:
        p['li_headline'] = prof.get('headline', '')
        p['li_connections'] = prof.get('connectionsCount', '')
        p['li_followers'] = prof.get('followerCount', '')
        current = prof.get('currentPosition', [])
        current_co = current[0].get('companyName', '') if current else ''
        company_name = p['company'].get('Company', '')
        if current_co and company_name.lower()[:6] in current_co.lower():
            p['role_verified'] = 'Yes'
        elif current_co:
            p['role_verified'] = f'MISMATCH - LinkedIn: {p["li_headline"][:50]}'
        else:
            p['role_verified'] = 'Profile found'
    else:
        p['li_headline'] = ''
        p['li_connections'] = ''
        p['li_followers'] = ''
        p['role_verified'] = 'No profile data'

# Steps 10-11: Generate connection notes + messages
print("\nSteps 10-11: Connection notes + messages...")

NOTE_PROMPT = """You are {sender_name}, an audit partner at VWC CPAs in Seattle. Write a short LinkedIn connection request note TO the prospect below.
Rules: MUST be under 200 characters. Address by first name. Warm, professional, not salesy. No emojis, no cliches. Do NOT mention VWC CPAs by name.
Positioning: VWC has been in business 50 years. Partner-level attention, same partners year after year. Boutique care. Never name competitors.
Return ONLY the note text."""

MSG_PROMPT = """You are writing LinkedIn follow-up messages for {sender_name}, an audit partner at VWC CPAs in Seattle.
Generate exactly 3 messages for a drip sequence to the prospect below.
Message 1 (after connect): Reference something specific. 2-4 sentences.
Message 2 (~2 weeks, no reply): Different angle. 2-4 sentences.
Message 3 (~4 weeks): Final light touch. 1-2 sentences.
Positioning: VWC: 50 years, partner-level attention, same partners year after year. Boutique care. Never name competitors.
Rules: No emojis. No cliches. Under 300 chars each. Sound human.
Return JSON array: [{{"step": 1, "text": "..."}}, {{"step": 2, "text": "..."}}, {{"step": 3, "text": "..."}}]
Return ONLY valid JSON."""

for i, p in enumerate(prospects):
    first = p.get('first_name', '')
    if not first:
        continue

    co = p['company']
    ctx = json.dumps({
        'prospect': {
            'first_name': first,
            'title': p.get('title', ''),
            'company_name': co.get('Company', ''),
            'location': co.get('Location', ''),
            'industry': co.get('Industry', ''),
            'headline': p.get('li_headline', p.get('title', '')),
        },
        'company': {
            'industry': co.get('Industry', ''),
            'employees': co.get('Employees (LinkedIn)', ''),
            'revenue': co.get('Revenue', ''),
            'description': co.get('LI Description', '')[:150],
        },
    }, indent=2)

    for sender in ['Melinda Johnson', 'Adrienne Nordland']:
        key = sender.split()[0].lower()
        try:
            # Connection note
            resp = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-5.4', 'messages': [
                    {'role': 'system', 'content': NOTE_PROMPT.format(sender_name=sender)},
                    {'role': 'user', 'content': ctx},
                ], 'temperature': 0.8, 'max_completion_tokens': 100}, timeout=60)
            note = resp.json()['choices'][0]['message']['content'].strip().strip('"')
            if len(note) > 200: note = note[:197] + '...'
            p[f'note_{key}'] = note

            # Messages
            resp2 = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-5.4', 'messages': [
                    {'role': 'system', 'content': MSG_PROMPT.format(sender_name=sender)},
                    {'role': 'user', 'content': ctx},
                ], 'temperature': 0.8, 'max_completion_tokens': 1000}, timeout=60)
            raw = resp2.json()['choices'][0]['message']['content'].strip()
            if raw.startswith('```'): raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
            msgs = json.loads(raw)
            for m in msgs:
                p[f'msg_{key}_{m["step"]}'] = m['text']
        except Exception as e:
            p[f'note_{key}'] = ''
            for s in range(1, 4):
                p[f'msg_{key}_{s}'] = ''

        time.sleep(0.5)

    if (i + 1) % 10 == 0:
        print(f"  Messages generated: {i+1}/{len(prospects)}")

# Build output CSV
output_headers = [
    'Company ICP Score', 'Pipeline Action', 'Company', 'Industry', 'Company Location',
    'Company LinkedIn URL', 'Company LI Followers',
    'First Name', 'Last Name', 'Title', 'Seniority',
    'LinkedIn URL', 'LinkedIn Headline', 'Role Verified',
    'Activity Level', 'Recent Post Date', 'Recent Post Text',
    'Posts Count', 'Reposts Count', 'Total Feed Items',
    'LinkedIn Connections', 'LinkedIn Followers',
    'Email', 'Email Status',
    "Melinda's Connection Note", "Adrienne's Connection Note",
    'Message 1 - Melinda', 'Message 2 - Melinda', 'Message 3 - Melinda',
    'Message 1 - Adrienne', 'Message 2 - Adrienne', 'Message 3 - Adrienne',
    'Data Source',
]

rows = []
for p in prospects:
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
        'Activity Level': '',
        'Recent Post Date': '',
        'Recent Post Text': '',
        'Posts Count': '',
        'Reposts Count': '',
        'Total Feed Items': '',
        'LinkedIn Connections': p.get('li_connections', ''),
        'LinkedIn Followers': p.get('li_followers', ''),
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
        'Data Source': f'Apollo search + enrich (contacts: {p.get("all_contacts_count", 1)})',
    })

rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if str(x.get('Company ICP Score', '')).lstrip('-').isdigit() else 0)))

outfile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_90_prospect_enrichment.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=output_headers)
    writer.writeheader()
    writer.writerows(rows)

print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"PROCEED companies: {len(companies)}")
print(f"Contacts found: {len(prospects)}")
print(f"No contacts: {len(no_contacts)}")
print(f"Ratio: {len(prospects)}/{len(companies)} ({len(prospects)/len(companies)*100:.0f}%)")
print(f"\nWith email: {sum(1 for p in prospects if p.get('email'))}")
print(f"With LinkedIn URL: {sum(1 for p in prospects if p.get('linkedin_url'))}")
print(f"Role verified: {sum(1 for p in prospects if p.get('role_verified') == 'Yes')}")
print(f"\nSaved: {outfile}")

# Also save the no-contacts list
no_contacts_file = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_no_contacts.csv')
if no_contacts:
    with open(no_contacts_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(no_contacts[0].keys()))
        writer.writeheader()
        writer.writerows(no_contacts)
    print(f"No-contacts list: {no_contacts_file} ({len(no_contacts)} companies)")
