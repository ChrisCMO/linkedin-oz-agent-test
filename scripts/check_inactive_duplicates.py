#!/usr/bin/env python3
"""Check if inactive LinkedIn contacts have duplicate/newer profiles.
Scrapes the known profile, then X-ray searches for alternate profiles."""
import sys, os, csv, time, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'
GOOGLE_SERP = 'nFJndFXA5zjCTuudP'
BASE = os.path.join(os.path.dirname(__file__), "..")

INPUT = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'companies_with_inactive_contacts.csv')
OUTPUT = os.path.join(BASE, 'docs', 'deliverables', 'samples', 'new batch', 'inactive_duplicate_check.csv')

FINANCE_KW = ['cfo', 'chief financial', 'controller', 'vp finance', 'vp of finance',
              'vice president of finance', 'director of finance', 'finance', 'accounting',
              'treasurer', 'comptroller']


def run_actor(actor_id, payload):
    for attempt in range(3):
        try:
            if attempt > 0:
                time.sleep(10)
            r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                              headers=apify_h, json=payload, timeout=30)
            if r.status_code == 402:
                print(f"    [OUT OF CREDITS]")
                return None
            if r.status_code != 201:
                continue
            run_id = r.json()['data']['id']
            ds = r.json()['data']['defaultDatasetId']
            for _ in range(30):
                time.sleep(5)
                s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                                 headers=apify_h, timeout=30).json()['data']['status']
                if s in ('SUCCEEDED', 'FAILED', 'ABORTED'):
                    break
            if s == 'SUCCEEDED':
                return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                                    headers=apify_h, timeout=15).json()
        except Exception as e:
            print(f"    Error: {e}")
    return []


def normalize_url(url):
    url = (url or '').strip().split('?')[0].rstrip('/')
    url = url.replace('http://', 'https://')
    if 'https://linkedin.com' in url:
        url = url.replace('https://linkedin.com', 'https://www.linkedin.com')
    return url


# Load inactive finance contacts
with open(INPUT) as fh:
    rows = list(csv.DictReader(fh))

inactive = [r for r in rows if r.get('Contact Status') == 'INACTIVE'
            and any(k in r.get('Title', '').lower() for k in FINANCE_KW)]

print(f"Inactive finance contacts to check: {len(inactive)}")
print("=" * 70)

results = []
out_of_credits = False

for i, c in enumerate(inactive):
    if out_of_credits:
        break

    company = c['Company']
    first = c['First Name']
    last = c['Last Name']
    title = c['Title']
    known_url = normalize_url(c.get('LinkedIn URL', ''))
    name = f"{first} {last}"

    print(f"\n[{i+1}/{len(inactive)}] {name} | {title[:40]} | {company}")
    print(f"  Known URL: {known_url}")

    # Step 1: Scrape the known (inactive) profile
    print(f"  Scraping known profile...")
    known_prof = None
    if known_url:
        prof_items = run_actor(PROFILE_SCRAPER, {'urls': [known_url]})
        if prof_items:
            known_prof = prof_items[0]
            kp_headline = known_prof.get('headline', '')
            kp_connections = known_prof.get('connectionsCount', 0)
            kp_followers = known_prof.get('followerCount', 0)
            kp_positions = known_prof.get('currentPosition', [])
            kp_company = kp_positions[0].get('companyName', '') if kp_positions else ''
            kp_location = known_prof.get('location', '')
            if isinstance(kp_location, dict):
                kp_location = kp_location.get('linkedinText', str(kp_location))
            print(f"  Known: {kp_headline[:50]} | {kp_company} | Conn: {kp_connections} | Loc: {kp_location}")
        else:
            print(f"  Known profile scrape failed")

    # Step 2: X-ray search for alternate profiles
    print(f"  Searching for alternate profiles...")
    co_short = company.split('(')[0].split(',')[0].strip()
    co_first = co_short.split()[0] if co_short else ''

    # Multiple search strategies
    search_queries = [
        f'site:linkedin.com/in "{first} {last}" "{co_first}"',
        f'site:linkedin.com/in "{first} {last}"',
    ]

    alt_urls = {}
    for q in search_queries:
        items = run_actor(GOOGLE_SERP, {
            'queries': q,
            'maxPagesPerQuery': 1,
            'resultsPerPage': 10,
            'countryCode': 'us',
        })
        if items is None:
            out_of_credits = True
            break
        if items:
            for item in items:
                for result in item.get('organicResults', [item]):
                    url = result.get('url', result.get('link', ''))
                    if 'linkedin.com/in/' in url:
                        norm = normalize_url(url)
                        if norm != known_url and norm not in alt_urls:
                            alt_urls[norm] = {
                                'title': result.get('title', ''),
                                'desc': result.get('description', ''),
                            }
        time.sleep(random.uniform(2, 3))

    if out_of_credits:
        break

    if not alt_urls:
        print(f"  No alternate profiles found")
        result = {
            'Company': company,
            'First Name': first,
            'Last Name': last,
            'Title': title,
            'Known LinkedIn URL': known_url,
            'Known Headline': known_prof.get('headline', '') if known_prof else '',
            'Known Connections': str(known_prof.get('connectionsCount', '')) if known_prof else '',
            'Known Location': kp_location if known_prof else '',
            'Known Company': kp_company if known_prof else '',
            'Alternate URL': '',
            'Alt Headline': '',
            'Alt Connections': '',
            'Alt Location': '',
            'Alt Company': '',
            'Verdict': 'NO ALTERNATE FOUND',
        }
        results.append(result)
        continue

    print(f"  Found {len(alt_urls)} alternate URL(s)")

    # Step 3: Scrape each alternate and compare
    for alt_url, alt_info in alt_urls.items():
        # Pre-filter: check if name appears in snippet
        snippet = (alt_info['title'] + ' ' + alt_info['desc']).lower()
        if last.lower() not in snippet:
            print(f"    Skip {alt_url[:50]} — name not in snippet")
            continue

        print(f"    Scraping {alt_url[:55]}...")
        alt_items = run_actor(PROFILE_SCRAPER, {'urls': [alt_url]})
        if not alt_items:
            print(f"    Scrape failed")
            continue

        alt = alt_items[0]
        alt_first = alt.get('firstName', '')
        alt_last = alt.get('lastName', '')
        alt_headline = alt.get('headline', '')
        alt_connections = alt.get('connectionsCount', 0)
        alt_followers = alt.get('followerCount', 0)
        alt_positions = alt.get('currentPosition', [])
        alt_company = alt_positions[0].get('companyName', '') if alt_positions else ''
        alt_location = alt.get('location', '')
        if isinstance(alt_location, dict):
            alt_location = alt_location.get('linkedinText', str(alt_location))

        # Check if this is the same person
        name_match = (last.lower() in alt_last.lower() or alt_last.lower() in last.lower())
        if not name_match:
            print(f"    Not same person: {alt_first} {alt_last}")
            continue

        # Check if this mentions the same company
        co_words = [w.lower() for w in co_short.split() if len(w) > 3]
        company_ref = any(w in (alt_company + ' ' + alt_headline).lower() for w in co_words)

        print(f"    Alt: {alt_first} {alt_last} | {alt_headline[:50]}")
        print(f"    Alt Company: {alt_company} | Conn: {alt_connections} | Loc: {alt_location}")

        # Determine verdict
        if name_match and company_ref and alt_connections > (known_prof.get('connectionsCount', 0) if known_prof else 0):
            verdict = 'LIKELY NEWER PROFILE — more connections + same company'
        elif name_match and company_ref:
            verdict = 'POSSIBLE DUPLICATE — same person, same company reference'
        elif name_match and not company_ref:
            verdict = 'SAME NAME, DIFFERENT COMPANY — may have moved'
        else:
            verdict = 'UNCLEAR — review manually'

        print(f"    Verdict: {verdict}")

        result = {
            'Company': company,
            'First Name': first,
            'Last Name': last,
            'Title': title,
            'Known LinkedIn URL': known_url,
            'Known Headline': known_prof.get('headline', '') if known_prof else '',
            'Known Connections': str(known_prof.get('connectionsCount', '')) if known_prof else '',
            'Known Location': kp_location if known_prof else '',
            'Known Company': kp_company if known_prof else '',
            'Alternate URL': alt_url,
            'Alt Headline': alt_headline,
            'Alt Connections': str(alt_connections),
            'Alt Location': str(alt_location),
            'Alt Company': alt_company,
            'Verdict': verdict,
        }
        results.append(result)
        time.sleep(1)

    # If no alternates passed filters, still record
    if not any(r['Known LinkedIn URL'] == known_url and r.get('Alternate URL') for r in results):
        if not any(r['Known LinkedIn URL'] == known_url for r in results):
            results.append({
                'Company': company,
                'First Name': first,
                'Last Name': last,
                'Title': title,
                'Known LinkedIn URL': known_url,
                'Known Headline': known_prof.get('headline', '') if known_prof else '',
                'Known Connections': str(known_prof.get('connectionsCount', '')) if known_prof else '',
                'Known Location': kp_location if known_prof else '',
                'Known Company': kp_company if known_prof else '',
                'Alternate URL': '',
                'Alt Headline': '',
                'Alt Connections': '',
                'Alt Location': '',
                'Alt Company': '',
                'Verdict': 'NO MATCHING ALTERNATE FOUND',
            })

    # Save progress
    if results:
        with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=list(results[0].keys()), extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)

# Summary
print(f"\n{'='*70}")
print(f"RESULTS")
print(f"{'='*70}")
print(f"Contacts checked: {len(inactive)}")
newer = [r for r in results if 'NEWER' in r.get('Verdict', '')]
dupes = [r for r in results if 'DUPLICATE' in r.get('Verdict', '')]
moved = [r for r in results if 'MOVED' in r.get('Verdict', '') or 'DIFFERENT COMPANY' in r.get('Verdict', '')]
none_found = [r for r in results if 'NO' in r.get('Verdict', '')]

print(f"\nLikely newer profiles: {len(newer)}")
for r in newer:
    print(f"  {r['First Name']} {r['Last Name']} at {r['Company']}")
    print(f"    Old: {r['Known LinkedIn URL']} (conn: {r['Known Connections']})")
    print(f"    New: {r['Alternate URL']} (conn: {r['Alt Connections']})")

print(f"\nPossible duplicates: {len(dupes)}")
for r in dupes:
    print(f"  {r['First Name']} {r['Last Name']} at {r['Company']}")
    print(f"    Old: {r['Known LinkedIn URL']}")
    print(f"    Alt: {r['Alternate URL']}")

print(f"\nMay have moved companies: {len(moved)}")
for r in moved:
    print(f"  {r['First Name']} {r['Last Name']} | Was: {r['Company']} → Now: {r['Alt Company']}")

print(f"\nNo alternate found: {len(none_found)}")
print(f"\nOutput: {OUTPUT}")
