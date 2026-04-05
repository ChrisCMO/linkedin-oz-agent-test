#!/usr/bin/env python3
"""Find LinkedIn company URLs for Google Places Seattle companies using Serper.dev."""
import sys, os, csv, time, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

SERPER_KEY = os.environ['SERPER_API_KEY']
BASE = os.path.join(os.path.dirname(__file__), "..")

# Load companies without LinkedIn URLs
infile = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_no_linkedin.csv')
with open(infile) as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    rows = list(reader)

print(f"Companies to search: {len(rows)}")

found = 0
not_found = 0

for i, r in enumerate(rows):
    name = (r.get('company_name') or '').strip()
    if not name:
        continue

    try:
        resp = requests.post('https://google.serper.dev/search',
            headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
            json={'q': f'site:linkedin.com/company "{name}"', 'num': 3},
            timeout=15)

        if resp.status_code != 200:
            print(f"  [{i+1}/{len(rows)}] {name}: HTTP {resp.status_code}")
            continue

        data = resp.json()
        results = data.get('organic', [])

        # Find first linkedin.com/company result
        li_url = ''
        for result in results:
            link = result.get('link', '')
            if 'linkedin.com/company/' in link:
                # Clean URL (remove country subdomain variants)
                if link.startswith('https://www.linkedin.com/company/') or link.startswith('http://www.linkedin.com/company/'):
                    li_url = link.split('?')[0]
                    break
                elif 'linkedin.com/company/' in link:
                    # Convert country variant to www
                    slug = link.split('linkedin.com/company/')[-1].split('/')[0].split('?')[0]
                    li_url = f'https://www.linkedin.com/company/{slug}'
                    break

        if li_url:
            r['linkedin_company_url'] = li_url
            r['linkedin_url_source'] = 'Serper.dev'
            found += 1
        else:
            r['linkedin_company_url'] = ''
            r['linkedin_url_source'] = 'Not found'
            not_found += 1

        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(rows)}] Found: {found}, Not found: {not_found}")

    except Exception as e:
        print(f"  [{i+1}/{len(rows)}] {name}: ERROR - {e}")
        r['linkedin_company_url'] = ''
        r['linkedin_url_source'] = 'Error'
        not_found += 1

    # Small delay to be respectful
    time.sleep(0.2)

# Save results
out_headers = headers + ['linkedin_company_url', 'linkedin_url_source']

# All with LinkedIn URLs added
outfile = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_serper_results.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Just the ones we found
found_rows = [r for r in rows if r.get('linkedin_company_url')]
outfile2 = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_newly_found_linkedin.csv')
with open(outfile2, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(found_rows)

print(f"\nFINAL RESULTS:")
print(f"  Searched: {len(rows)}")
print(f"  Found LinkedIn URL: {found}")
print(f"  Not found: {not_found}")
print(f"  Hit rate: {found/len(rows)*100:.1f}%")
print(f"\nSaved:")
print(f"  {outfile} (all {len(rows)} with results)")
print(f"  {outfile2} ({len(found_rows)} newly found)")

# Now merge with the ones that already had LinkedIn URLs
already_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_has_linkedin.csv')
already = list(csv.DictReader(open(already_file)))

total_with_li = len(already) + len(found_rows)
total_all = len(already) + len(rows)
print(f"\nOVERALL COVERAGE:")
print(f"  Previously had LinkedIn URL: {len(already)}")
print(f"  Newly found via Serper: {len(found_rows)}")
print(f"  Total with LinkedIn URL: {total_with_li}")
print(f"  Total Seattle companies: {total_all}")
print(f"  Coverage: {total_with_li/total_all*100:.1f}%")
