#!/usr/bin/env python3
"""Merge all verified Seattle companies from all sources into one deliverable CSV."""
import sys, os, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE = os.path.join(os.path.dirname(__file__), "..")

seattle_metro = ['seattle', 'bellevue', 'tacoma', 'redmond', 'kirkland', 'everett', 'renton',
    'kent', 'auburn', 'olympia', 'federal way', 'tukwila', 'shoreline', 'bothell',
    'issaquah', 'puyallup', 'lynnwood', 'woodinville', 'kenmore', 'burien', 'seatac',
    'sammamish', 'lakewood', 'sumner', 'bainbridge', 'mountlake terrace', 'mercer island',
    'des moines', 'maple valley', 'snoqualmie', 'duvall', 'snohomish', 'marysville',
    'lake stevens', 'arlington', 'monroe', 'enumclaw', 'bonney lake', 'buckley',
    'steilacoom', 'university place', 'fife', 'edgewood', 'milton', 'pacific', 'algona',
    'normandy park', 'clyde hill', 'medina', 'hunts point', 'yarrow point', 'newcastle',
    'covington', 'north bend', 'granite falls', 'sultan']

all_companies = {}  # key: company name lowercase -> merged record

# ── 1. Google Places (richest location data) ──
print("Loading Google Places...")
gp_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_metro.csv')
with open(gp_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip()
        name_key = name.lower()
        if not name:
            continue
        all_companies[name_key] = {
            'Company Name': name,
            'Address': row.get('address', ''),
            'City': row.get('city', ''),
            'State': row.get('state', 'WA'),
            'Phone': row.get('phone', ''),
            'Website': row.get('website', ''),
            'Domain': row.get('domain', ''),
            'Google Rating': row.get('rating', ''),
            'Google Reviews': row.get('review_count', ''),
            'Google Status': row.get('business_status', ''),
            'Industry Search': row.get('industry_search', ''),
            'LinkedIn Company URL': '',
            'LI Employees': '',
            'LI Followers': '',
            'LI Description': '',
            'Apollo Employees': '',
            'Apollo Revenue': '',
            'Apollo Industry': '',
            'Competitor Auditor': '',
            'Deep Research Source': '',
            'Data Sources': 'Google Places',
        }

gp_count = len(all_companies)
print(f"  Google Places: {gp_count}")

# ── 2. Google X-ray LinkedIn (has LinkedIn URLs + location) ──
print("Loading Google X-ray...")
xray_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google-x-search', 'xray_seattle_metro_verified.csv')
xray_added = 0
xray_enriched = 0
with open(xray_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('Company Name') or '').strip()
        name_key = name.lower()
        li_url = (row.get('LinkedIn Company URL') or '').strip()
        location = (row.get('Location') or '').strip()
        desc = (row.get('Description') or '').strip()
        industry = (row.get('Industry') or '').strip()

        if name_key in all_companies:
            # Enrich existing
            if li_url and not all_companies[name_key]['LinkedIn Company URL']:
                all_companies[name_key]['LinkedIn Company URL'] = li_url
            if desc and not all_companies[name_key]['LI Description']:
                all_companies[name_key]['LI Description'] = desc[:200]
            if industry and not all_companies[name_key]['Industry Search']:
                all_companies[name_key]['Industry Search'] = industry
            all_companies[name_key]['Data Sources'] += ', X-ray'
            xray_enriched += 1
        elif name:
            all_companies[name_key] = {
                'Company Name': name,
                'Address': '',
                'City': location.split(',')[0].strip() if ',' in location else '',
                'State': 'WA',
                'Phone': '',
                'Website': '',
                'Domain': '',
                'Google Rating': '',
                'Google Reviews': '',
                'Google Status': '',
                'Industry Search': industry,
                'LinkedIn Company URL': li_url,
                'LI Employees': '',
                'LI Followers': '',
                'LI Description': desc[:200],
                'Apollo Employees': '',
                'Apollo Revenue': '',
                'Apollo Industry': '',
                'Competitor Auditor': '',
                'Deep Research Source': '',
                'Data Sources': 'X-ray',
            }
            xray_added += 1

# X-ray batch1
batch_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google-x-search-batch1', 'xray_batch1_new_companies.csv')
try:
    with open(batch_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip()
            name_key = name.lower()
            li_url = (row.get('linkedin_url') or '').strip()
            if name_key in all_companies:
                if li_url and not all_companies[name_key]['LinkedIn Company URL']:
                    all_companies[name_key]['LinkedIn Company URL'] = li_url
                    all_companies[name_key]['Data Sources'] += ', X-ray batch1'
            elif name:
                all_companies[name_key] = {
                    'Company Name': name,
                    'Address': '', 'City': (row.get('city') or '').strip(), 'State': 'WA',
                    'Phone': '', 'Website': '', 'Domain': '',
                    'Google Rating': '', 'Google Reviews': '', 'Google Status': '',
                    'Industry Search': (row.get('industry') or '').strip(),
                    'LinkedIn Company URL': li_url,
                    'LI Employees': '', 'LI Followers': '', 'LI Description': '',
                    'Apollo Employees': '', 'Apollo Revenue': '', 'Apollo Industry': '',
                    'Competitor Auditor': '', 'Deep Research Source': '',
                    'Data Sources': 'X-ray batch1',
                }
                xray_added += 1
except: pass

print(f"  X-ray: {xray_added} new, {xray_enriched} enriched existing")

# ── 3. Apollo (verified Seattle) ──
print("Loading Apollo...")
apollo_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'apollo', 'apollo_seattle_metro_verified.csv')
apollo_added = 0
apollo_enriched = 0
try:
    with open(apollo_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip()
            name_key = name.lower()
            if name_key in all_companies:
                emp = (row.get('employees') or '').strip()
                rev = (row.get('revenue') or '').strip()
                ind = (row.get('apollo_industry') or '').strip()
                li = (row.get('linkedin_url') or '').strip()
                if emp: all_companies[name_key]['Apollo Employees'] = emp
                if rev: all_companies[name_key]['Apollo Revenue'] = rev
                if ind: all_companies[name_key]['Apollo Industry'] = ind
                if li and not all_companies[name_key]['LinkedIn Company URL']:
                    all_companies[name_key]['LinkedIn Company URL'] = li
                all_companies[name_key]['Data Sources'] += ', Apollo'
                apollo_enriched += 1
            elif name:
                all_companies[name_key] = {
                    'Company Name': name,
                    'Address': '', 'City': '', 'State': 'WA',
                    'Phone': '', 'Website': (row.get('domain') or '').strip(), 'Domain': (row.get('domain') or '').strip(),
                    'Google Rating': '', 'Google Reviews': '', 'Google Status': '',
                    'Industry Search': (row.get('icp_industry') or '').strip(),
                    'LinkedIn Company URL': (row.get('linkedin_url') or '').strip(),
                    'LI Employees': '', 'LI Followers': '', 'LI Description': '',
                    'Apollo Employees': (row.get('employees') or '').strip(),
                    'Apollo Revenue': (row.get('revenue') or '').strip(),
                    'Apollo Industry': (row.get('apollo_industry') or '').strip(),
                    'Competitor Auditor': '', 'Deep Research Source': '',
                    'Data Sources': 'Apollo',
                }
                apollo_added += 1
except: pass
print(f"  Apollo: {apollo_added} new, {apollo_enriched} enriched existing")

# ── 4. Deep Research ──
print("Loading Deep Research...")
dr_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'deep-research', 'deep_research_seattle_metro.csv')
dr_added = 0
dr_enriched = 0
try:
    with open(dr_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip()
            name_key = name.lower()
            source = (row.get('source') or '').strip()
            if name_key in all_companies:
                all_companies[name_key]['Deep Research Source'] = source
                all_companies[name_key]['Data Sources'] += ', Deep Research'
                dr_enriched += 1
            elif name:
                all_companies[name_key] = {
                    'Company Name': name,
                    'Address': '', 'City': (row.get('city') or '').strip(), 'State': 'WA',
                    'Phone': '', 'Website': '', 'Domain': '',
                    'Google Rating': '', 'Google Reviews': '', 'Google Status': '',
                    'Industry Search': (row.get('industry') or '').strip(),
                    'LinkedIn Company URL': (row.get('url') or '').strip(),
                    'LI Employees': '', 'LI Followers': '', 'LI Description': '',
                    'Apollo Employees': '', 'Apollo Revenue': '', 'Apollo Industry': '',
                    'Competitor Auditor': '', 'Deep Research Source': source,
                    'Data Sources': 'Deep Research',
                }
                dr_added += 1
except: pass
print(f"  Deep Research: {dr_added} new, {dr_enriched} enriched existing")

# ── 5. ZoomInfo (verified Seattle) ──
print("Loading ZoomInfo verified...")
zi_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'zoominfo', 'zoominfo_seattle_metro_verified.csv')
zi_added = 0
zi_enriched = 0
try:
    with open(zi_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip()
            name_key = name.lower()
            if name_key in all_companies:
                all_companies[name_key]['Data Sources'] += ', ZoomInfo'
                zi_enriched += 1
            elif name:
                all_companies[name_key] = {
                    'Company Name': name,
                    'Address': '', 'City': '', 'State': 'WA',
                    'Phone': '', 'Website': '', 'Domain': '',
                    'Google Rating': '', 'Google Reviews': '', 'Google Status': '',
                    'Industry Search': (row.get('industry_keyword') or '').strip(),
                    'LinkedIn Company URL': '',
                    'LI Employees': '', 'LI Followers': '', 'LI Description': '',
                    'Apollo Employees': '', 'Apollo Revenue': '', 'Apollo Industry': '',
                    'Competitor Auditor': '', 'Deep Research Source': '',
                    'Data Sources': 'ZoomInfo',
                }
                zi_added += 1
except: pass
print(f"  ZoomInfo: {zi_added} new, {zi_enriched} enriched existing")

# ── 6. Form 5500 Competitors ──
print("Loading Form 5500 competitors...")
f5500_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'form5500', 'form5500_competitors_seattle_metro.csv')
f5500_matched = 0
f5500_added = 0
try:
    with open(f5500_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company') or '').strip()
            name_key = name.lower()
            auditor = (row.get('auditor') or '').strip()
            # Normalize auditor
            aud_lower = auditor.lower()
            if 'baker tilly' in aud_lower: auditor_clean = 'Baker Tilly'
            elif 'moss adams' in aud_lower: auditor_clean = 'Moss Adams'
            elif 'sweeney conrad' in aud_lower: auditor_clean = 'Sweeney Conrad'
            elif 'bdo' in aud_lower: auditor_clean = 'BDO'
            else: auditor_clean = auditor

            if name_key in all_companies:
                all_companies[name_key]['Competitor Auditor'] = auditor_clean
                all_companies[name_key]['Data Sources'] += ', Form 5500'
                f5500_matched += 1
            else:
                # Try partial match
                matched = False
                for key in all_companies:
                    if name_key[:10] in key or key[:10] in name_key:
                        all_companies[key]['Competitor Auditor'] = auditor_clean
                        all_companies[key]['Data Sources'] += ', Form 5500'
                        f5500_matched += 1
                        matched = True
                        break
                if not matched:
                    all_companies[name_key] = {
                        'Company Name': name,
                        'Address': '', 'City': (row.get('city') or '').strip(), 'State': 'WA',
                        'Phone': '', 'Website': '', 'Domain': '',
                        'Google Rating': '', 'Google Reviews': '', 'Google Status': '',
                        'Industry Search': '',
                        'LinkedIn Company URL': '',
                        'LI Employees': '', 'LI Followers': '', 'LI Description': '',
                        'Apollo Employees': '', 'Apollo Revenue': '', 'Apollo Industry': '',
                        'Competitor Auditor': auditor_clean,
                        'Deep Research Source': '',
                        'Data Sources': 'Form 5500',
                    }
                    f5500_added += 1
except: pass
print(f"  Form 5500: {f5500_matched} matched, {f5500_added} new")

# ── 7. Pipeline scored results ──
print("Loading pipeline scores...")
scored_file = os.path.join(BASE, 'docs', 'TODO', 'seattle_100_company_pipeline_results.csv')
scored_matched = 0
try:
    with open(scored_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('Company') or '').strip()
            name_key = name.lower()
            if name_key in all_companies:
                all_companies[name_key]['ICP Score'] = row.get('Company ICP Score', '')
                all_companies[name_key]['Pipeline Action'] = row.get('Pipeline Action', '')
                all_companies[name_key]['LI Employees'] = row.get('Employees (LinkedIn)', '') or all_companies[name_key].get('LI Employees', '')
                all_companies[name_key]['LI Followers'] = row.get('LI Followers', '') or all_companies[name_key].get('LI Followers', '')
                if row.get('Company LinkedIn URL'):
                    all_companies[name_key]['LinkedIn Company URL'] = row.get('Company LinkedIn URL', '') or all_companies[name_key].get('LinkedIn Company URL', '')
                scored_matched += 1
except: pass
print(f"  Pipeline scores: {scored_matched} matched")

# ── Build output ──
out_headers = [
    'Company Name', 'City', 'State', 'Address', 'Phone', 'Website', 'Domain',
    'Google Rating', 'Google Reviews', 'Google Status',
    'LinkedIn Company URL', 'LI Employees', 'LI Followers', 'LI Description',
    'Apollo Employees', 'Apollo Revenue', 'Apollo Industry',
    'Industry Search', 'Competitor Auditor', 'Deep Research Source',
    'ICP Score', 'Pipeline Action',
    'Data Sources',
]

rows = sorted(all_companies.values(), key=lambda x: x['Company Name'])

# Add missing fields
for r in rows:
    r.setdefault('ICP Score', '')
    r.setdefault('Pipeline Action', '')

outfile = os.path.join(BASE, 'docs', 'TODO', 'deliverable', 'seattle_verified_companies_all_sources.csv')
os.makedirs(os.path.dirname(outfile), exist_ok=True)

with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Stats
has_li = sum(1 for r in rows if r.get('LinkedIn Company URL'))
has_address = sum(1 for r in rows if r.get('Address'))
has_phone = sum(1 for r in rows if r.get('Phone'))
has_website = sum(1 for r in rows if r.get('Website') or r.get('Domain'))
has_rating = sum(1 for r in rows if r.get('Google Rating'))
has_apollo_emp = sum(1 for r in rows if r.get('Apollo Employees'))
has_competitor = sum(1 for r in rows if r.get('Competitor Auditor'))
has_score = sum(1 for r in rows if r.get('ICP Score'))
multi_source = sum(1 for r in rows if ',' in r.get('Data Sources', ''))

print(f"\n{'='*60}")
print(f"MERGED DELIVERABLE")
print(f"{'='*60}")
print(f"Total unique Seattle companies: {len(rows)}")
print(f"")
print(f"Data coverage:")
print(f"  Has physical address:    {has_address:>5}")
print(f"  Has phone number:        {has_phone:>5}")
print(f"  Has website/domain:      {has_website:>5}")
print(f"  Has Google rating:       {has_rating:>5}")
print(f"  Has LinkedIn URL:        {has_li:>5}")
print(f"  Has Apollo employee ct:  {has_apollo_emp:>5}")
print(f"  Has competitor auditor:  {has_competitor:>5}")
print(f"  Has ICP score:           {has_score:>5}")
print(f"  In multiple sources:     {multi_source:>5}")
print(f"")
print(f"Saved: {outfile}")
