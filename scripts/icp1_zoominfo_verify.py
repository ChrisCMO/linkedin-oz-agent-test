#!/usr/bin/env python3
"""Cross-reference ZoomInfo companies against Google Places, Apollo, and LinkedIn X-ray for location verification."""
import sys, os, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE = os.path.join(os.path.dirname(__file__), "..")

# Load Google Places
gp = {}
gp_file = os.path.join(BASE, 'docs', 'TODO', 'v2', 'icp1_companies_google_places.csv')
with open(gp_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip().lower()
        if name:
            gp[name] = {
                'address': row.get('address', ''),
                'phone': row.get('phone', ''),
                'website': row.get('website', ''),
                'domain': row.get('domain', ''),
                'rating': row.get('rating', ''),
                'reviews': row.get('review_count', ''),
            }
print(f"Google Places: {len(gp)} companies")

# Load Apollo
apollo = {}
apollo_file = os.path.join(BASE, 'docs', 'TODO', 'apollo', 'apollo_seattle_all_industries.csv')
with open(apollo_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip().lower()
        if name:
            apollo[name] = {
                'employees': row.get('employees', ''),
                'revenue': row.get('revenue', ''),
                'domain': row.get('domain', ''),
                'linkedin_url': row.get('linkedin_url', ''),
                'city': row.get('city', ''),
                'state': row.get('state', ''),
                'apollo_industry': row.get('apollo_industry', ''),
            }
print(f"Apollo: {len(apollo)} companies")

# Load LinkedIn X-ray
xray = {}
xray_file = os.path.join(BASE, 'docs', 'TODO', 'v2', 'seattle_all_industries_combined.csv')
with open(xray_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('Company Name') or '').strip().lower()
        if name:
            xray[name] = {
                'li_url': row.get('LinkedIn Company URL', ''),
                'location': row.get('Location', ''),
                'description': row.get('Description', ''),
            }
print(f"X-ray: {len(xray)} companies")

# Load ZoomInfo
zi_file = os.path.join(BASE, 'docs', 'TODO', 'ZoomInfo', 'zoominfo_seattle_all_industries.csv')
zi_rows = []
with open(zi_file) as f:
    for row in csv.DictReader(f):
        zi_rows.append(row)
print(f"ZoomInfo: {len(zi_rows)} companies")

# Cross-reference
matched_gp = 0
matched_apollo = 0
matched_xray = 0
matched_any = 0

def partial_match(name, source):
    """Try exact then partial match."""
    if name in source:
        return source[name]
    words = name.split()
    for n in range(min(len(words), 4), 1, -1):
        partial = ' '.join(words[:n])
        if len(partial) < 6:
            continue
        for src_name in source:
            if src_name.startswith(partial) or partial in src_name:
                return source[src_name]
    return None

for r in zi_rows:
    name = r['company_name'].strip().lower()
    found_any = False

    # Google Places
    gp_match = partial_match(name, gp)
    if gp_match:
        r['gp_address'] = gp_match.get('address', '')
        r['website'] = gp_match.get('website', '') or gp_match.get('domain', '')
        r['gp_rating'] = gp_match.get('rating', '')
        matched_gp += 1
        found_any = True

    # Apollo
    ap_match = partial_match(name, apollo)
    if ap_match:
        r['employees'] = ap_match.get('employees', '')
        r['revenue'] = ap_match.get('revenue', '')
        r['city'] = ap_match.get('city', '')
        r['state'] = ap_match.get('state', '')
        r['apollo_industry'] = ap_match.get('apollo_industry', '')
        r['linkedin_url'] = ap_match.get('linkedin_url', '')
        if not r.get('website'):
            r['website'] = ap_match.get('domain', '')
        matched_apollo += 1
        found_any = True

    # LinkedIn X-ray
    xr_match = partial_match(name, xray)
    if xr_match:
        r['li_url'] = xr_match.get('li_url', '')
        r['li_location'] = xr_match.get('location', '')
        r['li_description'] = xr_match.get('description', '')
        matched_xray += 1
        found_any = True

    if found_any:
        matched_any += 1

    # Determine best location
    loc = ''
    loc_source = ''
    if r.get('gp_address'):
        loc = r['gp_address']
        loc_source = 'Google Places'
    elif r.get('city') and r.get('state'):
        loc = f"{r['city']}, {r['state']}"
        loc_source = 'Apollo'
    elif r.get('li_location'):
        loc = r['li_location']
        loc_source = 'LinkedIn X-ray'
    r['verified_location'] = loc
    r['location_source'] = loc_source

has_location = sum(1 for r in zi_rows if r.get('verified_location'))
has_employees = sum(1 for r in zi_rows if r.get('employees'))

print(f"\nCross-reference results:")
print(f"  Matched Google Places: {matched_gp}")
print(f"  Matched Apollo: {matched_apollo}")
print(f"  Matched LinkedIn X-ray: {matched_xray}")
print(f"  Matched ANY source: {matched_any}")
print(f"  Has verified location: {has_location}")
print(f"  Has employee count: {has_employees}")
print(f"  No match anywhere: {len(zi_rows) - matched_any}")

# Save
outfile = os.path.join(BASE, 'docs', 'TODO', 'ZoomInfo', 'zoominfo_seattle_verified.csv')
out_headers = ['icp_industry', 'company_name', 'industry_keyword', 'zi_id',
               'verified_location', 'location_source', 'employees', 'revenue',
               'website', 'linkedin_url', 'apollo_industry',
               'gp_address', 'gp_rating', 'li_location', 'li_description']

with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(zi_rows)

print(f"\nSaved: {outfile}")

by_source = {}
for r in zi_rows:
    src = r.get('location_source') or 'Unverified'
    by_source[src] = by_source.get(src, 0) + 1

print(f"\nLocation verification breakdown:")
for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
    print(f"  {src:<20} {count:>6}")
