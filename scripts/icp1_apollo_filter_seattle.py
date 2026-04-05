#!/usr/bin/env python3
"""Filter Apollo 3,944 companies to Seattle metro by cross-referencing all other sources."""
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

def is_seattle(location):
    loc = location.lower()
    return any(city in loc for city in seattle_metro)

# Load all verified location sources
print("Loading verified sources...")

# Google Places (has city field)
gp_names = {}
with open(os.path.join(BASE, 'docs', 'TODO', 'v2', 'icp1_companies_google_places.csv')) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip().lower()
        city = (row.get('city') or '').strip()
        if name and city:
            loc = f"{city}, {row.get('state', 'WA')}"
            gp_names[name] = {'location': loc, 'is_seattle': city.lower() in seattle_metro}

# Google X-ray (has Location field)
xray_names = {}
with open(os.path.join(BASE, 'docs', 'TODO', 'v2', 'seattle_all_industries_combined.csv')) as f:
    for row in csv.DictReader(f):
        name = (row.get('Company Name') or '').strip().lower()
        loc = (row.get('Location') or '').strip()
        if name and loc:
            xray_names[name] = {'location': loc, 'is_seattle': is_seattle(loc)}

# Deep Research
dr_names = {}
with open(os.path.join(BASE, 'docs', 'TODO', 'v2', 'deep_research_seattle_universe.csv')) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip().lower()
        city = (row.get('city') or '').strip()
        if name:
            dr_names[name] = {'location': city, 'is_seattle': True}  # All were Seattle

# ZoomInfo verified
zi_names = {}
with open(os.path.join(BASE, 'docs', 'TODO', 'ZoomInfo', 'zoominfo_seattle_verified.csv')) as f:
    for row in csv.DictReader(f):
        name = (row.get('company_name') or '').strip().lower()
        loc = (row.get('verified_location') or '').strip()
        if name and loc:
            zi_names[name] = {'location': loc, 'is_seattle': is_seattle(loc)}

print(f"  Google Places: {len(gp_names)} companies with location")
print(f"  Google X-ray: {len(xray_names)} companies with location")
print(f"  Deep Research: {len(dr_names)} companies")
print(f"  ZoomInfo verified: {len(zi_names)} companies with location")

# Load Apollo
apollo_file = os.path.join(BASE, 'docs', 'TODO', 'apollo', 'apollo_seattle_all_industries.csv')
with open(apollo_file) as f:
    reader = csv.DictReader(f)
    ap_headers = reader.fieldnames
    ap_rows = list(reader)

print(f"\nApollo companies to classify: {len(ap_rows)}")

def partial_match(name, source):
    """Match by exact name or first 2+ meaningful words."""
    if name in source:
        return source[name]
    words = name.split()
    for n in range(min(len(words), 4), 1, -1):
        partial = ' '.join(words[:n])
        if len(partial) < 5:
            continue
        for src_name in source:
            if src_name.startswith(partial) or partial in src_name:
                return source[src_name]
    return None

# Cross-reference each Apollo company
seattle_verified = []
seattle_likely = []  # From Seattle-specific Apollo search but no cross-ref
non_seattle = []
unverified = []

matched_gp = 0
matched_xray = 0
matched_dr = 0
matched_zi = 0

for r in ap_rows:
    name = r['company_name'].strip().lower()
    found = False
    location = ''
    source = ''

    # Try each source in priority order
    match = partial_match(name, gp_names)
    if match:
        location = match['location']
        source = 'Google Places'
        if match['is_seattle']:
            r['verified_location'] = location
            r['location_source'] = source
            seattle_verified.append(r)
            matched_gp += 1
            found = True
        else:
            r['verified_location'] = location
            r['location_source'] = source
            non_seattle.append(r)
            found = True

    if not found:
        match = partial_match(name, xray_names)
        if match:
            location = match['location']
            source = 'Google X-ray'
            if match['is_seattle']:
                r['verified_location'] = location
                r['location_source'] = source
                seattle_verified.append(r)
                matched_xray += 1
                found = True
            else:
                r['verified_location'] = location
                r['location_source'] = source
                non_seattle.append(r)
                found = True

    if not found:
        match = partial_match(name, dr_names)
        if match:
            r['verified_location'] = match['location']
            r['location_source'] = 'Deep Research'
            seattle_verified.append(r)
            matched_dr += 1
            found = True

    if not found:
        match = partial_match(name, zi_names)
        if match:
            location = match['location']
            source = 'ZoomInfo'
            if match['is_seattle']:
                r['verified_location'] = location
                r['location_source'] = source
                seattle_verified.append(r)
                matched_zi += 1
                found = True
            else:
                r['verified_location'] = location
                r['location_source'] = source
                non_seattle.append(r)
                found = True

    if not found:
        r['verified_location'] = ''
        r['location_source'] = 'Unverified'
        unverified.append(r)

print(f"\nCross-reference results:")
print(f"  Matched Google Places: {matched_gp}")
print(f"  Matched Google X-ray: {matched_xray}")
print(f"  Matched Deep Research: {matched_dr}")
print(f"  Matched ZoomInfo: {matched_zi}")
print(f"  Total Seattle verified: {len(seattle_verified)}")
print(f"  Non-Seattle (verified elsewhere): {len(non_seattle)}")
print(f"  Unverified: {len(unverified)}")

# Save
outdir = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'apollo')
os.makedirs(os.path.join(outdir, 'combined'), exist_ok=True)

out_headers = ap_headers + ['verified_location', 'location_source']

for name, rows in [
    ('seattle_metro_verified', seattle_verified),
    ('non_seattle', non_seattle),
    ('unverified', unverified),
]:
    outfile = os.path.join(outdir, f'apollo_{name}.csv')
    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

# Combined with tag
combined_file = os.path.join(outdir, 'combined', 'apollo_all_filtered.csv')
all_tagged = []
for r in seattle_verified:
    r2 = dict(r); r2['Filter Status'] = 'Seattle Metro (verified)'; all_tagged.append(r2)
for r in non_seattle:
    r2 = dict(r); r2['Filter Status'] = 'Non-Seattle (verified elsewhere)'; all_tagged.append(r2)
for r in unverified:
    r2 = dict(r); r2['Filter Status'] = 'Unverified'; all_tagged.append(r2)

with open(combined_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers + ['Filter Status'], extrasaction='ignore')
    writer.writeheader()
    writer.writerows(all_tagged)

# Per industry (Seattle only)
by_ind = {}
for r in seattle_verified:
    ind = (r.get('icp_industry') or 'Unknown').strip()
    if ind not in by_ind: by_ind[ind] = []
    by_ind[ind].append(r)

for ind, rows in by_ind.items():
    safe = ind.lower().replace(' ', '_')[:30]
    outfile = os.path.join(outdir, f'apollo_seattle_{safe}.csv')
    with open(outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

print(f"\nFiles saved to: {outdir}/")
print(f"  apollo_seattle_metro_verified.csv ({len(seattle_verified)})")
print(f"  apollo_non_seattle.csv ({len(non_seattle)})")
print(f"  apollo_unverified.csv ({len(unverified)})")
print(f"  combined/apollo_all_filtered.csv ({len(all_tagged)})")

print(f"\nSeattle per industry:")
for ind, rows in sorted(by_ind.items(), key=lambda x: -len(x[1])):
    print(f"  {ind:<25} {len(rows):>5}")

print(f"\n{'='*60}")
print(f"UPDATED SEATTLE METRO COUNTS")
print(f"{'='*60}")
print(f"  Google Places:      4,043")
print(f"  Google X-ray:         924")
print(f"  Apollo (verified):    {len(seattle_verified)}")
print(f"  Deep Research:        323")
print(f"  ZoomInfo (verified):  741")
