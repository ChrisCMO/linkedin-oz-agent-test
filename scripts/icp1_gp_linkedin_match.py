#!/usr/bin/env python3
"""Match LinkedIn company URLs to Google Places Seattle companies."""
import sys, os, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE = os.path.join(os.path.dirname(__file__), "..")

# Load Google Places Seattle
gp_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_metro.csv')
with open(gp_file) as f:
    reader = csv.DictReader(f)
    gp_headers = reader.fieldnames
    gp_rows = list(reader)
print(f"Google Places Seattle: {len(gp_rows)}")

# Load X-ray main (has LinkedIn URLs)
xray_map = {}
xray_file = os.path.join(BASE, 'docs', 'TODO', 'v2', 'seattle_all_industries_combined.csv')
with open(xray_file) as f:
    for row in csv.DictReader(f):
        name = (row.get('Company Name') or '').strip().lower()
        url = (row.get('LinkedIn Company URL') or '').strip()
        if name and url:
            xray_map[name] = url

# Load X-ray batch1
batch_file = os.path.join(BASE, 'docs', 'TODO', 'xray_batch1_seattle_linkedin.csv')
try:
    with open(batch_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip().lower()
            url = (row.get('linkedin_url') or '').strip()
            if name and url and name not in xray_map:
                xray_map[name] = url
except: pass

# Load Apollo verified Seattle
apollo_map = {}
apollo_file = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'apollo', 'apollo_seattle_metro_verified.csv')
try:
    with open(apollo_file) as f:
        for row in csv.DictReader(f):
            name = (row.get('company_name') or '').strip().lower()
            url = (row.get('linkedin_url') or '').strip()
            if name and url and 'linkedin.com' in url:
                apollo_map[name] = url
except: pass

print(f"X-ray URLs: {len(xray_map)}")
print(f"Apollo URLs: {len(apollo_map)}")

def find_url(name, sources):
    """Try exact then partial match across sources."""
    name_l = name.lower().strip()
    for source_name, source_map in sources:
        # Exact
        if name_l in source_map:
            return source_map[name_l], source_name
        # Partial
        words = name_l.split()
        for n in range(min(len(words), 4), 1, -1):
            partial = ' '.join(words[:n])
            if len(partial) < 6:
                continue
            for src_name, src_url in source_map.items():
                if src_name.startswith(partial) or partial in src_name:
                    return src_url, source_name + ' (partial)'
    return '', ''

# Match
matched = 0
for r in gp_rows:
    name = (r.get('company_name') or '').strip()
    url, source = find_url(name, [('X-ray', xray_map), ('Apollo', apollo_map)])
    r['linkedin_company_url'] = url
    r['linkedin_url_source'] = source
    if url:
        matched += 1

unmatched = len(gp_rows) - matched
print(f"\nMatched: {matched} ({matched/len(gp_rows)*100:.1f}%)")
print(f"Unmatched: {unmatched}")

# Save with LinkedIn URLs
out_headers = gp_headers + ['linkedin_company_url', 'linkedin_url_source']
outfile = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_with_linkedin.csv')
with open(outfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(gp_rows)

# Also save just the ones WITH LinkedIn URLs
has_li = [r for r in gp_rows if r.get('linkedin_company_url')]
outfile2 = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_has_linkedin.csv')
with open(outfile2, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(has_li)

# And ones WITHOUT
no_li = [r for r in gp_rows if not r.get('linkedin_company_url')]
outfile3 = os.path.join(BASE, 'docs', 'TODO', 'filtered', 'google maps', 'combined', 'google_places_seattle_no_linkedin.csv')
with open(outfile3, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=out_headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(no_li)

print(f"\nFiles saved:")
print(f"  google_places_seattle_with_linkedin.csv ({len(gp_rows)} all, with LI URL column)")
print(f"  google_places_seattle_has_linkedin.csv ({len(has_li)} with LinkedIn)")
print(f"  google_places_seattle_no_linkedin.csv ({len(no_li)} without LinkedIn)")

# Breakdown by source
by_source = {}
for r in gp_rows:
    src = r.get('linkedin_url_source') or 'No match'
    by_source[src] = by_source.get(src, 0) + 1

print(f"\nMatch source breakdown:")
for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
    print(f"  {src:<25} {count:>5}")
