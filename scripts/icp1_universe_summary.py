#!/usr/bin/env python3
"""Create summary documentation of all data sources."""
import sys, os, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE = os.path.join(os.path.dirname(__file__), "..")
summary_dir = os.path.join(BASE, 'docs', 'TODO', 'summary')
os.makedirs(summary_dir, exist_ok=True)

# Load all sources
gp_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'TODO', 'v2', 'icp1_companies_google_places.csv'))))
apollo_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'TODO', 'apollo', 'apollo_seattle_all_industries.csv'))))
xray_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'TODO', 'v2', 'seattle_all_industries_combined.csv'))))
zi_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'TODO', 'ZoomInfo', 'zoominfo_seattle_all_industries.csv'))))
zi_v_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'TODO', 'ZoomInfo', 'zoominfo_seattle_verified.csv'))))
f5500_rows = list(csv.DictReader(open(os.path.join(BASE, 'docs', 'ICP-Prospects', 'competitors', 'competitor_auditor_form5500_results.csv'))))

zi_verified = sum(1 for r in zi_v_rows if (r.get('verified_location') or '').strip())

scored_file = os.path.join(BASE, 'docs', 'TODO', 'seattle_100_company_pipeline_results.csv')
scored_rows = list(csv.DictReader(open(scored_file))) if os.path.exists(scored_file) else []

# Industry counts per source
ap_by_ind = {}
for r in apollo_rows:
    ind = (r.get('icp_industry') or 'Unknown').strip()
    ap_by_ind[ind] = ap_by_ind.get(ind, 0) + 1

xr_by_ind = {}
for r in xray_rows:
    ind = (r.get('Industry') or 'Unknown').strip()
    xr_by_ind[ind] = xr_by_ind.get(ind, 0) + 1

zi_by_ind = {}
for r in zi_rows:
    ind = (r.get('icp_industry') or 'Unknown').strip()
    zi_by_ind[ind] = zi_by_ind.get(ind, 0) + 1

industries = ['Manufacturing', 'Commercial Real Estate', 'Professional Services', 'Hospitality', 'Nonprofit', 'Construction']

# Pipeline scored
scored_proceed = sum(1 for r in scored_rows if r.get('Pipeline Action') == 'PROCEED')
scored_flag = sum(1 for r in scored_rows if r.get('Pipeline Action') == 'FLAG')
scored_skip = sum(1 for r in scored_rows if r.get('Pipeline Action') == 'SKIP')
scored_exclude = sum(1 for r in scored_rows if r.get('Pipeline Action') == 'HARD EXCLUDE')

# Source comparison CSV
with open(os.path.join(summary_dir, 'source_comparison.csv'), 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['Source', 'Total Companies', 'Has Location', 'Has Employees', 'Has Revenue', 'Has LinkedIn URL', 'Has Website', 'Cost', 'Notes'])
    w.writerow(['Google Places', len(gp_rows), 'Yes (full address)', 'No', 'No', 'No', 'Yes (some)', 'Free (~$0.03/query)', 'Verified operating businesses with physical addresses'])
    w.writerow(['Apollo (paginated)', len(apollo_rows), 'Partial (city/state)', 'Yes', 'Yes', 'Yes', 'Yes (domain)', 'Free search, $1/enrichment', 'Companies with finance/exec contacts. WA DC contamination.'])
    w.writerow(['Google X-ray LinkedIn', len(xray_rows), 'Yes (cross-referenced)', 'Yes (from LI scrape)', 'No', 'Yes (company page)', 'Yes (from LI)', '~$0.01/query Apify', 'Companies with LinkedIn pages. Zero ban risk.'])
    w.writerow(['ZoomInfo (company search)', len(zi_rows), 'No (free tier)', 'No (free tier)', 'No (free tier)', 'No', 'No', 'Free search', 'Largest count but names only in free tier.'])
    w.writerow(['ZoomInfo (verified)', zi_verified, 'Yes (cross-ref)', 'Some', 'Some', 'Some', 'Some', 'Free', 'ZoomInfo companies confirmed by another source.'])
    w.writerow(['Form 5500 Competitors', len(f5500_rows), 'Yes (city/state)', 'Yes (participants)', 'No', 'No', 'No', 'Free (DOL)', 'Confirmed competitor audit clients.'])

# Industry breakdown CSV
with open(os.path.join(summary_dir, 'industry_breakdown.csv'), 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['Industry', 'Google Places', 'Apollo', 'Google X-ray', 'ZoomInfo'])
    for ind in industries:
        gp_count = sum(1 for r in gp_rows if ind.lower()[:5] in (r.get('industry_search') or '').lower())
        w.writerow([ind, gp_count, ap_by_ind.get(ind, 0), xr_by_ind.get(ind, 0), zi_by_ind.get(ind, 0)])
    w.writerow(['TOTAL', len(gp_rows), len(apollo_rows), len(xray_rows), len(zi_rows)])

# Main summary text
with open(os.path.join(summary_dir, 'universe_summary.txt'), 'w') as f:
    f.write('VWC ICP 1 - SEATTLE PROSPECT UNIVERSE SUMMARY\n')
    f.write('=' * 60 + '\n')
    f.write('Date: March 26, 2026\n\n')

    f.write('DATA SOURCES AND COMPANY COUNTS\n')
    f.write('-' * 60 + '\n\n')
    f.write('Google Places:                  %6d companies\n' % len(gp_rows))
    f.write('  Verified operating businesses with physical addresses.\n\n')
    f.write('ZoomInfo (company search):      %6d companies\n' % len(zi_rows))
    f.write('  Largest count but free tier returns names only.\n')
    f.write('  %d verified against other sources.\n' % zi_verified)
    f.write('  %d unverified.\n\n' % (len(zi_rows) - zi_verified))
    f.write('Apollo (with pagination):       %6d companies\n' % len(apollo_rows))
    f.write('  Companies with finance/executive contacts.\n\n')
    f.write('Google X-ray LinkedIn:          %6d companies\n' % len(xray_rows))
    f.write('  Companies with LinkedIn company pages.\n\n')
    f.write('Form 5500 (competitors):        %6d filings (WA/OR)\n' % len(f5500_rows))
    f.write('  Companies using Moss Adams, BDO, Sweeney Conrad, Baker Tilly.\n\n')

    f.write('\nINDUSTRY BREAKDOWN\n')
    f.write('-' * 60 + '\n\n')
    f.write('%-25s %8s %8s %8s %8s\n' % ('Industry', 'GP', 'Apollo', 'X-ray', 'ZoomInfo'))
    f.write('-' * 60 + '\n')
    for ind in industries:
        gp_c = sum(1 for r in gp_rows if ind.lower()[:5] in (r.get('industry_search') or '').lower())
        f.write('%-25s %8d %8d %8d %8d\n' % (ind, gp_c, ap_by_ind.get(ind, 0), xr_by_ind.get(ind, 0), zi_by_ind.get(ind, 0)))
    f.write('%-25s %8d %8d %8d %8d\n' % ('TOTAL', len(gp_rows), len(apollo_rows), len(xray_rows), len(zi_rows)))

    f.write('\n\nPIPELINE SCORING RESULTS (%d Seattle companies tested)\n' % len(scored_rows))
    f.write('-' * 60 + '\n\n')
    f.write('PROCEED (80+):     %5d companies\n' % scored_proceed)
    f.write('FLAG (60-79):      %5d companies\n' % scored_flag)
    f.write('SKIP (<60):        %5d companies\n' % scored_skip)
    f.write('HARD EXCLUDE (0):  %5d companies\n' % scored_exclude)
    f.write('TOTAL SCORED:      %5d\n' % len(scored_rows))

    f.write('\n\nSOURCE STRENGTHS AND WEAKNESSES\n')
    f.write('-' * 60 + '\n\n')
    f.write('Google Places:\n')
    f.write('  + Verified physical addresses and operating status\n')
    f.write('  + Rating and review count (signals business maturity)\n')
    f.write('  + Free or near-free\n')
    f.write('  - No employee count or revenue\n')
    f.write('  - No finance contacts\n\n')
    f.write('Apollo:\n')
    f.write('  + Finance/executive contacts with titles\n')
    f.write('  + Employee count and revenue from org enrichment\n')
    f.write('  + LinkedIn URLs and email addresses\n')
    f.write('  - Washington DC contamination in location filter\n')
    f.write('  - Free search limited; enrichment $1/contact\n\n')
    f.write('Google X-ray LinkedIn:\n')
    f.write('  + Finds companies with LinkedIn presence\n')
    f.write('  + Zero ban risk (external, no account needed)\n')
    f.write('  + Company page scrape gives reliable employee count\n')
    f.write('  + Discovers companies invisible to databases\n')
    f.write('  - Limited to companies with LinkedIn pages\n')
    f.write('  - ~$0.01/query via Apify\n\n')
    f.write('ZoomInfo:\n')
    f.write('  + Largest company count (10,116)\n')
    f.write('  + Good for broad industry + metro region searches\n')
    f.write('  - Free tier returns names only (no details)\n')
    f.write('  - 90%% of results unverified\n')
    f.write('  - Enrichment credits needed for useful data\n\n')
    f.write('Form 5500:\n')
    f.write('  + Definitive competitor auditor data\n')
    f.write('  + Free public data from DOL\n')
    f.write('  + Covers private companies\n')
    f.write('  - Only benefit plan auditor, not general audit\n')
    f.write('  - Only companies with 100+ plan participants\n\n')

    f.write('\nRECOMMENDED PIPELINE APPROACH\n')
    f.write('-' * 60 + '\n\n')
    f.write('1. Start with Google Places (9,663 verified businesses) as base\n')
    f.write('2. Enrich with LinkedIn company page scrape for employee count\n')
    f.write('3. Score at company level (80+ PROCEED, 60-79 FLAG, <60 SKIP)\n')
    f.write('4. For PROCEED companies: Apollo contact search + enrichment\n')
    f.write('5. Cross-reference against Form 5500 for competitor signal\n')
    f.write('6. Zero-contact companies: Google X-ray LinkedIn discovery\n')
    f.write('7. Validate all contacts via Apify live LinkedIn profile scrape\n')
    f.write('8. Generate personalized connection notes and messages\n\n')

    f.write('\nFILES REFERENCE\n')
    f.write('-' * 60 + '\n\n')
    f.write('Google Places:     docs/TODO/v2/icp1_companies_google_places.csv\n')
    f.write('Apollo:            docs/TODO/apollo/apollo_seattle_all_industries.csv\n')
    f.write('Google X-ray:      docs/TODO/v2/seattle_all_industries_combined.csv\n')
    f.write('ZoomInfo:          docs/TODO/ZoomInfo/zoominfo_seattle_all_industries.csv\n')
    f.write('ZoomInfo verified: docs/TODO/ZoomInfo/zoominfo_seattle_verified.csv\n')
    f.write('Form 5500:         docs/ICP-Prospects/competitors/competitor_auditor_form5500_results.csv\n')
    f.write('Pipeline scored:   docs/TODO/seattle_100_company_pipeline_results.csv\n')

print('Summary files created:')
print('  docs/TODO/summary/universe_summary.txt')
print('  docs/TODO/summary/source_comparison.csv')
print('  docs/TODO/summary/industry_breakdown.csv')
