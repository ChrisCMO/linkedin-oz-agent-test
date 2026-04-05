#!/usr/bin/env python3
"""Re-score companies with Apollo ownership verification."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
OPENAI_KEY = os.environ['OPENAI_API_KEY']
apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}

TARGET_FILE = sys.argv[1] if len(sys.argv) > 1 else 'docs/deliverables/week2/scored/seattle_manufacturing_scored.csv'

with open(TARGET_FILE) as fh:
    reader = csv.DictReader(fh)
    headers = reader.fieldnames
    rows = list(reader)

print(f'File: {TARGET_FILE}')
print(f'Total: {len(rows)}')

# Step 1: Apollo ownership check
print('\nStep 1: Apollo ownership check...')
for i, r in enumerate(rows):
    domain = (r.get('Domain') or r.get('Website') or '').strip()
    domain = domain.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]

    if not domain:
        r['Ownership'] = 'Unknown - no domain'
        continue

    try:
        resp = requests.post('https://api.apollo.io/api/v1/organizations/enrich',
            headers=apollo_h, json={'domain': domain}, timeout=30)
        org = resp.json().get('organization', {}) or {}

        symbol = org.get('publicly_traded_symbol')
        funding_stage = org.get('latest_funding_stage')

        if symbol:
            r['Ownership'] = f'PUBLIC ({symbol})'
        elif funding_stage and any(s in str(funding_stage).lower() for s in ['series', 'private equity', 'venture']):
            r['Ownership'] = f'VC/PE-backed ({funding_stage})'
        else:
            r['Ownership'] = 'Private (confirmed via Apollo)'
    except:
        r['Ownership'] = 'Unknown - lookup failed'

    time.sleep(0.3)
    if (i + 1) % 50 == 0:
        print(f'  {i+1}/{len(rows)}')

by_own = {}
for r in rows:
    own = r.get('Ownership', '')
    if 'PUBLIC' in own: key = 'PUBLIC'
    elif 'VC/PE' in own: key = 'VC/PE-backed'
    elif 'Private' in own: key = 'Private'
    else: key = 'Unknown'
    by_own[key] = by_own.get(key, 0) + 1

print(f'\nOwnership:')
for k, v in by_own.items():
    print(f'  {k}: {v}')

# Step 2: Re-score
print('\nStep 2: Re-scoring...')
prompt = open(os.path.join(os.path.dirname(__file__), '..', 'mvp', 'backend', 'services', 'scoring.py')).read().split('COMPANY_SYSTEM_PROMPT = """')[1].split('"""')[0]

ownership_note = """
IMPORTANT OWNERSHIP UPDATE: Each company now has a verified ownership status from Apollo.
- "Private (confirmed via Apollo)" = confirmed private, no public ticker, no VC/PE funding. Score ownership_structure 15/15.
- "PUBLIC (TICKER)" = publicly traded. Hard exclude (score 0) for ICP 1.
- "VC/PE-backed (Series X)" = venture or PE backed. Hard exclude (score 0) for ICP 1.
- "Unknown" = could not verify. Score ownership_structure 12/15.
"""

all_scores = []
for i in range(0, len(rows), 15):
    batch = rows[i:i + 15]
    score_input = [{
        'company_id': c['Company'].lower().replace(' ', '_')[:25],
        'company_name': c['Company'],
        'industry': c.get('Industry', ''),
        'linkedin_employees': c.get('Employees (LinkedIn)', ''),
        'apollo_employees': c.get('Employees (Apollo)', ''),
        'revenue': c.get('Revenue', ''),
        'location': c.get('Location', ''),
        'ownership': c.get('Ownership', ''),
        'linkedin_page': c.get('Company LinkedIn URL', ''),
        'linkedin_followers': c.get('LI Followers', ''),
        'website': c.get('Domain', ''),
    } for c in batch]

    try:
        resp = requests.post('https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
            json={'model': 'gpt-5.4', 'messages': [
                {'role': 'system', 'content': prompt + ownership_note},
                {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
            ], 'temperature': 0.3, 'max_completion_tokens': 8000}, timeout=90)
        rj = resp.json()
        if 'choices' not in rj:
            print(f'  Batch {i // 15 + 1}: API error - {str(rj.get("error", ""))[:60]}')
            time.sleep(5)
            continue
        raw = rj['choices'][0]['message']['content'].strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        scored = json.loads(raw)
        scores = scored.get('companies', scored if isinstance(scored, list) else [])
        all_scores.extend(scores)
        print(f'  Batch {i // 15 + 1}: scored {len(scores)}')
    except Exception as e:
        print(f'  Batch {i // 15 + 1}: FAILED - {e}')
    time.sleep(2)

score_map = {s.get('company_name', ''): s for s in all_scores}

# Step 3: Update rows
changes = 0
for r in rows:
    sc = score_map.get(r['Company'], {})
    if not sc:
        continue

    old_score = r.get('Company ICP Score', '')
    old_action = r.get('Pipeline Action', '')
    score = sc.get('score', '')
    bd = sc.get('breakdown', {})

    r['Company ICP Score'] = score
    if score == 0: r['Pipeline Action'] = 'HARD EXCLUDE'
    elif isinstance(score, int) and score >= 80: r['Pipeline Action'] = 'PROCEED'
    elif isinstance(score, int) and score >= 60: r['Pipeline Action'] = 'FLAG'
    elif isinstance(score, int): r['Pipeline Action'] = 'SKIP'

    r['Score Breakdown'] = ' | '.join(f'{k}: {v}' for k, v in bd.items())
    r['Reasoning'] = sc.get('reasoning', '')

    # Why This Score
    dims = bd
    reasons = []
    li_emp = r.get('Employees (LinkedIn)', '')
    rev = r.get('Revenue', '')
    own = r.get('Ownership', '')

    if dims.get('company_size', 20) < 16:
        reasons.append(f'Small company ({li_emp} employees)')
    if dims.get('revenue_fit', 15) <= 10:
        if not rev: reasons.append('Revenue unknown')
        else: reasons.append(f'Revenue {rev} below sweet spot')
    if dims.get('ownership_structure', 15) < 15:
        if 'PUBLIC' in own: reasons.append(f'Public company ({own})')
        elif 'VC/PE' in own: reasons.append(f'{own}')
        elif 'Unknown' in own: reasons.append('Ownership unconfirmed')
    if dims.get('digital_footprint', 15) < 10:
        reasons.append('Weak digital footprint')
    if dims.get('industry_fit', 20) < 16:
        reasons.append(f'Industry: {r.get("Industry", "")}')
    if dims.get('geography', 15) < 13:
        reasons.append(f'Location: {r.get("Location", "")}')

    action = r['Pipeline Action']
    changed = str(old_score) != str(score)
    change_note = f' (was {old_score})' if changed else ''

    if action == 'PROCEED':
        r['Why This Score'] = f'Strong match across all dimensions.{change_note}'
    elif action == 'HARD EXCLUDE':
        if 'PUBLIC' in own: r['Why This Score'] = f'Public company - {own}. Hard exclusion.'
        elif 'VC/PE' in own: r['Why This Score'] = f'{own}. Hard exclusion per ICP spec.'
        elif rev and any(c in str(rev) for c in ['150', '200', '300', '400', '500', '600', '700', '800', '900']):
            r['Why This Score'] = f'Revenue {rev} exceeds $150M cap.{change_note}'
        else: r['Why This Score'] = sc.get('reasoning', '')[:120]
    elif action == 'FLAG':
        gap = 80 - int(score) if isinstance(score, int) else 0
        r['Why This Score'] = (f'{gap} points from PROCEED{change_note}. ' + '; '.join(reasons)) if reasons else f'Borderline.{change_note}'
    elif action == 'SKIP':
        r['Why This Score'] = ('Multiple weak dimensions: ' + '; '.join(reasons)) if reasons else 'Does not meet criteria.'

    if changed: changes += 1

rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if str(x.get('Company ICP Score', '')).lstrip('-').isdigit() else 0)))

with open(TARGET_FILE, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)

proceed = sum(1 for r in rows if r.get('Pipeline Action') == 'PROCEED')
flag = sum(1 for r in rows if r.get('Pipeline Action') == 'FLAG')
skip = sum(1 for r in rows if r.get('Pipeline Action') == 'SKIP')
exclude = sum(1 for r in rows if r.get('Pipeline Action') == 'HARD EXCLUDE')

print(f'\nFINAL: {len(rows)} companies, {changes} scores changed')
print(f'  PROCEED: {proceed}')
print(f'  FLAG:    {flag}')
print(f'  SKIP:    {skip}')
print(f'  EXCLUDE: {exclude}')

print(f'\nHard excluded:')
for r in rows:
    if r.get('Pipeline Action') == 'HARD EXCLUDE':
        print(f'  {r["Company"]:<40} {r["Ownership"]:<30} {r.get("Why This Score","")[:60]}')
