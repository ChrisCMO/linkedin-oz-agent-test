#!/usr/bin/env python3
"""Score the unscored companies from the pipeline results."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

OPENAI_KEY = os.environ['OPENAI_API_KEY']

all_rows = []
unscored = []
csvfile = 'docs/TODO/seattle_100_company_pipeline_results.csv'
with open(csvfile) as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        all_rows.append(row)
        if not row.get('Company ICP Score') or row['Company ICP Score'] == '':
            unscored.append(row)

print(f"Total: {len(all_rows)}, Unscored: {len(unscored)}")

prompt = open('mvp/backend/services/scoring.py').read().split('COMPANY_SYSTEM_PROMPT = """')[1].split('"""')[0]

all_scores = []
for i in range(0, len(unscored), 10):
    batch = unscored[i:i+10]
    score_input = [{
        'company_id': c['Company'].lower().replace(' ', '_')[:25],
        'company_name': c['Company'],
        'industry': c.get('Industry', ''),
        'linkedin_employees': c.get('Employees (LinkedIn)', ''),
        'apollo_employees': c.get('Employees (Apollo)', ''),
        'revenue': c.get('Revenue', ''),
        'location': c.get('Location', ''),
        'ownership': 'Unknown - appears private',
        'linkedin_page': c.get('Company LinkedIn URL', ''),
        'linkedin_followers': c.get('LI Followers', ''),
        'website': c.get('Website', ''),
    } for c in batch]

    try:
        resp = requests.post('https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
            json={'model': 'gpt-5.4', 'messages': [
                {'role': 'system', 'content': prompt},
                {'role': 'user', 'content': json.dumps({'companies': score_input}, indent=2)},
            ], 'temperature': 0.3, 'max_completion_tokens': 6000}, timeout=90)
        rj = resp.json()
        if 'choices' not in rj:
            print(f"  Batch {i//10+1}: API error - {str(rj.get('error',''))[:80]}")
            continue
        raw = rj['choices'][0]['message']['content'].strip()
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        scored = json.loads(raw)
        scores = scored.get('companies', scored if isinstance(scored, list) else [])
        all_scores.extend(scores)
        print(f"  Batch {i//10+1}: scored {len(scores)}")
    except Exception as e:
        print(f"  Batch {i//10+1}: FAILED - {e}")
    time.sleep(2)

score_map = {s.get('company_name', ''): s for s in all_scores}

for row in all_rows:
    if not row.get('Company ICP Score') or row['Company ICP Score'] == '':
        sc = score_map.get(row['Company'], {})
        score = sc.get('score', '')
        bd = sc.get('breakdown', {})
        row['Company ICP Score'] = score
        if score == 0: row['Pipeline Action'] = 'HARD EXCLUDE'
        elif isinstance(score, int) and score >= 80: row['Pipeline Action'] = 'PROCEED'
        elif isinstance(score, int) and score >= 60: row['Pipeline Action'] = 'FLAG'
        elif isinstance(score, int): row['Pipeline Action'] = 'SKIP'
        row['Score Breakdown'] = ' | '.join(f'{k}: {v}' for k, v in bd.items())
        row['Reasoning'] = sc.get('reasoning', '')

all_rows.sort(key=lambda x: (-(int(x['Company ICP Score']) if str(x['Company ICP Score']).isdigit() else 0)))

with open(csvfile, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_rows)

proceed = sum(1 for r in all_rows if r['Pipeline Action'] == 'PROCEED')
flag = sum(1 for r in all_rows if r['Pipeline Action'] == 'FLAG')
skip = sum(1 for r in all_rows if r['Pipeline Action'] == 'SKIP')
exclude = sum(1 for r in all_rows if r['Pipeline Action'] == 'HARD EXCLUDE')

print(f"\nFINAL: {len(all_rows)} companies")
print(f"  PROCEED: {proceed}")
print(f"  FLAG:    {flag}")
print(f"  SKIP:    {skip}")
print(f"  EXCLUDE: {exclude}")

by_cat = {}
for r in all_rows:
    cat = r.get('Category', '?')
    if cat not in by_cat: by_cat[cat] = {'total': 0, 'proceed': 0, 'flag': 0}
    by_cat[cat]['total'] += 1
    if r['Pipeline Action'] == 'PROCEED': by_cat[cat]['proceed'] += 1
    if r['Pipeline Action'] == 'FLAG': by_cat[cat]['flag'] += 1

print("\nBy industry:")
for cat, c in sorted(by_cat.items()):
    print(f"  {cat:<25} {c['total']:>3} total  {c['proceed']:>3} PROCEED  {c['flag']:>3} FLAG")

print("\nTop 15 newly scored:")
new_scored = sorted([r for r in all_rows if r['Company'] in score_map],
    key=lambda x: -(int(x['Company ICP Score']) if str(x['Company ICP Score']).isdigit() else 0))
for r in new_scored[:15]:
    print(f"  {str(r['Company ICP Score']):>3} {r['Pipeline Action']:>12}  {r['Category']:<22} {r['Company']:<40} {str(r['Employees (LinkedIn)']):>5} LI emp  {r['Location']}")
