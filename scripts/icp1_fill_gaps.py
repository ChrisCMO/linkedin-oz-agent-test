#!/usr/bin/env python3
"""Fill remaining gaps: profile data, messages, activity."""
import sys, os, csv, json, time, random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

OPENAI_KEY = os.environ['OPENAI_API_KEY']
APIFY_TOKEN = os.environ['APIFY_API_KEY']
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

BASE = os.path.join(os.path.dirname(__file__), "..")
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'

def run_actor(actor_id, payload):
    r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                      headers=apify_h, json=payload, timeout=30)
    if r.status_code != 201: return []
    run_id = r.json()['data']['id']
    ds = r.json()['data']['defaultDatasetId']
    for _ in range(30):
        time.sleep(5)
        s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                         headers=apify_h, timeout=15).json()['data']['status']
        if s in ('SUCCEEDED', 'FAILED', 'ABORTED'): break
    try:
        return requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                            headers=apify_h, timeout=15).json()
    except: return []

# Load contacts
f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'proceed_contacts_enriched_v2.csv')
with open(f) as fh:
    reader = csv.DictReader(fh)
    headers = reader.fieldnames
    rows = list(reader)

print(f"Total contacts: {len(rows)}")

# ── GAP 1: Profile data for contacts missing connections ──
missing_profile = [r for r in rows if (r.get('LinkedIn URL') or '').strip() and not str(r.get('LinkedIn Connections') or '').strip()]
print(f"\nGap 1: Missing profile data: {len(missing_profile)}")

if missing_profile:
    urls = [r['LinkedIn URL'] for r in missing_profile]
    print(f"  Scraping {len(urls)} profiles...")

    all_profiles = []
    for i in range(0, len(urls), 20):
        batch = urls[i:i+20]
        print(f"  Batch {i//20+1}: {len(batch)} profiles")
        items = run_actor(PROFILE_SCRAPER, {'urls': batch})
        all_profiles.extend(items)
        time.sleep(2)

    print(f"  Got {len(all_profiles)} profiles")

    # Match by first+last name
    prof_by_name = {}
    for prof in all_profiles:
        fn = (prof.get('firstName') or '').strip().lower()
        ln = (prof.get('lastName') or '').strip().lower()
        if fn and ln:
            prof_by_name[f"{fn} {ln}"] = prof

    fixed_profile = 0
    for r in missing_profile:
        fn = (r.get('First Name') or '').strip().lower()
        ln = (r.get('Last Name') or '').strip().lower()
        prof = prof_by_name.get(f"{fn} {ln}")

        if prof:
            r['LinkedIn Headline'] = prof.get('headline', '') or r.get('LinkedIn Headline', '')
            r['LinkedIn Connections'] = str(prof.get('connectionsCount', ''))
            r['LinkedIn Followers'] = str(prof.get('followerCount', ''))
            r['Open to Work'] = 'Yes' if prof.get('openToWork') else 'No'

            current = prof.get('currentPosition', [])
            current_co = current[0].get('companyName', '') if current else ''
            company_name = r.get('Company', '')
            if current_co and company_name.lower()[:6] in current_co.lower():
                r['Role Verified'] = 'Yes'
            elif current_co:
                r['Role Verified'] = f"MISMATCH - LI: {(prof.get('headline',''))[:40]}"
            fixed_profile += 1

    print(f"  Fixed: {fixed_profile}/{len(missing_profile)}")

# ── GAP 2: Posts/Activity ──
# Activity checking is now done via scripts/icp1_fix_posts_only.py using the
# Activity Index actor (kog75ERz9lcVNujbQ) which provides a complete 1-10
# activity score including posts, reposts, reactions, and comments.
# Run that script separately after this one completes.
print("\nGap 2: Skipped — activity checking now uses icp1_fix_posts_only.py with Activity Index actor")

# ── GAP 3: AI messages for contacts missing them ──
missing_msgs = [r for r in rows if not (r.get("Melinda's Connection Note") or '').strip()]
print(f"\nGap 3: Missing messages: {len(missing_msgs)}")

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

for i, r in enumerate(missing_msgs):
    first = (r.get('First Name') or '').strip()
    if not first:
        continue

    ctx = json.dumps({
        'prospect': {
            'first_name': first,
            'title': r.get('Title', ''),
            'company_name': r.get('Company', ''),
            'location': r.get('Company Location', ''),
            'industry': r.get('Industry', ''),
            'headline': r.get('LinkedIn Headline', r.get('Title', '')),
        },
        'company': {
            'industry': r.get('Industry', ''),
        },
    }, indent=2)

    for sender in ['Melinda Johnson', 'Adrienne Nordland']:
        key = sender.split()[0].lower()
        try:
            resp = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-5.4', 'messages': [
                    {'role': 'system', 'content': NOTE_PROMPT.format(sender_name=sender)},
                    {'role': 'user', 'content': ctx},
                ], 'temperature': 0.8, 'max_completion_tokens': 100}, timeout=60)
            note = resp.json()['choices'][0]['message']['content'].strip().strip('"')
            if len(note) > 200: note = note[:197] + '...'
            r[f"{'Melinda' if key == 'melinda' else 'Adrienne'}'s Connection Note"] = note

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
                col = f"Message {m['step']} - {'Melinda' if key == 'melinda' else 'Adrienne'}"
                r[col] = m['text']
        except Exception as e:
            pass

        time.sleep(0.5)

    if (i + 1) % 20 == 0:
        print(f"  Messages: {i+1}/{len(missing_msgs)}")
        # Save progress
        with open(f, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Progress saved.")

# Final save
with open(f, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Stats
has_li = sum(1 for r in rows if (r.get('LinkedIn URL') or '').strip())
has_email = sum(1 for r in rows if (r.get('Email') or '').strip())
has_conn = sum(1 for r in rows if str(r.get('LinkedIn Connections') or '').strip())
has_activity = sum(1 for r in rows if (r.get('Activity Level') or '').strip() and r.get('Activity Level') != 'No posts found')
has_note = sum(1 for r in rows if (r.get("Melinda's Connection Note") or '').strip())

print(f"\n{'='*60}")
print(f"FINAL RESULTS: {len(rows)} contacts")
print(f"  LinkedIn URL:     {has_li}")
print(f"  Email:            {has_email}")
print(f"  Connections:      {has_conn}")
print(f"  Activity data:    {has_activity}")
print(f"  Messages:         {has_note}")
print(f"\nSaved: {f}")
