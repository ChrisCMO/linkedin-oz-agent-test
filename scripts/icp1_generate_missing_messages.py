#!/usr/bin/env python3
"""Generate connection notes + 3-message sequences for contacts missing them."""
import sys, os, csv, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

OPENAI_KEY = os.environ['OPENAI_API_KEY']
BASE = os.path.join(os.path.dirname(__file__), "..")

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

# Load contacts
f = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'proceed_contacts_enriched_v2.csv')
with open(f) as fh:
    reader = csv.DictReader(fh)
    headers = list(reader.fieldnames)
    rows = list(reader)

missing = [r for r in rows if not (r.get("Melinda's Connection Note") or '').strip()]
print(f"Total contacts: {len(rows)}")
print(f"Missing messages: {len(missing)}")

generated = 0
errors = []

for i, r in enumerate(missing):
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

    print(f"  {i+1}/{len(missing)}: {first} {r.get('Last Name','')} ({r.get('Company','')})...", end='', flush=True)

    for sender in ['Melinda Johnson', 'Adrienne Nordland']:
        key = sender.split()[0]
        try:
            # Connection note
            resp = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-4o-mini', 'messages': [
                    {'role': 'system', 'content': NOTE_PROMPT.format(sender_name=sender)},
                    {'role': 'user', 'content': ctx},
                ], 'temperature': 0.8, 'max_tokens': 100}, timeout=60)
            note = resp.json()['choices'][0]['message']['content'].strip().strip('"')
            if len(note) > 200: note = note[:197] + '...'
            r[f"{key}'s Connection Note"] = note

            # Messages
            resp2 = requests.post('https://api.openai.com/v1/chat/completions',
                headers={'Authorization': f'Bearer {OPENAI_KEY}', 'Content-Type': 'application/json'},
                json={'model': 'gpt-4o-mini', 'messages': [
                    {'role': 'system', 'content': MSG_PROMPT.format(sender_name=sender)},
                    {'role': 'user', 'content': ctx},
                ], 'temperature': 0.8, 'max_tokens': 1000}, timeout=60)
            raw = resp2.json()['choices'][0]['message']['content'].strip()
            if raw.startswith('```'): raw = raw.split('\n', 1)[1].rsplit('```', 1)[0].strip()
            msgs = json.loads(raw)
            for m in msgs:
                col = f"Message {m['step']} - {key}"
                r[col] = m['text']
        except Exception as e:
            errors.append(f"{first} {r.get('Last Name','')} ({sender}): {e}")

        time.sleep(0.3)

    generated += 1
    print(f" done")

    # Save every 20
    if (i + 1) % 20 == 0:
        with open(f, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  ** Progress saved ({i+1}/{len(missing)}) **")

# Final save
with open(f, 'w', newline='', encoding='utf-8') as fh:
    writer = csv.DictWriter(fh, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)

# Stats
has_msg = sum(1 for r in rows if (r.get("Melinda's Connection Note") or '').strip())
print(f"\n{'='*60}")
print(f"Generated messages for: {generated} contacts")
print(f"Total with messages: {has_msg}/{len(rows)}")
if errors:
    print(f"\nErrors ({len(errors)}):")
    for e in errors[:10]:
        print(f"  {e}")
print(f"\nSaved: {f}")
