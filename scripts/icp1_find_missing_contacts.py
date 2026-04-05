#!/usr/bin/env python3
"""Find contacts for 15 PROCEED companies where Apollo returned 0 results.
Uses Google X-ray search, ZoomInfo, and Apify profile verification."""
import sys, os, csv, json, time, random, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import requests

APOLLO_KEY = os.environ['APOLLO_API_KEY']
APIFY_TOKEN = os.environ['APIFY_API_KEY']
OPENAI_KEY = os.environ['OPENAI_API_KEY']

apollo_h = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache', 'X-Api-Key': APOLLO_KEY}
apify_h = {'Authorization': f'Bearer {APIFY_TOKEN}', 'Content-Type': 'application/json'}

BASE = os.path.join(os.path.dirname(__file__), "..")
GOOGLE_SERP = 'nFJndFXA5zjCTuudP'     # Google SERP scraper
PROFILE_SCRAPER = 'LpVuK3Zozwuipa5bp'  # LinkedIn profile scraper
ACTIVITY_INDEX = 'kog75ERz9lcVNujbQ'    # Activity Index (LinkedScore)

TARGET_TITLES = ['CFO', 'Chief Financial Officer', 'Controller',
                 'VP Finance', 'Director of Finance', 'Owner', 'President', 'CEO']

MAX_RETRIES = 3
RETRY_DELAY = 10

def run_actor(actor_id, payload, label=""):
    """Run Apify actor with retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                print(f"      Retry {attempt-1} for {label}...")
                time.sleep(RETRY_DELAY)
            r = requests.post(f'https://api.apify.com/v2/acts/{actor_id}/runs',
                              headers=apify_h, json=payload, timeout=30)
            if r.status_code == 429:
                print(f"      [RATE LIMIT] Waiting 30s...")
                time.sleep(30)
                continue
            if r.status_code == 402:
                print(f"      [OUT OF CREDITS] Stopping.")
                return None
            if r.status_code != 201:
                print(f"      Actor failed: {r.status_code} - {r.text[:80]}")
                continue
            run_id = r.json()['data']['id']
            ds = r.json()['data']['defaultDatasetId']
            for _ in range(30):
                time.sleep(5)
                s = requests.get(f'https://api.apify.com/v2/actor-runs/{run_id}',
                                 headers=apify_h, timeout=30).json()['data']['status']
                if s in ('SUCCEEDED', 'FAILED', 'ABORTED'): break
            if s != 'SUCCEEDED':
                continue
            items = requests.get(f'https://api.apify.com/v2/datasets/{ds}/items',
                                 headers=apify_h, timeout=15).json()
            if items:
                return items
        except Exception as e:
            print(f"      Error: {e}")
    return []


def normalize_linkedin_url(url):
    url = url.strip()
    if not url: return url
    url = url.replace('http://', 'https://')
    if 'https://linkedin.com' in url:
        url = url.replace('https://linkedin.com', 'https://www.linkedin.com')
    return url


# ─── STEP 1: Google X-ray search ───
def xray_search(company_name):
    """Search Google for LinkedIn profiles at this company."""
    contacts = []
    queries = [
        f'site:linkedin.com/in "{company_name}" CFO OR "chief financial" OR controller',
        f'site:linkedin.com/in "{company_name}" president OR owner OR CEO',
    ]
    for q in queries:
        items = run_actor(GOOGLE_SERP, {
            'queries': q,
            'maxPagesPerQuery': 1,
            'resultsPerPage': 5,
            'countryCode': 'us',
        }, label=f"X-ray: {q[:50]}")
        if items is None:
            return None  # out of credits
        if not items:
            continue
        for item in items:
            # Handle nested results
            results = item.get('organicResults', [item]) if isinstance(item, dict) else [item]
            for result in results:
                url = result.get('url', result.get('link', ''))
                title = result.get('title', '')
                desc = result.get('description', result.get('snippet', ''))
                if 'linkedin.com/in/' in url:
                    contacts.append({
                        'linkedin_url': normalize_linkedin_url(url.split('?')[0]),
                        'title_hint': title,
                        'desc_hint': desc,
                        'source': 'Google X-ray',
                    })
        time.sleep(2)
    # Dedupe by URL
    seen = set()
    deduped = []
    for c in contacts:
        if c['linkedin_url'] not in seen:
            seen.add(c['linkedin_url'])
            deduped.append(c)
    return deduped


# ─── STEP 2: ZoomInfo contact search (free) ───
def zoominfo_search(company_name):
    """Search ZoomInfo for finance contacts at this company."""
    try:
        # Auth
        auth = requests.post('https://api.zoominfo.com/authenticate',
                             json={'username': os.environ.get('ZOOMINFO_USERNAME', ''),
                                   'password': os.environ.get('ZOOMINFO_PASSWORD', '')},
                             timeout=15)
        if auth.status_code != 200:
            return []
        token = auth.json().get('jwt', '')
        if not token:
            return []
        zi_h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

        resp = requests.post('https://api.zoominfo.com/search/contact', headers=zi_h, json={
            'companyName': company_name,
            'jobTitle': 'CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance OR President OR Owner OR CEO',
            'rpp': 5,
            'page': 1,
        }, timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json().get('data', [])
        contacts = []
        for p in data:
            contacts.append({
                'first_name': p.get('firstName', ''),
                'last_name': p.get('lastName', ''),
                'title': p.get('jobTitle', ''),
                'source': 'ZoomInfo',
            })
        return contacts
    except Exception as e:
        print(f"      ZoomInfo error: {e}")
        return []


# ─── STEP 3: Apollo search with broader terms ───
def apollo_broad_search(company_name, domain=''):
    """Try Apollo with broader search terms."""
    contacts = []
    search_body = {
        'person_titles': TARGET_TITLES + ['Finance Manager', 'Accounting Manager', 'General Manager'],
        'person_seniorities': ['c_suite', 'vp', 'director', 'owner', 'manager'],
        'per_page': 5,
    }
    if domain and domain not in ('', 'N/A'):
        search_body['q_organization_domains_list'] = [domain]
    else:
        search_body['q_keywords'] = company_name

    try:
        r = requests.post('https://api.apollo.io/api/v1/mixed_people/api_search',
                          headers=apollo_h, json=search_body, timeout=30)
        people = r.json().get('people', [])
        for p in people:
            contacts.append({
                'first_name': p.get('first_name', ''),
                'last_name': p.get('last_name', ''),
                'title': p.get('title', ''),
                'linkedin_url': normalize_linkedin_url(p.get('linkedin_url', '')),
                'email': p.get('email', ''),
                'apollo_id': p.get('id', ''),
                'source': 'Apollo (broad)',
            })
    except Exception as e:
        print(f"      Apollo error: {e}")
    return contacts


# ─── STEP 4: Verify contacts via Apify profile scraper ───
def verify_profile(linkedin_url, company_name):
    """Verify a LinkedIn profile is at the correct company."""
    items = run_actor(PROFILE_SCRAPER, {'urls': [linkedin_url]}, label=f"Profile: {linkedin_url}")
    if not items:
        return None
    prof = items[0]
    current = prof.get('currentPosition', [])
    current_co = current[0].get('companyName', '') if current else ''

    # Check if company matches
    co_short = company_name.lower()[:8]
    if current_co and co_short in current_co.lower():
        verified = 'Yes'
    elif current_co:
        verified = f'Check - LI: {prof.get("headline", "")[:40]}'
    else:
        verified = 'Profile found - no current position'

    return {
        'first_name': prof.get('firstName', ''),
        'last_name': prof.get('lastName', ''),
        'headline': prof.get('headline', ''),
        'connections': str(prof.get('connectionsCount', '')),
        'followers': str(prof.get('followerCount', '')),
        'open_to_work': 'Yes' if prof.get('openToWork') else 'No',
        'role_verified': verified,
        'current_company': current_co,
    }


# ─── STEP 5: Get activity score ───
def get_activity_score(linkedin_url, name=''):
    """Get LinkedIn activity score via Activity Index actor."""
    items = run_actor(ACTIVITY_INDEX, {'linkedinUrl': linkedin_url}, label=f"Activity: {name}")
    if items and items[0].get('success'):
        data = items[0]
        score = data.get('activity_score', 0)
        metrics = data.get('activity_metrics', {})
        if score >= 7: level = 'Very Active'
        elif score >= 5: level = 'Active'
        elif score >= 3: level = 'Moderate'
        elif score >= 1: level = 'Low'
        else: level = 'Inactive'
        return {
            'activity_score': str(score),
            'activity_level': level,
            'activity_recommendation': data.get('recommendation', ''),
            'activity_insights': ' | '.join(data.get('insights', [])),
            'posts_30d': str(metrics.get('posts_last_30_days', 0)),
            'reactions_30d': str(metrics.get('reactions_last_30_days', 0)),
            'last_activity_date': metrics.get('last_activity_date', ''),
            'days_since': str(metrics.get('days_since_last_activity', '')),
            'linkedin_active_status': f'{score}/10 {level}',
        }
    return None


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

# Load the 15 no-contacts companies
infile = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'proceed_no_contacts.csv')
with open(infile) as f:
    companies = list(csv.DictReader(f))

print(f"Companies with 0 contacts: {len(companies)}")
print("=" * 60)

all_found_contacts = []

for i, co in enumerate(companies):
    company_name = co.get('Company', '')
    domain = co.get('Domain', '')
    icp_score = co.get('Company ICP Score', '')
    category = co.get('Category', '')
    industry = co.get('Industry', '')

    print(f"\n[{i+1}/{len(companies)}] {company_name} (ICP: {icp_score}, Domain: {domain})")
    print("-" * 50)

    # Collect candidates from all sources
    candidates = []

    # Source 1: Google X-ray
    print("  1. Google X-ray search...")
    xray_results = xray_search(company_name)
    if xray_results is None:
        print("  [FATAL] Out of Apify credits. Stopping.")
        break
    print(f"     Found: {len(xray_results)} LinkedIn profiles")
    candidates.extend(xray_results)

    # Source 2: ZoomInfo
    print("  2. ZoomInfo search...")
    zi_results = zoominfo_search(company_name)
    print(f"     Found: {len(zi_results)} contacts")
    # ZoomInfo doesn't give LinkedIn URLs, so we'll X-ray search for them later
    for zi in zi_results:
        # Check if we already have this person from X-ray
        already_found = any(c.get('first_name', '').lower() == zi['first_name'].lower()
                           and c.get('last_name', '').lower() == zi['last_name'].lower()
                           for c in candidates if c.get('first_name'))
        if not already_found:
            candidates.append(zi)

    # Source 3: Apollo broad search
    print("  3. Apollo broad search...")
    apollo_results = apollo_broad_search(company_name, domain)
    print(f"     Found: {len(apollo_results)} contacts")
    for ap in apollo_results:
        already_found = any(c.get('linkedin_url', '') == ap['linkedin_url']
                           for c in candidates if c.get('linkedin_url') and ap.get('linkedin_url'))
        if not already_found:
            candidates.append(ap)

    print(f"  Total unique candidates: {len(candidates)}")

    if not candidates:
        print(f"  ⚠️ No contacts found from any source for {company_name}")
        continue

    # For ZoomInfo contacts without LinkedIn URLs, try X-ray search by name
    for c in candidates:
        if not c.get('linkedin_url') and c.get('first_name') and c.get('last_name'):
            print(f"    Finding LinkedIn for {c['first_name']} {c['last_name']}...")
            name_results = run_actor(GOOGLE_SERP, {
                'queries': f'site:linkedin.com/in "{c["first_name"]} {c["last_name"]}" "{company_name.split()[0]}"',
                'maxPagesPerQuery': 1,
                'resultsPerPage': 5,
                'countryCode': 'us',
            }, label=f"Name search: {c['first_name']} {c['last_name']}")
            if name_results:
                for item in name_results:
                    results = item.get('organicResults', [item]) if isinstance(item, dict) else [item]
                    for result in results:
                        url = result.get('url', result.get('link', ''))
                        if 'linkedin.com/in/' in url:
                            c['linkedin_url'] = normalize_linkedin_url(url.split('?')[0])
                            break
                    if c.get('linkedin_url'):
                        break
            time.sleep(1)

    # Verify each candidate with LinkedIn URL — cap at 5 per company
    MAX_PER_COMPANY = 5
    verified_contacts = []
    for c in candidates:
        if len(verified_contacts) >= MAX_PER_COMPANY:
            print(f"    Reached {MAX_PER_COMPANY} verified contacts, moving on")
            break

        li_url = c.get('linkedin_url', '')
        if not li_url:
            print(f"    Skipping {c.get('first_name','')} {c.get('last_name','')} - no LinkedIn URL")
            continue

        print(f"    Verifying: {li_url[:60]}...")
        prof = verify_profile(li_url, company_name)
        if not prof:
            print(f"      Could not verify profile")
            continue

        name = f"{prof['first_name']} {prof['last_name']}"
        print(f"      {name} | {prof['headline'][:50]} | Verified: {prof['role_verified']}")

        # Get activity score
        activity = get_activity_score(li_url, name)

        contact = {
            'Company ICP Score': icp_score,
            'Pipeline Action': 'PROCEED',
            'Category': category,
            'Company': company_name,
            'Industry': industry,
            'Company Location': co.get('Location', ''),
            'Company LinkedIn URL': co.get('Company LinkedIn URL', ''),
            'Company LI Followers': co.get('LI Followers', ''),
            'First Name': prof['first_name'],
            'Last Name': prof['last_name'],
            'Title': prof['headline'],
            'Seniority': '',
            'LinkedIn URL': li_url,
            'LinkedIn Headline': prof['headline'],
            'Role Verified': prof['role_verified'],
            'LinkedIn Connections': prof['connections'],
            'LinkedIn Followers': prof['followers'],
            'Open to Work': prof['open_to_work'],
            'Email': c.get('email', ''),
            'Email Status': '',
            'Apollo Person ID': c.get('apollo_id', ''),
            'Apollo Company ID': '',
            'Data Source': f'Missing contacts pipeline ({c.get("source", "unknown")})',
        }

        # Add activity data
        if activity:
            contact.update({
                'Activity Score': activity['activity_score'],
                'Activity Level': activity['activity_level'],
                'Activity Recommendation': activity['activity_recommendation'],
                'Activity Insights': activity['activity_insights'],
                'Posts Last 30 Days': activity['posts_30d'],
                'Reactions Last 30 Days': activity['reactions_30d'],
                'Last Activity Date': activity['last_activity_date'],
                'Days Since Last Activity': activity['days_since'],
                'LinkedIn Active Status': activity['linkedin_active_status'],
            })
        else:
            contact.update({
                'Activity Score': '0', 'Activity Level': 'Error',
                'Activity Recommendation': '', 'Activity Insights': '',
                'Posts Last 30 Days': '0', 'Reactions Last 30 Days': '0',
                'Last Activity Date': '', 'Days Since Last Activity': '',
                'LinkedIn Active Status': 'Activity check failed',
            })

        verified_contacts.append(contact)
        time.sleep(1)

    print(f"  ✅ Verified contacts for {company_name}: {len(verified_contacts)}")
    all_found_contacts.extend(verified_contacts)

    # Save progress after each company
    if all_found_contacts:
        progress_file = os.path.join(BASE, 'docs', 'deliverables', 'week2', 'scored', 'new', 'missing_contacts_found.csv')
        output_headers = [
            'Company ICP Score', 'Pipeline Action', 'Category', 'Company', 'Industry',
            'Company Location', 'Company LinkedIn URL', 'Company LI Followers',
            'First Name', 'Last Name', 'Title', 'Seniority',
            'LinkedIn URL', 'LinkedIn Headline', 'Role Verified',
            'LinkedIn Connections', 'LinkedIn Followers', 'Open to Work',
            'Email', 'Email Status', 'Apollo Person ID', 'Apollo Company ID',
            'Activity Score', 'Activity Level', 'Activity Recommendation', 'Activity Insights',
            'Posts Last 30 Days', 'Reactions Last 30 Days', 'Last Activity Date',
            'Days Since Last Activity', 'LinkedIn Active Status',
            'Data Source',
        ]
        with open(progress_file, 'w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=output_headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_found_contacts)
        print(f"  Progress saved: {len(all_found_contacts)} total contacts")

# Final stats
print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"Companies searched: {len(companies)}")
print(f"Total contacts found: {len(all_found_contacts)}")

companies_with_contacts = len(set(c['Company'] for c in all_found_contacts))
print(f"Companies with contacts: {companies_with_contacts}/{len(companies)}")
still_missing = len(companies) - companies_with_contacts
print(f"Still missing: {still_missing}")

if all_found_contacts:
    from collections import Counter
    sources = Counter(c.get('Data Source', '') for c in all_found_contacts)
    print(f"\nBy source:")
    for s, count in sources.most_common():
        print(f"  {s}: {count}")

    verified_yes = sum(1 for c in all_found_contacts if c.get('Role Verified') == 'Yes')
    print(f"\nRole verified: {verified_yes}/{len(all_found_contacts)}")

    act = Counter(c.get('Activity Level', '') for c in all_found_contacts)
    print(f"\nActivity:")
    for level in ['Very Active', 'Active', 'Moderate', 'Low', 'Inactive', 'Error']:
        if act.get(level, 0) > 0:
            print(f"  {level}: {act[level]}")

    print(f"\nSaved: {progress_file}")
    print(f"\nTo merge into main list, append these to proceed_contacts_enriched_v2.csv")
