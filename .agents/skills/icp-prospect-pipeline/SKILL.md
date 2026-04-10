---
name: icp-prospect-pipeline
description: Run the company-first ICP prospect sourcing pipeline for VWC CPAs. Use this skill whenever someone asks to "run ICP pipeline", "find prospects", "build a prospect list", "company-first search", "ICP scoring", "source prospects for VWC", "find companies matching the ICP", "enrich prospects", "score prospects", or any request related to finding, scoring, or enriching B2B audit/tax prospects in the Pacific Northwest. Also trigger when someone mentions Google Places + Apollo + ZoomInfo pipeline, or asks about the prospect sourcing workflow, or wants to generate connection notes/messages for LinkedIn outreach prospects.
---

# ICP Prospect Pipeline — VWC CPAs (Company-First)

This skill orchestrates a company-first prospect sourcing pipeline for VWC CPAs' LinkedIn outreach. It discovers real businesses via Google Places, cross-matches to ZoomInfo/Apollo for finance contacts, enriches, AI-scores against the ICP, checks LinkedIn activity, and generates personalized outreach messages.

## Why Company-First

The previous approach searched databases for *people* by title, which returned noisy results (wrong geography, defunct companies, stale data). Starting with Google Places gives us **verified, currently-operating businesses** with real addresses and websites — then we find the right person at each company. This dramatically improves list quality.

## Integration with Company Scoring Pipeline

**IMPORTANT:** The company scoring pipeline (`/icp-company-pipeline-revamped`) runs FIRST and creates stub contacts with `status: sourced` in the `prospects` table. These stubs have:
- `apollo_person_id` — from Apollo free search
- `zoominfo_contact_id` — from ZoomInfo company ID search
- `linkedin_url` / `linkedin_slug` — from Apollo or Serper cross-reference (unverified)
- `company_universe_id` — links to the scored company

**This pipeline should check for existing sourced contacts BEFORE re-discovering:**
```python
existing = sb.from("prospects").select("*").eq("company_universe_id", company_id).eq("status", "sourced")
if existing:
    # Use existing contacts — skip to validation/enrichment
    # Use stored apollo_person_id and zoominfo_contact_id for direct lookup
else:
    # Run full discovery (fallback)
```

This saves discovery credits — the company scorer already found the contacts, this pipeline just validates and enriches them.

## The 10-Step Pipeline

```
Step 0:  CHECK EXISTING CONTACTS   → Look for 'sourced' stubs from company scoring pipeline (free)
Step 1:  COMPANY DISCOVERY         → Google Places API (free) — SKIP if company already in companies_universe
Step 2:  COMPANY ENRICHMENT        → Apollo org enrich by domain ($1/company) — SKIP if already enriched
Step 3:  CONTACT DISCOVERY         → Apollo people search by domain (free) + ZoomInfo by company ID (free)
Step 3b: ZOOMINFO FINANCE CROSSREF → ZoomInfo search by companyId (preferred) or name+zip (fallback)
Step 3c: LINKEDIN X-RAY DISCOVERY  → Serper.dev X-ray search (default, 7x faster) or Apify SERP (fallback)
Step 4:  PERSON ENRICHMENT         → Apollo person enrich ($1/person) — use stored apollo_person_id when available
Step 4b: STALE DATA VALIDATION     → Apify profile scrape to verify contacts still at company
Step 5:  AI SCORING                → 0-100 score with company-location-aware reasoning (GPT-5.4)
Step 6:  LINKEDIN VALIDATION       → Profile scraper (role verification) + Activity Index (activity)
Step 7:  CONNECTION NOTES          → Adrienne + Melinda versions (≤200 chars)
Step 8:  MESSAGE SEQUENCES         → 3-message follow-up per prospect
```

---

## Step 1: Company Discovery (Google Places)

**Goal:** Find real, operating businesses by industry + city in the PNW.

**API:** `POST https://places.googleapis.com/v1/places:searchText`

**Headers:**
```
X-Goog-Api-Key: AIzaSyDBFl9GysZkM42uPS1wdKh8tTKeedWP67o
X-Goog-FieldMask: places.displayName,places.formattedAddress,places.id,places.rating,places.userRatingCount,places.businessStatus,places.location,places.nationalPhoneNumber,places.websiteUri,places.types,places.primaryType,places.primaryTypeDisplayName,nextPageToken
```

**Body:** `{"textQuery": "construction companies in Seattle Washington"}`

**ICP 1 Industries (priority order):**
1. Manufacturing — `"manufacturing companies in {city} {state}"`
2. Commercial Real Estate — `"commercial real estate companies in {city} {state}"`
3. Professional Services — `"professional services firms in {city} {state}"`, `"engineering firms in {city} {state}"`
4. Hospitality — `"hospitality companies in {city} {state}"`, `"hotel management companies in {city} {state}"`
5. Nonprofit — `"nonprofit organizations in {city} {state}"`
6. Construction — `"construction companies in {city} {state}"`, `"general contractors in {city} {state}"`

**ICP 1 Geography (priority order):**
- Primary: Seattle metro (Seattle, Bellevue, Tacoma, Redmond, Kirkland, Everett, Renton, Kent, Federal Way, Olympia)
- Secondary: Greater WA (Spokane, Vancouver, Yakima, Bellingham, Tri-Cities)
- Tertiary: Oregon (Portland, Salem, Eugene, Bend, Medford, Beaverton, Hillsboro, Corvallis)

**Pagination:** Use `nextPageToken` for additional pages. Up to 3 pages per query (60 results max).

**Rate limiting:** 1-3 second delay between queries. Add retry logic for connection resets.

**Output:** Extract `company_name`, `address`, `city`, `state`, `phone`, `website`, `domain`, `rating`, `review_count`, `google_place_id`, `industry_search`.

**Known issue:** Some companies have generic domains (facebook.com, yelp.com). Filter these out — they create noise in later enrichment steps.

**Script:** `scripts/icp1_company_first.py` — runs all industries × all cities, deduplicates, saves progress after each city.

---

## Step 2: ZoomInfo Cross-Match (Free Search)

**Goal:** For each company found in Google Places, check if ZoomInfo has a CFO/Controller contact.

**Auth:** `POST https://api.zoominfo.com/authenticate` with `ZOOMINFO_USERNAME` and `ZOOMINFO_PASSWORD` from `.env`. Returns JWT token.

**Company search:** `POST https://api.zoominfo.com/search/company` — returns only `id` and `name` (no revenue/employees in free search). Useful for confirming the company exists in ZoomInfo.

**Contact search:** `POST https://api.zoominfo.com/search/contact`

**CRITICAL: Always include `zipCode` + `zipCodeRadiusMiles` to validate the correct company location.** Without zip code filtering, ZoomInfo matches by company name only and may return contacts at a similarly-named company in a different state.

```json
{
  "companyName": "Sellen Construction",
  "jobTitle": "CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance",
  "zipCode": "98101",
  "zipCodeRadiusMiles": "50",
  "rpp": 5
}
```
Returns: `firstName`, `lastName`, `jobTitle`, `contactAccuracyScore`, `id` (ZoomInfo Contact ID), `hasEmail`, `hasDirectPhone`. Full names (not obfuscated).

**Validated results:** In testing, ZoomInfo with zip code found finance contacts at 9/10 companies where Apollo returned only executives. Hit rate: ~50% of companies yielded verified finance contacts after full LinkedIn profile verification.

**Free tier limitations:**
- Contact search with `companyName` + `zipCode` works (this is the approach to use)
- `companyId`-based contact search returns 0 results — do NOT use
- `website`/domain-based company search returns 0 results — do NOT use
- Company search returns IDs but no details (name, location, website all blank)
- No enrichment credits consumed — only search endpoints used

**Store ZoomInfo IDs:** Save `ZoomInfo Contact ID` from each contact result for traceability.

**Rate limiting:** 0.5-0.8s delay between calls.

**City → Zip code mapping (PNW):**
```python
CITY_ZIP = {
    'seattle': '98101', 'bellevue': '98004', 'tacoma': '98402',
    'redmond': '98052', 'kirkland': '98033', 'everett': '98201',
    'renton': '98057', 'kent': '98032', 'auburn': '98002',
    'olympia': '98501', 'lynnwood': '98036', 'lakewood': '98499',
    'federal way': '98003', 'vancouver': '98660', 'ferndale': '98248',
}
```

**Scripts:**
- `scripts/icp1_mfg_zoominfo_crossmatch.py` — original company cross-match
- `scripts/icp1_zoominfo_finance_crossref.py` — finance contact discovery with zip code validation + Apollo cross-match + LinkedIn verification

---

## Step 3: Apollo Cross-Match (Free Search)

**Goal:** For each company, find finance contacts in Apollo and get Apollo IDs (needed for enrichment).

**Two search methods:**

**Method A — By domain (preferred, more precise):**
```python
apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
    "q_organization_domains_list": ["sellen.com"],
    "person_titles": ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance", "Owner", "President", "CEO", "Partner", "Principal", "Member"],
    "person_seniorities": ["c_suite", "vp", "director", "owner"],
    "per_page": 3,
})
```

**Method B — By company name keyword (fallback when no domain):**
```python
apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
    "q_keywords": "John Smith Sellen Construction",
    "person_titles": ["CFO", "Chief Financial Officer", "Controller"],
    "per_page": 3,
})
```

**Known issue:** Apollo `person_locations: ["Washington, United States"]` includes Washington DC. Do NOT use Apollo for geographic filtering — use Google Places (Step 1) and ZoomInfo (Step 2) for geography. Apollo is for contact discovery and enrichment only.

**ZoomInfo → Apollo cross-match:** For ZoomInfo-only contacts (have name but no Apollo ID), search Apollo by `q_keywords: "{firstName} {lastName} {companyName}"` to find their Apollo ID. Historical match rate: ~16-43% depending on the batch.

**Script:** `scripts/icp1_mfg_apollo_crossmatch.py`

---

## Step 3b: ZoomInfo Finance Cross-Reference (for companies with no finance contacts)

**Goal:** Find CFOs, Controllers, and Directors of Finance at companies where no finance contacts were found. Uses ZoomInfo's zip code filtering for location-validated results.

**When to run:** After Step 3, for **any company with no CFO/Controller/Director of Finance** — regardless of how many other contacts Apollo or ZoomInfo returned. A company with 5 VPs of Engineering but no CFO still needs this step.

**Title tiers (from Chad's ICP spec — centralized in `lib/title_tiers.py`):**

Tier 1 — Primary Finance (always target first):
- CFO, Chief Financial Officer, Controller, Financial Controller
- VP Finance, VP of Finance, Vice President of Finance
- Director of Finance, Finance Director

Tier 2 — Executive (secondary option):
- Owner, President, CEO, Founder, Managing Director, Partner, Executive Director

Tier 3 — Junior Finance (last resort per Chad — "too junior to initiate an audit relationship"):
- Accounting Manager, Finance Manager, Treasurer, Bookkeeper, Staff Accountant

**Search strategy:** Apollo and ZoomInfo search ALL tiers in a single call. X-ray runs Tier 1 first, adds Tier 2 if < 2 contacts found, adds Tier 3 if still 0. Each contact is tagged with `tier` and `tier_label` for review.

**Validated approach (tested March 2026):** ZoomInfo with zip code found verified finance contacts at 13 out of 54 companies tested (24% company hit rate, 17 contacts total). All contacts verified via LinkedIn profile scrape.

**6-step verification per contact:**
1. **ZoomInfo contact search** with `companyName` + `zipCode` + `zipCodeRadiusMiles: 50` → name, title, ZI Contact ID
2. **Apollo cross-match** → search `q_keywords: "First Last CompanyName"` → Apollo ID, LinkedIn URL, email
3. **Google X-ray fallback** (if Apollo has no LinkedIn URL) → `site:linkedin.com/in "First Last" "company"`, then broader `site:linkedin.com/in "First Last"` with company name in snippet
4. **Skip if no LinkedIn URL** — if steps 2-3 cannot find a LinkedIn profile, skip this contact. Cannot do LinkedIn outreach without a URL.
5. **Apify profile scraper** → verify current company (strict match) + PNW location + finance title
6. **Activity Index** → LinkedIn engagement score (1-10)

Only contacts that pass step 5 (all three: company match + PNW location + finance title) get added.

**Store IDs:** `ZoomInfo Contact ID` and `Apollo Person ID` on every verified contact.

**Script:** `scripts/icp1_zoominfo_finance_crossref.py`

---

## Step 3c: LinkedIn X-Ray Discovery (for companies with no verified finance contacts)

**Goal:** Find finance contacts directly from LinkedIn via Google X-ray search. This is the last-resort discovery step and catches contacts invisible to both ZoomInfo and Apollo.

**When to run:** After Steps 2-3b, for **any company that still has no verified CFO/Controller/Director of Finance** — including:
- ZoomInfo found 0 contacts AND Apollo found 0 contacts (e.g., Carillon Properties — private family CRE with no database presence)
- ZoomInfo found contacts but Apollo couldn't cross-match them (need LinkedIn URLs)
- ZoomInfo and Apollo found contacts but **none with finance titles** (e.g., 5 VPs of Engineering but no CFO)
- Step 3b found ZoomInfo finance contacts but none could be verified on LinkedIn

**Tiered fallback:** When called from `prospect_enricher.py` with `max_tier=3`, X-ray searches all three tiers progressively. When called from `company_scorer.py` (default `max_tier=1`), only Tier 1 finance titles are searched.

**Method:** Google X-ray search via Apify Google SERP actor (`nFJndFXA5zjCTuudP`). Searches Google for `site:linkedin.com/in "company name" <title keyword>` — returns LinkedIn profile URLs from Google's index. No LinkedIn account needed, zero ban risk.

**Apify Actor:** `nFJndFXA5zjCTuudP` (Google Search Results Scraper)

**Tiered search queries (defined in `lib/title_tiers.py`, executed by `lib/xray.py`):**

Tier 1 — always run (~5 SERP queries):
```python
# Finance titles — the primary search
'site:linkedin.com/in "{company}" CFO'
'site:linkedin.com/in "{company}" "chief financial officer"'
'site:linkedin.com/in "{company}" controller'
'site:linkedin.com/in "{company}" "director of finance"'
'site:linkedin.com/in "{company}" "vp finance" OR "vp of finance"'
```

Tier 2 — run if < 2 Tier 1 contacts found (~2 SERP queries):
```python
# Executive titles — secondary option when no finance leadership exists
'site:linkedin.com/in "{company}" owner OR president OR CEO'
'site:linkedin.com/in "{company}" "managing director" OR "executive director"'
```

Tier 3 — run if still 0 contacts (~2 SERP queries):
```python
# Junior finance — last resort per Chad's spec
'site:linkedin.com/in "{company}" "accounting manager"'
'site:linkedin.com/in "{company}" "finance manager" OR treasurer'
```

Each contact found is tagged with `tier` (1, 2, or 3) and `tier_label` for Chad's review.

**Example:**
```python
payload = {
    "queries": 'site:linkedin.com/in "Carillon Properties" controller',
    "maxPagesPerQuery": 1,
    "resultsPerPage": 10,
    "countryCode": "us",
}
items = run_actor("nFJndFXA5zjCTuudP", payload)
for item in items:
    for result in item.get("organicResults", []):
        url = result.get("url", "")
        if "linkedin.com/in/" in url:
            # Found a prospect — extract URL + headline from snippet
            linkedin_url = url
            title_snippet = result.get("title", "")
            description = result.get("description", "")
```

**Snippet pre-filter (CRITICAL — saves ~80% of Apify costs):** Before scraping any X-ray result profile, check the Google snippet (title + description) for finance keywords. Skip results where the snippet clearly shows a non-finance role (e.g., "Project Manager", "Software Engineer"). Only scrape profiles where the snippet contains a finance keyword from `FINANCE_TITLES`.

```python
finance_snippet_kw = ['cfo', 'chief financial', 'controller', 'vp finance', 'vp of finance',
                      'vice president of finance', 'vice president, finance', 'director of finance',
                      'director, finance', 'finance director', 'financial controller',
                      'treasurer', 'accounting manager', 'finance manager']
snippet = (result.get('title', '') + ' ' + result.get('description', '')).lower()
if not any(k in snippet for k in finance_snippet_kw):
    continue  # Skip — not a finance contact
```

**After pre-filter passes:** Run Apify Profile Scraper (`LpVuK3Zozwuipa5bp`) to verify the person is currently at the company and get their live headline/title.

**Matching logic:** When matching search results to a company:
- Result title or description must contain the company name (or first word of it)
- Filter out results from different companies with similar names (e.g., "Skills Inc." vs "LifeSkills, Inc." vs "SkillNet Solutions")
- **IMPORTANT:** Generic company names like "Mountain Construction", "Northwest Construction", "TASC" will match many unrelated companies. For these, require 2+ significant words to match, not just 1.

**Real-world results from model client pipeline:**
- **Carillon Properties:** ZoomInfo 0, Apollo 0 → X-ray found Alina Wilson (Controller) and Beth Peterson (Senior Property Manager). Went from zero to 2 confirmed contacts.
- **Skills Inc.:** ZoomInfo 10, Apollo 3 → X-ray found Michelle Leavitt (Director of Finance), Patty Chappell (Finance Officer), Monica Dooley (Director of Operations). 3 new contacts not in either database.

**Cost:** ~$0.01 per search query × 4 queries per company = ~$0.04/company

**Rate limiting:** 2-3 second delay between queries.

---

## Step 4b: ZoomInfo Data Staleness Validation

**Goal:** Verify that ZoomInfo-only contacts (no Apollo match) are still at the company before including them in the pipeline.

**Why this is critical:** ZoomInfo data can be severely outdated. In the model client pipeline, 9 out of 10 ZoomInfo-only contacts for Skills Inc. were stale — the people had left the company, changed careers, or were completely wrong matches. Without validation, you'd generate outreach messages addressing someone as "CFO at Skills Inc." when they're actually a nurse in Colorado.

**Method:** Google X-ray search + Apify Profile Scraper (same actors as Step 3b).

**For each ZoomInfo-only contact:**
```python
# Step 1: Find them on LinkedIn via Google X-ray
queries = [
    f'site:linkedin.com/in "{first_name} {last_name}" "{company_name.split()[0]}"',
    f'site:linkedin.com/in "{first_name} {last_name}"',
]

# Step 2: If found, scrape live profile
profile = run_actor("LpVuK3Zozwuipa5bp", {"urls": [linkedin_url]})

# Step 3: STRICT company match — all significant words must match
def strict_company_match(target, linkedin_company, headline):
    target_words = [w for w in target.lower().split() if len(w) > 3
                    and w not in ('inc.', 'inc', 'llc', 'corp', 'corp.', 'the', 'and')]
    li_combined = (linkedin_company + ' ' + headline).lower()
    matched = [w for w in target_words if w in li_combined]
    if len(target_words) == 1:
        return len(matched) >= 1
    return len(matched) >= 2  # Need 2+ significant words

# Step 4: PNW location gate
PNW_KEYWORDS = ['washington', 'oregon', 'wa', 'or', 'seattle', 'bellevue', 'tacoma',
                'redmond', 'kirkland', 'everett', 'renton', 'kent', 'auburn', 'olympia',
                'lynnwood', 'lakewood', 'federal way', 'vancouver', 'portland', 'salem',
                'eugene', 'bend', 'spokane', 'greater seattle']
location = str(profile.get("location", "")).lower()
in_pnw = any(k in location for k in PNW_KEYWORDS)
```

**Validation outcomes (all three must pass):**
| Check | Pass | Fail |
|-------|------|------|
| Company match (strict) | Keep | **Remove** — wrong company or same name in different state |
| PNW location | Keep | **Remove** — same-named company elsewhere (e.g., Merit Construction in Tennessee) |
| Finance title in headline | Keep | Flag for review — at company but title unclear |
| Not found on LinkedIn | — | **Skip entirely** — cannot do LinkedIn outreach without a URL |

**IMPORTANT: Do NOT keep contacts with no LinkedIn URL.** The previous approach ("keep with caveat") leads to dead-end prospects that can never be contacted via LinkedIn.

**Real-world false positives caught by strict validation (April 2026):**
| Name | Target Company | Actually At | Location | Why Caught |
|------|---------------|-------------|----------|------------|
| Lynn Cooper | Merit Construction (Lakewood, WA) | Merit Construction, Inc. (Knoxville, TN) | Tennessee | PNW location gate |
| Michael Dahl | Northwest Construction (Bellevue, WA) | Northwest Construction (Tucson, AZ) | Arizona | PNW location gate |
| Suanne Dedmon | Mountain Construction (Tacoma, WA) | Rocky Mountain Construction Group | Colorado | Strict company match (2+ words) |
| Phyllis A. S. | AvtechTyee (Everett, WA) | AvtechTyee | Albuquerque, NM | PNW location gate |

**Name matching tips:**
- Last name is the primary match key (more unique than first name)
- Handle nicknames: Dan/Daniel, Chris/Christopher, Bob/Robert — check if either name is a prefix of the other
- Check first 3 chars of first name as fallback

**Real-world stale data examples from model client pipeline:**
| ZoomInfo Name | ZoomInfo Title | Actual LinkedIn | Verdict |
|---------------|---------------|-----------------|---------|
| Christopher Kuczek | CFO @ Skills Inc. | VP Operational Finance @ CommuniCare Health (Cincinnati) | LEFT COMPANY |
| Kathy Frey | CFO @ Skills Inc. | Registered Nurse @ Boulder Medical Center | WRONG PERSON |
| Sanjay Amdekar | Global CFO @ Skills Inc. | Global CFO @ SkillNet Solutions (Mumbai) | WRONG COMPANY |
| Barry Wilson | CFO @ Skills Inc. | Finance @ Palette Skills (Ottawa, Canada) | WRONG PERSON |

**Cost:** ~$0.01 per Google X-ray search + ~$0.003 per profile scrape = ~$0.013/contact

---

## Step 4: Apollo Person Enrichment

**Goal:** Unlock full contact + company data for prospects with Apollo IDs.

**Cost:** $1 per person (1 Apollo credit)

**Endpoint:** `POST /api/v1/people/match` with `{"id": "apollo_person_id", "reveal_personal_emails": true}`

**Data returned:**
| Field | Coverage |
|-------|----------|
| Full name | 100% |
| LinkedIn URL | 100% |
| Email | ~85% |
| Company industry | ~80% |
| Employee count | ~100% |
| Annual revenue | ~60% |
| Employment history | ~80% |

**Use `ApolloClient` from `lib/apollo.py`:**
```python
from lib.apollo import ApolloClient
apollo = ApolloClient()
result = apollo.enrich_person(apollo_id)
person = result.get("person")
extracted = apollo._extract_person(person)
```

**Rate limiting:** 1-2 second random delay between enrichment calls.

---

## Step 5: AI Scoring (Two-Tier: Company + Contact)

**The pipeline uses two separate scoring models:**

### 5a. Company-Level Scoring (runs FIRST, gates enrichment)

**Goal:** Score each COMPANY 0-100 against the VWC ICP. This determines whether we spend money enriching contacts at this company.

**Use `score_companies()` from `mvp/backend/services/scoring.py`:**
```python
from mvp.backend.services.scoring import score_companies
scores = score_companies(companies)
```

**Dimensions (calibrated against 4 VWC benchmark clients):**
- industry_fit (0-20): Manufacturing > CRE > Professional Services > Hospitality > Nonprofit > Construction
- company_size (0-20): Sweet spot 100-300 employees, acceptable 25-750
- revenue_fit (0-15): Sweet spot $50M-$100M, acceptable $5M-$150M, unknown = 10/15
- geography (0-15): Seattle metro = 15, greater WA = 13, OR = 11
- ownership_structure (0-15): Private/ESOP/family = 15, PE-backed = 0
- digital_footprint (0-15): LinkedIn page + Google Places + findable contacts + website

**Calibration scores (benchmark):** Formost Fuji 90, Shannon & Wilson 94, Skills Inc. 89, Carillon Properties 84

**Thresholds:**
- 80+ = strong match, proceed to contact enrichment
- 60-79 = partial match, flag for review
- Below 60 = skip, do not spend enrichment credits

**IMPORTANT:** This score goes BEFORE contact discovery (Steps 3-4). Only companies scoring 60+ proceed to Apollo/ZoomInfo contact search. This saves enrichment costs by filtering out non-ICP companies early.

### 5b. Contact-Level Scoring (runs AFTER enrichment)

**Goal:** Score each individual prospect 0-100 for outreach prioritization.

**Use `score_prospects()` from `mvp/backend/services/scoring.py`:**
```python
from mvp.backend.services.scoring import score_prospects
scores = score_prospects(prospects, icp_config, model="gpt-4o-mini")
```

**Critical: ICP config must include expanded industry labels.** Apollo classifies companies differently than expected. The `target_industries` must include:
```python
"target_industries": [
    "manufacturing", "machinery", "mechanical or industrial engineering",
    "electrical/electronic manufacturing", "building materials", "chemicals",
    "wholesale", "industrial automation", "aviation & aerospace", "automotive",
    "food production", "consumer goods", "construction", "civil engineering",
    "real estate", "property management", "professional services",
    "hospitality", "restaurants", "hotels",
    "nonprofit", "nonprofit organization management", "civic & social organization",
    "renewables & environment", "oil & energy",
]
```

**Critical: Pass `company_location` from Google Places for geography scoring.** The scoring prompt uses `company_location` (verified business address) instead of person location from Apollo. Set it on each prospect dict:
```python
prospect["company_location"] = "9131 10th Ave S, Seattle, WA 98108, USA"  # from Google Places
```

**Critical: `custom_notes` must say these are Google Places verified companies.** This prevents the AI from penalizing industry scores for Apollo's non-standard labels:
```python
"custom_notes": "These are Google Places VERIFIED [industry] companies physically located in [city]. Score industry generously."
```

**Hard exclusions (score 0):**
- Revenue > $150M
- Public Fortune 500 (>10,000 employees)
- Government agencies

**VWC ICP size criteria:**
- Sweet spot: $50M-$100M revenue, 100-300 employees
- Acceptable: $5M-$150M revenue, 11-750 employees

---

## Step 6: LinkedIn Validation (Profile + Activity + Company Page)

**Goal:** Verify the prospect is still in role, check their LinkedIn activity, and assess the company's LinkedIn presence.

**Three Apify actors for LinkedIn validation:**

### 6a. Profile Scraper — Role Verification (actor `LpVuK3Zozwuipa5bp`)
```python
run_actor("LpVuK3Zozwuipa5bp", {
    "urls": ["https://www.linkedin.com/in/username"],
})
```
- **Scrapes the prospect's LIVE LinkedIn profile.** Critical for verifying current role.
- Returns: `firstName`, `lastName`, `headline`, `location`, `connectionsCount`, `followerCount`, `currentPosition`, `topSkills`, `about`, `openToWork`, `premium`
- `currentPosition` array shows current company + start date — compare to Apollo data to detect role changes
- `headline` shows what the prospect calls themselves NOW (may differ from Apollo/ZoomInfo)
- `connectionsCount` indicates network size (relevant for outreach)

**Why this matters:** Apollo/ZoomInfo data can be months old. A prospect listed as "CFO" may have moved to "Board Advisor" or left the company entirely. This actor shows their LIVE LinkedIn headline and current position.

**Example finding:** Dan Semanskee at Formost Fuji — Apollo says "CFO & Board Member", LinkedIn headline says "Board Member / Advisor". Role has changed. This prevents sending a message addressing him as CFO when he's no longer in that role.

**Role verification logic:**
```
Apollo title: "Chief Financial Officer"
LinkedIn headline: "Board Member / Advisor"
→ FLAG: Role may have changed. Review before outreach.
```

### 6b. Activity Index — Activity Check (actor `kog75ERz9lcVNujbQ`)
```python
run_actor("kog75ERz9lcVNujbQ", {
    "linkedinUrl": "https://www.linkedin.com/in/username",
})
```
- Returns `activity_score` (1-10), `recommendation`, `score_breakdown` (profile_completeness, premium_status, network_size, recent_activity, engagement_frequency)
- Returns `activity_metrics` (last_activity_date, days_since_last_activity, posts_last_30_days, reactions_last_30_days, total_posts_scraped, total_reactions_scraped)
- Returns `insights` array and `profile_summary`
- Captures posts, reposts, reactions, comments — full activity picture, not just posts
- Input must be a single string URL (must be https), not an array
- Cost: ~$0.002/profile

### 6c. Company Page Scraper — Company Presence (actor `UwSdACBp7ymaGUJjS`)
```python
run_actor("UwSdACBp7ymaGUJjS", {
    "companies": ["https://www.linkedin.com/company/company-slug/"],
})
```
- Scrapes the company's LinkedIn page
- Returns: `name`, `followerCount`, `employeeCount`, `description`, `tagline`, `logo`, `foundedOn`, `locations`
- Input: array of company LinkedIn URLs (from Apollo org enrichment Step 2)

### Deprecated actors — DO NOT USE:
- **`LQQIXN9Othf8f7R5n`** (apimaestro) — `username` defaults to "satyanadella", returns wrong data
- **`A3cAPGpwBEG8RJwse`** (harvestapi posts) — returns 0 items for many profiles
- **`FiHYLewnJwS6GnRpo`** (harvestapi comments) — removed from pipeline
- **`RE0MriXnFhR3IgVnJ`** (Posts Scraper) — Only captures posts/reposts, misses reactions and comments. Replaced by Activity Index actor `kog75ERz9lcVNujbQ` which gives a complete 1-10 activity score including all engagement types.

### Validation approach:
1. **Profile scraper** (`LpVuK3Zozwuipa5bp`) — verify current role matches Apollo data
2. **Activity Index** (`kog75ERz9lcVNujbQ`) — get activity score (1-10) with full engagement breakdown
3. **Company page scraper** (`UwSdACBp7ymaGUJjS`) — assess company LinkedIn quality
4. Combine into validation report per prospect:
   - Role verified? (headline matches Apollo title)
   - Profile completeness (connections, skills, about section)
   - LinkedIn activity level (score-based, see below)
   - Company page quality (followers, description, logo)

### Activity classification (score-based):
- **Very Active** (7-10) — high engagement across posts, reactions, and comments
- **Active** (5-6) — regular engagement
- **Moderate** (3-4) — occasional engagement
- **Low** (1-2) — minimal engagement
- **Inactive** (0) — no detectable activity

**Cost:** ~$0.007 per prospect (profile $0.003 + Activity Index $0.002 + company page $0.002)

---

## Step 7: Connection Notes

**Goal:** Generate personalized LinkedIn connection request notes from Adrienne/Melinda TO each prospect.

**Use `generate_connection_note()` from `mvp/backend/services/message_gen_svc.py`:**
```python
from mvp.backend.services.message_gen_svc import generate_connection_note
note = generate_connection_note(prospect_dict, company_dict, "Adrienne Nordland")
```

**Constraints:**
- ≤ 200 characters (LinkedIn free account limit)
- Written FROM Adrienne/Melinda TO the prospect (address prospect by first name)
- Do NOT mention VWC CPAs by name
- Warm, professional, not salesy

---

## Step 8: Message Sequences

**Goal:** Generate 3 follow-up messages per prospect.

**Use `generate_messages()` from `mvp/backend/services/message_gen_svc.py`:**
```python
from mvp.backend.services.message_gen_svc import generate_messages
msgs = generate_messages(prospect_dict, company_dict, icp_config)
```

- **Message 1** (after connection accepted): Reference their role/company
- **Message 2** (~2 weeks later): Different angle, industry insight
- **Message 3** (~4 weeks): Final light touch

---

## Output Format

The final CSV should match this header structure:

```
Company ICP Score, Pipeline Action, Company, Industry, Company Location,
Company LinkedIn URL, Company LI Followers,
First Name, Last Name, Title, Seniority,
LinkedIn URL, LinkedIn Headline, Role Verified,
LinkedIn Connections, LinkedIn Followers, Open to Work,
Email, Email Status, Apollo Person ID, Apollo Company ID,
Activity Score, Activity Level, Activity Recommendation, Activity Insights,
Posts Last 30 Days, Reactions Last 30 Days, Last Activity Date, Days Since Last Activity,
LinkedIn Active Status,
Melinda's Connection Note, Adrienne's Connection Note,
Message 1 - Melinda, Message 2 - Melinda, Message 3 - Melinda,
Message 1 - Adrienne, Message 2 - Adrienne, Message 3 - Adrienne,
Data Source
```

---

## Post-Enrichment Validation Checks

Before finalizing any deliverable, run these validation passes. These were discovered through real pipeline issues where bad data slipped through.

### 1. LinkedIn Employee Count Re-Verification
After all companies are scored, re-scrape LinkedIn company pages via Apify (`UwSdACBp7ymaGUJjS`) to verify employee counts. Apollo employee counts are unreliable — LinkedIn is the source of truth.
- **DO NOT hard-exclude based on employee count alone.** The scoring model intentionally allows small companies through with a soft penalty (10-13/20 for 11-24 employees). VWC's benchmark client Carillon Properties has unknown employee count and scored 84. Adrienne confirmed that family-owned and small building companies are expected to have limited data.
- If LinkedIn employees differ significantly from Apollo (e.g., Apollo said 104, LinkedIn says 8), **re-score the company** with correct data. The original score was based on bad data.
- Companies where Apollo said 100+ but LinkedIn shows < 25 → investigate, likely wrong data or wrong company matched

**Example catch:** 4 Tomorrow scored 86 based on Apollo/LinkedIn showing 104 employees. Apify re-scrape revealed only 8. Re-scored, it would be ~74 (FLAG). Removed for bad data, not for being small.

### 2. Industry Validation
Verify company industry matches ICP 1 target industries: Manufacturing, Construction, CRE, Professional Services, Hospitality, Nonprofit.
- Staffing/recruiting firms → remove (Keltia Design was scored PROCEED but is a staffing firm)
- CPA/accounting firms → remove (competitors, not prospects — CBIZ Berntson Porter is a CPA firm)
- Companies acquired by public companies → remove (Field Roast acquired by Maple Leaf Foods, public Toronto company)

### 3. Contact Title Filtering
Only keep contacts with ICP-matching titles:
- **Primary:** CFO, Chief Financial Officer, Controller, VP Finance, Director of Finance
- **Secondary:** President, Owner, CEO, Founder, Managing Director, Executive Director, Partner, Member, Principal
- **Adjacent (acceptable):** Accounting Manager, Bookkeeper, Treasurer, FP&A

Remove all off-target titles: Engineers, Project Managers, Superintendents, Sales VPs, HR, Marketing, etc. These people don't make audit/tax engagement decisions.

### 4. Contact-Company Verification
For every contact, verify via Apify profile scraper that they are actually at the correct company:
- LinkedIn current company must match target company name (first 6-8 chars)
- Location should be in PNW area
- Watch for common name collisions in X-ray search (e.g., "Field" matches Field Aerospace, "Pennon" matches Pennon Group Plc UK, "SMC" matches SMC Financial)

### 5. Parent Company / Acquisition Check
Some companies may have been acquired or operate under a parent company:
- **Sealaska Constructors** → finance handled by parent Sealaska Corporation (domain: sealaska.com, not woocheen.com). Search parent domain for finance contacts.
- **Field Roast** → acquired by Maple Leaf Foods (public). No standalone finance contacts.
- **RAM Mounts** → operates as National Products Inc. on LinkedIn. Apollo domain `rammount.com` is correct.

When Apollo returns 0 contacts by domain, try these fallbacks in order:
1. Search by parent company domain (e.g., `sealaska.com` instead of `woocheen.com`)
2. Search by company name keyword: `q_keywords: "Company Name"` instead of `q_organization_domains_list`
3. Search for specific people by name if found via ZoomInfo or Google: `q_keywords: "First Last CompanyName"`

### 6. ZoomInfo Finance Cross-Reference
For companies where Apollo only returned executive titles (President/Owner/CEO), run ZoomInfo contact search with zip code validation:
```python
requests.post('https://api.zoominfo.com/search/contact', json={
    'companyName': company_name,
    'jobTitle': 'CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance',
    'zipCode': company_zip,
    'zipCodeRadiusMiles': '50',
    'rpp': 5,
})
```
This found 17 verified finance contacts at 13 companies where Apollo had none. Always verify via LinkedIn profile scraper before adding.

### 7. Deduplication
Run `scripts/icp1_clean_final_output.py` as the final step:
- Dedup by LinkedIn URL
- Dedup by (First Name + Last Name + Company)
- Remove rows where First Name = Last Name (data error)
- Normalize all LinkedIn URLs to https://

---

## Error Handling for Cloud Agents

- **LinkedIn URL normalization:** All URLs MUST be converted to `https://www.linkedin.com/in/...` before any Apify actor call. `http://` URLs will cause 400 errors.
- **Activity Index input:** Takes a single string `linkedinUrl`, NOT an array
- **Retry logic:** Retry up to 3 times on failure with 10s delay between attempts
- **Rate limits (429):** Wait 30 seconds and retry
- **Out of credits (402):** Save progress immediately and stop the script
- **Progress saves:** Write CSV every 10 contacts to prevent data loss on crashes
- **CSV writing:** Always use `extrasaction='ignore'` to prevent KeyError on missing columns
- **Apollo ID tracking:** All enrichment scripts must store `Apollo Person ID` and `Apollo Company ID` for traceability and re-enrichment
- **`--retry-errors` flag:** Use `python scripts/icp1_fix_posts_only.py --retry-errors` to retry only failed contacts

---

## VWC ICP Quick Reference

**ICP 1 — Audit & Tax:**
- Industries: Manufacturing (#1), Commercial RE (#2), Professional Services (#3), Hospitality (#4), Nonprofit (#5), Construction (#6)
- Geography: Seattle metro → PNW (WA/OR) → West Coast → National
- Size: 25-750 employees, $25M-$150M revenue (sweet spot: 100-300 emp, $50M-$100M)
- Titles: CFO, Controller, VP Finance, Director of Finance (primary); Owner, President, CEO (secondary)
- Ownership: Private, family-owned, ESOP, founder-led
- Exclude: Public, PE-backed, government, banking, >$150M revenue

**ICP 2 — Benefit Plan Audit:**
- Geography: National (no restriction)
- Size: 120+ employees (triggers audit requirement)
- Titles: CFO/Controller (private <$150M), HR Director/VP HR (private >$150M or public)
- All industries except government

**ICP Spec:** `docs/icp/VWC_ICP_Developer_Spec.docx` and `docs/vwc-icp-spec.md`

---

## Bundled Scripts

All scripts are in `scripts/` directory (relative to repo root, not skill directory):

| Script | What It Does | Credits |
|--------|-------------|---------|
| `scripts/icp1_company_first.py` | Step 1: Google Places company discovery for all PNW cities | Free |
| `scripts/icp1_mfg_zoominfo_crossmatch.py` | Step 2: ZoomInfo cross-match for manufacturing | Free |
| `scripts/icp1_mfg_apollo_crossmatch.py` | Step 3: Apollo cross-match for manufacturing | Free |
| `scripts/icp1_seattle_mfg_enrich.py` | Steps 4-8: Full enrichment pipeline for Seattle manufacturing | $1/person + ~$0.55/person Apify |
| `scripts/icp1_enriched_sample_v2.py` | Steps 4-8: Enrichment for a sample batch | $1/person + ~$0.55/person Apify |
| `scripts/icp1_fix_activity_v3.py` | Step 6 only: Re-run activity check with posts + comments | ~$0.55/person Apify |
| `scripts/icp1_model_client_footprint.py` | Digital footprint scan for 4 model clients | ~$2 Apify + 4 Apollo credits |
| `scripts/icp1_model_client_full_pipeline.py` | Full 10-step pipeline for model clients (all steps) | ~$2.60/person |
| `scripts/icp1_unipile_linkedin_discovery.py` | Step 3b via Unipile: LinkedIn search for missing contacts | Free (uses LinkedIn account) |
| `scripts/model_clients_to_prospects.py` | Reshape company-level CSV to one-row-per-prospect format | Offline only |

---

## Complete API & Actor Reference

### APIs (credentials in `.env`)

| API | Purpose | Auth | Cost |
|-----|---------|------|------|
| **Google Places** | Company discovery by industry + city | `X-Goog-Api-Key: AIzaSyDBFl9GysZkM42uPS1wdKh8tTKeedWP67o` | ~$0.032/query |
| **Apollo.io** | Contact search, person/org enrichment | `X-Api-Key` from `APOLLO_API_KEY` | Free search, $1/enrichment |
| **ZoomInfo** | Contact/company search (free, no enrichment) | JWT via `ZOOMINFO_USERNAME`/`ZOOMINFO_PASSWORD` | Free (NRD account) |
| **OpenAI** | ICP scoring + message generation | `OPENAI_API_KEY` | ~$0.01/scoring, ~$0.02/messages |

### Apify Actors (auth: `APIFY_API_KEY`)

| Actor ID | Name | Purpose | Input | Cost |
|----------|------|---------|-------|------|
| `nFJndFXA5zjCTuudP` | Google SERP | Google X-ray search for LinkedIn profiles — finds prospects not in ZoomInfo/Apollo | `{queries: "site:linkedin.com/in ...", maxPagesPerQuery: 1, resultsPerPage: 10, countryCode: "us"}` | ~$0.01/query |
| `kog75ERz9lcVNujbQ` | LinkedIn Activity Index (LinkedScore) | Activity score (1-10), engagement metrics, insights | `{linkedinUrl: "https://www.linkedin.com/in/username"}` | ~$0.002/profile |
| `LpVuK3Zozwuipa5bp` | Profile Scraper | LIVE profile data — role verification, headline, connections | `{urls: [url]}` | ~$0.003/profile |
| `UwSdACBp7ymaGUJjS` | Company Page | Company LinkedIn page — followers, description, tagline | `{companies: [url]}` | ~$0.002/company |

### Deprecated Apify Actors — DO NOT USE

| Actor ID | Why Deprecated |
|----------|---------------|
| `LQQIXN9Othf8f7R5n` | `username` defaults to "satyanadella" — returns wrong data |
| `A3cAPGpwBEG8RJwse` | Returns 0 items for many profiles — replaced by `RE0MriXnFhR3IgVnJ` |
| `FiHYLewnJwS6GnRpo` | Comments scraper — removed from pipeline |
| `RE0MriXnFhR3IgVnJ` | Only captures posts/reposts, misses reactions and comments. Replaced by Activity Index actor `kog75ERz9lcVNujbQ` |

### Internal Services (Python)

| Module | Function | Purpose |
|--------|----------|---------|
| `lib/apollo.py` | `ApolloClient.search_people()` | Free people search |
| `lib/apollo.py` | `ApolloClient.enrich_person(id)` | Person enrichment ($1) |
| `lib/apollo.py` | `ApolloClient._request("POST", "/api/v1/organizations/enrich", ...)` | Org enrichment ($1) |
| `mvp/backend/services/scoring.py` | `score_prospects(prospects, icp_config, model)` | AI scoring 0-100 |
| `mvp/backend/services/message_gen_svc.py` | `generate_connection_note(prospect, company, sender_name)` | ≤200 char connection note |
| `mvp/backend/services/message_gen_svc.py` | `generate_messages(prospect, company, icp_config)` | 3-message sequence |
| `db/connect.py` | `get_supabase()` | Supabase DB client |

---

## Cost Summary

| Step | Cost | Notes |
|------|------|-------|
| Google Places | ~$0.032/company | Text Search API |
| ZoomInfo search | Free | NRD account, no enrichment |
| Apollo search | Free | Search is unlimited |
| Apollo org enrichment | $1/company | By domain — revenue, employees, company LinkedIn |
| **Google X-ray (Step 3b)** | **~$0.04/company** | 4 queries × $0.01 — finds contacts invisible to ZoomInfo/Apollo |
| **Stale validation (Step 4b)** | **~$0.013/contact** | $0.01 X-ray + $0.003 profile scrape — catches outdated ZoomInfo data |
| Apollo person enrichment | $1/person | LinkedIn URL, email, employment history |
| AI scoring | ~$0.01/person | GPT-5.4 |
| Apify profile scraper | ~$0.003/person | Role verification |
| Apify Activity Index | ~$0.002/person | Activity check |
| Apify company page | ~$0.002/company | Company LinkedIn quality |
| Message generation | ~$0.02/person | GPT-5.4 |
| **Total per prospect (full pipeline)** | **~$2.10/person** | All 10 steps including X-ray discovery |
| **Total per prospect (X-ray only, no Apollo enrich)** | **~$0.07/person** | For contacts found via X-ray (no Apollo ID) |

### Key Pipeline Insight

ZoomInfo and Apollo have significant blind spots for private, family-owned, and smaller companies. In the model client benchmark:
- **Carillon Properties** (private family CRE): 0 contacts in both databases → Google X-ray found 2 (Controller + Senior Property Manager)
- **Skills Inc.** (nonprofit): ZoomInfo had 10 contacts but 9 were stale (wrong person or left company). X-ray found 3 additional valid contacts.
- **Overall:** ~40% of the final prospect list came from Google X-ray discovery, not ZoomInfo/Apollo.
