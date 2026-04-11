---
name: icp-company-pipeline-revamped
description: Run the revamped company-first ICP prospect pipeline for VWC CPAs with v2 scoring. This pipeline adds organizational complexity scoring (CFO/Controller detection), revenue mismatch handling, contact activity classification, client blacklist checks, and 3-tier finance contact verification. Use this skill when asked to "run revamped pipeline", "v2 scoring", "rescore companies", "find CFOs at flagged companies", "organizational complexity scoring", or when someone wants the improved pipeline with finance title scanning and activity filtering.
---

# ICP Company Pipeline — Revamped (v2 Scoring)

This is the improved version of the company-first prospect pipeline. It adds finance title detection, revenue mismatch handling, organizational complexity scoring, contact activity classification, and a client blacklist. The original `icp-prospect-company-pipeline` skill is preserved unchanged.

## Why v2

After the April 4, 2026 client review, Chad and the VWC partners identified gaps in v1:

1. **CFO/Controller = complexity signal.** A property manager at a great company scored low because there was no way to signal organizational complexity. Chad said: "if they have a CFO or Controller, that indicates increased complexity."
2. **Revenue is unreliable.** Machinists Inc showed $1M revenue with 83 employees. Adrienne confirmed data is wrong. Revenue shouldn't tank a score when employees tell a different story.
3. **Inactive contacts are a waste.** Michael Turner (1 connection, no activity) shouldn't be messaged.
4. **Existing clients must be excluded.** Anthony's Homeport is a VWC client for 401K — can't contact them.

## Edge Cases & Lessons Learned (April 8-11, 2026)

Critical discoveries from scoring 2,000+ companies:

### GPT Score Mismatch
GPT's final score doesn't always match its own dimension breakdown sum. Example: A-America got `score: 78` but breakdown sums to 84. **Fix:** Post-processing recalculates score from breakdown dimensions. GPT provides reasoning text, we compute the number.

### Benefit of the Doubt Rule
Companies scoring 75-79 with ANY tier contacts (CFO, CEO, Owner, Accounting Manager) get upgraded to PROCEED. Per Chad: "having a financial person is a strong signal." This caught 80+ companies that GPT underscored.

### Scoring Guardrails (`lib/score_guardrails.py`)
Two-layer correction system:
1. **Rule-based overrides** (instant, no API): CFO found → org_complexity ≥9. Seattle metro → geography=15. Missing revenue → 7/10. <25 LinkedIn employees + contacts → company_size ≥14 (Carillon pattern). Unknown ownership → 12.
2. **AI reviewer** (GPT call, only for borderline 70-79 without contacts): Compares against VWC benchmark companies.

### Hard Exclusion Guardrails
- **Revenue ceiling:** >$150M = HARD EXCLUDE (caught 16 companies including Darigold at $2.3B that GPT missed)
- **Employee ceiling:** >10,000 = HARD EXCLUDE
- **Public company detection:** Scans descriptions for "publicly traded", "NYSE", "NASDAQ" with word-boundary matching. Caught Getty Images, Columbia Sportswear. **Warning:** substring "ipo" matches inside normal words (equipo, BIPOC, tripod) — use phrase matching only.
- **Competitor CPA firms:** Exact-name matching for Moss Adams, BDO, Deloitte, KPMG, etc. These are VWC's competition, NOT targets. Different from Form 5500 boost which flags their CLIENTS as targets.

### Form 5500 Big-Firm Signal
Companies audited by Big Four/large CPA firms get a POSITIVE scoring boost (0-8 pts). Data from `data/form5500_big_firm_clients.csv` (5,042 companies). Per Chad: these companies "feel like a small fish in a big pond" — prime targets for VWC.

### X-ray False Positives
- Generic company names (e.g., "CJ Construction") produced match term `["construction"]` which matched anyone in construction. **Fix:** Keep short words when only word is an industry term → `["cj construction"]`.
- Location check: if match term is generic, require PNW location on the profile.
- Gene Boyer III (CFO at AR Construction, Pittsburgh) was incorrectly matched to CJ Construction (Bellevue). Profile verification caught it.

### ZoomInfo Company ID Search
ZoomInfo batch data includes `zi_id` (company ID). Searching contacts by `companyId` instead of name+zip finds **3x more contacts** with zero false positives. The original name+zip search missed 439 contacts across 164 companies in batch_01 alone.

### Contact Discovery Cross-Referencing
For ZoomInfo contacts (have zi_contact_id but no LinkedIn URL):
1. Apollo cross-match (free) → get apollo_id + linkedin_url
2. If Apollo has no match → Serper google search → get linkedin_url (unverified)
3. Prospect pipeline (Stage 2) validates the LinkedIn URLs later

### Serper.dev vs Apify SERP
Serper.dev is 7.6x faster than Apify SERP actor (~5s vs ~40s per company). Nearly identical result quality (90% overlap). Serper is now the default SERP provider. Toggle via `SERP_PROVIDER` env var ("serper" or "apify").

### Checkpoint/Resume
Pipeline crashes from internet drops don't lose progress. Smart recovery:
- `enriching` companies → reset to `raw` (re-enrich)
- `scoring` companies WITH score → promote to `scored` (saves GPT credits)
- `scoring` companies WITHOUT score → reset to `enriched` (re-score only)
- X-ray rescue skips companies with existing `xray_rescue` data
- Contact discovery skips companies with existing `contact_discovery` data

### Promoting Companies
When promoting PROCEED companies to `companies_universe`:
- Creates stub prospect records from discovered contacts (all tiers)
- Preserves apollo_id, zoominfo_contact_id, linkedin_slug
- Links ALL existing prospects to `company_universe_id` (not just newly created ones)
- "Promote Selected" button respects checkbox selection (not bulk all)

### Dashboard Performance
Exclude `enrichment_data` and `source_data` JSONB columns from list queries. Payload drops from ~144KB to ~17KB for 50 rows. Company detail page queries by `company_universe_id` first, falls back to `company_name` (avoids PostgREST comma parsing bug in `.or()`).

## Pipeline Sequencing — How v2 Fits With Prospect Pipeline

The v2 company pipeline and the `/icp-prospect-pipeline` are **separate but sequenced**:

```
Phase 0: LINKEDIN SCRAPE (batch, ~$0.002/company)
  ├─ Apify company page scraper for followers, employees, description
  ├─ HQ vs branch location detection
  └─ Domain extraction from LinkedIn website field

Phase 1: ENRICHMENT + SCORING (cheap, ~$1/company + free searches)
  ├─ Client blacklist check (free)
  ├─ Pre-filter: skip companies with no domain AND no LinkedIn (→ "incomplete")
  ├─ Apollo org enrichment ($1/company)
  ├─ Finance title scan — Tier 1+3: Apollo search (free, 14 titles)
  ├─ PSBJ cross-reference + Form 5500 big-firm signal (free)
  ├─ Revenue mismatch detection (free)
  ├─ V2 scoring (8 dimensions including organizational_complexity + big_firm_signal)
  ├─ Guardrails: rule overrides + score recalculation + hard exclusion checks
  └─ Output: PROCEED (80+ or 75-79 with contacts) / REVIEW / SKIP / HARD EXCLUDE

Phase 2: X-RAY RESCUE for REVIEW companies (~$0.01/company via Serper)
  ├─ Serper.dev X-ray search — ALL 3 tiers (Tier 1+2+3)
  ├─ Apify profile scrape verification (~$0.003/contact)
  ├─ Rescore with new organizational_complexity data
  └─ Output: some REVIEW companies cross threshold → become PROCEED

Phase 3: CONTACT DISCOVERY for ALL SCORED companies (free + ~$0.001/Serper query)
  ├─ Apollo free people search (all tiers, by domain)
  ├─ ZoomInfo free contact search (by company ID when available, by name+zip fallback)
  ├─ Serper X-ray discovery (if Apollo+ZoomInfo found no Tier 1)
  ├─ Cross-reference: ZoomInfo contacts → Apollo (free) → Serper for LinkedIn URLs
  ├─ All contacts stored as stubs (status: sourced) with apollo_id, zoominfo_contact_id, linkedin_slug
  ├─ Audit logged to discovery_log table
  └─ Output: stub prospects ready for Stage 2 enrichment

Phase 4: ALL PROCEED COMPANIES → /icp-prospect-pipeline (expensive, ~$2.10/person)
  ├─ Full contact validation + enrichment (uses stored IDs to skip re-discovery)
  ├─ LinkedIn validation + activity scoring
  ├─ Contact-level ICP scoring
  ├─ Connection notes + message sequences
  └─ Output: dashboard view with contacts table
```

**Key principle:** Score companies cheaply first, then spend money on contact discovery only where it matters. Finance contacts found during Phase 1-2 become the seed for Phase 3.

## The 14-Step Pipeline

```
Step 1:   COMPANY DISCOVERY              Google Places + LinkedIn X-ray (free/$0.01)
Step 1b:  CLIENT BLACKLIST CHECK          Exclude known VWC clients  ← NEW
Step 2:   COMPANY LINKEDIN ENRICHMENT     Apify company page scraper + HQ/branch detection ($0.002/company)
Step 3:   COMPANY DATA ENRICHMENT         Apollo org enrich ($1/company)
Step 3b:  FINANCE TITLE SCAN             3-tier: Apollo (free) → X-ray ($0.04) → Profile verify ($0.003)  ← UPDATED
Step 3c:  EXTERNAL DATA OVERLAY           PSBJ list cross-reference for revenue  ← NEW
Step 3d:  REVENUE MISMATCH DETECTION      Flag suspect revenue vs employee count  ← NEW
Step 4:   COMPANY-LEVEL ICP SCORING       score_companies_v2() with organizational complexity
Step 4b:  X-RAY RESCUE FOR REVIEW         Tier 2+3 for REVIEW companies with 0 contacts  ← NEW
Step 5:   CONTACT DISCOVERY               Apollo + ZoomInfo search (free) for 60+ companies
Step 5b:  LINKEDIN X-RAY CONTACTS         Google X-ray for companies with 0 contacts
Step 6:   STALE DATA VALIDATION           Verify ZoomInfo-only contacts via Apify profile scrape
Step 6b:  DUPLICATE PROFILE DETECTION     Flag same-person multiple LinkedIn profiles  ← NEW
Step 7:   PERSON ENRICHMENT               Apollo enrich ($1/person) for PROCEED only
Step 8:   LINKEDIN VALIDATION             Apify profile scraper + Activity Index
Step 8b:  ACTIVITY STATUS TAGGING         Tag contacts as ACTIVE/INACTIVE  ← NEW
Step 9:   CONTACT-LEVEL SCORING           score_prospects() for outreach prioritization
Step 10:  CONNECTION NOTES                Adrienne + Melinda versions (under 200 chars)
Step 11:  MESSAGE SEQUENCES               3-message follow-up per prospect
Step 12:  OUTPUT                          CSV matching all_proceed_companies.csv format + v2 columns
```

---

## Step 1b: Client Blacklist Check

**File:** `data/blacklist.csv`

**Columns:** `company_name,domain,reason,added_by,added_date`

Check every discovered company against the blacklist before spending enrichment credits. Uses:
- Substring name matching (catches "Anthony's HomePort Everett" → "Anthony's HomePort")
- Exact domain matching

Run again at Step 5 output to catch contacts whose company matches a blacklisted entry.

---

## Step 2: LinkedIn Enrichment + HQ/Branch Detection

The Apify company page scraper returns a `locations` array with a `headquarter` boolean flag. During Phase 0, the pipeline:

1. Scrapes LinkedIn company page for employees, followers, tagline, description, founded year
2. Extracts all locations and identifies which is HQ vs branch
3. If the company's listed PNW location is a **branch** (HQ is elsewhere), stores branch info in `enrichment_data.linkedin_scrape`:
   - `hq_location`: full HQ address (e.g., "425 Park Ave, Lake Villa, IL, 60046, US")
   - `branch_locations`: list of non-HQ office addresses
   - `is_branch`: true if the PNW location is a branch

**Important:** A Seattle branch still counts for geography scoring (full 15/15 points). The branch flag is informational for Chad's review — the company has local presence. The `location` field stays as the PNW address, not modified.

**Example:** ID Label Inc. has HQ in Lake Villa, IL but a Seattle branch at 3250 Airport Way South. It still scores 15/15 on geography because they have a Seattle office. Chad sees "BRANCH — HQ: Lake Villa, IL" in the review dashboard.

---

## Step 3b: Finance Title Scan — 3-Tier Approach

**Goal:** Before scoring, detect if a company has a CFO, Controller, or other finance leadership. This feeds the `organizational_complexity` scoring dimension.

### Title Tiers (from Chad's ICP spec — centralized in `lib/title_tiers.py`)

**Tier 1 — Primary Finance (always search first):**
CFO, Chief Financial Officer, Controller, Financial Controller, VP Finance, VP of Finance, Vice President of Finance, Director of Finance, Finance Director

**Tier 2 — Executive (secondary, for prospect outreach only — NOT used for company scoring):**
Owner, President, CEO, Founder, Managing Director, Partner, Executive Director

**Tier 3 — Junior Finance (last resort per Chad — "too junior to initiate an audit relationship"):**
Accounting Manager, Finance Manager, Treasurer, Bookkeeper, Staff Accountant

**Company scorer searches Tier 1 only** — finding a CFO/Controller is a scoring signal about organizational complexity. Tier 2/3 contacts are discovered later in the prospect enricher for outreach targets.

### Tier 1: Apollo people search ($0 — runs on ALL companies with a domain)

```python
from lib.apollo import ApolloClient
apollo = ApolloClient()
result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
    "q_organization_domains_list": [company["domain"]],
    "person_titles": FINANCE_TITLES,  # Tier 1 only: CFO, Controller, VP Finance, Director of Finance
    "per_page": 5,
})
```

For contacts Apollo returns WITHOUT a LinkedIn URL, run `xray_find_contact_linkedin()` to find their profile via Google X-ray search.

### Tier 2: Google X-ray discovery (~$0.04/company — ONLY for REVIEW companies where Tier 1 returned 0)

**Actor:** `nFJndFXA5zjCTuudP` (Google SERP)

**CRITICAL: Domain-first search.** If the company has a domain, search using the domain, NOT the company name. This prevents false positives from ambiguous names (e.g., "SMC" matches SMC Corporation Japan, not Seattle Manufacturing Corporation).

```python
# If domain available — use domain
queries = [
    f'site:linkedin.com/in "{domain}" CFO',
    f'site:linkedin.com/in "{domain}" "chief financial officer"',
    f'site:linkedin.com/in "{domain}" controller',
    f'site:linkedin.com/in "{domain}" "director of finance"',
]

# If no domain — use the most distinctive part of the company name
# "SMC - Seattle Manufacturing Corporation" → search "seattle manufacturing corporation"
# NOT "SMC" (too ambiguous)
```

**Company name parsing for search queries:**
- If name contains " - " separator: use the LONGER part (the full name, not the abbreviation)
- Strip generic suffixes: Inc, LLC, Corp, Corporation, Company, Co, Ltd, Group, Services, Management
- Use first 2+ meaningful words as match terms

**Result validation:** Every X-ray result is checked against match terms derived from the company name. Results that don't mention the company are SKIPped.

```python
match_terms = _build_company_match_terms(company_name)
# "SMC - Seattle Manufacturing Corporation"
#   → ["seattle manufacturing corporation", "seattle manufacturing"]
# "TASC - Technical & Assembly Services Corporation"
#   → ["technical & assembly services corporation", "technical assembly"]
```

### Tier 3: Profile scrape verification (~$0.003/contact — for ALL X-ray Tier 2 results)

**Actor:** `LpVuK3Zozwuipa5bp` (LinkedIn Profile Scraper)

Every contact found via X-ray MUST be verified by scraping their live LinkedIn profile:

```python
profiles = run_actor("LpVuK3Zozwuipa5bp", {"urls": [linkedin_url]})
# Check: currentPosition[].companyName matches target company
# Check: title/headline matches a finance role
# If wrong company → REJECTED
# If verified → update title from live data, record connections count
```

**Why verification is required:** X-ray search is fuzzy. Testing showed false positives when:
- Company name contains a common abbreviation (SMC, JJR, CJ)
- Company name is a common word ("Launch", "Pacific", "Field")
- Multiple companies share similar names ("Skills Inc." vs "SkillNet Solutions")

Without Tier 3, SMC returned "Kathy Nix (CFO)" and "Jason Nordwall (CFO)" — neither works at Seattle Manufacturing Corporation.

### Tier 2+3 timing: AFTER initial scoring

X-ray + profile verification only runs for companies that:
1. Scored as REVIEW (60-79) in the initial v2 scoring
2. Had 0 finance contacts from Tier 1 (Apollo)

This saves ~$0.04/company on PROCEED companies (already scored high enough) and SKIP companies (not worth the cost).

If X-ray finds verified contacts → rescore the company with the new `organizational_complexity` data. Some REVIEW companies will cross 80 → become PROCEED.

### Output columns:
- `Contacts Found` — number of verified finance contacts
- `Has CFO` — Yes/No
- `Has Controller` — Yes/No
- `Finance Contact 1-5 Name` — full name
- `Finance Contact 1-5 Title` — live title from LinkedIn profile (if verified)
- `Finance Contact 1-5 LinkedIn URL` — full LinkedIn profile URL

---

## Step 3c: External Data Overlay

Cross-reference discovered companies against external data sources for revenue validation:

**PSBJ (Puget Sound Business Journal):** `docs/deliverables/week2/universe/private/psbj_family_owned_wa_2026_86.csv`
- ~86 largest family-owned companies in WA
- Has: company name, revenue, employees, CEO
- If a flagged company matches PSBJ: fill missing revenue, confirm family ownership (→ ownership_structure = 15/15)

Match by fuzzy company name (substring match).

---

## Step 3d: Revenue Mismatch Detection

**Function:** `detect_revenue_mismatch()` in `mvp/backend/services/scoring.py`

**Rule:** If `revenue / employees < $30K per employee`, the revenue is likely wrong.

**Example:** Machinists Inc — $1M revenue with 83 employees = $12K/employee → SUSPECT. Score treats revenue as unknown (7/10 benefit of doubt instead of penalizing).

Pure pre-processing, no API cost.

---

## Domain Filtering — Junk Domain Protection

**CRITICAL:** Google Maps/Places sometimes returns social media pages or marketplace links as a company's "website". These must be filtered out before using the domain for Apollo enrichment or X-ray search.

**Blocked domains:**
```python
JUNK_DOMAINS = {
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com", "snapchat.com",
    "amazon.com", "lazada.com", "shopee.com", "ebay.com", "etsy.com",
    "alibaba.com", "aliexpress.com",
    "yelp.com", "yellowpages.com", "bbb.org", "mapquest.com",
    "google.com", "goo.gl", "bit.ly", "linktr.ee",
    "wix.com", "squarespace.com", "godaddy.com", "wordpress.com",
}
```

LinkedIn URLs (`linkedin.com/company/...`) are NOT affected — those are stored in the `linkedin_url` field, not the `domain` field.

If a company's only "website" is a junk domain, `extract_domain()` returns `None`, which means:
- Apollo org enrichment is skipped (no domain to search)
- X-ray Tier 2 falls back to company name search
- The company still gets scored, just with less data

---

## Step 4: Company-Level ICP Scoring (v2)

**Function:** `score_companies_v2()` in `mvp/backend/services/scoring.py`

### Scoring dimensions (v2, 100 points total):

| Dimension | Max | Change from v1 | What It Measures |
|-----------|-----|-----------------|------------------|
| industry_fit | 20 | unchanged | Manufacturing > CRE > Prof Services > Hospitality > Nonprofit > Construction |
| company_size | 20 | unchanged | LinkedIn employees (primary). Sweet spot 100-300. |
| revenue_fit | **10** | was 15 (-5) | Revenue is unreliable. SUSPECT/unknown = 7/10 benefit of doubt. |
| geography | 15 | unchanged | Seattle metro = 15, Greater WA = 13, Oregon = 11 |
| ownership_structure | 15 | unchanged | Private/family/ESOP = 15. PE-backed = 0. |
| digital_footprint | **10** | was 15 (-5) | LinkedIn page + Google Places + website. No finance contacts sub-signal (moved). |
| **organizational_complexity** | **10** | NEW | CFO/Controller = 9-10. VP Finance = 7-8. CEO only = 3-4. Unknown = 5. |

### Expected impact on flagged companies:
A company at score 77 with a CFO: gains ~6-7 net points (gains 9-10 on complexity, loses ~3-4 on revenue/digital redistribution) → pushes to 83-84 = PROCEED.

### Hard exclusions (unchanged):
Revenue > $150M, public, government, PE-backed, banking, >10K employees.

---

## Step 4b: X-Ray Rescue for REVIEW Companies

After initial v2 scoring, companies that scored REVIEW (60-79) with 0 finance contacts enter the X-ray rescue flow:

1. Run Tier 2 X-ray search (domain-first, with company name match validation)
2. Run Tier 3 profile scrape verification on any X-ray results
3. If verified contacts found → rescore with new organizational_complexity data
4. Companies that cross 80 → PROCEED

**Validated test result (April 7, 2026):**
- SMC (Seattle Manufacturing Corporation): scored 79 REVIEW with 0 contacts
- X-ray searched `"smcgear.com"` (domain-first) → 0 results (correct — no false positives)
- Previous run without domain-first search returned 2 fake CFOs from unrelated "SMC" companies

**Validated test result (April 7, 2026):**
- TASC (Technical & Assembly Services Corporation): Apollo found Controller (Rikki Nelson) → scored 86 PROCEED
- No X-ray needed — Apollo Tier 1 was sufficient

---

## Step 6b: Duplicate Profile Detection

After contact discovery, check for:
- Same person (first_name + last_name + company) with multiple LinkedIn URLs
- Keep the profile with most connections and most recent activity
- Flag the duplicate for review

**Validated finding:** Erin Flack at Ballard Industrial has two LinkedIn profiles — `erinflack206` (44 connections) and `erin-flack-07546330b` (0 connections).

---

## Step 8b: Activity Status Tagging

**Function:** `classify_contact_activity()` in `mvp/backend/services/scoring.py`

Tags each contact as `ACTIVE` or `INACTIVE`:

**ACTIVE** = any of:
- activity_score >= 4 (Moderate or above)
- Any post, reaction, repost, or comment in last 90 days
- Activity level is "Moderate", "Active", or "Very Active"

**INACTIVE** = all of:
- activity_score < 4 (Low or Inactive)
- No engagement in last 90 days
- Connections < 10
- OR: activity level is "Inactive" with 0 engagement ever

Contacts tagged INACTIVE are excluded from the outreach queue. The pipeline looks for alternative contacts at the same company.

---

## Output Format

The output CSV matches `all_proceed_companies.csv` format (columns 1-22) plus v2 additions (columns 23-41):

```
Category, Company, Company ICP Score, Pipeline Action, Industry,
Employees (LinkedIn), Employees (Apollo), Revenue, Location, Ownership,
Company LinkedIn URL, LI Followers, LI Description, LI Tagline, LI Founded, LI Has Logo,
Domain, Website, Contacts Found, Score Breakdown, Reasoning, Why This Score,
Has CFO, Has Controller, Revenue Suspect, Organizational Complexity,
Finance Contact 1 Name, Finance Contact 1 Title, Finance Contact 1 LinkedIn URL,
Finance Contact 2 Name, Finance Contact 2 Title, Finance Contact 2 LinkedIn URL,
Finance Contact 3 Name, Finance Contact 3 Title, Finance Contact 3 LinkedIn URL,
Finance Contact 4 Name, Finance Contact 4 Title, Finance Contact 4 LinkedIn URL,
Finance Contact 5 Name, Finance Contact 5 Title, Finance Contact 5 LinkedIn URL
```

---

## Cost Summary

| Step | API | Cost | When |
|------|-----|------|------|
| 1b | None | $0 | All companies |
| 2 | Apify company scraper | ~$0.002/company | All companies with LinkedIn URL |
| 3 | Apollo org enrich | $1/company | All companies with domain |
| 3b Tier 1 | Apollo people search | $0 | All companies with domain |
| 3b Tier 2 | Apify SERP (X-ray) | ~$0.04/company | REVIEW companies with 0 Tier 1 contacts |
| 3b Tier 3 | Apify profile scraper | ~$0.003/contact | X-ray Tier 2 results only |
| 3c | None | $0 | All companies |
| 3d | None | $0 | All companies |
| 4 | OpenAI GPT-5.4 | ~$0.01/company | All companies |
| 4b | (included in 3b Tier 2+3) | — | REVIEW companies only |
| 6b | None | $0 | All contacts |
| 8b | None | $0 | All contacts |

**Total cost per company (full pipeline):** ~$1.05 (Apollo org enrich + scoring + LinkedIn scrape)
**Additional cost for REVIEW X-ray rescue:** ~$0.04/company + $0.003/verified contact

---

## Key Files

| File | What |
|------|------|
| `lib/title_tiers.py` | Centralized 3-tier title config — single source of truth for all title lists |
| `lib/apify.py` | Apify actor runner + `build_company_match_terms()` with industry word protection |
| `lib/xray.py` | X-ray discovery with Serper (default) or Apify SERP. `max_tier` param. Profile verification. |
| `lib/serper.py` | Serper.dev Google SERP API adapter — 7x faster than Apify SERP |
| `lib/contact_discovery.py` | Shared discovery module: Apollo + ZoomInfo + X-ray + cross-referencing |
| `lib/score_guardrails.py` | Rule overrides + AI reviewer + public company detection + revenue/employee ceilings |
| `skills/company_scorer.py` | Company scoring pipeline — all phases including contact discovery |
| `skills/prospect_enricher.py` | Prospect enrichment — full tiered search for outreach targets |
| `mvp/backend/services/scoring.py` | `score_companies_v2()` with post-processing score recalculation |
| `scripts/export_pipeline_data.py` | CSV backup of all pipeline tables (auto-runs after scoring) |
| `scripts/import_pipeline_data.py` | Restore CSVs into new Supabase project |
| `scripts/compare_serp_providers.py` | Benchmark Serper vs Apify SERP |
| `data/blacklist.csv` | Client exclusion list (exact match) |
| `data/competitors.csv` | Competitor CPA firms (empty — exclusion now in guardrails) |
| `data/form5500_big_firm_clients.csv` | 5,042 companies audited by Big Four/large firms (positive signal) |
| `data/backups/` | Timestamped CSV backups (auto-generated, gitignored) |
| `db/migrate_discovery_log.sql` | Audit table for contact discovery API calls |

## Running the Pipeline

```bash
# Run on a CSV of raw companies
.venv/bin/python3 -m scripts.test_full_v2_pipeline --file /path/to/input.csv --output /path/to/output.csv

# Input CSV must have at minimum: company_name, linkedin_url
# Optional input columns: industry, city, state, domain, website, source
# Handles multiple CSV formats (ZoomInfo export, X-ray batch, manual list)
```

---

## Validated Test Results (April 7, 2026)

### X-ray batch (3 Seattle manufacturing companies):

| Company | Initial Score | Tier 1 (Apollo) | Tier 2 (X-ray) | Tier 3 (Verify) | Final Score | Action |
|---------|--------------|-----------------|-----------------|-----------------|-------------|--------|
| SMC - Seattle Manufacturing Corp | 79 REVIEW | 0 contacts | 0 results (domain search correct) | — | 79 | REVIEW |
| Weyerhaeuser | 0 HARD EXCLUDE | 5 contacts (CFO + 4 Directors) | — | — | 0 | HARD EXCLUDE |
| TASC - Technical & Assembly | 86 PROCEED | 1 contact (Rikki Nelson, Controller) | — | — | 86 | PROCEED |

### Finance title scan (10 flagged companies, April 5):

| Company | Score | Apollo Found | Title | LinkedIn Verified |
|---------|-------|-------------|-------|-------------------|
| Johansen Construction | 79 | Sue Lewis | **CFO** | Yes — headline "CFO at Johansen Construction", 138 connections |
| Prospect Construction | 76 | Joseph Carr | **Controller** | Yes — headline "Controller at Prospect Construction", 418 connections |
| Ballard Industrial | 74 | Erin Flack | **Financial Controller** | Yes — 44 connections (ALSO has duplicate profile with 0 connections) |
| Custom Cones USA | 79 | Nicolle Atchison | **Director of Finance** | Yes — 386 connections |
| Wesmar Company | 77 | Michelle (last name TBD) | **Director of Finance** | X-ray did not find LinkedIn profile |
| Machinists Inc. | 77 | — | (none in Apollo) | Needs X-ray |
| Fisheries Supply | 77 | — | (none in Apollo) | Needs X-ray |
| Fairbank Construction | 79 | — | (none in Apollo) | Needs X-ray |
| Jumbo Foods | 79 | — | (none in Apollo) | Needs X-ray |
| Pacific Tool | 79 | — | (none in Apollo) | Needs X-ray |

### Known X-ray accuracy issues (fixed April 7):

| Issue | Example | Fix |
|-------|---------|-----|
| Ambiguous abbreviations | "SMC" matched SMC Corporation (Japan) | Domain-first search: use `"smcgear.com"` not `"SMC"` |
| Short company names | "Field" matched Field Aerospace | `_build_company_match_terms()` extracts distinctive words |
| Junk domains from Google Maps | Company "website" was facebook.com or instagram.com | `JUNK_DOMAINS` filter in `extract_domain()` |
| False positive contacts | X-ray returned CFOs at wrong companies | Tier 3 profile scrape verifies `currentPosition.companyName` |
