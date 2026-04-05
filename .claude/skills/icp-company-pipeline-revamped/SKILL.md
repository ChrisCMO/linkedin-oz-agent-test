---
name: icp-company-pipeline-revamped
description: Run the revamped company-first ICP prospect pipeline for VWC CPAs with v2 scoring. This pipeline adds organizational complexity scoring (CFO/Controller detection), revenue mismatch handling, contact activity classification, and client blacklist checks. Use this skill when asked to "run revamped pipeline", "v2 scoring", "rescore companies", "find CFOs at flagged companies", "organizational complexity scoring", or when someone wants the improved pipeline with finance title scanning and activity filtering.
---

# ICP Company Pipeline — Revamped (v2 Scoring)

This is the improved version of the company-first prospect pipeline. It adds finance title detection, revenue mismatch handling, organizational complexity scoring, contact activity classification, and a client blacklist. The original `icp-prospect-company-pipeline` skill is preserved unchanged.

## Why v2

After the April 4, 2026 client review, Chad and the VWC partners identified gaps in v1:

1. **CFO/Controller = complexity signal.** A property manager at a great company scored low because there was no way to signal organizational complexity. Chad said: "if they have a CFO or Controller, that indicates increased complexity."
2. **Revenue is unreliable.** Machinists Inc showed $1M revenue with 83 employees. Adrienne confirmed data is wrong. Revenue shouldn't tank a score when employees tell a different story.
3. **Inactive contacts are a waste.** Michael Turner (1 connection, no activity) shouldn't be messaged.
4. **Existing clients must be excluded.** Anthony's Homeport is a VWC client for 401K — can't contact them.

## The 14-Step Pipeline

```
Step 1:   COMPANY DISCOVERY              Google Places + LinkedIn X-ray (free/$0.01)
Step 1b:  CLIENT BLACKLIST CHECK          Exclude known VWC clients  ← NEW
Step 2:   COMPANY LINKEDIN ENRICHMENT     Apify company page scraper ($0.002/company)
Step 3:   COMPANY DATA ENRICHMENT         Apollo org enrich ($1/company)
Step 3b:  FINANCE TITLE SCAN              Apollo search (free) + Google X-ray fallback ($0.01)  ← NEW
Step 3c:  EXTERNAL DATA OVERLAY           PSBJ list cross-reference for revenue  ← NEW
Step 3d:  REVENUE MISMATCH DETECTION      Flag suspect revenue vs employee count  ← NEW
Step 4:   COMPANY-LEVEL ICP SCORING       score_companies_v2() with organizational complexity
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
Step 12:  OUTPUT                          CSV with v1 vs v2 comparison
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

## Step 3b: Finance Title Scan

**Goal:** Before scoring, detect if a company has a CFO, Controller, or other finance leadership. This feeds the `organizational_complexity` scoring dimension.

### Tier 1: Free Apollo people search ($0)

```python
from lib.apollo import ApolloClient
apollo = ApolloClient()
result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
    "q_organization_domains_list": [company["domain"]],
    "person_titles": ["CFO", "Chief Financial Officer", "Controller",
                      "VP Finance", "Director of Finance"],
    "per_page": 5,
})
```

Tested on 10 flagged companies: **50% hit rate** (5/10 had finance contacts in Apollo).

### Tier 2: Google X-ray via Apify (~$0.01/company, only if Tier 1 returns 0)

**Actor:** `nFJndFXA5zjCTuudP` (Google SERP)

```python
queries = [
    f'site:linkedin.com/in "{company_name}" CFO',
    f'site:linkedin.com/in "{company_name}" "chief financial officer"',
    f'site:linkedin.com/in "{company_name}" controller',
    f'site:linkedin.com/in "{company_name}" "director of finance"',
]
payload = {
    "queries": "\n".join(queries),
    "maxPagesPerQuery": 1,
    "resultsPerPage": 5,
    "countryCode": "us",
}
```

Parse LinkedIn URLs from results. Extract name from title text ("Sue Lewis - Johansen Construction" → first_name: Sue, last_name: Lewis).

### Tier 3: Apify profile scrape for verification ($0.002/profile)

**Actor:** `LpVuK3Zozwuipa5bp`

```python
results = run_actor("LpVuK3Zozwuipa5bp", {"urls": [linkedin_url]})
# Returns: headline, currentPosition (title + company), connectionsCount
```

Confirms the contact actually holds the finance title at the target company. Also catches duplicate profiles (Erin Flack at Ballard Industrial had TWO LinkedIn profiles — one with 44 connections, one with 0).

### Output columns:
- `finance_contact_first_name`
- `finance_contact_last_name`
- `finance_contact_title` — exact title (CFO, Controller, etc.)
- `finance_contact_linkedin_url` — full LinkedIn profile URL
- `has_cfo` — boolean
- `has_controller` — boolean
- `finance_titles_found` — comma-separated list of all finance titles

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

## Cost Summary

| Step | API | Cost | When |
|------|-----|------|------|
| 1b | None | $0 | All companies |
| 3b Tier 1 | Apollo search | $0 | All companies with domain |
| 3b Tier 2 | Apify SERP | ~$0.01/company | Only if Tier 1 = 0 results |
| 3b Tier 3 | Apify profile | ~$0.002/profile | Only to verify X-ray results |
| 3c | None | $0 | All companies |
| 3d | None | $0 | All companies |
| 4 | OpenAI GPT-5.4 | ~$0.01/company | All companies |
| 6b | None | $0 | All contacts |
| 8b | None | $0 | All contacts |

**Total new cost per company:** ~$0.01-$0.02 (mostly OpenAI scoring + occasional X-ray)

---

## Key Files

| File | What |
|------|------|
| `mvp/backend/services/scoring.py` | `score_companies_v2()`, `classify_contact_activity()`, `detect_revenue_mismatch()` |
| `data/blacklist.csv` | Client exclusion list |
| `scripts/test_revamped_scoring.py` | Test script for v2 scoring on flagged companies |
| `docs/deliverables/week2/scored/new/v2_scoring_comparison.csv` | Output: v1 vs v2 comparison |
| `docs/deliverables/week2/universe/private/psbj_family_owned_wa_2026_86.csv` | PSBJ family-owned companies list |

---

## Validated Test Results (April 5, 2026)

Finance title scan tested on 10 flagged companies:

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
