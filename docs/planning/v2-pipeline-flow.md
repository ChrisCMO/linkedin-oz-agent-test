# V2 Pipeline Flow — Company Scoring to Outreach

## End-to-End Flow

```
                         RAW COMPANIES (6,137)
                    ZoomInfo + LinkedIn X-ray discovery
                                |
                    +-----------+-----------+
                    |                       |
              Has LinkedIn URL?        No LinkedIn URL
                    |                       |
                    v                       v
        Step 2: LinkedIn Company        Skip enrichment
        Page Scrape (Apify)             (score with minimal data)
                    |
                    v
        Step 3: Apollo Org Enrich
        ($1/company, by domain)
                    |
         +---------+---------+
         |                   |
    Has domain?         No domain (junk filtered)
         |                   |
         v                   v
    Step 3b Tier 1:     Score with
    Apollo Finance      available data
    Search (FREE)       only
         |
         v
    Step 3d: Revenue
    Mismatch Detection
         |
         v
    Step 4: V2 SCORING (all companies)
    7 dimensions including organizational_complexity
         |
    +----+----+----+----+
    |         |         |         |
  PROCEED   REVIEW    SKIP    HARD EXCLUDE
  (80+)    (60-79)   (1-59)     (0)
    |         |         |         |
    |         v         |      Excluded:
    |    Step 4b:       |      Public,
    |    X-ray Rescue   |      PE-backed,
    |    (Tier 2+3)     |      >$150M rev,
    |         |         |      Banking,
    |    +----+----+    |      Government
    |    |         |    |
    |  Found     Not    |
    |  contacts  found  |
    |    |         |    |
    |  RESCORE    |    |
    |    |         |    |
    | +--+--+     |    |
    | |     |     |    |
    |PROCEED|  REVIEW  |
    | |     |  (stays) |
    | |     |     |    |
    v v     |     v    v
  +---------+----+--------+
  |  ALL PROCEED          |
  |  (original + rescued) |
  +-----------+-----------+
              |
              v
    /icp-prospect-pipeline
    Phase 3: Full Contact
    Discovery + Enrichment
    ($2.10/person)
              |
              v
    Dashboard: Company detail
    + Contacts table
    (like RAM Mounts screenshot)
              |
              v
    Email to client for review
    → Client approves batch
    → Oz sends LinkedIn invites
```

## Pipeline Actions Explained

| Action | Score Range | What Happens | Cost Spent |
|--------|-----------|--------------|------------|
| **PROCEED** | 80+ | Promoted to Companies Universe → contact discovery → enrichment → outreach | Full |
| **REVIEW** | 60-79 | X-ray rescue attempted → if contacts found, rescore → may become PROCEED. Otherwise stays for Adrienne/CMO manual review | Partial (X-ray only) |
| **SKIP** | 1-59 | Too far from threshold. No further processing. | Minimal (scoring only) |
| **HARD EXCLUDE** | 0 | Public company, PE-backed, banking, government, >$150M revenue. Permanently excluded. | Minimal (scoring only) |

## Dashboard Views Per Stage

### Raw Pipeline Page
- **Status cards:** Raw → Enriched → Scored → Errors → Ready to Promote
- **Table:** All raw companies with status, ICP score, action
- **Expandable rows:** Domain, website, LinkedIn, enrichment data, finance contacts
- **Buttons:** "Enrich & Score" (triggers Oz), "Promote PROCEED" (moves to universe)

### Companies Page (Universe)
- **Only PROCEED companies** (promoted from Raw Pipeline)
- **Company detail view** (like RAM Mounts screenshot):
  - Identity: domain, website, LinkedIn, category, source
  - Firmographics: industry, location, employees, revenue, ownership
  - LinkedIn Profile: followers, tagline, description, founded
  - ICP Scoring: score badge, score breakdown, reasoning
  - Contacts table: name, title, email, status, activity, ICP score

### Contacts Page
- **All prospects** across all PROCEED companies
- **Filters:** status, activity level, seniority
- **Status flow:** scored → approved → invite_sent → connected → msg1_sent → ... → completed/replied

## Where Finance Contacts Live

Finance contacts discovered during company scoring (Phase 1-2) are stored as:

**On the `raw_companies` record:**
- Columns in the output CSV: `Finance Contact 1-5 Name/Title/LinkedIn URL`
- In DB: could be stored in `source_data` JSONB or dedicated columns on `raw_companies`

**When promoted to Companies Universe:**
- Finance contacts become seed data for `/icp-prospect-pipeline`
- The prospect pipeline discovers ADDITIONAL contacts beyond just finance titles
- All contacts go into the `prospects` table linked by `company_universe_id`

## Cost Breakdown Per Phase

| Phase | What | Cost | Companies |
|-------|------|------|-----------|
| Phase 1 | LinkedIn scrape + Apollo org enrich + Apollo finance search + scoring | ~$1.05/company | ALL raw companies |
| Phase 2 | X-ray Tier 2 + profile scrape verification + rescore | ~$0.04/company | REVIEW companies with 0 contacts only |
| Phase 3 | Full contact discovery + enrichment + activity + messages | ~$2.10/person | PROCEED companies only |

**Example with 250 test companies:**
- Phase 1: 250 × $1.05 = ~$262
- Phase 2: ~50 REVIEW × $0.04 = ~$2
- Phase 3: ~100 PROCEED × 3 contacts × $2.10 = ~$630
- **Total: ~$894 for 250 companies**

## Validated Test Results (April 7, 2026)

### 3-Tier Finance Scan Accuracy

| Tier | What | Hit Rate | False Positive Rate |
|------|------|----------|-------------------|
| Tier 1: Apollo search | Free people search by domain | ~50% (5/10 flagged companies had finance contacts) | Low — domain scoping is precise |
| Tier 2: Google X-ray | `site:linkedin.com/in` search | Varies — depends on company name uniqueness | **High without fixes** — "SMC" matched wrong companies |
| Tier 3: Profile scrape | Verify current company + title | N/A (verification step) | Catches Tier 2 false positives |

### X-ray Accuracy Fixes Applied

| Fix | Before | After |
|-----|--------|-------|
| Domain-first search | `"SMC" CFO` → 2 wrong CFOs | `"smcgear.com" CFO` → 0 results (correct) |
| Company name parsing | Used full name with abbreviation | Extracts distinctive part: "seattle manufacturing" |
| Match term validation | Checked first word only | Checks multiple distinctive terms |
| Junk domain filter | facebook.com used as company domain | Filtered out, falls back to name search |
| Profile scrape verification | Trusted X-ray results blindly | Scrapes live profile, rejects wrong company |
