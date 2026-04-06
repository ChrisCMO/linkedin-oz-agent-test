# Plan: Prospect Pipeline — Contact Discovery + Enrichment + Messages

## Context

PROCEED companies are in the Companies Universe but have no contacts. We need a new Oz skill (`skills/prospect_enricher.py`) that discovers contacts at selected PROCEED companies, enriches them, validates LinkedIn activity, scores them, and generates outreach messages. A new dashboard page ("Prospect Pipeline") lets the admin select which companies to process and monitor progress.

## Happy Path

```
1. Admin opens Prospect Pipeline page
2. Sees PROCEED companies from Companies Universe
3. Selects specific companies → clicks "Discover & Enrich Contacts"
4. Dashboard calls /api/trigger-prospect-enrichment with company IDs
5. Oz runs: python3 -m skills.prospect_enricher --tenant-id X --company-ids ID1,ID2
6. Per company:
   a. Apollo people search (free) → find CFO/Controller/President contacts
   b. ZoomInfo cross-match (free) → zip code validated contacts
   c. X-ray fallback ($0.01) → for companies with 0 contacts
   d. Profile scrape ($0.003) → verify still at company, get headline
   e. Apollo person enrich ($1.00) → email, full employment history
   f. Activity Index ($0.002) → activity score 1-10
   g. Contact-level scoring ($0.01) → score each person 0-100
   h. Connection notes generation → Adrienne + Melinda versions
   i. 3-message sequence generation
7. Results written to prospects table in Supabase
8. Dashboard shows contacts appearing per company
9. Admin clicks "Send for Review" → batch email to Adrienne/Melinda
10. Client approves → invite-sender cron picks them up
```

## Files to Create

### 1. `skills/prospect_enricher.py` — Oz skill (new)

Main entry: `python3 -m skills.prospect_enricher --tenant-id X [--company-ids ID1,ID2] [--limit N]`

**Pipeline per company:**
```
Phase 1: Contact Discovery — 4-tier (free/$0.04 per company)
  Tier 1: Apollo people search by domain (free) → structured data + Apollo ID
  Tier 2: ZoomInfo contact search (free, SEARCH ONLY — no enrichment credits)
         → companyName + zipCode + jobTitle filter
         → returns: firstName, lastName, jobTitle, contactAccuracyScore, ZI Contact ID
         → DO NOT use: companyId-based search, domain search, or enrichment endpoints
         → Then Apollo cross-match (q_keywords: "First Last CompanyName") to get Apollo ID
  Tier 3: Google X-ray ($0.04) → for companies where Tier 1+2 returned 0
         → domain-first search + company name matching
  Tier 4: Profile scrape verification ($0.003/contact) → ALL contacts from Tiers 1-3
         → verify person works at target company + title matches
         → catches stale ZoomInfo data + X-ray false positives
  → Dedup by LinkedIn URL + name

Phase 2: Person Enrichment ($1/person)
  → Apollo person enrich by apollo_id
  → Returns: email, employment history, LinkedIn URL

Phase 3: LinkedIn Validation ($0.007/person)
  → Profile scraper: verify current role
  → Activity Index: score 1-10, posts, reactions
  → classify_contact_activity() → ACTIVE/INACTIVE tag

Phase 4: Contact Scoring ($0.01/person)
  → score_prospects() from scoring.py
  → 0-100 score per contact

Phase 5: Message Generation ($0.03/person)
  → generate_connection_note() → ≤200 chars, Adrienne + Melinda versions
  → generate_messages() → 3-message follow-up sequence
```

**Reads from:** `companies_universe` (PROCEED companies by ID)
**Writes to:** `prospects` table

**Prospect record created per contact:**
```python
{
    "tenant_id": tenant_id,
    "campaign_id": DEFAULT_CAMPAIGN_ID,
    "first_name": "Rikki",
    "last_name": "Nelson",
    "title": "Controller",
    "seniority": "director",
    "email": "rikki@tasc-wa.com",
    "email_status": "valid",
    "linkedin_url": "https://linkedin.com/in/rikkinelson1010",
    "linkedin_slug": "rikkinelson1010",
    "headline": "Controller at TASC",
    "location": "Seattle, WA",
    "company_name": "TASC",
    "company_linkedin_url": "https://linkedin.com/company/tasc...",
    "company_universe_id": "uuid-of-tasc-in-universe",
    "status": "scored",
    "source": "apollo",
    "icp_score": 84,
    "activity_score": 7,
    "activity_level": "Active",
    "activity_recommendation": "Good candidate for outreach",
    "posts_last_30_days": 3,
    "reactions_last_30_days": 12,
    "last_activity_date": "2026-04-01",
    "days_since_last_activity": 6,
    "linkedin_connections": 418,
    "linkedin_active_status": "ACTIVE",
    "role_verified": true,
    "apollo_person_id": "abc123",
    "connection_notes": '{"adrienne": "Hi Rikki, ...", "melinda": "Hi Rikki, ..."}',
    "partner_messages": '{"adrienne": {"msg1": "...", "msg2": "...", "msg3": "..."}, "melinda": {...}}',
    "data_source": "apollo+zoominfo+xray",
    "contact_batch_name": "prospect_enrichment_2026-04-07",
}
```

**Reuses:**
- `lib/apify.py` — `run_actor()`, `extract_domain()`, actor constants
- `lib/xray.py` — `xray_discover_finance_contacts()`, `xray_find_contact_linkedin()`
- `lib/apollo.py` — `ApolloClient.search_people()`, `ApolloClient.enrich_person()`
- `mvp/backend/services/scoring.py` — `score_prospects()`, `classify_contact_activity()`
- `db/connect.py` — `get_supabase()`

### 2. `mvp/backend/services/message_gen_svc.py` — Message generation (new)

**Does not exist yet.** Needs to be created.

```python
def generate_connection_note(prospect: dict, company: dict, sender_name: str) -> str:
    """Generate ≤200 char LinkedIn connection note."""
    # Uses OpenAI GPT-5.4
    # FROM sender TO prospect, referencing company
    # Warm, professional, not salesy, no VWC mention

def generate_messages(prospect: dict, company: dict, icp_config: dict | None = None) -> dict:
    """Generate 3-message follow-up sequence."""
    # Returns {"msg1": "...", "msg2": "...", "msg3": "..."}
    # msg1: after connection accepted, reference role/company
    # msg2: ~2 weeks later, different angle
    # msg3: ~4 weeks, final light touch
```

### 3. `dashboard/src/app/api/trigger-prospect-enrichment/route.ts` — API route (new)

Pattern matches `trigger-scoring` route:
```typescript
POST /api/trigger-prospect-enrichment
Body: { tenantId: string, companyIds: string[] }
→ Counts companies to process
→ Calls Oz: python3 -m skills.prospect_enricher --tenant-id X --company-ids ID1,ID2
→ Returns { runId, companyCount, message }
```

### 4. `dashboard/src/app/clients/[tenantId]/prospect-pipeline/page.tsx` — Dashboard page (new)

**Layout:**
- Header: "Prospect Pipeline" + "Discover & Enrich" button
- Status cards: Companies Queued, Discovering, Enriched, Ready for Review
- Table: PROCEED companies from companies_universe
  - Columns: Checkbox, Name, Industry, ICP Score, Revenue, Location, Contacts Found, Status
  - Clicking a row → shows contacts found at that company (or navigates to company detail)
- Selected companies → "Discover & Enrich Contacts" button appears
- Cost estimate before triggering: "~$2.10/contact × estimated 3 contacts/company × N companies"

### 5. Sidebar update

Add "Prospect Pipeline" between "Raw Pipeline" and "Contacts":
```
Raw Pipeline       ← company scoring
Prospect Pipeline  ← NEW: contact discovery + enrichment  
Contacts           ← final enriched contacts
```

**File:** `dashboard/src/app/clients/[tenantId]/layout.tsx`

## Edge Cases & Handling

| Scenario | Expected Behavior | Pattern |
|---|---|---|
| Apollo returns 0 contacts for a company | Try ZoomInfo, then X-ray fallback | Tiered fallback |
| Apollo credits exhausted mid-pipeline | Save progress, stop, report error | Early exit + progress save |
| ZoomInfo data is stale (person left) | Profile scrape rejects, mark as stale | Tier 3 verification |
| X-ray returns wrong company contacts | Profile scrape + company name matching rejects them | Tier 3 verification |
| Contact has no LinkedIn URL | Skip LinkedIn validation, mark as "unverified" | Graceful degradation |
| Contact is INACTIVE (1 connection, no posts) | Still create prospect but tag as INACTIVE, exclude from outreach queue | Status tagging |
| Duplicate contacts (same person at same company) | Dedup by LinkedIn URL, keep one with most connections | Dedup before insert |
| Company already has contacts in prospects table | Skip discovery, show existing contacts | Idempotency check |
| message_gen_svc generates note > 200 chars | Retry with stricter prompt, truncate as last resort | Retry with constraint |
| Two admins trigger enrichment on same company | Check if company already being processed (status = "discovering") | Idempotency |

## Non-Functionals

- **Timeout:** Each company ~30-60s for discovery + enrichment. 10 companies = ~5-10 min Oz run.
- **Cost:** ~$2.10/contact × ~3 contacts/company = ~$6.30/company. 10 companies = ~$63.
- **Model:** GPT-5.4 for scoring + message generation. No cheaper model — quality matters for outreach.
- **Observability:** Each phase logs to console (Oz captures). Enrichment errors stored in `enrichment_data` JSONB on prospects table.
- **Progress:** Write each prospect to DB as soon as enriched (don't batch). Dashboard polls every 5s.

## Files to Create/Modify

| File | Action |
|------|--------|
| `skills/prospect_enricher.py` | **Create** — main Oz skill |
| `mvp/backend/services/message_gen_svc.py` | **Create** — connection notes + message sequences |
| `dashboard/src/app/api/trigger-prospect-enrichment/route.ts` | **Create** — API route |
| `dashboard/src/app/clients/[tenantId]/prospect-pipeline/page.tsx` | **Create** — dashboard page |
| `dashboard/src/app/clients/[tenantId]/layout.tsx` | **Edit** — add sidebar link |
| `companies_universe` table | **Edit** — add `contacts_enrichment_status` column (or track in enrichment_data) |

## Verification

1. Upload 3 test companies → score → promote to universe
2. Open Prospect Pipeline page → see 3 PROCEED companies
3. Select TASC → click "Discover & Enrich Contacts"
4. Oz runs → finds Rikki Nelson (Controller) via Apollo
5. Enriches: email, LinkedIn profile, activity score
6. Scores: 84 (contact-level)
7. Generates: connection notes (Adrienne + Melinda), 3 messages each
8. Prospect appears in Contacts page with all fields populated
9. Company detail page shows "1 contact found" with Rikki Nelson in table
