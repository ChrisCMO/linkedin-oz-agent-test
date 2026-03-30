# Agentic LinkedIn Outreach Pipeline — Complete Implementation Plan

> **Purpose:** This document contains everything a new Claude instance needs to build the agentic LinkedIn outreach pipeline in a new repo. Feed this file + the context docs listed at the bottom.

---

## 1. What We're Building

A Slack-triggered, email-approved, Oz-agent-executed LinkedIn outreach pipeline for VWC CPAs.

**The admin (yorCMO) talks to an agent in Slack. The client (CPA partners) clicks a button in an email. Everything else is autonomous.**

### End-to-End Flow

```
Admin in Slack: "@Oz send this list to Melinda" [attaches Excel]
  │
  ▼
Oz Agent (batch-sender skill):
  1. Parses Excel → imports prospects to Supabase (status: scored)
  2. Creates batch_review record with magic link token
  3. Generates HTML email with prospect cards + "Approve" button
  4. Sends email to Melinda via Microsoft Graph (Outlook)
  │
  ▼
Melinda receives email, reviews prospect cards, clicks "Approve & Start Connecting"
  │
  ▼
Supabase Edge Function (approve-batch):
  1. Validates token + batch_id
  2. Marks batch_review as completed
  3. Updates prospect statuses to 'approved'
  4. Triggers Oz invite-sender agent via API
  5. Returns confirmation page to Melinda
  │
  ▼
Oz Agent (invite-sender skill) — runs immediately + daily cron:
  1. Queries approved prospects ordered by ICP score
  2. Pre-flight: checks not already FIRST_DEGREE
  3. Sends bare invite (NO connection note) via Unipile
  4. Logs to activity_log
  5. Random delay 45-120s between invites
  6. Max 5/day, business hours only (8-18 PT, weekdays)
  │
  ▼
Oz Agent (acceptance-detector skill) — cron 3x/day:
  1. GET /api/v1/users/relations → build set of connected provider_ids
  2. Compare against 'sent' invitations in DB
  3. New connection found:
     a. Update invitation → accepted
     b. Update prospect → connected
     c. Create message records (msg1, msg2, msg3) from pre-generated text
     d. Send HTML notification email to partner via Outlook
        (email includes prospect details, messages, research, "Start Sequence" link)
  │
  ▼
Oz Agent (message-sender skill) — daily cron:
  1. Find prospects where msg is approved + scheduled_for has passed
  2. Check for replies first (GET /chats/{id}/messages)
  3. If reply → stop sequence, log reply_event, notify admin
  4. If no reply → send message via Unipile, update status
```

---

## 2. Architecture Diagram

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Admin    │    │ Oz Agent │    │ Client   │    │ Supabase │    │ Oz Agent │
│ (Slack)  │───►│ batch-   │───►│ email    │───►│ Edge Fn  │───►│ invite-  │
│ uploads  │    │ sender   │    │ Approve  │    │ approve  │    │ sender   │
│ Excel    │    │ → import │    │ button   │    │ -batch   │    │ 5/day    │
│          │    │ → email  │    │          │    │ → Oz API │    │ bare     │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                                      │
                     ┌────────────────────────────────────────────────┘
                     ▼
              ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
              │ Oz Agent     │    │ Client       │    │ Oz Agent     │
              │ acceptance-  │───►│ gets email   │───►│ message-     │
              │ detector     │    │ w/ messages  │    │ sender       │
              │ 3x/day cron  │    │ approves in  │    │ sends msg    │
              │ polls rels   │    │ web UI       │    │ sequence     │
              └──────────────┘    └──────────────┘    └──────────────┘
```

---

## 3. Data Model (Supabase Schema v3)

The schema already exists at `db/supabase_schema_v3.sql`. Key tables for this pipeline:

### Existing Tables Used

| Table | Role in Pipeline |
|-------|-----------------|
| `tenants` | VWC CPAs tenant with settings (timezone, business hours, brand voice) |
| `users` | Admin (yorCMO) + client (Melinda, Adrienne) users |
| `linkedin_accounts` | Melinda's and Adrienne's LinkedIn accounts via Unipile |
| `campaigns` | Active campaign with ICP config, timing, sender profile |
| `companies` | Deduplicated company data from Apollo |
| `prospects` | Central pipeline table — status machine, scoring, sequence state |
| `invitations` | Connection request tracking (replaces missing Unipile endpoint) |
| `messages` | Connection notes (step=0) + follow-ups (step=1,2,3) with HITL |
| `events` | Polymorphic event log (batch_approved, acceptance_detected, reply, etc.) |
| `activity_log` | Every API call — partitioned by month, used for rate limit enforcement |
| `batch_reviews` | Magic link batch review sessions (token_hash, prospect_ids, counts) |

### Batch Review Flow in DB

```sql
-- 1. Create batch_review when sending email
INSERT INTO batch_reviews (tenant_id, campaign_id, token_hash, prospect_ids,
                           total_count, sent_to_email, sent_by)
VALUES ($tenant, $campaign, sha256($raw_token), $prospect_id_array,
        $count, 'melinda@vwccpas.com', $admin_user_id);

-- 2. Edge Function marks approved when client clicks button
UPDATE batch_reviews SET completed_at = NOW(), approved_count = total_count
WHERE id = $batch_id AND token_hash = sha256($raw_token);

-- 3. Update prospects to approved
UPDATE prospects SET status = 'approved', status_changed_at = NOW()
WHERE id = ANY($prospect_ids) AND status IN ('scored', 'queued');
```

### Rate Limit Enforcement Query

```sql
-- Check today's invite count before sending
SELECT COUNT(*) FROM activity_log
WHERE linkedin_account_id = $account_id
  AND action_type = 'invite'
  AND success = TRUE
  AND created_at >= date_trunc('day', NOW());
```

### Acceptance Detection Query

```sql
-- Find pending invitations to check
SELECT i.*, p.linkedin_slug, p.first_name, p.last_name
FROM invitations i
JOIN prospects p ON p.id = i.prospect_id
WHERE i.linkedin_account_id = $account_id
  AND i.status = 'sent';

-- Mark accepted
UPDATE invitations SET status = 'accepted', accepted_at = NOW(),
  detection_method = 'poll_relations', chat_id = $chat_id
WHERE id = $invitation_id;

UPDATE prospects SET status = 'connected', status_changed_at = NOW()
WHERE id = $prospect_id;
```

---

## 4. Oz Environment Setup

```bash
# 1. Create environment
oz environment create \
  --name "vwc-linkedin-pipeline" \
  --docker-image warpdotdev/dev-base:latest-agents \
  --repo YorCMO/Linkedin-Testing \
  --setup-command "pip install -r requirements.txt" \
  --description "VWC LinkedIn outreach: batch email, bare invites, acceptance detection, message sequences"

# 2. Connect Slack
oz integration create slack --environment <ENV_ID>

# 3. Add secrets
oz secret create SUPABASE_URL --value "https://..."
oz secret create SUPABASE_SECRET_KEY --value "..."
oz secret create UNIPILE_DSN --value "https://api34.unipile.com:16495"
oz secret create UNIPILE_API_KEY --value "..."
oz secret create OPENAI_API_KEY --value "..."
oz secret create MICROSOFT_CLIENT_ID --value "..."
oz secret create MICROSOFT_CLIENT_SECRET --value "..."
oz secret create MICROSOFT_TENANT --value "..."
oz secret create MICROSOFT_SENDER_EMAIL --value "ai_team@yorcmo.com"

# 4. Create scheduled agents
oz schedule create \
  --name "VWC Daily Invites" \
  --cron "0 10 * * 1-5" \
  --environment <ENV_ID> \
  --skill "YorCMO/Linkedin-Testing:invite-sender" \
  --prompt "Send bare invites for approved prospects. Max 5/day, 45-120s delays, business hours."

oz schedule create \
  --name "VWC Acceptance Detector" \
  --cron "0 9,13,17 * * 1-5" \
  --environment <ENV_ID> \
  --skill "YorCMO/Linkedin-Testing:acceptance-detector" \
  --prompt "Poll relations for all active accounts. Detect accepted invitations. Send notification emails."

oz schedule create \
  --name "VWC Message Sender" \
  --cron "0 11 * * 1-5" \
  --environment <ENV_ID> \
  --skill "YorCMO/Linkedin-Testing:message-sender" \
  --prompt "Send due messages for connected prospects. Check for replies first. Stop sequence on reply."
```

---

## 5. Oz Skills Needed

### 5a. batch-sender

**Trigger:** Slack message "@Oz send this list to Melinda" + Excel attachment

**Steps:**
1. Parse Excel/CSV attachment
2. For each row: upsert company + prospect in Supabase (status=scored)
3. Generate token, create batch_review record
4. Build HTML email with prospect cards (template below)
5. Send via Microsoft Graph API
6. Confirm in Slack: "Sent batch #X (Y prospects) to melinda@vwccpas.com"

### 5b. invite-sender

**Trigger:** Oz API call from Edge Function + daily cron

**Steps:**
1. Query `daily_usage_summary` — if connections >= 5 today, exit
2. Query weekly total — if >= 20 this week, exit
3. Get approved prospects ordered by ICP score
4. For each (max 5 per run):
   - `GET /api/v1/users/{slug}?account_id=X` — check network_distance
   - If FIRST_DEGREE → mark connected, skip invite
   - `POST /api/v1/users/invite` with `account_id` + `provider_id` (NO message field)
   - Insert invitation record in Supabase
   - Update prospect status → invite_sent
   - Log to activity_log
   - `sleep(random 45-120s)`

### 5c. acceptance-detector

**Trigger:** Cron 3x/day (9 AM, 1 PM, 5 PM PT weekdays)

**Steps:**
1. For each active linkedin_account:
   - `GET /api/v1/users/relations?account_id=X` — paginate through all
   - Build set of connected provider_ids
2. Query all 'sent' invitations for this account
3. For each pending invitation:
   - If provider_id in connected set → acceptance detected
   - Update invitation → accepted
   - Update prospect → connected
   - Create message records (step 1,2,3) from pre-generated text
   - Send acceptance notification email via Outlook
4. Log all detections to events table

### 5d. message-sender

**Trigger:** Daily cron (11 AM PT weekdays)

**Steps:**
1. Find messages where status='approved' and scheduled_for <= now
2. For each:
   - `GET /api/v1/chats/{chat_id}/messages` — check for replies
   - If reply found (is_sender: 0):
     - Stop sequence, update prospect → replied
     - Log reply_event, notify admin
   - If no reply:
     - `POST /api/v1/chats/{chat_id}/messages` with approved_text
     - Update message → sent
     - Schedule next message (msg2 in 14 days, msg3 in 14 days)
   - `sleep(random 45-120s)`

---

## 6. HTML Email Templates

### 6a. Batch Review Email (sent to client)

Key elements:
- Header with batch number, prospect count, partner name
- Prospect cards with: name, title, company, ICP score badge, industry, location, activity level, LinkedIn profile link, first message preview
- Large green "Approve & Start Connecting" button (links to Edge Function)
- Footer with safety explanation (bare invites, ~5/day, business hours)
- "Hold" instructions (reply to email or contact yorCMO)

**Button URL format:**
```
https://<project>.supabase.co/functions/v1/approve-batch?batch_id={batch_id}&token={raw_token}
```

### 6b. Acceptance Notification Email (sent on connection acceptance)

Reuse the existing `_build_acceptance_html()` pattern from `mvp/backend/services/email_svc.py`:
- Prospect profile (name, title, headline, company, score, LinkedIn link)
- Company research sidebar content
- ICP reasoning
- 3 proposed messages (editable in web UI)
- "Review Messages" button linking to web UI

---

## 7. Supabase Edge Function

**Already built:** `supabase/functions/approve-batch/index.ts`

**What it does:**
1. Receives GET request with `batch_id` + `token` params
2. Hashes token, validates against `batch_reviews.token_hash`
3. Checks expiration (14 days default)
4. Idempotent — second click shows same confirmation
5. Marks batch approved, updates prospect statuses
6. Logs event to `events` table
7. Triggers Oz agent via `POST https://app.warp.dev/api/v1/agent/run` (fire-and-forget)
8. Returns styled HTML confirmation page

**Deploy:**
```bash
supabase functions deploy approve-batch
```

**Required secrets (in Supabase dashboard → Edge Functions → Secrets):**
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `WARP_API_KEY`
- `OZ_ENVIRONMENT_ID`

---

## 8. Safety Rules (Non-Negotiable)

### LinkedIn Rate Limits (Free Account)

| Action | Daily Max | Weekly Max | Delay Between |
|--------|-----------|------------|---------------|
| Connection requests | 5 | 20 | 45-120s random |
| Messages | 50 | - | 45-120s random |
| Profile views | 80 | - | 30-90s random |

### Connection Requests

- **BARE INVITES ONLY** — no connection notes (free accounts limited to ~5 notes/month)
- Business hours only: 8 AM - 6 PM Pacific, Monday-Friday
- Pre-flight check: verify not already FIRST_DEGREE before every invite
- Log every action to `activity_log` for enforcement

### 4-Week Ramp-Up (New/Reconnected Accounts)

```
Week 1: 3 invites/day
Week 2: 4 invites/day
Week 3: 5 invites/day
Week 4+: 5 invites/day (baseline)
```

7-day cooldown after account reconnection (zero actions).

### Acceptance Detection

- Primary: Poll `GET /api/v1/users/relations` 2-3x/day
- Backup: LinkUp API for stale invitations (>7 days)
- `new_message` webhook does NOT work for bare invites (no note = no chat created on accept)
- `new_relation` webhook has up to 8hr delay — polling is more reliable

### Reply Detection

- Before every follow-up: `GET /api/v1/chats/{chat_id}/messages`
- Reply = any message where `is_sender: 0`
- On reply: STOP sequence immediately, update prospect → replied, notify admin

---

## 9. Unipile API Quick Reference

**Base URL:** `https://api34.unipile.com:16495`
**Auth:** `X-API-KEY: {api_key}` header

### Endpoints Used

```
GET  /api/v1/accounts                              — Account health check
GET  /api/v1/users/{slug}?account_id=X&linkedin_sections=*  — Profile lookup
GET  /api/v1/users/relations?account_id=X          — List connections (for acceptance detection)
POST /api/v1/users/invite                          — Send connection request (bare, no message)
     Body: { "account_id": "...", "provider_id": "..." }
GET  /api/v1/chats?account_id=X                    — List chats
GET  /api/v1/chats/{id}/messages                   — Read messages (reply detection)
POST /api/v1/chats                                 — Start new chat (msg1 after acceptance)
     Body: { "account_id": "...", "text": "...", "attendees_ids": ["provider_id"] }
POST /api/v1/chats/{id}/messages                   — Send follow-up (msg2, msg3)
     Body: { "text": "..." }
```

### Known Gaps

- `GET /api/v1/users/invitations/sent` → **404 (does not exist)**
- Acceptance detection for bare invites requires polling relations (no webhook)

### Account Info

- **Laikah Mangahas** (test): Account ID `QL9z53hhSgebyLbTxcrOSw`
- Provider ID: `ACoAAGVLvUwBEiaDF9XHoQegCkdaeQxMdqDovUQ`

---

## 10. Microsoft Graph (Outlook Email)

**Existing integration:** `lib/outlook.py` — `OutlookClient` class

```python
from lib.outlook import OutlookClient
outlook = OutlookClient()
outlook.send_email(
    to="melinda@vwccpas.com",
    subject="10 new prospects ready — Batch #3",
    html_body=html_content,
    cc="christopher@yorcmo.com",
)
```

**Auth:** MSAL client credentials flow (app permission: Mail.Send)
**Sender:** `ai_team@yorcmo.com`

---

## 11. CSV Format (Input Data)

The master list is an Excel/CSV with these columns:

```
Company ICP Score, Pipeline Action, Company, Industry, Company Location,
Company LinkedIn URL, Company LI Followers, First Name, Last Name, Title,
Seniority, LinkedIn URL, LinkedIn Headline, Role Verified, LinkedIn Connections,
LinkedIn Followers, Open to Work, Email, Email Status,
Melinda's Connection Note, Adrienne's Connection Note,
Message 1 - Melinda, Message 2 - Melinda, Message 3 - Melinda,
Message 1 - Adrienne, Message 2 - Adrienne, Message 3 - Adrienne,
Data Source, Activity Level, Recent Post Date, Posts Count,
LinkedIn Active Status, Activity Score, Activity Recommendation,
Days Since Last Activity, Activity Insights
```

- Filter: only `Pipeline Action = "PROCEED"` rows
- Sort by `Company ICP Score` descending (best prospects first)
- Messages are pre-generated per partner (Melinda and Adrienne versions)
- Connection notes exist but are **NOT USED** (bare invites only)

---

## 12. Edge Cases & Handling

| Scenario | Behavior | Pattern |
|---|---|---|
| Client clicks Approve twice | Idempotent — shows same confirmation | Idempotency |
| Approval token expired (>14 days) | Shows "expired" page with contact info | TTL check |
| Excel has duplicate LinkedIn URLs | Deduplicate on import by linkedin_slug | Validation |
| Prospect already FIRST_DEGREE | Skip invite, mark connected | Pre-flight |
| Daily invite limit reached | Stop, resume next business day | Rate limit |
| Account disconnected | Pause all batches, alert admin in Slack | Circuit breaker |
| Edge Function timeout | Oz trigger is fire-and-forget, returns immediately | Async |
| Prospect replied between msg1 and msg2 | Stop sequence, don't send msg2 | Reply check |
| Weekend approve click | Batch marked approved, but invites only send on weekday cron | Time gate |
| No LinkedIn slug in CSV row | Skip row, log warning | Graceful degradation |
| Oz agent run fails | Batch stays approved, cron picks up remaining on next run | Retry via cron |

---

## 13. Testing Plan

### Phase 1: Prove the Agent Works
1. Import 2-3 test prospects manually to Supabase
2. Create a test batch_review with magic link
3. Click the Edge Function URL — verify it marks approved + triggers Oz
4. Run invite-sender manually — verify bare invite sent via Unipile
5. Run acceptance-detector — verify it detects test connections
6. Verify email notification sent on acceptance

### Phase 2: End-to-End
1. Say "@Oz send this to Laikah" in Slack with test CSV (3 prospects)
2. Verify email arrives with prospect cards
3. Click Approve button
4. Verify invites sent over 1-2 days
5. Accept from test LinkedIn account
6. Verify acceptance email with messages
7. Approve messages, verify delivery

---

## 14. Context Docs for New Claude Instance

Feed these files to the new Claude to give it full context:

### Required (Must Have)

| File | What It Contains |
|------|-----------------|
| **This file** (`docs/agentic-pipeline-plan.md`) | Complete implementation plan |
| `db/supabase_schema_v3.sql` | Full Supabase schema (11 tables) |
| `db/batch_reviews_migration.sql` | batch_reviews table DDL |
| `supabase/functions/approve-batch/index.ts` | Edge Function (already built) |
| `lib/outlook.py` | Microsoft Graph email client |
| `lib/unipile.py` | Unipile API client |
| `lib/apollo.py` | Apollo API client (for future enrichment) |
| `CLAUDE.md` (both parent + Linkedin-Testing) | Agent instructions, architecture, safety rules |

### Recommended (For Deeper Context)

| File | What It Contains |
|------|-----------------|
| `mvp/backend/services/email_svc.py` | Existing acceptance email template + Outlook integration |
| `mvp/backend/services/scoring.py` | ICP scoring logic |
| `mvp/backend/services/outreach_svc.py` | Existing invite/message sending logic |
| `docs/unipile/00-validation-report.md` | API endpoint test results |
| `docs/unipile/08-acceptance-detection.md` | 4-layer acceptance detection strategy |
| The CSV itself | Sample prospect data structure |

### Oz Skill Reference

| File | What It Contains |
|------|-----------------|
| `.claude/skills/unipile/SKILL.md` | Unipile safety rules + pre-flight checks |
| `.claude/skills/icp-prospect-pipeline/SKILL.md` | ICP pipeline steps |

---

## 15. Environment Variables

```env
# Supabase
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SECRET_KEY=eyJ...

# Unipile
UNIPILE_DSN=https://api34.unipile.com:16495
UNIPILE_API_KEY=...

# Microsoft Graph (Outlook)
MICROSOFT_CLIENT_ID=...
MICROSOFT_CLIENT_SECRET=...
MICROSOFT_TENANT=...
MICROSOFT_SENDER_EMAIL=ai_team@yorcmo.com

# OpenAI (for scoring + message generation)
OPENAI_API_KEY=...

# Apollo (for enrichment)
APOLLO_API_KEY=...

# Warp Oz
WARP_API_KEY=wk-...
OZ_ENVIRONMENT_ID=...
```
