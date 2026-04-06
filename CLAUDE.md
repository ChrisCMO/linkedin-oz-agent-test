# CLAUDE.md — VWC LinkedIn Outreach Pipeline

## What This Is

Agentic LinkedIn outreach pipeline for VWC CPAs. Slack-triggered, email-approved, Oz-agent-executed.

Admin uploads prospect list in Slack → client approves via email → invites sent automatically → acceptances detected → follow-up messages sent.

## Quick Start

```bash
pip install -r requirements.txt

# Run individual skills
python -m skills.batch_sender --file data.xlsx --name "Melinda" --email melinda@vwccpas.com
python -m skills.invite_sender
python -m skills.acceptance_detector
python -m skills.message_sender

# Deploy Edge Function
supabase functions deploy approve-batch
```

**Required `.env` keys:** `SUPABASE_URL`, `SUPABASE_SECRET_KEY`, `UNIPILE_BASE_URL`, `UNIPILE_API_KEY`, `MICROSOFT_CLIENT_ID`, `MICROSOFT_CLIENT_SECRET`, `MICROSOFT_TENANT`, `DEFAULT_TENANT_ID`, `DEFAULT_CAMPAIGN_ID`

## Architecture

```
Slack @Oz → batch-sender → Email to client → Client clicks Approve
  → Edge Function → invite-sender (5/day, bare invites)
  → acceptance-detector (3x/day poll) → notification email
  → message-sender (daily, 3-step sequence)
```

### Four Oz Skills

| Skill | Trigger | What It Does |
|-------|---------|-------------|
| `batch-sender` | Slack "@Oz send this to X at email" | Parse Excel, import prospects, send review email |
| `invite-sender` | Edge Function + daily cron 10 AM PT | Send bare invites, max 5/day, rate limited |
| `acceptance-detector` | Cron 3x/day (9, 13, 17 PT) | Poll relations, detect accepts, send notification |
| `message-sender` | Daily cron 11 AM PT | Send follow-ups, check replies first, stop on reply |

### API Integrations

| API | Role |
|-----|------|
| **Unipile** | LinkedIn actions — invites, messages, profile lookup, relations |
| **Microsoft Graph** | Outlook email via MSAL client credentials |
| **Supabase** | PostgreSQL database + Edge Functions |
| **Warp Oz** | Agent orchestration, Slack integration, cron scheduling |

## Project Layout

```
config.py                   — Environment config + safety constants
db/connect.py               — Supabase client singleton (service role key)
db/supabase_schema_v3.sql   — Full schema (11 tables)
lib/outlook.py              — OutlookClient (Microsoft Graph email)
lib/unipile.py              — UnipileClient (LinkedIn API + activity logging)
skills/helpers.py           — Business hours, delays, rate limits, event logging
skills/batch_sender.py      — Skill 1: Excel → Supabase → review email
skills/invite_sender.py     — Skill 2: Rate-limited bare invites
skills/acceptance_detector.py — Skill 3: Poll relations, detect accepts
skills/message_sender.py    — Skill 4: Follow-up message sequences
templates/batch_review_email.py   — HTML email for batch review
templates/acceptance_email.py     — HTML email for acceptance notification
supabase/functions/approve-batch/ — Edge Function (batch approval)
.agents/skills/*/SKILL.md        — Oz skill definitions
```

## Safety Rules (Non-Negotiable)

### LinkedIn Rate Limits (Free Account)

| Action | Daily | Weekly | Delay |
|--------|-------|--------|-------|
| Connection requests | 5 | 20 | 45-120s random |
| Messages | 50 | — | 45-120s random |

- **BARE INVITES ONLY** — no connection notes
- **Business hours only** — 8 AM to 6 PM Pacific, weekdays
- **Pre-flight check** before every invite — verify not FIRST_DEGREE
- **Reply check** before every follow-up — stop sequence on reply
- **4-week ramp-up** for new accounts (3→4→5/day)
- **7-day cooldown** after account reconnection

### Prospect State Machine

```
sourced → scored → approved → invite_sent → connected →
msg1_sent → msg2_sent → msg3_sent → completed
At any point: → replied (stops everything)
Terminal: skipped | declined | recycled | blacklisted
```

## Database

- **Supabase PostgreSQL** — v2 schema + pipeline migration, service role key bypasses RLS
- Schema reference: `db/supabase_schema_v3.sql` (aspirational), actual DB is v2 + `db/migrate_v2_to_pipeline.sql`
- Rate limiting enforced via `check_rate_limit()` and `get_effective_limit()` DB functions
- Activity logging via `activity_log` table

### Important: Actual Column Names (v2 schema)

The DB uses v2 column names which differ from the v3 spec:

| v3 Spec | Actual DB Column |
|---------|-----------------|
| `prospects.scoring` (JSONB) | `prospects.icp_score` (integer) |
| `prospects.raw_data` (JSONB) | `prospects.raw_apollo_data` (JSONB) |
| `companies` table | `prospect_companies` table |
| `linkedin_accounts.unipile_account_id` | `linkedin_accounts.provider_account_id` (renamed by migration) |
| `invitations.unipile_invitation_id` | `invitations.external_invitation_id` (renamed by migration) |

## Acceptance Detection

Bare invites don't create a chat on acceptance, so webhooks don't work reliably. We poll:
- Primary: `GET /api/v1/users/relations` — 3x/day
- Compare against 'sent' invitations in DB
- On detection: update invitation + prospect, create messages, send email

## Oz Environment

- **Environment ID:** `iR37ujTjeo7Ne6pZ9vHRcI` (name: `vwc-linkedin`)
- **GitHub repo:** `ChrisCMO/linkedin-oz-agent-test`
- **Docker image:** `warpdotdev/dev-base:latest-agents`
- **Setup command:** `pip install --break-system-packages -r /workspace/linkedin-oz-agent-test/requirements.txt`
- **Workspace path:** `/workspace/linkedin-oz-agent-test`
- **Slack:** Connected (team-wide)
- **Skills:** Use `python3` (system Python, not venv)

### Dashboard → Oz Integration

The "Enrich & Score" button triggers Oz via REST API:
```
dashboard/src/app/api/trigger-scoring/route.ts
  → POST https://app.warp.dev/api/v1/agent/run
  → Oz runs: python3 -m skills.company_scorer --tenant-id <ID>
  → Results written to raw_companies table
  → Dashboard auto-polls every 5s while processing
```
Requires `WARP_API_KEY` and `OZ_ENVIRONMENT_ID` in `dashboard/.env.local`.

### Manual Test Commands

```bash
# Test company-scorer v2 (enrichment + scoring pipeline)
oz agent run-cloud -e iR37ujTjeo7Ne6pZ9vHRcI --prompt 'Run: cd /workspace/linkedin-oz-agent-test && python3 -m skills.company_scorer --tenant-id 00000000-0000-0000-0000-000000000001 --limit 3'

# Test batch-sender
oz agent run-cloud -e iR37ujTjeo7Ne6pZ9vHRcI --prompt 'Run: cd /workspace/linkedin-oz-agent-test && python3 -m skills.batch_sender --file test_prospects.csv --name "Chris" --email "christopher@yorcmo.com"'

# Test invite-sender
oz agent run-cloud -e iR37ujTjeo7Ne6pZ9vHRcI --prompt "Run: cd /workspace/linkedin-oz-agent-test && python3 -m skills.invite_sender"

# Test acceptance-detector
oz agent run-cloud -e iR37ujTjeo7Ne6pZ9vHRcI --prompt "Run: cd /workspace/linkedin-oz-agent-test && python3 -m skills.acceptance_detector"

# Test message-sender
oz agent run-cloud -e iR37ujTjeo7Ne6pZ9vHRcI --prompt "Run: cd /workspace/linkedin-oz-agent-test && python3 -m skills.message_sender"

# Check run status
oz run get <RUN_ID>
```

### Oz Setup Gotchas

**Secrets & Auth:**
- `oz secret create` requires `--value-file <path>` not `--value "string"`
- **Personal API keys** work with personal GitHub accounts. **Team API keys** require a GitHub Organization with the Warp GitHub App installed at the org level.
- Secrets are team-scoped and shared across ALL environments. Updating `SUPABASE_URL` for one project may break another. Verify before updating.
- When wiring up the REST API (`/api/v1/agent/run`), test with `curl` first to isolate auth issues from app issues.

**Environment & Code:**
- Oz pulls from **`main` branch by default**. Feature branch changes won't be picked up until merged.
- The workspace path is `/workspace/<repo-name>`. If you rename a repo, update: (1) `oz environment update` repo, (2) setup command path, (3) any hardcoded workspace paths in API routes/prompts.
- Oz agent may auto-fix code issues (e.g., create missing files) instead of just reporting errors. Push your own fixes to `main` before triggering Oz to avoid conflicts.
- Long `oz` commands in Warp terminal get line-wrapped — newlines become part of the stored string. Run from Claude Code terminal or standard terminal instead.
- Debian-based image blocks bare `pip install` (PEP 668) — use `--break-system-packages`

### Tested & Working

- [x] batch-sender: imports CSV, creates batch_review, sends HTML email via Outlook
- [x] invite-sender: runs, checks business hours, exits cleanly outside hours
- [x] company-scorer v2: enriches + scores companies via Oz, results in `raw_companies`
- [x] Dashboard → Oz trigger: "Enrich & Score" button triggers Oz via REST API
- [x] Dashboard auto-poll: counts and table refresh every 5s while pipeline runs
- [ ] invite-sender: actual invite sending (not tested — needs business hours + approved prospects)
- [ ] acceptance-detector: not yet tested end-to-end
- [ ] message-sender: not yet tested end-to-end
- [ ] Edge Function (approve-batch): not yet deployed
- [ ] Slack trigger (@Oz with file attachment): not yet tested
