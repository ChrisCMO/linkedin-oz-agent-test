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

- **Supabase PostgreSQL** — 11 tables, service role key bypasses RLS
- Schema: `db/supabase_schema_v3.sql`
- Rate limiting enforced via `check_rate_limit()` and `get_effective_limit()` DB functions
- Activity logging via `activity_log` table (partitioned by quarter)

## Acceptance Detection

Bare invites don't create a chat on acceptance, so webhooks don't work reliably. We poll:
- Primary: `GET /api/v1/users/relations` — 3x/day
- Compare against 'sent' invitations in DB
- On detection: update invitation + prospect, create messages, send email
