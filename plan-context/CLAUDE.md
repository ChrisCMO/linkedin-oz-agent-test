# CLAUDE.md — Linkedin-Testing Repository

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt        # Python: requests, supabase, python-dotenv, fastapi, uvicorn, openai
cd mvp/frontend && npm install         # Node: next, react, tailwindcss, shadcn/ui

# Run the web app
uvicorn mvp.backend.app:app --reload   # Backend on :8000 (docs at /api/docs)
cd mvp/frontend && npm run dev         # Frontend on :3000

# Run standalone scripts
python scripts/apollo_pipeline_example.py  # Full Apollo → Unipile pipeline demo
python scripts/apollo_test.py              # ICP pool size validation
python db/connect.py                       # Database connectivity test
```

**Required `.env` keys:** `UNIPILE_DSN`, `UNIPILE_API_KEY`, `SUPABASE_URL`, `SUPABASE_KEY`, `APOLLO_API_KEY`, `LINKUP_API_KEY`, `OPENAI_API_KEY`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `OUTLOOK_SENDER_EMAIL`

---

## Architecture

**Lifecycle loop:** TARGET → DISCOVER → ENGAGE → NURTURE → CONVERT → LEARN → (feedback to TARGET)

The app is a **FastAPI backend** + **Next.js frontend** backed by **Supabase PostgreSQL**. AI-assisted ICP scoring and message generation use **OpenAI GPT-4o-mini** directly (not autonomous agents).

### API Integrations

| API | Role | Status |
|-----|------|--------|
| **Apollo.io** | Prospect sourcing — search 275M+ contacts, enrich profiles | Validated, fully integrated (`lib/apollo.py`) |
| **Unipile** | LinkedIn actions — invitations, messaging, profile lookup | Validated (7/8 tests pass), fully integrated (`lib/unipile.py`) |
| **LinkUp** | Invitation status backup (fills Unipile gap) | Integrated (`lib/linkup.py`) |
| **OpenAI** | ICP scoring + 3-message sequence generation | Integrated (GPT-4o-mini via `message_gen_svc.py`, `scoring.py`) |
| **Microsoft Graph** | Email notifications via Outlook | Integrated (`lib/outlook.py`) |
| **Supabase** | Database + auth | Integrated (20-table multi-tenant schema) |
| **Reply.io** | Email sequences for non-responders | Draft test plans only |

### Database

- **Supabase PostgreSQL** — multi-tenant schema v2 (`db/supabase_schema_v2.sql`, 20+ tables)
- **Connection:** `db/connect.py` → `get_supabase()` returns cached Supabase client
- Legacy SQLite schema exists at `db/schema.sql` but is not used by the app

### Prospect State Machine

```
SOURCED → SCORED → APPROVED → INVITE_SENT → CONNECTED → RESEARCHED →
SEQUENCE_QUEUED → MSG1_APPROVED → MSG1_SENT → MSG2_PENDING → MSG2_SENT →
MSG3_PENDING → MSG3_SENT → COMPLETED
```
Terminal states: `REPLIED` (stops sequence), `SKIPPED`, `INVITE_DECLINED`, `RECYCLED`, `BLACKLISTED`

---

## Project Layout

```
mvp/backend/
  app.py                    # FastAPI app (CORS, 8 routers, health check)
  config.py                 # Singleton config & env loading
  middleware.py              # Request logging middleware
  routers/                  # API endpoints (8 routers, ~28 endpoints)
    dashboard.py             #   KPIs, activity chart, usage rates
    prospects.py             #   CRUD, approve/skip, score, add-by-url, bulk-approve
    sequences.py             #   Get/edit/start message sequences
    admin.py                 #   Unipile account management
    apollo.py                #   ICP CRUD + Apollo search pipeline
    campaigns.py             #   Campaign CRUD, send invites, poll acceptance
    email.py                 #   Email notifications (send, test, history)
    demo_test.py             #   End-to-end test route
  services/                 # Business logic (9 services, ~2300 lines)
    apollo_svc.py            #   ICP management + search/enrich/score pipeline
    campaign_svc.py          #   Campaign CRUD + status transitions
    email_svc.py             #   Outlook email sending + notification history
    message_gen_svc.py       #   OpenAI 3-message sequence generation
    outreach_svc.py          #   Invitations, acceptance detection, rate limiting (largest)
    prospect_svc.py          #   Prospect list, status updates, add-by-url
    scoring.py               #   AI-powered ICP scoring (0-100)
    sequence_svc.py          #   Sequence/message queries + status updates
    stats.py                 #   Dashboard KPIs + activity chart data

mvp/frontend/
  src/app/
    page.tsx                 # Dashboard — KPI grid + activity chart
    admin/page.tsx           # Admin — account health + usage table
    campaigns/page.tsx       # Campaigns — create, list, send invites
    prospects/page.tsx       # Prospects — list, filter, approve, Apollo search
    sequences/[prospectId]/  # Sequences — message editor per prospect
    email/page.tsx           # Email — send test, view notification history
    demo-test/page.tsx       # Demo — end-to-end test runner
  src/features/              # Feature modules (admin, campaigns, dashboard, email, prospects, sequences)
    */components/            #   18 feature-specific components
    */hooks/                 #   19 custom hooks
  src/components/ui/         # shadcn/ui primitives (badge, button, card, input, table, textarea)
  src/lib/                   # API client, constants, utils
  src/types/api.ts           # Shared TypeScript types

lib/                        # Third-party API clients
  unipile.py                 # UnipileClient — LinkedIn API + activity logging
  apollo.py                  # ApolloClient — prospect search + enrichment
  linkup.py                  # LinkUpClient — invitation status checker
  outlook.py                 # OutlookClient — Microsoft Graph email

db/
  connect.py                 # get_supabase() — cached Supabase client
  supabase_schema_v2.sql     # Production schema (multi-tenant, 20+ tables)
  schema.sql                 # Legacy SQLite schema (not used)
  migrate_v1_to_v2.sql       # Migration script

scripts/                    # Standalone test/demo scripts
  apollo_pipeline_example.py # Full search → enrich → score demo
  apollo_test.py             # ICP pool size validation
  apollo_full_test.py        # Full Apollo test suite
  test_email_on_acceptance.py# Email notification test

docs/
  architecture.md            # System design overview
  apollo-api-reference.md    # Apollo endpoint reference + credit model
  unipile/                   # Unipile validation (8 test reports)
  linkup/                    # LinkUp test plans + demo
  reply-io/draft/            # Reply.io test plans (not executed)
  heyreach/draft/            # HeyReach exploration (not executed)

output/                     # Apollo API test output (JSON)
```

---

## Backend API Endpoints

All routes are prefixed with `/api`. FastAPI docs at `/api/docs`.

| Router | Endpoints | Key Operations |
|--------|-----------|----------------|
| **dashboard** | `GET /kpis`, `GET /activity-chart`, `GET /linkedin-usage` | KPI counts, time-series activity, usage rates |
| **prospects** | `GET /prospects`, `POST /.../approve`, `POST /.../skip`, `POST /.../score`, `POST /.../add-by-url`, `POST /.../bulk-approve` | Paginated list with filters, approve/skip, AI scoring, add from LinkedIn URL |
| **sequences** | `GET /sequences/{prospectId}`, `PUT /messages/{id}`, `POST /sequences/{prospectId}/start` | Get sequence + messages, edit message, activate sequence |
| **admin** | `GET /usage`, `GET /accounts`, `GET /linkedin-accounts` | Unipile usage, account health, LinkedIn accounts |
| **apollo** | `GET /icps`, `POST /icps`, `PUT /icps/{id}`, `POST /apollo/search`, `GET /apollo/usage` | ICP CRUD, search + enrich + score pipeline, credit usage |
| **campaigns** | `POST /campaigns`, `GET /campaigns`, `PATCH /.../`, `PUT /.../status`, `POST /.../send-invites`, `POST /.../poll-acceptance`, `GET /.../rate-limit`, `GET /.../invitations` | Campaign CRUD, send invites (background), poll acceptance, rate limits |
| **email** | `POST /email/send-acceptance-notification`, `POST /email/test`, `GET /email/notifications` | Acceptance email, test email, notification history |
| **demo_test** | `POST /demo-test/run` | Seed data → poll acceptance → verify email |

---

## Frontend Pages

| Page | Route | Features |
|------|-------|----------|
| **Dashboard** | `/` | KPI grid (prospects, invites, messages, replies) + activity chart |
| **Prospects** | `/prospects` | Filterable list, approve/skip, Apollo search drawer, add by URL |
| **Campaigns** | `/campaigns` | Campaign cards, create drawer, detail panel, send invites |
| **Sequences** | `/sequences/[prospectId]` | 3-message editor, prospect sidebar, approve + send |
| **Admin** | `/admin` | Account health cards, API usage table |
| **Email** | `/email` | Send test email, notification history |
| **Demo Test** | `/demo-test` | End-to-end test runner |

---

## Key Conventions & Safety Rules

### Mandatory API Parameters
- **Profile lookups:** Always use `linkedin_sections=*` — without it you only get name/headline
- **Connection requests:** Always include a personalized note (required for near-real-time acceptance detection)
- **Account ID:** Required as query param on most Unipile endpoints (`?account_id=`)

### Rate Limiting (Full details: `docs/master-architecture-plan.md` Section 11)
- Random delays: **45-120 seconds** between LinkedIn API actions
- Business hours only: **8 AM - 6 PM** (tenant timezone), no weekends
- Daily limits: **6 min / 10 max connections**, 50 messages, 80 profile views
- Weekly limit: 50 connections
- 4-week ramp-up: 2/day week 1 → 3 week 2 → 4 week 3 → 6 min (full baseline) week 4
- Max (10/day) only when acceptance rate >15% and account age >30 days
- 7-day cooldown after account reconnection (zero actions)
- All actions logged to `activity_log` table for enforcement
- Connection request notes are **optional** (improve acceptance rate + enable webhook detection, but not required)

### Reply Detection
- **Primary:** `GET /api/v1/chats/{id}/messages` — check for `is_sender: 0`
- **Webhook:** `new_message` event — single self-sent message = acceptance, not reply
- **Before every follow-up:** Always check for replies first, never rely solely on webhooks

### Known API Gaps
- `GET /api/v1/users/invitations/sent` returns **404** — endpoint doesn't exist
- **Acceptance detection** uses a 4-layer strategy documented in `docs/unipile/08-acceptance-detection.md`
- LinkUp `POST /v1/network/invitation-status` provides backup invitation status checks

---

## Linked Resources

- **Parent CLAUDE.md** (`../CLAUDE.md`): Full agent instructions, Unipile API reference, ICP scoring framework, message sequence rules
- **Client context** (`client.md`): Project background, stakeholders, goals, contract scope
- **Unipile validation** (`docs/unipile/00-validation-report.md`): 8 endpoint tests (7 PASS, 1 FAIL)
- **Acceptance detection** (`docs/unipile/08-acceptance-detection.md`): 4-layer acceptance detection strategy
- **Apollo reference** (`docs/apollo-api-reference.md`): Endpoint reference and credit model
- **System architecture** (`docs/architecture.md`): Full design overview

## Important!
- Remember:
Focus your attention understanding the prompt.
Make sure you are 95% sure with your understanding. Otherwise ask.
