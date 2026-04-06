# Plan: Replace Oz with FastAPI + Keep Oz for Agentic Tasks Only

## Context

Oz adds 60-90s overhead per run (Docker spin-up, pip install, Claude interpreting a fixed command). The pipeline scripts (`company_scorer.py`, `prospect_enricher.py`) are deterministic — they don't need AI to decide what to run. But Tyler wants the architecture to be "agentic," meaning the system should handle errors, make decisions, and self-heal without human intervention.

**Solution:** FastAPI server runs the pipeline scripts directly (fast, zero overhead). Oz is reserved for tasks that actually need AI reasoning (Slack interactions, error diagnosis, client onboarding, weekly reports).

## What Changes

### Current (slow)
```
Dashboard → POST /api/trigger-scoring
  → Oz API (60-90s overhead)
    → Docker + pip install + Claude reads prompt
    → python3 -m skills.company_scorer
  → Results in Supabase
```

### Proposed (fast + agentic)
```
Dashboard → POST /api/score-companies
  → FastAPI server (0s overhead)
    → python3 -m skills.company_scorer (runs directly)
    → If error → logs to Supabase error table
    → If critical failure → triggers Oz to diagnose
  → Results in Supabase
```

## Files to Create/Modify

### 1. `server.py` — FastAPI server (new)

Simple server that exposes pipeline skills as HTTP endpoints:

```python
POST /score-companies      → runs company_scorer.run()
POST /enrich-prospects     → runs prospect_enricher.run()
POST /health               → returns status
```

Runs as a background task (non-blocking) so the dashboard gets an immediate response with a job ID, then polls for completion.

Deploy to: Railway / Render / Digital Ocean App Platform (~$7/month)

### 2. Dashboard API routes — point to FastAPI instead of Oz

```
/api/trigger-scoring             → POST {fastapi_url}/score-companies
/api/trigger-prospect-enrichment → POST {fastapi_url}/enrich-prospects
```

Same request/response shape. Dashboard doesn't know or care whether it's Oz or FastAPI behind the scenes.

### 3. Keep Oz for agentic tasks

Oz stays connected to Slack. Used for:
- Slack triggers: "@Oz send this batch to Adrienne"
- Error escalation: FastAPI logs a critical error → Oz investigates
- Client onboarding: CMO gives Oz an ICP doc → Oz creates tenant config
- Weekly reports: Oz cron analyzes pipeline results and posts to Slack

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌───────────┐
│  Dashboard   │────→│  FastAPI Server   │────→│  Supabase  │
│  (Next.js)   │     │  (always-on)      │     │  (DB)      │
└─────────────┘     │                    │     └───────────┘
                    │  /score-companies   │          ↑
                    │  /enrich-prospects  │          │
                    │  /send-invites     │          │
                    └──────────────────┘          │
                                                   │
┌─────────────┐     ┌──────────────────┐          │
│  Slack       │────→│  Oz (Warp)        │─────────┘
│  (@Oz)       │     │  (AI reasoning)   │
└─────────────┘     │                    │
                    │  Client onboarding  │
                    │  Error diagnosis    │
                    │  Weekly reports     │
                    │  Slack commands     │
                    └──────────────────┘
```

## FastAPI Server Details

### Endpoints

```
POST /score-companies
  Body: { tenant_id, batch_id?, limit? }
  → Spawns background task running company_scorer.run()
  → Returns: { job_id, status: "started" }

POST /enrich-prospects
  Body: { tenant_id, company_ids }
  → Spawns background task running prospect_enricher.run()
  → Returns: { job_id, status: "started" }

GET /jobs/{job_id}
  → Returns: { status: "running" | "completed" | "failed", result?, error? }

POST /health
  → Returns: { status: "ok", uptime, active_jobs }
```

### Agentic error handling (built into FastAPI, not Oz)

The "agentic" behavior lives in the pipeline code itself:

```python
# In company_scorer.py — already has this:
- Apollo 402 → stop and report "credits exhausted"
- No domain → try extracting from LinkedIn scrape
- Junk domain → filter and skip Apollo
- Scoring returns weird result → log and continue

# Add to FastAPI wrapper:
- If any skill fails → log to supabase `pipeline_errors` table
- If critical (Apollo credits gone, Supabase down) → POST to Oz for diagnosis
- Retry transient errors (network timeouts, 429s) with exponential backoff
- Report completion summary to Slack webhook
```

### Deployment

**Railway (recommended):** 
- Connect GitHub repo → auto-deploys on push to main
- $5/month hobby plan, $0.000463/min compute
- Environment variables from Railway dashboard (same as .env)
- Always-on, no cold start

**Start command:** `uvicorn server:app --host 0.0.0.0 --port $PORT`

**Required env vars:** Same as current .env (Supabase, Apollo, Apify, OpenAI, etc.)

## Migration Path

1. **Create `server.py`** with FastAPI endpoints wrapping existing skills
2. **Deploy to Railway** with env vars from .env
3. **Update dashboard API routes** to call FastAPI URL instead of Oz
4. **Add `FASTAPI_URL` to dashboard .env.local** (e.g., `https://linkedin-pipeline.up.railway.app`)
5. **Test:** Upload companies → click Enrich & Score → should be 2-3x faster
6. **Keep Oz Slack integration** for agentic tasks that need reasoning

## Expected Performance

| Metric | Oz (current) | FastAPI (proposed) |
|--------|-------------|-------------------|
| Cold start | 60-90s | 0s (always-on) |
| Pipeline run (3 companies) | ~2-5 min | ~1-2 min |
| Cost per run | 6-70 Oz credits | ~$0.001 compute |
| Monthly cost (50 runs) | $50-100 in credits | ~$7 server |
| Error recovery | AI-powered (good) | Code-based retry + Oz escalation |

## Verification

1. Deploy FastAPI to Railway
2. `curl -X POST https://your-app.up.railway.app/score-companies -d '{"tenant_id": "00000000-..."}'`
3. Check job status: `curl https://your-app.up.railway.app/jobs/{job_id}`
4. Update dashboard to use FastAPI URL
5. Upload 3 test companies → Enrich & Score → verify same results, faster
