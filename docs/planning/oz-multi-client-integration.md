# Oz Multi-Client Integration Plan

## Context

Joe wants this LinkedIn outreach pipeline to become a **general agent** inside YorCMO AI — not hardcoded to VWC. Any CMO with any client should be able to:

1. Build an ICP in a working session
2. Ingest that ICP into the agent
3. Test scoring + approve messaging
4. Deploy LinkedIn outreach

> "If we took this process and it became a general agent inside the YorCMO AI toolkit, then any CMO with any client could go through that same process." — Joe Frost, April 6 2026

> "We need to make sure we're not making this agent so specific to that one type of client. We need to make sure it's more of a framework that can be applied to different clients." — Joe Frost

## Current State: What's Hardcoded to VWC

| What | Where | VWC-specific? |
|------|-------|---------------|
| Oz Environment ID | `dashboard/.env.local` → `OZ_ENVIRONMENT_ID=iR37ujTjeo7Ne6pZ9vHRcI` | Yes — single env |
| Default Tenant ID | `.env` / `config.py` → `00000000-0000-0000-0000-000000000001` | Yes |
| ICP scoring dimensions | `scoring.py` → `score_companies_v2()` | Yes — weights, industries, geography all VWC |
| Finance title list | `FINANCE_TITLES` in `test_full_v2_pipeline.py` | Partially — CFO/Controller is common but not universal |
| Target industries | Hardcoded in scoring prompt | Yes — Manufacturing > CRE > Prof Services etc. |
| Geography scoring | Hardcoded Seattle metro = 15, WA = 13, OR = 11 | Yes |
| Size sweet spots | 100-300 employees, $50M-$100M revenue | Yes |
| Hard exclusions | Revenue > $150M, public, PE-backed, banking, gov | Mostly universal but thresholds are VWC-specific |

### What's Already Multi-Tenant

| What | Where | Status |
|------|-------|--------|
| Database schema | All tables have `tenant_id` column | Ready |
| Dashboard routes | `/clients/[tenantId]/...` | Ready |
| Skills | Accept `--tenant-id` parameter | Ready |
| Trigger scoring API | Passes `tenantId` to Oz | Ready (but routes to single env) |
| Batch sender | Creates prospects scoped to tenant | Ready |
| Invite/message sender | Queries by tenant_id | Ready |

## Two Options

### Option A: One Oz Environment, Per-Tenant ICP Config (Recommended First)

**How it works:**
- Keep the single Oz environment (`iR37ujTjeo7Ne6pZ9vHRcI`)
- Store per-tenant ICP config as JSONB in Supabase
- Skills read the ICP config for whatever tenant they're running for
- `score_companies_v2(companies, icp_config)` — all weights, industries, titles, geography come from config

**New table:**
```sql
CREATE TABLE tenant_icp_configs (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id UUID REFERENCES tenants(id) NOT NULL,
    name TEXT NOT NULL,                    -- "ICP 1 - Audit & Tax", "ICP 2 - Benefit Plan Audit"
    is_active BOOLEAN DEFAULT true,
    config JSONB NOT NULL,                 -- full ICP configuration
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    created_by TEXT                         -- CMO who built the ICP
);
```

**ICP config structure:**
```json
{
  "target_industries": {
    "priority_1": ["manufacturing", "machinery", "aviation & aerospace"],
    "priority_2": ["commercial real estate", "property management"],
    "priority_3": ["professional services", "engineering"],
    "priority_4": ["hospitality", "hotels", "restaurants"],
    "priority_5": ["nonprofit"],
    "priority_6": ["construction", "civil engineering"]
  },
  "industry_weights": {
    "manufacturing": 20, "machinery": 20, "aviation & aerospace": 19,
    "commercial real estate": 18, "construction": 15, "nonprofit": 14
  },
  "geography": {
    "primary": {
      "cities": ["Seattle", "Bellevue", "Tacoma", "Redmond", "Kirkland", "Everett"],
      "score": 15
    },
    "secondary": {
      "regions": ["Washington"],
      "score": 13
    },
    "tertiary": {
      "regions": ["Oregon"],
      "score": 11
    }
  },
  "company_size": {
    "sweet_spot": [100, 300],
    "acceptable": [25, 750],
    "max_employees": 10000
  },
  "revenue": {
    "sweet_spot": [50000000, 100000000],
    "acceptable": [5000000, 150000000],
    "unknown_score": 7
  },
  "ownership": {
    "preferred": ["private", "family-owned", "ESOP", "founder-led"],
    "exclude": ["public", "PE-backed"]
  },
  "target_titles": {
    "primary": ["CFO", "Chief Financial Officer", "Controller", "VP Finance", "Director of Finance"],
    "secondary": ["President", "Owner", "CEO", "Founder", "Managing Director"],
    "adjacent": ["Accounting Manager", "Bookkeeper", "Treasurer"]
  },
  "hard_exclusions": {
    "max_revenue": 150000000,
    "public_companies": true,
    "pe_backed": true,
    "industries": ["banking", "government"]
  },
  "scoring_weights": {
    "industry_fit": 20,
    "company_size": 20,
    "revenue_fit": 10,
    "geography": 15,
    "ownership_structure": 15,
    "digital_footprint": 10,
    "organizational_complexity": 10
  },
  "complexity_signal": {
    "description": "If they have a CFO or Controller, that indicates increased complexity",
    "boost_titles": ["CFO", "Chief Financial Officer", "Controller"],
    "max_score": 10
  }
}
```

**What changes in code:**
1. `score_companies_v2()` takes `icp_config` parameter instead of hardcoded values
2. `test_full_v2_pipeline.py` reads ICP config from Supabase by tenant_id (or from a local JSON file for testing)
3. `FINANCE_TITLES` comes from `icp_config["target_titles"]["primary"]`
4. Dashboard `trigger-scoring` passes tenant_id → skill reads config from DB
5. SKILL.md becomes a template, not VWC-specific

**Pros:**
- Minimal infrastructure change — same Oz environment, same codebase
- ICP config is version-controlled per tenant
- CMOs can review/edit the config (Joe's "MacDaddy file" concept)
- Fast to implement — mostly parameterizing existing code

**Cons:**
- All tenants share the same Oz environment (compute, secrets, rate limits)
- If one client's pipeline is heavy, it could slow others
- API keys (Apollo, Apify, ZoomInfo) are shared across all tenants

---

### Option B: Separate Oz Environment Per Client

**How it works:**
- Each client/tenant gets their own Oz environment in Warp
- Each environment has its own AGENT.md with client-specific context
- Dashboard maps tenant → Oz environment ID
- Complete isolation between clients

**New mapping:**
```sql
-- Add to tenants table or create new table
ALTER TABLE tenants ADD COLUMN oz_environment_id TEXT;
ALTER TABLE tenants ADD COLUMN oz_agent_md TEXT;  -- the "MacDaddy file" content
```

**Dashboard change:**
```typescript
// trigger-scoring/route.ts
// Instead of: process.env.OZ_ENVIRONMENT_ID
// Use: tenant.oz_environment_id
const tenant = await supabase
  .from('tenants')
  .select('oz_environment_id')
  .eq('id', tenantId)
  .single();
```

**Per-client Oz setup:**
```bash
# For a new client "Bagel Co"
oz environment create --name "bagelco-linkedin" \
  --repo ChrisCMO/linkedin-oz-agent-test \
  --docker-image warpdotdev/dev-base:latest-agents

# Set client-specific secrets (if different API keys)
oz secret create APOLLO_API_KEY --value-file /tmp/bagelco_apollo.key

# Store the environment ID in Supabase
UPDATE tenants SET oz_environment_id = 'new-env-id' WHERE name = 'Bagel Co';
```

**Pros:**
- Full isolation — one client's pipeline can't affect another
- Each client can have different API keys / rate limits
- AGENT.md per environment = Joe's "MacDaddy file" vision
- Easier to debug per-client issues

**Cons:**
- More infrastructure to manage (environments, secrets, setup per client)
- Oz secrets are team-scoped — updating a secret affects ALL environments
- Higher cost (each environment has its own compute)
- More complex onboarding process per client

---

## Recommended Path

### Phase 1: Parameterize ICP Config (Option A) — Do Now
- Create `tenant_icp_configs` table
- Refactor `score_companies_v2()` to accept `icp_config` parameter
- Move VWC's ICP from hardcoded Python to a JSON config row
- Test with VWC — same results, but config-driven
- This is what makes the pipeline "general purpose"

### Phase 2: Client Onboarding Flow — Next
- CMO fills out ICP template (Joe's "minimum detail" requirement)
- System generates `icp_config` JSON from the ICP document
- CMO reviews and approves the config
- Config stored in `tenant_icp_configs`
- Pipeline runs using that config

### Phase 3: Per-Client Oz Environments (Option B) — Later, If Needed
- Only if client isolation becomes a real requirement
- Or if different clients need different API keys
- Or if "MacDaddy file" per environment adds value beyond what DB config provides

## Joe's "Bagel Company" Test

To validate the framework is general enough, here's what a bagel wholesale client would look like:

```json
{
  "target_industries": {
    "priority_1": ["restaurants", "food & beverages", "bakeries"],
    "priority_2": ["hotels", "hospitality", "catering"],
    "priority_3": ["grocery", "food retail"]
  },
  "geography": {
    "primary": { "cities": ["New York", "Brooklyn", "Queens"], "score": 15 },
    "secondary": { "regions": ["New Jersey", "Connecticut"], "score": 13 }
  },
  "target_titles": {
    "primary": ["Head Chef", "Executive Chef", "Culinary Director", "F&B Director"],
    "secondary": ["Owner", "General Manager", "Purchasing Manager"]
  },
  "company_size": {
    "sweet_spot": [5, 50],
    "acceptable": [1, 200]
  },
  "complexity_signal": {
    "description": "If they have a Head Chef or Culinary Director, they care about ingredient quality",
    "boost_titles": ["Head Chef", "Executive Chef"],
    "max_score": 10
  }
}
```

The same `score_companies_v2(companies, icp_config)` function handles both VWC CPAs and the bagel company — different config, same pipeline logic.

## Open Questions

1. **Oz secrets scope:** Oz secrets are team-wide. If Client A has their own Apollo key, updating the secret breaks Client B. Option A avoids this (shared keys), Option B needs Warp to support per-environment secrets.

2. **Scoring model:** Currently uses GPT-5.4 with a VWC-tuned prompt. For multi-client, the prompt needs to be generated from `icp_config` rather than hardcoded. The scoring dimensions (industry_fit, company_size, etc.) stay the same — the weights and targets change per client.

3. **Rate limits:** LinkedIn rate limits (5 invites/day, 50 messages/day) are per-LinkedIn-account, not per-client. If multiple clients share a LinkedIn account, they share limits. Each client likely needs their own LinkedIn account connected via Unipile.

4. **Dashboard access:** Tyler wants clients to log in and see their pipeline. Current dashboard is admin-only. Need client-facing views (read-only or approve-only) vs. admin views (full config).

5. **Who owns the ICP config?** Joe says CMOs build the ICP → CMO reviews the generated config → system runs it. There should be a review/approval step before the pipeline uses a new config.
