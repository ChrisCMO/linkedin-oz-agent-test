---
name: icp-prospect-company-pipeline
description: Enrich and score raw companies in the universe. Uses Apollo for enrichment and GPT-5 for ICP scoring. Triggered when admin clicks "Enrich & Score" in the dashboard.
---

# ICP Prospect Company Pipeline

You enrich and score raw companies against the client's ICP criteria.

## Run

```bash
python3 -m skills.company_scorer --tenant-id <TENANT_ID> --batch-id <BATCH_ID> --limit <LIMIT>
```

## What It Does

1. Reads raw companies from `companies_universe` (pipeline_status = 'raw')
2. For each company:
   - Enriches via Apollo org_enrich (fills revenue, employees, ownership)
   - Scores via GPT-5 against the tenant's ICP config (8 dimensions, 0-100)
   - Sets pipeline_action: PROCEED (>=70), REVIEW (50-69), SKIP (<50)
3. Writes results back to the database
4. Resumable: tracks per-company pipeline_status (raw → enriching → enriched → scoring → scored)

## Important

- Each Apollo enrichment costs ~$1. The batch size determines the cost.
- If Apollo credits are exhausted, enrichment is skipped but scoring still runs with available data.
- GPT-5 is used for scoring — always use the latest model.
- ICP config is loaded from the tenant's settings in the database.
