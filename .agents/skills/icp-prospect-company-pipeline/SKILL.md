---
name: icp-prospect-company-pipeline
description: Enrich and score raw companies using v2 pipeline — blacklist check, Apollo enrichment, finance title scan, PSBJ cross-reference, revenue mismatch detection, and GPT-5.4 ICP scoring with organizational complexity.
---

# ICP Company Scoring Pipeline (v2)

You enrich and score raw companies against the VWC CPA ICP using the revamped v2 pipeline.

## Run

```bash
python3 -m skills.company_scorer --tenant-id <TENANT_ID> --batch-id <BATCH_ID> --limit <LIMIT>
```

Default tenant ID: use the `DEFAULT_TENANT_ID` from the environment.

## What It Does

For each company in `companies_universe` (pipeline_status = 'raw'):

1. **Blacklist check** — Skip known VWC clients (loaded from `data/blacklist.csv`)
2. **Apollo org enrichment** — Fill revenue, employees, ownership, industry (~$1/company)
3. **Finance title scan** — Free Apollo people search for CFO/Controller/VP Finance/Director of Finance at the company domain. Detects organizational complexity.
4. **PSBJ cross-reference** — Cross-reference against the Puget Sound Business Journal family-owned companies list. Fills missing revenue, confirms ownership.
5. **Revenue mismatch detection** — Flag companies where revenue / employees < $30K (data is likely wrong). Suspect revenue gets benefit-of-doubt scoring.
6. **v2 ICP scoring** — 7-dimension scoring via GPT-5.4:
   - industry_fit (20), company_size (20), revenue_fit (10), geography (15), ownership_structure (15), digital_footprint (10), **organizational_complexity (10)**

Pipeline actions based on score:
- Score >= 80 → PROCEED
- Score 60-79 → REVIEW
- Score 0 → HARD EXCLUDE
- Score < 60 → SKIP

## Key Differences from v1

| Feature | v1 | v2 |
|---------|----|----|
| Blacklist check | No | Yes — skips known VWC clients |
| Finance title scan | No | Yes — Apollo free people search for CFO/Controller |
| PSBJ cross-reference | No | Yes — validates revenue, confirms family ownership |
| Revenue mismatch | No | Yes — flags suspect revenue (trust employees over revenue) |
| Scoring dimensions | 8 (incl. growth, leadership) | 7 (adds organizational complexity, reduces revenue/digital weight) |
| Scoring model | GPT-5 | GPT-5.4 |
| Scoring method | Per-company GPT prompt | Batch scoring via score_companies_v2() |

## Important

- Apollo org enrichment costs ~$1/company. Finance title scan is free (people search).
- If Apollo credits are exhausted, enrichment is skipped but scoring still runs with available data.
- Resumable: tracks per-company pipeline_status (raw → enriching → enriched → scoring → scored).
- Finance scan results are persisted in `source_data.finance_scan` JSONB column.
- Blacklisted companies are immediately marked as SKIP with score 0.
