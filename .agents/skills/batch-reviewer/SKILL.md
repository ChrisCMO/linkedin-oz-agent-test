---
name: batch-reviewer
description: AI batch reviewer — pre-screen monthly prospect batches before sending to Chad for approval. Checks for duplicates, competitors, inactive profiles, blacklisted companies, and non-ICP titles. Sends Chad a summary email with a review link to the dashboard. Use when asked to "review batch", "AI review", "screen prospects", "check the list", "send to Chad", or "prepare monthly batch".
---

# AI Batch Reviewer

Pre-screens monthly prospect batches before Chad reviews them. Catches duplicates, competitors, VWC clients, inactive profiles, and bad titles. Sends Chad a summary email with a link to the dashboard.

## Flow

```
Dashboard "AI Review" button → Oz runs this skill
  1. Load prospects for the month from Supabase
  2. Cross-batch dedup: check against previously contacted prospects
  3. Rule-based checks: competitors, blacklist, inactive, no LinkedIn, unverified, bad titles
  4. GPT-5.4: review borderline titles for ICP relevance
  5. Update statuses: approved / skipped
  6. Send Chad an email summary with "Review & Approve" link to dashboard
```

## Running

```bash
python3 -m skills.batch_reviewer --tenant-id 00000000-0000-0000-0000-000000000001 --month 2026-04
python3 -m skills.batch_reviewer --tenant-id 00000000-0000-0000-0000-000000000001 --month 2026-04 --dry-run
python3 -m skills.batch_reviewer --tenant-id 00000000-0000-0000-0000-000000000001 --month 2026-04 --send-email chad@yorcmo.com
```

## Checks (in order)

### Rule-based (instant, no API cost):

| Check | Auto-skip if | Why |
|-------|-------------|-----|
| Cross-batch duplicate | Same linkedin_slug in previous months with status approved/invite_sent/connected/etc. | Already contacted |
| Within-batch duplicate | Same linkedin_slug or first+last+company appears twice | Dedup |
| Blacklisted company | Matches `data/blacklist.csv` (VWC clients) | Can't contact our own clients |
| Competitor firm | Matches `data/competitors.csv` (Moss Adams, BDO, Big 4, etc.) | CPA competitors |
| No LinkedIn URL | linkedin_url is empty | Can't do LinkedIn outreach |
| Inactive profile | activity_score = 0 AND linkedin_connections < 10 | Ghost account |
| Role not verified | role_verified = false (profile scrape couldn't confirm) | May not be at this company |
| Non-ICP title | Title doesn't contain any finance/exec keywords from lib/title_tiers.py | Not a decision-maker |

### GPT-5.4 (borderline cases only, ~$0.01/batch):

| Check | When | What GPT decides |
|-------|------|-----------------|
| Title relevance | Title is Tier 0 (unknown) from classify_title_tier() | "Is this title relevant for CPA audit/tax outreach?" → approve or skip with reason |

## Data files

| File | What |
|------|------|
| `data/blacklist.csv` | VWC clients — company_name, domain, reason |
| `data/competitors.csv` | Competitor CPA firms — company_name, domain, reason |

## Email to Chad

After review completes, sends an email via `lib/outlook.py` (Microsoft Graph):

**Subject:** "VWC Prospect Batch — {month} ({approved} prospects ready for review)"

**Body includes:**
- Batch summary: X companies, Y prospects approved, Z removed
- Top issues found (duplicates, inactive, competitors)
- "Review & Approve" button linking to dashboard: `/clients/{tenantId}/review-batches`
- List of top 10 companies with contact counts

**Sent from:** ai_team@yorcmo.com (configured in MICROSOFT_SENDER_EMAIL)

## Dashboard integration

- Dashboard page: `/clients/[tenantId]/review-batches`
- API route: `/api/trigger-review` → calls Oz REST API → Oz runs this skill
- Dashboard polls Supabase every 5s for status changes while Oz is running
- After Oz finishes: dashboard shows review results (approved/skipped counts, issues)
- Dev reviews AI's work, then clicks "Send to Approver" (or Oz already sent the email)

## Key files

| File | What |
|------|------|
| `skills/batch_reviewer.py` | Main script — all checks + email sending |
| `lib/outlook.py` | OutlookClient for Microsoft Graph email |
| `lib/title_tiers.py` | classify_title_tier() for title checks |
| `data/blacklist.csv` | VWC client exclusion list |
| `data/competitors.csv` | Competitor CPA firms |
| `templates/batch_review_email.py` | Existing email template (for prospect cards) |
