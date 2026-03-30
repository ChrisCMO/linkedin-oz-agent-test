---
name: unipile
description: Use when working with the Unipile API for LinkedIn messaging, user lookups, chat management, or sending messages via LinkedIn. Triggers on Unipile, LinkedIn API, LinkedIn messaging tasks.
---

# Unipile API Skill

Unipile provides a REST API for LinkedIn messaging automation — send messages, look up users, list chats, and manage conversations programmatically.

## ⚠️ CRITICAL: LinkedIn Account Safety

**LinkedIn actively detects and restricts accounts using automation.** Every API call carries risk. Follow these rules strictly to avoid account restriction or permanent ban.

### Before ANY API Call

1. **Always confirm with the user** before executing write operations (sending messages, connection requests, new chats). READ-ONLY operations (list accounts, list chats, read messages, user lookups) are lower risk but should still be spaced out.
2. **Check account status first** — run `GET /api/v1/accounts` and verify the source status is NOT `"CREDENTIALS"` or disconnected before proceeding.
3. **Never batch or loop API calls** — no bulk sends, no rapid-fire requests. One action at a time, with user confirmation between each.

### Pre-Flight Guards (MANDATORY before write operations)

Before executing ANY write operation, you MUST perform these read-only checks first. Do NOT skip these even if the user says "just send it."

#### Before sending a connection request (`POST /api/v1/users/invite`):
1. **Look up the target profile** — `GET /api/v1/users/{slug}?account_id={id}`
2. **Check `network_distance`** — must be `SECOND_DEGREE` or `THIRD_DEGREE`
   - If `FIRST_DEGREE` → already connected, SKIP the invite
   - If `is_relationship: true` → already connected, SKIP
3. **Check existing connections** — `GET /api/v1/users/relations?account_id={id}` and search for the target's provider_id
4. **If any check fails** → inform the user and do NOT send the request

#### Before sending a message (`POST /api/v1/chats` or `POST /api/v1/chats/{id}/messages`):
1. **Check for existing chat** — `GET /api/v1/chats?account_id={id}` and look for existing conversation with this person
2. **If starting a new chat** — verify target is `FIRST_DEGREE` (must be connected)
3. **If following up in existing chat** — `GET /api/v1/chats/{id}/messages` to check for replies first (stop sequence on reply)

#### General rules:
- **One write operation per user confirmation** — never chain writes
- **Add 45-90s random delay** before every write operation: `sleep $(python3 -c "import random; print(random.uniform(45, 90))")`
- **If any pre-flight check returns an error, STOP** — do not proceed to the write operation

#### Usage-Limit Checks (MANDATORY before ANY automated action)

Before performing any action that counts toward LinkedIn's rate limits, you MUST check current usage for the session/day. These limits apply to ALL actions, not just writes.

| Action Type | How to Check Current Usage | Daily Cap | Stop Threshold |
|-------------|---------------------------|-----------|----------------|
| **Connection requests** | `GET /api/v1/users/invitations/sent?account_id={id}` — count today's entries | 20-30 | STOP if near limit |
| **Profile lookups** | Track in-session count (no API to query history) | 80 | STOP at 80 |
| **New messages (new chats)** | `GET /api/v1/chats?account_id={id}` — count chats created today | 30-50 | STOP if near limit |
| **Messages (existing chats)** | Track in-session count | 100 | STOP at 100 |
| **LinkedIn searches** | Track in-session count | 30 | STOP at 30 |

**Rules:**
- **Report current usage to the user** before every write operation — e.g., "You've sent 12 connection requests today (limit: 25). Proceed?"
- **For new/reconnected accounts (< 30 days)**, use the warmup limits from the Hard Limits table — these are much lower
- **If you cannot determine current usage, assume you are near the limit** and warn the user before proceeding
- **Never proceed if usage count is unknown AND the user cannot confirm** the day's activity

### ⚠️ Hard Limits (CRITICAL — Check Usage Before Every Action)

| Action | Daily Limit | Weekly Limit | Notes |
|--------|------------|--------------|-------|
| Connection requests | 20-30 max | 100 max | LinkedIn's official cap is ~100/week. Stay well under. |
| Messages (new convos) | 30-50 max | — | Start at 15/day for new accounts, increase by 5/week |
| Messages (existing chats) | 100 max | — | Lower risk but still throttle |
| Profile lookups | 80 max | — | Counts toward LinkedIn's profile view limits |
| InMails | 30-50 max | — | Subscription-dependent |

### Behavioral Rules

- **Randomize timing** — never send requests at fixed intervals. Space calls by at least 30-60 seconds with random variation.
- **Work during business hours only** — LinkedIn flags off-hours automation.
- **Warm up new/reconnected accounts** — start with minimal activity (5-10 actions/day) and increase gradually over 2+ weeks.
- **Personalize messages** — generic/templated messages increase spam reports and restriction risk.
- **Monitor acceptance rates** — if connection request acceptance drops below 30%, stop sending immediately.
- **Never test with real API calls unless explicitly asked** — always show the curl command and let the user decide whether to execute it.

### If Account Gets Restricted

1. **Stop ALL automation immediately** — do not make any API calls
2. Wait at least 1 week before any automated activity
3. Re-authenticate manually through the Unipile dashboard
4. Warm up slowly: 5 actions/day for week 1, 10 for week 2, etc.
5. Consider using LinkedIn manually for 2-4 weeks before resuming API use

## Quick Start

All requests use:
- **Base URL**: `https://api22.unipile.com:15258`
- **Auth**: Header `X-API-KEY: <api_key>`
- **Content-Type**: `application/json`

## Core Workflow

1. **Verify connection** — `GET /api/v1/accounts` to confirm API key and account are active (check source status is NOT `"CREDENTIALS"`)
2. **Look up a user** — `GET /api/v1/users/{linkedin_slug}?account_id={id}` to get their profile/provider_id
3. **List chats** — `GET /api/v1/chats?account_id={id}` to find existing conversations
4. **Send a message** — `POST /api/v1/chats/{chat_id}/messages` for existing chats, or `POST /api/v1/chats` to start a new conversation
5. **Send connection request** — `POST /api/v1/users/invite` with provider_id and optional message (max 300 chars)
6. **Read messages** — `GET /api/v1/chats/{chat_id}/messages` to see conversation history

## Important Notes

- You can only message LinkedIn users who are **1st-degree connections**
- The `account_id` parameter is required on most endpoints
- User lookup uses the LinkedIn **public identifier** (slug from their profile URL)
- New conversations require the recipient's `attendees_ids` (their provider_id from user lookup)
- Connection request notes are limited to **300 characters** (paid accounts) or **200 characters** (free accounts)

## Detailed Reference

See [references/api-reference.md](references/api-reference.md) for full endpoint documentation with curl examples, error handling, and n8n integration notes.
