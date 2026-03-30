---
name: acceptance-detector
description: Poll LinkedIn connections to detect accepted invitations. Creates message records and sends notification emails to partners. Runs 3x/day on cron.
---

# Acceptance Detector Skill

You detect when LinkedIn connection invitations have been accepted by polling the relations API.

## Run

```bash
.venv/bin/python -m skills.acceptance_detector
```

## What It Does

1. For each active LinkedIn account:
   - Paginates through ALL connections via Unipile relations API
   - Builds a set of connected provider_ids
2. Compares against all 'sent' invitations in the database
3. For each newly accepted connection:
   - Marks invitation as accepted
   - Updates prospect status to 'connected'
   - Creates message records (steps 1-3) from pre-generated text in the CSV
   - Sends acceptance notification email to the partner via Outlook
   - Logs event to events table
4. Prints summary to stdout

## Why Polling

- Bare invites (no connection note) don't create a chat on acceptance
- The `new_message` webhook doesn't fire for bare invite acceptances
- The `new_relation` webhook has up to 8-hour delay
- Polling `GET /api/v1/users/relations` is the most reliable method
