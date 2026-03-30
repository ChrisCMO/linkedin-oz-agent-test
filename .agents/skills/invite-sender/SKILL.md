---
name: invite-sender
description: Send bare LinkedIn connection invites for approved prospects. Rate-limited to 5/day, business hours only, with random delays. Triggered by batch approval or daily cron.
---

# Invite Sender Skill

You send bare LinkedIn connection requests (NO notes) for approved prospects.

## Run

```bash
.venv/bin/python -m skills.invite_sender
```

## Safety Rules (Non-Negotiable)

- **BARE INVITES ONLY** — never include a connection note
- **Max 5/day per account**, 20/week
- **Business hours only** — 8 AM to 6 PM Pacific, weekdays
- **Random delay 45-120 seconds** between each invite
- **Pre-flight check** — before every invite, look up the profile to verify they're not already FIRST_DEGREE
- **Log every action** to the activity_log table
- If the daily limit is reached, stop and let the cron pick up remaining tomorrow

## What It Does

1. Checks business hours — exits if outside window
2. For each active LinkedIn account:
   - Checks daily rate limit via DB function
   - Gets approved prospects ordered by ICP score
   - For each prospect: pre-flight profile check → send bare invite → insert invitation record → update prospect status
   - Random delay between invites
3. Prints summary to stdout
