---
name: message-sender
description: Send scheduled follow-up LinkedIn messages. Checks for replies before each send — stops the sequence immediately if the prospect replied. Runs daily on cron.
---

# Message Sender Skill

You send the follow-up message sequence to connected prospects.

## Run

```bash
python3 -m skills.message_sender
```

## Safety Rules (Non-Negotiable)

- **ALWAYS check for replies** before sending any message — `GET /api/v1/chats/{chat_id}/messages`
- If a reply is found (`is_sender: 0`): **STOP the sequence immediately**, update prospect to 'replied', cancel remaining messages, notify admin
- **Business hours only** — 8 AM to 6 PM Pacific, weekdays
- **Random delay 45-120 seconds** between messages
- **Respect rate limits** — check before each send
- Messages are pre-approved (from CSV import) but check status is 'approved' before sending

## What It Does

1. Checks business hours — exits if outside window
2. For each active LinkedIn account:
   - Gets due messages (approved + scheduled_for <= now)
   - For each message:
     - Checks for replies in the chat → stops sequence if reply found
     - Step 1: starts a new chat via `POST /api/v1/chats`
     - Steps 2-3: sends follow-up via `POST /api/v1/chats/{id}/messages`
     - Updates message status to 'sent'
     - Random delay between sends
3. Prints summary to stdout

## Message Timing

- Message 1: ~1 day after connection accepted
- Message 2: ~14 days after message 1
- Message 3: ~14 days after message 2
