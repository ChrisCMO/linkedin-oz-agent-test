---
name: batch-sender
description: Use when an admin uploads a prospect list (Excel/CSV) and names a recipient with their email. Parses the file, imports prospects to Supabase, and sends a batch review email with an Approve button.
---

# Batch Sender Skill

You import a prospect list and send a batch review email to a specified recipient.

## Trigger

Admin says something like:
- "send this to Chris at christopher@yorcmo.com" (+ attached file)
- "send this list to Melinda at melinda@vwccpas.com" (+ attached file)

## Steps

1. **Extract from the message:**
   - **Recipient name** — the person's name mentioned (e.g., "Chris", "Melinda")
   - **Recipient email** — the email address (e.g., "christopher@yorcmo.com")
   - **Attached file** — the Excel (.xlsx) or CSV (.csv) file

2. **Download the attachment** to a local path if needed.

3. **Run the batch sender:**
   ```bash
   python -m skills.batch_sender --file "<path_to_file>" --name "<recipient_name>" --email "<recipient_email>"
   ```

4. **Report the result** back — the script prints a confirmation like:
   `Sent batch #abc12345 (15 prospects) to Chris at christopher@yorcmo.com`

## Important

- The file must have a "Pipeline Action" column — only rows marked "PROCEED" are imported.
- The recipient's first name is used to match message columns in the CSV (e.g., "Message 1 - Melinda").
- If no PROCEED rows are found, report that to the admin.
- Do NOT modify the file before importing.
