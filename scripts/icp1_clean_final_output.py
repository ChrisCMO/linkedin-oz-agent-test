#!/usr/bin/env python3
"""
Clean the enriched contacts CSV:
1. Remove duplicates (by First Name + Last Name + Company)
2. Flag "Chris Chris" style name issues
3. Normalize LinkedIn URLs (http → https)
4. Add Apollo ID columns
5. Remove legacy activity columns
6. Reorder columns to clean structure
7. Print final stats
"""

import csv
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

INPUT_FILE = Path(__file__).resolve().parent.parent / "docs/deliverables/week2/scored/new/proceed_contacts_enriched_v2.csv"

# Columns to remove (legacy/redundant from old activity scraper)
REMOVE_COLUMNS = {
    "Recent Post Text",
    "Total Reactions Received",
    "Total Comments Received",
    "Reposts Count",
    "Total Feed Items",
    "Recent Post Date",
    "Posts Count",
}

# Desired column order
FINAL_COLUMNS = [
    "Company ICP Score", "Pipeline Action", "Company", "Industry", "Company Location",
    "Company LinkedIn URL", "Company LI Followers",
    "First Name", "Last Name", "Title", "Seniority",
    "LinkedIn URL", "LinkedIn Headline", "Role Verified",
    "LinkedIn Connections", "LinkedIn Followers", "Open to Work",
    "Email", "Email Status", "Apollo Person ID", "Apollo Company ID",
    "Activity Score", "Activity Level", "Activity Recommendation", "Activity Insights",
    "Posts Last 30 Days", "Reactions Last 30 Days", "Last Activity Date", "Days Since Last Activity",
    "LinkedIn Active Status",
    "Melinda's Connection Note", "Adrienne's Connection Note",
    "Message 1 - Melinda", "Message 2 - Melinda", "Message 3 - Melinda",
    "Message 1 - Adrienne", "Message 2 - Adrienne", "Message 3 - Adrienne",
    "Data Source",
]


def count_filled(row: dict) -> int:
    """Count non-empty fields in a row."""
    return sum(1 for v in row.values() if v and str(v).strip())


def normalize_linkedin_url(url: str) -> str:
    """Change http:// to https:// in LinkedIn URLs."""
    if url and url.startswith("http://"):
        return "https://" + url[7:]
    return url


def main():
    # --- Read ---
    print(f"Reading: {INPUT_FILE}")
    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        original_columns = reader.fieldnames or []
        rows = list(reader)
    total_original = len(rows)
    print(f"  Original rows: {total_original}")
    print(f"  Original columns ({len(original_columns)}): {original_columns}")

    # --- 1. Remove duplicates ---
    seen: dict[str, dict] = {}  # key -> best row
    duplicates_removed = []
    for row in rows:
        key = (
            (row.get("First Name") or "").strip().lower(),
            (row.get("Last Name") or "").strip().lower(),
            (row.get("Company") or "").strip().lower(),
        )
        if key in seen:
            existing = seen[key]
            if count_filled(row) > count_filled(existing):
                duplicates_removed.append(
                    f"  Dropped (less data): {existing.get('First Name')} {existing.get('Last Name')} @ {existing.get('Company')}"
                )
                seen[key] = row
            else:
                duplicates_removed.append(
                    f"  Dropped (less data): {row.get('First Name')} {row.get('Last Name')} @ {row.get('Company')}"
                )
        else:
            seen[key] = row

    deduped_rows = list(seen.values())
    num_dupes = total_original - len(deduped_rows)
    if duplicates_removed:
        print(f"\n  Duplicates removed ({num_dupes}):")
        for d in duplicates_removed:
            print(d)
    else:
        print(f"\n  No duplicates found.")

    # --- 2. Flag "Chris Chris" name issues ---
    print("\n  Name check (First == Last):")
    flagged = False
    for row in deduped_rows:
        fn = (row.get("First Name") or "").strip()
        ln = (row.get("Last Name") or "").strip()
        if fn and ln and fn.lower() == ln.lower():
            print(f"  WARNING: Duplicate name detected: '{fn} {ln}' @ {row.get('Company')}")
            flagged = True
    if not flagged:
        print("  No duplicate-name issues found.")

    # --- 3. Normalize LinkedIn URLs ---
    url_fixes = 0
    for row in deduped_rows:
        for col in ("LinkedIn URL", "Company LinkedIn URL"):
            old = row.get(col, "")
            new = normalize_linkedin_url(old)
            if old != new:
                row[col] = new
                url_fixes += 1
    print(f"\n  LinkedIn URL fixes (http→https): {url_fixes}")

    # --- 4. Add Apollo ID columns (blank) ---
    for row in deduped_rows:
        row["Apollo Person ID"] = ""
        row["Apollo Company ID"] = ""

    # --- 5. Remove legacy columns ---
    removed = [c for c in REMOVE_COLUMNS if c in original_columns]
    print(f"\n  Removing legacy columns: {removed}")
    for row in deduped_rows:
        for col in REMOVE_COLUMNS:
            row.pop(col, None)

    # --- 6. Reorder & write ---
    # Check for any columns in data not in FINAL_COLUMNS
    sample_keys = set(deduped_rows[0].keys()) if deduped_rows else set()
    missing_in_order = sample_keys - set(FINAL_COLUMNS)
    missing_in_data = set(FINAL_COLUMNS) - sample_keys
    if missing_in_order:
        print(f"\n  WARNING: Columns in data but NOT in final order (will be dropped): {missing_in_order}")
    if missing_in_data:
        print(f"\n  NOTE: Columns in final order but not in data (will be blank): {missing_in_data}")

    # Write output
    print(f"\n  Writing: {INPUT_FILE}")
    with open(INPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(deduped_rows)

    # --- 7. Final stats ---
    print(f"\n--- Final Stats ---")
    print(f"  Total contacts: {len(deduped_rows)}")
    print(f"  Duplicates removed: {num_dupes}")
    print(f"  Columns in final output: {len(FINAL_COLUMNS)}")
    print(f"  Column list: {FINAL_COLUMNS}")
    print("Done.")


if __name__ == "__main__":
    main()
