#!/usr/bin/env python3
"""Import pipeline data from CSV backups into a Supabase project.

Usage:
    python3 scripts/import_pipeline_data.py --backup-dir data/backups/20260410_120000
    python3 scripts/import_pipeline_data.py --backup-dir data/backups/20260410_120000 --tables raw_companies,prospects

Imports CSV backups created by export_pipeline_data.py. Idempotent — uses
upsert with dedup_key for raw_companies/companies_universe, and id-based
upsert for other tables. Safe to run multiple times.

IMPORTANT: Target Supabase project must have the schema already created
(run migration SQL files first).
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SECRET_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

BATCH_SIZE = 200

# Columns that contain JSONB data (need deserialization from CSV strings)
JSONB_COLUMNS = {
    "enrichment_data", "source_data", "raw_apollo_data", "score_breakdown",
    "icp_score_breakdown", "settings", "connection_notes", "partner_messages",
    "generation", "data", "provider_config", "rate_limits",
    "apollo_search_params", "scoring_config", "request_params",
    "response_summary", "raw_payload", "locations",
}

# Array columns (stored as JSON arrays in CSV)
ARRAY_COLUMNS = {
    "prospect_ids", "target_titles", "target_seniorities", "target_industries",
    "target_locations", "employee_count_ranges", "revenue_ranges", "keywords",
    "sic_codes", "naics_codes", "tech_stack",
}

# Tables and their upsert conflict columns
TABLE_UPSERT_KEYS = {
    "tenants": "id",
    "raw_companies": "id",
    "companies_universe": "id",
    "prospects": "id",
    "company_batches": "id",
    "contact_batches": "id",
    "batch_reviews": "id",
}

# Import order (respects foreign key dependencies)
IMPORT_ORDER = [
    "tenants",
    "company_batches",
    "contact_batches",
    "raw_companies",
    "companies_universe",
    "prospects",
    "batch_reviews",
]


def deserialize_row(row: dict) -> dict:
    """Deserialize JSONB and array columns from CSV strings."""
    out = {}
    for key, value in row.items():
        if value == "" or value is None:
            out[key] = None
            continue

        if key in JSONB_COLUMNS or key in ARRAY_COLUMNS:
            try:
                out[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                out[key] = value
        elif value == "true":
            out[key] = True
        elif value == "false":
            out[key] = False
        else:
            # Try to parse integers
            try:
                if "." not in value and value.lstrip("-").isdigit():
                    out[key] = int(value)
                else:
                    out[key] = value
            except (ValueError, AttributeError):
                out[key] = value

    return out


def read_csv(filepath: str) -> list[dict]:
    """Read CSV file and deserialize rows."""
    if not os.path.exists(filepath):
        return []

    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(deserialize_row(row))

    return rows


def upsert_batch(table: str, rows: list[dict], conflict_col: str) -> tuple[int, int]:
    """Upsert a batch of rows. Returns (success, errors)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"

    headers = {
        **HEADERS,
        "Prefer": f"resolution=merge-duplicates",
    }

    success = 0
    errors = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]

        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code in (200, 201):
            success += len(batch)
        else:
            # Try individual inserts on batch failure
            print(f"  Batch upsert failed ({resp.status_code}), retrying individually...")
            for row in batch:
                resp2 = requests.post(url, headers=headers, json=[row])
                if resp2.status_code in (200, 201):
                    success += 1
                else:
                    errors += 1
                    print(f"    ERROR: {row.get('id', '?')[:8]}... — {resp2.text[:100]}")

    return success, errors


def main():
    parser = argparse.ArgumentParser(description="Import pipeline data from CSV backups")
    parser.add_argument("--backup-dir", required=True, help="Directory containing CSV backup files")
    parser.add_argument("--tables", help="Comma-separated table names to import (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be imported without writing")
    args = parser.parse_args()

    if not os.path.isdir(args.backup_dir):
        print(f"ERROR: Backup directory not found: {args.backup_dir}")
        sys.exit(1)

    # Check manifest
    manifest_path = os.path.join(args.backup_dir, "manifest.json")
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        print(f"Backup from: {manifest.get('exported_at', 'unknown')}")
        print(f"Source: {manifest.get('supabase_url', 'unknown')}")
        print(f"Tenant: {manifest.get('tenant_id', 'unknown')}")
        print(f"Total rows: {manifest.get('total_rows', 'unknown')}")
        print()

    tables_filter = set(args.tables.split(",")) if args.tables else None

    print(f"Importing to: {SUPABASE_URL}")
    print(f"From: {args.backup_dir}\n")

    total_success = 0
    total_errors = 0

    for table in IMPORT_ORDER:
        if tables_filter and table not in tables_filter:
            continue

        filepath = os.path.join(args.backup_dir, f"{table}.csv")
        if not os.path.exists(filepath):
            continue

        rows = read_csv(filepath)
        if not rows:
            continue

        conflict_col = TABLE_UPSERT_KEYS.get(table, "id")

        print(f"Importing {table}: {len(rows)} rows (upsert on {conflict_col})...")

        if args.dry_run:
            print(f"  DRY RUN — would import {len(rows)} rows")
            continue

        success, errors = upsert_batch(table, rows, conflict_col)
        total_success += success
        total_errors += errors
        print(f"  {success} success, {errors} errors")

    print(f"\nImport complete: {total_success} success, {total_errors} errors")


if __name__ == "__main__":
    main()
