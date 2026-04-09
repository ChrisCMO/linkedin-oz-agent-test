#!/usr/bin/env python3
"""Export pipeline data from Supabase to timestamped CSV backups.

Usage:
    python3 scripts/export_pipeline_data.py --tenant-id <UUID>
    python3 scripts/export_pipeline_data.py --tenant-id <UUID> --output-dir data/backups/custom

Exports:
    raw_companies, companies_universe, prospects, company_batches, contact_batches

Each table is exported as a CSV with all columns. JSONB columns are serialized
as JSON strings. Files are timestamped for versioning.
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
}

PAGE_SIZE = 1000

# Tables to export with their tenant filter column (None = no tenant filter)
TABLES = [
    ("raw_companies", "tenant_id"),
    ("companies_universe", "tenant_id"),
    ("prospects", "tenant_id"),
    ("company_batches", "tenant_id"),
    ("contact_batches", "tenant_id"),
    ("batch_reviews", "tenant_id"),
    ("tenants", None),
]

# Columns that contain JSONB data (need special serialization)
JSONB_COLUMNS = {
    "enrichment_data", "source_data", "raw_apollo_data", "score_breakdown",
    "icp_score_breakdown", "settings", "connection_notes", "partner_messages",
    "generation", "data", "provider_config", "rate_limits",
    "apollo_search_params", "scoring_config", "request_params",
    "response_summary", "raw_payload", "locations",
}


def fetch_table(table: str, tenant_id: str | None, tenant_col: str | None) -> list[dict]:
    """Fetch all rows from a table, paginated."""
    rows = []
    offset = 0

    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*&order=created_at&offset={offset}&limit={PAGE_SIZE}"
        if tenant_id and tenant_col:
            url += f"&{tenant_col}=eq.{tenant_id}"

        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  ERROR fetching {table}: {resp.status_code} {resp.text[:200]}")
            break

        batch = resp.json()
        if not batch:
            break

        rows.extend(batch)
        offset += PAGE_SIZE

        if len(batch) < PAGE_SIZE:
            break

    return rows


def serialize_row(row: dict) -> dict:
    """Serialize JSONB columns to JSON strings for CSV storage."""
    out = {}
    for key, value in row.items():
        if key in JSONB_COLUMNS and value is not None and not isinstance(value, str):
            out[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, (list, dict)):
            out[key] = json.dumps(value, ensure_ascii=False)
        else:
            out[key] = value
    return out


def write_csv(rows: list[dict], filepath: str):
    """Write rows to CSV file."""
    if not rows:
        print(f"  No data to write for {filepath}")
        return

    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(serialize_row(row))

    print(f"  Wrote {len(rows)} rows → {filepath}")


def export_schema(output_dir: str):
    """Concatenate migration SQL files into a schema snapshot."""
    db_dir = os.path.join(BASE_DIR, "db")
    sql_files = sorted(f for f in os.listdir(db_dir) if f.endswith(".sql"))

    snapshot_path = os.path.join(output_dir, "schema_snapshot.sql")
    with open(snapshot_path, "w") as out:
        out.write(f"-- Schema snapshot exported {datetime.now(timezone.utc).isoformat()}\n")
        out.write(f"-- Source files: {', '.join(sql_files)}\n\n")
        for sql_file in sql_files:
            out.write(f"-- ========== {sql_file} ==========\n\n")
            with open(os.path.join(db_dir, sql_file)) as f:
                out.write(f.read())
            out.write("\n\n")

    print(f"  Schema snapshot → {snapshot_path} ({len(sql_files)} files)")


def main():
    parser = argparse.ArgumentParser(description="Export pipeline data to CSV backups")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID to export")
    parser.add_argument("--output-dir", help="Custom output directory (default: data/backups/YYYYMMDD_HHMMSS)")
    parser.add_argument("--tables", help="Comma-separated table names to export (default: all)")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or os.path.join(BASE_DIR, "data", "backups", timestamp)
    os.makedirs(output_dir, exist_ok=True)

    tables_filter = set(args.tables.split(",")) if args.tables else None

    print(f"Exporting pipeline data for tenant {args.tenant_id}")
    print(f"Output: {output_dir}\n")

    total_rows = 0
    for table, tenant_col in TABLES:
        if tables_filter and table not in tables_filter:
            continue

        print(f"Exporting {table}...")
        rows = fetch_table(table, args.tenant_id, tenant_col)
        total_rows += len(rows)

        filepath = os.path.join(output_dir, f"{table}.csv")
        write_csv(rows, filepath)

    # Export schema
    print("\nExporting schema...")
    export_schema(output_dir)

    # Write manifest
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "tenant_id": args.tenant_id,
        "total_rows": total_rows,
        "tables": {t: 0 for t, _ in TABLES},
        "supabase_url": SUPABASE_URL,
    }
    # Re-count from files
    for table, _ in TABLES:
        filepath = os.path.join(output_dir, f"{table}.csv")
        if os.path.exists(filepath):
            with open(filepath) as f:
                manifest["tables"][table] = sum(1 for _ in f) - 1  # minus header

    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nExport complete: {total_rows} total rows across {len(TABLES)} tables")
    print(f"Manifest → {manifest_path}")
    return output_dir


if __name__ == "__main__":
    main()
