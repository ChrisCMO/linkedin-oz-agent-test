"""Benchmark company_scorer pipeline — import sample CSV, run scorer, report timing.

Usage:
    python -m scripts.benchmark_scorer --csv /path/to/file.csv --limit 5
    python -m scripts.benchmark_scorer --cleanup  # remove benchmark rows
"""

import argparse
import csv
import sys
import time
import uuid
from datetime import datetime, timezone

from db.connect import get_supabase
from skills.company_scorer import process_companies, load_icp_config
from lib.apify import extract_domain

TENANT_ID = "00000000-0000-0000-0000-000000000001"
BATCH_NAME = "benchmark-test"


def import_csv(csv_path: str, limit: int) -> list[dict]:
    """Import companies from Apify CSV into raw_companies as 'raw'."""
    sb = get_supabase()
    rows = []

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break

            name = row.get("Company Name", "").strip()
            if not name:
                continue

            domain = extract_domain(row.get("Website"))
            linkedin_url = row.get("LinkedIn URL", "").strip()
            location_parts = [
                row.get("Address", ""),
                row.get("City", ""),
                row.get("State", ""),
            ]
            location = ", ".join(p for p in location_parts if p).strip(", ")

            employees = None
            try:
                employees = int(row.get("Employee Count", 0))
            except (ValueError, TypeError):
                pass

            li_followers = None
            try:
                li_followers = int(row.get("Follower Count", 0))
            except (ValueError, TypeError):
                pass

            record = {
                "id": str(uuid.uuid4()),
                "tenant_id": TENANT_ID,
                "name": name,
                "domain": domain,
                "website": row.get("Website", ""),
                "linkedin_url": linkedin_url,
                "industry": row.get("Industry", ""),
                "employees": employees,
                "li_followers": li_followers,
                "li_description": (row.get("Description") or "")[:500],
                "li_tagline": row.get("Tagline", ""),
                "li_founded": row.get("Founded Year", ""),
                "location": location,
                "source": "apify_linkedin",
                "source_data": {
                    "csv_row": i,
                    "specialities": row.get("Specialities", ""),
                    "employee_range": row.get("Employee Range", ""),
                },
                "pipeline_status": "raw",
                "batch_name": BATCH_NAME,
                "dedup_key": f"{name.lower().strip()}|{domain or ''}",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            rows.append(record)

    if rows:
        sb.table("raw_companies").insert(rows).execute()
        print(f"Imported {len(rows)} companies as 'raw' (batch: {BATCH_NAME})")

    return rows


def run_benchmark(csv_path: str, limit: int):
    """Import, run scorer, report timing."""
    sb = get_supabase()

    # Import
    print(f"\n=== Importing {limit} companies from {csv_path} ===")
    t0 = time.time()
    rows = import_csv(csv_path, limit)
    t_import = time.time() - t0
    print(f"  Import: {t_import:.1f}s")

    if not rows:
        print("No rows imported — nothing to benchmark.")
        return

    # Load companies back from DB (scorer expects DB rows)
    ids = [r["id"] for r in rows]
    companies = []
    for rid in ids:
        result = sb.table("raw_companies").select("*").eq("id", rid).single().execute()
        if result.data:
            companies.append(result.data)

    print(f"\n=== Running company_scorer on {len(companies)} companies ===")
    icp_config = load_icp_config(sb, TENANT_ID)

    t1 = time.time()
    scored, errors, skipped = process_companies(sb, companies, icp_config)
    t_total = time.time() - t1

    print(f"\n{'='*60}")
    print(f"BENCHMARK RESULTS")
    print(f"{'='*60}")
    print(f"  Companies:    {len(companies)}")
    print(f"  Scored:       {scored}")
    print(f"  Errors:       {errors}")
    print(f"  Skipped:      {skipped}")
    print(f"  Total time:   {t_total:.1f}s")
    print(f"  Per company:  {t_total / len(companies):.1f}s")
    print(f"{'='*60}")

    # Show results
    print(f"\nResults:")
    for rid in ids:
        result = sb.table("raw_companies").select(
            "name, icp_score, pipeline_action, pipeline_status"
        ).eq("id", rid).single().execute()
        if result.data:
            r = result.data
            print(f"  {r['name'][:40]:<40} score={r['icp_score']}  action={r['pipeline_action']}  status={r['pipeline_status']}")


def cleanup():
    """Remove all benchmark rows."""
    sb = get_supabase()
    result = sb.table("raw_companies").delete().eq("batch_name", BATCH_NAME).execute()
    count = len(result.data) if result.data else 0
    print(f"Cleaned up {count} benchmark rows")


def main():
    parser = argparse.ArgumentParser(description="Benchmark company_scorer pipeline")
    parser.add_argument("--csv", help="Path to Apify CSV file")
    parser.add_argument("--limit", type=int, default=5, help="Number of companies to test (default: 5)")
    parser.add_argument("--cleanup", action="store_true", help="Remove benchmark rows from DB")
    args = parser.parse_args()

    if args.cleanup:
        cleanup()
        return

    if not args.csv:
        print("Error: --csv is required (unless --cleanup)")
        sys.exit(1)

    run_benchmark(args.csv, args.limit)


if __name__ == "__main__":
    main()
