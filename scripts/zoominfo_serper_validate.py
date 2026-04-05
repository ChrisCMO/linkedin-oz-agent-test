#!/usr/bin/env python3
"""
ZoomInfo Company Validation — LinkedIn URL + Location via Google Search.
Two searches per company:
  1. site:linkedin.com/company "Company Name" → LinkedIn company URL
  2. "Company Name" company location address Seattle Washington → address/city/state

Backends: Serper.dev (fast, $0.002/company) or Apify Google SERP (slower, ~$0.007/company)
Supports checkpointing, resume, backend switching, and API key switching.

Usage:
  python scripts/zoominfo_serper_validate.py --count 5
  python scripts/zoominfo_serper_validate.py --count 500 --serper-key YOUR_KEY
  python scripts/zoominfo_serper_validate.py --count 100 --backend apify
  python scripts/zoominfo_serper_validate.py --all
  python scripts/zoominfo_serper_validate.py --retry
  python scripts/zoominfo_serper_validate.py --split
"""

import os, sys, csv, json, time, argparse, re, requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

BASE = os.path.join(os.path.dirname(__file__), "..")
FILTER_DIR = os.path.join(BASE, "docs", "deliverables", "week2", "universe", "untapped", "zoominfo-filtering")
INPUT_FILE = os.path.join(FILTER_DIR, "raw-list", "zoominfo_raw_10116.csv")
CHECKPOINT_FILE = os.path.join(FILTER_DIR, "checkpoint.json")
SCAN_LOG = os.path.join(FILTER_DIR, "scan_log.csv")
FOUND_FILE = os.path.join(FILTER_DIR, "found", "zoominfo_linkedin_found.csv")
NOT_FOUND_FILE = os.path.join(FILTER_DIR, "not-found", "zoominfo_linkedin_not_found.csv")
RETRY_FILE = os.path.join(FILTER_DIR, "errors", "retry_queue.csv")

SERPER_SEARCH_URL = "https://google.serper.dev/search"
SERPER_PLACES_URL = "https://google.serper.dev/places"

# Apify
APIFY_ACTOR_ID = "nFJndFXA5zjCTuudP"
APIFY_BASE = "https://api.apify.com/v2"

INPUT_FIELDS = ["icp_industry", "company_name", "industry_keyword", "zi_id",
                "zi_industry", "employees", "revenue", "city", "state", "website", "phone"]
SERPER_FIELDS = [
    "serper_linkedin_url", "serper_linkedin_name", "serper_snippet", "serper_name_match",
    "serper_place_name", "serper_address", "serper_city", "serper_state",
    "serper_rating", "serper_cid",
    "serper_status",  # found_both, found_linkedin_only, found_place_only, not_found, error, credits_exhausted
]
ALL_FIELDS = INPUT_FIELDS + SERPER_FIELDS


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(processed_ids):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(processed_ids), f)


def clean_linkedin_name(title):
    name = re.sub(r'\s*\|\s*LinkedIn\s*$', '', title)
    name = re.sub(r'\s*-\s*LinkedIn\s*$', '', name)
    name = re.sub(r'\s*on LinkedIn.*$', '', name)
    return name.strip()


def name_match_quality(input_name, serper_name):
    a = input_name.lower().strip()
    b = serper_name.lower().strip()
    for suffix in [' llc', ' inc', ' inc.', ' corp', ' corporation', ' co', ' co.', ' ltd', ' group', ' company']:
        a = a.replace(suffix, '').strip()
        b = b.replace(suffix, '').strip()
    if a == b:
        return "exact"
    if a in b or b in a:
        return "partial"
    a_words = a.split()[:3]
    b_words = b.split()[:3]
    if len(a_words) >= 2 and len(b_words) >= 2 and a_words[:2] == b_words[:2]:
        return "partial"
    return "low"


def parse_address(address_str):
    """Extract city and state from address like '227 Westlake Ave N, Seattle, WA 98109'"""
    city = ""
    state = ""
    if not address_str:
        return city, state
    parts = [p.strip() for p in address_str.split(",")]
    if len(parts) >= 3:
        city = parts[-2].strip()
        # State is usually in the last part with zip
        state_zip = parts[-1].strip()
        state_match = re.match(r'([A-Z]{2})\s*\d*', state_zip)
        if state_match:
            state = state_match.group(1)
    elif len(parts) == 2:
        city = parts[0].strip()
        state_match = re.match(r'([A-Z]{2})\s*\d*', parts[1].strip())
        if state_match:
            state = state_match.group(1)
    return city, state


def serper_call(url, payload, serper_key, retries=3):
    """Make a Serper API call with retry logic. Returns (data_dict, error_string)."""
    for attempt in range(retries):
        try:
            resp = requests.post(url,
                headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                json=payload, timeout=15)

            if resp.status_code in (422, 403, 401):
                return None, "credits_exhausted"
            if resp.status_code == 429:
                time.sleep(10 * (attempt + 1))
                continue

            resp.raise_for_status()
            data = resp.json()
            return data, None

        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
            else:
                return None, f"error: {str(e)[:50]}"
    return None, "error: max retries"


def apify_call(query, retries=2):
    """Run a single Google search via Apify. Returns (data_dict, error_string).
    data_dict has 'organic' key with list of {title, link, snippet} like Serper."""
    apify_token = os.environ.get("APIFY_API_KEY", "")
    if not apify_token:
        return None, "error: no APIFY_API_KEY"

    apify_headers = {"Authorization": f"Bearer {apify_token}", "Content-Type": "application/json"}

    for attempt in range(retries):
        try:
            r = requests.post(f"{APIFY_BASE}/acts/{APIFY_ACTOR_ID}/runs",
                headers=apify_headers,
                json={
                    "queries": query,
                    "maxPagesPerQuery": 1,
                    "resultsPerPage": 5,
                    "countryCode": "us",
                }, timeout=30)

            if r.status_code == 402:
                return None, "credits_exhausted"
            if r.status_code != 201:
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
                return None, f"error: start failed {r.status_code}"

            run_id = r.json()["data"]["id"]
            ds = r.json()["data"]["defaultDatasetId"]

            # Poll for completion
            for _ in range(30):
                time.sleep(3)
                status = requests.get(f"{APIFY_BASE}/actor-runs/{run_id}",
                    headers=apify_headers, timeout=15).json()["data"]["status"]
                if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                    break

            if status != "SUCCEEDED":
                return None, f"error: run {status}"

            items = requests.get(f"{APIFY_BASE}/datasets/{ds}/items",
                headers=apify_headers, timeout=15).json()

            # Convert to Serper-like format
            organic = []
            for item in items:
                for r2 in item.get("organicResults", []):
                    organic.append({
                        "title": r2.get("title", ""),
                        "link": r2.get("url", ""),
                        "snippet": r2.get("description", "")[:200],
                    })

            return {"organic": organic}, None

        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(5)
            else:
                return None, f"error: {str(e)[:50]}"

    return None, "error: max retries"


def search_company(company_name, serper_key, backend="serper"):
    """
    Two Google searches per company:
      1. LinkedIn X-ray search
      2. Location search
    Only populates fields if real data is found. Returns dict of serper fields.
    Backend: 'serper' or 'apify'
    """
    result = {f: "" for f in SERPER_FIELDS}
    result["serper_status"] = "not_found"

    if not company_name or not company_name.strip():
        result["serper_status"] = "skipped_blank_name"
        return result

    has_linkedin = False
    has_place = False

    # --- Call 1: LinkedIn X-ray search ---
    def do_search(query):
        if backend == "apify":
            return apify_call(query)
        else:
            return serper_call(SERPER_SEARCH_URL, {"q": query, "num": 3}, serper_key)

    data, err = do_search(f'site:linkedin.com/company "{company_name}"')

    if err == "credits_exhausted":
        result["serper_status"] = "credits_exhausted"
        return result
    if err:
        result["serper_status"] = "error"
        return result

    if data:
        organic = data.get("organic", [])
        linkedin_results = [r for r in organic if "linkedin.com/company" in r.get("link", "")]

        if linkedin_results:
            top = linkedin_results[0]
            url = top.get("link", "").split("?")[0].rstrip("/")
            title = top.get("title", "")
            snippet = top.get("snippet", "")[:200]
            li_name = clean_linkedin_name(title)
            match_q = name_match_quality(company_name, li_name)

            # Only count as found if URL is not blank and name match isn't garbage
            if url and li_name:
                result["serper_linkedin_url"] = url
                result["serper_linkedin_name"] = li_name
                result["serper_snippet"] = snippet
                result["serper_name_match"] = match_q
                has_linkedin = True

    time.sleep(0.3 if backend == "serper" else 1)

    # --- Call 2: Regular search for location ---
    data2, err2 = do_search(f'"{company_name}" company location address Seattle Washington')

    if err2 == "credits_exhausted":
        result["serper_status"] = "credits_exhausted_after_search"
        if has_linkedin:
            result["serper_status"] = "found_linkedin_only"
        return result
    if err2:
        if has_linkedin:
            result["serper_status"] = "found_linkedin_only"
        else:
            result["serper_status"] = "error"
        return result

    if data2:
        # Check knowledgeGraph first (most structured)
        kg = data2.get("knowledgeGraph", {})
        kg_address = kg.get("address", "")

        # Then check organic snippets for address patterns
        location_text = kg_address
        if not location_text:
            for r in data2.get("organic", []):
                snippet = r.get("snippet", "")
                title = r.get("title", "")
                # Look for WA addresses in snippet
                if re.search(r'(?:WA|Washington)\s*\d{5}', snippet) or ', WA' in snippet or 'Seattle' in snippet or 'Bellevue' in snippet:
                    location_text = snippet
                    break

        if location_text:
            # Try to extract structured address
            # Pattern: "Street, City, WA ZIP"
            addr_match = re.search(r'([\d]+[^,]+),\s*([A-Za-z\s]+),\s*(WA|Washington)\s*(\d{5})?', location_text)
            if addr_match:
                street = addr_match.group(1).strip()
                city = addr_match.group(2).strip()
                state = "WA"
                zip_code = addr_match.group(4) or ""
                full_address = f"{street}, {city}, WA {zip_code}".strip().rstrip(",").strip()
                result["serper_address"] = full_address
                result["serper_city"] = city
                result["serper_state"] = state
                has_place = True
            else:
                # Try simpler pattern: "City, WA" or "in City, WA"
                city_match = re.search(r'(?:in\s+)?([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s*(?:WA|Washington)', location_text)
                if city_match:
                    city = city_match.group(1).strip()
                    result["serper_city"] = city
                    result["serper_state"] = "WA"
                    result["serper_address"] = location_text[:150]
                    has_place = True

            # Store the raw snippet for reference
            if not result["serper_place_name"]:
                result["serper_place_name"] = location_text[:150]

    # Set final status
    if has_linkedin and has_place:
        result["serper_status"] = "found_both"
    elif has_linkedin:
        result["serper_status"] = "found_linkedin_only"
    elif has_place:
        result["serper_status"] = "found_place_only"
    else:
        result["serper_status"] = "not_found"

    return result


def append_to_scan_log(row):
    file_exists = os.path.exists(SCAN_LOG) and os.path.getsize(SCAN_LOG) > 0
    with open(SCAN_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def split_results():
    if not os.path.exists(SCAN_LOG):
        print("No scan_log.csv found.")
        return

    found = []
    not_found = []
    errors = []

    with open(SCAN_LOG, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            status = row.get("serper_status", "")
            if status in ("found_both", "found_linkedin_only", "found_place_only"):
                found.append(row)
            elif status in ("error", "credits_exhausted", "credits_exhausted_after_search"):
                errors.append(row)
            else:
                not_found.append(row)

    place_only = [r for r in found if r.get("serper_status") == "found_place_only"]
    found = [r for r in found if r.get("serper_status") != "found_place_only"]
    PLACE_ONLY_FILE = os.path.join(FILTER_DIR, "not-found", "zoominfo_place_only_no_linkedin.csv")

    for filepath, rows, label in [
        (FOUND_FILE, found, "found (has LinkedIn)"),
        (PLACE_ONLY_FILE, place_only, "place only (no LinkedIn)"),
        (NOT_FOUND_FILE, not_found, "not-found"),
        (RETRY_FILE, errors, "errors/retry"),
    ]:
        if rows:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=ALL_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            print(f"  {label}: {len(rows)} → {os.path.basename(filepath)}")

    print(f"\n  Total: found={len(found)}, place_only={len(place_only)}, not_found={len(not_found)}, errors={len(errors)}")


def main():
    parser = argparse.ArgumentParser(description="ZoomInfo Serper.dev LinkedIn + Location validation")
    parser.add_argument("--count", type=int, default=5, help="Number of companies to process")
    parser.add_argument("--all", action="store_true", help="Process all remaining")
    parser.add_argument("--serper-key", default="296bf89238cb95fea9024757279ba723019d5817", help="Serper.dev API key")
    parser.add_argument("--backend", default="serper", choices=["serper", "apify"], help="Search backend: serper or apify")
    parser.add_argument("--retry", action="store_true", help="Re-process error companies")
    parser.add_argument("--split", action="store_true", help="Just split scan_log into found/not-found")
    args = parser.parse_args()

    if args.split:
        split_results()
        return

    input_file = RETRY_FILE if args.retry else INPUT_FILE
    if args.retry and not os.path.exists(RETRY_FILE):
        print("No retry_queue.csv found.")
        return

    with open(input_file, encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    processed = load_checkpoint()
    remaining = [r for r in all_rows if r.get("zi_id", "") not in processed]

    print(f"Input: {len(all_rows)} companies")
    print(f"Already processed: {len(processed)}")
    print(f"Remaining: {len(remaining)}")

    if not remaining:
        print("All done! Use --split to regenerate output files.")
        return

    batch_size = len(remaining) if args.all else min(args.count, len(remaining))
    batch = remaining[:batch_size]
    cost_per = 0.002 if args.backend == "serper" else 0.007
    print(f"Processing: {batch_size} companies (2 searches each via {args.backend})")
    print(f"Estimated cost: ${batch_size * cost_per:.2f}")
    if args.backend == "serper":
        print(f"Serper key: {args.serper_key[:8]}...")
    else:
        print(f"Backend: Apify (~12s per company, ~{batch_size * 12 / 60:.0f} min total)")

    print()

    counts = {"found_both": 0, "found_linkedin_only": 0, "found_place_only": 0, "not_found": 0, "error": 0}

    for i, row in enumerate(batch, 1):
        company_name = (row.get("company_name") or "").strip()
        zi_id = (row.get("zi_id") or "").strip()

        if not zi_id or zi_id in processed:
            continue

        print(f"[{i}/{batch_size}] {company_name}", end="", flush=True)

        serper_result = search_company(company_name, args.serper_key, backend=args.backend)

        # Check credits exhausted
        if serper_result["serper_status"] in ("credits_exhausted", "credits_exhausted_after_search"):
            if serper_result["serper_status"] == "credits_exhausted_after_search" and serper_result["serper_linkedin_url"]:
                # Save the LinkedIn result we got before places failed
                output_row = {field: row.get(field, "") for field in INPUT_FIELDS}
                output_row.update(serper_result)
                append_to_scan_log(output_row)
                processed.add(zi_id)

            print(f"\n\n{'='*60}")
            print(f"CREDITS EXHAUSTED after {i-1} fully processed companies.")
            print(f"Total in checkpoint: {len(processed)}")
            print(f"\nResume: python scripts/zoominfo_serper_validate.py --count {batch_size - i + 1} --serper-key NEW_KEY")
            print(f"{'='*60}")
            save_checkpoint(processed)
            split_results()
            return

        # Build output row
        output_row = {field: row.get(field, "") for field in INPUT_FIELDS}
        output_row.update(serper_result)

        status = serper_result["serper_status"]
        counts[status] = counts.get(status, 0) + 1

        if status == "found_both":
            print(f" → BOTH | {serper_result['serper_name_match']} | {serper_result['serper_linkedin_url'][:50]} | {serper_result['serper_address']}")
        elif status == "found_linkedin_only":
            print(f" → LI only ({serper_result['serper_name_match']}) | {serper_result['serper_linkedin_url'][:50]}")
        elif status == "found_place_only":
            print(f" → Place only | {serper_result['serper_address']}")
        elif status == "error":
            print(f" → ERROR")
        else:
            print(f" → not found")

        append_to_scan_log(output_row)
        processed.add(zi_id)
        save_checkpoint(processed)

        time.sleep(0.3 if args.backend == "serper" else 1)

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE")
    print(f"Processed: {batch_size}")
    for status, count in sorted(counts.items(), key=lambda x: -x[1]):
        if count:
            print(f"  {status}: {count}")
    print(f"Total in checkpoint: {len(processed)}/{len(all_rows)}")
    print(f"\nSplitting results...")
    split_results()
    print(f"\nDone!")


if __name__ == "__main__":
    main()
