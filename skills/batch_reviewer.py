"""AI Batch Reviewer — pre-screen prospects before Chad's review.

Checks for duplicates, competitors, blacklisted companies, inactive profiles,
and bad titles. Uses GPT-5.4 for borderline title relevance checks.

Usage:
    python3 -m skills.batch_reviewer --tenant-id X --month 2026-04
    python3 -m skills.batch_reviewer --tenant-id X --month 2026-04 --dry-run
"""

import argparse
import csv
import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

from db.connect import get_supabase
from lib.title_tiers import classify_title_tier
from mvp.backend.config import get_openai
from skills.helpers import setup_logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BLACKLIST_FILE = os.path.join(BASE_DIR, "data", "blacklist.csv")
COMPETITORS_FILE = os.path.join(BASE_DIR, "data", "competitors.csv")

REVIEW_MODEL = "gpt-5.4"


# ---------------------------------------------------------------------------
# Load exclusion lists
# ---------------------------------------------------------------------------

def load_exclusion_list(filepath: str) -> dict:
    """Load a CSV with company_name, domain columns into a lookup dict."""
    data = {"names": set(), "domains": set(), "entries": []}
    if not os.path.exists(filepath):
        logger.warning("Exclusion file not found: %s", filepath)
        return data
    with open(filepath) as f:
        for row in csv.DictReader(f):
            name = row.get("company_name", "").strip().lower()
            domain = row.get("domain", "").strip().lower()
            reason = row.get("reason", "")
            if name:
                data["names"].add(name)
            if domain:
                data["domains"].add(domain)
            data["entries"].append({"name": name, "domain": domain, "reason": reason})
    return data


def is_excluded(company_name: str, domain: str | None, exclusion_list: dict) -> str | None:
    """Check if company matches exclusion list. Returns reason or None."""
    name_lower = company_name.strip().lower()
    domain_lower = (domain or "").strip().lower()
    if domain_lower:
        for entry in exclusion_list["entries"]:
            if entry["domain"] and entry["domain"] == domain_lower:
                return entry["reason"]
    for entry in exclusion_list["entries"]:
        if entry["name"] and (entry["name"] in name_lower or name_lower in entry["name"]):
            return entry["reason"]
    return None


# ---------------------------------------------------------------------------
# Rule-based checks
# ---------------------------------------------------------------------------

def check_duplicates(prospects: list[dict]) -> dict[str, str]:
    """Find duplicate contacts. Returns {prospect_id: reason}."""
    skips = {}

    # By linkedin_slug
    slug_seen = {}
    for p in prospects:
        slug = (p.get("linkedin_slug") or "").lower()
        if not slug:
            continue
        if slug in slug_seen:
            skips[p["id"]] = f"Duplicate LinkedIn profile (same as {slug_seen[slug]})"
        else:
            slug_seen[slug] = f"{p.get('first_name', '')} {p.get('last_name', '')}"

    # By first+last+company
    name_seen = {}
    for p in prospects:
        key = f"{p.get('first_name', '').lower()}|{p.get('last_name', '').lower()}|{p.get('company_name', '').lower()}"
        if key in name_seen and p["id"] not in skips:
            skips[p["id"]] = f"Duplicate contact (same name+company as {name_seen[key]})"
        else:
            name_seen[key] = f"{p.get('first_name', '')} {p.get('last_name', '')}"

    return skips


def check_inactive(prospect: dict) -> str | None:
    """Check if prospect is too inactive for outreach."""
    activity_score = prospect.get("activity_score") or 0
    connections = prospect.get("linkedin_connections") or 0
    if activity_score == 0 and connections < 10:
        return f"Inactive profile (activity: {activity_score}, connections: {connections})"
    return None


def check_no_linkedin(prospect: dict) -> str | None:
    """Check if prospect has a LinkedIn URL."""
    if not prospect.get("linkedin_url"):
        return "No LinkedIn URL — cannot do outreach"
    return None


def check_role_verified(prospect: dict) -> str | None:
    """Check if role was verified via profile scrape."""
    if prospect.get("role_verified") is False:
        return "Role not verified on LinkedIn"
    return None


def check_title_tier(prospect: dict) -> tuple[str | None, bool]:
    """Check title against ICP tiers. Returns (skip_reason, is_borderline)."""
    title = prospect.get("title", "")
    tier, label = classify_title_tier(title)
    if tier == 0:
        return None, True  # borderline — needs GPT review
    return None, False  # known tier, OK


# ---------------------------------------------------------------------------
# GPT-5.4 review for borderline cases
# ---------------------------------------------------------------------------

def gpt_review_titles(borderline_prospects: list[dict]) -> dict[str, dict]:
    """Ask GPT-5.4 to assess borderline titles for ICP relevance.

    Returns {prospect_id: {"relevant": bool, "reason": str}}.
    """
    if not borderline_prospects:
        return {}

    client = get_openai()

    prospects_for_review = []
    for p in borderline_prospects:
        prospects_for_review.append({
            "id": p["id"],
            "name": f"{p.get('first_name', '')} {p.get('last_name', '')}",
            "title": p.get("title", ""),
            "company": p.get("company_name", ""),
            "industry": p.get("category", ""),
        })

    prompt = f"""You are reviewing a prospect list for VWC CPAs, a Seattle-based audit and tax firm.

For each prospect below, determine if they are a RELEVANT contact for audit/tax outreach.

RELEVANT contacts are people who:
- Make or influence decisions about hiring a CPA firm for audit, tax, or advisory
- Hold finance, accounting, or executive leadership roles
- CFO, Controller, VP Finance, Director of Finance, Owner, President, CEO = always relevant

NOT RELEVANT contacts are:
- Sales, marketing, HR, engineering, operations roles (unless Owner/President)
- Administrative assistants, coordinators (unless finance-specific)
- People whose title has no connection to financial decision-making

Return a JSON object with a "results" array. Each element has:
- "id": the prospect ID (pass through exactly)
- "relevant": true or false
- "reason": 1 sentence explaining why

Prospects to review:
{json.dumps(prospects_for_review, indent=2)}"""

    try:
        response = client.chat.completions.create(
            model=REVIEW_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        parsed = json.loads(raw)
        results = parsed.get("results", [])
        return {r["id"]: {"relevant": r.get("relevant", True), "reason": r.get("reason", "")} for r in results}
    except Exception as e:
        logger.error("GPT review failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Main reviewer
# ---------------------------------------------------------------------------

def review_batch(tenant_id: str, month: str, dry_run: bool = False,
                  send_email: str | None = None):
    """Run all checks on a monthly batch of prospects."""
    sb = get_supabase()

    # Parse month range
    year, mon = month.split("-")
    start = f"{year}-{mon}-01T00:00:00+00:00"
    if int(mon) == 12:
        end = f"{int(year) + 1}-01-01T00:00:00+00:00"
    else:
        end = f"{year}-{int(mon) + 1:02d}-01T00:00:00+00:00"

    # Fetch prospects for this month
    result = sb.table("prospects").select("*").eq("tenant_id", tenant_id).gte("created_at", start).lt("created_at", end).execute()
    prospects = result.data or []

    if not prospects:
        print(f"No prospects found for {month}")
        return

    print(f"Reviewing {len(prospects)} prospects for {month}")
    print(f"{'='*60}")

    # Load exclusion lists
    blacklist = load_exclusion_list(BLACKLIST_FILE)
    competitors = load_exclusion_list(COMPETITORS_FILE)
    print(f"  Blacklist: {len(blacklist['names'])} companies")
    print(f"  Competitors: {len(competitors['names'])} firms")

    # --- Cross-batch dedup: check previously contacted prospects ---
    previous = sb.table("prospects").select("linkedin_slug, company_name").eq(
        "tenant_id", tenant_id
    ).in_("status", ["approved", "invite_sent", "connected", "msg1_sent", "msg2_sent", "msg3_sent", "completed"]).lt(
        "created_at", start
    ).execute().data or []

    prev_slugs = {(p.get("linkedin_slug") or "").lower() for p in previous if p.get("linkedin_slug")}
    prev_companies = {(p.get("company_name") or "").lower() for p in previous if p.get("company_name")}
    print(f"  Previously contacted: {len(prev_slugs)} people, {len(prev_companies)} companies")

    # Track results
    auto_approved = []
    auto_skipped = []  # (prospect, reason)
    borderline = []
    skip_reasons = Counter()

    # --- Rule-based checks ---

    # 1. Duplicates (within-batch)
    dup_skips = check_duplicates(prospects)

    for p in prospects:
        pid = p["id"]
        name = f"{p.get('first_name', '')} {p.get('last_name', '')}"
        company = p.get("company_name", "")
        title = p.get("title", "")

        # Already skipped by user
        if p.get("status") == "skipped":
            auto_skipped.append((p, "Previously skipped by reviewer"))
            skip_reasons["previously_skipped"] += 1
            continue

        # Within-batch duplicate
        if pid in dup_skips:
            auto_skipped.append((p, dup_skips[pid]))
            skip_reasons["duplicate"] += 1
            continue

        # Cross-batch duplicate (same person already contacted)
        slug = (p.get("linkedin_slug") or "").lower()
        if slug and slug in prev_slugs:
            auto_skipped.append((p, "Already contacted in a previous batch"))
            skip_reasons["cross_batch_duplicate"] += 1
            continue

        # Blacklist check (VWC clients)
        bl_reason = is_excluded(company, p.get("company_domain"), blacklist)
        if bl_reason:
            auto_skipped.append((p, f"Blacklisted: {bl_reason}"))
            skip_reasons["blacklisted"] += 1
            continue

        # Competitor check
        comp_reason = is_excluded(company, p.get("company_domain"), competitors)
        if comp_reason:
            auto_skipped.append((p, f"Competitor: {comp_reason}"))
            skip_reasons["competitor"] += 1
            continue

        # No LinkedIn URL
        reason = check_no_linkedin(p)
        if reason:
            auto_skipped.append((p, reason))
            skip_reasons["no_linkedin"] += 1
            continue

        # Inactive profile
        reason = check_inactive(p)
        if reason:
            auto_skipped.append((p, reason))
            skip_reasons["inactive"] += 1
            continue

        # Role not verified — flag only, don't auto-skip
        # Headlines may differ from titles; needs manual review
        reason = check_role_verified(p)
        if reason:
            # Still approve but tag the reason for visibility
            p["_flag"] = reason
            auto_approved.append(p)
            continue

        # Title tier check
        _, is_borderline = check_title_tier(p)
        if is_borderline:
            borderline.append(p)
            continue

        # Passed all checks — auto-approve
        auto_approved.append(p)

    # --- GPT review for borderline titles ---
    gpt_results = {}
    if borderline:
        print(f"\n  GPT-5.4 reviewing {len(borderline)} borderline titles...")
        gpt_results = gpt_review_titles(borderline)

    gpt_approved = []
    gpt_skipped = []
    for p in borderline:
        review = gpt_results.get(p["id"])
        if review and not review.get("relevant", True):
            gpt_skipped.append((p, f"GPT: {review.get('reason', 'Not relevant')}"))
            skip_reasons["gpt_not_relevant"] += 1
        else:
            reason = review.get("reason", "") if review else ""
            gpt_approved.append((p, reason))

    # --- Summary ---
    all_approved = auto_approved + [p for p, _ in gpt_approved]
    all_skipped = auto_skipped + gpt_skipped

    print(f"\n{'='*60}")
    print(f"REVIEW RESULTS — {month}")
    print(f"{'='*60}")
    print(f"  Total prospects:    {len(prospects)}")
    print(f"  Auto-approved:      {len(auto_approved)}")
    print(f"  GPT-approved:       {len(gpt_approved)}")
    print(f"  Auto-skipped:       {len(auto_skipped)}")
    print(f"  GPT-skipped:        {len(gpt_skipped)}")
    print(f"  Total approved:     {len(all_approved)}")
    print(f"  Total skipped:      {len(all_skipped)}")
    print(f"\n  Skip reasons:")
    for reason, count in skip_reasons.most_common():
        print(f"    {reason}: {count}")

    # Show skipped prospects
    if all_skipped:
        print(f"\n  Skipped prospects:")
        for p, reason in all_skipped:
            name = f"{p.get('first_name', '')} {p.get('last_name', '')}"
            print(f"    {name:<30} @ {p.get('company_name', ''):<30} — {reason}")

    # Show GPT-approved with reasons
    if gpt_approved:
        print(f"\n  GPT-approved (borderline titles):")
        for p, reason in gpt_approved:
            name = f"{p.get('first_name', '')} {p.get('last_name', '')}"
            print(f"    {name:<30} ({p.get('title', '')}) — {reason}")

    # --- Apply to database ---
    if dry_run:
        print(f"\n  [DRY RUN] No changes written to database")
        return

    print(f"\n  Writing results to database...")
    now = datetime.now(timezone.utc).isoformat()

    for p in all_approved:
        sb.table("prospects").update({
            "status": "approved",
            "updated_at": now,
        }).eq("id", p["id"]).execute()

    for p, reason in all_skipped:
        if p.get("status") == "skipped":
            continue  # already skipped
        sb.table("prospects").update({
            "status": "skipped",
            "icp_reasoning": f"AI Review: {reason}",
            "updated_at": now,
        }).eq("id", p["id"]).execute()

    print(f"  Done: {len(all_approved)} approved, {len(all_skipped)} skipped")

    # --- Send email to approver ---
    if send_email:
        print(f"\n  Sending review email to {send_email}...")
        _send_review_email(
            to=send_email,
            month=month,
            tenant_id=tenant_id,
            total=len(prospects),
            approved_count=len(all_approved),
            skipped_count=len(all_skipped),
            skip_reasons=dict(skip_reasons),
            top_companies=_get_top_companies(all_approved),
        )
        print(f"  Email sent to {send_email}")


def _get_top_companies(approved_prospects: list[dict], limit: int = 10) -> list[dict]:
    """Get top companies by ICP score from approved prospects."""
    company_map = defaultdict(lambda: {"contacts": 0, "score": 0})
    for p in approved_prospects:
        company = p.get("company_name", "Unknown")
        company_map[company]["contacts"] += 1
        company_map[company]["score"] = max(company_map[company]["score"], p.get("icp_score", 0))
        company_map[company]["name"] = company
        company_map[company]["category"] = p.get("category", "")

    sorted_companies = sorted(company_map.values(), key=lambda c: -c["score"])
    return sorted_companies[:limit]


def _send_review_email(
    to: str,
    month: str,
    tenant_id: str,
    total: int,
    approved_count: int,
    skipped_count: int,
    skip_reasons: dict,
    top_companies: list[dict],
):
    """Send the batch review summary email via Outlook."""
    from lib.outlook import OutlookClient

    date = datetime.strptime(month, "%Y-%m").strftime("%B %Y")
    dashboard_url = f"https://linkedin-vwc-outreach.vercel.app/clients/{tenant_id}/review-batches"

    # Build issues summary HTML
    issues_html = ""
    if skip_reasons:
        issues_rows = ""
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            label = reason.replace("_", " ").title()
            issues_rows += f'<tr><td style="padding:4px 12px 4px 0;font-size:13px;color:#64748b">{label}</td><td style="padding:4px 0;font-size:13px;font-weight:600;color:#ef4444">{count}</td></tr>'
        issues_html = f"""
        <div style="margin:20px 0">
          <p style="font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#94a3b8;margin:0 0 8px;font-weight:600">Issues Found & Removed</p>
          <table>{issues_rows}</table>
        </div>"""

    # Build top companies HTML
    companies_html = ""
    if top_companies:
        rows = ""
        for c in top_companies:
            score_color = "#16a34a" if c["score"] >= 80 else "#ca8a04"
            rows += f"""
            <tr>
              <td style="padding:6px 12px 6px 0;font-size:13px;font-weight:500">{c['name']}</td>
              <td style="padding:6px 8px;font-size:12px;color:#64748b">{c.get('category', '')}</td>
              <td style="padding:6px 8px;text-align:center"><span style="background:{score_color};color:#fff;padding:1px 8px;border-radius:4px;font-size:11px;font-weight:700">{c['score']}</span></td>
              <td style="padding:6px 0;text-align:right;font-size:12px;color:#64748b">{c['contacts']} contacts</td>
            </tr>"""
        companies_html = f"""
        <div style="margin:20px 0">
          <p style="font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#94a3b8;margin:0 0 8px;font-weight:600">Top Companies</p>
          <table style="width:100%">{rows}</table>
        </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f8fafc">
  <div style="max-width:640px;margin:0 auto;background:#fff">

    <div style="background:linear-gradient(135deg,#0f172a,#1e293b);color:#fff;padding:28px 32px">
      <p style="margin:0;font-size:11px;text-transform:uppercase;letter-spacing:1.5px;color:rgba(255,255,255,.4);font-weight:500">VWC CPAs</p>
      <h1 style="margin:8px 0 0;font-size:22px;font-weight:700;letter-spacing:-.3px">Prospect Batch — {date}</h1>
      <p style="margin:6px 0 0;font-size:14px;color:rgba(255,255,255,.55)">AI review complete. {approved_count} prospects ready for your review.</p>
    </div>

    <div style="padding:24px 32px">

      <div style="display:flex;gap:1px;background:#e2e8f0;border-radius:12px;overflow:hidden;margin-bottom:20px">
        <div style="flex:1;background:#fff;padding:18px;text-align:center">
          <div style="font-size:28px;font-weight:700;color:#0f172a">{total}</div>
          <div style="font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#94a3b8;margin-top:4px;font-weight:500">Total</div>
        </div>
        <div style="flex:1;background:#fff;padding:18px;text-align:center">
          <div style="font-size:28px;font-weight:700;color:#16a34a">{approved_count}</div>
          <div style="font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#94a3b8;margin-top:4px;font-weight:500">Approved</div>
        </div>
        <div style="flex:1;background:#fff;padding:18px;text-align:center">
          <div style="font-size:28px;font-weight:700;color:#ef4444">{skipped_count}</div>
          <div style="font-size:11px;text-transform:uppercase;letter-spacing:.8px;color:#94a3b8;margin-top:4px;font-weight:500">Removed</div>
        </div>
      </div>

      {issues_html}
      {companies_html}

      <div style="text-align:center;padding:24px 0">
        <a href="{dashboard_url}" style="display:inline-block;background:#16a34a;color:#fff;padding:14px 40px;border-radius:10px;text-decoration:none;font-size:15px;font-weight:600;letter-spacing:.3px">
          Review &amp; Approve Batch
        </a>
        <p style="margin:12px 0 0;font-size:12px;color:#94a3b8">
          Nothing sends until you approve. 5 connection requests per day once approved.
        </p>
      </div>

    </div>

    <div style="padding:16px 32px;background:#f8fafc;border-top:1px solid #e2e8f0">
      <p style="margin:0;font-size:11px;color:#94a3b8">VWC LinkedIn Outreach System — yorCMO</p>
    </div>

  </div>
</body>
</html>"""

    client = OutlookClient()
    client.send_email(
        to=to,
        subject=f"VWC Prospect Batch — {date} ({approved_count} prospects ready for review)",
        html_body=html,
    )


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="AI Batch Reviewer — pre-screen prospects")
    parser.add_argument("--tenant-id", required=True, help="Tenant UUID")
    parser.add_argument("--month", required=True, help="Month to review (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't update DB")
    parser.add_argument("--send-email", default=None, help="Send review email to this address after review")
    args = parser.parse_args()

    try:
        review_batch(args.tenant_id, args.month, args.dry_run, args.send_email)
    except Exception as e:
        logger.error("batch_reviewer failed: %s", e, exc_info=True)
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
