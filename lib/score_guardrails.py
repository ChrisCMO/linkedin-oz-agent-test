"""Score guardrails — rule-based corrections + AI reviewer for borderline companies.

Two-layer system:
1. Rule-based overrides: fix known GPT mistakes (CFO=9+, Seattle=15, etc.)
2. AI reviewer: for companies 1-5 points from PROCEED, compare against VWC benchmarks

VWC Benchmark Companies (all score 80+):
- Shannon & Wilson: ESOP, Professional services, 320 emp, $28M, Seattle — 94
- Formost Fuji: Private, Manufacturing, 62 emp, $9M, Everett — 90
- Skills Inc.: Nonprofit, Aerospace mfg, 430 emp, $26M, Auburn — 89
- Carillon Properties: Family-owned, CRE/Hospitality, unknown size, Kirkland — 84
  (No database presence, no finance contacts found, still 84)
"""

import json
import logging
import os

from lib.title_tiers import classify_title_tier

logger = logging.getLogger(__name__)

# VWC benchmark patterns for AI reviewer context
BENCHMARK_CONTEXT = """VWC benchmark clients (all confirmed 80+ ICP fit):
- Shannon & Wilson: ESOP engineering firm, 320 emp, $28M, Seattle = 94
- Formost Fuji: Private manufacturer, 62 emp, $9M, Everett = 90
- Skills Inc.: Nonprofit aerospace mfg, 430 emp, $26M, Auburn = 89
- Carillon Properties: Family-owned CRE, unknown size/rev, Kirkland = 84 (no DB contacts)

Key patterns from benchmarks:
- Companies with 25-750 employees in Seattle metro are strong fits
- Revenue $5M-$150M is acceptable, missing revenue = benefit of doubt
- Private/family/ESOP ownership = full points
- LinkedIn employee count is a MINIMUM — actual headcount is always higher
- <25 LinkedIn employees does NOT mean a small company (Carillon has 0 listed)
- Having any tier contact (CEO, CFO, Controller, Accounting Manager) = strong signal
- No contacts found does NOT disqualify (Carillon pattern)
"""

# PNW cities for geography check
PNW_CITIES = {
    "seattle", "bellevue", "tacoma", "redmond", "kirkland", "everett",
    "renton", "kent", "auburn", "olympia", "lynnwood", "lakewood",
    "federal way", "vancouver", "puyallup", "bainbridge", "woodinville",
    "bothell", "issaquah", "shoreline", "tukwila", "burien", "sammamish",
    "sumner", "mukilteo", "mercer island", "des moines", "snoqualmie",
}

# Target industries
TARGET_INDUSTRIES = {
    "manufacturing", "construction", "commercial real estate", "hospitality",
    "professional services", "engineering", "nonprofit", "aerospace",
    "machinery", "electronics", "food", "packaging", "printing",
    "defense", "aviation", "industrial", "fabrication",
}


def apply_rule_overrides(breakdown: dict, company_data: dict, finance_scan: dict) -> dict:
    """Apply deterministic rule-based corrections to GPT dimension scores.

    Returns corrected breakdown dict. Only adjusts UP, never down.
    """
    corrected = dict(breakdown)
    changes = []

    # --- Organizational complexity: CFO/Controller must be 9+ ---
    org_score = int(corrected.get("organizational_complexity", 0))
    has_cfo = finance_scan.get("has_cfo", False)
    has_controller = finance_scan.get("has_controller", False)
    has_accounting_mgr = finance_scan.get("has_accounting_manager", False)
    contacts = finance_scan.get("contacts", [])

    if (has_cfo or has_controller) and org_score < 9:
        corrected["organizational_complexity"] = 9
        changes.append(f"org_complexity: {org_score}→9 (CFO/Controller found)")
    elif has_accounting_mgr and org_score < 7:
        corrected["organizational_complexity"] = 7
        changes.append(f"org_complexity: {org_score}→7 (Accounting Manager found)")
    elif contacts and org_score < 6:
        # Any contact found (CEO, Owner, etc.)
        corrected["organizational_complexity"] = 6
        changes.append(f"org_complexity: {org_score}→6 (tier contact found)")

    # --- Geography: PNW cities must be 13-15 ---
    geo_score = int(corrected.get("geography", 0))
    location = (company_data.get("location") or "").lower()
    is_seattle_metro = any(city in location for city in PNW_CITIES)
    is_wa = "washington" in location or ", wa" in location
    is_or = "oregon" in location or ", or" in location

    if is_seattle_metro and geo_score < 15:
        corrected["geography"] = 15
        changes.append(f"geography: {geo_score}→15 (Seattle metro)")
    elif is_wa and geo_score < 13:
        corrected["geography"] = 13
        changes.append(f"geography: {geo_score}→13 (Washington state)")
    elif is_or and geo_score < 11:
        corrected["geography"] = 11
        changes.append(f"geography: {geo_score}→11 (Oregon)")

    # --- Revenue: missing/unknown = 7 benefit of doubt ---
    rev_score = int(corrected.get("revenue_fit", 0))
    revenue = company_data.get("revenue", "")
    if (not revenue or revenue == "--") and rev_score < 7:
        corrected["revenue_fit"] = 7
        changes.append(f"revenue_fit: {rev_score}→7 (missing revenue, benefit of doubt)")

    # --- Company size: <25 LinkedIn employees with contacts = at least 14 ---
    size_score = int(corrected.get("company_size", 0))
    li_emp = company_data.get("employees") or 0
    if li_emp < 25 and contacts and size_score < 14:
        corrected["company_size"] = 14
        changes.append(f"company_size: {size_score}→14 (<25 LI emp but contacts found, Carillon pattern)")

    # --- Ownership: unknown but appears private = 12 ---
    own_score = int(corrected.get("ownership_structure", 0))
    ownership = (company_data.get("ownership") or "").lower()
    if not ownership and own_score < 12:
        corrected["ownership_structure"] = 12
        changes.append(f"ownership: {own_score}→12 (unknown, benefit of doubt)")

    if changes:
        logger.info("Rule overrides for %s: %s", company_data.get("name", "?"), "; ".join(changes))

    return corrected


def recalculate_score(breakdown: dict) -> int:
    """Sum breakdown dimensions, cap at 100."""
    total = 0
    for v in breakdown.values():
        try:
            total += int(v)
        except (ValueError, TypeError):
            pass
    return min(total, 100)


def ai_review_borderline(company_data: dict, breakdown: dict, score: int,
                          finance_scan: dict) -> dict | None:
    """AI reviewer for companies 1-5 points from PROCEED (75-79).

    Compares against VWC benchmarks and returns corrected breakdown if
    the company deserves an upgrade, or None if score is fair.

    Only called for borderline cases — not every company.
    """
    try:
        from mvp.backend.services.scoring import get_openai, SCORING_MODEL
    except ImportError:
        logger.warning("Cannot import OpenAI client for AI review")
        return None

    name = company_data.get("name", "Unknown")
    contacts_summary = ""
    for c in finance_scan.get("contacts", []):
        tier, label = classify_title_tier(c.get("title", ""))
        contacts_summary += f"  - {c.get('first_name', '')} {c.get('last_name', '')} ({c.get('title', '')}) [Tier {tier}: {label}]\n"

    if not contacts_summary:
        contacts_summary = "  None found\n"

    prompt = f"""You are a scoring QA reviewer for VWC CPAs' ICP pipeline. A company scored {score} (REVIEW) but is only 1-{80-score} points from PROCEED (80+).

{BENCHMARK_CONTEXT}

Company being reviewed:
- Name: {name}
- Industry: {company_data.get('industry', 'unknown')}
- Location: {company_data.get('location', 'unknown')}
- Employees (LinkedIn): {company_data.get('employees', 'unknown')}
- Revenue: {company_data.get('revenue', 'unknown')}
- Ownership: {company_data.get('ownership', 'unknown')}
- Contacts found:
{contacts_summary}
Current breakdown: {json.dumps(breakdown)}

Review each dimension against the rubric. Are any dimensions scored too conservatively?
Consider:
- Does this company have similarities with the benchmark companies?
- If it has a CEO/Owner/CFO/Controller, is organizational_complexity scored appropriately?
- Is geography scored correctly for PNW?
- Is company_size fair given LinkedIn undercounts?
- Missing revenue should get benefit of doubt (7/10)

Return JSON only:
{{"corrected_breakdown": {{...dimension scores...}}, "changes": ["what you corrected and why"], "should_proceed": true/false}}

If the score is fair as-is, return {{"corrected_breakdown": null, "changes": [], "should_proceed": false}}"""

    try:
        client = get_openai()
        response = client.chat.completions.create(
            model=SCORING_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        result = json.loads(response.choices[0].message.content)

        if result.get("corrected_breakdown"):
            new_score = recalculate_score(result["corrected_breakdown"])
            logger.info("AI reviewer for %s: %d → %d (%s). Changes: %s",
                        name, score, new_score,
                        "PROCEED" if new_score >= 80 else "still REVIEW",
                        "; ".join(result.get("changes", [])))
            return result

        return None

    except Exception as e:
        logger.warning("AI review failed for %s: %s", name, e)
        return None


def apply_guardrails(score: int, breakdown: dict, company_data: dict,
                     finance_scan: dict) -> tuple[int, dict, str]:
    """Apply full guardrail pipeline: rule overrides + AI review if borderline.

    Returns (corrected_score, corrected_breakdown, action).
    """
    # Step 1: Rule-based overrides
    corrected = apply_rule_overrides(breakdown, company_data, finance_scan)
    corrected_score = recalculate_score(corrected)

    # Step 2: Determine action
    has_any_contact = bool(finance_scan.get("contacts"))

    if corrected_score >= 80:
        return corrected_score, corrected, "PROCEED"

    if corrected_score >= 75 and has_any_contact:
        return corrected_score, corrected, "PROCEED"

    # Step 3: AI review for borderline cases (75-79 without contacts, or 70-74 with contacts)
    if 70 <= corrected_score <= 79:
        review = ai_review_borderline(company_data, corrected, corrected_score, finance_scan)
        if review and review.get("corrected_breakdown"):
            ai_breakdown = review["corrected_breakdown"]
            ai_score = recalculate_score(ai_breakdown)
            ai_score = min(ai_score, 100)

            if ai_score >= 80:
                return ai_score, ai_breakdown, "PROCEED"
            elif ai_score >= 75 and has_any_contact:
                return ai_score, ai_breakdown, "PROCEED"
            else:
                return ai_score, ai_breakdown, "REVIEW"

    # Default action
    if corrected_score >= 60:
        action = "REVIEW"
    elif corrected_score == 0:
        action = "HARD EXCLUDE"
    else:
        action = "SKIP"

    return corrected_score, corrected, action
