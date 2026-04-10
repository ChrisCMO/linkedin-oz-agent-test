"""AI-powered ICP scoring using OpenAI."""

import json
import logging
import re

from mvp.backend.config import get_openai

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are an ICP (Ideal Customer Profile) scoring agent for a CPA firm's B2B outreach tool.
You will receive an ICP definition and a list of prospects. Score each prospect 0-100 against the ICP.

Return a JSON array where each element has:
- "apollo_id": the prospect's Apollo ID (pass through exactly)
- "score": integer 0-100
- "breakdown": object with dimension scores, e.g. {"title": 25, "industry": 18, "company_size": 20, "location": 10, "seniority": 12, "linkedin": 5, "revenue": 10}
- "reasoning": 1-2 sentence human-readable explanation for why this score

Scoring dimensions and their max points:
- title (0-25): How well does their title match target titles? CFO/Controller/VP Finance = full marks. Owner/President/CEO = 15-20. Director of Finance = 20. Other finance titles = partial.
- industry (0-18): Is the company in or adjacent to a target industry? Be GENEROUS with industry matching — many companies are classified differently in databases than their actual sector. Examples: "machinery" = manufacturing, "building materials" = construction, "civil engineering" = professional services/construction, "aviation & aerospace" = manufacturing, "mechanical engineering" = manufacturing, "wholesale" = could be any sector. Score 12-18 for direct matches, 8-12 for adjacent industries, 4-8 for loosely related, 0 only for explicitly excluded industries.
- company_size (0-20): Is employee count in the target range? This is the PRIMARY size indicator. Score based on the employee_count_ranges provided. Companies slightly outside the range still get partial credit.
- revenue (0-12): Is company revenue in the target range? Revenue data is often unavailable for private companies — if null/missing, score 8/12 (benefit of the doubt). If available, score based on fit. Apply hard exclusions if specified (e.g., >$150M for audit & tax).
- location (0-10): Is the COMPANY in a target location? Use company_location (Google Places verified address) if available — this is the most reliable. Fall back to person_location only if company_location is empty. A company physically located in a target city = full marks.
- seniority (0-10): Does their seniority match target seniorities? c_suite/vp/director/owner = full marks.
- linkedin (0-5): Do they have a LinkedIn profile available for outreach?

HARD EXCLUSIONS — score 0 overall if any apply:
- If ICP specifies a revenue ceiling (e.g., $150M) and company revenue EXCEEDS it, score 0.
- If company is in an explicitly excluded industry (e.g., banking, government, technology for audit & tax), score 0.
- If company is clearly a massive public corporation (e.g., >10,000 employees, Fortune 500), score 0 unless the ICP specifically includes public companies.

IMPORTANT: When in doubt, score HIGHER not lower. These prospects have already been pre-filtered by company discovery (Google Places) and contact discovery (ZoomInfo/Apollo). A prospect reaching this scoring stage is likely a reasonable match — the question is HOW good a match, not whether they match at all.

Return ONLY valid JSON. No markdown, no explanation outside the JSON array."""


COMPANY_SYSTEM_PROMPT = """You are an ICP (Ideal Customer Profile) scoring agent for VWC CPAs, a Seattle-based audit and tax firm.

You will receive company-level data. Score each COMPANY 0-100 against the VWC ICP.
This is a company-level score only. Do NOT factor in individual contact titles or seniority.

Return a JSON object with a "companies" key containing an array where each element has:
- "company_id": pass through exactly
- "company_name": pass through exactly
- "score": integer 0-100
- "breakdown": object with dimension scores
- "reasoning": 1-2 sentence explanation
- "calibration_notes": any insights about how this company's profile should inform scoring thresholds

Scoring dimensions and weights (calibrated against VWC benchmark clients):

- industry_fit (0-20): Target industries in priority order: Manufacturing (#1), Commercial RE (#2), Professional Services (#3), Hospitality (#4), Nonprofit (#5), Construction (#6). Score 18-20 for top priority, 14-17 for secondary. Be GENEROUS with industry matching (e.g., "machinery" = manufacturing, "civil engineering" = professional services).

- company_size (0-20): Employee count. Use linkedin_employees as the PRIMARY source (most reliable per client). Fall back to apollo_employees only if LinkedIn is unavailable. Sweet spot: 100-300 employees (18-20). Acceptable: 25-750 (14-17). Slightly outside: 11-24 or 751-1000 (10-13). Below 11 or above 1000 (5-9). Unknown for private companies: score 14/20 (benefit of doubt).

- revenue_fit (0-15): Revenue range. Sweet spot: $50M-$100M (14-15). Acceptable: $5M-$150M (10-13). Hard exclude above $150M (score 0 overall). Unknown for private companies: score 10/15 (benefit of doubt - private companies rarely disclose revenue).

- geography (0-15): Company location. Seattle metro (Seattle, Bellevue, Kirkland, Redmond, Tacoma, Everett, Renton, Kent, Auburn, Olympia) = 15. Greater WA = 13. Oregon = 11. West Coast = 8. National = 5.

- ownership_structure (0-15): Private, family-owned, ESOP, founder-led = 15. Unknown but appears private = 12. PE-backed = 0 (hard exclude). Public = 0 for Audit & Tax ICP (in scope for Benefit Plan Audit).

- digital_footprint (0-15): How discoverable is the company online? LinkedIn company page with followers/description (0-5). Google Places verified with rating/reviews (0-3). Finance contacts findable in databases (0-4). Company website exists (0-3). IMPORTANT: A weak digital footprint should NOT disqualify an otherwise ideal private company (calibration insight from Carillon Properties benchmark).

HARD EXCLUSIONS (score 0 overall):
- Revenue above $150M
- Public company (for Audit & Tax ICP)
- Government entity
- PE-backed company
- Banking/financial institution
- More than 10,000 employees

CALIBRATION REFERENCE (VWC benchmark clients - all should score 80+):
- Formost Fuji: Manufacturing, 62 emp, $9M rev, Everett WA, private = 90
- Shannon & Wilson: Engineering, 320 emp, $28M rev, Seattle WA, ESOP = 94
- Skills Inc.: Nonprofit/Aerospace mfg, 430 emp, $26M rev, Auburn WA, nonprofit = 89
- Carillon Properties: CRE/Hospitality, unknown size, Kirkland WA, private family = 84

Return ONLY valid JSON."""


SCORING_MODEL = "gpt-5.4"


def score_companies(companies: list[dict], icp_config: dict | None = None, model: str | None = None) -> list[dict]:
    """Score a batch of companies against the VWC ICP at the company level.

    This is separate from contact-level scoring. It evaluates whether the
    COMPANY matches the ICP, regardless of which contacts we can find there.

    Args:
        companies: List of company dicts with keys like:
            company_id, company_name, industry, employees, revenue,
            location, ownership, linkedin_page, google_places, website,
            finance_contacts_found
        icp_config: Optional ICP overrides (uses VWC defaults if None)
        model: Override the scoring model

    Returns:
        List of {company_id, company_name, score, breakdown, reasoning, calibration_notes}
    """
    if not companies:
        return []

    scoring_model = model or SCORING_MODEL
    logger.info("Scoring %d companies against ICP (model=%s)", len(companies), scoring_model)
    client = get_openai()

    company_summaries = []
    for c in companies:
        # LinkedIn employee count is primary, Apollo is fallback
        li_employees = c.get("linkedin_employees", c.get("li_employees", ""))
        apollo_employees = c.get("apollo_employees", c.get("employees", c.get("employee_count", "")))

        company_summaries.append({
            "company_id": c.get("company_id", c.get("google_place_id", "")),
            "company_name": c.get("company_name", c.get("name", "")),
            "industry": c.get("industry", ""),
            "linkedin_employees": li_employees,
            "apollo_employees": apollo_employees,
            "revenue": c.get("revenue", ""),
            "location": c.get("location", c.get("address", "")),
            "ownership": c.get("ownership", ""),
            "linkedin_page": c.get("linkedin_page", c.get("company_linkedin", "")),
            "linkedin_followers": c.get("linkedin_followers", c.get("li_followers", "")),
            "google_places": c.get("google_places", ""),
            "website": c.get("website", c.get("domain", "")),
            "finance_contacts_found": c.get("finance_contacts_found", ""),
            "notes": c.get("notes", ""),
        })

    user_message = json.dumps({"companies": company_summaries}, indent=2)

    response = client.chat.completions.create(
        model=scoring_model,
        messages=[
            {"role": "system", "content": COMPANY_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content
    parsed = json.loads(raw)

    if isinstance(parsed, dict) and "companies" in parsed:
        scores = parsed["companies"]
    elif isinstance(parsed, list):
        scores = parsed
    elif isinstance(parsed, dict) and "company_id" in parsed:
        scores = [parsed]
    else:
        scores = []

    logger.info("Company scoring complete: %d scores returned", len(scores))
    return scores


def score_prospects(prospects: list[dict], icp_config: dict, model: str | None = None) -> list[dict]:
    """Score a batch of prospects against an ICP.

    Args:
        prospects: List of enriched prospect dicts from Apollo
        icp_config: ICP definition with target_titles, target_seniorities, etc.
        model: Override the scoring model (default: gpt-4.5-preview)

    Returns:
        List of {apollo_id, score, breakdown, reasoning} dicts
    """
    if not prospects:
        return []

    scoring_model = model or SCORING_MODEL
    logger.info("Scoring %d prospects against ICP (model=%s)", len(prospects), scoring_model)
    client = get_openai()

    # Build prospect summaries for the prompt
    prospect_summaries = []
    for p in prospects:
        # Company location (from Google Places) takes priority over person location (from Apollo)
        company_location = p.get("company_location") or p.get("_address") or ""
        person_location = f"{p.get('city', '')}, {p.get('state', '')}".strip(", ")

        prospect_summaries.append({
            "apollo_id": p.get("apollo_id"),
            "name": p.get("name"),
            "title": p.get("title"),
            "seniority": p.get("seniority"),
            "company_name": p.get("company_name"),
            "company_industry": p.get("company_industry"),
            "company_employees": p.get("company_employees"),
            "company_employee_range": p.get("company_employee_range"),
            "company_revenue": p.get("company_revenue"),
            "company_location": company_location,
            "person_location": person_location,
            "has_linkedin": bool(p.get("linkedin_url")),
        })

    user_message = json.dumps({
        "icp": {
            "target_titles": icp_config.get("target_titles", []),
            "target_seniorities": icp_config.get("target_seniorities", []),
            "target_industries": icp_config.get("keywords", []) + icp_config.get("target_industries", []),
            "target_locations": icp_config.get("target_locations", []),
            "employee_count_ranges": icp_config.get("employee_count_ranges", []),
            "revenue_ranges": icp_config.get("revenue_ranges") or (icp_config.get("scoring_config") or {}).get("revenue_ranges", []),
            "hard_exclusions": icp_config.get("hard_exclusions") or (icp_config.get("scoring_config") or {}).get("hard_exclusions", []),
            "custom_notes": icp_config.get("custom_notes") or (icp_config.get("scoring_config") or {}).get("custom_notes", ""),
        },
        "prospects": prospect_summaries,
    }, indent=2)

    response = client.chat.completions.create(
        model=scoring_model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content
    parsed = json.loads(raw)

    # Handle multiple response formats from different models:
    # - Array: [{...}, {...}]
    # - Wrapped array: {"scores": [{...}]} or {"results": [{...}]}
    # - Single object: {"apollo_id": "...", "score": 80, ...} (GPT-5.4 does this for single prospects)
    if isinstance(parsed, list):
        scores = parsed
    elif isinstance(parsed, dict):
        if "scores" in parsed:
            scores = parsed["scores"]
        elif "results" in parsed:
            scores = parsed["results"]
        elif "apollo_id" in parsed and "score" in parsed:
            # Single prospect response — wrap in array
            scores = [parsed]
        else:
            scores = []
    else:
        scores = []

    logger.info("Scoring complete: %d scores returned", len(scores))
    return scores


# ---------------------------------------------------------------------------
# V2 Scoring — Revamped company scoring with organizational complexity
# ---------------------------------------------------------------------------

COMPANY_SYSTEM_PROMPT_V2 = """You are an ICP (Ideal Customer Profile) scoring agent for VWC CPAs, a Seattle-based audit and tax firm.

You will receive company-level data. Score each COMPANY 0-100 against the VWC ICP.
This is a company-level score only.

Return a JSON object with a "companies" key containing an array where each element has:
- "company_id": pass through exactly
- "company_name": pass through exactly
- "score": integer 0-100 — MUST equal the exact sum of all dimension scores in "breakdown". Do not apply any subjective adjustment. Sum your dimensions and use that number.
- "breakdown": object with dimension scores (each dimension as an integer)
- "reasoning": 1-2 sentence explanation
- "calibration_notes": any insights about scoring

Base dimensions total 100. The big_firm_signal (0-8) is a BOLSTER-ONLY bonus — add it on top, then cap the final score at 100.

Scoring dimensions and weights (v2 — calibrated against VWC benchmark clients):

- industry_fit (0-20): Target industries in priority order: Manufacturing (#1), Commercial RE (#2), Professional Services (#3), Hospitality (#4), Nonprofit (#5), Construction (#6). Score 18-20 for top priority, 14-17 for secondary. Be GENEROUS with industry matching (e.g., "machinery" = manufacturing, "civil engineering" = professional services, "aviation & aerospace" = manufacturing).

- company_size (0-20): Employee count. IMPORTANT: LinkedIn employee count is the MINIMUM — it only counts people who list the company on their profile. The actual company is almost always larger. When apollo_employees > linkedin_employees, use the HIGHER number (Apollo's estimate is closer to reality). When apollo_employees < linkedin_employees or is missing, use LinkedIn. Sweet spot: 100-300 employees (18-20). Acceptable: 25-750 (14-17). Slightly outside: 11-24 or 751-1000 (10-13). Below 11 or above 1000 (5-9). Unknown for private companies: score 14/20 (benefit of doubt). CRITICAL: If a CFO or Controller is found at a company with low LinkedIn employees (<25), the company is likely larger than LinkedIn shows — score at least 14/20 (benefit of doubt, Carillon Properties pattern).

- revenue_fit (0-10): Revenue range. IMPORTANT: Revenue data is frequently unreliable for private companies — Chad (VWC partner) confirmed this. If revenue is marked as "SUSPECT" or missing, score 7/10 (strong benefit of doubt). Sweet spot: $50M-$100M (9-10). Acceptable: $5M-$150M (7-8). Below $5M with employees suggesting larger company: 7 (trust employees over revenue). Hard exclude above $150M (score 0 overall).

- geography (0-15): Company location. Seattle metro (Seattle, Bellevue, Kirkland, Redmond, Tacoma, Everett, Renton, Kent, Auburn, Olympia, Federal Way, Lynnwood, Lakewood, Puyallup, Sumner, Mukilteo, Woodinville, Bothell, Bainbridge Island) = 15. Greater WA (Spokane, Vancouver, Yakima, Bellingham, Tri-Cities, Olympia, Ferndale, Clarkston, Woodland, Arlington) = 13. Oregon = 11. West Coast = 8. National = 5.

- ownership_structure (0-15): Private, family-owned, ESOP, founder-led = 15. Unknown but appears private = 12. PE-backed = 0 (hard exclude). Public = 0 for Audit & Tax ICP.

- digital_footprint (0-10): How discoverable is the company online? LinkedIn company page with followers/description (0-4). Google Places verified (0-2). Company website exists (0-2). Findable online via search (0-2). IMPORTANT: A weak digital footprint should NOT disqualify an otherwise ideal private company.

- big_firm_signal (0-8): Is the company currently audited by a Big Four or large CPA firm? This is a POSITIVE signal — these companies may feel underserved ("small fish in a big pond") and are prime targets for VWC. Data comes from Form 5500 filings. If big_firm_auditor is present:
  * Big Four (Deloitte, PwC, EY, KPMG): 8 (strongest signal)
  * Large national/regional (BDO, Baker Tilly, Moss Adams, Sweeney Conrad, Clark Nuber, Plant Moran): 7
  * Other named auditor: 5
  * No data / not found: 0 (no penalty — most private companies won't appear)
  IMPORTANT: This is a BOLSTER-ONLY signal. It can only increase a score, never decrease it. A company without Form 5500 data should not be penalized.

- organizational_complexity (0-10): Does the company have dedicated finance leadership? This signals the company is complex enough to need audit/tax services. Score based on finance_titles field:
  * CFO or Controller found: 9-10 (strong signal of financial complexity)
  * Both CFO AND Controller: 10 (maximum — robust finance function)
  * VP Finance or Director of Finance: 7-8
  * Accounting Manager or Finance Manager found: 6-7 (dedicated accounting staff — company has financial complexity but not C-level finance leadership yet)
  * Only CEO/President/Owner found (no dedicated finance titles) at a NON-private/family company: 3-4 (may be too small/simple)
  * Only CEO/President/Owner found BUT company is private/family-owned/founder-led: 7-8 (Carillon Properties pattern — the owner IS the financial decision-maker at family businesses. They may have an internal Accounting Manager or bookkeeper not visible on LinkedIn. Benefit of the doubt.)
  * No contacts found at all: 5 (benefit of doubt — many private companies have unlisted staff)
  CRITICAL: This dimension exists because Chad said "if they have a CFO or Controller, that indicates increased complexity" and companies with only a CEO/President "are oftentimes maybe too small." HOWEVER, for private/family-owned companies, the Owner/CEO typically makes audit/tax decisions directly — similar to how Carillon Properties (family-owned, no finance contacts found) scored 84 as a benchmark client.
  OVERRIDE: When CFO/Controller is found at a company with low LinkedIn employees (<25), this is a strong signal — dedicated finance leadership despite small LinkedIn footprint. Score 10/10.

HARD EXCLUSIONS (score 0 overall):
- Revenue above $150M
- Public company (for Audit & Tax ICP)
- Government entity
- PE-backed company
- Banking/financial institution
- More than 10,000 employees

CALIBRATION REFERENCE (VWC benchmark clients - all should score 80+):
- Formost Fuji: Manufacturing, 62 emp, $9M rev, Everett WA, private, has Controller = ~90
- Shannon & Wilson: Engineering, 320 emp, $28M rev, Seattle WA, ESOP, has CFO = ~94
- Skills Inc.: Nonprofit/Aerospace mfg, 430 emp, $26M rev, Auburn WA, nonprofit, has Controller = ~89
- Carillon Properties: CRE/Hospitality, unknown size, Kirkland WA, private family, no finance contacts found = ~84
- AudioControl Pro: Manufacturing/electronics, 27 LinkedIn emp / 90 Apollo emp, CFO found, Seattle WA, private = ~83 (use Apollo's higher count, CFO overrides small LinkedIn footprint)
- SSI Construction: Construction, 12 LinkedIn emp, Co-Founder/CFO found, Kent WA, private = ~82 (Carillon pattern — CFO proves complexity despite small LinkedIn count)
- Seattle Chocolate Company: Food manufacturing, 40 emp, Owner/CEO found (no CFO), Seattle WA, private family-owned = ~82 (Carillon pattern — family-owned, owner is decision-maker, no visible finance staff but legitimate ICP target)

Return ONLY valid JSON."""


def _parse_revenue(revenue_str: str) -> float | None:
    """Parse revenue string like '$10M', '$1.5M', '$150M' to a float in dollars."""
    if not revenue_str:
        return None
    m = re.search(r'\$?([\d,.]+)\s*[Mm]', str(revenue_str))
    if m:
        return float(m.group(1).replace(',', '')) * 1_000_000
    m = re.search(r'\$?([\d,.]+)\s*[Bb]', str(revenue_str))
    if m:
        return float(m.group(1).replace(',', '')) * 1_000_000_000
    m = re.search(r'\$?([\d,.]+)', str(revenue_str))
    if m:
        return float(m.group(1).replace(',', ''))
    return None


def _parse_employees(emp_str) -> int | None:
    """Parse employee count from string or int."""
    if emp_str is None or emp_str == '':
        return None
    try:
        return int(str(emp_str).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def detect_revenue_mismatch(revenue_str: str, employees_str) -> bool:
    """Return True if revenue appears suspect (too low for employee count).

    Rule: if revenue / employees < $30K, revenue is likely wrong.
    Example: Machinists Inc — $1M revenue with 83 employees = $12K/emp → suspect.
    """
    revenue = _parse_revenue(revenue_str)
    employees = _parse_employees(employees_str)
    if revenue is None or employees is None or employees == 0:
        return False
    return (revenue / employees) < 30_000


def score_companies_v2(companies: list[dict], icp_config: dict | None = None, model: str | None = None) -> list[dict]:
    """Score companies with v2 algorithm (organizational complexity + reduced revenue weight).

    Changes from v1:
    - revenue_fit reduced from 0-15 to 0-10 (unreliable for private companies)
    - digital_footprint reduced from 0-15 to 0-10 (finance contacts signal moved out)
    - NEW: organizational_complexity 0-10 (CFO/Controller = strong signal)
    - Revenue mismatch detection: suspect revenue treated as unknown
    """
    if not companies:
        return []

    scoring_model = model or SCORING_MODEL
    logger.info("Scoring %d companies against ICP v2 (model=%s)", len(companies), scoring_model)
    client = get_openai()

    company_summaries = []
    for c in companies:
        li_employees = c.get("linkedin_employees", c.get("li_employees", ""))
        apollo_employees = c.get("apollo_employees", c.get("employees", c.get("employee_count", "")))

        # Revenue mismatch detection — trust employees over revenue
        revenue = c.get("revenue", "")
        emp_for_check = li_employees or apollo_employees
        revenue_suspect = detect_revenue_mismatch(revenue, emp_for_check)
        if revenue_suspect:
            logger.info("Revenue suspect for %s: %s rev with %s employees",
                        c.get("company_name", ""), revenue, emp_for_check)
            revenue = f"SUSPECT ({revenue} — inconsistent with {emp_for_check} employees, treat as unknown)"

        company_summaries.append({
            "company_id": c.get("company_id", c.get("google_place_id", "")),
            "company_name": c.get("company_name", c.get("name", "")),
            "industry": c.get("industry", ""),
            "linkedin_employees": li_employees,
            "apollo_employees": apollo_employees,
            "revenue": revenue,
            "location": c.get("location", c.get("address", "")),
            "ownership": c.get("ownership", ""),
            "linkedin_page": c.get("linkedin_page", c.get("company_linkedin", "")),
            "linkedin_followers": c.get("linkedin_followers", c.get("li_followers", "")),
            "google_places": c.get("google_places", ""),
            "website": c.get("website", c.get("domain", "")),
            "finance_titles": c.get("finance_titles", ""),
            "has_cfo": c.get("has_cfo", False),
            "has_controller": c.get("has_controller", False),
            "has_accounting_manager": c.get("has_accounting_manager", False),
            "finance_contact_name": c.get("finance_contact_name", ""),
            "finance_contact_linkedin": c.get("finance_contact_linkedin", ""),
            "big_firm_auditor": c.get("big_firm_auditor", ""),
            "notes": c.get("notes", ""),
        })

    user_message = json.dumps({"companies": company_summaries}, indent=2)

    response = client.chat.completions.create(
        model=scoring_model,
        messages=[
            {"role": "system", "content": COMPANY_SYSTEM_PROMPT_V2},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content
    parsed = json.loads(raw)

    if isinstance(parsed, dict) and "companies" in parsed:
        scores = parsed["companies"]
    elif isinstance(parsed, list):
        scores = parsed
    elif isinstance(parsed, dict) and "company_id" in parsed:
        scores = [parsed]
    else:
        scores = []

    # Post-processing: recalculate score from breakdown dimensions.
    # GPT sometimes gives a subjective total that doesn't match its own breakdown.
    for s in scores:
        breakdown = s.get("breakdown", {})
        if breakdown:
            calculated = 0
            for v in breakdown.values():
                try:
                    calculated += int(v)
                except (ValueError, TypeError):
                    pass
            calculated = min(calculated, 100)
            gpt_score = s.get("score", 0)
            if calculated != gpt_score and calculated > 0:
                logger.warning("Score mismatch for %s: GPT=%d, calculated=%d — using calculated",
                               s.get("company_name", "?"), gpt_score, calculated)
                s["score"] = calculated

    logger.info("Company scoring v2 complete: %d scores returned", len(scores))
    return scores


def classify_contact_activity(contact: dict) -> str:
    """Classify a contact as ACTIVE or INACTIVE based on LinkedIn engagement.

    ACTIVE = any of:
      - activity_score >= 4 (Moderate or above)
      - Any post, reaction, repost, or comment in last 90 days
      - Activity level is "Moderate", "Active", or "Very Active"

    INACTIVE = all of:
      - activity_score < 4 (Low or Inactive)
      - No posts/reactions/reposts/comments in last 90 days
      - connections < 10
      - OR: activity level is "Inactive" with 0 engagement ever
    """
    activity_score = 0
    try:
        activity_score = int(contact.get("activity_score") or 0)
    except (ValueError, TypeError):
        pass

    activity_level = str(contact.get("activity_level", "")).lower()
    connections = 0
    try:
        connections = int(contact.get("linkedin_connections",
                          contact.get("connections", contact.get("connectionsCount", 0))) or 0)
    except (ValueError, TypeError):
        pass

    days_since = None
    try:
        days_since = int(contact.get("days_since_last_activity") or 0)
    except (ValueError, TypeError):
        pass

    posts_30d = 0
    try:
        posts_30d = int(contact.get("posts_last_30_days",
                        contact.get("Posts Last 30 Days", 0)) or 0)
    except (ValueError, TypeError):
        pass

    reactions_30d = 0
    try:
        reactions_30d = int(contact.get("reactions_last_30_days",
                            contact.get("Reactions Last 30 Days", 0)) or 0)
    except (ValueError, TypeError):
        pass

    # ACTIVE checks
    if activity_score >= 4:
        return "ACTIVE"
    if activity_level in ("moderate", "active", "very active"):
        return "ACTIVE"
    if days_since is not None and days_since <= 90 and (posts_30d > 0 or reactions_30d > 0):
        return "ACTIVE"

    # INACTIVE checks
    if activity_level == "inactive" and connections < 10:
        return "INACTIVE"
    if activity_score < 4 and connections < 10 and (days_since is None or days_since > 90):
        return "INACTIVE"

    # Edge cases — low activity but enough connections to potentially see messages
    return "ACTIVE"
