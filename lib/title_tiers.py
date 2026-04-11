"""Centralized title tier definitions for VWC ICP prospect pipeline.

3-tier hierarchy based on Chad's ICP spec (March 2025):
  Tier 1 — Primary Finance: CFO, Controller, etc. (always target first)
  Tier 2 — Executive: Owner, President, CEO (secondary option)
  Tier 3 — Junior Finance: Accounting Manager, etc. (last resort per Chad's spec)

All pipeline code should import from here instead of defining local title lists.
"""

# ---------------------------------------------------------------------------
# Tier definitions
# ---------------------------------------------------------------------------

TIER_1_TITLES = [
    "CFO", "Chief Financial Officer",
    "Controller", "Financial Controller",
    "VP Finance", "VP of Finance", "Vice President of Finance",
    "Director of Finance", "Finance Director",
]

TIER_2_TITLES = [
    "Owner", "President", "CEO",
    "Founder", "Managing Director",
    "Partner", "Executive Director",
]

TIER_3_TITLES = [
    "Accounting Manager",
    "Finance Manager",
    "Treasurer",
    "Bookkeeper",
    "Staff Accountant",
]

ALL_TITLES = TIER_1_TITLES + TIER_2_TITLES + TIER_3_TITLES

TIER_LABELS = {
    1: "Primary Finance",
    2: "Executive",
    3: "Junior Finance",
    0: "Unknown",
}

# Lowercase substrings → tier mapping for classify_title_tier().
# Order matters: check most specific first (e.g., "chief financial" before "financial").
_TIER_MAP = [
    # Tier 1
    ("chief financial", 1),
    ("cfo", 1),
    ("controller", 1),
    ("vp finance", 1),
    ("vp of finance", 1),
    ("vice president of finance", 1),
    ("vice president, finance", 1),
    ("director of finance", 1),
    ("director, finance", 1),
    ("finance director", 1),
    ("financial controller", 1),
    # Tier 2
    ("chief executive", 2),
    ("managing director", 2),
    ("executive director", 2),
    ("president", 2),
    ("owner", 2),
    ("ceo", 2),
    ("founder", 2),
    ("partner", 2),
    # Tier 3
    ("accounting manager", 3),
    ("finance manager", 3),
    ("treasurer", 3),
    ("bookkeeper", 3),
    ("staff accountant", 3),
]


def classify_title_tier(title: str) -> tuple[int, str]:
    """Classify a job title into its tier.

    Returns (tier_number, tier_label). Returns (0, "Unknown") for unrecognized titles.
    """
    if not title:
        return (0, "Unknown")
    t = title.lower()
    for keyword, tier in _TIER_MAP:
        if keyword in t:
            return (tier, TIER_LABELS[tier])
    return (0, "Unknown")


# ---------------------------------------------------------------------------
# API-specific formatters
# ---------------------------------------------------------------------------

def get_titles_for_apollo(tier: int | None = None) -> list[str]:
    """Return title list for Apollo person_titles parameter.

    If tier is specified, only return titles for that tier.
    """
    if tier == 1:
        return list(TIER_1_TITLES)
    if tier == 2:
        return list(TIER_2_TITLES)
    if tier == 3:
        return list(TIER_3_TITLES)
    return list(ALL_TITLES)


def get_titles_for_zoominfo(tier: int | None = None) -> str:
    """Return OR-joined title string for ZoomInfo jobTitle parameter."""
    titles = get_titles_for_apollo(tier)
    return " OR ".join(titles)


# ---------------------------------------------------------------------------
# X-ray search keywords
# ---------------------------------------------------------------------------

# (search_query_keyword, display_label, tier)
_XRAY_KEYWORDS = [
    # Tier 1
    ("CFO", "CFO", 1),
    ('"chief financial officer"', "Chief Financial Officer", 1),
    ("controller", "Controller", 1),
    ('"director of finance"', "Director of Finance", 1),
    ('"vp finance" OR "vp of finance"', "VP Finance", 1),
    # Tier 2
    ("owner OR president OR CEO", "Owner/President/CEO", 2),
    ('"managing director" OR "executive director"', "Managing/Executive Director", 2),
    # Tier 3
    ('"accounting manager"', "Accounting Manager", 3),
    ('"finance manager" OR treasurer', "Finance Manager/Treasurer", 3),
]

# Snippet pre-filter keywords — check Google snippet before scraping profile
FINANCE_SNIPPET_KEYWORDS = [
    "cfo", "chief financial", "controller", "financial controller",
    "vp finance", "vp of finance", "vice president of finance",
    "vice president, finance", "director of finance", "director, finance",
    "finance director", "treasurer", "accounting manager", "finance manager",
    "bookkeeper", "staff accountant",
    "owner", "president", "ceo", "founder", "managing director",
    "executive director", "partner",
]


def get_xray_keywords(tier: int | None = None) -> list[tuple[str, str, int]]:
    """Return X-ray search keywords as (query_keyword, display_label, tier).

    If tier is specified, only return keywords for that tier.
    """
    if tier is None:
        return list(_XRAY_KEYWORDS)
    return [(kw, label, t) for kw, label, t in _XRAY_KEYWORDS if t == tier]
