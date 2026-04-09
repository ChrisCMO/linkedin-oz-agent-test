"""X-ray discovery — Google SERP search + LinkedIn profile verification for finance contacts.

SERP provider: Serper.dev (primary, fast) with Apify SERP actor as fallback.
Toggle via SERP_PROVIDER env var: "serper" (default) or "apify".
"""

import logging
import os
import time

import config
from lib.apify import (
    run_actor, build_company_match_terms,
    SERP_ACTOR, PROFILE_SCRAPER,
)
from lib.serper import serper_search_batch
from lib.title_tiers import get_xray_keywords, classify_title_tier, FINANCE_SNIPPET_KEYWORDS

# "serper" (default, fast) or "apify" (legacy, slower)
SERP_PROVIDER = os.environ.get("SERP_PROVIDER", "serper").lower()

logger = logging.getLogger(__name__)


def xray_find_contact_linkedin(contacts: list[dict], company_name: str) -> list[dict]:
    """Find LinkedIn URLs for Apollo contacts that are missing them."""
    if not config.APIFY_SERP_ENABLED:
        logger.info("X-ray SERP disabled (APIFY_SERP_ENABLED=false) — skipping LinkedIn URL lookup")
        return contacts
    needs_lookup = [c for c in contacts if not c.get("linkedin_url")]
    if not needs_lookup:
        return contacts

    queries = []
    for c in needs_lookup:
        name = c.get("first_name", c.get("name", ""))
        queries.append(f'site:linkedin.com/in "{name}" "{company_name}"')

    logger.info("X-ray: searching %d contacts via SERP (%s)...", len(queries), SERP_PROVIDER)
    if SERP_PROVIDER == "serper":
        results = serper_search_batch(queries, num=3)
    else:
        results = run_actor(SERP_ACTOR, {
            "queries": "\n".join(queries),
            "maxPagesPerQuery": 1,
            "resultsPerPage": 3,
            "countryCode": "us",
        })

    for i, c in enumerate(needs_lookup):
        if i >= len(results):
            break
        name_lower = c.get("first_name", "").lower()
        for item in results[i].get("organicResults", []):
            url = item.get("url", "")
            title_text = (item.get("title", "") or "").lower()
            if "linkedin.com/in/" in url and name_lower in title_text:
                c["linkedin_url"] = url
                full_name = item.get("title", "").split(" - ")[0].split(" | ")[0].strip()
                if full_name and len(full_name) > len(c.get("name", "")):
                    c["name"] = full_name
                    parts = full_name.split(" ", 1)
                    c["first_name"] = parts[0]
                    c["last_name"] = parts[1] if len(parts) > 1 else ""
                logger.info("  %s → %s", c.get("name", "?"), url)
                break

    return contacts


def _run_xray_queries(keywords, company_name, domain, match_terms, seen_urls):
    """Run X-ray SERP queries for a set of title keywords. Returns raw contacts."""
    search_name = match_terms[0] if match_terms else company_name

    queries = []
    for kw, label, tier in keywords:
        if domain:
            queries.append(f'site:linkedin.com/in "{domain}" {kw}')
        else:
            queries.append(f'site:linkedin.com/in "{search_name}" {kw}')

    if not queries:
        return []

    if SERP_PROVIDER == "serper":
        results = serper_search_batch(queries, num=5)
        logger.info("  [Serper] %d queries", len(queries))
    else:
        results = run_actor(SERP_ACTOR, {
            "queries": "\n".join(queries),
            "maxPagesPerQuery": 1,
            "resultsPerPage": 5,
            "countryCode": "us",
        })

    raw_contacts = []
    for i, batch in enumerate(results or []):
        kw_tier = keywords[i][2] if i < len(keywords) else 0
        kw_label = keywords[i][1] if i < len(keywords) else "Unknown"
        for item in batch.get("organicResults", []):
            url = item.get("url", "")
            if "linkedin.com/in/" not in url or url in seen_urls:
                continue
            title_text = item.get("title", "")
            desc = item.get("description", "")
            combined = (title_text + " " + desc).lower()

            # Snippet pre-filter — skip obviously non-matching results
            if not any(sk in combined for sk in FINANCE_SNIPPET_KEYWORDS):
                logger.debug("  SKIP (no snippet match): %s", title_text[:60])
                continue

            matched = any(term in combined for term in match_terms)
            if not matched:
                logger.debug("  SKIP (no company match): %s", title_text[:60])
                continue

            seen_urls.add(url)
            name_part = title_text.split(" - ")[0].split(" | ")[0].strip()
            parts = name_part.split(" ", 1)
            raw_contacts.append({
                "name": name_part,
                "first_name": parts[0] if parts else "",
                "last_name": parts[1] if len(parts) > 1 else "",
                "title": kw_label,
                "linkedin_url": url,
                "tier": kw_tier,
                "tier_label": {1: "Primary Finance", 2: "Executive", 3: "Junior Finance"}.get(kw_tier, "Unknown"),
            })
            logger.info("  MATCH: %s (%s, T%d) → %s", name_part, kw_label, kw_tier, url)

    return raw_contacts


def xray_discover_finance_contacts(
    company_name: str,
    domain: str | None = None,
    max_tier: int = 1,
) -> dict:
    """X-ray search for contacts. Tier 1 always runs. Higher tiers only if max_tier allows.

    Args:
        max_tier: 1 = finance titles only (company_scorer default).
                  3 = full tiered search: T1 always, T2 if < 2 found, T3 if 0 found
                      (for prospect_enricher outreach targets).

    Returns {"contacts": [...], "verified": [...], "rejected": [...]}.
    """
    if not config.APIFY_SERP_ENABLED:
        logger.info("X-ray SERP disabled (APIFY_SERP_ENABLED=false) — skipping contact discovery")
        return {"contacts": [], "verified": [], "rejected": []}

    match_terms = build_company_match_terms(company_name)
    seen_urls = set()

    # Tier 1 — always run
    tier1_kw = get_xray_keywords(tier=1)
    logger.info("X-ray Tier 1: searching finance titles for %s", domain or company_name)
    raw_contacts = _run_xray_queries(tier1_kw, company_name, domain, match_terms, seen_urls)
    logger.info("  Tier 1: %d raw contacts", len(raw_contacts))

    # Tier 2 — run if allowed and < 2 Tier 1 contacts found
    if max_tier >= 2 and len(raw_contacts) < 2:
        tier2_kw = get_xray_keywords(tier=2)
        logger.info("X-ray Tier 2: searching executive titles for %s", domain or company_name)
        tier2_contacts = _run_xray_queries(tier2_kw, company_name, domain, match_terms, seen_urls)
        raw_contacts.extend(tier2_contacts)
        logger.info("  Tier 2: %d additional contacts", len(tier2_contacts))

    # Tier 3 — run if allowed and still 0 contacts
    if max_tier >= 3 and len(raw_contacts) == 0:
        tier3_kw = get_xray_keywords(tier=3)
        logger.info("X-ray Tier 3: searching junior finance titles for %s", domain or company_name)
        tier3_contacts = _run_xray_queries(tier3_kw, company_name, domain, match_terms, seen_urls)
        raw_contacts.extend(tier3_contacts)
        logger.info("  Tier 3: %d additional contacts", len(tier3_contacts))

    # --- Tier 3: Profile scrape verification ---
    verified = []
    rejected = []

    if raw_contacts:
        logger.info("Tier 3: Verifying %d X-ray contacts via profile scrape...", len(raw_contacts))
        urls_to_scrape = [
            c["linkedin_url"].replace("http://", "https://") if not c["linkedin_url"].startswith("https://")
            else c["linkedin_url"]
            for c in raw_contacts if c.get("linkedin_url")
        ]

        profiles = run_actor(PROFILE_SCRAPER, {"urls": urls_to_scrape}) if urls_to_scrape else []
        profile_by_url = {}
        for p in profiles:
            p_url = p.get("url", p.get("linkedinUrl", ""))
            if p_url:
                profile_by_url[p_url.rstrip("/")] = p

        for c in raw_contacts:
            li = c["linkedin_url"].rstrip("/")
            li_https = li if li.startswith("https://") else li.replace("http://", "https://")
            profile = profile_by_url.get(li_https) or profile_by_url.get(li, {})

            if not profile:
                logger.info("  UNVERIFIED (no profile data): %s", c["name"])
                rejected.append({**c, "reason": "no profile data"})
                continue

            headline = (profile.get("headline") or "").lower()
            current_positions = profile.get("currentPosition") or []
            current_companies = [pos.get("companyName", "").lower() for pos in current_positions]
            current_titles = [pos.get("title", "").lower() for pos in current_positions]

            company_text = " ".join(current_companies + [headline])
            company_matched = any(
                term in company_text
                for term in match_terms
            )

            # Extra check: if match term is a single generic industry word,
            # require at least 2 match terms to hit, or verify location is PNW
            if company_matched and len(match_terms) == 1:
                from lib.apify import _INDUSTRY_WORDS
                term = match_terms[0]
                single_words = term.split()
                if len(single_words) == 1 and single_words[0] in _INDUSTRY_WORDS:
                    # Generic match — verify location to reduce false positives
                    profile_location = (profile.get("location", "") or "").lower()
                    pnw_markers = ["seattle", "bellevue", "tacoma", "washington",
                                   "portland", "oregon", ", wa", ", or"]
                    if not any(m in profile_location for m in pnw_markers):
                        actual = current_companies[0] if current_companies else headline[:50]
                        loc = profile.get("location", "unknown location")
                        logger.info("  REJECTED (generic match + wrong location): %s — at \"%s\" in %s",
                                    c["name"], actual, loc)
                        rejected.append({**c, "reason": f"generic match, wrong location: {actual} ({loc})"})
                        continue

            if not company_matched:
                actual = current_companies[0] if current_companies else headline[:50]
                logger.info("  REJECTED (wrong company): %s — actually at \"%s\"", c["name"], actual)
                rejected.append({**c, "reason": f"wrong company: {actual}"})
                continue

            # Update title from live profile and re-classify tier
            live_title = current_titles[0] if current_titles else profile.get("headline", "")
            if live_title:
                c["title"] = live_title.title()
                tier, tier_label = classify_title_tier(c["title"])
                if tier > 0:
                    c["tier"] = tier
                    c["tier_label"] = tier_label
            c["connections"] = profile.get("connectionsCount", "")
            verified.append(c)
            logger.info("  VERIFIED: %s — %s (T%d, %s connections)",
                        c["name"], c["title"], c.get("tier", 0), c.get("connections", "?"))

    return {
        "contacts": raw_contacts,
        "verified": verified,
        "rejected": rejected,
    }
