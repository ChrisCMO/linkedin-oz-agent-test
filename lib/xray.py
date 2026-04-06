"""X-ray discovery — Google SERP search + LinkedIn profile verification for finance contacts."""

import logging
import time

from lib.apify import (
    run_actor, build_company_match_terms,
    SERP_ACTOR, PROFILE_SCRAPER,
)

logger = logging.getLogger(__name__)


def xray_find_contact_linkedin(contacts: list[dict], company_name: str) -> list[dict]:
    """Find LinkedIn URLs for Apollo contacts that are missing them."""
    needs_lookup = [c for c in contacts if not c.get("linkedin_url")]
    if not needs_lookup:
        return contacts

    queries = []
    for c in needs_lookup:
        name = c.get("first_name", c.get("name", ""))
        queries.append(f'site:linkedin.com/in "{name}" "{company_name}"')

    logger.info("X-ray: searching %d contacts via SERP...", len(queries))
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


def xray_discover_finance_contacts(
    company_name: str,
    domain: str | None = None,
) -> dict:
    """Tier 2+3: Google X-ray search for finance titles, then profile scrape verification.

    Returns {"contacts": [...], "verified": [...], "rejected": [...]}.
    """
    title_keywords = [
        ("CFO", "CFO"),
        ('"chief financial officer"', "Chief Financial Officer"),
        ("controller", "Controller"),
        ('"director of finance"', "Director of Finance"),
    ]

    match_terms = build_company_match_terms(company_name)
    search_name = match_terms[0] if match_terms else company_name

    # Build queries: domain-first if available
    queries = []
    for kw, _ in title_keywords:
        if domain:
            queries.append(f'site:linkedin.com/in "{domain}" {kw}')
        else:
            queries.append(f'site:linkedin.com/in "{search_name}" {kw}')

    logger.info("X-ray Tier 2: searching finance titles (term: %s)", domain or search_name)
    results = run_actor(SERP_ACTOR, {
        "queries": "\n".join(queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 5,
        "countryCode": "us",
    })

    raw_contacts = []
    seen_urls = set()

    for i, batch in enumerate(results or []):
        title_label = title_keywords[i][1] if i < len(title_keywords) else "Finance"
        for item in batch.get("organicResults", []):
            url = item.get("url", "")
            if "linkedin.com/in/" not in url or url in seen_urls:
                continue
            title_text = item.get("title", "")
            desc = item.get("description", "")
            combined = (title_text + " " + desc).lower()

            matched = any(term in combined for term in match_terms)
            if not matched:
                logger.debug("  SKIP (no match): %s", title_text[:60])
                continue

            seen_urls.add(url)
            name_part = title_text.split(" - ")[0].split(" | ")[0].strip()
            parts = name_part.split(" ", 1)
            raw_contacts.append({
                "name": name_part,
                "first_name": parts[0] if parts else "",
                "last_name": parts[1] if len(parts) > 1 else "",
                "title": title_label,
                "linkedin_url": url,
            })
            logger.info("  MATCH: %s (%s) → %s", name_part, title_label, url)

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

            company_matched = any(
                term in " ".join(current_companies + [headline])
                for term in match_terms
            )
            if not company_matched:
                actual = current_companies[0] if current_companies else headline[:50]
                logger.info("  REJECTED (wrong company): %s — actually at \"%s\"", c["name"], actual)
                rejected.append({**c, "reason": f"wrong company: {actual}"})
                continue

            # Update title from live profile
            live_title = current_titles[0] if current_titles else profile.get("headline", "")
            c["title"] = live_title.title() if live_title else c["title"]
            c["connections"] = profile.get("connectionsCount", "")
            verified.append(c)
            logger.info("  VERIFIED: %s — %s (%s connections)", c["name"], c["title"], c.get("connections", "?"))

    return {
        "contacts": raw_contacts,
        "verified": verified,
        "rejected": rejected,
    }
