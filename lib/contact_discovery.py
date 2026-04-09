"""Shared contact discovery module — Apollo + ZoomInfo + X-ray.

Used by both company_scorer (discovery only, no enrichment) and
prospect_enricher (discovery + validation + enrichment).

All contacts returned with source IDs preserved for later enrichment:
  - apollo_id: Apollo person ID
  - zoominfo_id: ZoomInfo contact ID
  - linkedin_slug: LinkedIn profile slug
"""

import logging
import os
import time
from datetime import datetime, timezone

import requests

from lib.apollo import ApolloClient
from lib.title_tiers import (
    TIER_1_TITLES, TIER_2_TITLES, TIER_3_TITLES,
    classify_title_tier,
)
from lib.xray import xray_discover_finance_contacts, xray_find_contact_linkedin

logger = logging.getLogger(__name__)

ALL_TARGET_TITLES = TIER_1_TITLES + TIER_2_TITLES + TIER_3_TITLES

# City → zip code mapping for ZoomInfo radius search
CITY_ZIP = {
    "seattle": "98101", "bellevue": "98004", "tacoma": "98402",
    "redmond": "98052", "kirkland": "98033", "everett": "98201",
    "renton": "98057", "kent": "98032", "auburn": "98002",
    "olympia": "98501", "lynnwood": "98036", "lakewood": "98499",
    "federal way": "98003", "vancouver": "98660", "ferndale": "98248",
    "puyallup": "98371", "bainbridge island": "98110", "woodinville": "98072",
}


def get_zip_for_location(location: str) -> str | None:
    """Extract zip code from company location string."""
    if not location:
        return None
    loc_lower = location.lower()
    for city, zip_code in CITY_ZIP.items():
        if city in loc_lower:
            return zip_code
    return None


def _extract_slug(linkedin_url: str) -> str:
    """Extract LinkedIn slug from URL."""
    if not linkedin_url or "/in/" not in linkedin_url:
        return ""
    return linkedin_url.split("/in/")[1].rstrip("/").split("?")[0]


# ---------------------------------------------------------------------------
# Source 1: Apollo free people search
# ---------------------------------------------------------------------------

def discover_contacts_apollo(apollo: ApolloClient, domain: str,
                             titles: list[str] | None = None) -> list[dict]:
    """Apollo free people search — 0 credits. Returns contacts with apollo_id.

    Args:
        titles: Override title list. Defaults to ALL_TARGET_TITLES (Tier 1+2+3).
    """
    if not domain:
        return []
    search_titles = titles or ALL_TARGET_TITLES
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_organization_domains_list": [domain],
            "person_titles": search_titles,
            "per_page": 10,
        })
        contacts = []
        for p in result.get("people", []):
            tier, tier_label = classify_title_tier(p.get("title", ""))
            contacts.append({
                "first_name": p.get("first_name", ""),
                "last_name": p.get("last_name", ""),
                "name": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                "title": p.get("title", ""),
                "headline": p.get("headline", ""),
                "seniority": p.get("seniority", ""),
                "linkedin_url": p.get("linkedin_url", ""),
                "linkedin_slug": _extract_slug(p.get("linkedin_url", "")),
                "apollo_id": p.get("id", ""),
                "zoominfo_id": "",
                "email": p.get("email", ""),
                "city": p.get("city", ""),
                "state": p.get("state", ""),
                "source": "apollo_search",
                "sources": ["apollo"],
                "tier": tier,
                "tier_label": tier_label,
                "verified": False,  # Not profile-verified, but Apollo data is trusted
            })
        return contacts
    except Exception as e:
        logger.warning("Apollo contact search failed for %s: %s", domain, e)
        return []


# ---------------------------------------------------------------------------
# Source 2: ZoomInfo free contact search
# ---------------------------------------------------------------------------

def discover_contacts_zoominfo(company_name: str, location: str,
                               titles: list[str] | None = None) -> list[dict]:
    """ZoomInfo free contact search — SEARCH ONLY, no enrichment credits.

    Returns contacts with zoominfo_id (no LinkedIn URL from ZoomInfo).
    """
    username = os.environ.get("ZOOMINFO_USERNAME", "")
    password = os.environ.get("ZOOMINFO_PASSWORD", "")
    if not username or not password:
        logger.info("ZoomInfo credentials not configured — skipping")
        return []

    zip_code = get_zip_for_location(location)
    if not zip_code:
        logger.info("No zip code for location '%s' — skipping ZoomInfo", location)
        return []

    search_titles = titles or TIER_1_TITLES + TIER_3_TITLES  # Finance only for ZoomInfo
    try:
        auth_resp = requests.post("https://api.zoominfo.com/authenticate", json={
            "username": username, "password": password,
        }, timeout=15)
        if auth_resp.status_code != 200:
            logger.warning("ZoomInfo auth failed: %d", auth_resp.status_code)
            return []
        token = auth_resp.json().get("jwt", "")

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        search_resp = requests.post("https://api.zoominfo.com/search/contact", headers=headers, json={
            "companyName": company_name,
            "jobTitle": " OR ".join(search_titles),
            "zipCode": zip_code,
            "zipCodeRadiusMiles": "50",
            "rpp": 5,
        }, timeout=15)

        contacts = []
        if search_resp.status_code == 200:
            for c in search_resp.json().get("data", []):
                tier, tier_label = classify_title_tier(c.get("jobTitle", ""))
                contacts.append({
                    "first_name": c.get("firstName", ""),
                    "last_name": c.get("lastName", ""),
                    "name": f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
                    "title": c.get("jobTitle", ""),
                    "headline": "",
                    "seniority": "",
                    "linkedin_url": "",  # ZoomInfo doesn't return LinkedIn URLs
                    "linkedin_slug": "",
                    "apollo_id": "",
                    "zoominfo_id": str(c.get("id", "")),
                    "email": "",
                    "city": c.get("city", ""),
                    "state": c.get("state", ""),
                    "source": "zoominfo_search",
                    "sources": ["zoominfo"],
                    "tier": tier,
                    "tier_label": tier_label,
                    "verified": False,
                })
        return contacts
    except Exception as e:
        logger.warning("ZoomInfo search failed for %s: %s", company_name, e)
        return []


# ---------------------------------------------------------------------------
# Source 3: X-ray Google search + profile verification
# ---------------------------------------------------------------------------

def discover_contacts_xray(company_name: str, domain: str | None,
                           max_tier: int = 3) -> dict:
    """X-ray discovery with LinkedIn profile verification.

    Returns dict with "contacts", "verified", "rejected" lists.
    X-ray contacts are profile-scraped — verified=True means confirmed at company.
    """
    return xray_discover_finance_contacts(company_name, domain=domain, max_tier=max_tier)


# ---------------------------------------------------------------------------
# Cross-matching: get Apollo ID for non-Apollo contacts
# ---------------------------------------------------------------------------

def crossmatch_apollo(apollo: ApolloClient, first_name: str, last_name: str,
                      company_name: str) -> dict | None:
    """Cross-match a ZoomInfo/X-ray contact against Apollo to get apollo_id + LinkedIn URL.

    Free search, 0 credits.
    """
    try:
        result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
            "q_keywords": f"{first_name} {last_name} {company_name}",
            "per_page": 3,
        })
        for p in result.get("people", []):
            p_first = p.get("first_name", "").lower()
            p_last = p.get("last_name", "").lower()
            fn_lower = first_name.lower()
            # Strict: exact last name + first name prefix match
            if p_last == last_name.lower() and (
                p_first == fn_lower or p_first.startswith(fn_lower) or fn_lower.startswith(p_first)
            ):
                return {
                    "apollo_id": p.get("id", ""),
                    "linkedin_url": p.get("linkedin_url", ""),
                    "email": p.get("email", ""),
                    "seniority": p.get("seniority", ""),
                    "headline": p.get("headline", ""),
                }
        return None
    except Exception as e:
        logger.warning("Apollo cross-match failed for %s %s: %s", first_name, last_name, e)
        return None


# ---------------------------------------------------------------------------
# Orchestrator: full multi-source discovery
# ---------------------------------------------------------------------------

def discover_all_contacts(apollo: ApolloClient, company: dict,
                          log_fn=None) -> list[dict]:
    """Run full contact discovery across all sources for a company.

    Args:
        company: dict with name, domain, location
        log_fn: Optional callback(source, endpoint, contacts_found, contacts_verified,
                contacts_rejected, duration_ms, error_message, request_params)
                for audit logging to discovery_log table.

    Returns list of deduplicated contacts with all source IDs.
    """
    name = company.get("name", "")
    domain = company.get("domain", "")
    location = company.get("location", "")
    seen_keys = set()
    all_contacts = []

    def _dedup_key(c):
        slug = c.get("linkedin_slug") or _extract_slug(c.get("linkedin_url", ""))
        if slug:
            return f"li:{slug}"
        return f"name:{c.get('first_name', '').lower()}-{c.get('last_name', '').lower()}-{name.lower()}"

    def add_contacts(contacts, source_name):
        added = 0
        for c in contacts:
            key = _dedup_key(c)
            if key in seen_keys:
                # Merge source IDs into existing contact
                for existing in all_contacts:
                    if _dedup_key(existing) == key:
                        if c.get("apollo_id") and not existing.get("apollo_id"):
                            existing["apollo_id"] = c["apollo_id"]
                        if c.get("zoominfo_id") and not existing.get("zoominfo_id"):
                            existing["zoominfo_id"] = c["zoominfo_id"]
                        if c.get("linkedin_url") and not existing.get("linkedin_url"):
                            existing["linkedin_url"] = c["linkedin_url"]
                            existing["linkedin_slug"] = _extract_slug(c["linkedin_url"])
                        if c.get("email") and not existing.get("email"):
                            existing["email"] = c["email"]
                        if source_name not in existing.get("sources", []):
                            existing["sources"].append(source_name)
                        break
                continue
            seen_keys.add(key)
            all_contacts.append(c)
            added += 1
        if added:
            logger.info("  %s: +%d contacts (total: %d)", source_name, added, len(all_contacts))
        return added

    # --- Source 1: Apollo free search (all tiers) ---
    print(f"    Apollo...", end=" ", flush=True)
    t0 = time.time()
    apollo_contacts = discover_contacts_apollo(apollo, domain)
    apollo_ms = int((time.time() - t0) * 1000)
    added = add_contacts(apollo_contacts, "apollo")
    print(f"{len(apollo_contacts)} found")
    if log_fn:
        log_fn("apollo", "/api/v1/mixed_people/api_search",
               len(apollo_contacts), 0, 0, apollo_ms, None,
               {"domain": domain, "titles": "ALL_TARGET_TITLES"})
    time.sleep(0.5)

    # --- Source 2: ZoomInfo free search (if Apollo missed finance contacts) ---
    apollo_has_finance = any(
        c.get("tier") == 1 for c in apollo_contacts
    )
    zi_contacts = []
    if not apollo_has_finance:
        print(f"    ZoomInfo...", end=" ", flush=True)
        t0 = time.time()
        zi_contacts = discover_contacts_zoominfo(name, location)
        zi_ms = int((time.time() - t0) * 1000)

        # Cross-match against Apollo for IDs + LinkedIn URLs
        for c in zi_contacts:
            match = crossmatch_apollo(apollo, c["first_name"], c["last_name"], name)
            if match:
                c["apollo_id"] = match.get("apollo_id", "")
                c["linkedin_url"] = match.get("linkedin_url", "")
                c["linkedin_slug"] = _extract_slug(match.get("linkedin_url", ""))
                c["email"] = match.get("email", "") or c.get("email", "")
                c["sources"].append("apollo")
            time.sleep(0.3)

        added = add_contacts(zi_contacts, "zoominfo")
        print(f"{len(zi_contacts)} found")
        if log_fn:
            log_fn("zoominfo", "search/contact",
                   len(zi_contacts), 0, 0, zi_ms, None,
                   {"company": name, "location": location})
    else:
        if log_fn:
            log_fn("zoominfo", "search/contact", 0, 0, 0, 0, "skipped: Apollo found Tier 1", {})

    # --- Source 3: X-ray with profile verification (if still no finance contacts) ---
    has_finance = any(c.get("tier") == 1 for c in all_contacts)
    xray_verified = []
    xray_rejected = []
    if not has_finance and (domain or name):
        print(f"    X-ray...", end=" ", flush=True)
        t0 = time.time()
        xray_result = discover_contacts_xray(name, domain=domain or None, max_tier=3)
        xray_ms = int((time.time() - t0) * 1000)
        xray_verified = xray_result.get("verified", [])
        xray_rejected = xray_result.get("rejected", [])

        # Normalize X-ray contacts to standard format + cross-match
        for c in xray_verified:
            c["linkedin_slug"] = _extract_slug(c.get("linkedin_url", ""))
            c["zoominfo_id"] = ""
            c["sources"] = ["xray"]
            c["source"] = "linkedin_search"
            c["verified"] = True
            tier, tier_label = classify_title_tier(c.get("title", ""))
            c["tier"] = tier
            c["tier_label"] = tier_label

            # Cross-match for Apollo ID
            match = crossmatch_apollo(
                apollo, c.get("first_name", ""), c.get("last_name", ""), name
            )
            if match:
                c["apollo_id"] = match.get("apollo_id", "")
                c["email"] = match.get("email", "")
                c["sources"].append("apollo")
            else:
                c.setdefault("apollo_id", "")
                c.setdefault("email", "")
            time.sleep(0.3)

        added = add_contacts(xray_verified, "xray")
        print(f"{len(xray_verified)} verified, {len(xray_rejected)} rejected")
        if log_fn:
            log_fn("xray", "serp+profile_scrape",
                   len(xray_result.get("contacts", [])),
                   len(xray_verified), len(xray_rejected), xray_ms, None,
                   {"company": name, "domain": domain})
    else:
        if log_fn:
            reason = "skipped: Tier 1 already found" if has_finance else "skipped: no domain"
            log_fn("xray", "serp+profile_scrape", 0, 0, 0, 0, reason, {})

    # NOTE: Do NOT use Serper/X-ray to find LinkedIn URLs for Apollo/ZoomInfo contacts.
    # Those URLs will come from Apollo enrichment in Stage 2 (prospect pipeline).
    # Serper credits should only be spent on X-ray discovery of NEW contacts.

    return all_contacts
