"""Shared Apify utilities — actor runner, domain extraction, company name matching."""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

APIFY_TOKEN = os.environ.get("APIFY_API_KEY", "")
APIFY_HEADERS = {
    "Authorization": f"Bearer {APIFY_TOKEN}",
    "Content-Type": "application/json",
}

# Actor IDs
COMPANY_SCRAPER = "UwSdACBp7ymaGUJjS"   # LinkedIn company page scraper
PROFILE_SCRAPER = "LpVuK3Zozwuipa5bp"    # LinkedIn profile scraper
SERP_ACTOR = "nFJndFXA5zjCTuudP"          # Google SERP for X-ray search

# Domains that are social media / marketplace pages, not real company websites.
# Google Maps sometimes returns these as a company's "website".
JUNK_DOMAINS = {
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com", "snapchat.com",
    "amazon.com", "lazada.com", "shopee.com", "ebay.com", "etsy.com",
    "alibaba.com", "aliexpress.com",
    "yelp.com", "yellowpages.com", "bbb.org", "mapquest.com",
    "google.com", "goo.gl", "bit.ly", "linktr.ee",
    "wix.com", "squarespace.com", "godaddy.com", "wordpress.com",
}


def run_actor(actor_id: str, payload: dict, max_wait: int = 120) -> list:
    """Start an Apify actor, poll until done, return dataset items."""
    if not APIFY_TOKEN:
        logger.warning("APIFY_API_KEY not set — skipping actor %s", actor_id)
        return []
    try:
        r = requests.post(
            f"https://api.apify.com/v2/acts/{actor_id}/runs",
            headers=APIFY_HEADERS, json=payload, timeout=30,
        )
        if r.status_code != 201:
            logger.warning("Actor %s start failed: %d %s", actor_id, r.status_code, r.text[:200])
            return []
        run_data = r.json()["data"]
        run_id = run_data["id"]
        ds = run_data["defaultDatasetId"]

        for _ in range(max_wait // 5):
            time.sleep(5)
            status_resp = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                headers=APIFY_HEADERS, timeout=15,
            )
            status = status_resp.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED"):
                break

        items = requests.get(
            f"https://api.apify.com/v2/datasets/{ds}/items",
            headers=APIFY_HEADERS, timeout=15,
        ).json()
        return items if isinstance(items, list) else []
    except Exception as e:
        logger.warning("Apify actor %s failed: %s", actor_id, e)
        return []


def extract_domain(website: str | None) -> str | None:
    """Extract domain from website URL, filtering out junk domains."""
    if not website:
        return None
    raw = (website
           .replace("https://", "")
           .replace("http://", "")
           .replace("www.", "")
           .split("/")[0]
           .strip())
    if raw.lower() in JUNK_DOMAINS:
        return None
    return raw if raw else None


def build_company_match_terms(company_name: str) -> list[str]:
    """Build match terms to verify X-ray results belong to the right company.

    For "SMC - Seattle Manufacturing Corporation":
      → ["seattle manufacturing corporation", "seattle manufacturing"]
    For "TASC - Technical & Assembly Services Corporation":
      → ["technical & assembly services corporation", "technical assembly"]
    """
    GENERIC_WORDS = {
        "the", "inc", "inc.", "llc", "corp", "corp.", "corporation",
        "company", "co", "co.", "ltd", "group", "services", "management",
    }
    terms = []
    name = company_name.strip()

    # If name has a separator like " - ", use the longer part as the real name
    for sep in [" - ", " – ", " — "]:
        if sep in name:
            parts = name.split(sep)
            long_part = max(parts, key=len).strip()
            terms.append(long_part.lower())
            words = [w for w in long_part.split()
                     if len(w) > 2 and w.lower() not in GENERIC_WORDS]
            if len(words) >= 2:
                terms.append(" ".join(words[:2]).lower())
            break

    # Fallback: filter out generic words, take first 2+ meaningful words
    if not terms:
        words = [w for w in name.split()
                 if len(w) > 2 and w.lower() not in GENERIC_WORDS]
        if len(words) >= 2:
            terms.append(" ".join(words[:2]).lower())
        elif words:
            terms.append(words[0].lower())

    return terms
