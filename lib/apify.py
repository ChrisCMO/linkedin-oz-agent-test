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


def _request_with_retry(method: str, url: str, max_retries: int = 3,
                        timeout: tuple[int, int] = (10, 30),
                        **kwargs) -> requests.Response:
    """HTTP request with retry on timeout and 429/5xx errors.

    Args:
        timeout: (connect_timeout, read_timeout) in seconds.
                 Connect should be short (server is up or not).
                 Read can be longer (large dataset downloads).
    """
    kwargs.setdefault("headers", APIFY_HEADERS)
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code == 429:
                wait = min(30, 10 * (attempt + 1))
                logger.warning("Apify 429 — waiting %ds (attempt %d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                logger.warning("Apify %d — retrying (attempt %d/%d)", resp.status_code, attempt + 1, max_retries)
                time.sleep(5)
                continue
            return resp
        except requests.exceptions.Timeout:
            logger.warning("Apify timeout — retrying (attempt %d/%d)", attempt + 1, max_retries)
            time.sleep(5)
        except requests.exceptions.ConnectionError:
            logger.warning("Apify connection error — retrying (attempt %d/%d)", attempt + 1, max_retries)
            time.sleep(5)
    raise requests.exceptions.Timeout(f"Failed after {max_retries} retries: {url}")


def _estimate_max_wait(actor_id: str, payload: dict) -> int:
    """Estimate max poll time based on actor type and payload size.

    LinkedIn scrapers take ~5-10s per URL. SERP takes ~10-20s per query batch.
    We add generous headroom because Apify cold starts vary.
    """
    base = 60  # minimum wait for any actor

    if actor_id == COMPANY_SCRAPER:
        n_urls = len(payload.get("companies", []))
        return max(base, n_urls * 10 + 60)  # ~10s/url + 60s buffer

    if actor_id == PROFILE_SCRAPER:
        n_urls = len(payload.get("urls", []))
        return max(base, n_urls * 15 + 60)  # ~15s/url + 60s buffer

    if actor_id == SERP_ACTOR:
        queries = payload.get("queries", "")
        n_queries = len(queries.split("\n")) if isinstance(queries, str) else 1
        return max(base, n_queries * 10 + 60)  # ~10s/query + 60s buffer

    return 120  # default for unknown actors


def run_actor(actor_id: str, payload: dict, max_wait: int | None = None,
              retries: int = 2) -> list:
    """Start an Apify actor, poll until done, return dataset items.

    Args:
        actor_id: Apify actor ID.
        payload: Actor input payload.
        max_wait: Max seconds to poll. Auto-estimated from payload if None.
        retries: Number of full actor-run retries on failure/timeout.
    """
    if not APIFY_TOKEN:
        logger.warning("APIFY_API_KEY not set — skipping actor %s", actor_id)
        return []

    if max_wait is None:
        max_wait = _estimate_max_wait(actor_id, payload)

    actor_label = {
        COMPANY_SCRAPER: "CompanyScraper",
        PROFILE_SCRAPER: "ProfileScraper",
        SERP_ACTOR: "SERP",
    }.get(actor_id, actor_id[:8])

    for run_attempt in range(retries):
        t_start = time.time()
        try:
            r = _request_with_retry(
                "POST",
                f"https://api.apify.com/v2/acts/{actor_id}/runs",
                json=payload,
            )
            if r.status_code != 201:
                logger.warning("[%s] start failed: %d %s",
                               actor_label, r.status_code, r.text[:200])
                return []
            run_data = r.json()["data"]
            run_id = run_data["id"]
            ds = run_data["defaultDatasetId"]

            # Poll for completion
            status = "RUNNING"
            for _ in range(max_wait // 5):
                time.sleep(5)
                status_resp = _request_with_retry(
                    "GET",
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                )
                status = status_resp.json()["data"]["status"]
                if status in ("SUCCEEDED", "FAILED", "ABORTED"):
                    break

            elapsed = time.time() - t_start

            if status == "FAILED" or status == "ABORTED":
                logger.warning("[%s] %s after %.1fs (attempt %d/%d)",
                               actor_label, status, elapsed, run_attempt + 1, retries)
                if run_attempt < retries - 1:
                    time.sleep(5)
                    continue
                return []

            if status != "SUCCEEDED":
                logger.warning("[%s] still %s after %.1fs/max %ds — retrying (attempt %d/%d)",
                               actor_label, status, elapsed, max_wait,
                               run_attempt + 1, retries)
                if run_attempt < retries - 1:
                    time.sleep(5)
                    continue
                return []

            # Fetch results — use longer read timeout for large datasets
            resp = _request_with_retry(
                "GET",
                f"https://api.apify.com/v2/datasets/{ds}/items",
                timeout=(10, 60),
            )
            try:
                items = resp.json()
            except ValueError:
                logger.warning("[%s] non-JSON dataset response", actor_label)
                return []

            n_items = len(items) if isinstance(items, list) else 0
            logger.info("[%s] done: %d items in %.1fs (max_wait=%ds)",
                        actor_label, n_items, time.time() - t_start, max_wait)
            return items if isinstance(items, list) else []

        except Exception as e:
            elapsed = time.time() - t_start
            logger.warning("[%s] error after %.1fs: %s (attempt %d/%d)",
                           actor_label, elapsed, e, run_attempt + 1, retries)
            if run_attempt < retries - 1:
                time.sleep(5)
                continue
            return []

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



# Industry words too generic for company matching — "construction" alone
# matches thousands of unrelated companies.
_INDUSTRY_WORDS = {
    "construction", "manufacturing", "engineering", "consulting",
    "technology", "solutions", "industries", "industrial", "enterprises",
    "associates", "systems", "design", "development", "international",
    "logistics", "properties", "mechanical", "electrical", "aerospace",
    "fabrication", "welding", "machining", "automation", "supply",
}


def build_company_match_terms(company_name: str) -> list[str]:
    """Build match terms to verify X-ray results belong to the right company.

    For "SMC - Seattle Manufacturing Corporation":
      → ["seattle manufacturing corporation", "seattle manufacturing"]
    For "TASC - Technical & Assembly Services Corporation":
      → ["technical & assembly services corporation", "technical assembly"]
    For "CJ Construction Company":
      → ["cj construction"] (keeps short words when needed to avoid generic match)
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
            # Single word remaining — check if it's too generic (industry word)
            if words[0].lower() in _INDUSTRY_WORDS:
                # Include ALL words (even short ones) to make a more specific term
                # e.g., "CJ Construction" → "cj construction" instead of just "construction"
                all_words = [w for w in name.split()
                             if w.lower() not in GENERIC_WORDS]
                if len(all_words) >= 2:
                    terms.append(" ".join(all_words[:2]).lower())
                else:
                    terms.append(words[0].lower())
            else:
                terms.append(words[0].lower())

    return terms
