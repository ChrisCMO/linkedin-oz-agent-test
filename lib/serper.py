"""Serper.dev Google SERP API — fast alternative to Apify SERP actor.

Drop-in replacement for SERP queries in lib/xray.py.
Returns results in the same format as Apify SERP actor for compatibility.

Usage:
    from lib.serper import serper_search, serper_search_batch

    # Single query
    results = serper_search('site:linkedin.com/in "company.com" CFO')

    # Batch (returns list of result dicts, one per query — same as Apify)
    results = serper_search_batch([query1, query2, query3])
"""

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
SERPER_URL = "https://google.serper.dev/search"


def serper_search(query: str, num: int = 5, gl: str = "us") -> list[dict]:
    """Run a single SERP query via Serper.dev.

    Returns list of organic results in Apify-compatible format:
        [{"url": "...", "title": "...", "description": "..."}]
    """
    if not SERPER_API_KEY:
        logger.warning("SERPER_API_KEY not configured — skipping")
        return []

    try:
        resp = requests.post(SERPER_URL, json={
            "q": query,
            "gl": gl,
            "num": num,
        }, headers={"X-API-KEY": SERPER_API_KEY}, timeout=10)

        if resp.status_code != 200:
            logger.warning("Serper error %d: %s", resp.status_code, resp.text[:100])
            return []

        data = resp.json()
        # Convert Serper format → Apify-compatible format
        results = []
        for item in data.get("organic", []):
            results.append({
                "url": item.get("link", ""),
                "title": item.get("title", ""),
                "description": item.get("snippet", ""),
            })
        return results

    except Exception as e:
        logger.warning("Serper request failed: %s", e)
        return []


def serper_search_batch(queries: list[str], num: int = 5, gl: str = "us") -> list[dict]:
    """Run multiple SERP queries via Serper.dev.

    Returns list of result dicts (one per query), matching Apify SERP actor output:
        [{"organicResults": [{"url": ..., "title": ..., "description": ...}]}, ...]

    This format is compatible with _run_xray_queries() in lib/xray.py.
    """
    if not SERPER_API_KEY:
        logger.warning("SERPER_API_KEY not configured — skipping")
        return []

    results = []
    for query in queries:
        organic = serper_search(query, num=num, gl=gl)
        results.append({"organicResults": organic})
        time.sleep(0.1)  # Gentle rate limit

    return results
