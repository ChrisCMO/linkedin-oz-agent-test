"""Integrated contact discovery — Apollo + ZoomInfo (by company ID) + Serper + Apify verify.

Single function that runs the full discovery pipeline per company:
1. Apollo free people search (by domain) → contacts with LinkedIn URLs
2. ZoomInfo free search (by company ID) → contacts with zi_contact_id
3. Apollo cross-match ZoomInfo contacts (free) → get apollo_id + LinkedIn URL
4. Serper open query for ZoomInfo contacts where Apollo failed → LinkedIn URL
5. Apify profile verification for Serper results → store linkedin_profile_data

Serper audit: logs credits per company to discovery_log table.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

from lib.apollo import ApolloClient
from lib.title_tiers import classify_title_tier, TIER_1_TITLES, TIER_3_TITLES, ALL_TITLES
from lib.serper import serper_search
from lib.apify import run_actor, PROFILE_SCRAPER

logger = logging.getLogger(__name__)

ALL_TARGET_TITLES = TIER_1_TITLES + ["Owner", "President", "CEO", "Founder",
    "Managing Director", "Partner", "Executive Director"] + TIER_3_TITLES

FINANCE_TITLES_ZI = ('CFO OR Controller OR "VP Finance" OR "Director of Finance" '
                     'OR Owner OR President OR CEO OR "Accounting Manager" OR "Finance Manager"')


def _extract_slug(linkedin_url: str) -> str:
    if not linkedin_url or "/in/" not in linkedin_url:
        return ""
    return linkedin_url.split("/in/")[1].rstrip("/").split("?")[0]


def _get_zi_id(company: dict) -> str:
    """Extract ZoomInfo company ID from source_data."""
    sd = company.get("source_data") or {}
    if isinstance(sd, str):
        try:
            sd = json.loads(sd)
        except:
            sd = {}
    return str(sd.get("zi_id", "")).strip()


def discover_contacts_integrated(
    sb, apollo: ApolloClient, company: dict, tenant_id: str,
    zi_token: str = "",
) -> dict:
    """Run full integrated contact discovery for one company.

    Returns dict with:
        contacts: list of all discovered contacts
        serper_credits: number of Serper credits used
        audit: dict with per-step counts for logging
    """
    name = company.get("name", "Unknown")
    domain = company.get("domain", "")
    company_id = company["id"]
    zi_id = _get_zi_id(company)
    campaign_id = "00000000-0000-0000-0000-000000000001"

    all_contacts = []
    seen_keys = set()
    audit = {
        "apollo_contacts": 0,
        "zi_contacts": 0,
        "apollo_crossmatched": 0,
        "serper_searched": 0,
        "serper_found": 0,
        "serper_credits": 0,
        "xray_searched": False,
        "xray_verified": 0,
        "apify_verified": 0,
        "apify_false_positive": 0,
    }

    def _dedup_key(c):
        slug = _extract_slug(c.get("linkedin_url", ""))
        if slug:
            return f"li:{slug}"
        return f"name:{c.get('first_name', '').lower()}-{c.get('last_name', '').lower()}-{name.lower()}"

    # === Step 3: Apollo free people search ===
    apollo_contacts = []
    if domain:
        try:
            result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
                "q_organization_domains_list": [domain],
                "person_titles": ALL_TARGET_TITLES,
                "per_page": 10,
            })
            for p in result.get("people", []):
                contact = {
                    "first_name": p.get("first_name", ""),
                    "last_name": p.get("last_name", ""),
                    "title": p.get("title", ""),
                    "linkedin_url": p.get("linkedin_url", ""),
                    "linkedin_slug": _extract_slug(p.get("linkedin_url", "")),
                    "apollo_id": p.get("id", ""),
                    "zoominfo_id": "",
                    "email": p.get("email", ""),
                    "seniority": p.get("seniority", ""),
                    "headline": p.get("headline", ""),
                    "source": "apollo_search",
                    "_raw": p,
                }
                key = _dedup_key(contact)
                if key not in seen_keys:
                    seen_keys.add(key)
                    apollo_contacts.append(contact)
                    all_contacts.append(contact)
        except Exception as e:
            logger.warning("Apollo search failed for %s: %s", name, e)

    audit["apollo_contacts"] = len(apollo_contacts)

    # === Step 4: ZoomInfo free search by company ID ===
    zi_contacts = []
    if zi_id and zi_token:
        try:
            zi_headers = {"Authorization": f"Bearer {zi_token}", "Content-Type": "application/json"}
            resp = requests.post("https://api.zoominfo.com/search/contact", headers=zi_headers, json={
                "companyId": zi_id,
                "jobTitle": FINANCE_TITLES_ZI,
                "rpp": 10,
            }, timeout=15)
            if resp.status_code == 200:
                for c in resp.json().get("data", []):
                    contact = {
                        "first_name": c.get("firstName", ""),
                        "last_name": c.get("lastName", ""),
                        "title": c.get("jobTitle", ""),
                        "linkedin_url": "",
                        "linkedin_slug": "",
                        "apollo_id": "",
                        "zoominfo_id": str(c.get("id", "")),
                        "email": "",
                        "seniority": "",
                        "headline": "",
                        "source": "import",
                        "_raw": c,
                    }
                    key = _dedup_key(contact)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        zi_contacts.append(contact)
                        all_contacts.append(contact)
        except Exception as e:
            logger.warning("ZoomInfo search failed for %s: %s", name, e)

    audit["zi_contacts"] = len(zi_contacts)

    # === Step 5: Apollo cross-match ZoomInfo contacts ===
    for c in zi_contacts:
        try:
            result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
                "q_keywords": f"{c['first_name']} {c['last_name']} {name}",
                "per_page": 3,
            })
            for p in result.get("people", []):
                if p.get("last_name", "").lower() == c["last_name"].lower():
                    if p.get("linkedin_url"):
                        c["linkedin_url"] = p["linkedin_url"]
                        c["linkedin_slug"] = _extract_slug(p["linkedin_url"])
                    c["apollo_id"] = p.get("id", "")
                    c["email"] = p.get("email", "") or c.get("email", "")
                    audit["apollo_crossmatched"] += 1
                    break
            time.sleep(0.3)
        except:
            pass

    # === Step 6: Serper for ZoomInfo contacts where Apollo failed ===
    serper_key = os.environ.get("SERPER_API_KEY", "")
    needs_serper = [c for c in zi_contacts if not c.get("linkedin_url") and not c.get("apollo_id")]
    serper_urls_to_verify = []

    for c in needs_serper:
        first = c["first_name"]
        last = c["last_name"]
        # Clean certifications
        last_clean = last
        for cert in [" Mba", " Cpa", " Acca", " Acma", " Fca", " MBA", " CPA", "(ACMA)", "(ACCA)"]:
            last_clean = last_clean.replace(cert, "").strip()

        title_short = c["title"].split(",")[0].split("&")[0].strip()
        q = f'{first} {last_clean} | {title_short} at {name} linkedin'

        try:
            r = requests.post("https://google.serper.dev/search", json={
                "q": q, "gl": "us", "num": 5,
            }, headers={"X-API-KEY": serper_key}, timeout=10)
            audit["serper_searched"] += 1
            audit["serper_credits"] += 1

            if r.status_code == 200:
                for item in r.get("organic", []) if hasattr(r, "get") else r.json().get("organic", []):
                    link = item.get("link", "")
                    if "linkedin.com/in/" in link:
                        c["linkedin_url"] = link
                        c["linkedin_slug"] = _extract_slug(link)
                        audit["serper_found"] += 1
                        serper_urls_to_verify.append((c, link))
                        break
        except:
            pass
        time.sleep(0.2)

    # === Step 7: Serper X-ray discovery (if 0 contacts total) ===
    if not all_contacts and (domain or name):
        from lib.xray import xray_discover_finance_contacts
        audit["xray_searched"] = True
        xray_result = xray_discover_finance_contacts(name, domain=domain or None, max_tier=3)
        verified = xray_result.get("verified", [])
        for c in verified:
            contact = {
                "first_name": c.get("first_name", ""),
                "last_name": c.get("last_name", ""),
                "title": c.get("title", ""),
                "linkedin_url": c.get("linkedin_url", ""),
                "linkedin_slug": _extract_slug(c.get("linkedin_url", "")),
                "apollo_id": "",
                "zoominfo_id": "",
                "source": "linkedin_search",
                "verified": True,
            }
            key = _dedup_key(contact)
            if key not in seen_keys:
                seen_keys.add(key)
                all_contacts.append(contact)
                audit["xray_verified"] += 1

    # === Step 8: Apify profile verification for Serper results ===
    if serper_urls_to_verify:
        urls = list(set(url for _, url in serper_urls_to_verify))
        urls = [u.replace("http://", "https://") for u in urls]

        profiles = run_actor(PROFILE_SCRAPER, {"urls": urls}) if urls else []

        profile_by_url = {}
        for p in (profiles or []):
            orig = (p.get("originalQuery") or {}).get("url", "").rstrip("/")
            li_url = (p.get("linkedinUrl") or "").rstrip("/")
            if orig: profile_by_url[orig] = p
            if li_url: profile_by_url[li_url] = p
            slug = li_url.split("/in/")[-1].rstrip("/") if "/in/" in li_url else ""
            if slug: profile_by_url[slug] = p

        for contact, url in serper_urls_to_verify:
            p = profile_by_url.get(url.rstrip("/"))
            if not p:
                slug = url.split("/in/")[-1].rstrip("/")
                p = profile_by_url.get(slug)
            if not p:
                continue

            current_companies = [pos.get("companyName", "") for pos in (p.get("currentPosition") or [])]
            company_lower = name.lower()
            current_match = any(company_lower in co.lower() or co.lower() in company_lower for co in current_companies if co)

            profile_data = {
                "firstName": p.get("firstName"),
                "lastName": p.get("lastName"),
                "headline": p.get("headline"),
                "currentPosition": p.get("currentPosition"),
                "experience": (p.get("experience") or [])[:10],
                "connectionsCount": p.get("connectionsCount"),
                "followerCount": p.get("followerCount"),
                "location": p.get("location"),
                "photo": p.get("photo"),
                "skills": [s.get("name") for s in (p.get("skills") or [])[:10]],
                "about": (p.get("about") or "")[:500],
                "verified": current_match,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

            contact["linkedin_profile_data"] = profile_data
            contact["headline"] = p.get("headline", "")
            contact["linkedin_connections"] = p.get("connectionsCount")
            contact["linkedin_followers"] = p.get("followerCount")

            if current_match:
                contact["role_verified"] = True
                audit["apify_verified"] += 1
            else:
                # False positive — clear URL
                contact["linkedin_url"] = ""
                contact["linkedin_slug"] = ""
                contact["role_verified"] = False
                profile_data["false_positive"] = True
                audit["apify_false_positive"] += 1

    # === Store contacts to DB ===
    stubs_created = 0
    for c in all_contacts:
        slug = c.get("linkedin_slug", "")

        # Dedup check in DB
        if slug:
            existing = sb.table("prospects").select("id").eq("tenant_id", tenant_id).eq("linkedin_slug", slug).limit(1).execute()
            if existing.data:
                continue

        source_enum = "apollo_search" if c.get("apollo_id") else "linkedin_search" if c.get("source") == "linkedin_search" else "import"

        record = {
            "tenant_id": tenant_id,
            "campaign_id": campaign_id,
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "title": c.get("title", ""),
            "headline": c.get("headline", ""),
            "seniority": c.get("seniority", ""),
            "email": c.get("email") or None,
            "linkedin_url": c.get("linkedin_url") or None,
            "linkedin_slug": slug or None,
            "apollo_person_id": c.get("apollo_id") or None,
            "zoominfo_contact_id": c.get("zoominfo_id") or None,
            "company_name": name,
            "company_domain": domain or None,
            "company_linkedin_url": company.get("linkedin_url", ""),
            "icp_score": company.get("icp_score"),
            "status": "sourced",
            "source": source_enum,
            "data_source": c.get("source", ""),
            "linkedin_profile_data": c.get("linkedin_profile_data"),
            "linkedin_connections": c.get("linkedin_connections"),
            "linkedin_followers": c.get("linkedin_followers"),
            "role_verified": c.get("role_verified"),
        }

        try:
            sb.table("prospects").insert(record).execute()
            stubs_created += 1
        except:
            pass

    audit["stubs_created"] = stubs_created

    # === Log to discovery_log ===
    try:
        sb.table("discovery_log").insert({
            "tenant_id": tenant_id,
            "company_id": company_id,
            "company_name": name,
            "source": "integrated_discovery",
            "actor_or_endpoint": "apollo+zoominfo+serper+apify",
            "contacts_found": len(all_contacts),
            "contacts_verified": audit["apify_verified"],
            "contacts_rejected": audit["apify_false_positive"],
            "duration_ms": 0,
            "error_message": None,
            "request_params": {
                "apollo_contacts": audit["apollo_contacts"],
                "zi_contacts": audit["zi_contacts"],
                "zi_id": zi_id,
                "apollo_crossmatched": audit["apollo_crossmatched"],
                "serper_searched": audit["serper_searched"],
                "serper_found": audit["serper_found"],
                "serper_credits": audit["serper_credits"],
                "xray_searched": audit["xray_searched"],
                "xray_verified": audit["xray_verified"],
                "stubs_created": stubs_created,
            },
        }).execute()
    except Exception as e:
        logger.warning("Failed to log discovery for %s: %s", name, e)

    return {
        "contacts": all_contacts,
        "stubs_created": stubs_created,
        "audit": audit,
    }
