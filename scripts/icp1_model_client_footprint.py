#!/usr/bin/env python3
"""Digital footprint scan for 4 VWC model clients — what Chad actually asked for."""

import sys, os, time, json, csv
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests
import logging
from datetime import datetime, timedelta
from lib.apollo import ApolloClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

apollo = ApolloClient()
APIFY_TOKEN = os.environ["APIFY_API_KEY"]
POSTS_ACTOR = "A3cAPGpwBEG8RJwse"
COMMENTS_ACTOR = "FiHYLewnJwS6GnRpo"

GP_HEADERS = {
    "X-Goog-Api-Key": "AIzaSyDBFl9GysZkM42uPS1wdKh8tTKeedWP67o",
    "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.businessStatus",
    "Content-Type": "application/json",
}

# ZoomInfo auth
zi_resp = requests.post("https://api.zoominfo.com/authenticate", json={
    "username": os.environ["ZOOMINFO_USERNAME"],
    "password": os.environ["ZOOMINFO_PASSWORD"],
})
zi_jwt = zi_resp.json()["jwt"]
zi_headers = {"Authorization": f"Bearer {zi_jwt}", "Content-Type": "application/json"}


def run_actor(actor_id, payload):
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    try:
        r = requests.post(f"https://api.apify.com/v2/acts/{actor_id}/runs", headers=headers, json=payload, timeout=30)
        if r.status_code != 201:
            return []
        run_id = r.json()["data"]["id"]
        dataset_id = r.json()["data"]["defaultDatasetId"]
        for _ in range(24):
            time.sleep(5)
            sr = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}", headers=headers, timeout=15)
            if sr.json()["data"]["status"] in ("SUCCEEDED", "FAILED", "ABORTED"):
                break
        return requests.get(f"https://api.apify.com/v2/datasets/{dataset_id}/items", headers=headers, timeout=15).json()
    except Exception as e:
        log.warning(f"  Apify error: {e}")
        return []


def parse_date(ds):
    if not ds:
        return None
    try:
        return datetime.fromisoformat(ds.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


MODEL_CLIENTS = [
    {
        "name": "Formost Fuji Corporation",
        "domain": "formostfuji.com",
        "industry": "Manufacturing",
        "icp": "Audit & Tax",
        "notes": "Long-tenured client. ~100 employees. Ownership/leadership transition.",
        "zi_search": "Formost Fuji",
        "gp_search": "Formost Fuji Corporation",
    },
    {
        "name": "Shannon & Wilson",
        "domain": "shannonwilson.com",
        "industry": "Professional services (engineering)",
        "icp": "Audit & Tax + Benefit Plan",
        "notes": "ESOP. Top 5 VWC client. 2 benefit plan audits + tax + review.",
        "zi_search": "Shannon Wilson",
        "gp_search": "Shannon Wilson engineering Seattle",
    },
    {
        "name": "Skills Inc.",
        "domain": "skillsinc.com",
        "industry": "Nonprofit / Aerospace manufacturing",
        "icp": "Audit & Tax + Benefit Plan",
        "notes": "Nonprofit like for-profit. Boeing airplane parts.",
        "zi_search": "Skills Inc",
        "gp_search": "Skills Inc Auburn Washington",
    },
    {
        "name": "Carillon Properties",
        "domain": "carillonpoint.com",
        "industry": "Commercial real estate / Hospitality",
        "icp": "Audit & Tax",
        "notes": "CRE + hotel. Old family money. Previously Deloitte.",
        "zi_search": "Carillon Properties",
        "gp_search": "Carillon Properties Kirkland Washington",
    },
]

FINANCE_TITLES_ZI = "CFO OR Chief Financial Officer OR Controller OR VP Finance OR Director of Finance OR Treasurer OR President OR Owner"
FINANCE_TITLES_APOLLO = [
    "CFO", "Chief Financial Officer", "Controller", "VP Finance",
    "Director of Finance", "President", "Owner", "CEO", "Executive Director", "Treasurer",
]

report = []

for mc in MODEL_CLIENTS:
    log.info(f"\n{'=' * 60}")
    log.info(f"SCANNING: {mc['name']}")
    log.info(f"{'=' * 60}")

    row = {
        "Company": mc["name"],
        "Industry": mc["industry"],
        "ICP Applicable": mc["icp"],
        "Chad Notes": mc["notes"],
    }

    # 1. Google Places
    log.info("  Google Places...")
    gp_resp = requests.post(
        "https://places.googleapis.com/v1/places:searchText",
        headers=GP_HEADERS,
        json={"textQuery": mc["gp_search"]},
    )
    gp_places = gp_resp.json().get("places", [])
    if gp_places:
        gp = gp_places[0]
        row["GP Found"] = "Yes"
        row["GP Address"] = gp.get("formattedAddress", "")
        row["GP Phone"] = gp.get("nationalPhoneNumber", "")
        row["GP Website"] = gp.get("websiteUri", "")
        row["GP Rating"] = str(gp.get("rating", ""))
        row["GP Reviews"] = str(gp.get("userRatingCount", ""))
        row["GP Status"] = gp.get("businessStatus", "")
        log.info(f"    Found: {gp.get('displayName', {}).get('text')} | {row['GP Address']}")
    else:
        row.update({"GP Found": "No", "GP Address": "", "GP Phone": "", "GP Website": "", "GP Rating": "", "GP Reviews": "", "GP Status": ""})
        log.info("    Not found")

    # 2. Apollo Org Enrichment
    log.info("  Apollo Org Enrichment...")
    org_result = apollo._request("POST", "/api/v1/organizations/enrich", json_body={"domain": mc["domain"]})
    org = org_result.get("organization", {})
    has_org = bool(org.get("name"))
    row["Apollo Org Found"] = "Yes" if has_org else "No"
    row["Apollo Company Name"] = org.get("name", "")
    row["Apollo Industry"] = org.get("industry", "")
    row["Apollo Employees"] = str(org.get("estimated_num_employees", ""))
    rev = org.get("annual_revenue")
    row["Apollo Revenue"] = f"${rev / 1e6:.0f}M" if rev and rev >= 1e6 else (str(rev) if rev else "")
    row["Apollo Founded"] = str(org.get("founded_year", ""))
    row["Apollo City"] = f"{org.get('city', '')}, {org.get('state', '')}".strip(", ")
    row["Company LinkedIn URL"] = org.get("linkedin_url", "")
    row["Company Website"] = org.get("website_url", "")
    log.info(f"    {row['Apollo Company Name']} | {row['Apollo Industry']} | emp={row['Apollo Employees']} | rev={row['Apollo Revenue']} | LinkedIn: {row['Company LinkedIn URL']}")

    # 3. ZoomInfo Contacts
    log.info("  ZoomInfo Contacts...")
    zi_resp2 = requests.post("https://api.zoominfo.com/search/contact", headers=zi_headers, json={
        "companyName": mc["zi_search"],
        "jobTitle": FINANCE_TITLES_ZI,
        "rpp": 10,
    })
    zi_contacts = zi_resp2.json().get("data", [])
    row["ZoomInfo Finance Contacts Found"] = str(len(zi_contacts))
    zi_names = [f"{c.get('firstName', '')} {c.get('lastName', '')} ({c.get('jobTitle', '')})" for c in zi_contacts[:5]]
    row["ZoomInfo Contact Names"] = "; ".join(zi_names)
    log.info(f"    {len(zi_contacts)} contacts")
    time.sleep(0.5)

    # 4. Apollo People Search (by domain)
    log.info("  Apollo People Search...")
    ap_result = apollo._request("POST", "/api/v1/mixed_people/api_search", json_body={
        "q_organization_domains_list": [mc["domain"]],
        "person_titles": FINANCE_TITLES_APOLLO,
        "person_seniorities": ["c_suite", "vp", "director", "owner"],
        "per_page": 10,
    })
    ap_contacts = ap_result.get("people", [])
    row["Apollo Finance Contacts Found"] = str(len(ap_contacts))
    ap_names = [f"{c.get('first_name', '')} {c.get('last_name_obfuscated', '')} ({c.get('title', '')})" for c in ap_contacts[:5]]
    row["Apollo Contact Names"] = "; ".join(ap_names)
    log.info(f"    {len(ap_contacts)} contacts")

    # 5. Enrich top contact
    log.info("  Enriching top contact...")
    if ap_contacts:
        enriched = apollo.enrich_person(ap_contacts[0]["id"])
        person = enriched.get("person")
        if person:
            e = apollo._extract_person(person)
            row["Top Contact Name"] = e.get("name", "")
            row["Top Contact Title"] = e.get("title", "")
            row["Top Contact LinkedIn"] = e.get("linkedin_url", "")
            row["Top Contact Email"] = e.get("email", "")
            row["Top Contact Email Status"] = (e.get("raw_person") or {}).get("email_status", "")
            row["Top Contact Seniority"] = e.get("seniority", "")
            row["Top Contact City"] = f"{e.get('city', '')}, {e.get('state', '')}".strip(", ")
            log.info(f"    {row['Top Contact Name']} | {row['Top Contact Title']} | {row['Top Contact LinkedIn']}")
        else:
            row.update({"Top Contact Name": "", "Top Contact Title": "", "Top Contact LinkedIn": "", "Top Contact Email": "", "Top Contact Email Status": "", "Top Contact Seniority": "", "Top Contact City": ""})
    else:
        row.update({"Top Contact Name": "", "Top Contact Title": "", "Top Contact LinkedIn": "", "Top Contact Email": "", "Top Contact Email Status": "", "Top Contact Seniority": "", "Top Contact City": ""})

    # 6. LinkedIn Activity
    li_url = row.get("Top Contact LinkedIn", "")
    if li_url:
        slug = li_url.rstrip("/").split("/")[-1]
        log.info(f"  LinkedIn Activity ({slug})...")
        posts_items = run_actor(POSTS_ACTOR, {"usernames": [li_url], "limit": 20})
        comments_items = run_actor(COMMENTS_ACTOR, {"maxItems": 20, "profiles": [li_url]})

        now = datetime.now()
        activities = []
        slug_lower = slug.lower()

        for item in posts_items:
            target = (item.get("query") or {}).get("targetUrl", "")
            if slug_lower not in target.lower():
                continue
            author_id = (item.get("author", {}).get("publicIdentifier") or "").lower()
            rb = item.get("repostedBy")
            content = (item.get("content") or "")[:150]
            pd = parse_date((item.get("postedAt") or {}).get("date", ""))
            rd = parse_date((item.get("repostedAt") or {}).get("date", "")) if item.get("repostedAt") else None
            if rb and slug_lower in (rb.get("publicIdentifier") or "").lower():
                activities.append({"date": rd or pd, "type": "Repost", "detail": content[:80]})
            elif slug_lower in author_id:
                activities.append({"date": pd, "type": "Post", "detail": content[:80]})

        for item in comments_items:
            cd = parse_date(item.get("createdAt", ""))
            if cd:
                activities.append({"date": cd, "type": "Comment", "detail": (item.get("commentary") or "")[:80]})

        activities.sort(key=lambda x: x["date"] or datetime.min, reverse=True)

        row["LinkedIn Posts Found"] = str(sum(1 for a in activities if a["type"] == "Post"))
        row["LinkedIn Reposts Found"] = str(sum(1 for a in activities if a["type"] == "Repost"))
        row["LinkedIn Comments Found"] = str(sum(1 for a in activities if a["type"] == "Comment"))
        row["LinkedIn Total Activity"] = str(len(activities))

        if activities:
            latest = activities[0]
            ld = latest["date"]
            row["LinkedIn Last Activity Date"] = ld.strftime("%Y-%m-%d") if ld else ""
            row["LinkedIn Last Activity Type"] = latest["type"]
            row["LinkedIn Last Activity Detail"] = latest["detail"]
            thirty_d = now - timedelta(days=30)
            ninety_d = now - timedelta(days=90)
            if ld and ld >= thirty_d:
                row["LinkedIn Activity Level"] = "Active (< 30 days)"
            elif ld and ld >= ninety_d:
                row["LinkedIn Activity Level"] = "Moderate (< 90 days)"
            else:
                days = (now - ld).days if ld else 999
                row["LinkedIn Activity Level"] = f"Inactive (last: {ld.strftime('%Y-%m-%d') if ld else '?'}, {days}d ago)"
        else:
            row.update({"LinkedIn Last Activity Date": "", "LinkedIn Last Activity Type": "", "LinkedIn Last Activity Detail": "", "LinkedIn Activity Level": "No activity detected"})

        log.info(f"    {row['LinkedIn Activity Level']} | Posts: {row['LinkedIn Posts Found']} | Comments: {row['LinkedIn Comments Found']}")
    else:
        row.update({
            "LinkedIn Posts Found": "0", "LinkedIn Reposts Found": "0", "LinkedIn Comments Found": "0",
            "LinkedIn Total Activity": "0", "LinkedIn Last Activity Date": "", "LinkedIn Last Activity Type": "",
            "LinkedIn Last Activity Detail": "", "LinkedIn Activity Level": "No LinkedIn URL",
        })

    # 7. Data Completeness Score
    completeness = 0
    if row.get("GP Found") == "Yes":
        completeness += 1
    if row.get("Apollo Org Found") == "Yes":
        completeness += 1
    if row.get("Company LinkedIn URL"):
        completeness += 1
    if row.get("Apollo Employees") and row["Apollo Employees"] != "None":
        completeness += 1
    if row.get("Apollo Revenue") and row["Apollo Revenue"] not in ("", "None"):
        completeness += 1
    if row.get("Top Contact Name"):
        completeness += 1
    if row.get("Top Contact LinkedIn"):
        completeness += 1
    if row.get("Top Contact Email"):
        completeness += 1
    if int(row.get("LinkedIn Total Activity", "0") or "0") > 0:
        completeness += 1
    if int(row.get("ZoomInfo Finance Contacts Found", "0") or "0") >= 2:
        completeness += 1

    row["Data Completeness Score"] = f"{completeness}/10"

    # Summary
    data_parts = []
    if row.get("GP Found") == "Yes":
        data_parts.append("Google Places (address, phone)")
    if row.get("Apollo Org Found") == "Yes":
        data_parts.append(f"Apollo Org ({row['Apollo Employees']} emp, {row['Apollo Revenue']} rev, LinkedIn page)")
    if int(row.get("ZoomInfo Finance Contacts Found", "0") or "0") > 0:
        data_parts.append(f"ZoomInfo ({row['ZoomInfo Finance Contacts Found']} finance contacts)")
    if int(row.get("Apollo Finance Contacts Found", "0") or "0") > 0:
        data_parts.append(f"Apollo ({row['Apollo Finance Contacts Found']} finance contacts)")
    if row.get("Top Contact LinkedIn"):
        data_parts.append("Contact LinkedIn profile")
    if row.get("Top Contact Email"):
        data_parts.append("Contact email")

    row["Data Availability Summary"] = " | ".join(data_parts) if data_parts else "Very limited data"

    report.append(row)
    log.info(f"  Completeness: {row['Data Completeness Score']}")

# Export
outfile = "docs/ICP-Prospects/model_clients_digital_footprint.csv"
os.makedirs(os.path.dirname(outfile), exist_ok=True)

headers = [
    "Company", "Industry", "ICP Applicable", "Chad Notes", "Data Completeness Score",
    "GP Found", "GP Address", "GP Phone", "GP Website", "GP Rating", "GP Reviews", "GP Status",
    "Apollo Org Found", "Apollo Company Name", "Apollo Industry", "Apollo Employees", "Apollo Revenue",
    "Apollo Founded", "Apollo City", "Company LinkedIn URL", "Company Website",
    "ZoomInfo Finance Contacts Found", "ZoomInfo Contact Names",
    "Apollo Finance Contacts Found", "Apollo Contact Names",
    "Top Contact Name", "Top Contact Title", "Top Contact LinkedIn", "Top Contact Email",
    "Top Contact Email Status", "Top Contact Seniority", "Top Contact City",
    "LinkedIn Activity Level", "LinkedIn Posts Found", "LinkedIn Reposts Found",
    "LinkedIn Comments Found", "LinkedIn Total Activity", "LinkedIn Last Activity Date",
    "LinkedIn Last Activity Type", "LinkedIn Last Activity Detail",
    "Data Availability Summary",
]

with open(outfile, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(report)

log.info(f"\n{'=' * 60}")
log.info(f"EXPORTED: {outfile}")
log.info(f"{'=' * 60}")
for r in report:
    log.info(f"  {r['Company']}: {r['Data Completeness Score']} | {r.get('Data Availability Summary', '')[:80]}")
