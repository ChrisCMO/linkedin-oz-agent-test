#!/usr/bin/env python3
"""ICP 1 Company-First Pipeline: Google Places → company list for PNW."""

import os, sys, csv, time, json, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger()

GOOGLE_API_KEY = "AIzaSyDBFl9GysZkM42uPS1wdKh8tTKeedWP67o"
HEADERS = {
    "X-Goog-Api-Key": GOOGLE_API_KEY,
    "X-Goog-FieldMask": (
        "places.displayName,places.formattedAddress,places.id,places.rating,"
        "places.userRatingCount,places.businessStatus,places.location,"
        "places.nationalPhoneNumber,places.websiteUri,places.types,"
        "places.primaryType,places.primaryTypeDisplayName,"
        "nextPageToken"
    ),
    "Content-Type": "application/json",
}

# ICP 1 industries
INDUSTRIES = [
    "construction companies",
    "general contractors",
    "commercial construction",
    "manufacturing companies",
    "industrial manufacturing",
    "commercial real estate companies",
    "real estate investment firms",
    "professional services firms",
    "engineering firms",
    "accounting firms",       # potential referral partners or comparison
    "hospitality companies",
    "hotel management companies",
    "nonprofit organizations",
    "foundations",
]

# PNW cities — priority order per ICP spec
CITIES = [
    # Primary: Seattle metro
    ("Seattle", "Washington"),
    ("Bellevue", "Washington"),
    ("Tacoma", "Washington"),
    ("Redmond", "Washington"),
    ("Kirkland", "Washington"),
    ("Everett", "Washington"),
    ("Renton", "Washington"),
    ("Kent", "Washington"),
    ("Federal Way", "Washington"),
    ("Olympia", "Washington"),
    # Greater WA
    ("Spokane", "Washington"),
    ("Vancouver", "Washington"),
    ("Yakima", "Washington"),
    ("Bellingham", "Washington"),
    ("Tri-Cities", "Washington"),
    # Oregon
    ("Portland", "Oregon"),
    ("Salem", "Oregon"),
    ("Eugene", "Oregon"),
    ("Bend", "Oregon"),
    ("Medford", "Oregon"),
    ("Beaverton", "Oregon"),
    ("Hillsboro", "Oregon"),
    ("Corvallis", "Oregon"),
]


def search_places(query, page_token=None):
    """Search Google Places API with retry. Returns (places, next_page_token)."""
    body = {"textQuery": query}
    if page_token:
        body["pageToken"] = page_token

    for attempt in range(3):
        try:
            resp = requests.post(
                "https://places.googleapis.com/v1/places:searchText",
                headers=HEADERS,
                json=body,
                timeout=30,
            )

            if resp.status_code == 429:
                log.warning(f"  Rate limited. Waiting 10s...")
                time.sleep(10)
                continue

            if resp.status_code != 200:
                log.warning(f"  API error {resp.status_code}: {resp.text[:100]}")
                return [], None

            data = resp.json()
            places = data.get("places", [])
            next_token = data.get("nextPageToken")
            return places, next_token

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            log.warning(f"  Connection error (attempt {attempt+1}/3): {str(e)[:80]}")
            time.sleep(5 * (attempt + 1))

    return [], None


def _save_progress(all_companies):
    """Save current progress to CSV."""
    os.makedirs("output", exist_ok=True)
    outfile = "output/icp1_companies_google_places.csv"
    sorted_companies = sorted(
        all_companies.values(),
        key=lambda c: (c["state"], c["search_city"], c["company_name"]),
    )
    fields = [
        "company_name", "address", "city", "state", "search_city",
        "phone", "website", "domain", "business_status",
        "rating", "review_count", "primary_type", "google_types",
        "industry_search", "google_place_id", "latitude", "longitude",
        "source",
    ]
    with open(outfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted_companies)


def extract_domain(website_url):
    """Extract domain from URL."""
    if not website_url:
        return ""
    domain = website_url.replace("https://", "").replace("http://", "").replace("www.", "")
    domain = domain.split("/")[0].split("?")[0]
    return domain.lower()


def main():
    all_companies = {}  # key: (name_lower, city_lower) → dedup

    total_queries = len(INDUSTRIES) * len(CITIES)
    query_count = 0

    log.info(f"Starting company-first search: {len(INDUSTRIES)} industries × {len(CITIES)} cities = {total_queries} queries")
    log.info("=" * 60)

    for city, state in CITIES:
        city_count = 0
        for industry in INDUSTRIES:
            query = f"{industry} in {city} {state}"
            query_count += 1

            places, next_token = search_places(query)

            # Get up to 3 pages per query (60 results max)
            all_pages = list(places)
            pages_fetched = 1
            while next_token and pages_fetched < 3:
                time.sleep(1)  # Required delay between page token requests
                more_places, next_token = search_places(query, page_token=next_token)
                all_pages.extend(more_places)
                pages_fetched += 1

            for p in all_pages:
                name = p.get("displayName", {}).get("text", "")
                address = p.get("formattedAddress", "")
                phone = p.get("nationalPhoneNumber", "")
                website = p.get("websiteUri", "")
                status = p.get("businessStatus", "")
                rating = p.get("rating", "")
                reviews = p.get("userRatingCount", 0)
                place_id = p.get("id", "")
                loc = p.get("location", {})
                lat = loc.get("latitude", "")
                lng = loc.get("longitude", "")
                primary_type = p.get("primaryTypeDisplayName", {}).get("text", "")
                types = p.get("types", [])

                domain = extract_domain(website)

                # Dedup key: company name + city (case insensitive)
                # Extract city from address
                addr_city = ""
                if address:
                    parts = address.split(",")
                    if len(parts) >= 2:
                        addr_city = parts[-3].strip() if len(parts) >= 3 else parts[0].strip()

                dedup_key = (name.lower().strip(), domain or addr_city.lower())

                if dedup_key not in all_companies:
                    all_companies[dedup_key] = {
                        "company_name": name,
                        "address": address,
                        "city": addr_city,
                        "state": state,
                        "search_city": city,
                        "phone": phone,
                        "website": website,
                        "domain": domain,
                        "business_status": status,
                        "rating": rating,
                        "review_count": reviews,
                        "google_place_id": place_id,
                        "latitude": lat,
                        "longitude": lng,
                        "primary_type": primary_type,
                        "google_types": ", ".join(types[:5]),
                        "industry_search": industry,
                        "source": "Google Places",
                    }
                    city_count += 1

            time.sleep(1)  # Rate limit between queries

        log.info(f"  {city}, {state}: {city_count} unique companies (query {query_count}/{total_queries})")

        # Save progress after each city
        _save_progress(all_companies)

    log.info("")
    log.info("=" * 60)
    log.info(f"TOTAL UNIQUE COMPANIES: {len(all_companies)}")
    log.info("=" * 60)

    # Export CSV
    os.makedirs("output", exist_ok=True)
    outfile = "output/icp1_companies_google_places.csv"

    # Sort by state, then search_city priority, then name
    sorted_companies = sorted(
        all_companies.values(),
        key=lambda c: (c["state"], c["search_city"], c["company_name"]),
    )

    with open(outfile, "w", newline="") as f:
        fields = [
            "company_name", "address", "city", "state", "search_city",
            "phone", "website", "domain", "business_status",
            "rating", "review_count", "primary_type", "google_types",
            "industry_search", "google_place_id", "latitude", "longitude",
            "source",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted_companies)

    log.info(f"Exported to: {outfile}")

    # Summary by state and city
    from collections import Counter
    state_counts = Counter(c["state"] for c in sorted_companies)
    city_counts = Counter(c["search_city"] for c in sorted_companies)
    industry_counts = Counter(c["industry_search"] for c in sorted_companies)

    log.info("\nBy state:")
    for s, count in state_counts.most_common():
        log.info(f"  {s}: {count}")

    log.info("\nTop cities:")
    for c, count in city_counts.most_common(10):
        log.info(f"  {c}: {count}")

    log.info("\nBy industry search:")
    for ind, count in industry_counts.most_common():
        log.info(f"  {ind}: {count}")


if __name__ == "__main__":
    main()
