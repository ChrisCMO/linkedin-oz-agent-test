"""
Extract contacts from model_clients_digital_footprint.csv into
one-row-per-prospect format matching seattle_mfg_pipeline_v2.csv columns.

Sources per company row:
  - "Top Contact" fields (full detail: name, title, LinkedIn, email, seniority, city)
  - "ZoomInfo Contact Names" (semicolon-separated "Name (Title)" entries)
  - "Apollo Contact Names" (semicolon-separated, partially redacted)
"""

import csv
import re
import os

INPUT = os.path.join(os.path.dirname(__file__), "..",
    "docs/ICP-Prospects/model_clients_digital_footprint.csv")
OUTPUT = os.path.join(os.path.dirname(__file__), "..",
    "docs/ICP-Prospects/model_clients_digital_footprint_by_prospect.csv")

HEADERS = [
    "ICP Score", "First Name", "Last Name", "Title", "Company", "Industry",
    "Employees", "Revenue", "Company City", "Company Domain", "Email",
    "Email Status", "LinkedIn URL", "Seniority", "Headline",
    "LinkedIn Headline", "LinkedIn Connections", "LinkedIn Followers",
    "Open to Work", "LinkedIn Current Company", "Role Verified",
    "Company LinkedIn URL", "Company LI Followers", "Company LI Employees",
    "Company LI Tagline", "Company LI Description", "Company LI Founded",
    "Company LI Has Logo", "Activity Level", "Recent Post Date",
    "Recent Post Text", "Posts Count", "Reposts Count", "Total Feed Items",
    "ICP Reasoning", "ICP Score Breakdown",
    "Melinda's Connection Note", "Adrienne's Connection Note",
    "Message 1 (after connect)", "Message 2 (2 weeks)", "Message 3 (4 weeks)",
    "Google Places Address", "Google Phone", "Google Rating", "Google Reviews",
    "Apollo ID", "Data Source Pipeline"
]


def parse_name_title(entry: str):
    """Parse 'First Last (Title)' or 'First La***t (Title)' into (first, last, title)."""
    entry = entry.strip()
    m = re.match(r'^(.+?)\s*\((.+)\)$', entry)
    if m:
        full_name, title = m.group(1).strip(), m.group(2).strip()
    else:
        full_name, title = entry, ""

    parts = full_name.split(None, 1)
    first = parts[0] if parts else full_name
    last = parts[1] if len(parts) > 1 else ""
    return first, last, title


def split_contacts(field_value: str):
    """Split semicolon-separated contact strings."""
    if not field_value or not field_value.strip():
        return []
    return [c.strip() for c in field_value.split(";") if c.strip()]


def make_prospect_row(first, last, title, source, company_data, top_contact_data=None):
    """Build a row dict matching the target headers."""
    row = {h: "" for h in HEADERS}

    # Company-level fields
    row["Company"] = company_data.get("Company", "")
    row["Industry"] = company_data.get("Industry", "")
    row["Employees"] = company_data.get("Apollo Employees", "")
    row["Revenue"] = company_data.get("Apollo Revenue", "")
    row["Company City"] = company_data.get("Apollo City", "")
    row["Company Domain"] = company_data.get("Company Website", "")
    row["Company LinkedIn URL"] = company_data.get("Company LinkedIn URL", "")
    row["Company LI Followers"] = company_data.get("Company LI Followers", "")
    row["Company LI Employees"] = company_data.get("Company LI Employee Count", "")
    row["Company LI Tagline"] = company_data.get("Company LI Tagline", "")
    row["Company LI Description"] = company_data.get("Company LI Description", "")
    row["Company LI Founded"] = company_data.get("Company LI Founded", "")
    row["Company LI Has Logo"] = company_data.get("Company LI Has Logo", "")
    row["Google Places Address"] = company_data.get("GP Address", "")
    row["Google Phone"] = company_data.get("GP Phone", "")
    row["Google Rating"] = company_data.get("GP Rating", "")
    row["Google Reviews"] = company_data.get("GP Reviews", "")
    row["Data Source Pipeline"] = f"Model Client Digital Footprint ({source})"
    row["ICP Score"] = company_data.get("Data Completeness Score", "")
    row["ICP Reasoning"] = company_data.get("Chad Notes", "")

    # Person-level fields
    row["First Name"] = first
    row["Last Name"] = last
    row["Title"] = title
    row["Headline"] = title

    # If this is the top contact, fill in the rich fields
    if top_contact_data:
        row["LinkedIn URL"] = top_contact_data.get("Top Contact LinkedIn", "")
        row["Email"] = top_contact_data.get("Top Contact Email", "")
        row["Email Status"] = top_contact_data.get("Top Contact Email Status", "")
        row["Seniority"] = top_contact_data.get("Top Contact Seniority", "")
        row["Company City"] = top_contact_data.get("Top Contact City", "") or row["Company City"]
        row["Activity Level"] = company_data.get("LinkedIn Activity Level", "")
        row["Recent Post Date"] = company_data.get("LinkedIn Last Activity Date", "")
        row["Recent Post Text"] = company_data.get("LinkedIn Last Activity Detail", "")
        row["Posts Count"] = company_data.get("LinkedIn Posts Found", "")
        row["Reposts Count"] = company_data.get("LinkedIn Reposts Found", "")
        row["Total Feed Items"] = company_data.get("LinkedIn Total Activity", "")

    return row


def main():
    with open(INPUT, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = [r for r in reader if r.get("Company", "").strip()]

    all_prospects = []

    for co in companies:
        seen_names = set()  # deduplicate across sources

        # 1) Top Contact (richest data)
        top_name = co.get("Top Contact Name", "").strip()
        if top_name:
            parts = top_name.split(None, 1)
            first = parts[0]
            last = parts[1] if len(parts) > 1 else ""
            title = co.get("Top Contact Title", "")
            seen_names.add(top_name.lower())
            all_prospects.append(
                make_prospect_row(first, last, title, "Top Contact", co, top_contact_data=co)
            )

        # 2) ZoomInfo contacts
        for entry in split_contacts(co.get("ZoomInfo Contact Names", "")):
            first, last, title = parse_name_title(entry)
            key = f"{first} {last}".lower().strip()
            if key not in seen_names:
                seen_names.add(key)
                all_prospects.append(
                    make_prospect_row(first, last, title, "ZoomInfo", co)
                )

        # 3) Apollo contacts (may be partially redacted with ***)
        for entry in split_contacts(co.get("Apollo Contact Names", "")):
            first, last, title = parse_name_title(entry)
            # Skip if redacted name matches a seen name's prefix
            key = f"{first} {last}".lower().strip()
            is_redacted = "***" in f"{first} {last}"

            if is_redacted:
                # Check if any seen name starts with the non-redacted prefix
                prefix = key.split("*")[0].strip()
                if any(s.startswith(prefix) for s in seen_names):
                    continue

            if key not in seen_names:
                seen_names.add(key)
                all_prospects.append(
                    make_prospect_row(first, last, title, "Apollo", co)
                )

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(all_prospects)

    print(f"Wrote {len(all_prospects)} prospect rows to:\n  {OUTPUT}")

    # Summary
    companies_seen = set(r["Company"] for r in all_prospects)
    for co in sorted(companies_seen):
        count = sum(1 for r in all_prospects if r["Company"] == co)
        print(f"  {co}: {count} contacts")


if __name__ == "__main__":
    main()
