"""
Compile Google X-ray LinkedIn search results (Batch 1 - Seattle only) into CSV.
Source: WebSearch site:linkedin.com/company queries.
"""

import csv
import os
from collections import OrderedDict

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "docs", "TODO", "xray_batch1_seattle_linkedin.csv")

# Each entry: (Company Name, LinkedIn URL, Search Query / Industry)
results = [
    # === MANUFACTURING ===
    ("SMC - Seattle Manufacturing Corporation", "https://www.linkedin.com/company/smc-seattle-manufacturing-corporation", "Manufacturing"),
    ("Weyerhaeuser", "https://www.linkedin.com/company/weyerhaeuser", "Manufacturing"),
    ("TASC - Technical & Assembly Services Corporation", "https://www.linkedin.com/company/tasc-technical-&-assembly-services-corporation", "Manufacturing"),
    ("Darigold", "https://www.linkedin.com/company/darigold", "Manufacturing"),
    ("Kocer Consulting + Engineering PLLC", "https://www.linkedin.com/company/kocer-consulting-and-engineering", "Manufacturing"),
    ("The Gear Works Seattle Inc.", "https://www.linkedin.com/company/the-gear-works-seattle-inc.", "Manufacturing"),
    ("Machinists Inc.", "https://www.linkedin.com/company/machinistsinc", "Manufacturing"),
    ("Directed Machines", "https://www.linkedin.com/company/directed-machines", "Manufacturing"),
    ("Synesso", "https://www.linkedin.com/company/synesso", "Manufacturing"),
    ("Bardahl Manufacturing Corp", "https://www.linkedin.com/company/bardahl-mfg-corp", "Manufacturing"),

    # === AEROSPACE ===
    ("Seattle Aviation Solutions", "https://www.linkedin.com/company/seattle-aviation", "Aerospace"),
    ("Aerospace Development LLC", "https://www.linkedin.com/company/aerospace-development-llc", "Aerospace"),
    ("TLG Aerospace", "https://www.linkedin.com/company/tlg-aerospace-llc", "Aerospace"),
    ("Spaceflight Inc.", "https://www.linkedin.com/company/spaceflightinc", "Aerospace"),
    ("AeroTEC", "https://www.linkedin.com/company/aerospace-testing-engineering-&-certification-l-l-c-", "Aerospace"),
    ("Pacific Northwest Aerospace Alliance", "https://www.linkedin.com/company/pnaa", "Aerospace"),
    ("Aero Northwest", "https://www.linkedin.com/company/aero-northwest", "Aerospace"),
    ("Orion Aerospace", "https://www.linkedin.com/company/orion-aerospace", "Aerospace"),

    # === INDUSTRIAL ===
    ("Capital Industries Inc.", "https://www.linkedin.com/company/capital-industries-inc-", "Industrial / Manufacturing"),
    ("Seattle Art and Industrial", "https://www.linkedin.com/company/art-and-industrial", "Industrial / Manufacturing"),
    ("Seattle Tacoma Box Company", "https://www.linkedin.com/company/seattle-tacoma-box-company", "Industrial / Manufacturing"),

    # === FABRICATION ===
    ("LightArt", "https://www.linkedin.com/company/lightart", "Fabrication / Manufacturing"),
    ("Specialty Metals Corporation", "https://www.linkedin.com/company/specialty-metals-corporation", "Fabrication / Manufacturing"),
    ("Seattle Makers", "https://www.linkedin.com/company/seattlemakershq", "Fabrication / Manufacturing"),
    ("Mechanical Sales Inc.", "https://www.linkedin.com/company/mechanical-sales-inc.", "Fabrication / Manufacturing"),
    ("New Standard Building Materials", "https://www.linkedin.com/company/nsbmseattle", "Fabrication / Manufacturing"),
    ("Mako Fabrication", "https://www.linkedin.com/company/makofabrication", "Fabrication / Manufacturing"),
    ("QB Fabrication & Welding Inc.", "https://www.linkedin.com/company/qb-fabrication-&-welding", "Fabrication / Manufacturing"),

    # === MACHINING ===
    ("Khan Machine Tool Co. Ltd", "https://www.linkedin.com/company/khan-machine-tools-co.", "Machining / Manufacturing"),
    ("Markey Machine LLC", "https://www.linkedin.com/company/markey-machine", "Machining / Manufacturing"),
    ("Horizon Manufacturing", "https://www.linkedin.com/company/horizon-mfg", "Machining / Manufacturing"),
    ("Monroe Machined Products Inc.", "https://www.linkedin.com/company/monroe-machined-products-inc-", "Machining / Manufacturing"),

    # === PRECISION ===
    ("Precision Management Company", "https://www.linkedin.com/company/precision-management-company-inc-", "Precision / Property Management"),
    ("Taylor Precision Products Inc.", "https://www.linkedin.com/company/taylor-precision-products", "Precision / Manufacturing"),
    ("Precision Rigging", "https://www.linkedin.com/company/precision-rigging", "Precision / Events"),
    ("Precision Equipment Inc.", "https://www.linkedin.com/company/precision-equipment-inc.", "Precision / Manufacturing"),

    # === REAL ESTATE ===
    ("Specialty Real Estate Group", "https://www.linkedin.com/company/specialtyrealestategroup", "Commercial Real Estate"),
    ("Dwellings Seattle Real Estate", "https://www.linkedin.com/company/dwellings-seattle-real-estate-and-property-management", "Commercial Real Estate"),
    ("SRE", "https://www.linkedin.com/company/sre-commercial-inc", "Commercial Real Estate"),
    ("Lake & Company Real Estate", "https://www.linkedin.com/company/lake-&-company-real-estate", "Commercial Real Estate"),
    ("Northwest Commercial Real Estate Investments", "https://www.linkedin.com/company/nwcrei", "Commercial Real Estate"),
    ("URBAN Commercial Partners", "https://www.linkedin.com/company/urban-commercial-partners", "Commercial Real Estate"),
    ("West Coast Commercial Realty", "https://www.linkedin.com/company/wccommercialrealty", "Commercial Real Estate"),
    ("Pacific Continental Realty LLC", "https://www.linkedin.com/company/pacific-continental-realty-llc", "Commercial Real Estate"),
    ("Cadence Commercial Real Estate", "https://www.linkedin.com/company/cadence-commercial-real-estate", "Commercial Real Estate"),

    # === PROPERTY MANAGEMENT ===
    ("Seattle's Property Management", "https://www.linkedin.com/company/seattle's-property-management", "Property Management"),
    ("JMW Group | Windermere Property Management", "https://www.linkedin.com/company/windermere-property-management-jmw", "Property Management"),
    ("North Pacific Properties", "https://www.linkedin.com/company/north-pacific-properties", "Property Management"),
    ("Davis Property Management", "https://www.linkedin.com/company/davis-property-management", "Property Management"),
    ("Milestone Properties", "https://www.linkedin.com/company/milestone-properties", "Property Management"),
    ("HOPM", "https://www.linkedin.com/company/hopm", "Property Management"),
    ("Security Properties", "https://www.linkedin.com/company/security-properties", "Property Management"),
    ("Comble Property Management", "https://www.linkedin.com/company/comble-property-management", "Property Management"),
    ("Sound Property Management", "https://www.linkedin.com/company/sound-property-management", "Property Management"),

    # === REAL ESTATE INVESTMENT ===
    ("Pacific Crest Real Estate", "https://www.linkedin.com/company/pacific-crest-property-management", "Real Estate Investment"),
    ("Goodman Real Estate", "https://www.linkedin.com/company/goodman-real-estate", "Real Estate Investment"),
    ("Horizon Realty Advisors", "https://www.linkedin.com/company/horizon-realty-advisors", "Real Estate Investment"),

    # === ENGINEERING ===
    ("Engineering Systems Inc. (ESi)", "https://www.linkedin.com/company/engineeringsystems", "Professional Services / Engineering"),
    ("ENGEO", "https://www.linkedin.com/company/engeo", "Professional Services / Engineering"),
    ("EA Engineering Science and Technology", "https://www.linkedin.com/company/eaest-pbc", "Professional Services / Engineering"),
    ("EEIS Consulting Engineers Inc.", "https://www.linkedin.com/company/eeis-consulting-engineers", "Professional Services / Engineering"),
    ("Engineering Economics Inc.", "https://www.linkedin.com/company/engineering-economics-inc.", "Professional Services / Engineering"),

    # === CONSULTING ===
    ("Seattle Consulting Group", "https://www.linkedin.com/company/the-seattle-consulting-group", "Professional Services / Consulting"),
    ("Slalom", "https://www.linkedin.com/company/slalom-consulting", "Professional Services / Consulting"),
    ("Hanson Consulting Group", "https://www.linkedin.com/company/hanson-consulting-group", "Professional Services / Consulting"),
    ("Ascend Consulting", "https://www.linkedin.com/company/ascend-consulting---seattle-wa", "Professional Services / Consulting"),
    ("JeffreyM Consulting", "https://www.linkedin.com/company/jeffreym-consulting", "Professional Services / Consulting"),
    ("Muchmore Consulting", "https://www.linkedin.com/company/muchmoreconsulting", "Professional Services / Consulting"),
    ("Strategic Business Decisions Consulting", "https://www.linkedin.com/company/strategic-business-decisions-consulting", "Professional Services / Consulting"),
    ("SEBA Professional Services LLC", "https://www.linkedin.com/company/seba-professional-services-llc", "Professional Services"),

    # === ARCHITECTURE ===
    ("Best Practice Architecture", "https://www.linkedin.com/company/best-practice-architecture", "Professional Services / Architecture"),
    ("MAKERS architecture and urban design", "https://www.linkedin.com/company/makersarch", "Professional Services / Architecture"),
    ("SHED Architecture & Design", "https://www.linkedin.com/company/shed-architecture-&-design", "Professional Services / Architecture"),
    ("BuildingWork", "https://www.linkedin.com/company/buildingwork", "Professional Services / Architecture"),
    ("HYBRID ARCHITECTS LLC", "https://www.linkedin.com/company/hybrid-architects-llc", "Professional Services / Architecture"),
    ("TCA Architecture + Planning + Design", "https://www.linkedin.com/company/tca-architecture-and-planning", "Professional Services / Architecture"),
    ("SHKS Architects", "https://www.linkedin.com/company/shksarchitects", "Professional Services / Architecture"),
    ("Shuler Architecture", "https://www.linkedin.com/company/shuler-architecture", "Professional Services / Architecture"),

    # === ENVIRONMENTAL ===
    ("Environment International Ltd", "https://www.linkedin.com/company/environment-international-ltd", "Professional Services / Environmental"),
    ("Environmental Science Associates", "https://www.linkedin.com/company/environmental-science-associates", "Professional Services / Environmental"),
    ("EHS International Inc.", "https://www.linkedin.com/company/ehs-international-inc-seattle-wa", "Professional Services / Environmental"),
    ("Environmental Assessment Services", "https://www.linkedin.com/company/environmental-assessment-services", "Professional Services / Environmental"),

    # === HOTEL / HOSPITALITY ===
    ("The Charter Hotel Seattle", "https://www.linkedin.com/company/thecharterseattle", "Hospitality"),
    ("Fairmont Olympic Hotel", "https://www.linkedin.com/company/the-fairmont-olympic-hotel", "Hospitality"),
    ("The State Hotel", "https://www.linkedin.com/company/the-state-hotel", "Hospitality"),
    ("Hyatt Regency Seattle", "https://www.linkedin.com/company/hyattregencyseattle", "Hospitality"),
    ("Hotel Theodore", "https://www.linkedin.com/company/the-roosevelt-hotel_2", "Hospitality"),
    ("Staypineapple Hotels", "https://www.linkedin.com/company/staypineapple-hotels-inc", "Hospitality"),
    ("Hotel Interurban", "https://www.linkedin.com/company/hotel-interurban", "Hospitality"),
    ("Hilton Seattle Airport & Conference Center", "https://www.linkedin.com/company/hilton-seattle-airport-&-conference-center", "Hospitality"),
    ("Sheraton Grand Seattle", "https://www.linkedin.com/company/sheraton-grand-seattle", "Hospitality"),
    ("Seattle Hospitality Group", "https://www.linkedin.com/company/seattle-hospitality-group", "Hospitality"),
    ("Renaissance Seattle Hotel", "https://www.linkedin.com/company/renaissanceseattle", "Hospitality"),
    ("Hotel 1000", "https://www.linkedin.com/company/hotel-1000", "Hospitality"),
    ("LOTTE HOTEL SEATTLE", "https://www.linkedin.com/company/lottehotelseattle", "Hospitality"),
    ("Columbia Hospitality", "https://www.linkedin.com/company/columbia-hospitality", "Hospitality"),
    ("DSquared Hospitality Company", "https://www.linkedin.com/company/dsquared-hospitality-company", "Hospitality"),
    ("Hyatt at Olive 8", "https://www.linkedin.com/company/hyatt-at-olive-8", "Hospitality"),

    # === NONPROFIT ===
    ("Nonprofit Association of Washington", "https://www.linkedin.com/company/nonprofit-association-of-washington", "Nonprofit"),
    ("The Mockingbird Society", "https://www.linkedin.com/company/mockingbirdsociety", "Nonprofit"),
    ("The Service Board", "https://www.linkedin.com/company/the-service-board", "Nonprofit"),
    ("Seattle Foundation", "https://www.linkedin.com/company/seattle-foundation", "Nonprofit"),
    ("Seattle Neighborhood Group", "https://www.linkedin.com/company/seattle-neighborhood-group", "Nonprofit"),
    ("We Heart Seattle", "https://www.linkedin.com/company/we-heart-seattle", "Nonprofit"),
    ("Communities Rise", "https://www.linkedin.com/company/communities-rise", "Nonprofit"),
    ("Rebuilding Together Seattle", "https://www.linkedin.com/company/rebuilding-together-seattle", "Nonprofit"),
    ("Seattle Works", "https://www.linkedin.com/company/seattle-works", "Nonprofit"),
    ("Facing Homelessness", "https://www.linkedin.com/company/facing-homelessness", "Nonprofit"),
    ("Refugee Women's Alliance (ReWA)", "https://www.linkedin.com/company/refugee-women-s-alliance", "Nonprofit"),
    ("Seattle Jobs Initiative", "https://www.linkedin.com/company/sjiopeningdoors", "Nonprofit"),
    ("Downtown Seattle Association", "https://www.linkedin.com/company/downtown-seattle-association", "Nonprofit"),

    # === FOUNDATIONS ===
    ("ARCS Foundation Seattle Chapter", "https://www.linkedin.com/company/arcsseattle", "Nonprofit / Foundation"),
    ("Seattle Parks Foundation", "https://www.linkedin.com/company/seattle-parks-foundation", "Nonprofit / Foundation"),
    ("Seattle Public Library Foundation", "https://www.linkedin.com/company/seattle-public-library-foundation", "Nonprofit / Foundation"),
    ("Seattle Police Foundation", "https://www.linkedin.com/company/seattle-police-foundation", "Nonprofit / Foundation"),
    ("Seattle International Foundation", "https://www.linkedin.com/company/seattle-international-foundation", "Nonprofit / Foundation"),
    ("Seattle Science Foundation", "https://www.linkedin.com/company/seattle-science-foundation", "Nonprofit / Foundation"),

    # === COMMUNITY ===
    ("Community Roots Housing", "https://www.linkedin.com/company/community-roots-housing", "Nonprofit / Community"),
    ("Commute Seattle", "https://www.linkedin.com/company/commuteseattle", "Nonprofit / Community"),
    ("Communities In Schools of Seattle", "https://www.linkedin.com/company/cisseattle", "Nonprofit / Community"),

    # === CONSTRUCTION ===
    ("Seattle Construction LLC", "https://www.linkedin.com/company/seattle-construction-llc", "Construction"),
    ("Cornerstone Construction of Seattle", "https://www.linkedin.com/company/cornerstone-construction-of-seattle-inc", "Construction"),
    ("United Seattle", "https://www.linkedin.com/company/united-seattle", "Construction"),
    ("ACG Builds Inc.", "https://www.linkedin.com/company/acg-builds-inc", "Construction"),
    ("Sellen Construction", "https://www.linkedin.com/company/sellen-construction", "Construction"),
    ("Cascade Built", "https://www.linkedin.com/company/cascade-built", "Construction"),
    ("Coastal Construction LLC", "https://www.linkedin.com/company/coastal-construction-llc", "Construction"),
    ("STS Construction Services", "https://www.linkedin.com/company/stsconstructionservices", "Construction"),
    ("Emerald City Construction", "https://www.linkedin.com/company/emerald-city-construction", "Construction"),
    ("MRJ Constructors", "https://www.linkedin.com/company/mrj-constructors", "Construction"),
    ("Venture General Contracting", "https://www.linkedin.com/company/venture-general-contracting", "Construction"),
    ("MDI Inc General Contractors", "https://www.linkedin.com/company/mdi-inc-general-contractors", "Construction"),

    # === DISTRIBUTION / LOGISTICS ===
    ("Global Fulfillment", "https://www.linkedin.com/company/global-fulfillment", "Distribution / Logistics"),
    ("Ocean Beauty Seafoods", "https://www.linkedin.com/company/ocean-beauty-seafoods", "Distribution / Logistics"),
    ("Expeditors", "https://www.linkedin.com/company/expeditors", "Distribution / Logistics"),
    ("Delivery Express Logistics", "https://www.linkedin.com/company/delivery-express-inc", "Distribution / Logistics"),
    ("MTI Worldwide Logistics", "https://www.linkedin.com/company/mti-worldwide-logistics", "Distribution / Logistics"),
    ("Specialty Cargo Inc.", "https://www.linkedin.com/company/specialty-cargo-inc-", "Distribution / Logistics"),
    ("SEA LOGIX Inc.", "https://www.linkedin.com/company/sea-logix-inc.", "Distribution / Logistics"),
    ("Warehouse Services Inc.", "https://www.linkedin.com/company/warehouse-services-inc-", "Distribution / Logistics"),

    # === FOOD ===
    ("Food Northwest", "https://www.linkedin.com/company/foodnorthwest", "Food Processing"),
    ("SeafoodSource", "https://www.linkedin.com/company/seafoodsource-com", "Food Processing"),
]


def compile():
    companies = OrderedDict()
    for name, url, industry in results:
        key = name.lower().strip()
        if key not in companies:
            companies[key] = {
                "company_name": name,
                "linkedin_url": url,
                "industry": industry,
                "city": "Seattle",
                "source": "Google X-ray LinkedIn Search"
            }

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["company_name", "linkedin_url", "industry", "city", "source"])
        writer.writeheader()
        for c in companies.values():
            writer.writerow(c)

    print(f"Total unique companies: {len(companies)}")
    print(f"Output: {OUTPUT_FILE}")

    industry_counts = {}
    for c in companies.values():
        ind = c["industry"]
        industry_counts[ind] = industry_counts.get(ind, 0) + 1
    print("\nBreakdown:")
    for ind, count in sorted(industry_counts.items(), key=lambda x: -x[1]):
        print(f"  {ind}: {count}")


if __name__ == "__main__":
    compile()
