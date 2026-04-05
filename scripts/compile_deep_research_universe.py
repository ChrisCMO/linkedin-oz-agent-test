"""
Compile deep research results into a single raw universe CSV.
Sources: Web research (various directories, Wikipedia, PSBJ, ESOP lists, etc.)
No enrichment - just company names, URLs where available, industry, and source.
"""

import csv
import os
from collections import OrderedDict

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "ICP-Prospects", "official")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "deep_research_seattle_universe.csv")

# ========== RAW DATA FROM WEB RESEARCH ==========

# MANUFACTURING (ICP Priority 1)
manufacturing = [
    # From IndustrySelect top 10 WA manufacturers
    ("The Boeing Co.", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Schweitzer Engineering Laboratories", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Terex Aerial Work Platforms", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Philips Oral Healthcare LLC", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Seagen Inc.", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Borton & Sons Inc.", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("Tyson Fresh Meats Inc.", None, "IndustrySelect Top 10 WA Manufacturers"),
    ("HP Inc.", None, "IndustrySelect Top 10 WA Manufacturers"),
    # From BuiltIn Seattle
    ("Monolithic Power Systems", None, "BuiltIn Seattle Manufacturing"),
    ("Alliance Packaging", None, "BuiltIn Seattle Manufacturing"),
    ("Aviation Technical Services", None, "BuiltIn Seattle Manufacturing"),
    ("NWT3K", None, "BuiltIn Seattle Manufacturing"),
    # From Greater Seattle Partners
    ("ZapEnergy", None, "Greater Seattle Partners"),
    ("Helion Energy", None, "Greater Seattle Partners"),
    ("Avalanche Energy", None, "Greater Seattle Partners"),
    ("Kyoto Fusioneering", None, "Greater Seattle Partners"),
    ("Group14 Technologies", None, "Greater Seattle Partners"),
    ("First Mode", None, "Greater Seattle Partners"),
    # From Wikipedia Seattle companies (manufacturing-relevant)
    ("Darigold", None, "Wikipedia Seattle Companies"),
    ("Trident Seafoods", None, "Wikipedia Seattle Companies"),
    ("Fluke Corporation", None, "Wikipedia Seattle Companies"),
    ("Paccar", None, "Wikipedia Seattle Companies"),
    ("Oberto Sausage Company", None, "Wikipedia Seattle Companies"),
    ("K2 Sports", None, "Wikipedia Seattle Companies"),
    ("Cascade Designs", None, "Wikipedia Seattle Companies"),
    ("Brooks Sports", None, "Wikipedia Seattle Companies"),
    ("Pacific Coast Feather Company", None, "Wikipedia Seattle Companies"),
    ("Filson", None, "Wikipedia Seattle Companies"),
    ("Cutter & Buck", None, "Wikipedia Seattle Companies"),
    ("Tom Bihn", None, "Wikipedia Seattle Companies"),
    ("Allied Marble & Granite", None, "Wikipedia Seattle Companies"),
    ("Stanley 1913", None, "Wikipedia Seattle Companies"),
    ("Ventec Life Systems", None, "Wikipedia Seattle Companies"),
    ("Outdoor Research", None, "Wikipedia Seattle Companies"),
    # Wikipedia WA companies (manufacturing)
    ("Kaiser Aluminum", None, "Wikipedia WA Companies"),
    ("Nautilus Inc.", None, "Wikipedia WA Companies"),
    ("Pacific Research Laboratories", None, "Wikipedia WA Companies"),
    ("SAFE Boats International", None, "Wikipedia WA Companies"),
    ("Talking Rain", None, "Wikipedia WA Companies"),
    ("Tim's Cascade Snacks", None, "Wikipedia WA Companies"),
    ("Taylor Shellfish Company", None, "Wikipedia WA Companies"),
    ("Stemilt Growers", None, "Wikipedia WA Companies"),
    ("Weyerhaeuser", None, "Wikipedia WA Companies"),
    ("Simpson Investment Company", None, "Wikipedia WA Companies"),
    ("Liberty Orchards", None, "Wikipedia WA Companies"),
]

# AEROSPACE / BOEING SUPPLY CHAIN (subset of manufacturing)
aerospace = [
    ("General Plastics Manufacturing Company", None, "Pierce County Aerospace"),
    ("GKN Aerospace", None, "Pierce County Aerospace"),
    ("Globe Machine Manufacturing Co.", None, "Pierce County Aerospace"),
    ("Sekisui Aerospace", None, "Pierce County Aerospace"),
    ("Toray Composite Materials America Inc.", None, "Pierce County Aerospace"),
    ("3V Precision Machining Inc.", None, "Pierce County Aerospace"),
    ("AAI/Textron Systems", None, "Pierce County Aerospace"),
    ("Aeroforge", None, "Pierce County Aerospace"),
    ("All Composite Inc.", None, "Pierce County Aerospace"),
    ("American Autoclave Co.", None, "Pierce County Aerospace"),
    ("Baker Manufacturing", None, "Pierce County Aerospace"),
    ("A.S. Inc.", None, "Pierce County Aerospace"),
    ("Cadence Aerospace", None, "Pierce County Aerospace"),
    ("Core Tech LLC", None, "Pierce County Aerospace"),
    ("Dualos", None, "Pierce County Aerospace"),
    ("Farwest Aircraft Inc.", None, "Pierce County Aerospace"),
    ("Future Metals", None, "Pierce County Aerospace"),
    ("GlobalTech Plastics LLC", None, "Pierce County Aerospace"),
    ("Hunter Manufacturing", None, "Pierce County Aerospace"),
    ("IR Specialty Foam", None, "Pierce County Aerospace"),
    ("Locke Precision LLC", None, "Pierce County Aerospace"),
    ("Machine Repair & Design", None, "Pierce County Aerospace"),
    ("Metaltech Inc.", None, "Pierce County Aerospace"),
    ("Mitsui-Soko (USA) Inc.", None, "Pierce County Aerospace"),
    ("Morley Machine Tool Alignment", None, "Pierce County Aerospace"),
    ("Norfil LLC", None, "Pierce County Aerospace"),
    ("Northwest Precision Inc.", None, "Pierce County Aerospace"),
    ("NW Etch", None, "Pierce County Aerospace"),
    ("Omada", None, "Pierce County Aerospace"),
    ("P&J Machining", None, "Pierce County Aerospace"),
    ("PM Testing Laboratory Inc.", None, "Pierce County Aerospace"),
    ("PSI Solutions", None, "Pierce County Aerospace"),
    ("Precision Spring and Stamping Corp.", None, "Pierce County Aerospace"),
    ("Rentiel Precision Laser Cutting LLC", None, "Pierce County Aerospace"),
    ("RTC Aerospace-Fife Division", None, "Pierce County Aerospace"),
    ("Quality Stamping & Machining", None, "Pierce County Aerospace"),
    ("Service Steel Aerospace", None, "Pierce County Aerospace"),
    ("Stenerson & Sons", None, "Pierce County Aerospace"),
    ("Sweeny Industries", None, "Pierce County Aerospace"),
    ("Tool Gauge", None, "Pierce County Aerospace"),
    ("Woodland Trade Co.", None, "Pierce County Aerospace"),
    ("ZEVA Aero", None, "Pierce County Aerospace"),
    ("AeroTEC", None, "Zetwerk Seattle Aerospace"),
    ("magniX", None, "Zetwerk Seattle Aerospace"),
    ("ZeroAvia", None, "Zetwerk Seattle Aerospace"),
    ("Universal Hydrogen", None, "Zetwerk Seattle Aerospace"),
    ("Collins Aerospace", None, "Mix Aerospace Seattle"),
    ("Alaska Star Ventures", None, "Zetwerk Seattle Aerospace"),
    ("Aero Controls Inc.", None, "Wikipedia Seattle Companies"),
]

# COMMERCIAL REAL ESTATE (ICP Priority 2)
commercial_re = [
    ("Kidder Mathews", None, "Clutch.co Seattle CRE"),
    ("Martin Selig Real Estate", None, "Clutch.co Seattle CRE"),
    ("Unico Properties LLC", None, "Clutch.co Seattle CRE"),
    ("SRE", None, "Clutch.co Seattle CRE"),
    ("Infiniti Real Estate & Development", None, "Clutch.co Seattle CRE"),
    ("Washington Holdings", None, "Clutch.co Seattle CRE"),
    ("Clise Properties", None, "Clutch.co Seattle CRE"),
    ("Real Property Associates Inc.", None, "Clutch.co Seattle CRE"),
    ("Redside Partners LLC", None, "Clutch.co Seattle CRE"),
    ("North Pacific Properties", None, "Clutch.co Seattle CRE"),
    ("TARRAGON LLC", None, "Clutch.co Seattle CRE"),
    ("Koehler & Company", None, "Clutch.co Seattle CRE"),
    ("Blanton Turner", None, "Clutch.co Seattle CRE"),
    ("GFK Management Inc.", None, "Clutch.co Seattle CRE"),
    ("Coho Real Estate Group LLC", None, "Clutch.co Seattle CRE"),
    ("Champions Real Estate Services", None, "Clutch.co Seattle CRE"),
    ("JSH Properties Inc.", None, "Clutch.co Seattle CRE"),
    ("Icon Real Estate Services Inc.", None, "Clutch.co Seattle CRE"),
    ("The Andover Company Inc.", None, "Clutch.co Seattle CRE"),
    ("Comble Property Management", None, "Clutch.co Seattle CRE"),
    ("Lori Gill and Associates", None, "Clutch.co Seattle CRE"),
    ("Hughes Marino", None, "Clutch.co Seattle CRE"),
    # Additional CRE firms from web search
    ("Colliers Puget Sound", None, "Web Search Seattle CRE"),
    ("CBRE Seattle", None, "Web Search Seattle CRE"),
    ("Lee & Associates Northwest", None, "Web Search Seattle CRE"),
    ("Broderick Group", None, "Web Search Seattle CRE"),
    ("NAI Puget Sound Properties", None, "Web Search Seattle CRE"),
    ("Skyline Properties Inc.", None, "Web Search Seattle CRE"),
    ("Davis Property Management", None, "Web Search Seattle CRE"),
    ("Azose Commercial Properties Inc.", None, "Web Search Seattle CRE"),
    # From Wikipedia
    ("Windermere Real Estate", None, "Wikipedia Seattle Companies"),
    ("John L. Scott", None, "Wikipedia Seattle Companies"),
    ("Plum Creek Timber", None, "Wikipedia Seattle Companies"),
    ("Howard S. Wright Companies", None, "Wikipedia Seattle Companies"),
    ("Diamond Parking", None, "Wikipedia Seattle Companies"),
    ("Realogics Sotheby's International Realty", None, "PSBJ Family-Owned List"),
]

# PROFESSIONAL SERVICES (ICP Priority 3) - engineering, consulting, architecture
professional_services = [
    # Engineering firms
    ("Shannon and Wilson", "https://www.shannonwilson.com/", "Web Search Seattle Engineering"),
    ("Coffman Engineers", "https://www.coffman.com/", "Web Search Seattle Engineering"),
    ("Harris Group", "https://www.harrisgroup.com/", "Web Search Seattle Engineering"),
    ("Parametrix", "https://www.parametrix.com/", "Web Search Seattle Engineering"),
    ("Landau Associates", None, "ESOP List WA"),
    ("Perteet", None, "ESOP List WA"),
    ("Anvil Corporation", None, "ESOP List WA"),
    ("GeoEngineers Inc.", None, "PSBJ Largest Private Companies"),
    # Architecture firms
    ("NBBJ", None, "Wikipedia Seattle Companies"),
    ("Olson Kundig Architects", None, "Wikipedia Seattle Companies"),
    ("MG2", None, "Wikipedia Seattle Companies"),
    ("Miller Hull Partnership", None, "Wikipedia Seattle Companies"),
    ("Mithun", None, "Wikipedia Seattle Companies"),
    ("Bassetti Architects", None, "Wikipedia Seattle Companies"),
    ("Callison", None, "Wikipedia Seattle Companies"),
    ("Johnson Braund Design Group", None, "Wikipedia Seattle Companies"),
    ("Weber Thompson", None, "Wikipedia Seattle Companies"),
    ("MulvannyG2 Architecture", None, "Wikipedia WA Companies"),
    ("LMN Architects", None, "Wikipedia WA Companies"),
    # Consulting firms
    ("Slalom Consulting", None, "Wikipedia Seattle Companies"),
    ("Point B", None, "ESOP List WA - 100% Employee-Owned"),
    ("Alvarez and Marsal", None, "Wikipedia Seattle Companies"),
    ("Avanade", None, "Wikipedia Seattle Companies"),
    ("Westlake Associates Inc.", None, "PSBJ Fastest Growing"),
    ("Stokes Lawrence", None, "PSBJ Fastest Growing"),
    ("Foushee & Associates Co. Inc.", None, "PSBJ Largest Private Companies"),
    # Other professional services
    ("Davis Wright Tremaine", None, "Wikipedia Seattle Companies"),
    ("Lane Powell", None, "Wikipedia Seattle Companies"),
    ("Perkins Coie", None, "Wikipedia Seattle Companies"),
    ("Waggener Edstrom", None, "Wikipedia Seattle Companies"),
    ("SoundEarth Strategies", None, "ESOP List WA"),
    ("Meier Architecture Engineering", None, "ESOP List WA"),
    ("CDM Smith", None, "Web Search Seattle Engineering"),
    ("HDR", None, "Web Search Seattle Engineering"),
]

# HOSPITALITY (ICP Priority 4)
hospitality = [
    ("Seattle Hospitality Group", "https://www.shgllc.com", "Web Search Seattle Hospitality"),
    ("BMI Hospitality Management", "https://www.bmihospitality.com/", "Web Search Seattle Hospitality"),
    ("Columbia Hospitality", "https://columbiahospitality.com/", "Web Search Seattle Hospitality"),
    ("The Dow Hotel Company", None, "Web Search Seattle Hospitality"),
    ("Holland America Line", None, "Web Search Seattle Hospitality"),
    # From Wikipedia
    ("Ambassadors International", None, "Wikipedia Seattle Companies"),
    ("Windstar Cruises", None, "Wikipedia Seattle Companies"),
    # Additional
    ("Saltchuk", None, "Wikipedia Seattle Companies"),
]

# NONPROFIT (ICP Priority 5) - larger orgs with 25+ employees
nonprofit = [
    ("Bill & Melinda Gates Foundation", None, "Web Search Seattle Nonprofits"),
    ("PATH", None, "Web Search Seattle Nonprofits"),
    ("Fred Hutchinson Cancer Research Center", None, "Wikipedia Seattle Companies"),
    ("Casey Family Programs", None, "Seattle Nonprofit Directory"),
    ("Food Lifeline", None, "Web Search Seattle Nonprofits"),
    ("Evergreen Goodwill", None, "Seattle Nonprofit Directory"),
    ("Pioneer Human Services", None, "Seattle Nonprofit Directory"),
    ("YMCA of Greater Seattle", None, "Seattle Nonprofit Directory"),
    ("YWCA of Seattle / King County / Snohomish County", None, "Seattle Nonprofit Directory"),
    ("United Way of King County", None, "Seattle Nonprofit Directory"),
    ("World Vision", None, "Seattle Nonprofit Directory"),
    ("Boys and Girls Clubs of Puget Sound", None, "Seattle Nonprofit Directory"),
    ("Big Brothers Big Sisters of Puget Sound", None, "Seattle Nonprofit Directory"),
    ("Hopelink", None, "Seattle Nonprofit Directory"),
    ("Mary's Place", None, "Seattle Nonprofit Directory"),
    ("Catholic Community Services of Western Washington", None, "Seattle Nonprofit Directory"),
    ("Lutheran Community Services Northwest", None, "Seattle Nonprofit Directory"),
    ("Northwest Center", None, "Seattle Nonprofit Directory"),
    ("Solid Ground", None, "Seattle Nonprofit Directory"),
    ("FareStart", None, "Seattle Nonprofit Directory"),
    ("Treehouse", None, "Seattle Nonprofit Directory"),
    ("Volunteers of America Western Washington", None, "Seattle Nonprofit Directory"),
    ("Habitat for Humanity Seattle/King County", None, "Seattle Nonprofit Directory"),
    ("Plymouth Housing Group", None, "Seattle Nonprofit Directory"),
    ("Low Income Housing Institute", None, "Seattle Nonprofit Directory"),
    ("Neighborcare Health", None, "Seattle Nonprofit Directory"),
    ("Sea Mar Community Health Centers", None, "Seattle Nonprofit Directory"),
    ("Bloodworks Northwest", None, "Seattle Nonprofit Directory"),
    ("Downtown Emergency Service Center (DESC)", None, "Seattle Nonprofit Directory"),
    ("Forterra", None, "Seattle Nonprofit Directory"),
    ("IslandWood", None, "Seattle Nonprofit Directory"),
    ("Seattle Aquarium", None, "Seattle Nonprofit Directory"),
    ("Woodland Park Zoo", None, "Seattle Nonprofit Directory"),
    ("Seattle Opera", None, "Seattle Nonprofit Directory"),
    ("Seattle Symphony Orchestra", None, "Seattle Nonprofit Directory"),
    ("Pacific Northwest Ballet", None, "Seattle Nonprofit Directory"),
    ("Seattle Art Museum", None, "Seattle Nonprofit Directory"),
    ("Seattle Children's Theatre", None, "Seattle Nonprofit Directory"),
    ("5th Avenue Theatre", None, "Seattle Nonprofit Directory"),
    ("Seattle Repertory Theatre", None, "Seattle Nonprofit Directory"),
    ("Code.org", None, "Seattle Nonprofit Directory"),
    ("Upaya Social Ventures", None, "Medium Top 20 Nonprofits Seattle"),
    ("The Nature Conservancy", None, "Medium Top 20 Nonprofits Seattle"),
    ("VertueLab", None, "Medium Top 20 Nonprofits Seattle"),
    ("Grist", None, "Medium Top 20 Nonprofits Seattle"),
    ("Snow Leopard Trust", None, "Medium Top 20 Nonprofits Seattle"),
    ("Gage Academy of Art", None, "Medium Top 20 Nonprofits Seattle"),
    ("Washington Research Foundation", None, "Web Search Seattle Foundations"),
    ("Seattle Foundation", None, "Web Search Seattle Foundations"),
    ("Bezos Family Foundation", None, "Web Search Seattle Foundations"),
    ("Paul G. Allen Family Foundation", None, "Web Search Seattle Foundations"),
    ("Children's Home Society of Washington", None, "Seattle Nonprofit Directory"),
    ("Ryther Child Center", None, "Seattle Nonprofit Directory"),
    ("Childhaven", None, "Seattle Nonprofit Directory"),
    ("El Centro de la Raza", None, "Seattle Nonprofit Directory"),
    ("Asian Counseling and Referral Service", None, "Seattle Nonprofit Directory"),
    ("Refugee Women's Alliance", None, "Seattle Nonprofit Directory"),
    ("Holt International", None, "Seattle Nonprofit Directory"),
    ("Medical Teams International", None, "Seattle Nonprofit Directory"),
    ("Emeritus Senior Living", None, "Wikipedia Seattle Companies"),
]

# CONSTRUCTION (ICP Priority 6)
construction = [
    ("Sellen Construction", None, "Wikipedia Seattle Companies"),
    ("Lease Crutcher Lewis", None, "ESOP List WA / Seattle Architects"),
    ("GLY Construction", None, "Web Search Seattle Construction"),
    ("W.G. Clark Construction", None, "Clutch.co / Seattle Architects"),
    ("Schuchart", None, "Seattle Architects Best Construction"),
    ("Bayley Construction", None, "Seattle Architects / PSBJ"),
    ("Unimark Commercial Builders", None, "Seattle Architects Best Construction"),
    ("JTM Construction", None, "Seattle Architects Best Construction"),
    ("Venture General Contracting LLC", None, "Seattle Architects Best Construction"),
    ("Prestige Residential Construction", None, "Seattle Architects Best Construction"),
    ("Hammer & Hand", None, "Seattle Architects Best Construction"),
    ("Rainier Custom Homes", None, "Seattle Architects Best Construction"),
    ("Walter F. Toth Construction", None, "Seattle Architects Best Construction"),
    ("Charter Construction", None, "ESOP List WA / Clutch.co Seattle CRE"),
    ("Skyline Construction", None, "Web Search Seattle Construction"),
    ("Rush Commercial Construction", None, "Web Search Seattle Construction"),
    ("Absher Construction", None, "PSBJ Largest Private Companies"),
    ("BNBuilders", None, "PSBJ Largest Private Companies"),
    ("J.R. Abbott Construction Inc.", None, "PSBJ Largest Private Companies"),
    ("Manson Construction Company", None, "ESOP List WA"),
    ("PCL Construction Services", None, "Web Search Seattle Construction"),
    ("Turner Construction Company", None, "Web Search Seattle Construction"),
    ("Cochran Inc.", None, "PSBJ Largest Private Companies"),
    ("PSF Mechanical", None, "PSBJ Largest Private Companies"),
]

# ESOP / EMPLOYEE-OWNED (bonus - high ICP value)
esop_companies = [
    ("Red Dot Corporation", None, "ESOP List WA"),
    ("Buse Timber & Sales", None, "ESOP List WA"),
    ("Custom Interface", None, "ESOP List WA"),
    ("EmpRes Healthcare", None, "ESOP List WA"),
    ("Healthcare Resource Group", None, "ESOP List WA"),
    ("Lloyd Enterprises", None, "ESOP List WA"),
    ("Manhasset Specialty Company", None, "ESOP List WA"),
    ("Paine Electronics", None, "ESOP List WA"),
    ("Parts Wholesalers", None, "ESOP List WA"),
    ("PLEXSYS Interface Products", None, "ESOP List WA"),
    ("Riverview Community Bank", None, "ESOP List WA"),
    ("Stoneway Electric Supply", None, "ESOP List WA"),
    ("TRS Group", None, "ESOP List WA"),
    ("UniWest", None, "ESOP List WA"),
    ("Mud Bay", None, "Seattle Times ESOP Article"),
    ("Keller Supply Co.", None, "PSBJ Largest Private Companies"),
    ("Central Welding Supply", None, "PSBJ Largest Private Companies"),
]

# ADDITIONAL FROM AGENT RESEARCH (with URLs/LinkedIn)
agent_hospitality = [
    ("Silver Cloud Hotels", "https://www.linkedin.com/company/silver-cloud-hotels", "Agent Research - Hospitality"),
    ("Puget Sound Hospitality Group", "https://www.linkedin.com/company/pugetsoundhospitality", "Agent Research - Hospitality"),
    ("R.C. Hedreen Company", "http://www.rchco.com/", "Agent Research - Hospitality"),
    ("Provenance Hotels", "https://www.provenance.com/", "Agent Research - Hospitality"),
    ("McMenamins", "https://www.mcmenamins.com/", "Agent Research - Hospitality"),
    ("The Hotel Group", "https://www.thehotelgroup.com/", "Agent Research - Hospitality"),
]

agent_nonprofit = [
    ("CRISTA Ministries", "https://crista.org/", "Agent Research - Nonprofit"),
    ("Northwest Harvest", "https://www.northwestharvest.org/", "Agent Research - Nonprofit"),
    ("YouthCare", "https://www.linkedin.com/company/youthcare-seattle", "Agent Research - Nonprofit"),
    ("Kindering", "https://www.linkedin.com/company/kindering", "Agent Research - Nonprofit"),
    ("Ballmer Group", "https://www.ballmergroup.org/", "Agent Research - Nonprofit"),
    ("Raikes Foundation", "https://www.raikesfoundation.org/", "Agent Research - Nonprofit"),
    ("Medina Foundation", "https://www.medinafoundation.org/", "Agent Research - Nonprofit"),
    ("Associated Ministries", "https://www.linkedin.com/company/associated-ministries", "Agent Research - Nonprofit"),
    ("Bellevue LifeSpring", "https://www.bellevuelifespring.org/", "Agent Research - Nonprofit"),
    ("Jubilee REACH", "https://www.jubileereach.org/", "Agent Research - Nonprofit"),
    ("Pacific Science Center", "https://pacificsciencecenter.org/", "Agent Research - Nonprofit"),
    ("Museum of Flight", "https://www.museumofflight.org/", "Agent Research - Nonprofit"),
    ("Seattle Theatre Group", "https://www.stgpresents.org/", "Agent Research - Nonprofit"),
]

agent_other = [
    ("McKinstry", "https://www.mckinstry.com/", "Agent Research - Construction/Energy"),
    ("Vulcan Real Estate", "https://vulcanrealestate.com/", "Agent Research - Real Estate"),
    ("Wright Runstad & Company", "https://www.linkedin.com/company/wright-runstad-&-company", "Agent Research - Real Estate"),
    ("Urban Renaissance Group", "https://www.urbanrengroup.com/", "Agent Research - Real Estate"),
    ("Charlie's Produce", "https://www.linkedin.com/company/charlie's-produce", "Agent Research - Food Distribution"),
    ("Mortenson Seattle", "https://www.mortenson.com/locations/seattle", "Agent Research - Construction"),
    ("Metropolitan Market", "https://metropolitan-market.com/", "Agent Research - Retail/Grocery"),
    ("Ivar's", "https://www.ivars.com/", "Agent Research - Restaurant"),
]

# OTHER / MISC from Wikipedia & PSBJ that fit ICP (privately held, non-tech, non-banking)
other_icp_fit = [
    # Food/beverage manufacturing
    ("Beecher's Handmade Cheese", None, "Wikipedia Seattle Companies"),
    ("Caffe Vita Coffee Roasting Company", None, "Wikipedia Seattle Companies"),
    ("Jones Soda", None, "Wikipedia Seattle Companies"),
    ("MOD Pizza", None, "Wikipedia Seattle Companies"),
    ("Piroshky Piroshky", None, "Wikipedia Seattle Companies"),
    ("Theo Chocolate", None, "Wikipedia Seattle Companies"),
    ("Tully's Coffee", None, "Wikipedia Seattle Companies"),
    ("Uwajimaya", None, "Wikipedia Seattle Companies"),
    ("Dick's Drive-In", None, "Wikipedia WA Companies"),
    # Retail / consumer (privately held, Seattle-based)
    ("Nordstrom", None, "Wikipedia Seattle Companies"),
    ("REI", None, "Wikipedia Seattle Companies"),
    ("Sur La Table", None, "Wikipedia Seattle Companies"),
    ("Tommy Bahama", None, "Wikipedia Seattle Companies"),
    ("Zumiez", None, "Wikipedia Seattle Companies"),
    ("PCC Community Markets", None, "Wikipedia WA Companies"),
    ("Bartell Drugs", None, "Wikipedia Seattle Companies"),
    # Transportation/logistics
    ("Convoy", None, "Wikipedia Seattle Companies"),
    ("Expeditors International", None, "Wikipedia Seattle Companies"),
    # Other services
    ("Gravity Payments", None, "Wikipedia Seattle Companies"),
    ("Premera Blue Cross", None, "Wikipedia WA Companies"),
    ("Puget Sound Energy", None, "Wikipedia WA Companies"),
    ("PEMCO", None, "Wikipedia Seattle Companies"),
    ("Safeco", None, "Wikipedia Seattle Companies"),
]


def compile_universe():
    """Compile all sources into a single deduplicated CSV."""

    all_companies = OrderedDict()  # name -> (url, industry, source)

    datasets = [
        (manufacturing, "Manufacturing"),
        (aerospace, "Aerospace / Manufacturing"),
        (commercial_re, "Commercial Real Estate"),
        (professional_services, "Professional Services"),
        (hospitality, "Hospitality"),
        (nonprofit, "Nonprofit"),
        (construction, "Construction"),
        (esop_companies, "ESOP / Employee-Owned"),
        (agent_hospitality, "Hospitality"),
        (agent_nonprofit, "Nonprofit"),
        (agent_other, "Other ICP-Relevant"),
        (other_icp_fit, "Other ICP-Relevant"),
    ]

    for dataset, industry in datasets:
        for item in dataset:
            name = item[0].strip()
            url = item[1] if item[1] else ""
            source = item[2]

            # Normalize name for dedup
            key = name.lower().replace(",", "").replace(".", "").replace(" inc", "").replace(" llc", "").replace(" ltd", "").strip()

            if key not in all_companies:
                all_companies[key] = {
                    "company_name": name,
                    "url": url,
                    "industry": industry,
                    "source": source,
                    "city": "Seattle Metro / Puget Sound",
                }
            else:
                # Append source if different
                existing = all_companies[key]
                if source not in existing["source"]:
                    existing["source"] += f" | {source}"
                # Keep URL if we have one and existing doesn't
                if url and not existing["url"]:
                    existing["url"] = url

    # Write CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["company_name", "url", "industry", "city", "source"])
        writer.writeheader()
        for company in all_companies.values():
            writer.writerow(company)

    print(f"Total unique companies: {len(all_companies)}")
    print(f"Output: {OUTPUT_FILE}")

    # Print breakdown by industry
    industry_counts = {}
    for c in all_companies.values():
        ind = c["industry"]
        industry_counts[ind] = industry_counts.get(ind, 0) + 1

    print("\nBreakdown by industry:")
    for ind, count in sorted(industry_counts.items(), key=lambda x: -x[1]):
        print(f"  {ind}: {count}")


if __name__ == "__main__":
    compile_universe()
