# VWC CPAs — ICP Developer Specification

**Source:** VWC ICP Developer Spec (Chad Person, March 17, 2025)
**Derived from:** VWC workshop, March 16, 2025
**Purpose:** Guide prospect list construction, LinkedIn targeting, scoring logic, and campaign segmentation

---

## Two Separate ICPs

VWC runs **two distinct campaigns** with different ICPs, lists, and targeting logic:

| | ICP 1: Audit & Tax | ICP 2: Benefit Plan Audit |
|---|---|---|
| **Service** | Financial statement audit, tax, advisory | Benefit plan (401k/pension) audit |
| **Buyer** | Finance leader (CFO/Controller) | Finance leader OR HR leader (size-dependent) |
| **Geography** | PNW first, then expand | National |
| **Industry** | Specific priority list | Industry-agnostic (employee count drives it) |
| **Key trigger** | Growth signals, leadership changes | Crossing 120 eligible plan participants |
| **Revenue ceiling** | Hard cap at $150M | No ceiling |

---

## ICP 1 — Audit & Tax

### Company Size

| Attribute | Sweet Spot (prioritize) | Acceptable Range | Hard Limit |
|-----------|------------------------|------------------|------------|
| Revenue | $50M - $100M | $25M - $150M | Do NOT include >$150M |
| Employees | 100 - 300 | 25 - 750 | — |

**Notes:**
- Revenue data (ZoomInfo/Apollo) is directional, not definitive — self-reporting issues
- LinkedIn employee count is more reliable — use as primary size filter when revenue unavailable
- Above $150M, companies typically seek Big 4 / national firms — not a realistic target

### Ownership Structure

**Include:**
- Privately owned
- Family-owned
- ESOP (employee stock ownership)
- Founder-led with long-term horizon (10+ years) — **flag as high-value**

**Exclude:**
- Private equity-backed (transactional culture, short holds, not relationship-oriented)
- Public companies
- Government entities
- Banking / financial institutions

### Industries (Priority Order)

| Priority | Industry | Notes |
|----------|----------|-------|
| 1 | Manufacturing | Strong audit fit — debt-driven audit need. Depth in aerospace/Boeing supply chain. |
| 2 | Commercial real estate | Debt-driven audit need. Good existing client experience. |
| 3 | Professional services | Strong existing client base. Often reviews/compilations, strong outsourced CFO + tax opp. |
| 4 | Hospitality | Existing client experience. Hotels and operators. |
| 5 | Nonprofit | Lower priority — price sensitivity. Larger foundations attractive but need credential building. Include, score lower. |
| 6 | Construction | Limited current experience. Include, score lower. |

**Excluded:** Government, banking/financial institutions, public companies, technology (tech is in scope for ICP 2 only).

### Geography (Priority Order)

1. **Primary:** Seattle metro + greater Pacific Northwest
2. **Secondary:** West Coast
3. **Tertiary:** National (only if profile is otherwise very strong)

Build lists in geographic priority order. Exhaust PNW universe before expanding.

### Target Titles

| Priority | Titles | Notes |
|----------|--------|-------|
| **Primary** | CFO, Controller, Director of Finance, VP of Finance | Finance contact is preferred first contact. Many smaller companies ($25M-$75M) use "CFO" for what's effectively a controller — do NOT filter these out. |
| **Secondary** | Owner, President, CEO | Target if no finance contact identifiable. Score slightly lower — potential independence issues if advisory expands. |
| **Do not target** | Staff Accountant, Bookkeeper, Accounting Manager | Too junior to initiate audit relationship. Only use if no other contact exists. |

**Dual contact rule:** When both finance + owner are identifiable at the same company, reach out to **finance first**. No simultaneous dual outreach unless specifically instructed.

### Scoring Signals & Trigger Events

| Signal Type | Indicator | Why It Matters |
|------------|-----------|----------------|
| Growth | New office/facility opening | Increasing complexity = increasing audit need |
| Growth | New acquisition announced | Post-acquisition complexity requires outside advisory |
| Growth | New product/product line | Business expansion = new audit/tax complexity |
| Growth | High job posting volume (15+ active) | Strong growth trajectory indicator |
| Growth | Investment in new equipment/plant | Capital investment signals growth + sophistication |
| Leadership | New CFO or Controller (within 12 months) | New finance leadership often brings new audit partners |
| Competitor | LinkedIn connections to Moss Adams, BDO, Sweeney Conrad, Baker Tilly | Soft signal of current advisor relationship — potential dissatisfaction. **Secondary scoring boost only**, not a hard filter. Connection does NOT confirm they're a client. |

### Hard Exclusions

- Public companies
- Government entities (any level)
- Banking / financial institutions
- Revenue > $150M
- Startups (pre-revenue or early stage)
- Private equity-backed companies

---

## ICP 2 — Benefit Plan Audit

> **Fundamentally different service line.** Different buyer profile, trigger events, company size range, and geographic scope. Build and run as a **separate list and separate campaign**.

### Company Size

| Attribute | Sweet Spot | Acceptable Range |
|-----------|-----------|------------------|
| Revenue | $50M - $100M | No hard ceiling. Larger companies with Big 4 relationships are attractive — VWC offers significant fee savings. |
| Employees | 100 - 300 (first-time threshold crossers) | 120 - 10,000+. The 120-employee threshold triggers audit requirement. |

**Key:** Minimum qualifying employee count is **120 eligible plan participants** (measured by account balances, not headcount). Companies just crossing this threshold are highest priority — they need a new provider by definition.

### Ownership Structure

Same as ICP 1 **with one addition:**
- **Public companies ARE in scope** IF they do not offer company stock as a plan investment option
- If company stock is an investment option → 11-K filing required → VWC does not perform → flag for manual review (don't auto-exclude)

Still exclude: PE-backed, government entities.

### Industries

**Industry is largely a non-factor.** Audit requirement is triggered by employee count, not sector.

- All ICP 1 industries remain valid
- **Technology companies are IN scope** (excluded from Audit & Tax but valid here)
- Exclude: government entities
- Low priority: government-adjacent nonprofits (403b plans) — fee pressure

### Geography

**Geography is a non-factor.** VWC serves benefit plan audit clients nationally. Prioritize PNW in early campaigns, then expand nationally.

### Target Titles

| Company Profile | Primary Titles | Rationale |
|----------------|---------------|-----------|
| Private, under ~$150M | CFO, Controller, Director of Finance | Finance drives the engagement at this size |
| Private, over ~$150M or 500+ employees | HR Director, VP of HR, Chief People Officer | HR owns the benefit plan audit relationship at larger companies |
| Public (any size) | HR Director, VP of HR, Chief People Officer | Default to HR for public companies regardless of size |
| Smaller private | Owner, President (secondary) | Same as ICP 1 |

### Scoring Signals & Trigger Events

| Signal Type | Indicator | Why It Matters |
|------------|-----------|----------------|
| **Threshold** | Crossed or approaching 120 eligible participants | **Highest priority.** Legally triggers audit requirement. |
| Leadership | New HR Director/VP/CPO (within 12 months) | New HR leadership frequently changes benefit plan audit providers — confirmed strong conversion signal |
| Leadership | New CFO or Controller (within 12 months) | New finance leadership brings new partners — valid at all sizes |
| Growth | High job posting volume | Headcount trajectory toward 120 threshold |
| Growth | General growth indicators (new locations, acquisitions, product launches) | Growing companies = growing participant counts |

### Hard Exclusions

- Government entities
- Companies with fewer than 100 employees (below threshold)
- Companies where plan offers employer stock as investment (requires 11-K) — **flag for manual review, don't auto-exclude**

---

## Model Clients (Scoring Calibration)

These four VWC clients are benchmarks for what a strong match looks like. Use to calibrate scoring.

| Company | Industry | ICPs | Notes |
|---------|----------|------|-------|
| **Foremost Fuji Corporation** | Manufacturing | Audit & Tax | Long-tenured client. ~100 employees. Ownership/leadership transition. LinkedIn presence may be limited. |
| **Shannon and Wilson** | Professional services (engineering) | Both | Employee-owned (ESOP). Complex multi-service relationship. VWC does 2 benefit plan audits + tax + review. Top 5 client by revenue. |
| **Skills Inc.** | Nonprofit / Aerospace manufacturing | Both | Nonprofit operating like for-profit. Manufactures airplane parts for Boeing. Boeing supply chain opportunity. |
| **Carillon Properties** | Commercial real estate / Hospitality | Audit & Tax | Owns CRE + hotel. Old family money — board is family, not operationally involved. Previously with Deloitte for audit. VWC does not currently do tax for them. |

---

## Outreach Voice & Positioning

### Tone
- Warm, natural, business-like
- **Not** templated-sounding
- Every message must include at least one research-driven personalization (company name, what they do, location, tenure, recent news)

### Core Differentiation
- **Partner-level attention and continuity** — VWC has been in business 50 years, no plans to sell, clients work with same partners year after year
- Direct counter-positioning to Moss Adams, BDO, Sweeney Conrad, Baker Tilly — where clients often feel under-served
- Audit and tax expertise competitive with regional/national firms, delivered with boutique care and accessibility

### Rules
- **Never reference competitors by name** in outreach — positioning is implied through what VWC offers
- Two LinkedIn profiles: **Adrienne Nordland** and **Melinda Johnson** — each with individualized voice

---

## Mapping to `campaigns.icp` JSONB

This is how the ICP spec maps to the database schema:

```jsonc
// ICP 1 — Audit & Tax campaign
{
  "version": 1,
  "name": "VWC Audit & Tax",
  "type": "audit_tax",

  // Section 1.1 — Company size
  "revenue_range": {"min": 25000000, "max": 150000000},
  "revenue_sweet_spot": {"min": 50000000, "max": 100000000},
  "employee_range": {"min": 25, "max": 750},
  "employee_sweet_spot": {"min": 100, "max": 300},

  // Section 1.2 — Ownership
  "ownership_include": ["private", "family_owned", "esop", "founder_led"],
  "ownership_exclude": ["pe_backed", "public", "government"],
  "ownership_flags": {"founder_led_long_horizon": "high_value"},

  // Section 1.3 — Industries (priority order)
  "industries": [
    {"name": "manufacturing", "priority": 1, "notes": "Debt-driven audit need, Boeing supply chain"},
    {"name": "commercial_real_estate", "priority": 2},
    {"name": "professional_services", "priority": 3, "notes": "Often reviews/compilations, strong CFO + tax opp"},
    {"name": "hospitality", "priority": 4},
    {"name": "nonprofit", "priority": 5, "notes": "Score lower, price sensitivity"},
    {"name": "construction", "priority": 6, "notes": "Score lower, limited experience"}
  ],
  "industries_excluded": ["government", "banking", "financial_institutions", "technology"],

  // Section 1.4 — Geography (priority order)
  "geography": [
    {"region": "Seattle metro / Pacific Northwest", "priority": 1},
    {"region": "West Coast", "priority": 2},
    {"region": "National", "priority": 3, "notes": "Only if profile is very strong"}
  ],

  // Section 1.5 — Titles
  "titles_primary": ["CFO", "Controller", "Director of Finance", "VP of Finance"],
  "titles_secondary": ["Owner", "President", "CEO"],
  "titles_excluded": ["Staff Accountant", "Bookkeeper", "Accounting Manager"],
  "title_rules": {
    "dual_contact": "Finance first, no simultaneous dual outreach",
    "small_company_cfo_note": "Many $25M-$75M companies use CFO title for controller role — do not filter out"
  },

  // Section 1.6 — Scoring signals
  "trigger_signals": [
    {"type": "growth", "indicator": "new_office_facility"},
    {"type": "growth", "indicator": "acquisition"},
    {"type": "growth", "indicator": "new_product_line"},
    {"type": "growth", "indicator": "high_job_postings", "threshold": 15},
    {"type": "growth", "indicator": "equipment_investment"},
    {"type": "leadership", "indicator": "new_cfo_controller", "recency_months": 12},
    {"type": "competitor", "indicator": "linkedin_connections", "firms": ["Moss Adams", "BDO", "Sweeney Conrad", "Baker Tilly"], "usage": "secondary_boost_only"}
  ],

  // Section 1.7 — Hard exclusions
  "hard_exclusions": ["public", "government", "banking", "revenue_above_150m", "startup_pre_revenue", "pe_backed"],

  // Scoring weights (calibrate against model clients)
  "weights": {
    "title": 0.25,
    "industry": 0.20,
    "company_size": 0.15,
    "geography": 0.15,
    "ownership": 0.10,
    "triggers": 0.15
  }
}
```

```jsonc
// ICP 2 — Benefit Plan Audit campaign
{
  "version": 1,
  "name": "VWC Benefit Plan Audit",
  "type": "benefit_plan_audit",

  // Section 2.1 — Company size
  "revenue_range": {"min": 50000000, "max": null},
  "revenue_sweet_spot": {"min": 50000000, "max": 100000000},
  "employee_range": {"min": 120, "max": null},
  "employee_sweet_spot": {"min": 100, "max": 300},
  "employee_threshold_note": "120 eligible plan participants triggers audit requirement — highest priority signal",

  // Section 2.2 — Ownership
  "ownership_include": ["private", "family_owned", "esop", "founder_led", "public_no_company_stock"],
  "ownership_exclude": ["pe_backed", "government"],
  "ownership_flags": {"public_with_company_stock": "flag_manual_review"},

  // Section 2.3 — Industries
  "industries": "all",
  "industries_additional": ["technology"],
  "industries_excluded": ["government"],
  "industries_low_priority": ["government_adjacent_nonprofit_403b"],

  // Section 2.4 — Geography
  "geography": [
    {"region": "Pacific Northwest", "priority": 1, "notes": "Early campaigns only"},
    {"region": "National", "priority": 2}
  ],

  // Section 2.5 — Titles (size-dependent logic)
  "title_rules": {
    "private_under_150m": {"primary": ["CFO", "Controller", "Director of Finance"]},
    "private_over_150m_or_500_employees": {"primary": ["HR Director", "VP of Human Resources", "Chief People Officer"]},
    "public_any_size": {"primary": ["HR Director", "VP of Human Resources", "Chief People Officer"]},
    "small_private": {"secondary": ["Owner", "President"]}
  },

  // Section 2.6 — Scoring signals
  "trigger_signals": [
    {"type": "threshold", "indicator": "approaching_120_participants", "priority": "highest"},
    {"type": "leadership", "indicator": "new_hr_leader", "recency_months": 12, "notes": "Confirmed strong conversion signal"},
    {"type": "leadership", "indicator": "new_cfo_controller", "recency_months": 12},
    {"type": "growth", "indicator": "high_job_postings"},
    {"type": "growth", "indicator": "general_growth_signals"}
  ],

  // Section 2.7 — Hard exclusions
  "hard_exclusions": ["government", "employees_below_100", "plan_offers_employer_stock_flag_review"],

  // Scoring weights
  "weights": {
    "title": 0.20,
    "employee_threshold": 0.25,
    "company_size": 0.15,
    "ownership": 0.10,
    "triggers": 0.20,
    "geography": 0.10
  }
}
```

---

## Implementation Notes

1. **Two campaigns, two lists** — never combine ICP 1 and ICP 2 prospects in the same campaign
2. **ICP 2 title selection requires logic** — use revenue + employee count + ownership structure to decide finance vs HR targeting
3. **Public company handling in ICP 2** — don't auto-exclude, flag for manual review (11-K check)
4. **Competitor connection scoring** — secondary boost only, never the sole reason to include a prospect
5. **Model clients** — run digital footprint scan on all four to calibrate what "good" looks like in Apollo/LinkedIn data
6. **PE detection** — score -10 penalty + flag for manual review, don't auto-disqualify (per master architecture edge case #10)
7. **Revenue data unreliability** — prefer LinkedIn employee count over ZoomInfo/Apollo revenue when available
