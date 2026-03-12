# Aradune — Master Vision & Architecture Document
> **Canonical reference for all development, product decisions, and Claude Code sessions.**
> Last updated: March 5, 2026 · Last commit: `b549998` · Live: https://www.aradune.co

---

## 1. What Aradune Is

Aradune (aradune.co) is the **definitive intelligence platform for everyone who works with Medicaid** — the one place where any stakeholder can access, query, analyze, and upload data related to the $880B Medicaid program, without needing a consulting contract or a research data use agreement.

The ceiling is the Bloomberg Terminal of Medicaid. Not a CPRA tool. Not a rate lookup. A living, queryable, AI-native data system that aggregates every meaningful public signal about Medicaid — rates, spending, enrollment, provider networks, policy documents, economic conditions, caseload forecasts, quality measures — and makes all of it connectable, comparable, and actionable through a Claude-driven interface.

**Anyone who works with Medicaid should be able to use Aradune.** State Medicaid directors. MCO actuaries. Hospital CFOs. Independent researchers. Legislative aides. Providers deciding whether to participate. Advocates building a case. Journalists following the money.

Named after Brad McQuaid's EverQuest paladin character. Domain: aradune.co.

---

## 2. Current State (March 2026)

**13 live tools.** React 18 + TypeScript + Vite, deployed on Vercel Pro. All computation client-side; 3 small Vercel serverless functions in `api/`.

| Group | Tool | Route | Status |
|-------|------|-------|--------|
| Transparency | Spending Explorer | `/#/explorer` | live |
| Transparency | Medicare Comparison | `/#/decay` | live |
| Transparency | State Fee Schedule Directory | `/#/fees` | live |
| Transparency | Rate Lookup | `/#/lookup` | live |
| Transparency | Compliance Report | `/#/compliance` | live |
| **Transparency** | **CPRA Generator** | **`/#/cpra`** | **live — wedge product** |
| Adequacy | Rate & Wage Comparison | `/#/wages` | live |
| Adequacy | Quality Linkage | `/#/quality` | live |
| Adequacy | Rate Reduction Analyzer | `/#/reduction` | live |
| Adequacy | HCBS Compensation Tracker | `/#/hcbs8020` | live |
| Modeling | Rate Builder | `/#/builder` | live |
| Modeling | AHEAD Calculator | `/#/ahead` | live |
| Modeling | Policy Analyst | `/#/analyst` | beta (no auth yet) |

**Data already ingested:**
- T-MSIS claims: 227M rows (OT file, Jan 2018–Dec 2024, from Feb 2026 HHS/DOGE release)
- State fee schedules: 42 states (via PDF extraction and direct scrape)
- NPPES: Full NPI registry
- CPRA pipeline (Terminal B): 1.79M rows in DuckDB, 34 states with E/M rate comparisons, 258K DQ flags
- CMS supplemental data: 15 datasets / 9.3M rows (FMAP, enrollment, NADAC, SDUD, CMS-64, managed care, quality measures, DSH, NCCI, HCPCS Level II, 1115 waivers, drug rebate)

**Known issues to fix before next feature work:**
- White page on `/#/cpra` after deploy — likely fixed (ErrorBoundary added), needs production confirmation
- T-MSIS DuckDB (17.5GB) connects but is empty — R pipeline must run to populate
- 6 states missing from CPRA (AZ, DE, IA, NC, NY, WV) — code normalization needed in join
- Conversion factor discrepancy: `medicare_pfs.py` uses $32.3465 (QPP-adjusted, wrong) — reconcile to $33.4009

---

## 3. The Core Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          INTERFACE LAYER                                │
│                                                                         │
│   Free: Dashboards · Lookups · Rate Builder · State Profiles           │
│   Paid: Claude Policy Analyst · Structured Reports · API Access        │
│   Institutional: White-Label · Custom Integrations · DaaS Licensing    │
│                                                                         │
│   [ USER DATA UPLOAD / CONNECT ]                                        │
│   Upload your own fee schedules, cost reports, forecasts, contracts.    │
│   Connect your own data systems via API key or file upload.             │
│   Claude analyzes your data in the context of Aradune's national layer. │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     CLAUDE-NATIVE ANALYTICS ENGINE                      │
│                                                                         │
│   NL2SQL · RAG over policy corpus · Cross-state comparisons            │
│   Rate adequacy modeling · Caseload & expenditure forecasting          │
│   AHEAD / global budget modeling · SPA impact analysis                 │
│   MCO gap analysis · Network adequacy · Border arbitrage               │
│   ML model scoring + performance tracking over time                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                        UNIFIED DATA LAYER                               │
│                                                                         │
│   Normalized schema across all states and sources                      │
│   Versioned · timestamped · auditable · source-provenant              │
│   Hive-partitioned Parquet on S3 (DuckDB reads directly)               │
│   pgvector index for document corpus (RAG)                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION PIPELINES                            │
│                                                                         │
│   Automated scrapers (scheduled, change-detected)                      │
│   Per-source ETL: fetch → parse → validate → normalize → load          │
│   PDF extraction (pdfplumber → Claude API normalization)               │
│   Historical snapshots + rollback capability                           │
│   Pause/resume controls · Update notifications · Admin dashboard       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                          RAW DATA UNIVERSE                              │
│                                                                         │
│   MEDICAID CORE: T-MSIS/TAF · Fee Schedules (51 states) · CMS-64      │
│   MANAGED CARE: MCO contracts · Rate certifications · MLR reports      │
│   HOSPITALS: HCRIS cost reports · DSH/UPL · Price transparency MRFs   │
│   POLICY: SPAs · Waivers (1115/1915) · CIBs · SHO letters · Fed Reg  │
│   ECONOMIC: BLS wages · CPI · Unemployment · Poverty rates · Housing  │
│   DEMOGRAPHIC: ACS · Census · SDOH indicators · HRSA shortage areas   │
│   QUALITY: Medicaid Scorecard · HEDIS · Core Sets · DQ Atlas          │
│   PHARMACY: NADAC · SDUD · State MAC prices                           │
│   USER-UPLOADED: Any data the user brings to the platform              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. The CPRA Wedge — 42 CFR 447.203

The Comparative Payment Rate Analysis (CPRA) is a structured report every state must publish by **July 1, 2026** under 42 CFR 447.203 (CMS Ensuring Access Final Rule). It shows Medicaid rates as a percentage of Medicare for primary care, OB/GYN, and outpatient MH/SUD services.

**Aradune's CPRA Generator (`/#/cpra`) is the first tool that automates this.** No existing platform produces it. It is the wedge — it gets state agencies in the door, and once they're using Aradune for CPRA compliance, they discover the rest of the platform.

**Current CPRA data (Terminal B, March 2026):**
- 34 states with E/M rate comparisons
- 74 codes across 3 categories: primary care (29), OB/GYN (18), MH/SUD (27)
- Median pct_of_medicare: 84.8%
- Primary care avg: 81.4% · MH/SUD avg: 99.6% · OB/GYN avg: 132.9%
- 6 states missing (AZ, DE, IA, NC, NY, WV) — code normalization fix needed

**Compliance rules enforced in every CPRA output:**
- Base rates only — no supplemental payments in the percentage
- Non-facility Medicare rate is the benchmark (not facility)
- CY2025 Medicare PFS rates at CF $33.4009 (non-QPP)
- Organized by service category per 42 CFR 447.203(b)(6)
- July 1, 2026 deadline; updated biennially thereafter

**The regulatory status:** No legal challenge. No injunction. No proposed delay. Survived Trump regulatory freeze (DOGE exempted CMS-2442-F from review). One Big Beautiful Bill explicitly excluded CMS-2442-F. **The deadline has not moved.**

---

## 5. The Data Universe

### 5.1 Medicaid Base Rates & Claims

| Source | Description | Format | Cadence | Status |
|--------|-------------|--------|---------|--------|
| T-MSIS OT file (HHS/DOGE) | 227M rows, provider-level spending by HCPCS, 2018–2024 | Parquet | Done Feb 2026 | ✅ ingested |
| T-MSIS TAF (full, via ResDAC) | Person-level claims + eligibility, 119M beneficiaries | Parquet | Quarterly | Ring 3 — later |
| Medicare PFS RVU Files | Work/PE/MP RVUs, GPCIs, status indicators | ZIP/CSV | Annual + quarterly | ✅ ingested (CY2025) |
| State Medicaid Fee Schedules | All 51 jurisdictions — base rates only | PDF/XLSX/CSV | Annual | 42 states ✅ |
| NPPES NPI Registry | Full provider directory | ZIP/CSV + API | Weekly | ✅ ingested |
| Medicaid Provider Enrollment Files | State-specific actively enrolled providers (distinct from NPPES) | State portals | Annual | planned |
| CMS-64 Expenditure Reports | Federal/state expenditure by category and quarter | CSV | Quarterly | via Terminal B |
| MBES/CBES Enrollment/Expenditure | State-level FFS + MCO enrollment + spending | Excel | Quarterly | via Terminal B |
| HCRIS Hospital Cost Reports | Hospital-level costs, payer mix, charge data | CSV | Quarterly (2–4yr lag) | planned |
| Provider of Services (POS) File | Facility characteristics: beds, ownership, CCN | CSV | Quarterly | planned |

### 5.2 Supplemental Payment Programs ← MAJOR DATA GAP, HIGH PRIORITY

This is the most underrepresented category in Aradune. For safety net hospitals, supplemental payments can exceed base Medicaid rates by 2–5x. Any "Medicaid pays X% of Medicare" analysis that ignores supplemental payments is telling half the story. All sources below are **Ring 0 — publicly available, no DUA required.**

| Source | Program | Description | Format | Cadence | Priority |
|--------|---------|-------------|--------|---------|----------|
| CMS DSH Allotment Reports | DSH | Federal allotments by state; hospital-level audits (2-yr lag) | PDF/Excel | Annual | **P1** |
| CMS-64 Schedule A/B | UPL / DSH | State expenditure claims including supplemental payments | CSV | Quarterly | **P1** |
| UPL Demonstrations (state SPAs) | UPL | Upper Payment Limit demonstration filings — IGT/CPE mechanics | PDF | Ongoing | **P1** |
| State Directed Payment filings (42 CFR 438.6(c)) | SDP | Directed payments through managed care — fastest-growing supplemental category | PDF/state portals | Annual | **P1** |
| HRSA GME data | GME | Direct and indirect graduate medical education payments to teaching hospitals | CSV | Annual | **P1** |
| CMS 1115 waiver terms & financial data | LIP/DSRIP/UC | Low Income Pool, Delivery System Reform Incentive Payments, Uncompensated Care pools | PDF | Ongoing | **P1** |
| MACPAC supplemental payment reports | All | Best secondary source for cross-state supplemental payment analysis | PDF | Annual | **P2** |
| OIG DSH audit reports | DSH | Hospital-level audit findings, overpayments, recoupment | PDF | Ongoing | **P2** |

**Key program definitions:**
- **DSH:** Disproportionate Share Hospital payments. Federal allotments distributed to states, then to qualifying hospitals with high Medicaid/uninsured share. CMS publishes annual allotments and audits.
- **UPL:** Upper Payment Limit — the cap on aggregate Medicaid payments. IGT/CPE financing allows local government funds to draw down federal match above base rates, creating supplemental payments.
- **IGT/CPE:** Intergovernmental Transfers / Certified Public Expenditures — the financing mechanism. Local government transfers → state match → federal draw → payment back to public hospital.
- **State Directed Payments (SDP):** States direct MCOs to pay providers at specific rates above capitation. Growing rapidly. CMS approval required; filings are public.
- **LIP:** Low Income Pool — Florida-specific name, but analogous programs exist in TX, CA, NY. Negotiated in 1115 waivers; can be $1B+/year in large states.
- **DSRIP:** Delivery System Reform Incentive Payments — NY, CA, TX had major programs. Most wound down, but historical data remains valuable.
- **GME:** Direct GME covers resident salary/benefits; Indirect GME covers teaching cost add-on. Both flow through Medicaid for teaching hospitals.

**The killer product this enables:** "All-in Medicaid rate" for any hospital = base rate + DSH + UPL/IGT + SDP + LIP/DSRIP + GME. No platform shows this in one place.

### 5.3 Terminal B CMS Supplemental Data (already ingested)
15 datasets / 9.3M rows via `cms_data.py` and `ncci_scraper.py`:
- FMAP rates, managed care enrollment, adult/child Core Set quality measures, DSH allotments
- NCCI edits (2.5M pairs), HCPCS Level II codes (8,623), 1115 waivers (647 via Playwright)
- Drug rebate products, NADAC, SDUD, CMS-64

### 5.4 Long-Term Services & Supports (LTSS) / HCBS ← SIGNIFICANT GAP

LTSS is ~40% of Medicaid spending. The data exists and is largely Ring 0.

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| CMS-64 Schedule B (HCBS) | HCBS expenditure by waiver authority by state | CSV | Quarterly | **P1** |
| 1915(c) Waiver Utilization & Expenditure | Participants, expenditure, services by waiver | CMS reports | Annual | **P1** |
| HCBS Quality Measures (CMS national framework) | 23 measures across 5 domains; state-level reporting | Web/Excel | Annual | **P1** |
| HCBS Waitlist Data | State-reported 1915(c) waitlists — people waiting, avg wait time | KFF/state | Annual | **P1** |
| Nursing Facility Cost Reports (CMS-2540) | NF-level costs, payer mix, staffing, beds | CSV | Annual | **P1** |
| Five-Star Quality Rating (Care Compare) | NF ratings: health inspections, staffing, quality measures | API/CSV | Monthly | **P1** |
| Payroll-Based Journal (PBJ) staffing data | NF-level staffing hours by employee type per quarter | CSV | Quarterly | **P1** |
| Direct Support Workforce data (PHI/ANCOR) | DSW wages, vacancy rates, turnover — workforce crisis data | PDF/survey | Annual | **P1** |
| Minimum Data Set (MDS) facility-level aggregates | Resident acuity, functional status, diagnoses | CSV | Quarterly | **P2** |
| 1915(k) Community First Choice utilization | CFC option — attendant services, state-by-state | CMS reports | Annual | **P2** |
| PACE enrollment & spending | Programs of All-Inclusive Care for the Elderly | CMS reports | Annual | **P2** |
| Money Follows the Person outcomes | MFP transition data, rebalancing metrics | CMS reports | Annual | **P2** |

> **Note:** 700,000+ people are on HCBS waitlists nationally. This is a measurable, documented access crisis. Aradune should surface it prominently.

### 5.5 Managed Care Data

| Source | Description | Priority |
|--------|-------------|----------|
| MCO Contracts (all states) | Capitation methodology, network requirements, quality provisions | P1 |
| MCO Rate Certifications | Actuarial capitation rates, risk adjustment, trend factors | P1 |
| Managed Care MLR Reports | Per-MCO medical loss ratio, remittance | P1 |
| MCPAR Reports | Plan-level enrollment, network adequacy, grievances, quality | P2 |
| Hospital Price Transparency MRFs | Medicaid MCO negotiated rates by hospital | P2 |
| Encounter data completeness by MCO | T-MSIS encounter reporting quality by plan | DQ Atlas | P1 |

### 5.6 Hospital Quality & Value-Based Programs ← NOT IN ARADUNE YET

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| CMS Care Compare (hospitals) | Star ratings, mortality, readmissions, safety, patient experience | API/CSV | Quarterly | **P1** |
| Inpatient Quality Reporting (IQR) | Hospital-level clinical quality measures | CMS API | Quarterly | **P1** |
| Hospital Value-Based Purchasing (VBP) | Payment adjustment scores: clinical outcomes, efficiency, patient experience | CSV | Annual | **P1** |
| Hospital Readmissions Reduction Program (HRRP) | Excess readmissions ratio and penalty amounts by hospital | CSV | Annual | **P1** |
| Hospital-Acquired Condition (HAC) Reduction | HAC scores and Medicare payment penalties by hospital | CSV | Annual | **P2** |
| Joint Commission accreditation status | Accreditation type and status by facility | API | Ongoing | **P2** |

### 5.7 Behavioral Health ← UNDERREPRESENTED

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| SAMHSA NSDUH | State-level MH disorder and SUD prevalence, treatment utilization | CSV | Annual | **P1** |
| SAMHSA Block Grant Expenditure Reports | State mental health and SUD block grant spending | Excel | Annual | **P1** |
| Psychiatric bed capacity (SAMHSA) | Inpatient psychiatric and crisis beds per capita by state | CSV | Annual | **P1** |
| 1115 IMD Waiver utilization | States using IMD exclusion waivers for BH treatment | CMS reports | Annual | **P1** |
| HRSA Behavioral Health HPSA designations | BH-specific shortage area designations | API | Ongoing | **P1** |
| Crisis stabilization capacity | Mobile crisis, crisis stabilization units by state | State reports | Annual | **P2** |

### 5.8 Policy Document Corpus

| Source | Format | Cadence | Priority |
|--------|--------|---------|----------|
| State Plan Amendments (SPAs) | PDF | Continuous | P1 |
| 1115 / 1915(b)(c)(k) Waivers | PDF | Continuous | P1 |
| CMS Informational Bulletins (CIBs) | PDF/HTML | Continuous | P1 |
| State Health Official (SHO) Letters | PDF | Continuous | P1 |
| Federal Register (Medicaid rules) | API | Continuous | P1 |
| MACPAC Reports | PDF | Quarterly | P2 |
| OIG / GAO Reports | PDF | Ongoing | P2 |

### 5.9 Pharmacy (Deeper)

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| NADAC | National Average Drug Acquisition Cost | CSV | Weekly | ✅ via Terminal B |
| State Drug Utilization Data (SDUD) | State × quarter × NDC utilization | CSV + API | Quarterly | ✅ via Terminal B |
| 340B covered entity data (HRSA) | Hospitals and health centers in 340B program | CSV | Quarterly | P1 |
| State MAC prices | State Medicaid Maximum Allowable Cost lists | State portals | Varies | P2 |
| Unit Rebate Amounts (URA) | Medicaid drug rebate amounts by NDC | CMS | Annual | P2 |

### 5.10 Children's Health / CHIP / EPSDT

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| CHIP enrollment & expenditure | State-level CHIP-specific data (separate from Medicaid) | CMS/MBES | Quarterly | P1 |
| EPSDT Participation Reports (CMS-416) | EPSDT screening rates, services rendered by age group | CMS | Annual | P1 |
| Children's Core Set measures | Pediatric-specific quality measures | CMS | Annual | P1 ✅ via Terminal B |
| Title V MCH Block Grant data | Maternal and child health spending and outcomes | HRSA | Annual | P2 |

### 5.11 Eligibility, Enrollment & Unwinding

| Source | Description | Format | Cadence | Priority |
|--------|-------------|--------|---------|----------|
| Medicaid Unwinding / Redetermination data | State-level outcomes: renewals, terminations, pending | CMS dashboard | Monthly | P1 |
| KFF Medicaid eligibility policy tracker | State policy choices: expansion status, income thresholds, MAGI rules | Web | Ongoing | P1 |
| Ex parte renewal rates | Share of renewals completed without member contact | CMS reports | Quarterly | P2 |
| Presumptive eligibility utilization | State use of hospital/FQHC presumptive eligibility | CMS reports | Annual | P2 |

### 5.12 Economic & Contextual Data (Ring 0.5 — Essential for Forecasting)

| Source | Use Case | Cadence | Priority |
|--------|----------|---------|----------|
| BLS OEWS | Healthcare occupation wages by state | Annual | P0 |
| BLS CPI (medical care) | Rate decay / real-value analysis | Monthly | P0 |
| BLS LAUS Unemployment | Primary caseload driver | Monthly | P0 |
| Census/ACS | Poverty rates, income, coverage by state/county | Annual | P0 |
| FRED | GDP, personal income, poverty, enrollment trends | Monthly | P0 |
| HUD Housing Cost Data | Cost-of-living / HCBS wage adequacy | Annual | P1 |
| USDA Food Insecurity | County-level food insecurity | Annual | P1 |
| HRSA HPSA / MUA | Health professional shortage areas | Ongoing | P1 |
| CDC PLACES / BRFSS | County-level health outcomes | Annual | P2 |
| Maternal/infant mortality (CDC WONDER) | Outcome context for rate adequacy | Annual | P2 |
| Opioid prescribing / overdose (CDC) | SUD service demand forecasting | Annual | P2 |

### 5.13 Quality & Outcomes

| Source | Description | Priority |
|--------|-------------|----------|
| DQ Atlas (CMS) | State-level T-MSIS data quality assessments | P0 — always check before publishing |
| Adult / Child Core Set Measures | State-level HEDIS/CAHPS measures for Medicaid MCOs | P1 ✅ via Terminal B |
| Medicaid Scorecard | State-level quality measures, per capita expenditures | P1 |
| HRSA UDS | FQHC patient/payer mix, services, quality measures | P2 |
| CMS Open Payments | Manufacturer payments to physicians, 16M+ records/yr | P2 |


## 6. Caseload & Expenditure Forecasting

Every state Medicaid agency does this manually in Excel or contracts it to actuaries for $200K+. Aradune will build the first open, transparent, multi-state forecasting system — and publicly track ML model accuracy against actual outcomes. No one else does this.

### 6.1 What We're Forecasting

**Enrollment / caseload:**
- Total enrollment by state by month
- Enrollment by eligibility group (children, adults, aged, disabled, expansion)
- Managed care vs. FFS enrollment
- New enrollments, disenrollments, net change

**Expenditure:**
- Total Medicaid spending by state by quarter
- Spending by service category (inpatient, outpatient, pharmacy, HCBS, MCO capitation)
- Federal vs. state share (FMAP-adjusted)
- Per-enrollee expenditure by eligibility group

**Key drivers:**
- Unemployment rate (primary economic trigger)
- Poverty rate and income distribution
- Age/disability demographics
- Policy changes (expansion, work requirements, redeterminations)
- Rate changes (directly affect expenditure)
- FMAP rates

### 6.2 Forecasting Phases

**Phase 1 — Baseline (ship first):**
ARIMA/ETS on historical enrollment and expenditure per state. Simple regression: enrollment ~ unemployment + poverty + policy_dummy. Display forecast + confidence intervals on state profiles.

**Phase 2 — Driver-based:**
Add economic covariates from FRED/BLS/Census. Incorporate policy change dummies. Publish elasticity estimates ("1 point increase in unemployment → X% enrollment increase in this state").

**Phase 3 — ML with public performance tracking:**
Gradient boosting / random forest on cross-state panel data. Track MAPE against actuals as new data arrives. Version models — never overwrite, always append. Public **model leaderboard**: which algorithm has performed best per state over rolling 12/24/36-month windows. Allow user-submitted model specifications.

**Phase 4 — Ensemble + scenarios:**
Blend top performers. Scenario builder: "What if unemployment rises 2 points?" → enrollment + expenditure impact with comparable state precedents.

### 6.3 Forecast Schema (additions to unified schema)

```sql
CREATE TABLE forecast_enrollment (
    state_code VARCHAR(2), eligibility_group VARCHAR,
    forecast_date DATE, run_date DATE, model_id VARCHAR,
    point_estimate INTEGER, lower_80 INTEGER, upper_80 INTEGER,
    lower_95 INTEGER, upper_95 INTEGER,
    PRIMARY KEY (state_code, eligibility_group, forecast_date, model_id)
);

CREATE TABLE forecast_expenditure (
    state_code VARCHAR(2), service_category VARCHAR,
    fiscal_year INTEGER, fiscal_quarter INTEGER,
    run_date DATE, model_id VARCHAR,
    point_estimate DECIMAL(15,2), lower_80 DECIMAL(15,2), upper_80 DECIMAL(15,2),
    PRIMARY KEY (state_code, service_category, fiscal_year, fiscal_quarter, model_id)
);

CREATE TABLE model_performance (
    model_id VARCHAR, state_code VARCHAR(2), target_variable VARCHAR,
    forecast_horizon_months INTEGER, evaluation_date DATE,
    mape DECIMAL(8,4), rmse DECIMAL(15,2), bias DECIMAL(15,2),
    n_observations INTEGER,
    PRIMARY KEY (model_id, state_code, target_variable, forecast_horizon_months, evaluation_date)
);

CREATE TABLE economic_indicators (
    state_code VARCHAR(2), indicator_name VARCHAR, reference_area VARCHAR,
    period_date DATE, value DECIMAL(12,4), source VARCHAR,
    PRIMARY KEY (state_code, indicator_name, reference_area, period_date)
);
```

---

## 7. User Data Upload & Connection

This is the feature that creates the deepest moat. A state analyst uploads their internal forecast and sees it against Aradune's national layer. An MCO actuary uploads a rate certification and Claude compares it to all other states. A hospital CFO connects their cost report and gets Medicaid underpayment quantified in seconds.

### 7.1 What Users Can Upload
- **Fee schedules** (any state, any format) → auto-parsed and normalized against national database
- **Caseload/expenditure projections** → overlaid against Aradune forecasts and actuals
- **Cost reports / financial data** → Medicaid underpayment analysis
- **MCO encounter data** (aggregated/de-identified) → network gap analysis
- **Custom rate proposals** → instant fiscal impact modeling
- **Any CSV/Excel/PDF** → Claude parses and connects to relevant Aradune data

### 7.2 Claude as the Bridge
1. **Ingest & understand**: Claude reads the file, identifies what it is, maps to unified schema
2. **Contextualize**: Claude surfaces the most relevant Aradune data alongside the user's data
3. **Analyze**: User asks natural language questions; Claude queries both datasets simultaneously
4. **Generate**: Claude produces reports combining both sources

### 7.3 Privacy Rules
- User-uploaded data is **session-scoped by default** — not persisted, not shared
- Optional persistent private workspace: saved uploads, encrypted, user-only
- Aggregated benchmarks use only Aradune's public layer
- No PHI without a BAA. Build for Ring 0/1 first.

### 7.4 Claude Code Integration (Institutional Tier)
- **Aradune MCP server**: exposes Aradune's data layer as MCP tools Claude Code can call
- Users write Claude Code sessions querying Aradune's DuckDB + their own data
- Outputs pushed back to user's Aradune workspace
- Turns Aradune from a product into a platform

---

## 8. The Claude-Native Interface

### 8.1 Interface Modes

**Explore Mode (free):**
Dashboards, rate lookups, state profiles, guided queries. No AI required — fast, static, works for journalists and quick lookups.

**Analyze Mode (paid):**
Natural language query against the full Aradune data warehouse. Claude runs NL2SQL against DuckDB, retrieves results, formats and explains them. Cites specific source records — not generated from training data.

**Build Mode (paid, institutional):**
User uploads data or connects a source. Claude analyzes in context of Aradune's national layer. Produces structured deliverables: compliance reports, rate comparisons, fiscal impact analyses, SPA templates. Claude Code-compatible.

### 8.2 Query Routing

```
User query
    ├── Pre-computed lookup → DuckDB-WASM + CDN Parquet (sub-second)
    ├── Analytical query   → Server-side DuckDB REST API (seconds)
    └── Policy reasoning   → RAG + Claude pipeline (5–15 seconds)
```

### 8.3 Model Routing

| Query type | Model | Cost/query |
|-----------|-------|-----------| 
| Route classification, simple lookups | claude-haiku-4-5-20251001 | ~$0.004 |
| NL2SQL, RAG, standard analysis | claude-sonnet-4-6 | ~$0.03–0.06 |
| SPA drafting, CPRA narrative, AHEAD modeling | claude-opus-4-6 | ~$0.28 |

Use **prompt caching** (90% input cost reduction for repeated system context) and **batch API** (50% discount for non-real-time work).

---

## 9. Automated Data Pipeline System

### 9.1 Core Pattern (every source implements all five steps)

```python
def fetch_raw(source_config) -> bytes | Path:
    """Download/scrape. Use ETag/Last-Modified for change detection."""

def parse(raw) -> list[dict]:
    """Per-source parsers. PDF → pdfplumber → Claude API normalization."""

def validate(parsed) -> ValidationResult:
    """Hard stops vs. soft flags (see below)."""

def normalize(validated) -> list[dict]:
    """Map to unified schema. Source provenance (URL + date) on every record."""

def load(normalized, db_conn) -> LoadResult:
    """Upsert with version tracking. Create S3 snapshot. Record hash."""
```

### 9.2 Orchestration: Dagster
Asset-centric model maps to datasets-as-assets. Built-in lineage tracking. Partition model: `state × year × quarter`. Strong DuckDB integration.

### 9.3 Scheduling

| Source | Frequency |
|--------|-----------|
| NPPES NPI Registry | Weekly |
| NADAC Pharmacy | Weekly |
| Federal Register / CIBs / SHOs | Continuous |
| SPAs / Waivers | Continuous |
| BLS Unemployment, CPI, FRED | Monthly |
| Managed Care Enrollment | Monthly |
| T-MSIS / SDUD / MBES-CBES | Quarterly |
| Medicare PFS RVU | Annual + quarterly updates |
| State Fee Schedules | Annual (change-detected) |
| HCRIS Cost Reports, BLS OEWS, ACS | Annual |

### 9.4 Operational Controls

**Pause/Resume:**
```yaml
# data/pipeline_config.yaml
pipeline_paused: false
sources:
  FL_fee_schedule:
    paused: false
```

**Versioning & Rollback:**
- Every load snapshots to: `s3://aradune-datalake/snapshots/{source}/{YYYY-MM-DD}/`
- `ingestion_log` table: source, version, timestamp, row_count, hash, status, snapshot_path
- One-command rollback: `aradune rollback --source FL_fee_schedule --version 2026-01-15`
- Forecasting models: **never deleted** — always append, track performance over time

**Validation Gates:**
- Hard stops (do not load): rate changed >90%, code count dropped >20%, schema mismatch
- Soft flags (load with warning): rate unchanged >24 months, new codes without description, rate >3 SDs from national mean

**Notifications (Slack/email webhook):**
New data loaded · Validation failures · Rollback events · Weekly digest

**Admin Dashboard:**
Visual status of all 51 × N sources: last checked, last changed, current version, health. One-click pause/resume, rollback UI, validation alert queue.

### 9.5 State Fee Schedule Registry (YAML)

```yaml
# data/sources/fee_schedules.yaml
states:
  FL:
    url: "https://ahca.myflorida.com/medicaid/..."
    format: "pdf"
    parser: "florida_fee_schedule_parser"
    update_frequency: "annual"
    last_checked: "2026-03-01"
    last_changed: "2025-10-15"
    paused: false
    tier: 1
    notes: "Rates cannot have both facility AND PC/TC split. Special: 46924, 91124, 91125."
  TX:
    url: "https://public.tmhp.com/FeeSchedules"
    format: "excel"
    parser: "texas_tmhp_parser"
    update_frequency: "monthly"
    tier: 1
```

**Tier definitions:**
- Tier 1 (~15–20 states): Clean CSV/XLSX. Fully automated.
- Tier 2 (~15–20 states): Structured but messy — web portals, multiple files, custom parsing.
- Tier 3 (~10–15 states): PDF-only or fragmented. pdfplumber + Claude extraction.

---

## 10. Analytical Products (Structured Outputs)

### 10.1 CPRA Auto-Generation (42 CFR 447.203) — LIVE
**Status:** Live at `/#/cpra`. 34 states. Deadline July 1, 2026.
Medicaid vs. Medicare rates by primary care / OB/GYN / outpatient MH/SUD. Exports PDF, Excel, HTML. No existing product automates this. This is the wedge.

### 10.2 Rate Adequacy Analysis
**Input:** State + service category
**Output:** Comparison to Medicare, neighboring states, national percentiles. Wage adequacy overlay. Rate decay trends. Access indicators (HRSA shortage areas). Quality correlation.
**Replaces:** $200K–$500K consulting rate adequacy study.

### 10.3 AHEAD / Global Budget Modeling — LIVE (basic)
**Status:** Live at `/#/ahead` (simplified from Meridian's 12 engines).
**Full build:** State + hospital(s) → current revenue by payer (HCRIS), Medicaid volume/rate analysis, quality baseline, projected global budget under CMS parameters, sensitivity analysis, peer comparison.

### 10.4 SPA Impact Analyzer
**Input:** Proposed rate change (state, affected codes, magnitude, effective date)
**Output:** Fiscal impact (T-MSIS utilization × rate delta), 80% Medicare threshold and 4% SFY cap compliance check, comparable state actions from SPA archive, historical precedent, template SPA language.

### 10.5 Caseload & Expenditure Forecast
**Input:** State + projection horizon + scenario assumptions
**Output:** Enrollment forecast by eligibility group with confidence intervals; expenditure forecast by service category; economic driver decomposition; model performance history; comparison to budget office projections.

### 10.6 Network Adequacy / MCO Gap Analysis
**Input:** State + MCO
**Output:** Provider-to-beneficiary ratios by specialty/geography, appointment wait time proxy metrics, comparison to CMS standards and peer MCOs, gap identification, HRSA shortage area overlay.

### 10.7 All-In Medicaid Rate Analysis (Hospital) ← NEW PRODUCT
**Input:** Hospital CCN or name
**Output:** Base Medicaid rate + DSH allotment + UPL/IGT supplement + State Directed Payments + LIP/DSRIP + GME (direct + indirect) = **total all-in Medicaid revenue per case**. Medicaid-to-cost ratio using HCRIS. Rate comparison to Medicare and peer hospitals. National percentile. No other platform shows this in one place.

### 10.8 Supplemental Payment Landscape (State)
**Input:** State
**Output:** Full map of supplemental payment programs in the state — DSH allotment, UPL demonstrations, IGT/CPE mechanics, SDP approvals, any LIP/DSRIP/UC pool. Total supplemental payments as % of base Medicaid. Trend over time. Comparison to peer states.

### 10.9 LTSS Access Dashboard
**Input:** State (or national view)
**Output:** HCBS waitlist size and trend, waiver slots vs. people waiting, NF Five-Star distribution, direct support workforce vacancy rates, rebalancing ratio (HCBS vs. NF spending), comparison to national averages.

### 10.10 Medicaid Underpayment Quantification (Hospital)
**Input:** Hospital CCN or name
**Output:** Medicaid-to-cost ratio (HCRIS), Medicaid volume (T-MSIS), base rate comparison to Medicare and neighboring states, supplemental payment overlay (DSH/UPL/SDP), all-in effective rate vs. cost, national peer comparison.

### 10.11 Border Arbitrage / Rate Decay / Reverse Cash Flows
Original Aradune frameworks — now powered by national data across all 50 states.

---

## 11. HIPAA & Data Sensitivity Architecture

Build in concentric rings. Launch from Ring 0/1. Expand as state relationships develop.

| Ring | Data Type | HIPAA | Examples |
|------|-----------|-------|---------|
| **Ring 0** | Public regulatory | None | Fee schedules, RVUs, SPAs, waivers, provider directories |
| **Ring 0.5** | Economic/contextual | None | BLS, FRED, Census, HRSA — informs Medicaid but isn't Medicaid |
| **Ring 1** | Aggregated/de-identified | Minimal | DOGE open dataset, state-published utilization counts |
| **Ring 2** | Provider-level (no patient info) | Low — may need BAA | Provider billing volumes, network participation |
| **Ring 3** | Claims/encounter data | Full HIPAA | T-MSIS/TAF via ResDAC, state claims warehouses |

**Current Aradune is Ring 0/0.5/1. Stay here until BAA infrastructure, SOC 2 Type II, and HITRUST CSF are in place.**

---

## 12. Technical Stack

### Current (production, March 2026)
```
Frontend:       React 18 + TypeScript + Vite
Visualization:  Recharts
Routing:        Hash-based in Platform.tsx
Hosting:        Vercel Pro
Backend:        3 Vercel serverless functions (api/)
Design:         #0A2540 ink · #2E6B4A brand · #C4590A accent
                SF Mono for numbers · Helvetica Neue for body · No Google Fonts
Data store:     DuckDB-WASM (browser-side) · DuckDB (Terminal B, aradune_cpra.duckdb)
                17.5GB T-MSIS DuckDB (local, currently empty — needs R pipeline)
AI:             Claude API via Vercel serverless (api/chat.js)
Pipeline:       Python (cpra_engine.py, cms_data.py, ncci_scraper.py) — Terminal B
                R (tmsis_pipeline_duckdb.R, hcpcs_reference.R) — T-MSIS processing
PDF extraction: pdfplumber → Claude API normalization
```

### Target (next 6 months)
```
Orchestration:  Dagster (replaces manual scripts)
Backend:        Python FastAPI
Database:       PostgreSQL + pgvector (RAG + relational metadata)
Analytics:      DuckDB (queries) + Hive-partitioned Parquet on S3
Vector index:   pgvector + Voyage-3-large embeddings
NL2SQL:         Vanna (open-source, DuckDB native, 200+ Medicaid examples)
Maps:           Leaflet or Mapbox
Forecasting:    statsforecast · scikit-learn · Prophet / LightGBM
ML tracking:    MLflow
```

### Infrastructure Costs

| Stage | Monthly |
|-------|---------|
| Current (static + small backend) | $200–400 |
| Growth (full backend + pipeline) | $900–1,700 |
| Scale (institutional clients + 5B rows) | $2,000–5,000 |

Use AWS Activate credits ($10K–$100K available for startups).

### DuckDB Rules (always follow)
- `DECIMAL(10,2)` for rates — never `FLOAT`
- `DATE` for effective dates — never string
- Skip primary keys during bulk load; add after
- Query Parquet directly: `SELECT * FROM read_parquet('s3://aradune-datalake/rates/state=FL/year=2025/*.parquet', hive_partitioning=true)`
- DuckDB-WASM for browser queries <5M rows; route larger to server-side API
- Pre-compute aggregated rollup tables for common dashboard queries

---

## 13. Database Schema (Core Tables)

```sql
-- Dimension tables
CREATE TABLE dim_state (
    state_code VARCHAR(2) PRIMARY KEY, state_name VARCHAR, region VARCHAR,
    expansion_status BOOLEAN, fmap DECIMAL(5,4),
    managed_care_model VARCHAR, medicaid_agency_name VARCHAR, agency_url VARCHAR
);

CREATE TABLE dim_procedure (
    cpt_hcpcs_code VARCHAR(10) PRIMARY KEY, description VARCHAR,
    category_447 VARCHAR,    -- 'primary_care', 'obgyn', 'mh_sud', 'hcbs', 'other'
    betos_code VARCHAR, is_em_code BOOLEAN,
    work_rvu DECIMAL(8,4), nonfac_pe_rvu DECIMAL(8,4),
    fac_pe_rvu DECIMAL(8,4), mp_rvu DECIMAL(8,4), status_indicator VARCHAR(2)
);

CREATE TABLE dim_medicare_locality (
    locality_id VARCHAR(10) PRIMARY KEY, carrier VARCHAR,
    state_code VARCHAR(2), locality_name VARCHAR,
    gpci_work DECIMAL(6,4), gpci_pe DECIMAL(6,4), gpci_mp DECIMAL(6,4),
    urban_rural VARCHAR
);

-- Fact tables
CREATE TABLE fact_medicaid_rate (
    state_code VARCHAR(2), cpt_hcpcs_code VARCHAR(10), modifier VARCHAR(10),
    provider_type VARCHAR, population VARCHAR, geographic_area VARCHAR,
    effective_date DATE, medicaid_rate DECIMAL(10,2),
    source_file VARCHAR, last_updated DATE, dq_flag VARCHAR,
    PRIMARY KEY (state_code, cpt_hcpcs_code, modifier, provider_type, population, geographic_area, effective_date)
);

CREATE TABLE fact_rate_comparison (
    state_code VARCHAR(2), cpt_hcpcs_code VARCHAR(10), category_447 VARCHAR,
    population VARCHAR, provider_type VARCHAR, geographic_area VARCHAR, year INTEGER,
    medicaid_rate DECIMAL(10,2), medicare_nonfac_rate DECIMAL(10,2),
    pct_of_medicare DECIMAL(6,4), claim_count INTEGER, beneficiary_count INTEGER,
    PRIMARY KEY (state_code, cpt_hcpcs_code, category_447, population, provider_type, geographic_area, year)
);

CREATE TABLE fact_expenditure_cms64 (
    state_code VARCHAR(2), category VARCHAR, subcategory VARCHAR,
    fiscal_year INTEGER, fiscal_quarter INTEGER,
    federal_share DECIMAL(15,2), state_share DECIMAL(15,2), total DECIMAL(15,2),
    PRIMARY KEY (state_code, category, subcategory, fiscal_year, fiscal_quarter)
);

CREATE TABLE fact_enrollment (
    state_code VARCHAR(2), eligibility_group VARCHAR,
    period_date DATE, enrollment_count INTEGER,
    managed_care_count INTEGER, ffs_count INTEGER, source VARCHAR,
    PRIMARY KEY (state_code, eligibility_group, period_date)
);

CREATE TABLE spa_documents (
    state_code VARCHAR(2), spa_id VARCHAR,
    submission_date DATE, effective_date DATE, category VARCHAR,
    summary_text TEXT, full_text TEXT, status VARCHAR, source_url VARCHAR,
    PRIMARY KEY (state_code, spa_id)
);

CREATE TABLE ingestion_log (
    id SERIAL PRIMARY KEY, source_id VARCHAR, run_timestamp TIMESTAMP,
    rows_loaded INTEGER, content_hash VARCHAR, version_label VARCHAR,
    status VARCHAR, validation_flags TEXT, snapshot_path VARCHAR
);

CREATE TABLE pipeline_alerts (
    id SERIAL PRIMARY KEY, source_id VARCHAR, alert_type VARCHAR,
    alert_message TEXT, run_timestamp TIMESTAMP, resolved BOOLEAN
);
```

---

## 14. Known Policy Rules (Always Enforce)

- **FL Medicaid: rates cannot have both a facility rate AND a PC/TC split.** Codes requiring special handling: **46924, 91124, 91125.**
- **FL production CFs:** Regular `$24.9779582769` · Lab `$26.1689186096`. The $24.9876 ad hoc CF is stale — never use for CY2026.
- **FL has 8 schedule types** in the fee schedule.
- **Medicare comparison baseline:** Always use the non-facility rate (not facility), per 42 CFR 447.203.
- **CY2025/2026 Medicare CF:** `$33.4009` (non-QPP). The `$32.3465` value is QPP-adjusted and wrong for Medicaid comparison.
- **CPRA base rates only:** No supplemental payments in the Medicaid-to-Medicare percentage.
- **CHIP excluded** from per-enrollee Medicaid calculations.
- **Minimum cell size:** n ≥ 11 for any published utilization count.
- **T-MSIS 2024 data:** Flag Nov–Dec 2024 as incomplete. Always specify service year; never say "current."
- **DQ Atlas:** Check state-level ratings before publishing any state-specific claims analysis. Flag "unusable" or "high concern" states.

---

## 14b. Security & HIPAA Architecture

### Data Sensitivity Rings

| Ring | Data Type | HIPAA | Status |
|------|-----------|-------|--------|
| **Ring 0** | Public regulatory: fee schedules, RVUs, SPAs, waivers, provider directories | None | ✅ Build here now |
| **Ring 0.5** | Economic/contextual: BLS, FRED, Census | None | ✅ Build here now |
| **Ring 1** | Aggregated/de-identified: DOGE open data, state utilization counts | Minimal | ✅ Build here now |
| **Ring 2** | Provider-level (no patient info): billing volumes, network data | Low — may need BAA | When state relationships develop |
| **Ring 3** | Claims/encounter: T-MSIS/TAF, state claims warehouses, MCO encounters | Full HIPAA | After BAA + HITRUST + SOC2 |

**Aradune stays in Ring 0/0.5/1 until BAA infrastructure, SOC 2 Type II, and HITRUST CSF are in place.**

### Technical Requirements (When HIPAA Applies)

- **Encryption:** AES-256 at rest; TLS 1.2+ in transit
- **Access controls:** RBAC + MFA; principle of least privilege; Provider A cannot see Provider B's data
- **Audit logging:** Immutable logs of all data access; retain 6–10 years per state contract terms
- **Breach notification:** 60 days under HIPAA; many state contracts require **24–72 hours**
- **Secrets:** Never hardcode API keys; use environment variables; rotate immediately if exposed
- **User data isolation:** Session-scoped by default; encrypted persistent workspace if opted in
- **Minimum cell size:** n ≥ 11 for all published utilization counts
- **Never re-host raw PHI** on Aradune infrastructure without DUA + HITRUST certification

### Business Associate Agreements

Required before handling Ring 2/3 data from any covered entity. BAA template must cover: permitted uses, minimum necessary standard, safeguard requirements, breach notification, data retention/destruction, right to audit.

### Certifications (in pursuit order)

1. **SOC 2 Type II** — Minimum for enterprise sales
2. **HITRUST CSF** — Preferred/required for state contracts
3. **StateRAMP** — For state cloud deployments
4. **FedRAMP** — Only if pursuing federal contracts directly

### HIPAA Exposure by Product Area

| Product | Exposure | Notes |
|---------|----------|-------|
| CPRA Generator | None | Public fee schedule data only |
| Cross-state rate comparisons | None | Ring 0 |
| Policy document search (SPAs/waivers) | None | Public regulatory docs |
| Rate adequacy analysis | None–Low | Aggregated public utilization counts only |
| MCO network adequacy | Low–Moderate | Provider directory data = public; encounter data = Ring 3 |
| Hospital all-in rate analysis | Low | HCRIS = public; T-MSIS aggregate = Ring 1 |
| Provider-uploaded fee schedules | None | No PHI in fee schedules |
| Provider self-reported claims/remittance | Moderate | Requires BAA with provider org |
| T-MSIS analytics tooling | High | Start with de-identified DOGE dataset; never host raw TAF |
| Fraud pattern detection | Very High | Tier 3 product — requires established state BAA relationships first |


## 15. Competitive Position

No existing platform combines: (1) comprehensive public Medicaid data aggregation, (2) cross-state analytical capabilities, (3) multi-stakeholder access, (4) real-time policy intelligence, (5) AI-powered analysis and report generation, (6) user data upload and integration, (7) caseload forecasting with public model tracking.

**Key advantages:**
- No conflict of interest — unlike Optum (owned by UnitedHealthcare) or HMAIS (consulting firm)
- Public data moat: curation + standardization + linkage + AI analysis is the value
- AI-native from day one — not bolting Claude onto a 2000s-era system
- User data integration — no other platform lets you analyze your data against the national layer
- Forecasting transparency — first platform to publicly track ML model performance against Medicaid actuals
- 447.203 timing — states need compliance help NOW; July 1, 2026 deadline is fixed

**Closest competitor:** HMAIS (Health Management Associates Information Services) — subscription service for cross-state Medicaid market data. Differentiate on: code-level data depth (vs. program-level), AI capability, user data upload, forecasting engine, and pricing.

### Startup Landscape (no direct competitors as of March 2026)

| Company | Raised | Focus | Overlap? |
|---------|--------|-------|----------|
| Fortuna Health | $22M (a16z) | "TurboTax for Medicaid" eligibility | No — enrollment |
| Unite Us | $253M+ | Social care referral | No — care delivery |
| Turquoise Health | $60M+ | Commercial price transparency | Adjacent — hospital MRFs, not Medicaid |
| Trilliant Health | $115M+ | Market analytics for health systems | Adjacent — commercial focus |
| Quest Analytics | Private | Network adequacy (time/distance only) | Adjacent — static geographic access |
| Pair Team / Cayaba / Pear Suite | Various | Value-based primary care / CHW / maternal | No — care delivery |

**Key insight:** Only 7.7% of the $101B invested in digital health 2011–2022 went to Medicaid-focused companies — and even that went to enrollment and care delivery, not rate analytics, policy intelligence, or MCO oversight. **No funded startup exists in rate analytics, fee schedule management, or Medicaid policy intelligence.**

---

## 15b. User Segments & Their Core Needs

### State Medicaid Agencies
**Pain:** 447.203 compliance by July 2026. Rate-setting done in Excel/SAS. Manual, error-prone.
**Need:** CPRA auto-generation · 80% Medicare threshold flagging · 4% SFY cap tracker · Rate change monitoring · 30-day update deadline alerts
**Pricing:** $50K–$200K/year. If qualifies as Medicaid IT expenditure → **75% Federal Financial Participation** → effective state cost $12.5K–$50K

### Medicaid Consulting Firms (Milliman, HMA, Myers & Stauffer, Mathematica, Mercer, Guidehouse, Manatt, Sellers Dorsey)
**Pain:** Cross-state rate comparison requires assembling 51 fee schedules manually. No standardized national dataset.
**Need:** All 50 states in a queryable database · Cross-state benchmarking by code · SPA/waiver tracking · CPRA as client deliverable · Rate reduction impact modeling · Branded PDF reports
**Pricing:** $50K–$250K/year enterprise license

### MCOs (Centene, Elevance, UnitedHealthcare, Molina, CVS/Aetna)
**Pain:** Rate underfunding (post-unwinding MLR rising 89→93%+) + network adequacy compliance across 40+ state portfolios
**Need:** Cross-state capitation benchmarking · SPA/waiver policy alerts · Network adequacy analytics · MCO financial benchmarking · Quality measure tracking across states
**Pricing:** $100K–$500K/year

### Hospitals & Providers
**Pain:** Medicaid underpayment ($130B nationally in 2023 per AHA). No tool shows all-in Medicaid rate including supplementals.
**Need:** All-in Medicaid rate (base + DSH + UPL + SDP + LIP + GME) · Medicaid-to-cost analysis · Fee schedule monitoring · Rate change alerts
**Pricing:** $500–$2,000/month

### Journalists, Legislative Aides, Advocacy Organizations
**Pain:** Limited to FOIA requests and manually compiled KFF/MACPAC data.
**Need:** Free dashboards · Data download · Enrollment tracking · MCO financial performance · Rate adequacy visualization
**Pricing:** Free tier. Premium at $29–$49/month.


## 16. Monetization

| Tier | Price | Target User |
|------|-------|-------------|
| Free (Aradune Open) | $0 | Journalists, advocates, legislative aides, researchers |
| Analyst | $99/month | Individual analysts, small consultants |
| Pro | $299/month | Power users, consulting teams |
| State Agency | $50K–$200K/year | State Medicaid agencies (may qualify for 75% FFP) |
| Enterprise | $50K–$500K/year | Consulting firms, MCOs, hospital systems |
| Data as a Service | $25K–$100K/year per dataset | Firms wanting bulk normalized data via API |

---

## 17. Build Priority Sequence

### Immediate (now — fix before new features)
1. Confirm CPRA white page fixed in production (`/#/cpra`)
2. Fix 6 missing CPRA states (AZ, DE, IA, NC, NY, WV) — code normalization in cpra_engine.py join
3. Reconcile conversion factor — update `medicare_pfs.py` to $33.4009
4. Delete `cpra_precomputed.json` (superseded by cpra_em.json)
5. Append `fl_methodology_addendum.md` to system prompt in `api/chat.js`

### Near-term (Weeks 1–6)
6. Nav redesign → grouped Explore/Analyze/Build dropdowns (reference `docs/AraduneMockup.jsx`)
7. Landing page redesign from `docs/AraduneMockup.jsx`
8. Pipeline ingestion framework (Section 9 pattern + ingestion_log + pipeline_alerts tables)
9. Economic indicator ingestion (BLS, FRED, Census) — foundational for forecasting
10. `dq_state_notes.json` from Terminal B → wire into CPRA DQ panel
11. Category-level summaries in `cpra_summary.json` → replace client-side computation
12. Expand CPRA state coverage — 34 → all 50 states

### Weeks 7–16
13. State fee schedule adapters — Tier 1 states (all 51 jurisdictions target)
14. Cross-state rate comparison improvements (from 42 states to 51)
15. SPA + waiver archive ingestion + search
16. RAG pipeline over policy corpus (pgvector + Voyage embeddings)
17. NL2SQL interface (Vanna + Medicaid training examples)
18. Policy Analyst auth + Stripe payment
19. Baseline caseload/expenditure forecasting (ARIMA per state)
20. User data upload + Claude analysis interface (Build Mode)
21. UX features per `docs/UX_FEATURES_SPEC.md`: glossary tooltips, nav search, state profiles, explain buttons

### Months 4–6
22. Dagster orchestration (replaces manual pipeline scripts)
23. Automated pipeline controls (pause/resume, rollback, admin dashboard)
24. AHEAD modeling tool full build (Meridian 12-engine refactor into Aradune)
25. Driver-based forecasting models (economic covariates)
26. MLflow for model version tracking + public leaderboard
27. Aradune MCP server (Claude Code integration for institutional users)
28. API access layer (institutional tier)

### Months 6–12
29. ML model ensemble + scenario builder
30. White-label / embedded analytics for institutional clients
31. Federal Register impact automation
32. Grant applications (RWJF, Commonwealth Fund, Arnold Ventures)

---

## 18. Guiding Principles for Every Session

1. **Always build to the unified schema.** No one-off scripts dumping to random formats.
2. **Validation is not optional.** Every parser validates before loading.
3. **Source provenance is not optional.** Every record traces to URL + download date.
4. **Ship ugly.** Working data for 50 states beats beautiful UI for 5 states.
5. **Federal data first.** Federal sources cover all states at once.
6. **Florida pipeline is the template.** Abstract, parameterize, replicate.
7. **PDF parsing prompts are versioned.** Build test suite with known-correct outputs.
8. **FL rate rule always enforced.** No facility + PC/TC split. Special: 46924, 91124, 91125.
9. **Forecasting models are never deleted.** Always append; track performance over time.
10. **User data is never mixed with Aradune's public layer** without explicit opt-in.
11. **Don't be CPRA-forward.** CPRA is the wedge, not the product. Build for the platform.
12. **Economic/contextual data matters.** Ingest data that informs Medicaid, not just from Medicaid.
13. **Log predictions. Compare to actuals. Publish accuracy.** Credibility compounds.
14. **Claude's job is to connect the dots.** Build the data layer so Claude can reason across it.

---

## 19. What Success Looks Like

**1–3 months:** All 50 states have rate data. Cross-state comparisons work. CPRA tool confirmed working in production. Economic data feeds live. A journalist or state staffer cites Aradune.

**3–6 months:** SPA search live. Forecasting models running for all states. Rate adequacy reports generating. First paying institutional client. User data upload in beta.

**6–12 months:** Cited in a MACPAC report or state filing. Forecast accuracy dashboard published. Multiple institutional clients. Revenue covers infrastructure costs. ML model leaderboard public.

**1–3 years:** Default reference for Medicaid data. CMS links to it. Firms license the data. State agencies use it for compliance. Seven-figure revenue. The Bloomberg Terminal of Medicaid — not just a tagline.

---

*Sources: CMS-2442-F final rule · 42 CFR 447.203 · T-MSIS Data Guide V4.0 · ResDAC TAF documentation · DQ Atlas · MACPAC MACStats Feb 2026 · KFF State Health Facts · McDermott+ DOGE dataset analysis · Terminal A/B handoff documents · March 2026*
