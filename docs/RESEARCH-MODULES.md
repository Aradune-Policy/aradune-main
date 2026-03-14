# Aradune Research Modules — Planning & Wording Guide

> 10 research modules that exploit the unique intersections of Aradune's 700-table Medicaid data lake to answer questions that existing research has not been able to fully address.

---

## Why These Modules Matter

Most Medicaid research uses 1-2 federal datasets in isolation. Academic papers pull a single CMS file. State agencies see their own data but cannot benchmark. MACPAC and KFF analyze policy slices. CMS itself has the data but it sits in dozens of siloed offices.

Aradune has 700 tables from 60+ federal sources with a common state-level join key, queryable in the same DuckDB instance. These modules exploit that structural advantage to answer questions that require cross-domain evidence.

---

## Module 1: Rate-Quality Nexus

**Route:** `/api/research/rate-quality/*`
**Frontend:** `src/tools/research/RateQualityNexus.tsx`
**URL:** `/#/research/rate-quality`

### Research Question
Does paying Medicaid providers more (as a percentage of Medicare) actually improve access and quality outcomes?

### Why It Matters
CMS finalized the Medicaid Access Rule (42 CFR 447.203) requiring states to prove payment adequacy. MACPAC has been asked by Congress for years to answer this. Nobody has had procedure-level rate data + quality measures + workforce supply in the same queryable environment. This module provides the cross-state empirical evidence base.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_rate_comparison` | Medicaid vs Medicare rates by procedure | 302K |
| `fact_quality_core_set_2024` | 57 quality measures by state | 11K |
| `fact_quality_core_set_combined` | Quality measures 2017-2024 | 36K |
| `fact_hpsa` | Health Professional Shortage Areas | 69K |
| `fact_bls_wage` | Healthcare worker wages by state | varies |
| `fact_enrollment` | Total enrollment by state | varies |
| `fact_mc_enrollment_summary` | MC penetration by state | 513 |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Compute state-level average `pct_of_medicare` from `fact_rate_comparison` (overall + by E/M category)
2. Join to `fact_quality_core_set_2024` on `state_code` — correlate rate levels with access-sensitive quality measures (well-child visits, prenatal care, ED utilization)
3. Join to `fact_hpsa` aggregated by state — do higher-paying states have fewer shortage designations?
4. Control for MC penetration (from `fact_mc_enrollment_summary`) and enrollment size
5. Layer BLS wage data to test whether rates track workforce supply

### Tabs
1. **Rate-Quality Correlation** — Scatter plot: avg % of Medicare (x) vs quality measure rate (y), one dot per state. Selectable quality measure. Correlation coefficient + regression line. Color by region.
2. **Access Impact** — States ranked by HPSA count per capita. Overlay median Medicaid rate level. Bar chart showing inverse relationship.
3. **Workforce Connection** — BLS healthcare wages vs Medicaid rates vs quality. Triple-axis analysis. Which states pay workers well but reimburse Medicaid poorly?
4. **State Detail** — Searchable table: state, avg rate %, quality rank, HPSA count, workforce wage, MC penetration. Sortable on all columns.

### Header Copy
- **Title:** Rate-Quality Nexus
- **Badge:** CPRA + Core Set + HPSA + BLS
- **Subtitle:** Does paying providers more improve Medicaid outcomes? Cross-domain analysis of rate adequacy, quality measures, workforce supply, and provider access across all states.

### Source Line
Sources: CMS Medicaid Rate Comparison (CY2025) . Medicaid & CHIP Core Set (2024) . HRSA HPSA Designations . BLS Occupational Employment (2024)

---

## Module 2: Managed Care Value Assessment

**Route:** `/api/research/mc-value/*`
**Frontend:** `src/tools/research/ManagedCareValue.tsx`
**URL:** `/#/research/mc-value`

### Research Question
Is Medicaid managed care actually saving money and improving quality compared to fee-for-service, or is it primarily transferring risk and profit to MCOs?

### Why It Matters
States moved ~70% of Medicaid enrollment into managed care on the promise of cost savings and quality improvement. This is a $500B+ annual question. The evidence base for MC effectiveness is shockingly thin because nobody has unified cross-state fiscal, quality, enrollment, and MCO financial data.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_mc_enrollment_summary` | MC penetration by state/year | 513 |
| `fact_mco_mlr` | MCO medical loss ratios | 2,282 |
| `fact_macpac_spending_per_enrollee` | Per-enrollee spending by state | varies |
| `fact_cms64_multiyear` | Total expenditure FY2018-2024 | 118K |
| `fact_quality_core_set_combined` | Quality measures 2017-2024 | 36K |
| `fact_enrollment` | Enrollment trends | varies |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Tier states by MC penetration % (High >80%, Medium 50-80%, Low <50%)
2. Compare per-enrollee spending across tiers (from MACPAC data)
3. Compare quality Core Set measure averages across tiers
4. Analyze MCO MLR — what share of capitation goes to care vs. admin/profit?
5. Trend analysis: as states increased MC penetration over time, did spending/quality change?

### Tabs
1. **Penetration vs Spending** — Scatter: MC penetration % (x) vs per-enrollee spending (y). Regression line. State labels. Color by expansion status.
2. **MCO Financials** — MLR distribution by state. Bar chart of avg MLR. Highlight states where MLR < 85% (profit retention). Total remittance amounts.
3. **Quality by MC Tier** — Group states by MC penetration tier. Compare avg quality measure rates for key access measures. Box plots or grouped bars.
4. **Trend Analysis** — Time series: as MC penetration grew 2017-2024, did per-enrollee spending and quality measures move? Dual-axis line chart.

### Header Copy
- **Title:** Managed Care Value Assessment
- **Badge:** MCO MLR + CMS-64 + Core Set + Enrollment
- **Subtitle:** Evaluating whether Medicaid managed care delivers on its promise of lower costs and better outcomes. Cross-state analysis of MCO finances, spending efficiency, and quality performance.

### Source Line
Sources: CMS MCO MLR Reports (PY2018-2020) . MACPAC Per-Enrollee Spending . CMS-64 Expenditure (FY2018-2024) . Medicaid Core Set (2017-2024)

---

## Module 3: Opioid Treatment Gap

**Route:** `/api/research/treatment-gap/*`
**Frontend:** `src/tools/research/TreatmentGap.tsx`
**URL:** `/#/research/treatment-gap`

### Research Question
Where is the gap between SUD/opioid prevalence and treatment access widest, and is it a capacity problem, a reimbursement problem, or a coverage problem?

### Why It Matters
Billions are spent on the opioid crisis but nobody has mapped the full demand-supply-spending pipeline at state level in a single view. This module identifies states where Medicaid is failing to connect people to available treatment.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_nsduh_prevalence` | SUD/MH prevalence by state | 5.9K |
| `fact_opioid_prescribing` | Opioid prescribing rates | 539K |
| `fact_mh_facility` | MH/SUD treatment facilities | 28K |
| `fact_sdud_2025` | Drug utilization (filter for MAT drugs) | 2.6M |
| `fact_teds_admissions` | Treatment admissions | varies |
| `fact_block_grant` | SAMHSA block grant funding | varies |
| `fact_bh_by_condition` | BH conditions by state | 4.2K |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. From NSDUH: extract OUD prevalence + "needing but not receiving treatment" rates by state
2. From MH facility directory: count SUD treatment facilities per capita by state (filter for offers_su, offers_detox, otp)
3. From SDUD: identify MAT drugs (buprenorphine NDCs, naloxone, methadone) and sum Medicaid spending by state
4. Compute "Treatment Gap Score" = prevalence rank - treatment capacity rank - MAT spending rank
5. Overlay block grant funding: are high-prevalence states getting proportional SAMHSA funds?

### Tabs
1. **Demand-Supply Map** — States ranked by OUD prevalence. Overlay: SUD treatment facility count per 100K population. Gap highlighted where prevalence is high but capacity is low.
2. **MAT Utilization** — Medicaid spending on MAT drugs by state (from SDUD, filtered to buprenorphine/naloxone NDCs). Per-enrollee MAT spending. Compare to OUD prevalence.
3. **Prescribing Patterns** — Opioid prescribing rates by state. Trend over time. Correlation with OUD prevalence (chicken-and-egg analysis).
4. **Funding Alignment** — SAMHSA block grant $ per capita vs SUD prevalence. Are federal dollars following the need?

### Header Copy
- **Title:** Opioid Treatment Gap
- **Badge:** NSDUH + SDUD + N-SUMHSS + TEDS
- **Subtitle:** Mapping the full demand-supply-spending pipeline for opioid use disorder treatment. Identifies states where prevalence outstrips treatment capacity, MAT access, and federal funding.

### Source Line
Sources: SAMHSA NSDUH (2023-2024) . State Drug Utilization Data (2025) . N-SUMHSS Facility Directory . TEDS Admissions . SAMHSA Block Grants

---

## Module 4: Safety Net Stress Test

**Route:** `/api/research/safety-net/*`
**Frontend:** `src/tools/research/SafetyNetStress.tsx`
**URL:** `/#/research/safety-net`

### Research Question
Which states have the most stressed safety net — financially struggling hospitals, understaffed nursing homes, long HCBS wait lists, and declining enrollment — simultaneously?

### Why It Matters
Nobody looks at hospitals + nursing homes + HCBS + enrollment in the same analysis. A state with struggling hospitals, understaffed nursing homes, AND 20,000-person HCBS wait lists has a systemic problem that no single-domain analysis reveals.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_hospital_cost` | Hospital financials (HCRIS) | 18K |
| `fact_dsh_hospital` | DSH supplemental payments | 6K |
| `fact_five_star` | Nursing facility quality | 14.7K |
| `fact_pbj_nurse_staffing` | Nursing home staffing | 1.3M |
| `fact_hcbs_waitlist` | HCBS waiting lists | 607K |
| `fact_enrollment` | Enrollment trends (unwinding) | varies |
| `fact_fmap_historical` | Federal match rates | 612 |
| `fact_cms64_multiyear` | Total spending | 118K |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Hospital stress: % of hospitals with negative operating margin by state (from HCRIS)
2. DSH dependency: avg DSH payment as % of net revenue by state
3. Nursing quality: avg Five-Star overall rating by state + avg nursing HPRD from PBJ
4. HCBS pressure: total waitlist count per 1000 Medicaid enrollees by state
5. Enrollment disruption: % enrollment change during unwinding period
6. Composite "Safety Net Stress Index" = weighted average of normalized subscores

### Tabs
1. **Hospital Financial Stress** — % of hospitals with negative margins by state. DSH dependency ratio. Uncompensated care burden. Bar chart ranked by stress level.
2. **LTSS Pressure** — HCBS waitlist count per 1000 enrollees vs avg nursing home quality rating. Scatter plot. States with long waits AND low nursing quality are the pressure points.
3. **Staffing Crisis** — PBJ nursing hours per resident day by state. % below CMS proposed staffing minimum (3.48 total HPRD). Contract nurse dependency %.
4. **Composite Index** — Combined stress score ranking all states. Heatmap or ranked bar chart. Drill-down table with all sub-scores.

### Header Copy
- **Title:** Safety Net Stress Test
- **Badge:** HCRIS + Five-Star + PBJ + HCBS + CMS-64
- **Subtitle:** Multi-dimensional assessment of safety net strain across hospitals, nursing facilities, HCBS programs, and enrollment stability. Identifies states where the entire care continuum is under simultaneous pressure.

### Source Line
Sources: CMS HCRIS Cost Reports . Care Compare Five-Star . Payroll-Based Journal Staffing . HCBS Waiver Waitlists . CMS-64 Expenditure . Medicaid Enrollment

---

## Module 5: Program Integrity Risk Index

**Route:** `/api/research/integrity-risk/*`
**Frontend:** `src/tools/research/IntegrityRisk.tsx`
**URL:** `/#/research/integrity-risk`

### Research Question
Which states have the highest composite program integrity risk when you combine financial influence patterns, prescribing anomalies, payment error rates, and enforcement capacity?

### Why It Matters
OIG and MFCUs work case-by-case. Nobody does state-level integrity risk scoring that combines Open Payments influence, opioid prescribing outliers, PERM error rates, and enforcement outcomes. This creates a "Program Integrity Risk Index" that does not exist anywhere.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_open_payments` | Industry payments to providers | 39.6K |
| `fact_leie` | Excluded individuals/entities | 82K |
| `fact_opioid_prescribing` | Prescribing rate anomalies | 539K |
| `fact_perm_rates` | Improper payment rates | 12 |
| `fact_mfcu_stats` | Fraud unit outcomes | varies |
| `fact_cms64_multiyear` | Total spending at risk | 118K |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Open Payments: total $ per Medicaid enrollee by state. High influence = high risk signal.
2. LEIE: exclusion count per 1000 providers by state. High exclusions = high detected fraud.
3. Opioid prescribing: rate deviation from national mean. States >1 SD above = elevated risk.
4. PERM: improper payment rate by state (17 states per cycle, use most recent).
5. MFCU: convictions per $ spent on investigations. Low conviction efficiency = enforcement gap.
6. Composite Index: z-score each factor, weight, sum. Higher = more risk.

### Tabs
1. **Composite Index** — All states ranked by composite integrity risk score. Bar chart with color gradient. Tooltip shows sub-scores.
2. **Financial Influence** — Open Payments $ per Medicaid enrollee by state. Which states have highest industry-to-provider payment concentration?
3. **Enforcement Capacity** — MFCU stats: cases opened, convictions, recoveries per $ investigated. LEIE exclusions per capita. Which states have enforcement gaps?
4. **Payment Accuracy** — PERM improper payment rates. Trend over time. FFS vs managed care error rates.

### Header Copy
- **Title:** Program Integrity Risk Index
- **Badge:** Open Payments + LEIE + PERM + MFCU
- **Subtitle:** Composite state-level integrity risk scoring combining financial influence patterns, provider exclusions, payment error rates, and fraud enforcement capacity. A view that no single federal agency produces.

### Source Line
Sources: CMS Open Payments (PY2024, $13B) . OIG LEIE Exclusion List . CMS PERM Error Rates . MFCU Statistical Reports

---

## Module 6: Fiscal Cliff Analysis

**Route:** `/api/research/fiscal-cliff/*`
**Frontend:** `src/tools/research/FiscalCliff.tsx`
**URL:** `/#/research/fiscal-cliff`

### Research Question
Which states face the most severe fiscal pressure as enhanced FMAP expires, Medicaid spending grows, and state revenue capacity varies?

### Why It Matters
State budget officers and NASBO do this analysis for their own state. Nobody does it comparatively across all 50 states with granular enrollment-by-eligibility + CMS-64-by-category + state economic indicators.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_fmap_historical` | Federal match rates over time | 612 |
| `fact_cms64_multiyear` | Total Medicaid spending FY2018-2024 | 118K |
| `fact_state_gdp` | State GDP | 13.4K |
| `fact_state_tax_collections` | State tax revenue | 271 |
| `fact_census_state_finances` | State budget data | varies |
| `fact_enrollment` | Enrollment trends | varies |
| `fact_bea_personal_income` | Personal income by state | varies |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Compute Medicaid spending as % of state general fund (CMS-64 state share / state tax collections)
2. Track FMAP changes: how much more is each state paying post-enhanced FMAP?
3. Spending growth rate vs revenue growth rate: which states are falling behind?
4. Enrollment growth trajectory: which states are still growing enrollment?
5. Vulnerability Score = (Medicaid as % of budget) x (spending growth / revenue growth) x (1 - FMAP)

### Tabs
1. **Spending vs Revenue** — Dual bar: Medicaid state share growth rate vs tax revenue growth rate by state. States where spending outpaces revenue are in red.
2. **FMAP Impact** — Waterfall: enhanced FMAP (100% for expansion) phasing down. Dollar impact per state. Which states lose the most federal support?
3. **Budget Pressure** — Medicaid as % of state general fund by state. Trend over FY2018-2024. Heatmap showing compression.
4. **Vulnerability Ranking** — Composite fiscal vulnerability index. Ranked bar chart. Detail table with all factors.

### Header Copy
- **Title:** Fiscal Cliff Analysis
- **Badge:** CMS-64 + FMAP + Census + BEA
- **Subtitle:** Comparative state fiscal pressure analysis as enhanced federal matching expires and Medicaid spending grows against state revenue capacity. Identifies states approaching fiscal unsustainability.

### Source Line
Sources: CMS-64 Expenditure (FY2018-2024) . MACPAC FMAP Historical . Census State Finances . BEA State GDP . Tax Foundation Collections

---

## Module 7: Maternal Health Deserts

**Route:** `/api/research/maternal-health/*`
**Frontend:** `src/tools/research/MaternalHealth.tsx`
**URL:** `/#/research/maternal-health`

### Research Question
Where do maternal mortality, social vulnerability, provider shortages, and poor Medicaid quality measure performance overlap to create maternal health deserts?

### Why It Matters
Maternal mortality is politically salient and actively researched, but most analyses use CDC vital statistics alone. Multi-layered analysis reveals whether the problem is access, coverage, quality of care, or social determinants — and how it differs by state.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_cdc_maternal_mortality_prov` | Maternal mortality by state | varies |
| `fact_maternal_mortality_national` | National maternal mortality trends | varies |
| `fact_svi_county` | Social Vulnerability Index | 3.1K |
| `fact_hpsa` | Shortage areas (filter OB/GYN) | 69K |
| `fact_quality_core_set_2024` | Prenatal/postpartum measures | 11K |
| `fact_cdc_natality` | Birth data | varies |
| `fact_prematurity_smm` | Severe maternal morbidity | varies |
| `fact_infant_mortality_state` | Infant mortality rates | varies |
| `fact_enrollment` | Medicaid enrollment | varies |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Maternal mortality rates by state (CDC vital statistics)
2. SVI: average county-level SVI score by state (aggregate from county to state)
3. HPSA: count OB/GYN shortage designations by state
4. Quality: extract prenatal and postpartum care Core Set measure rates
5. Composite "Maternal Health Desert Score" = weighted factors
6. Overlay: Medicaid enrollment among women of childbearing age (if available)

### Tabs
1. **Mortality Landscape** — Maternal mortality rate by state. Bar chart ranked. National average line. Trend over time if data permits.
2. **Access Barriers** — OB/GYN HPSA count by state + avg SVI score. States with high SVI AND many shortage areas flagged.
3. **Quality Gaps** — Prenatal/postpartum Core Set measure rates. Which states report poorly or score poorly on maternal quality measures?
4. **Composite Risk** — Multi-factor maternal health desert score. Ranked chart. Detail table.

### Header Copy
- **Title:** Maternal Health Deserts
- **Badge:** CDC Mortality + SVI + HPSA + Core Set
- **Subtitle:** Multi-dimensional mapping of maternal health risk across social vulnerability, provider access, quality measure performance, and mortality outcomes. Reveals whether state-level gaps are driven by access, coverage, or social determinants.

### Source Line
Sources: CDC/NCHS Vital Statistics . CDC/ATSDR Social Vulnerability Index . HRSA HPSA Designations . Medicaid Core Set (2024) . CDC WONDER Natality

---

## Module 8: Pharmacy Spread Analysis

**Route:** `/api/research/pharmacy-spread/*`
**Frontend:** `src/tools/research/PharmacySpread.tsx`
**URL:** `/#/research/pharmacy-spread`

### Research Question
What is the actual spread between drug acquisition cost (NADAC) and Medicaid reimbursement (SDUD), and which drugs and states have the widest gaps?

### Why It Matters
Most Medicaid pharmacy analyses use pre-rebate spending because rebate data is confidential. NADAC represents what pharmacies actually pay. Comparing NADAC vs SDUD reimbursement triangulates the pharmacy spread — overpayment at the counter before rebates enter the picture.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_nadac` | Drug acquisition costs by NDC | 1.9M |
| `fact_sdud_2025` | Medicaid drug reimbursement by NDC | 2.6M |
| `fact_sdud_combined` | Historical drug utilization | 28.3M |
| `fact_drug_rebate_products` | Drug product reference | 1.9M |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Join `fact_nadac` (latest effective_date per NDC) to `fact_sdud_2025` on NDC
2. Compute spread = (SDUD total_reimbursed / units_reimbursed) - NADAC nadac_per_unit
3. Aggregate by state: average spread, total overpayment dollars
4. Aggregate by therapeutic class (from drug_rebate_products): which classes have widest spreads?
5. Identify top 50 drugs by total overpayment volume (spread x units)

### Tabs
1. **Spread Overview** — Distribution of per-unit spread across all NDCs. Histogram. Summary stats: median, mean, P90. National aggregate overpayment estimate.
2. **State Variation** — Average spread by state. Map or ranked bar chart. States with highest pharmacy overpayment per enrollee.
3. **Top Overpayment Drugs** — Top 50 drugs ranked by total spread dollars. Drug name, NDC, NADAC price, Medicaid reimbursement, unit spread, total volume, total overpayment.
4. **Therapeutic Class** — Spread aggregated by drug class. Which therapeutic categories (generics vs brand, oral vs injectable) have the widest margin?

### Header Copy
- **Title:** Pharmacy Spread Analysis
- **Badge:** NADAC + SDUD + Drug Rebate
- **Subtitle:** The gap between what pharmacies pay for drugs (NADAC) and what Medicaid reimburses (SDUD). Identifies overpayment hotspots by drug, state, and therapeutic class before rebates are applied.

### Source Line
Sources: CMS NADAC (Mar 2026) . State Drug Utilization Data (2025) . Medicaid Drug Rebate Product List

---

## Module 9: Nursing Home Ownership & Quality

**Route:** `/api/research/nursing-ownership/*`
**Frontend:** `src/tools/research/NursingOwnership.tsx`
**URL:** `/#/research/nursing-ownership`

### Research Question
Do chain-affiliated and for-profit nursing homes have systematically worse quality, lower staffing, and more deficiencies than independent and nonprofit facilities?

### Why It Matters
The debate about private equity and for-profit ownership of nursing homes is national news. The data to actually test the hypothesis — staffing levels + deficiency citations + chain affiliation + cost reports + quality ratings + ownership records — exists in the Aradune lake. It just needs the joins.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_five_star` | Quality ratings + chain affiliation | 14.7K |
| `fact_pbj_nurse_staffing` | Detailed staffing by facility | 1.3M |
| `fact_nh_deficiency` | Deficiency citations | 419K |
| `fact_nh_chain_performance` | Chain-level quality aggregates | 619 |
| `fact_nh_ownership` | Ownership type and corporate parent | 144K |
| `fact_snf_cost` | Cost reports | varies |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Join Five-Star data (which has chain_name) to ownership data to classify: for-profit chain, for-profit independent, nonprofit, government
2. Compare by ownership type: avg overall rating, staffing rating, quality measure rating, deficiency count
3. PBJ staffing analysis: avg HPRD (total, RN, CNA) by ownership type. Contract nurse % by ownership type.
4. Deficiency analysis: avg deficiency count, severity distribution, fine amounts by ownership type
5. Cost analysis: cost per resident day, admin cost %, by ownership type

### Tabs
1. **Quality by Ownership** — Five-Star rating distribution by ownership type (for-profit chain / for-profit independent / nonprofit / government). Box plots or grouped bars. Overall, staffing, QM, health inspection sub-ratings.
2. **Staffing Comparison** — PBJ nurse hours per resident day by ownership type. RN vs CNA vs LPN breakdown. Contract nurse dependency %. Bar charts.
3. **Deficiency Patterns** — Avg deficiency count by ownership type. Severity distribution (scope-severity grid). Top deficiency tags by ownership type.
4. **Chain Scoreboard** — The 50 largest chains ranked by avg quality rating. Facility count, avg staffing, avg deficiencies, total fines. Sortable table.

### Header Copy
- **Title:** Nursing Home Ownership & Quality
- **Badge:** Five-Star + PBJ + Deficiencies + HCRIS
- **Subtitle:** Systematic comparison of quality, staffing, deficiency citations, and costs across for-profit chain, independent, nonprofit, and government nursing facilities. Data-driven analysis of the ownership-quality relationship.

### Source Line
Sources: CMS Five-Star Quality Rating . Payroll-Based Journal . CMS Deficiency Citations . HCRIS SNF Cost Reports . CMS Ownership Data

---

## Module 10: Section 1115 Waiver Impact

**Route:** `/api/research/waiver-impact/*`
**Frontend:** `src/tools/research/WaiverImpact.tsx`
**URL:** `/#/research/waiver-impact`

### Research Question
Do Section 1115 waivers actually achieve their stated goals? How do enrollment, spending, and quality change after waiver implementation?

### Why It Matters
States use 1115 waivers for everything from work requirements to reentry programs to HCBS expansions. The evaluation evidence is usually a single-state contractor report published years later. Aradune has waiver approval dates + enrollment time series + spending time series + quality measures over 8 years, enabling quasi-experimental before/after analysis at scale.

### Data Tables
| Table | Role | Rows |
|-------|------|------|
| `fact_kff_1115_waivers` | Waiver approvals and provisions | varies |
| `fact_kff_1115_approved_waivers` | Approved waivers with dates | varies |
| `ref_1115_waivers` | 647 waiver records | 647 |
| `fact_enrollment` | Monthly enrollment time series | varies |
| `fact_cms64_multiyear` | Spending over FY2018-2024 | 118K |
| `fact_quality_core_set_combined` | Quality measures 2017-2024 | 36K |
| `dim_state` | State metadata | 51 |

### Analysis Method
1. Identify waiver approval dates from KFF data and ref_1115_waivers
2. For each waiver: pull enrollment time series for that state 24 months before and after approval
3. Pull CMS-64 spending for that state over the same period
4. Pull quality Core Set measures for that state across available years
5. Compute before/after differences. Compare to control states (states without that waiver type in the same period)
6. Categorize waivers by type: expansion, work requirements, HCBS, SUD, reentry, continuous eligibility

### Tabs
1. **Waiver Catalog** — Searchable table of all 1115 waivers with approval dates, states, types, provisions. Filter by waiver type or state.
2. **Enrollment Impact** — For selected waiver: enrollment time series before/after approval. Interrupted time series chart. Did enrollment grow, shrink, or stay flat?
3. **Spending Impact** — CMS-64 total computable and state share before/after waiver. Bar chart of spending growth rate pre vs post.
4. **Quality Trajectory** — Core Set measure averages for waiver state vs national average, 2017-2024. Line chart. Did quality improve after waiver implementation?

### Header Copy
- **Title:** Section 1115 Waiver Impact
- **Badge:** KFF Waivers + CMS-64 + Core Set + Enrollment
- **Subtitle:** Quasi-experimental evaluation of Section 1115 waiver effectiveness. Before/after analysis of enrollment, spending, and quality outcomes for expansion, work requirement, SUD, and HCBS waivers across all states.

### Source Line
Sources: KFF 1115 Waiver Tracker . CMS-64 Expenditure (FY2018-2024) . Medicaid Core Set (2017-2024) . CMS Monthly Enrollment Reports

---

## Integration Notes

### Registration
When ready to go live, each module needs:
1. Import in `server/main.py`: `from server.routes.research import rate_quality, mc_value, ...`
2. Register router: `app.include_router(rate_quality.router)`
3. Lazy import in `Platform.tsx`: `const RateQualityNexus = lazy(() => import("./tools/research/RateQualityNexus"))`
4. Add to TOOLS array and NAV_GROUPS (new "research" group)
5. Add to toolMap routing

### Design Rules
- Follow existing Aradune v14 design tokens exactly
- All inline styles (no CSS files)
- Responsive: `useIsMobile(768)` hook
- Tables in `overflowX: "auto"` wrappers
- Charts in `ChartActions` wrapper for export
- Color-code values: POS for good, NEG for bad, WARN for caution
- Footer: "Ask Intelligence about this" button + source line

### Data Accuracy
- Every number must be verifiable against source
- Zero tolerance for wrong data
- Filter SDUD state_code != 'XX' (national totals)
- Strip MACPAC footnote markers from state names
- Opioid data: join dim_state on geo_desc for state codes
- Quality measures: verify measure_id against Core Set spec
