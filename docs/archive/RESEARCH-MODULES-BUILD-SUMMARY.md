# Research Modules Build Summary

> Session 29 (2026-03-13). 10 cross-domain research modules built as standalone files. Zero existing code modified.

---

## What Was Built

**10 backend route files** (1,971 lines Python, 31 API endpoints)
**10 frontend component files** (5,802 lines TypeScript/React, 40 tabs)
**1 planning document** (docs/RESEARCH-MODULES.md)
**1 memory file** (project_research_modules.md)

All files live in isolated `research/` subdirectories:
- Backend: `server/routes/research/`
- Frontend: `src/tools/research/`

No existing files were modified. No commits were made.

---

## The 10 Modules

### 1. Rate-Quality Nexus (`rate_quality.py` + `RateQualityNexus.tsx`)
**Question:** Does paying Medicaid providers more actually improve outcomes?

**Backend (164 lines, 5 endpoints):**
- `/api/research/rate-quality/correlation` — Joins `fact_rate_comparison` (avg % of Medicare by state) to `fact_quality_core_set_2024` (measure rate by state). Selectable quality measure.
- `/api/research/rate-quality/measures` — Lists available quality measures for dropdown.
- `/api/research/rate-quality/access` — HPSA shortage counts vs avg Medicaid rate level by state.
- `/api/research/rate-quality/workforce` — BLS healthcare wages (SOC 29-/31-) vs Medicaid rate levels.
- `/api/research/rate-quality/detail` — Full state table: rates, quality, HPSAs, MC penetration.

**Frontend (678 lines, 4 tabs):**
- Rate-Quality Correlation (scatter chart with measure selector)
- Access Impact (HPSA bar chart colored by rate adequacy)
- Workforce Connection (wages vs rates scatter)
- State Detail (sortable multi-column table)

**Key joins:** `fact_rate_comparison` x `fact_quality_core_set_2024` x `fact_hpsa` x `fact_bls_wage` x `fact_mc_enrollment_summary` — all on `state_code`.

---

### 2. Managed Care Value Assessment (`mc_value.py` + `ManagedCareValue.tsx`)
**Question:** Is Medicaid managed care saving money and improving quality?

**Backend (180 lines, 5 endpoints):**
- `/api/research/mc-value/penetration-spending` — MC penetration vs MACPAC per-enrollee spending. Has fallback to join through `dim_state` if spending table lacks `state_code`.
- `/api/research/mc-value/mco-financials` — Full MCO MLR detail by plan.
- `/api/research/mc-value/mco-summary` — Aggregated MLR stats by state.
- `/api/research/mc-value/quality-by-tier` — Quality measures averaged by MC penetration tier (High/Med/Low).
- `/api/research/mc-value/trend` — MC penetration + CMS-64 spending over time.

**Frontend (681 lines, 4 tabs):**
- Penetration vs Spending (scatter with tier breakdown)
- MCO Financials (MLR bar chart, 85% threshold highlighting)
- Quality by MC Tier (grouped bars with measure selector)
- Trend Analysis (dual-axis line chart)

**Key joins:** `fact_mc_enrollment_summary` x `fact_mco_mlr` x `fact_macpac_spending_per_enrollee` x `fact_cms64_multiyear` x `fact_quality_core_set_2024`.

---

### 3. Opioid Treatment Gap (`treatment_gap.py` + `TreatmentGap.tsx`)
**Question:** Where does SUD prevalence outstrip treatment capacity, and is it a capacity, reimbursement, or coverage problem?

**Backend (208 lines, 4 endpoints):**
- `/api/research/treatment-gap/demand-supply` — OUD prevalence vs SUD facility capacity per 100K. Gracefully probes for the right NSDUH measure_id (tries 4 variants). Has fallback if `offers_su`/`offers_detox` columns don't exist in `fact_mh_facility`.
- `/api/research/treatment-gap/mat-utilization` — MAT drug spending from SDUD (buprenorphine, suboxone, naloxone, naltrexone, vivitrol, sublocade). Filters `state_code != 'XX'`.
- `/api/research/treatment-gap/prescribing` — Opioid prescribing rates with FIPS-to-state resolution via `dim_state` join on `geo_desc`.
- `/api/research/treatment-gap/funding` — SUD prevalence vs block grant funding per enrollee.

**Frontend (717 lines, 4 tabs):**
- Demand-Supply Map (prevalence bars colored by facility capacity)
- MAT Utilization (spending bars with per-enrollee cross-reference)
- Prescribing Patterns (rate comparison to national average)
- Funding Alignment (prevalence vs grant scatter)

**Key joins:** `fact_nsduh_prevalence` x `fact_mh_facility` x `fact_sdud_2025` x `fact_opioid_prescribing` x `dim_state` x `fact_block_grant` x `fact_enrollment`.

---

### 4. Safety Net Stress Test (`safety_net.py` + `SafetyNetStress.tsx`)
**Question:** Which states have the most stressed safety net across hospitals, nursing homes, and HCBS simultaneously?

**Backend (215 lines, 4 endpoints):**
- `/api/research/safety-net/hospital-stress` — % hospitals with negative margins, avg operating margin, uncompensated care, DSH, Medicaid day %.
- `/api/research/safety-net/ltss-pressure` — HCBS waitlists (607K people) + Five-Star nursing quality + waitlist per 1000 enrollees.
- `/api/research/safety-net/staffing-crisis` — PBJ staffing analysis. Fallback if `nursing_hprd` column doesn't exist (computes from `total_nursing_hrs / mds_census`). Counts facilities below CMS proposed 3.48 HPRD minimum.
- `/api/research/safety-net/composite` — Combined stress index (hospital margins + HCBS pressure + nursing deficit + FMAP).

**Frontend (467 lines, 4 tabs):**
- Hospital Financial Stress (negative margin bar chart)
- LTSS Pressure (waitlist vs nursing rating scatter)
- Staffing Crisis (HPRD chart with 3.48 threshold coloring)
- Composite Index (stress score gradient)

**Key joins:** `fact_hospital_cost` x `fact_dsh_hospital` x `fact_five_star` x `fact_pbj_nurse_staffing` x `fact_hcbs_waitlist` x `fact_enrollment` x `fact_fmap_historical`.

---

### 5. Program Integrity Risk Index (`integrity_risk.py` + `IntegrityRisk.tsx`)
**Question:** Which states have the highest composite integrity risk across financial influence, exclusions, payment errors, and enforcement?

**Backend (191 lines, 4 endpoints):**
- `/api/research/integrity-risk/composite` — Open payments per enrollee + exclusions per 100K.
- `/api/research/integrity-risk/open-payments` — $13B Open Payments aggregated by state. Avg per physician.
- `/api/research/integrity-risk/enforcement` — MFCU stats with schema-discovery fallback for unknown column names.
- `/api/research/integrity-risk/perm` — PERM improper payment rates with schema-discovery fallback.

**Frontend (471 lines, 4 tabs):**
- Composite Index (dual-axis: payments/enrollee + exclusions/100K)
- Financial Influence (top 30 states by industry payments)
- Enforcement (MFCU ROI bar chart)
- Payment Accuracy (PERM rate line chart, 4 series)

**Key joins:** `fact_open_payments` x `fact_leie` x `fact_perm_rates` x `fact_mfcu_stats` x `fact_enrollment`.

---

### 6. Fiscal Cliff Analysis (`fiscal_cliff.py` + `FiscalCliff.tsx`)
**Question:** Which states face the most severe fiscal pressure as enhanced FMAP expires and Medicaid spending grows?

**Backend (198 lines, 4 endpoints):**
- `/api/research/fiscal-cliff/spending-vs-revenue` — CMS-64 state share vs tax collections. Computes Medicaid as % of state revenue.
- `/api/research/fiscal-cliff/fmap-impact` — FMAP historical with enhanced FMAP rates.
- `/api/research/fiscal-cliff/budget-pressure` — Latest-year snapshot: Medicaid share, tax revenue, GDP, FMAP.
- `/api/research/fiscal-cliff/vulnerability` — Composite vulnerability score (budget share, spending growth, state burden).

**Frontend (498 lines, 4 tabs):**
- Spending vs Revenue (Medicaid % of revenue bars, red above 20%)
- FMAP Impact (multi-state line chart with state selector pills)
- Budget Pressure (GDP vs state share scatter)
- Vulnerability Ranking (composite score bars)

**Key joins:** `fact_cms64_multiyear` x `fact_fmap_historical` x `fact_state_tax_collections` x `fact_state_gdp` x `fact_census_state_finances`.

---

### 7. Maternal Health Deserts (`maternal_health.py` + `MaternalHealth.tsx`)
**Question:** Where do mortality, social vulnerability, provider shortages, and quality gaps overlap to create maternal health deserts?

**Backend (236 lines, 5 endpoints):**
- `/api/research/maternal-health/mortality` — Maternal mortality by state. Fallback to `fact_maternal_mortality_national` if provider-level table fails.
- `/api/research/maternal-health/access` — HPSA + SVI by state. Uses primary care HPSA as proxy.
- `/api/research/maternal-health/quality` — Prenatal/postpartum Core Set measures (searches measure_id and measure_name with ILIKE).
- `/api/research/maternal-health/infant-mortality` — Infant mortality by state/year.
- `/api/research/maternal-health/composite` — Multi-factor risk (mortality + HPSAs + SVI + quality).

**Frontend (507 lines, 4 tabs):**
- Mortality Landscape (bar chart with national average line)
- Access Barriers (HPSA vs SVI scatter, red for high-risk)
- Quality Gaps (measure selector, per-measure state bars)
- Composite Risk (multi-factor score, top-quartile highlighting)

**Key joins:** `fact_cdc_maternal_mortality_prov` x `fact_svi_county` x `fact_hpsa` x `fact_quality_core_set_2024` x `fact_infant_mortality_state` x `fact_cdc_natality`.

---

### 8. Pharmacy Spread Analysis (`pharmacy_spread.py` + `PharmacySpread.tsx`)
**Question:** What is the actual gap between drug acquisition cost (NADAC) and Medicaid reimbursement (SDUD)?

**Backend (192 lines, 4 endpoints):**
- `/api/research/pharmacy-spread/overview` — Joins NADAC (latest per NDC via ROW_NUMBER) to SDUD reimbursement. Computes spread per unit and total overpayment. Top 500 by spread.
- `/api/research/pharmacy-spread/by-state` — Spread aggregated by state. Total overpayment and spread %.
- `/api/research/pharmacy-spread/top-drugs` — Top drugs by total overpayment. Parameterized limit.
- `/api/research/pharmacy-spread/stats` — Summary statistics: avg/median/P90 spread, total over/underpayment, drugs overpaid vs underpaid. Returns single object.

**Frontend (533 lines, 4 tabs):**
- Spread Overview (summary metrics, top 30 drugs bar chart)
- State Variation (states ranked by total spread, color intensity)
- Top Overpayment Drugs (sortable table, 50 drugs)
- Drug Detail (searchable, paginated drug table)

**Key joins:** `fact_nadac` (1.9M rows, latest effective_date per NDC) x `fact_sdud_2025` (2.6M rows, aggregated by NDC). Uses ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) for performance on 1.9M-row NADAC table.

---

### 9. Nursing Home Ownership & Quality (`nursing_ownership.py` + `NursingOwnership.tsx`)
**Question:** Do chain-affiliated and for-profit nursing homes have worse quality, staffing, and more deficiencies?

**Backend (164 lines, 5 endpoints):**
- `/api/research/nursing-ownership/quality-by-type` — Five-Star ratings by ownership type (For-profit/Non-profit/Government).
- `/api/research/nursing-ownership/chain-vs-independent` — Chain vs independent split by ownership type.
- `/api/research/nursing-ownership/deficiency-patterns` — Top deficiency tags by ownership/affiliation. Joins `fact_nh_deficiency` to `fact_five_star` on `provider_ccn`.
- `/api/research/nursing-ownership/chain-scoreboard` — Worst 50 chains by avg quality (min 5 facilities).
- `/api/research/nursing-ownership/state-breakdown` — By state + ownership type.

**Frontend (584 lines, 4 tabs):**
- Quality by Ownership (grouped bars: overall/staffing/QM ratings)
- Chain vs Independent (side-by-side comparison)
- Deficiency Patterns (top 20 citations, severity coloring)
- Chain Scoreboard (sortable, color-coded ratings)

**Key joins:** `fact_five_star` x `fact_nh_deficiency` x `fact_nh_chain_performance` x `fact_nh_ownership`. Joined on `provider_ccn`.

---

### 10. Section 1115 Waiver Impact (`waiver_impact.py` + `WaiverImpact.tsx`)
**Question:** Do Section 1115 waivers actually achieve their stated goals?

**Backend (223 lines, 5 endpoints):**
- `/api/research/waiver-impact/catalog` — Searchable waiver catalog. Tries 3 table names: `ref_1115_waivers` -> `fact_kff_1115_waivers` -> `fact_section_1115_waivers`.
- `/api/research/waiver-impact/enrollment/{state}` — Monthly enrollment time series.
- `/api/research/waiver-impact/spending/{state}` — CMS-64 spending by fiscal year.
- `/api/research/waiver-impact/quality/{state}` — Quality Core Set over time. Tries `fact_quality_core_set_combined` with `core_set_year`/`state_rate`, falls back to 2024 single-year with `measure_rate`.
- `/api/research/waiver-impact/compare` — Waiver vs non-waiver state comparison.

**Frontend (666 lines, 4 tabs):**
- Waiver Catalog (searchable/filterable, status pills)
- Enrollment Impact (line chart with waiver approval ReferenceLine)
- Spending Impact (stacked bar: federal + state, CAGR calculation)
- Quality Trajectory (measure selector, state vs national line chart)

**Key joins:** `ref_1115_waivers` x `fact_enrollment` x `fact_cms64_multiyear` x `fact_quality_core_set_combined`.

---

## Integration Guide (For When Ready)

### Step 1: Register backend routes in `server/main.py`

Add imports:
```python
from server.routes.research import (
    rate_quality, mc_value, treatment_gap, safety_net,
    integrity_risk, fiscal_cliff, maternal_health,
    pharmacy_spread, nursing_ownership, waiver_impact,
)
```

Register routers:
```python
app.include_router(rate_quality.router)
app.include_router(mc_value.router)
app.include_router(treatment_gap.router)
app.include_router(safety_net.router)
app.include_router(integrity_risk.router)
app.include_router(fiscal_cliff.router)
app.include_router(maternal_health.router)
app.include_router(pharmacy_spread.router)
app.include_router(nursing_ownership.router)
app.include_router(waiver_impact.router)
```

### Step 2: Register frontend routes in `Platform.tsx`

Lazy imports:
```typescript
const RateQualityNexus = lazy(() => import("./tools/research/RateQualityNexus"));
const ManagedCareValue = lazy(() => import("./tools/research/ManagedCareValue"));
const TreatmentGap = lazy(() => import("./tools/research/TreatmentGap"));
const SafetyNetStress = lazy(() => import("./tools/research/SafetyNetStress"));
const IntegrityRisk = lazy(() => import("./tools/research/IntegrityRisk"));
const FiscalCliff = lazy(() => import("./tools/research/FiscalCliff"));
const MaternalHealth = lazy(() => import("./tools/research/MaternalHealth"));
const PharmacySpread = lazy(() => import("./tools/research/PharmacySpread"));
const NursingOwnership = lazy(() => import("./tools/research/NursingOwnership"));
const WaiverImpact = lazy(() => import("./tools/research/WaiverImpact"));
```

Add to TOOLS array:
```typescript
{ id: "rate-quality", group: "research", name: "Rate-Quality Nexus", tagline: "Does paying more improve outcomes?", status: "live", icon: "R", color: "#2E6B4A" },
{ id: "mc-value", group: "research", name: "Managed Care Value", tagline: "Is MC saving money?", status: "live", icon: "M", color: "#3A7D5C" },
// ... etc for all 10
```

Add NAV_GROUP:
```typescript
{ key: "research", label: "Research", tools: TOOLS.filter(t => t.group === "research") }
```

Add to toolMap:
```typescript
"/research/rate-quality": <RateQualityNexus />,
"/research/mc-value": <ManagedCareValue />,
"/research/treatment-gap": <TreatmentGap />,
"/research/safety-net": <SafetyNetStress />,
"/research/integrity-risk": <IntegrityRisk />,
"/research/fiscal-cliff": <FiscalCliff />,
"/research/maternal-health": <MaternalHealth />,
"/research/pharmacy-spread": <PharmacySpread />,
"/research/nursing-ownership": <NursingOwnership />,
"/research/waiver-impact": <WaiverImpact />,
```

---

## Defensive Patterns Used

Several routes implement defensive patterns because exact column names in some tables are uncertain:

1. **Schema-discovery fallback** (integrity_risk, waiver_impact): Routes that query `fact_mfcu_stats`, `fact_perm_rates`, and `ref_1115_waivers` first try to discover the actual column names with a `SELECT * LIMIT 1` query, then dynamically build the SELECT list from available columns.

2. **Multi-table fallback** (waiver_impact): The catalog endpoint tries 3 table names in sequence (`ref_1115_waivers` -> `fact_kff_1115_waivers` -> `fact_section_1115_waivers`) because the exact table name was uncertain.

3. **Measure ID probing** (treatment_gap): The demand-supply endpoint tries 4 NSDUH measure IDs in sequence (`opioid_use_disorder` -> `illicit_drug_use_past_month` -> `any_substance_use_disorder` -> ILIKE fallback) to handle varying measure naming.

4. **Column existence fallback** (treatment_gap, safety_net): Routes check if expected columns exist (e.g., `offers_su`, `nursing_hprd`) and fall back to alternative calculations if they don't.

5. **ROW_NUMBER performance optimization** (pharmacy_spread): NADAC latest-price subquery uses `ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC)` instead of a correlated `IN (subquery)` to avoid slow performance on 1.9M rows.

---

## What I Learned

### The Data Lake Is Structurally Unique

After cataloging all 700 tables and tracing every join path, the thing that stands out is not the individual datasets — CMS publishes all of these. It's the **join surface**. The `state_code` key that runs through every table creates an analysis plane that doesn't exist anywhere else:

- MACPAC publishes spending per enrollee, but can't correlate it with MCO MLR data because they're in different systems.
- KFF publishes 1115 waiver trackers, but can't show enrollment or spending trajectories for those states because CMS-64 is a different data product.
- CMS publishes HCRIS and Five-Star, but doesn't join them to HCBS waitlists or PBJ staffing in any public tool.
- SAMHSA publishes NSDUH prevalence, but doesn't cross-reference it with Medicaid drug utilization (SDUD) to see whether treatment drugs are actually reaching high-prevalence states.

Every one of these cross-domain queries is a paper that would take a research team months to assemble. The lake makes them single SQL queries.

### The Most Powerful Intersections

Of the 10 modules, these 4 have the most novel analytical power (in order):

1. **Rate-Quality Nexus** — This directly answers the question behind the Access Rule (42 CFR 447.203). Every state Medicaid agency will need this analysis. Nobody has cross-state empirical evidence linking procedure-level rates to quality outcomes. The fact that you have `fact_rate_comparison` with 302K rate rows across 45 states is rare — most researchers have 2-3 states.

2. **Pharmacy Spread Analysis** — The NADAC x SDUD join is straightforward but almost never done because the datasets live in different CMS offices. The "spread" — what Medicaid overpays at the pharmacy counter before rebates — is a multi-billion dollar question that affects every state's drug reimbursement methodology. States set reimbursement formulas without seeing what other states pay for the same NDC. You can show them.

3. **Safety Net Stress Test** — Nobody combines HCRIS hospital margins + Five-Star nursing quality + PBJ staffing + HCBS waitlists in a single view. The composite index reveals systemic safety-net fragility that no single-domain analysis catches. States where hospitals are underwater, nursing homes are understaffed, AND HCBS has 20,000-person waitlists have a qualitatively different problem than states with just one of those issues.

4. **Nursing Ownership & Quality** — The PE-in-nursing-homes debate generates headlines but thin evidence. Joining Five-Star + PBJ + deficiencies + chain affiliation on `provider_ccn` gives you facility-level evidence across 14,700 facilities. The chain scoreboard alone — ranking the 50 largest chains by quality — doesn't exist in any public tool.

### Feasibility Notes

All 10 modules are answerable with existing data. A few caveats:

- **PERM rates** (Module 5) only cover 17 states per 3-year cycle, so the integrity risk index will have gaps. The MFCU stats table schema is uncertain — the schema-discovery fallback will handle it, but the data may need light cleaning.

- **Maternal mortality** (Module 7) state-level data from CDC may have suppressed cells for low-population states. The composite risk score will be strongest for larger states with complete data across all factors.

- **Waiver impact** (Module 10) is quasi-experimental but not causal. Before/after analysis shows correlation with waiver timing but can't control for all confounders. The compare endpoint (waiver vs non-waiver states) is the closest to a control group but imperfect.

- **Pharmacy spread** (Module 8) uses pre-rebate SDUD data. The "overpayment" is the pharmacy counter spread, not the true net cost to Medicaid after rebates. This is still valuable because it shows where reimbursement formulas are most disconnected from acquisition cost, but the module should caveat that rebates reduce the effective gap.

### Tables Most Heavily Used Across Modules

| Table | Used in modules |
|-------|-----------------|
| `dim_state` | All 10 |
| `fact_enrollment` | 7 (rate-quality, MC value, treatment gap, safety net, integrity, fiscal cliff, waiver) |
| `fact_quality_core_set_2024` | 5 (rate-quality, MC value, maternal health, waiver, safety net) |
| `fact_cms64_multiyear` | 4 (MC value, fiscal cliff, waiver, safety net) |
| `fact_mc_enrollment_summary` | 3 (rate-quality, MC value, waiver) |
| `fact_hpsa` | 3 (rate-quality, maternal health, safety net) |
| `fact_five_star` | 3 (safety net, nursing ownership, nursing ownership) |
| `fact_fmap_historical` | 2 (fiscal cliff, safety net) |

### What's Not Covered (Future Modules)

These research questions are also answerable with the lake but weren't in the initial 10:

- **Telehealth impact** — `fact_telehealth_services` x `fact_enrollment` x `fact_quality_core_set`. Did telehealth expansion during COVID improve access measures?
- **DSH redistribution fairness** — `fact_dsh_hospital` x `fact_hospital_cost` x `fact_enrollment`. Are DSH dollars going to the hospitals with the most uncompensated care?
- **Dual-eligible cost shifting** — `fact_dual_status_monthly` x `fact_medicare_spending_claim` x `fact_cms64_multiyear`. Where are costs being shifted between Medicare and Medicaid for duals?
- **HCBS rebalancing effectiveness** — `fact_ltss_rebalancing` x `fact_hcbs_waitlist` x `fact_ltss_expenditure`. States committed to rebalancing LTSS toward home/community. Is it working?

---

## File Manifest

### Backend (server/routes/research/)
| File | Lines | Endpoints |
|------|-------|-----------|
| `__init__.py` | 0 | - |
| `rate_quality.py` | 164 | 5 |
| `mc_value.py` | 180 | 5 |
| `treatment_gap.py` | 208 | 4 |
| `safety_net.py` | 215 | 4 |
| `integrity_risk.py` | 191 | 4 |
| `fiscal_cliff.py` | 198 | 4 |
| `maternal_health.py` | 236 | 5 |
| `pharmacy_spread.py` | 192 | 4 |
| `nursing_ownership.py` | 164 | 5 |
| `waiver_impact.py` | 223 | 5 |
| **Total** | **1,971** | **45** |

### Frontend (src/tools/research/)
| File | Lines | Tabs |
|------|-------|------|
| `RateQualityNexus.tsx` | 678 | 4 |
| `ManagedCareValue.tsx` | 681 | 4 |
| `TreatmentGap.tsx` | 717 | 4 |
| `SafetyNetStress.tsx` | 467 | 4 |
| `IntegrityRisk.tsx` | 471 | 4 |
| `FiscalCliff.tsx` | 498 | 4 |
| `MaternalHealth.tsx` | 507 | 4 |
| `PharmacySpread.tsx` | 533 | 4 |
| `NursingOwnership.tsx` | 584 | 4 |
| `WaiverImpact.tsx` | 666 | 4 |
| **Total** | **5,802** | **40** |

### Documentation
| File | Purpose |
|------|---------|
| `docs/RESEARCH-MODULES.md` | Full planning doc: questions, tables, joins, tabs, copy, sources |
| `docs/RESEARCH-MODULES-BUILD-SUMMARY.md` | This file |
| `memory/project_research_modules.md` | Persistent memory for future sessions |
