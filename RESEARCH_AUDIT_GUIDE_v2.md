# Aradune Research Modules — Forensic Audit Guide (v2)

**Purpose:** Systematic stress-test of all 10 research modules. Designed as Claude Code prompts to be run sequentially. Each prompt tells Claude Code what to fix automatically vs. what to flag for human decision.

**Status:** Session 30 already ran a forensic audit that fixed many schema issues and added advanced statistical methods. This v2 guide incorporates those fixes and shifts from "discover problems" to "verify fixes + stress-test what remains."

**Key reference documents:**
- `CLAUDE.md` (Session 30 — project operating manual)
- `ARADUNE-COMPLETE-REFERENCE.md` (complete schema, endpoints, audit test catalog)
- `docs/RESEARCH-FINDINGS.md` (academic findings paper)
- `docs/RESEARCH-ADVANCED-METHODS.md` (IV/2SLS, PSM, Random Forest, etc.)
- `scripts/research_advanced_methods.py` (advanced method implementations)
- `scripts/research_replication.py` (core replication script)

---

## Pre-Audit: What Session 30 Fixed (and What's Still Open)

### Schema Fixes Already Applied

These were identified and fixed during Session 30. Prompt 1 should VERIFY these are actually in the code, not re-discover them:

| Issue | Fix Applied | Verify In |
|-------|------------|-----------|
| `fact_mco_mlr` had no `mlr` column | Use `adjusted_mlr` (or compute from `mlr_numerator/mlr_denominator`) | mc_value.py |
| `fact_five_star` had no `number_of_certified_beds` | Use `certified_beds` (ref says key cols include `hprd_total`, `hprd_rn`, `deficiency_count`, `fine_total_dollars`) | nursing_ownership.py |
| NSDUH measure_id for OUD | Changed from `opioid_use_disorder` to `oud_past_year` | treatment_gap.py |
| MAT drug matching returned $0 | Fixed — national total now $978M | treatment_gap.py |
| MFCU column names | Corrected to `total_investigations`, `total_convictions`, `total_recoveries`, `mfcu_grant_expenditures` | integrity_risk.py |
| PERM column names | Corrected: `year` (not `fiscal_year`), `overall_rate_pct`, `ffs_rate_pct`, `mc_rate_pct`, `eligibility_rate_pct` | integrity_risk.py |
| pct_of_medicare filter | Changed from `>0 AND <10` to `BETWEEN 10 AND 500` | rate_quality.py |
| Waiver table name | Uses `fact_section_1115_waivers` (665 rows) with fallback to `ref_1115_waivers` and `fact_kff_1115_waivers` | waiver_impact.py |
| SVI join key | `fact_svi_county.st_abbr` (not `state_code`) | maternal_health.py, safety_net.py |
| Maternal morbidity join | `UPPER(geography) = UPPER(state_name)` via dim_state | maternal_health.py |
| Infant mortality | Requires FIPS mapping through `fact_svi_county` | maternal_health.py |
| NADAC table | `fact_nadac` (1,882,296 rows), not `fact_nadac_mar2026` | pharmacy_spread.py |

### New Numbers from Session 30 (Need Verification)

The CLAUDE.md Session 30 entry and ARADUNE-COMPLETE-REFERENCE.md Section 10 now report different numbers than the original findings paper. Some changes are dramatic:

| Metric | Original Findings | Session 30 Corrected | Change | Risk |
|--------|------------------|---------------------|--------|------|
| Rate-Quality OLS p-value | 0.178 (not significant) | **0.044 (significant)** | Narrative reversal — rates now DO predict quality | HIGH — verify the model spec that flipped this |
| MCO retention | $113B | **$120B** | +$7B | Medium — verify MLR calculation |
| MAT spending | $0 (query failed) | **$978M** | Fixed from zero | LOW — just verify it's non-zero and plausible |
| Treatment desert states | Not reported | **26 states** | New metric | LOW |
| Pharmacy spread R² | Not reported | **RF R²=0.75** | Random Forest added | Verify RF isn't overfitting |
| Nursing PSM | Not done | **10,737 matched pairs** | Propensity score matching added | Verify matching quality |
| Advanced methods | None | IV/2SLS, VIF, PSM, CHOW, RF, quantile regression, K-means, spatial mismatch | Major expansion | Needs its own audit pass |

**CRITICAL:** The Rate-Quality p-value flip from 0.178 to 0.044 changes the entire headline finding from "rates don't predict quality" to "rates DO predict quality." This MUST be verified carefully. Possible explanations:
1. The VIF diagnostics removed collinear variables (SVI + poverty), changing the model spec
2. The sample changed (different N)
3. An IV/2SLS approach was used instead of OLS
4. The filter fix (BETWEEN 10 AND 500 vs >0 AND <10) changed the rate variable distribution

### Still Open Issues

These were NOT mentioned as fixed and likely remain:

1. **Cross-document inconsistency:** RESEARCH-FINDINGS.md still has the old numbers (p=0.178, $113B, etc.) unless it was updated. RESEARCH-REPLICATION-RESULTS.md still has the negative R² and billion-scale SEs.
2. **Division-by-zero guards:** No mention of these being audited.
3. **SQL injection (parameterized queries):** No mention of this being audited.
4. **Frontend-backend contract mismatches:** The complete reference documents endpoint response shapes, but no mention of verifying the frontend actually consumes them correctly.
5. **Edge cases with territories:** AS, GU, VI, MP, PR — not mentioned.
6. **Performance on large joins:** Pharmacy spread (1.9M x 2.6M) — not mentioned.

---

## Audit Structure (Updated)

The audit is now 8 prompts across 3 phases: Verify, Stress-Test, Reconcile.

| Phase | # | Prompt | Scope | Est. Time |
|-------|---|--------|-------|-----------|
| **Verify** | 1 | Schema Verification | Confirm Session 30 fixes are in the code | 15-20 min |
| **Verify** | 2 | Endpoint Smoke Test | Every endpoint returns 200 with valid data | 15-20 min |
| **Verify** | 3 | Statistical Results Verification | Reproduce the corrected numbers, especially Rate-Quality p=0.044 | 30-45 min |
| **Stress-Test** | 4 | Data Accuracy Benchmarks | Key numbers against external sources (using test IDs DA-01 through DA-10) | 20-30 min |
| **Stress-Test** | 5 | Advanced Methods Audit | IV/2SLS, PSM, Random Forest, CHOW, K-means — new Session 30 code | 30-45 min |
| **Stress-Test** | 6 | Edge Cases & Robustness | Division-by-zero, SQL injection, nulls, territories, performance | 15-20 min |
| **Reconcile** | 7 | Frontend-Backend Contract | Components compile, API mapping correct, charts render | 20-30 min |
| **Reconcile** | 8 | Cross-Document Reconciliation | All docs agree on all numbers | 15-20 min |

---

## PROMPT 1: Schema Verification

**Goal:** Confirm every Session 30 schema fix is actually in the code. Not re-discovering problems — verifying solutions.

```
Read CLAUDE.md (Session 30) and ARADUNE-COMPLETE-REFERENCE.md (especially Section 3: Complete Table Catalog and Section 5: Research Module Detail).

Session 30 ran a forensic audit that fixed schema issues in the research modules. Your task: VERIFY every fix is actually present in the code. Do not re-discover problems — confirm solutions.

For EACH of the 10 route files in server/routes/research/:

1. Read the file
2. For each SQL query, cross-reference column names against ARADUNE-COMPLETE-REFERENCE.md Section 3 (Complete Table Catalog)
3. Confirm these specific fixes are present:

VERIFICATION CHECKLIST:

□ rate_quality.py: pct_of_medicare filter uses BETWEEN 10 AND 500 (not >0 AND <10)
□ mc_value.py: Uses adjusted_mlr from fact_mco_mlr (not a nonexistent "mlr" column)
□ mc_value.py: member_months and remittance_amount columns referenced correctly
□ treatment_gap.py: NSDUH measure_id tries oud_past_year first (not opioid_use_disorder)
□ treatment_gap.py: MAT drug ILIKE uses product_name with correct drug names
□ safety_net.py: PBJ staffing uses nursing_hprd from fact_pbj_nurse_staffing (or computes from hrs columns)
□ safety_net.py: fact_hcbs_waitlist column is total_waiting (51 rows, state-level)
□ integrity_risk.py: MFCU uses total_investigations, total_convictions, total_recoveries, mfcu_grant_expenditures
□ integrity_risk.py: PERM uses year (not fiscal_year), overall_rate_pct, ffs_rate_pct, mc_rate_pct, eligibility_rate_pct
□ fiscal_cliff.py: Census finances joined on state_code + fiscal_year where category = 'Total Taxes'
□ fiscal_cliff.py: BEA GDP joined via dim_state.state_name = geo_name
□ maternal_health.py: fact_maternal_morbidity joins via UPPER(geography) = UPPER(state_name) through dim_state
□ maternal_health.py: fact_svi_county uses st_abbr (not state_code) for state join
□ maternal_health.py: Infant mortality uses FIPS mapping through fact_svi_county
□ pharmacy_spread.py: Uses fact_nadac (not fact_nadac_mar2026)
□ pharmacy_spread.py: NADAC latest price uses ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC)
□ pharmacy_spread.py: SDUD excludes state_code = 'XX'
□ nursing_ownership.py: fact_five_star uses ownership_type, overall_rating, chain_name, hprd_total, hprd_rn, deficiency_count, fine_total_dollars
□ nursing_ownership.py: Deficiency join uses provider_ccn linking fact_five_star to fact_nh_deficiency
□ waiver_impact.py: Tries fact_section_1115_waivers (665 rows) first, then ref_1115_waivers, then fact_kff_1115_waivers

Then ALSO run each query with LIMIT 5 to confirm it executes:

For each query that fails, check the ARADUNE-COMPLETE-REFERENCE.md table catalog and fix. Note: some tables have columns not listed in the reference — use DESCRIBE to discover them.

RULES:
- FIX automatically: any remaining schema mismatches
- FLAG for human decision: cases where a column exists but the name differs from what the reference doc says (possible reference doc error vs code error)
- Produce a verification table:
| File | Check | Status (✓/✗) | Fix Applied |
```

---

## PROMPT 2: Endpoint Smoke Test

**Goal:** Every research endpoint returns 200 with valid, non-empty data. Uses the endpoint inventory from ARADUNE-COMPLETE-REFERENCE.md Section 5.

```
Read CLAUDE.md and ARADUNE-COMPLETE-REFERENCE.md Section 5 (Research Module Detail) and Section 6 (API Endpoint Reference).

Smoke-test every research module endpoint. Use the complete reference's endpoint tables as the authoritative list.

For EACH endpoint (44 total across 10 modules):

1. Call the endpoint (via curl or direct handler invocation)
2. Verify: HTTP 200
3. Verify: Response body contains rows (or an appropriate structure)
4. Verify: Row count > 0 (unless the endpoint legitimately returns 0 for a filter)
5. Log response time

USE THESE SPECIFIC TESTS from ARADUNE-COMPLETE-REFERENCE.md Section 9:

□ FB-02: Each research endpoint returns {rows: [...], count: N} (or documented alternative)
□ FB-05: Pharmacy NADAC search works: GET /api/pharmacy/nadac?search=metformin
□ EC-01: State code 'XX' excluded from SDUD aggregations
□ EC-03: pct_of_medicare filter 10-500 active
□ EC-05: NADAC join uses latest effective_date per NDC
□ EC-06: HCBS waitlist returns 51 rows (state-level, not individual)

ADDITIONAL TESTS per module:

Rate-Quality:
- /api/research/rate-quality/measures → returns list of measure_ids
- /api/research/rate-quality/correlation?measure_id=prenatal_care → returns state-level scatter data

Managed Care Value:
- /api/research/mc-value/mco-summary → MLR data, verify adjusted_mlr is populated
- /api/research/mc-value/mco-financials → plan-level detail

Treatment Gap:
- /api/research/treatment-gap/mat-utilization → national total should be ~$978M (not $0)

Pharmacy Spread:
- /api/research/pharmacy-spread/stats → verify net_overpayment ~$3.15B
- /api/research/pharmacy-spread/top-drugs?limit=10 → verify drugs have both NADAC and SDUD prices

Nursing Ownership:
- /api/research/nursing-ownership/quality-by-type → verify 3+ ownership categories with ratings
- /api/research/nursing-ownership/chain-scoreboard?limit=10 → verify chain names and ratings

Waiver Impact:
- /api/research/waiver-impact/catalog → verify 600+ waiver records

PERFORMANCE CHECKS (from Section 9, PT tests):
- PT-04: Pharmacy spread overview < 10s
- PT-05: PBJ staffing summary < 15s

RULES:
- FIX automatically: endpoints returning 500, missing try/except blocks, response structure mismatches
- FLAG: endpoints returning empty data (0 rows) — may indicate data gap vs code bug
- Produce endpoint audit table:
| Module | Endpoint | Status | Rows | Time (ms) | Issues |
```

---

## PROMPT 3: Statistical Results Verification

**Goal:** Reproduce every corrected number from Session 30. The Rate-Quality p-value flip (0.178 → 0.044) is the highest-priority item.

```
Read CLAUDE.md, ARADUNE-COMPLETE-REFERENCE.md Section 10 (Corrected Research Findings), and docs/RESEARCH-FINDINGS.md.

Session 30 produced corrected statistical results. Your task: independently reproduce each one from the data.

CRITICAL INVESTIGATION — RATE-QUALITY P-VALUE:

The original findings paper says p=0.178 (rates do NOT predict quality).
Session 30 CLAUDE.md says p=0.044 (rates DO predict quality).
This is a complete narrative reversal. Determine which is correct and WHY they differ.

Steps:
1. Run the OLS: avg_quality ~ avg_pct_medicare + controls
2. Report the exact model specification (which variables, which sample)
3. If Session 30 used different controls than the original (e.g., dropped SVI after VIF diagnostics), document the change
4. If an IV/2SLS model was used instead of OLS, document the instrument and first-stage F-stat
5. Report: coefficient, SE, t-stat, p-value, R², N, and the full control variable list
6. FLAG FOR SCOTT: "The Rate-Quality headline finding changed from 'rates don't predict quality' to 'rates do predict quality.' Here's why: [explanation]. Here's the evidence: [numbers]. You need to decide which framing to use."

VERIFICATION OF ALL CORRECTED NUMBERS (from ARADUNE-COMPLETE-REFERENCE.md Section 10):

Use test IDs SM-01 through SM-06 from the Audit Test Catalog:

□ SM-01: Rate-quality correlation. Run OLS on fact_rate_comparison AVG(pct_of_medicare) vs fact_quality_core_set_2024 state_rate. Target: verify p-value (~0.044 or ~0.178 — determine which).

□ SM-02: Nursing ownership effect. Compare AVG(overall_rating) for-profit vs non-profit from fact_five_star. Target: d ~0.59, penalty ~0.67 stars.

□ SM-03: Pharmacy spread total. Sum (reimbursement_per_unit - nadac_per_unit) * total_units across matched NDCs. Target: ~$3.15B net.

□ SM-04: MC value per-enrollee differential. Regress per-enrollee spending on mc_penetration_pct. Target: coefficient ~-$16.

□ SM-05: Fiscal cliff spending growth. Calculate year-over-year per-enrollee change. Target: ~$489/enrollee/yr.

□ SM-06: Pharmacy spread filter validation. Verify NADAC join uses latest price per NDC (ROW_NUMBER window function).

ALSO VERIFY:
- MCO retention: $120B (or $113B — which is correct?)
- MAT national spending: $978M
- HCBS waitlist: 606,895 (or whatever the current total is)
- Quality trend: approximately -1.3pp/year
- Treatment desert states: 26

For each number, report:
| Metric | Reference Doc Value | Reproduced Value | Match? | Notes |

RULES:
- FIX automatically: calculation bugs in replication scripts
- FLAG for human decision: the Rate-Quality p-value (this changes the headline narrative), any number where reproduced value differs by >10% from reference doc
- Produce a definitive CORRECTED RESULTS TABLE that becomes the single source of truth
```

---

## PROMPT 4: Data Accuracy Benchmarks

**Goal:** Verify Aradune's data against independently known external values. Uses test IDs DA-01 through DA-10 from ARADUNE-COMPLETE-REFERENCE.md.

```
Read ARADUNE-COMPLETE-REFERENCE.md Section 9 (Audit Test Catalog), specifically Data Accuracy Benchmarks (DA-01 through DA-10).

Run EVERY benchmark test. For each, query the Aradune lake AND search the web for the external benchmark.

□ DA-01: Total CMS-64 computable across all states/years → Expected ~$5.7T
   Query: SELECT SUM(total_computable) FROM fact_cms64_multiyear

□ DA-02: Open Payments total → Expected ~$10.83B (3 CMS categories)
   Query: SELECT SUM(total_amount) FROM fact_open_payments

□ DA-03: LEIE active exclusions → Expected ~82K
   Query: SELECT COUNT(*) FROM fact_leie WHERE reinstatement_date IS NULL

□ DA-04: Nursing facilities (Five-Star) → Expected ~14,700
   Query: SELECT COUNT(*) FROM fact_five_star

□ DA-05: HPSA designations → Expected ~69K
   Query: SELECT COUNT(*) FROM fact_hpsa

□ DA-06: Section 1115 waivers → Expected ~647-665
   Query: SELECT COUNT(*) FROM fact_section_1115_waivers
   (Fallback: ref_1115_waivers, fact_kff_1115_waivers)

□ DA-07: SDUD 2025 total reimbursed → Cross-check with CMS published figures
   Query: SELECT SUM(total_amount_reimbursed) FROM fact_sdud_2025 WHERE state_code != 'XX'

□ DA-08: Enrollment totals → Within 5% of CMS Enrollment Dashboard
   Query: SELECT SUM(total_enrollment) FROM fact_enrollment WHERE year = 2024 AND month = 12

□ DA-09: Medicaid rate count per state → >100 per state for most
   Query: SELECT state_code, COUNT(*) FROM fact_medicaid_rate GROUP BY state_code ORDER BY COUNT(*)

□ DA-10: PBJ nurse staffing rows → Expected ~1.3M
   Query: SELECT COUNT(*) FROM fact_pbj_nurse_staffing

ALSO run cross-module consistency tests (CM-01 through CM-07):

□ CM-01: Enrollment totals consistent across fact_enrollment and fact_mc_enrollment_summary
□ CM-02: All state_codes in fact tables appear in dim_state
□ CM-03: Spending per enrollee = CMS-64 total / enrollment (spot check 5 states)
□ CM-06: FMAP rates between 0.50 and 0.83
□ CM-07: MC penetration does not exceed 100%

RULES:
- Pass = within 15% of known value (different time periods may cause variance)
- Fail = >15% off or order of magnitude wrong
- FLAG any failures — these indicate bad ETL, not bad research module code
- Do NOT attempt to fix data issues — just document them

Produce:
| Test ID | Query Result | Expected | % Diff | Pass/Fail | Notes |
```

---

## PROMPT 5: Advanced Methods Audit

**Goal:** Session 30 added sophisticated statistical methods. These are NEW and have NOT been independently verified. Audit each one.

```
Read CLAUDE.md, then read scripts/research_advanced_methods.py and docs/RESEARCH-ADVANCED-METHODS.md.

Session 30 added these advanced statistical methods. Each needs independent verification:

1. IV/2SLS (INSTRUMENTAL VARIABLES)
   - What instrument is used? (likely FMAP or GPCI)
   - Check: first-stage F-statistic > 10 (weak instrument test)
   - Check: exclusion restriction plausibility (does the instrument affect quality ONLY through rates?)
   - Check: Hausman test — does IV differ significantly from OLS?
   - If the Rate-Quality p=0.044 comes from IV rather than OLS, this changes the interpretation entirely. FLAG for Scott.

2. VIF DIAGNOSTICS
   - Run VIF on all control variables in the Rate-Quality model
   - Verify: any VIF > 10 was removed (likely SVI and poverty, which are near-collinear)
   - Report which variables were dropped and whether this explains the p-value change

3. PROPENSITY SCORE MATCHING (Nursing Homes)
   - Claims 10,737 matched pairs
   - Check: what covariates were used for matching? (beds, state, acuity?)
   - Check: standardized mean differences after matching (all should be < 0.1)
   - Check: does the matched estimate differ from the unmatched? (should be similar if the effect is robust)
   - Report: matched ATE, SE, p-value, Cohen's d

4. CHOW EVENT STUDY (Ownership Changes)
   - Claims 4,952 ownership transfers
   - Check: data source — is this from fact_snf_chow or fact_nh_ownership?
   - Check: pre-trend parallel assumption (was quality trending similarly before transfer?)
   - Check: event window (how many quarters before/after?)
   - Report: pre-transfer rating, post-transfer rating, difference, p-value

5. RANDOM FOREST (Pharmacy Spread)
   - Claims R²=0.75
   - Check: is this in-sample or out-of-sample (cross-validated) R²?
   - If in-sample only, FLAG — RF can overfit badly
   - Check: feature importance ranking — what predicts spread?
   - Run 5-fold cross-validation and report mean R² ± SD

6. QUANTILE REGRESSION
   - What outcome variable and what quantiles?
   - Verify the coefficients at each quantile make substantive sense

7. K-MEANS CLUSTERING
   - What variables are clustered?
   - How many clusters (k)?
   - Was k chosen by elbow method / silhouette score?
   - Report cluster centers and sizes

8. SPATIAL MISMATCH INDEX
   - How is "mismatch" defined? (likely OUD prevalence rank vs MAT spending rank)
   - Verify the index calculation
   - Report top 10 most mismatched states

RULES:
- FIX automatically: incorrect R² computations (e.g., in-sample reported as cross-validated), VIF calculations
- FLAG for human decision:
  - The IV instrument choice and exclusion restriction (this is a judgment call)
  - Whether the RF R² is cross-validated (if not, the 0.75 is meaningless)
  - The PSM covariate balance — if balance is poor, the matching is unreliable
  - The Rate-Quality p-value change: if it's driven by IV vs OLS, Scott needs to decide which to headline

Produce:
| Method | Module | Key Result | Verified? | Issues/Flags |
```

---

## PROMPT 6: Edge Cases & Robustness

**Goal:** Stress-test for scenarios the builder didn't consider. Uses EC-01 through EC-10 from ARADUNE-COMPLETE-REFERENCE.md.

```
Read ARADUNE-COMPLETE-REFERENCE.md Section 9, Edge Case Tests (EC-01 through EC-10).

Run ALL edge case tests, PLUS additional robustness checks:

FROM REFERENCE DOC:
□ EC-01: State code 'XX' excluded from SDUD aggregations
□ EC-02: Census sentinel -888888888 not treated as real value
□ EC-03: pct_of_medicare filter 10-500 (not >0 AND <10)
□ EC-04: Illinois T-MSIS claims not naively aggregated (incremental credit/debit)
□ EC-05: NADAC join uses latest effective_date per NDC
□ EC-06: HCBS waitlist is state-level (51 rows)
□ EC-07: MFCU stats use corrected column names
□ EC-08: PERM rates use 'year' not 'fiscal_year'
□ EC-09: Maternal morbidity geography join uses UPPER()
□ EC-10: Infant mortality requires FIPS mapping

ADDITIONAL TESTS:

DIVISION BY ZERO:
- Search all research route files for division operations (/)
- For each, verify NULLIF or CASE WHEN guard exists
- Specific risks:
  □ Per-enrollee calculations where enrollment = 0
  □ MLR where member_months = 0
  □ Markup ratio where nadac_per_unit = 0 (Sodium Chloride has NADAC = $0.00)
  □ Per-capita calculations where population is NULL

SQL INJECTION:
- Search for f-string or .format() SQL construction with user-supplied parameters
- The measure_id parameter in rate-quality is user input — must be parameterized ($1/$2)
- The state parameter in multiple endpoints — must be parameterized
- The search parameter in waiver-impact/catalog — must be parameterized

NULL PROPAGATION:
- Do AVG/SUM operations handle all-NULL columns gracefully?
- Does the frontend handle null values in chart data arrays? (Recharts handles null; undefined crashes)

TERRITORY HANDLING:
- Pass AS, GU, VI, MP, PR to every endpoint that accepts a state parameter
- Verify: graceful response (empty or valid data), never a 500 error

LARGE RESULT SETS:
- pharmacy-spread/top-drugs without limit → does it have a default LIMIT?
- waiver-impact/catalog → 665 rows, is there pagination?
- nursing-ownership/chain-scoreboard without limit → does it cap?

CONCURRENT ACCESS:
- Are DuckDB cursors thread-safe? (get_cursor() should return new cursor per request)
- Any shared mutable state in route files?

RULES:
- FIX automatically: missing NULLIF guards, unparameterized queries, missing default LIMITs
- FLAG: performance issues requiring architectural changes, territory data gaps
- Produce:
| Test ID | Result | Fix Applied |
```

---

## PROMPT 7: Frontend-Backend Contract

**Goal:** Every React component compiles, renders, and correctly maps API responses to UI elements.

```
Read CLAUDE.md and ARADUNE-COMPLETE-REFERENCE.md Section 5 (Research Module Detail) for endpoint response shapes.

Audit every frontend research component in src/tools/research/. Use the ARADUNE-COMPLETE-REFERENCE.md Section 5 endpoint documentation as the contract spec.

For EACH of the 10 components:

1. IMPORT VALIDATION
   - Verify imports resolve: ../../lib/api, ../../components/LoadingBar, ../../context/AraduneContext, ../../design
   - Verify design tokens exist: A, AL, POS, NEG, WARN, SF, BD, WH, cB, FM, SH
   - Verify shared components: Card, CH, Met, Pill, SafeTip, ChartActions

2. API ENDPOINT MAPPING
   - For each fetch call, verify the URL matches an endpoint from ARADUNE-COMPLETE-REFERENCE.md Section 5
   - Verify response destructuring matches actual response shape (from Prompt 2 results)
   - Example: if component does data.rows.map(r => r.adjusted_mlr), verify the endpoint returns adjusted_mlr

3. CHART DATA INTEGRITY
   - Recharts XAxis dataKey points to actual field
   - ResponsiveContainer has explicit height
   - Null values handled (not undefined)

4. COMPILATION TEST
   - Run npx tsc --noEmit on each file
   - Fix TypeScript errors

5. INTEGRATION WIRING (replaces old Prompt 6)
   - server/main.py: all 10 research routers imported and registered
   - Platform.tsx (or equivalent): all 10 components lazy-imported, in TOOLS array with group:"research", in NAV_GROUP, in toolMap
   - Paths match between frontend routes and backend prefixes

RULES:
- FIX: wrong import paths, mismatched API field names, TypeScript errors, missing null checks
- FLAG: design discrepancies, components expecting data the API doesn't provide

| Component | Imports | API Mapping | Charts | Types | Wiring | Status |
```

---

## PROMPT 8: Cross-Document Reconciliation

**Goal:** After all verification is complete, ensure every document agrees on every number.

```
Read CLAUDE.md (Session 30 entries), ARADUNE-COMPLETE-REFERENCE.md Section 10, docs/RESEARCH-FINDINGS.md, and docs/RESEARCH-REPLICATION-RESULTS.md.

Using the verified numbers from Prompts 3 and 5, update all documents to be consistent.

RECONCILIATION TABLE (fill in with verified values):

| Metric | CLAUDE.md | Complete Ref | Findings Paper | Replication | Verified | Action |
|--------|-----------|-------------|----------------|-------------|----------|--------|
| Rate-Quality p-value | 0.044 | ~0.178 | 0.178 | 0.214 | [run] | [update] |
| MCO retention | $120B | $113B | $113B | — | [run] | [update] |
| MAT national spending | $978M | — | $954M | $0 | [run] | [update] |
| Pharmacy net overpayment | $3.15B | $3.15B | $3.43B | $4.82B | [run] | [update] |
| Nursing FP penalty | -0.67 | -0.67 | -0.67 | -0.67 | [run] | [verify] |
| Nursing Cohen's d | — | 0.59 | 0.585 | — | [run] | [update] |
| Quality trend (pp/yr) | — | — | -1.27 | -1.232 | [run] | [update] |
| HCBS waitlist total | — | — | 606,895 | — | [run] | [verify] |
| Five-star facility count | — | 14,710 | 14,710 | — | [run] | [verify] |
| Open Payments total | $10.83B | $10.83B | $13.18B | — | [run] | [update] |
| Treatment desert states | 26 | — | — | — | [run] | [verify] |
| PSM matched pairs | 10,737 | — | — | — | [run] | [verify] |
| RF R² pharmacy | 0.75 | — | — | — | [run] | [verify] |

UPDATE PRIORITY:
1. RESEARCH-FINDINGS.md — this is the academic paper, highest stakes for accuracy
2. RESEARCH-REPLICATION-RESULTS.md — contains negative R² and broken queries, needs complete rewrite
3. ARADUNE-COMPLETE-REFERENCE.md Section 10 — authoritative reference, must match findings
4. CLAUDE.md Session 30 entry — brief, but the numbers people see first
5. RESEARCH-HANDOFF.md landing page metrics — drives what appears on the website

FLAG FOR SCOTT:
- If Rate-Quality p-value is verified as 0.044: the Findings paper needs a new interpretation section (rates DO predict quality after proper specification). Should the headline change?
- If the Findings paper's narrative sections reference p=0.178, they need rewriting.
- The landing page metric cards in RESEARCH-HANDOFF.md — are the recommended 4 metrics still the right ones given corrected numbers?

RULES:
- FIX: update numbers in all documents to match verified values
- FLAG: any case where corrected numbers invalidate the interpretive text
- Produce a final reconciliation table showing which documents were updated and what changed
```

---

## Post-Audit Checklist

After all 8 prompts:

- [ ] All 44 research endpoints return 200 with valid data
- [ ] All 10 frontend components compile without TypeScript errors
- [ ] All schema fixes from Session 30 verified as present in code
- [ ] Rate-Quality p-value independently reproduced and narrative resolved
- [ ] Advanced methods (IV, PSM, RF, CHOW) independently verified
- [ ] RF R²=0.75 confirmed as cross-validated (not in-sample)
- [ ] PSM covariate balance verified (standardized mean diff < 0.1)
- [ ] No division-by-zero risks remain unguarded
- [ ] No SQL injection vulnerabilities (all user inputs parameterized)
- [ ] Key numbers match external benchmarks within 15%
- [ ] All 5 reference documents agree on all key numbers
- [ ] Landing page headline metrics are correct
- [ ] Replication script runs end-to-end without errors

---

## Decision Log Template

### FLAG [N]: [Brief Description]

**Module:** [which research module]
**Location:** [file:line or endpoint]
**Issue:** [what's wrong]
**Options:**
1. [Option A]: [description, trade-offs]
2. [Option B]: [description, trade-offs]
3. [Option C if applicable]
**Recommendation:** [which option and why]
**Decision:** [to be filled in by Scott]

---

## Key Decision: Rate-Quality Headline

This is the single most important decision coming out of this audit.

**Before Session 30:** "Rates don't predict quality (p=0.178)." The findings paper is built around this.
**After Session 30:** "Rates predict quality (p=0.044)." CLAUDE.md reports this.

These are opposite conclusions. The audit must determine:
1. Which model specification produced each p-value
2. Whether the change is due to variable selection (VIF → dropped SVI/poverty), IV estimation, sample change, or the filter fix
3. Which result is more defensible
4. Whether the findings paper needs a full rewrite or just an update

Scott decides the headline framing. The audit provides the evidence.
