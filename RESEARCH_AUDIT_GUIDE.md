# Aradune Research Modules — Forensic Audit Guide

**Purpose:** Systematic stress-test of all 10 research modules, every endpoint, every formula, every data pull. Designed as a series of Claude Code prompts that should be run sequentially. Each prompt is self-contained and tells Claude Code what to fix automatically vs. what to flag for human decision.

**Critical context:** The research modules were built by a separate Claude Code terminal that did NOT have live access to the database schema. This means column names, table names, and join keys were often guessed. The replication results document proves this — multiple queries failed with schema errors, and the statistical results in the replication script contradict the findings paper.

---

## Pre-Audit: Known Red Flags

Before running any prompts, here's what I found wrong by reading the documents side-by-side:

### 1. Statistical Results Contradict Across Documents

The RESEARCH-REPLICATION-RESULTS.md and RESEARCH-FINDINGS.md are supposed to describe the same analyses but give **different numbers everywhere:**

| Metric | Replication | Findings Paper | Problem |
|--------|-------------|----------------|---------|
| OLS R² | **-0.918** | 0.348 | Negative R² is impossible for OLS. Implementation bug. |
| OLS Rate coefficient | 0.067 (p=0.214) | 0.042 (p=0.178) | Different coefficients, different p-values |
| OLS intercept SE | **88,150,959** | 47.90 | Off by 7 orders of magnitude. Perfect collinearity. |
| OLS SVI SE | **176,304,139** | 0.088 | Same — SVI is collinear with something |
| Panel N | 378 obs, 49 states | 395 obs, 51 states | Different sample sizes |
| Panel within-R² | 0.142 | 0.172 | Different |
| Panel MC coeff | -0.100 | -0.094 | Different |
| Panel year trend | -1.232 | -1.271 | Different |
| DiD estimate | +1.68pp (p=0.440) | -0.20pp (p=0.907) | **Opposite sign, different magnitude** |
| Pharmacy overpayment | $4.82B | $3.43B | $1.4B discrepancy |
| Pharmacy N drugs | 23,530 | 23,617 | Different counts |
| Pharmacy median markup | 2.27x | not reported same way | — |

**Verdict:** One of these documents was fabricated from memory/estimates rather than actual query results. The replication script's negative R² and billion-scale standard errors prove it has bugs. The findings paper's numbers are plausible but unverified. **Neither can be trusted as ground truth.** The audit must re-derive everything from scratch.

### 2. SQL Queries That Failed in Replication

These queries crashed when actually run against the database:

1. **MLR analysis:** `WHERE mlr IS NOT NULL` → column `mlr` doesn't exist. Actual columns: `mlr_numerator`, `mlr_denominator`
2. **Nursing home State FE:** `COALESCE(number_of_certified_beds, 0)` → column doesn't exist. Actual: `certified_beds`
3. **MAT spending:** Returned **$0M total** — the `SIMILAR TO` regex on `product_name` matched nothing

### 3. Schema-Discovery Fallbacks = Code Smell

The build summary documents that 5+ routes use "try a query, catch error, try different column name" patterns. This is defensive but means:
- First calls to these endpoints are slow (multiple failed queries)
- If all fallbacks fail, the user gets a 500 error with no useful message
- The "discovered" schema might not be what the module actually needs

### 4. Documentation Line Count Discrepancies

| Metric | Build Summary | Handoff |
|--------|--------------|---------|
| Backend lines | 1,971 | 2,025 |
| Backend endpoints | 31 | 45 |
| Frontend lines | 5,802 | 5,915 |

These documents were written in the same session about the same code. If the metadata is wrong, what else is?

### 5. Headline Numbers Used in Multiple Places

The handoff recommends specific numbers for the landing page (e.g., "$3.4B pharmacy overpayment", "607K HCBS waitlist", "-1.3pp/yr quality decline"). If any of these are wrong, they'll be wrong everywhere. Each needs independent verification.

---

## Audit Structure

The audit is divided into **8 prompts**, each targeting a specific layer. Run them in order — later prompts depend on fixes made by earlier ones.

| # | Prompt | Scope | Est. Time |
|---|--------|-------|-----------|
| 1 | Schema Validation | Every SQL query against actual lake schema | 20-30 min |
| 2 | Backend Route Audit | Every endpoint: crash test + response validation | 20-30 min |
| 3 | Statistical Methods Audit | OLS, Panel FE, DiD, Cohen's d implementations | 30-45 min |
| 4 | Data Accuracy Spot Checks | Key numbers verified against known sources | 20-30 min |
| 5 | Frontend Component Audit | Every component renders, tabs work, data flows | 20-30 min |
| 6 | Integration Wiring | Platform.tsx + main.py registration correctness | 10-15 min |
| 7 | Cross-Document Reconciliation | Findings paper, replication, build summary, handoff | 15-20 min |
| 8 | Edge Cases & Robustness | Empty states, nulls, division by zero, outliers | 15-20 min |

---

## PROMPT 1: Schema Validation

**Goal:** Verify every SQL query in every backend route against the actual DuckDB lake schema. Fix column names, table names, and join keys. This is the foundation — nothing else matters if the queries don't run.

```
Read CLAUDE.md, then read docs/RESEARCH-MODULES-BUILD-SUMMARY.md and docs/RESEARCH-HANDOFF.md for context on what was built.

Your task: Forensic schema validation of all 10 research module backend routes in server/routes/research/. The person who built these did NOT have live access to the database schema. Column names and table names were often guessed. Multiple queries are known to fail.

For EACH of the 10 route files (rate_quality.py, mc_value.py, treatment_gap.py, safety_net.py, integrity_risk.py, fiscal_cliff.py, maternal_health.py, pharmacy_spread.py, nursing_ownership.py, waiver_impact.py):

1. Read the file
2. Extract every SQL query string
3. For each query, run a DESCRIBE or SELECT * LIMIT 0 on every referenced table to get the actual column names
4. Compare the query's column references against the actual schema
5. If a column doesn't exist, find the correct column name and fix the query
6. If a table doesn't exist, check what similar tables exist (e.g., fact_nadac vs fact_nadac_mar2026)
7. Test each query by running it (with LIMIT 10 if it's a large result) to confirm it executes without error

KNOWN FAILURES (from the replication run):
- fact_mco_mlr: column "mlr" doesn't exist. Candidates: mlr_numerator, mlr_denominator
- fact_five_star: column "number_of_certified_beds" doesn't exist. Candidate: certified_beds
- fact_sdud_2025: SIMILAR TO on product_name for MAT drugs returned 0 rows (regex may be wrong, or column name may differ)
- fact_perm_rates: schema uncertain
- fact_mfcu_stats: schema uncertain
- ref_1115_waivers / fact_kff_1115_waivers / fact_section_1115_waivers: table name uncertain

RULES:
- FIX automatically: wrong column names, wrong table names, missing WHERE clauses, incorrect JOINs
- FLAG for human decision: cases where the data doesn't exist in the lake at all, cases where the query logic is ambiguous (i.e., you're not sure what the developer intended), cases where fixing the query would change the analytical meaning
- For every fix, add a comment: # AUDIT FIX: [old] -> [new], reason: [schema mismatch]
- For every flag, add a comment: # AUDIT FLAG: [description of issue, options for resolution]
- Keep a running tally: [file]: [N fixes, M flags]

After completing all 10 files, produce a summary table:
| File | Queries | Fixes | Flags | Status |
```

---

## PROMPT 2: Backend Route Audit

**Goal:** Every endpoint must return valid data, not crash, and have proper error handling. This runs AFTER Prompt 1 has fixed the schema issues.

```
Read CLAUDE.md. Read docs/RESEARCH-HANDOFF.md for the endpoint list.

Prompt 1 fixed the SQL schema issues in server/routes/research/. Now audit every endpoint for runtime correctness.

For EACH endpoint across all 10 route files:

1. Start the FastAPI server (or import the route and test the handler directly)
2. Call the endpoint with default parameters
3. Call the endpoint with edge-case parameters (empty state, invalid measure_id, etc.)
4. Verify the response structure matches what the frontend expects:
   - Must return {"rows": [...], "count": N} OR a documented alternative
   - Rows must contain the keys the frontend component references
5. Check error handling:
   - Does a bad query parameter return 400, not 500?
   - Does a missing table return a useful error message?
   - Are schema-discovery fallbacks actually working?

SPECIFIC TESTS (from known issues):

A. Rate-Quality endpoints:
   - /api/research/rate-quality/measures — verify it returns a list of measure_ids
   - /api/research/rate-quality/correlation?measure=W30-CH — verify it returns state-level data with rate and quality columns
   - /api/research/rate-quality/workforce — verify BLS wage data joins correctly (SOC codes 29-/31-)

B. Managed Care Value:
   - /api/research/mc-value/mco-summary — verify MLR is computed from mlr_numerator/mlr_denominator, not a missing "mlr" column
   - /api/research/mc-value/mco-financials — verify plan-level detail works

C. Treatment Gap:
   - /api/research/treatment-gap/mat-utilization — verify MAT drugs actually match (this returned $0 in replication)
   - /api/research/treatment-gap/demand-supply — verify the NSDUH measure_id probe finds a valid measure

D. Safety Net:
   - /api/research/safety-net/staffing-crisis — verify PBJ staffing computation (nursing_hprd vs total_nursing_hrs/mds_census)

E. Integrity Risk:
   - /api/research/integrity-risk/enforcement — verify MFCU schema discovery
   - /api/research/integrity-risk/perm — verify PERM schema discovery

F. Waiver Impact:
   - /api/research/waiver-impact/catalog — verify which of the 3 table names actually exists

G. Pharmacy Spread:
   - /api/research/pharmacy-spread/stats — verify NADAC table name (fact_nadac vs fact_nadac_mar2026)
   - /api/research/pharmacy-spread/top-drugs — verify NDC join between NADAC and SDUD

H. Nursing Ownership:
   - /api/research/nursing-ownership/quality-by-type — verify ownership_type classification logic
   - /api/research/nursing-ownership/chain-scoreboard — verify chain_name field exists

RULES:
- FIX automatically: response structure mismatches, missing try/except, incorrect status codes, broken fallbacks
- FLAG for human decision: endpoints that return empty data because the underlying table is empty or doesn't have the expected data
- For each endpoint, log: [endpoint] [status_code] [row_count] [pass/fail] [fix/flag if applicable]

Produce a final endpoint audit table:
| Module | Endpoint | Status | Rows | Issues |
```

---

## PROMPT 3: Statistical Methods Audit

**Goal:** Verify the correctness of every statistical computation. The replication script produced a negative R² (impossible for OLS) and standard errors in the billions. The implementations are broken.

```
Read CLAUDE.md. Read docs/RESEARCH-FINDINGS.md and docs/RESEARCH-REPLICATION-RESULTS.md.

The statistical computations in the research modules have critical bugs. The replication script produced:
- R² = -0.918 for an OLS regression (impossible — OLS R² is always between 0 and 1)
- Standard errors of 88 million and 176 million for intercept and SVI (indicates perfect multicollinearity)
- A DiD estimate of +1.68pp when the findings paper says -0.20pp (opposite sign)

Your task: Find and fix every statistical implementation. The computation code is in scripts/research_replication.py (and possibly inline in the route files if any routes compute statistics server-side).

SPECIFIC AUDITS:

1. OLS IMPLEMENTATION
   - Find the OLS code. Check: is R² computed as 1 - SS_res/SS_tot? This should always be [0,1] for in-sample OLS.
   - If R² is negative, the code is likely using a wrong baseline (e.g., comparing to a different model, or computing out-of-sample R²)
   - Check for multicollinearity: compute the VIF for each predictor. SVI and poverty rate are likely near-perfectly collinear (SVI includes poverty as a component). If VIF > 10, the model is unstable.
   - Fix: either drop one of the collinear variables (recommend dropping SVI since poverty_rate is more interpretable) or document the collinearity
   - Recompute all OLS results and compare to both the replication doc and findings paper

2. PANEL FIXED EFFECTS
   - Find the panel FE code. Check: is it using within-transformation (demeaning by group)?
   - Verify: the demeaning should subtract group means from both Y and all X variables
   - Check: are standard errors clustered by state? They should be for panel data.
   - Verify the sample: the replication says N=378, 49 states. The findings say N=395, 51 states. Run the data query and report the actual N.
   - Recompute and compare.

3. DIFFERENCE-IN-DIFFERENCES
   - Find the DiD code. Check: is the treatment definition correct? (FMAP ≤ 52% = high burden = treatment)
   - Verify: DiD = (treatment_post - treatment_pre) - (control_post - control_pre)
   - The replication says DiD = +1.68pp. The findings say DiD = -0.20pp. These can't both be right. One has the signs flipped.
   - Check: what are the actual pre/post means for treatment and control groups?
   - Recompute and report.

4. COHEN'S D
   - Find the Cohen's d code. Verify: d = (M1 - M2) / pooled_SD
   - Pooled SD = sqrt(((n1-1)*s1² + (n2-1)*s2²) / (n1+n2-2))
   - The findings report d=0.585. Verify this is correct.

5. CROSS-CHECK ALL HEADLINE NUMBERS
   After fixing the implementations, recompute and report the corrected values for:
   - Rate-Quality OLS: coefficient, SE, t, p, R²
   - Panel FE: MC coefficient, year trend, within-R²
   - DiD: estimate, SE, p
   - Nursing home: for-profit penalty stars, Cohen's d
   - Pharmacy: total overpayment, net overpayment, median markup
   - MCO: average MLR, % below 85%, total retention
   - National quality trend: pp/year

RULES:
- FIX automatically: R² formula bugs, sign errors, wrong demeaning, missing clustering
- FLAG for human decision: model specification choices (which controls to include, treatment definition for DiD, measure selection for quality composite)
- Every fix must include before/after comparison of the result
- Produce a CORRECTED version of the key results table that can replace both the replication doc and the findings paper
```

---

## PROMPT 4: Data Accuracy Spot Checks

**Goal:** Verify key numbers against independently known values. If Aradune says Florida has X enrollment, does CMS agree?

```
Read CLAUDE.md.

Spot-check the data accuracy of the research modules by verifying Aradune's numbers against known external benchmarks. For each check, query the Aradune lake, state the result, and compare to the known value.

SPOT CHECKS:

1. HCBS WAITLIST
   - Aradune claims: 606,895 people waiting nationally
   - External source: KFF publishes HCBS waiting lists. Search the web for the most recent KFF HCBS waiting list total.
   - Query: SELECT SUM(waitlist_count) FROM fact_hcbs_waitlist (or whatever the actual column is)
   - Compare. If >10% off, flag.

2. NATIONAL MEDICAID ENROLLMENT
   - Aradune has fact_enrollment. Query total enrollment for the most recent year.
   - External: CMS monthly enrollment reports. Verify against known total (~80-90M during unwinding, declining from ~100M peak).

3. FLORIDA ENROLLMENT (our state, we know this)
   - Query FL enrollment from fact_enrollment for latest available.
   - Known: FL Medicaid enrollment was approximately 5.5-6M during 2024.

4. MCO MLR NATIONAL AVERAGE
   - Aradune claims: 91.9% average MLR
   - Query: SELECT AVG(mlr_numerator/mlr_denominator * 100) FROM fact_mco_mlr WHERE mlr_denominator > 0
   - External: CMS MLR reports are published. Typical national Medicaid MCO MLR is ~88-92%.

5. FIVE-STAR FACILITY COUNT
   - Aradune claims: 14,710 facilities in fact_five_star
   - Query: SELECT COUNT(*) FROM fact_five_star
   - External: CMS Care Compare has ~15,000 nursing facilities. Should be close.

6. NADAC RECORD COUNT
   - Query: SELECT COUNT(*) FROM fact_nadac (or fact_nadac_mar2026)
   - Aradune claims ~1.9M rows. Verify.

7. OPEN PAYMENTS TOTAL
   - Aradune claims: $13.18B total (or $10.83B depending on document)
   - Query: SELECT SUM(total_amount) FROM fact_open_payments (or similar)
   - External: CMS Open Payments publishes annual totals. ~$12-13B is plausible for all categories.

8. FMAP RANGE
   - Query: SELECT MIN(fmap), MAX(fmap), AVG(fmap) FROM dim_state WHERE fmap IS NOT NULL
   - Known: FMAP ranges from 50% (floor) to ~77% (Mississippi). Average ~57%.

9. CMS-64 TOTAL MEDICAID SPENDING
   - Query: SELECT SUM(total_computable) FROM fact_cms64_multiyear WHERE fiscal_year = 2024
   - Known: Total Medicaid spending ~$800-900B. Verify order of magnitude.

10. SDUD TOTAL REIMBURSEMENT
    - Query: SELECT SUM(total_amount_reimbursed) FROM fact_sdud_2025 WHERE state_code != 'XX'
    - Known: State drug utilization is a significant portion of Medicaid pharmacy spending.

RULES:
- For each check: report [Table] [Query Result] [External Benchmark] [% Difference] [Pass/Fail]
- Pass = within 15% of known value (accounting for different time periods)
- Fail = >15% off, or order of magnitude wrong
- FLAG any failures — these indicate either bad data or bad queries
- Do NOT fix data issues here — just document them. Data fixes require understanding the ETL pipeline.
```

---

## PROMPT 5: Frontend Component Audit

**Goal:** Verify every React component compiles, renders, and correctly maps API response data to UI elements.

```
Read CLAUDE.md. Read docs/RESEARCH-HANDOFF.md for integration instructions.

Audit every frontend research component in src/tools/research/. The builder claims they follow the exact patterns from BehavioralHealth.tsx and PharmacyIntelligence.tsx. Verify this.

For EACH of the 10 component files:

1. IMPORT VALIDATION
   - Verify all imports resolve:
     - ../../lib/api (the API client)
     - ../../components/LoadingBar
     - ../../context/AraduneContext
     - ../../design (design tokens)
   - Check if these exact paths exist. If the project uses different paths, fix.
   - Verify the imported design tokens match the actual exports from design.ts/design.tsx:
     - A, AL, POS, NEG, WARN, SF, BD, WH, cB, FM, SH — do all of these exist?
   - Verify shared component imports: Card, CH, Met, Pill, SafeTip, ChartActions — do all exist?

2. API ENDPOINT MAPPING
   - For each useEffect/fetch call, verify:
     - The URL matches an actual backend endpoint (from Prompt 2)
     - The response destructuring matches the actual response shape
     - Example: if the component does `data.rows.map(r => r.avg_pct_medicare)`, verify that the endpoint actually returns rows with an `avg_pct_medicare` field

3. RECHARTS USAGE
   - Verify chart data arrays are correctly shaped for the Recharts components used
   - Check for common bugs: XAxis dataKey pointing to wrong field, missing YAxis domain, ResponsiveContainer without explicit height

4. DESIGN TOKEN CONSISTENCY
   - Verify color usage matches Aradune v14 design system
   - Check: no hardcoded hex colors that should be design tokens
   - Check: responsive behavior uses useIsMobile(768)

5. TAB STRUCTURE
   - Verify each component has exactly 4 tabs as documented
   - Verify tab switching works (state management correct)

6. TYPE SAFETY
   - Run `npx tsc --noEmit` on each file (or the whole project)
   - Fix any TypeScript errors

RULES:
- FIX automatically: wrong import paths, mismatched API field names (if the correct field name is clear from Prompt 2), TypeScript errors, missing null checks
- FLAG for human decision: design discrepancies where it's unclear which pattern is "correct", cases where the component expects data the API doesn't provide
- Produce a component audit table:
| Component | Imports | API Mapping | Charts | Types | Status |
```

---

## PROMPT 6: Integration Wiring

**Goal:** Verify that the Platform.tsx and main.py changes are correct and complete.

```
Read CLAUDE.md. Read docs/RESEARCH-HANDOFF.md.

Verify the integration wiring for the research modules.

1. BACKEND (server/main.py)
   - Open main.py
   - Check: are all 10 research route files imported?
   - Check: are all 10 routers registered with app.include_router()?
   - Check: do the import paths match the actual file locations?
   - Check: is __init__.py present in server/routes/research/?
   - Check: does the research package not conflict with any existing routes (prefix collisions)?

2. FRONTEND (Platform.tsx or equivalent routing file)
   - Open the main routing file
   - Check: are all 10 research components lazy-imported?
   - Check: are all 10 added to the TOOLS array with group: "research"?
   - Check: is the "research" NAV_GROUP defined?
   - Check: are all 10 added to toolMap with correct paths?
   - Check: do the paths match the backend route prefixes?

3. NAVIGATION
   - Verify the research group appears in the nav
   - Verify each tool has: name, description, path, icon, group
   - Check for duplicates or conflicts with existing tools

4. COMPILATION TEST
   - Run npm run build (or equivalent) to verify the frontend compiles with all new components
   - Fix any import errors

RULES:
- FIX automatically: missing imports, wrong paths, missing registrations
- FLAG for human decision: nav ordering, tool descriptions, icon choices
- This should be quick — it's mostly checking that copy-paste instructions were followed correctly
```

---

## PROMPT 7: Cross-Document Reconciliation

**Goal:** After Prompts 1-6 have fixed the code, regenerate all key numbers and produce a single source-of-truth document that replaces the conflicting docs.

```
Read CLAUDE.md.

After the previous audit prompts have fixed schema issues, statistical bugs, and endpoint errors, it's time to reconcile the documentation.

The problem: RESEARCH-FINDINGS.md, RESEARCH-REPLICATION-RESULTS.md, RESEARCH-MODULES-BUILD-SUMMARY.md, and RESEARCH-HANDOFF.md all contain specific numbers that were generated during the build. Many of these numbers conflict with each other (see the Pre-Audit table in the audit guide). Now that the code is fixed, we need one authoritative set of numbers.

TASK:

1. Run the corrected replication script (scripts/research_replication.py) end to end
2. Capture every key number it produces
3. Compare each number against what's claimed in RESEARCH-FINDINGS.md
4. Produce a RECONCILIATION TABLE:

| Metric | Old Replication | Old Findings | New Corrected | Changed? |
|--------|----------------|--------------|---------------|----------|

5. Update RESEARCH-FINDINGS.md with the corrected numbers
6. Update RESEARCH-REPLICATION-RESULTS.md to match
7. Update the headline metrics in RESEARCH-HANDOFF.md (the landing page metrics section)
8. Update the "Key Findings" section in RESEARCH-HANDOFF.md

SPECIFIC NUMBERS TO RECONCILE:
- Rate-Quality OLS: R², rate coefficient, p-value, N
- Panel FE: N, groups, within-R², MC coefficient, year trend
- DiD: estimate, p-value, treatment/control group sizes
- MC Value panel: N, groups, MC coefficient, year trend, within-R²
- MCO MLR: average, median, % below 85%, total retention $
- Nursing home: for-profit penalty (stars), Cohen's d, N per group
- Pharmacy: total overpayment, net overpayment, drugs matched, median markup
- MAT spending: national total (was $0 — should now be correct)
- Quality trend: pp/year, p-value

FLAG any number where the corrected value is dramatically different from what's in the findings paper (>20% change), as this may affect the narrative/interpretation sections.

Do NOT change interpretive text unless a corrected number invalidates the conclusion. For example, if the for-profit nursing penalty goes from 0.67 stars to 0.65 stars, the interpretation is fine. If it goes from 0.67 to 0.15, the "strongest finding" claim needs revision.
```

---

## PROMPT 8: Edge Cases & Robustness

**Goal:** Stress-test for scenarios that the builder likely didn't consider.

```
Read CLAUDE.md.

Final robustness pass on all research modules. Test for edge cases and failure modes.

1. EMPTY STATE HANDLING
   For each endpoint that accepts a state parameter:
   - Pass a valid state with sparse data (e.g., 'AS', 'GU', 'VI', 'MP' — territories)
   - Pass an invalid state code ('ZZ', '', null)
   - Verify the response is either empty with a clear message, or gracefully excludes the state
   - No 500 errors

2. DIVISION BY ZERO
   Search all route files for division operations (/). For each:
   - Is there a NULLIF or CASE WHEN denominator = 0 guard?
   - Specific risks:
     - Per-enrollee calculations where enrollment could be 0
     - MLR computation where mlr_denominator could be 0
     - Per-capita calculations where population could be null
     - Markup ratio where NADAC could be 0 (Sodium Chloride in the top drugs list has NADAC = $0.00)

3. NULL PROPAGATION
   For each query that uses AVG(), SUM(), or COUNT():
   - Are NULLs properly handled? (AVG ignores NULLs by default in DuckDB, but if the column is all NULL, AVG returns NULL)
   - Does the frontend handle null values in chart data? (Recharts typically handles null gracefully but undefined causes crashes)

4. LARGE RESULT SETS
   For endpoints that return potentially large result sets:
   - Is there a LIMIT clause or pagination?
   - The pharmacy spread top-drugs endpoint could return thousands of rows
   - The waiver catalog could return 647 rows

5. CONCURRENT ACCESS
   - Are the DuckDB cursors thread-safe? (get_cursor() should return a new cursor per request)
   - Is there any shared mutable state in the route files?

6. SQL INJECTION
   - Search for any f-string or .format() SQL construction with user-supplied parameters
   - All parameters should use parameterized queries ($1, $2) not string interpolation
   - The measure_id parameter in rate-quality is a user input — verify it's parameterized

7. PERFORMANCE
   For each endpoint:
   - Note the approximate response time
   - Flag any endpoint taking >5 seconds
   - The pharmacy spread analysis joins 1.9M NADAC rows to 2.6M SDUD rows — this could be slow
   - The ROW_NUMBER optimization for NADAC latest-price was noted as critical — verify it's present

RULES:
- FIX automatically: missing NULLIF guards, unparameterized queries, missing LIMIT clauses
- FLAG for human decision: performance issues requiring architectural changes (e.g., pre-computed materialized views), large result sets where the appropriate limit is unclear
- Produce a robustness report:
| Test | Endpoints Affected | Result | Fix/Flag |
```

---

## Post-Audit Checklist

After all 8 prompts are complete, verify:

- [ ] All 45 endpoints return 200 with valid data
- [ ] All 10 frontend components compile without TypeScript errors
- [ ] All statistical results are internally consistent across documents
- [ ] No SQL queries reference non-existent columns or tables
- [ ] No division-by-zero risks remain unguarded
- [ ] All user-supplied parameters are properly parameterized
- [ ] Key numbers match external benchmarks within 15%
- [ ] The findings paper's conclusions are still supported by corrected numbers
- [ ] The landing page headline metrics are correct
- [ ] The replication script runs end-to-end without errors

---

## Decision Log Template

For items flagged for human decision, use this format:

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
