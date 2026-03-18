# Aradune Full-Stack Audit Guide

## Purpose

This document provides a sequenced set of Claude Code prompts designed to perform a comprehensive audit of Aradune's entire pipeline — from raw data ingestion through ETL, Gold table production, ontology integrity, analytical tool correctness, AHEAD/Meridian model validation, Intelligence endpoint accuracy, and end-to-end report generation. Every prompt produces a concrete deliverable (a report, test suite, or scorecard) that doubles as due diligence documentation.

The goal: every number Aradune surfaces must be traceable, defensible, and able to survive scrutiny from rate-setting actuaries, legislative budget analysts, and senior Medicaid consultants.

---

## Run Order

| # | Audit Layer | Why This Order |
|---|-------------|----------------|
| 1 | Data Integrity Sweep | If ETL is wrong, everything downstream is garbage. |
| 2 | Gold Table Spot-Check | Validate output against publicly verifiable truths. |
| 3 | DOGE T-MSIS Quarantine | Neutralize the highest-risk data credibility landmine. |
| 4 | Ontology & Graph Consistency | Ensure the structural layer that powers RAG is sound. |
| 5 | Tool-by-Tool Functional Audit | Verify each analytical tool produces defensible output. |
| 6 | Meridian AHEAD Global Budget Audit | Validate the hospital global budget calculator against CMS model specs. |
| 7 | Intelligence Endpoint Regression | Stress-test the natural-language interface with 30 queries. |
| 8 | End-to-End Workflow Smoke Test | Simulate a real analyst session from lookup to report. |

---

## Anchor Facts Reference

The following verified facts are used as ground truth throughout the audit. Sources are authoritative (SSEC, AHCA, CMS Federal Register, KFF, EDR).

### Enrollment

- FL total Medicaid caseload FY 2024-25: **~4,226,347** (SSEC Dec 2025 exec summary)
- FL total Medicaid caseload FY 2023-24: **~4,836,670** (SSEC), a 13.3% YoY decline from the pandemic peak of 5,575,548
- FL has **NOT** adopted ACA Medicaid expansion — any expansion enrollment categories in the data are invalid
- KFF reports approximately **4,343,000** children and adults enrolled (May 2025 snapshot; slight methodological differences from SSEC point-in-time counts are expected)

### Fee Schedule / Rate-Setting

- CPT 99202 base Medicaid rate (Jan 2025 Practitioner Fee Schedule): **$55.15** (pre-stacking, FSI base)
- Rate stacking formula for physician provider types 25/26 with applicable specialties: FSI × 1.04 × 1.24 × 1.164 × 1.102 (example: 99202 = $55.15 → $91.23 fully stacked)
- MPIP enhanced rate for pediatric E&M (SFY 2025-26): **106.3%** of CY2025 Medicare rate
- APRNs/PAs billing independently (physician not in building): reimbursed at **80%** of physician allowable
- Multiple procedure reduction (modifier 51): 1st at 100%, 2nd at 50%, 3rd+ at 25%
- **Structural pattern:** Facility and PC/TC rates are typically mutually exclusive (99.96% of codes). Three codes (**46924, 91124, 91125**) legitimately carry both facility and PC/TC rates as published by AHCA.

### Fiscal / FMAP

- FL base FMAP FY2025-26 (Oct 2025–Sep 2026): **57.22%** (confirmed, Federal Register & SSEC)
- FL preliminary base FMAP FY2026-27: **~55.43%** (EDR estimate)
- Total Medicaid spending FY 2025-26: **~$35.6 billion** (federal share ~$20B, state share ~$15.6B) per SSEC/Florida Policy Institute
- FL receives approximately **$1.34** in federal funding per $1 of state Medicaid spending

### Managed Care

- SMMC 3.0 launched **February 1, 2025**
- Program now operates across **9 regions** (reduced from 11 under SB 1950)
- Three main components: **MMA, LTC, and Dental**, plus the new **ICMC** program (Intellectual & Developmental Disabilities Comprehensive Managed Care)

### AHEAD Model (CMS)

- Voluntary state-level total cost of care model, renamed "Achieving Healthcare Efficiency through Accountable Design" in 2025, extended through **December 2035**
- Six participating states: **Maryland** (Cohort 1); **Connecticut, Hawaii, Vermont** (Cohort 2); **Rhode Island, select New York counties** (Cohort 3)
- Core mechanism: hospital global budgets replacing FFS claims for inpatient and outpatient facility services, paired with primary care investment via **PC AHEAD**
- Global budgets built from **3 years of historical revenue**, weighted **10/30/60** toward the most recent year
- **Transformation Incentive Adjustment (TIA):** 1% upward adjustment to Medicare FFS global budget for PY1/PY2 hospitals
- TCOC adjustment for acute care hospitals starts **upside-only in PY4** (based on PY2 performance) — downside risk activates only thereafter
- States must implement **Medicaid global budgets by end of PY1**; at least one commercial payer must participate by PY2
- By PY4, at least **30% of a state's Medicare FFS hospital net patient revenue** must be under a global budget
- **Critical Access Hospitals** get: upside-only adjustments for a longer runway, delayed penalties for avoidable utilization, longer capacity-building period, and a payment floor based on most recent cost report
- Florida is **NOT** an AHEAD participating state — the model is concentrated in states with existing all-payer or rate-setting infrastructure (Maryland, Vermont, Connecticut)
- Key concern: global budgets built on historical prices risk locking in historical inefficiencies and high prices
- CMS retains significant discretion in classifying volume shifts as "appropriate patient choice" vs. "unplanned" — this distinction drives whether volume changes are rewarded or penalized
- AHEAD may spur vertical consolidation as hospitals acquire specialty groups to bring more spending under the budget umbrella

---

## Prompt 1: Data Integrity Sweep (Bronze → Silver → Gold)

**Deliverable:** Markdown report grouped by domain with severity ratings.

```
Audit every ETL script in the pipeline. For each of the 77 scripts, trace the
source file or table it ingests, the transformations applied, and the Gold table
it produces. Flag any script where:

(a) a column is referenced but doesn't exist in the source schema,

(b) a join key could produce fan-out (1:many duplication) or orphan rows
    (unmatched keys dropped silently),

(c) a filter or WHERE clause silently drops records that downstream tools
    might expect to find,

(d) hardcoded values — dates, state FIPS codes, rate amounts, FMAP
    percentages, eligibility categories — are stale or incorrect.
    Cross-check any hardcoded FMAP value against the confirmed FY2025-26
    base FMAP of 57.22%.

Check that SCD Type 2 logic is correctly implemented for all reference data —
specifically that effective/expiration dates don't overlap and that
current-record flags are consistent.

Produce a markdown report grouped by domain (all 19), with each finding rated
critical / warning / info.
```

---

## Prompt 2: Gold Table Spot-Check Against Known Truths

**Deliverable:** Validation script with expected vs. actual vs. deviation, plus backward ETL trace for any failures.

```
For each of the 19 Medicaid domains in Aradune, validate the single most
important Gold table against publicly verifiable facts. Write a validation
script that checks each fact below, reports expected vs. actual vs. percentage
deviation, and traces any discrepancy backward through the ETL to identify
where it enters.

ENROLLMENT DOMAIN:
- Florida total Medicaid enrollment for FY 2024-25 should be approximately
  4,226,347 (source: SSEC December 2025 executive summary, total caseload).
  Acceptable tolerance: ±2%.
- FY 2023-24 total caseload was approximately 4,836,670 (SSEC), reflecting a
  13.3% year-over-year decline from the pandemic peak of 5,575,548.
- Florida has NOT adopted ACA Medicaid expansion. If any expansion enrollment
  categories appear in the data, flag as critical.

FEE SCHEDULE / RATE-SETTING DOMAIN:
- CPT 99202 base Medicaid rate on the January 2025 Practitioner Fee Schedule
  should be $55.15 (pre-stacking). Verify this is the base FSI rate before
  the 4%, 24%, 16.4%, and 10.2% multipliers are applied.
- The MPIP enhanced rate for pediatric E&M services (SFY 2025-26) is 106.3%
  of the CY2025 Medicare rate. Verify the multiplier is correctly applied and
  not confused with the base Medicaid rate.
- Confirm that no code simultaneously carries both a Facility rate AND a
  PC/TC split — this is structurally invalid in Florida Medicaid. Specifically
  check CPTs 46924, 91124, 91125 which have been flagged previously.

FISCAL / FMAP DOMAIN:
- Florida base FMAP for FY2025-26 (Oct 2025 – Sep 2026) is 57.22%. The
  preliminary FY2026-27 FMAP is approximately 55.43% per EDR.
- Total Medicaid spending for FY 2025-26 is projected at approximately
  $35.6 billion, with the federal share at approximately $20 billion and
  state share approximately $15.6 billion (source: SSEC/Florida Policy
  Institute).

MANAGED CARE DOMAIN:
- SMMC 3.0 launched February 1, 2025. The program now operates across 9
  regions (reduced from 11 under SB 1950). The program has three main
  components: MMA, LTC, and Dental, plus the new ICMC program.

ALL OTHER DOMAINS:
For any domain where a specific anchor fact is not supplied above, identify
the most important Gold table in that domain and perform a basic
reasonableness check: row count is nonzero, date ranges cover the expected
period, no NULL primary keys, no duplicate records on the natural key, and
value distributions don't contain obvious outliers (e.g., negative dollar
amounts, enrollment counts of zero for large counties).
```

---

## Prompt 3: DOGE T-MSIS Dataset Quarantine Check

**Deliverable:** Code path inventory with critical/warning ratings for every potential DOGE data leak.

```
Audit every location in the codebase where the DOGE T-MSIS dataset is
referenced or could be pulled into a query result. The known limitations
that MUST be enforced are:

(a) OT (Other Therapy) claims file only — no IP, RX, or LT claim types,
(b) no beneficiary state variable — meaning you cannot filter or group
    claims by beneficiary state of residence,
(c) managed care states show misleading paid amounts because capitation
    payments are not reflected in claim-level paid fields,
(d) November and December 2024 data is incomplete/truncated.

Check that:

(1) No analytical tool or query silently blends DOGE data with production
    T-MSIS data without an explicit flag or disclaimer in the output.

(2) The RAG engine's retrieval does not surface DOGE records for general
    queries unless the user specifically asks about DOGE data.

(3) Any dashboard, report, or API response that could include DOGE-sourced
    data has a visible caveat string.

(4) The ontology/entity registry correctly tags DOGE tables as quarantined
    or limited-scope.

(5) The Intelligence endpoint, when asked a question that would require IP
    or RX claims, does NOT silently fall back to DOGE OT data as a
    substitute.

List every code path where DOGE data could leak into a user-facing result
unmarked. Rate each as critical (user would see wrong numbers with no
warning) or warning (data is present but unlikely to surface without
specific query).
```

---

## Prompt 4: Ontology & Graph Consistency Audit

**Deliverable:** Graph health report with node/edge counts, orphan inventory, and registry-vs-lake reconciliation.

```
Read the YAML entity registry (reference ONTOLOGY_SPEC.md) and the
auto-generated DuckPGQ property graph. Verify that:

(a) Every entity defined in the registry maps to a real Gold table that
    exists in the DuckDB lake and has >0 rows.

(b) Every relationship edge connects two entities that actually share a
    join key, and that join key has referential integrity (no orphan
    foreign keys exceeding 5% of records).

(c) There are no orphan nodes — entities with zero edges — that would cause
    the RAG engine to retrieve nothing when they're referenced.

(d) The Soda Core v4 + dbt-duckdb + Pandera validation stack is actually
    running and its most recent results show pass/fail status for each
    table.

Produce a graph health report:
- Total node count
- Total edge count
- List of orphan nodes (entities with no edges)
- List of edges with >5% referential integrity failures
- Top 10 most-connected entities
- Registry-vs-lake reconciliation: list any of the 544 fact tables that
  are defined in metadata but missing from the lake, or present in the
  lake but missing from the registry
```

---

## Prompt 5: Tool-by-Tool Functional Audit

**Deliverable:** Tool scorecard with pass/fail for each check and specific findings.

```
For each of the six core v1 analytical tools — (1) wage-adequacy, (2) MCO
gap analysis, (3) quality-rate correlation, (4) rate decay, (5) border
arbitrage, (6) reverse cash flows — do the following:

(a) Map every data dependency: which Gold tables, which specific columns,
    which date ranges, which join paths through the ontology.

(b) Run each tool with default/typical parameters and inspect the output
    for reasonableness:

    - Wage-adequacy: ratios should generally fall between 0.3 and 3.0.
      Flag any result outside this range and trace whether it's a data
      issue or a calculation issue.

    - MCO gap analysis: gaps should be expressed relative to the FFS
      floor. Verify the FFS floor values match the published fee schedule
      (e.g., 99202 = $55.15 base).

    - Quality-rate correlation: check that quality metrics are sourced
      from the correct measurement year and not mixed across years.

    - Rate decay: verify the time series uses consistent deflators and
      that the CPI or other index values are current.

    - Border arbitrage: confirm that cross-state comparisons only use
      states/rates that are actually loaded in the lake — don't
      hallucinate comparison data.

    - Reverse cash flows: verify dollar amounts tie back to actual
      claims/encounter data and aren't synthetic.

(c) Run each tool with edge cases: single provider type only, single
    county, statewide aggregate, oldest available data period, most recent
    data period, a provider type with very few claims.

(d) Check that any narrative or explanatory text the tool generates
    actually describes what the numbers show — no boilerplate text that
    contradicts the data (e.g., saying "rates have declined" when the
    data shows an increase).

Produce a tool-by-tool scorecard: pass/fail for each check, with specific
findings.
```

---

## Prompt 6: Meridian AHEAD Global Budget Audit

**Deliverable:** Model validation report covering parameter accuracy, calculation logic, boundary behavior, and policy alignment with CMS AHEAD specifications.

```
Audit the Meridian hospital global budget calculator end to end. This tool
models CMS's AHEAD (Achieving Healthcare Efficiency through Accountable
Design) total cost of care framework. Validate every layer:

PARAMETER ACCURACY:
(a) Verify that the global budget baseline methodology uses 3 years of
    historical revenue weighted 10/30/60 toward the most recent year, as
    specified by CMS. Check the actual weights applied in code.

(b) Verify that the Transformation Incentive Adjustment (TIA) is correctly
    implemented as a 1% upward adjustment to Medicare FFS global budget
    for PY1/PY2 participating hospitals. Confirm the adjustment is
    additive, not compounded.

(c) Verify that the TCOC (Total Cost of Care) adjustment for acute care
    hospitals is modeled as upside-only through PY4 (based on PY2
    performance), with downside risk only activating thereafter. Check
    that the calculator does not prematurely apply downside penalties.

(d) Verify that Critical Access Hospital (CAH) protections are correctly
    modeled: upside-only adjustments for a longer runway, delayed
    penalties for avoidable utilization, and a payment floor based on
    the most recent cost report. If the calculator does not distinguish
    CAH from non-CAH hospitals, flag as a gap.

(e) Verify the volume corridor logic: "appropriate patient choice" volume
    shifts should be treated differently from "unplanned" upward volume
    changes. Check how the calculator classifies volume changes and
    whether the distinction is configurable or hardcoded.

CALCULATION LOGIC:
(f) Run the calculator for a synthetic hospital with known inputs:
    - Year 1 revenue: $50M, Year 2: $55M, Year 3: $60M
    - Expected baseline: ($50M x 0.10) + ($55M x 0.30) + ($60M x 0.60) =
      $5M + $16.5M + $36M = $57.5M
    - Expected PY1 budget with TIA: $57.5M x 1.01 = $58.075M
    - Verify the calculator produces these exact values (or document the
      deviation and why).

(g) Run edge cases:
    - A hospital with sharply declining revenue (Year 1: $80M, Year 2:
      $60M, Year 3: $40M) — does the 10/30/60 weighting correctly
      produce a low baseline?
    - A hospital with zero revenue in one year — does it handle gracefully
      or error?
    - A CAH vs. non-CAH hospital with identical financials — do the
      outputs differ as expected?

MEDICAID INTEGRATION:
(h) AHEAD requires states to implement Medicaid global budgets by end of
    PY1. Check whether the calculator models the Medicaid component
    separately from Medicare FFS, or conflates them. If it only models
    Medicare, flag what Medicaid-specific adjustments are missing (e.g.,
    different payer mix assumptions, state FMAP implications, managed
    care capitation interaction).

(i) Check whether the calculator accounts for the all-payer alignment
    requirement: at least one commercial payer must participate by PY2.
    If the calculator can model multi-payer scenarios, test that the
    payer mix inputs produce reasonable blended budgets. If it cannot,
    document the limitation.

(j) Verify that the calculator correctly reflects that Florida is NOT an
    AHEAD participating state. The tool should be usable for hypothetical
    modeling ("what if Florida opted in") but should not present Florida
    results as reflecting actual AHEAD participation.

POLICY COHERENCE:
(k) Check that any narrative text, labels, tooltips, or documentation
    generated by the calculator accurately describes the AHEAD model as
    of its current CMS specifications (renamed in 2025, extended through
    December 2035, six participating states). Flag any outdated references
    to the original model name or earlier cohort structure.

(l) Verify that the calculator does not model or imply applicability to
    states without existing rate-setting or all-payer infrastructure
    without appropriate caveats. The early participating states
    (Maryland, Vermont, Connecticut) have structural preconditions that
    most states lack.

(m) Check that the model acknowledges known policy risks: global budgets
    built on historical prices may lock in inefficiencies; CMS retains
    significant discretion in classifying volume shifts; and the model
    may incentivize vertical consolidation as hospitals acquire specialty
    groups to capture more spending under the budget umbrella.

Produce a Meridian validation report: parameter-by-parameter accuracy
check, calculation logic test results, Medicaid integration gap analysis,
and a policy coherence review.
```

---

## Prompt 7: Intelligence Endpoint Regression Suite

**Deliverable:** 30-query test harness with full logging of tables accessed, raw data, final responses, and pass/fail grades.

```
Write and execute a test harness that sends 30 natural-language queries to
Aradune's Intelligence endpoint. Structure them as follows:

--- 10 SIMPLE LOOKUPS (fast, accurate, single-table) ---

1.  What is the Florida Medicaid FFS rate for CPT 99213?
2.  How many people are enrolled in Florida Medicaid?
3.  What is Florida's FMAP for FY2025-26?
4.  What is the reimbursement rate for CPT 99214 with the MPIP pediatric
    enhancement?
5.  How many SMMC regions does Florida have?
6.  What is the Medicaid rate for CPT 90834 (psychotherapy, 45 min)?
7.  What provider types are eligible for the 4% FSI increase?
8.  What is the multiple procedure reduction rule for modifier 51?
9.  What was Florida's total Medicaid caseload in FY 2023-24?
10. Is Florida a Medicaid expansion state?

--- 10 ANALYTICAL QUESTIONS (joins, calculations, multi-table) ---

1.  How does Florida's Medicaid rate for primary care E&M codes compare to
    Medicare rates?
2.  Which counties have the highest Medicaid enrollment per capita?
3.  What is the rate decay trend for E&M codes over the last 5 years?
4.  Which MCO plans have the largest gap between their payments and the
    FFS floor?
5.  What is the wage-adequacy ratio for behavioral health providers?
6.  How has the FMAP trended over the last 5 fiscal years and what's the
    projected direction?
7.  What percentage of total Medicaid spending goes to managed care vs. FFS?
8.  Which provider specialties have the most codes reimbursed below Medicare?
9.  What is the fiscal impact of the 106.3% MPIP enhancement over baseline
    rates?
10. Show me the top 10 highest-volume CPT codes by Medicaid claims and
    their current rates.

--- 10 ADVERSARIAL / EDGE-CASE QUESTIONS (should decline, caveat, or
    handle gracefully) ---

1.  What is Florida's capitation rate for dental in 2030?
    → Future date — should decline or heavily caveat.

2.  Compare Florida Medicaid spending to California's.
    → Out-of-scope if CA data is not loaded.

3.  What is the fee schedule rate for CPT 99999?
    → Nonexistent code — should say so.

4.  Pull all beneficiary-level claims from the DOGE dataset by state.
    → DOGE has no beneficiary state variable — should flag limitation.

5.  What were total Medicaid paid amounts in December 2024 from T-MSIS?
    → DOGE Dec 2024 data is incomplete — should caveat.

6.  What is the Medicaid rate in Texas for 99213?
    → Out-of-scope if TX data is not loaded.

7.  Generate a report on Florida Medicaid fraud cases.
    → Likely not in scope of loaded data — should say so.

8.  What is the IP inpatient claims total from the DOGE dataset?
    → DOGE only has OT claims — should flag this explicitly.

9.  Show me the trend in Medicaid expansion enrollment in Florida.
    → FL is non-expansion — should state this clearly.

10. What will Florida's FMAP be in FY 2030-31?
    → Speculative — should caveat with EDR preliminary estimates at most.

For each of the 30 queries, log:
- The query text
- Which Gold tables were accessed
- The raw data values returned
- The final natural-language response
- Whether citations/sources were provided
- A pass/fail grade with notes

Flag any response where the numbers are not traceable to actual data in
the lake, or where the system hallucinated a statistic or citation.
```

---

## Prompt 8: End-to-End Workflow Smoke Test

**Deliverable:** Step-by-step trace log showing every table, query, prompt, and output — with pass/fail at each stage.

```
Simulate a realistic Medicaid analyst workflow from start to finish, logging
everything that happens under the hood at each step:

STEP 1 — SIMPLE LOOKUP:
"What is the current Medicaid reimbursement rate for CPT 99214 in Florida
and how does it compare to Medicare?"
Expected: System retrieves the base Medicaid rate, applies any applicable
multipliers, pulls the Medicare comparison rate, and produces a clear
narrative. Verify the Medicaid base rate ties to the January 2025 fee
schedule.

STEP 2 — ANALYTICAL TOOL:
"Show me the rate decay for E&M codes over the last 5 years adjusted for
inflation."
Expected: System runs the rate decay tool, applies CPI adjustment, produces
a table or visualization. Verify the CPI values used are current.

STEP 3 — CROSS-DOMAIN SYNTHESIS:
"How does provider wage adequacy for primary care in Florida compare across
the 9 SMMC regions?"
Expected: System pulls wage data, rate data, and regional enrollment data,
joins them correctly, produces a regional comparison.

STEP 4 — AHEAD SCENARIO:
"Model a hypothetical AHEAD global budget for a mid-size Florida hospital
with $60M in annual net patient revenue. What would the PY1 budget look
like with the TIA adjustment?"
Expected: System runs the Meridian calculator, applies 10/30/60 baseline
weighting (using the $60M as Year 3 with reasonable Year 1/Year 2 inputs),
applies the 1% TIA, and produces a clear output. System should note that
Florida is not an AHEAD participating state and this is a hypothetical.

STEP 5 — REPORT GENERATION:
"Generate a one-pager summarizing Florida's rate adequacy for primary care,
suitable for a legislative audience."
Expected: System produces a formatted document with sourced data points,
key findings, and policy context. Verify that every number in the one-pager
traces to an actual Gold table value.

STEP 6 — ADVERSARIAL FOLLOW-UP:
"Now include California as a comparison state."
Expected: System gracefully explains that CA data is not loaded rather than
hallucinating or erroring silently.

At each step, log:
- The tables queried
- The SQL or query logic executed
- The prompts sent to Claude
- The raw data returned
- The final output

Flag any step where:
- The chain breaks
- Data is stale
- A number can't be traced to source
- The narrative contradicts the data
- The output wouldn't survive review by a rate-setting actuary or
  legislative budget analyst
```

---

## Post-Audit Deliverables

After running all eight prompts, the following artifacts should exist:

1. **Data Integrity Report** — every ETL script audited, domain by domain
2. **Anchor Fact Validation Report** — expected vs. actual for every verifiable number
3. **DOGE Quarantine Inventory** — every code path where tainted data could leak
4. **Graph Health Report** — node/edge counts, orphans, referential integrity failures
5. **Tool Scorecard** — pass/fail for each analytical tool across default and edge-case runs
6. **Meridian Validation Report** — parameter accuracy, calculation logic, Medicaid integration gaps, policy coherence
7. **Intelligence Endpoint Regression Log** — 30 graded queries with full trace data
8. **Workflow Trace Log** — six-step analyst simulation with under-the-hood logging

These eight artifacts collectively serve as Aradune's **data quality assurance package**. They are suitable for:

- Due diligence in consulting firm partnership discussions
- Demonstrating data governance to prospective enterprise clients
- Internal confidence that the platform produces defensible, traceable outputs
- Ongoing regression testing as new data runs are ingested (re-run Prompts 2, 5, 6, and 7 after each data ingestion batch)
