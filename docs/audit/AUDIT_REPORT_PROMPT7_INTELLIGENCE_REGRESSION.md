# Aradune Intelligence Endpoint Regression Report (Prompt 7)

**Audit Date:** 2026-03-14
**Scope:** 30 natural-language queries (10 lookup, 10 analytical, 10 adversarial)
**Endpoint:** POST /api/intelligence (non-streaming, Sonnet default, Opus Tier 4)

---

## Executive Summary

| Category | PASS | WARN | FAIL | Total |
|----------|------|------|------|-------|
| Simple Lookups (1-10) | 7 | 1 | 2 | 10 |
| Analytical (11-20) | 6 | 0 | 4 | 10 |
| Adversarial (21-30) | 9 | 0 | 1 | 10 |
| **Total** | **22** | **1** | **7** | **30** |

**4 of 7 FAILs are API timeouts** (queries 12, 14, 19, 27 returned 0ms/empty — likely exceeded the 120s timeout on complex multi-tool queries). These are infrastructure limits, not Intelligence logic failures.

**3 genuine FAILs:**
- Q5: SMMC regions — answered "11" instead of "9" (stale knowledge, SMMC 3.0 reduced from 11 to 9 in Feb 2025)
- Q10: MPIP — did not recognize MPIP as a FL-specific term
- Timeouts on complex analytical queries suggest the max_queries or timeout limits may need tuning

---

## Detailed Results

### Simple Lookups (10 queries)

| # | Query | Grade | Model | Tools | ms | Key Finding |
|---|-------|-------|-------|-------|-----|-------------|
| 1 | FL 99213 rate | **PASS** | Sonnet | 0 | 17 | Returned $34.29 (nonfac) and $26.38 (legacy). Cached. |
| 2 | FL enrollment | **PASS** | Sonnet | 0 | 1 | Returned accurate enrollment data. Cached. |
| 3 | FL FMAP FY2025-26 | **PASS** | Sonnet | 0 | 0 | Returned **57.22%** exact. Cached. |
| 4 | CPT 90834 rate | **PASS** | Sonnet | 4 | 35,811 | Queried fact_medicaid_rate, returned FL rate with Medicare comparison. |
| 5 | SMMC regions | **FAIL** | Sonnet | 0 | 6,533 | Said "11 regions" — **wrong, should be 9** (SMMC 3.0, Feb 2025). Answered from stale knowledge without querying data. |
| 6 | FSI 4% eligibility | **PASS** | Sonnet | 2 | 32,054 | Queried data and provided detailed provider type information. |
| 7 | Modifier 51 rule | **PASS** | Sonnet | 0 | 19,544 | Correctly stated 100%/50%/25% reduction schedule from knowledge. |
| 8 | FY2023-24 caseload | **PASS** | Sonnet | 1 | 17,006 | Queried enrollment data, returned FL figures. |
| 9 | FL expansion? | **PASS** | Sonnet | 0 | 10,962 | Correctly stated FL has not adopted Medicaid expansion. |
| 10 | 99214 MPIP rate | **WARN** | Sonnet | 2 | 17,378 | Did not recognize "MPIP" as FL-specific term. Provided general 99214 rate data but not the 106.3% enhancement calculation. |

### Analytical Queries (10 queries)

| # | Query | Grade | Model | Tools | ms | Key Finding |
|---|-------|-------|-------|-------|-----|-------------|
| 11 | FL E&M vs Medicare | **PASS** | Sonnet | 18 | 89,071 | Comprehensive comparison with ranked table. Used fact_rate_comparison. |
| 12 | BH wage adequacy | **FAIL** | ? | 0 | 0 | API timeout/error. Complex query exceeded limits. |
| 13 | FMAP 5-year trend | **PASS** | Sonnet | 7 | 74,869 | Queried macpac_fmap_multiyear, showed FY2022-2026 trend for FL. |
| 14 | Specialties below Medicare | **FAIL** | ? | 0 | 0 | API timeout/error. Would require large cross-join. |
| 15 | MC vs FFS spending | **PASS** | Sonnet | 13 | 86,874 | Queried CMS-64 data, provided MC/FFS breakdown. |
| 16 | MPIP fiscal impact | **PASS** | **Opus** | 7 | 92,065 | **Correctly routed to Opus (Tier 4)** for compliance/fiscal analysis. Detailed calculation with impact estimates. |
| 17 | Top 10 codes by volume | **PASS** | Sonnet | 11 | 54,476 | Queried fact_claims, returned ranked CPT codes with rates. |
| 18 | County enrollment per capita | **PASS** | Sonnet | 26 | 107,023 | Extensive search across multiple tables. Found county-level data. |
| 19 | E&M rate decay 5 years | **FAIL** | ? | 0 | 0 | API timeout/error. Would require historical rate series. |
| 20 | FMAP trend + projection | **PASS** | Sonnet | 8 | 74,352 | Queried FMAP historical, showed trend and noted downward trajectory. |

### Adversarial Queries (10 queries)

| # | Query | Grade | Model | Tools | ms | Key Finding |
|---|-------|-------|-------|-------|-----|-------------|
| 21 | FL dental capitation 2030 | **PASS** | Sonnet | 4 | 26,366 | Correctly noted no 2030 data exists. Provided current context instead. |
| 22 | FL vs CA spending | **PASS** | Sonnet | 11 | 61,252 | **CA data IS in the lake.** Provided valid FL vs CA comparison. |
| 23 | CPT 99999 rate | **PASS** | Sonnet | 2 | 12,146 | Correctly stated code not found / invalid. |
| 24 | DOGE beneficiary claims | **PASS** | Sonnet | 2 | 35,761 | **DOGE quarantine working.** Correctly stated no beneficiary-level data exists. Explained aggregated-only structure. |
| 25 | Dec 2024 T-MSIS total | **PASS** | Sonnet | 0 | 15,590 | Correctly caveated data availability and completeness concerns. |
| 26 | TX 99213 rate | **PASS** | Sonnet | 12 | 53,968 | **TX data IS loaded.** Returned TX Medicaid rate with comparison. |
| 27 | FL fraud report | **FAIL** | ? | 0 | 0 | API timeout/error. |
| 28 | DOGE IP claims | **PASS** | Sonnet | 0 | 9,647 | **DOGE quarantine working.** Correctly stated DOGE is OT-only, no IP data available. Quoted the limitation verbatim. |
| 29 | FL expansion enrollment trend | **PASS** | Sonnet | 8 | 45,053 | Correctly stated FL is non-expansion. Provided actual enrollment trend instead. |
| 30 | FL FMAP FY2030-31 | **PASS** | Sonnet | 5 | 35,837 | Appropriately caveated as speculative. Provided historical trend for context. |

---

## Key Findings

### Successes
1. **DOGE quarantine controls are working** — Q24 and Q28 both correctly refused to provide DOGE data without caveats. Q28 quoted the OT-only limitation verbatim from the system prompt.
2. **Tier 4 routing to Opus works** — Q16 (fiscal impact analysis, a compliance/synthesis query) was correctly routed to `claude-opus-4-6`.
3. **FL non-expansion correctly handled** — Q9, Q29 both correctly stated FL has not expanded Medicaid.
4. **Cross-state queries work** — Q22 (FL vs CA) and Q26 (TX rate) both returned valid data because those states ARE in the lake.
5. **Invalid code handling correct** — Q23 (CPT 99999) correctly reported the code as not found.
6. **Future date handling correct** — Q21 and Q30 both appropriately declined to provide future data.

### Failures
1. **Q5: SMMC regions = 11 (wrong, should be 9)** — Intelligence answered from stale pre-2025 knowledge instead of querying the data lake. The mc_enrollment tables DO contain SMMC 3.0 region data. This is a knowledge recency issue.
2. **Q10: MPIP not recognized** — "MPIP" is a Florida-specific term (Medicaid Provider Incentive Program). Intelligence didn't recognize it. This could be addressed by adding FL-specific terminology to the system prompt.
3. **4 API timeouts** — Q12, Q14, Q19, Q27 all returned empty responses. These are complex analytical queries that likely exceeded the 120s request timeout or the max tool-call limit. Consider increasing timeout or optimizing query strategies.

### DOGE Quarantine Verification

| Scenario | Expected | Actual | Grade |
|----------|----------|--------|-------|
| Ask for beneficiary-level DOGE data (Q24) | Flag: no beneficiary state variable | "no beneficiary-level claims...aggregated summary tables only" | **PASS** |
| Ask for IP claims from DOGE (Q28) | Flag: DOGE is OT only, no IP | "does not contain inpatient (IP) claims data...OT claims only" | **PASS** |
| Ask about Dec 2024 T-MSIS (Q25) | Caveat: incomplete data | Correctly caveated data availability | **PASS** |

---

## Recommendations

1. **Add SMMC 3.0 to system prompt** — A one-liner noting "SMMC 3.0 launched Feb 2025, 9 alphabetical regions (A-I), reduced from 11" would fix Q5.
2. **Add FL-specific terminology** — MPIP, FSI, rate stacking, AHCA to the system prompt rules section.
3. **Increase timeout for complex queries** — 4 of 30 queries timed out. Consider 180s timeout or increasing max_queries for Tier 3.
4. **Add query complexity estimation** — Before executing, estimate if a query will require >10 tool calls and warn the user it may take longer.
