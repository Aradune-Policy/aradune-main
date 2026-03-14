# Aradune Tool-by-Tool Functional Audit Report (Prompt 5)

**Audit Date:** 2026-03-13
**Scope:** 6 core analytical tools from audit guide

---

## Executive Summary

Of the 6 tools specified in the audit guide, **3 exist as built modules** and **3 do not exist** (they represent future capabilities described in early design docs but never implemented).

| Tool | Exists? | Grade | Key Finding |
|------|---------|-------|-------------|
| 1. Wage Adequacy | YES | **WARNING** | T-MSIS per-claim rates treated as per-unit (3-15x inflation for 15-min codes). Caveats added. |
| 2. Quality-Rate Correlation | YES | **PASS** (fixed) | API was returning empty HCPCS linkage. Fixed. Correlation math is correct. |
| 3. Rate Decay | YES | **PASS** (fixed) | Medicare year label was wrong (2025→2026). Static JSON fallback broken. API path works correctly. |
| 4. MCO Gap Analysis | NO | N/A | MCO contracted rates not available in public data (Ring 2+). FFS floor rates available. |
| 5. Border Arbitrage | NO | N/A | No state adjacency map. Cross-state comparison exists but is user-selected, not proximity-based. |
| 6. Reverse Cash Flows | NO | N/A | CMS-64/T-MSIS reconciliation acknowledged as impossible. SpendingEfficiency covers aggregate analysis. |

---

## Detailed Scorecards

### Tool 1: Wage Adequacy

| Dimension | Grade | Finding |
|-----------|-------|---------|
| Data Dependencies | PASS | BLS OEWS + T-MSIS + SOC-HCPCS crosswalk + fee schedules |
| Default Run | **WARNING** | 15-min codes (T1019, etc.) produce ratios 3-15x inflated; per-session codes (90834) pass |
| Edge Cases | PASS | Missing data handled gracefully |
| Narrative Accuracy | **FIXED** | Was "145 occupations/52 states", now correct "16/51". T-MSIS bundling caveat added at 3 locations. |

**Root cause (not fixed — needs your decision):** T-MSIS `avg_paid_per_claim` bundles multiple 15-min units into one claim. Multiplying by `units_per_hour` (4) double-counts. Fix options:
- A) Divide T-MSIS rate by estimated units-per-claim before applying units_per_hour
- B) Use fee schedule rates instead of T-MSIS for the implied wage calculation
- C) Accept as directional with prominent caveat (current state)

### Tool 2: Quality-Rate Correlation

| Dimension | Grade | Finding |
|-----------|-------|---------|
| Data Dependencies | PASS | Core Set 2024 + HCPCS linkage + T-MSIS rates |
| Default Run | PASS | Pearson r = -0.24 for WCV-CH, mathematically correct |
| Edge Cases | **FIXED** | API was returning empty `measure_hcpcs`. Now returns full 21-measure linkage. |
| Narrative Accuracy | PASS | Year consistency verified; causation caveat present |

### Tool 3: Rate Decay

| Dimension | Grade | Finding |
|-----------|-------|---------|
| Data Dependencies | **WARNING** | Static JSON fallback structurally incompatible; API path works |
| Default Run | PASS (API) | FL shows 27-41% of Medicare for E&M — plausible |
| Edge Cases | PASS | Duplicate codes handled; missing data gracefully skipped |
| Narrative Accuracy | **WARNING** | "Rate Decay" name implies temporal tracking but tool is point-in-time Medicaid/Medicare comparison only. No CPI adjustment. Year label fixed (2025→2026). |

### Tools 4-6: Not Implemented

| Tool | Data Available | What Exists Instead |
|------|---------------|---------------------|
| MCO Gap Analysis | FFS floor rates (597K), MCO MLR (2,282), MC penetration | ManagedCareValue research module |
| Border Arbitrage | Cross-state rates (302K, 45 states) | TmsisExplorer manual 3-state comparison |
| Reverse Cash Flows | CMS-64 (118K), T-MSIS claims (713K), SDUD (2.6M) | SpendingEfficiency module |

---

## Fixes Deployed

1. `/api/bulk/quality-measures` now returns full `measure_hcpcs` with 21 Core Set measures linked to HCPCS codes
2. Medicare rates year label corrected: 2025 → 2026
3. WageAdequacy narrative corrected + T-MSIS bundling caveat added at 3 locations
