# Aradune Meridian AHEAD Global Budget Audit Report (Prompt 6)

**Audit Date:** 2026-03-13
**Scope:** AheadCalculator.tsx (~3,200 lines) + aheadScoring.ts (348 lines)

---

## Executive Summary

| Check Category | PASS | WARNING | FAIL | Fixed |
|---------------|------|---------|------|-------|
| Parameter Accuracy (a-e) | 1 | 2 | 2 | 2 |
| Calculation Logic (f-g) | 1 | 1 | 1 | 0 (structural) |
| Medicaid Integration (h-j) | 1 | 1 | 1 | 0 (feature gap) |
| Policy Coherence (k-m) | 0 | 1 | 2 | 2 |
| **Total** | **3** | **5** | **6** | **4** |

---

## Fixes Deployed (4)

1. **TIA PY1/PY2 limitation** — Added `tia = py <= 2 ? 1.01 : 1.00` conditional. Was applied to all years.
2. **TCOC PY4 upside-only** — PY4 now `Math.max(0,...)` (upside only). Downside activates PY5+.
3. **State roster corrected** — 6 confirmed AHEAD states labeled. CO/NJ/NM labeled as hypothetical.
4. **Policy risk acknowledgments** — Added 6 known limitations + rate-setting infrastructure caveats.

---

## Structural Issues Deferred (3)

These require architectural changes beyond a fix:

| Issue | Impact | Effort |
|-------|--------|--------|
| **Three-year historical input** — Baseline formula uses single year + synthetic growth instead of 3 separate year values | Cannot accurately model hospitals with irregular revenue trajectories | Rewrite baseline formula + data model + import form |
| **Volume corridor logic** — No "appropriate patient choice" vs "unplanned" classification | Cannot model CMS's volume discretion, which is a core AHEAD mechanism | New classification engine + UI |
| **Commercial payer integration** — CMS requires commercial payer by PY2 | Cannot model all-payer alignment, a core AHEAD requirement | New payer mix engine + multi-payer budget aggregation |

---

## Detailed Results

### Parameter Accuracy

| # | Check | Result | Detail |
|---|-------|--------|--------|
| (a) | 10/30/60 weighting | **WARNING** | Weights correct but applied to single year with 1.02/1.04 synthetic growth, not 3 separate inputs |
| (b) | TIA 1% PY1/PY2 | **FIXED** | Was compounded with volume and applied to all PYs. Now isolated and PY1/PY2 limited. |
| (c) | TCOC downside timing | **FIXED** | PY4 now upside-only. Downside starts PY5+. |
| (d) | CAH protections | **PASS** | Quality upside-only, cost-based floor with 3.5% trend, Monte Carlo CAH tracking |
| (e) | Volume corridor | **FAIL** | Entirely absent. No appropriate vs unplanned classification. |

### Calculation Logic

| # | Check | Result | Detail |
|---|-------|--------|--------|
| (f) | Synthetic hospital ($50M/$55M/$60M) | **FAIL** | Cannot input 3 separate year values. Single-year model cannot replicate test case. |
| (g-1) | Declining revenue edge case | **FAIL** | Single-year + growth model always produces ascending trajectory |
| (g-2) | Zero revenue | **WARNING** | Produces NaN propagation but doesn't crash |
| (g-3) | CAH vs non-CAH | **PASS** | Outputs correctly differ (quality floor, cost-based floor) |

### Medicaid Integration

| # | Check | Result | Detail |
|---|-------|--------|--------|
| (h) | Medicaid separate from Medicare | **PASS** | Two distinct engines with appropriate structural differences |
| (i) | Multi-payer commercial | **FAIL** | Not modeled at all |
| (j) | FL not AHEAD | **WARNING** | Correctly excluded from dropdown. State roster now labels CO/NJ/NM as hypothetical. |

### Policy Coherence

| # | Check | Result | Detail |
|---|-------|--------|--------|
| (k) | Labels/documentation | **FIXED** | State roster corrected. Program duration (Dec 2035) added. |
| (l) | Rate-setting infrastructure | **FIXED** | CO/NJ/NM now have explicit caveat about lacking regulatory apparatus |
| (m) | Policy risks | **FIXED** | 6 limitations now listed: historical price lock-in, CMS volume discretion, vertical consolidation, plus 3 structural gaps |

### Readiness Scoring Engine (aheadScoring.ts)

| Check | Result |
|-------|--------|
| Financial Stability scoring | **PASS** — operating margin, current ratio, CCR, net income properly weighted |
| Revenue Concentration scoring | **PASS** — gov payer %, uncompensated care, IP/OP balance |
| Supplemental Exposure scoring | **PASS** — DSH+IME as % of revenue |
| Volume Stability scoring | **PASS** — discharges/bed, occupancy, cost/discharge |
| Peer benchmarking | **PASS** — state + national medians from HCRIS |
| Self-report bonus | **PASS** — 8 questions, +15 max, properly capped |
