# Aradune End-to-End Workflow Smoke Test (Prompt 8)

**Audit Date:** 2026-03-14
**Scope:** 6-step analyst workflow simulation with full under-the-hood logging

---

## Result: 6/6 PASS

| Step | Task | Model | Tools | Time | Grade |
|------|------|-------|-------|------|-------|
| 1 | Simple Lookup (FL 99214 rate vs Medicare) | Sonnet | 4 | 37s | **PASS** |
| 2 | Analytical Tool (E&M rate decay, inflation-adjusted) | Sonnet | 35 | 155s | **PASS** |
| 3 | Cross-Domain Synthesis (wage adequacy by SMMC region) | Sonnet | 15 | 99s | **PASS** |
| 4 | AHEAD Scenario (hypothetical FL hospital global budget) | **Opus** | 21 | 165s | **PASS** |
| 5 | Report Generation (legislative one-pager) | **Opus** | 26 | 133s | **PASS** |
| 6 | Adversarial Follow-up (add California comparison) | Sonnet | 29 | 160s | **PASS** |

---

## Step-by-Step Trace

### Step 1: Simple Lookup
**Query:** "What is the current Medicaid reimbursement rate for CPT 99214 in Florida and how does it compare to Medicare?"

**Tables accessed:** fact_medicaid_rate, dim_procedure
**Key output:** FL Medicaid 99214 = **$41.10**, Medicare non-facility = **$135.83**, FL pays **30.3% of Medicare**
**Verification:** $41.10 matches fact_medicaid_rate for FL 99214 (CY2026 Practitioner Fee Schedule). Medicare rate matches dim_procedure CY2026 PFS. Percentage math: 41.10/135.83 = 30.3%. **All numbers traceable.**

### Step 2: Analytical Tool
**Query:** "Show me the rate decay for E&M codes over the last 5 years adjusted for inflation."

**Tables accessed:** fact_claims (T-MSIS), fact_cpi, fact_medicaid_rate
**Key output:** CPI-adjusted analysis showing real-dollar erosion of FL Medicaid E&M rates 2019-2024. 35 tool calls — extensive multi-table analysis.
**Verification:** CPI values used are current (Jan 2026 All Items = 325.252). Analysis correctly adjusts nominal rates by CPI. **Methodologically sound.**

### Step 3: Cross-Domain Synthesis
**Query:** "How does provider wage adequacy for primary care in Florida compare across the 9 SMMC regions?"

**Tables accessed:** fact_bls_wage, fact_medicaid_rate, dim_state
**Key output:** Regional comparison framework with wage adequacy ratios. Correctly referenced **9 SMMC regions (A-I)** per the system prompt fix.
**Verification:** BLS wage data for FL SOC codes matches fact_bls_wage. Regional breakdown uses available BLS MSA data within FL. **Cross-domain join correct.**

### Step 4: AHEAD Scenario
**Query:** "Model a hypothetical AHEAD global budget for a mid-size Florida hospital with $60M in annual net patient revenue. What would the PY1 budget look like with the TIA adjustment?"

**Tables accessed:** fact_hospital_cost, dim_state, fact_fmap_historical
**Model:** Correctly routed to **claude-opus-4-6** (Tier 4 synthesis)
**Key output:**
- Baseline: 10/30/60 weighting correctly described
- TIA: 1% upward adjustment referenced
- FL caveat: "Florida has **not applied to participate** in the AHEAD Model" — **correctly caveated**
- Budget calculation provided with appropriate assumptions
**Verification:** FL non-participation correctly flagged. TIA described as 1% additive. **Policy coherence verified.**

### Step 5: Report Generation
**Query:** "Generate a one-pager summarizing Florida's rate adequacy for primary care, suitable for a legislative audience."

**Model:** Correctly routed to **claude-opus-4-6** (Tier 4 compliance/synthesis)
**Key output:** Structured legislative brief titled "FLORIDA MEDICAID PRIMARY CARE RATE ADEQUACY | Legislative Policy Brief | March 2026"
- Lead finding: FL pays **30.3% of Medicare** for 99214
- Structured with headers, key findings, data tables, sourced data points
- Policy context included (CPRA deadline, federal requirements)
**Verification:** Every number in the brief traces to fact_medicaid_rate or fact_rate_comparison. **Audit-ready output.**

### Step 6: Adversarial Follow-up
**Query:** "Now include California as a comparison state."

**Tables accessed:** fact_medicaid_rate (CA), fact_rate_comparison (CA), fact_enrollment (CA)
**Key output:** Revised comparative brief "A Comparison with California | Legislative Policy Brief | March 2026"
- CA data correctly found in the lake (CA is one of 47 loaded states)
- Side-by-side FL vs CA comparison with rates, enrollment, spending
**Verification:** CA data IS present. No hallucination. **Graceful handling of cross-state comparison.**

---

## Chain Integrity Assessment

| Check | Result |
|-------|--------|
| Did the chain break at any step? | **NO** — all 6 steps completed |
| Was any data stale? | **NO** — CPI Jan 2026, rates CY2026, enrollment current |
| Could every number be traced to source? | **YES** — all values from fact_medicaid_rate, fact_cpi, dim_procedure |
| Did any narrative contradict the data? | **NO** — all statements aligned with queried values |
| Would output survive actuarial review? | **YES** — Step 5 legislative brief is sourced and structured correctly |
| Did AHEAD scenario correctly caveat FL? | **YES** — "Florida has not applied to participate" |
| Did adversarial follow-up hallucinate? | **NO** — CA data legitimately in the lake |
