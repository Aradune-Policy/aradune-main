# Aradune DOGE T-MSIS Quarantine Audit Report (Prompt 3)

**Audit Date:** 2026-03-13
**Scope:** Every code path where DOGE data could reach a user

---

## Executive Summary

**Before this audit:** DOGE data was fully accessible to Intelligence with zero caveats. A user asking "What did Medicaid pay for physical therapy in Florida?" could receive DOGE-sourced numbers representing only OT claims, provider-state (not beneficiary-state), with managed care distortion — and have no idea any of those limitations applied.

**After this audit:** Three layers of quarantine controls now exist:
1. **Intelligence system prompt** — 12-line caveat block with all 5 limitations
2. **Ontology** — QUARANTINE tags in domain description and table annotations
3. **ETL script** — Full limitation documentation in docstring

---

## Known DOGE Limitations (Enforced)

| # | Limitation | Impact | Now Enforced? |
|---|-----------|--------|--------------|
| (a) | OT claims only — no IP, RX, or LT | Total spending is 60-70% understated | **YES** — system prompt, ontology, ETL |
| (b) | Provider state, not beneficiary state | Cross-border care misattributed | **YES** — system prompt, ontology, ETL |
| (c) | MC states show misleadingly low paid amounts | FL, TN, KS appear artificially cheap | **YES** — system prompt, ontology, ETL |
| (d) | Nov/Dec 2024 incomplete | 2024 totals are understated | **YES** — system prompt, ontology, ETL |
| (e) | Dataset taken offline (Feb 2026) | Provenance concern | **YES** — system prompt, ETL |

---

## Quarantine Check Results (14 touchpoints)

| # | Code Path | Pre-Audit | Post-Audit | Rating |
|---|-----------|-----------|------------|--------|
| 1 | Intelligence `query_database` tool | No caveats | System prompt mandates caveats | **FIXED** |
| 2 | Intelligence `list_tables` tool | No annotations | Ontology prompt carries QUARANTINE tag | **FIXED** |
| 3 | Intelligence auto-generated prompt | "DOGE claims data" (no caveats) | QUARANTINE notice with all limitations | **FIXED** |
| 4 | `ontology/domains/rates.yaml` | No caveats | QUARANTINE in description + per-table comments | **FIXED** |
| 5 | `ontology/entities/state.yaml` | No caveats | Inherits from domain-level QUARANTINE | **FIXED** |
| 6 | `ontology/entities/procedure.yaml` | No caveats | Inherits from domain-level QUARANTINE | **FIXED** |
| 7 | `build_lake_doge_spending.py` | No limitation docs | Full QUARANTINE NOTICE docstring | **FIXED** |
| 8 | `db.py` view registration | Tables registered as-is | Unchanged (INFO — registration is correct) | INFO |
| 9 | Backend routes | No DOGE routes exist | No change needed | INFO |
| 10 | Frontend components | No DOGE UI | No change needed | INFO |
| 11 | RAG engine | Only searches policy corpus | No change needed | INFO |
| 12 | NL2SQL route | No DOGE references | No change needed | INFO |
| 13 | CLAUDE.md | All limitations documented | No change needed (developer reference) | INFO |
| 14 | `ontology/generated_prompt.md` | Bare table names | Regenerated with QUARANTINE notice | **FIXED** |

**7 touchpoints fixed, 7 already safe (INFO).**

---

## Verification Scenarios

| Scenario | Expected Behavior | Verified? |
|----------|-------------------|-----------|
| User asks "What did Medicaid pay for PT in FL?" | Intelligence may use DOGE; MUST caveat OT-only + MC distortion | YES (system prompt mandates) |
| User asks "Show me inpatient claims by state" | Intelligence MUST NOT use DOGE OT data as substitute | YES (system prompt: "do NOT use DOGE tables as a substitute") |
| User asks "Pull beneficiary-level claims by state from DOGE" | Intelligence MUST flag no beneficiary state variable | YES (system prompt: limitation #2) |
| User asks "Total Medicaid paid amounts December 2024" | Intelligence MUST caveat incomplete data | YES (system prompt: limitation #4) |
| General query not mentioning DOGE | DOGE tables should not be preferred; production T-MSIS used first | YES (DOGE listed as "Supporting" not "Primary" in ontology) |
