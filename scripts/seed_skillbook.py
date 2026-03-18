#!/usr/bin/env python3
"""
seed_skillbook.py -- Seed the Aradune Skillbook with initial domain knowledge.

Sources: CLAUDE.md response rules, DOGE quarantine, research audit findings,
FL rate-setting rules, build principles.

Usage:
    python3 scripts/seed_skillbook.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from server.engines.skillbook import ensure_table, add_skill

SEEDS = [
    # -- Domain Rules (from Response Rules) --
    ("rates", "domain_rule",
     "Always specify data vintage (e.g., 'Based on CY2025 fee schedule data'). Never say 'current' or 'latest' without a date.",
     "CLAUDE.md Response Rules"),

    ("rates", "domain_rule",
     "CPRA uses $32.3465 conversion factor (CY2025 PFS). General Medicare CF is $33.4009 (CY2026). Do not confuse them.",
     "42 CFR 447.203"),

    ("rates", "domain_rule",
     "FL Medicaid: Facility and PC/TC rates typically mutually exclusive (99.96% of codes). Codes 46924, 91124, 91125 legitimately carry both per AHCA.",
     "FL SPA rate methodology"),

    ("enrollment", "domain_rule",
     "CHIP enrollment must be excluded from per-enrollee Medicaid spending calculations. Use total_enrollment minus chip_enrollment.",
     "CMS enrollment methodology"),

    ("enrollment", "domain_rule",
     "Medicaid expansion dates are in dim_state (expansion_date, expansion_status). 41 states expanded, 10 did not. WI is 'Not Expanded' but covers adults to 100% FPL.",
     "KFF Medicaid expansion tracker"),

    ("expenditure", "domain_rule",
     "CMS-64 fact_cms64_multiyear has total_computable and federal_share. State share = total_computable - federal_share. Always verify which is needed.",
     "CMS MBES/CBES"),

    ("rates", "domain_rule",
     "fact_rate_comparison has published fee schedule rates only (302K rows, 45 states). fact_rate_comparison_v2 has 483K rows across 54 jurisdictions including T-MSIS and computed rates. Always note the source when using v2.",
     "Session 30 audit"),

    # -- Caveats (data quality warnings) --
    ("rates", "caveat",
     "T-MSIS encounter amounts represent actual paid claims (avg 72% of fee schedule max), NOT fee schedule rates. Always label T-MSIS-derived rates as 'claims-based effective rate, not fee schedule.'",
     "Session 30 T-MSIS calibration analysis"),

    ("rates", "caveat",
     "DOGE data (fact_doge_*) is QUARANTINED: OT claims only (no IP/RX/LT), uses provider state not beneficiary state, managed care states show misleadingly low paid amounts, Nov/Dec 2024 incomplete.",
     "CLAUDE.md DOGE quarantine"),

    ("pharmacy", "caveat",
     "SDUD total_amount_reimbursed is PRE-REBATE. Manufacturer and supplemental rebates reduce effective cost by 50-70% for brand drugs. Always note 'pre-rebate' when citing SDUD spending.",
     "CMS drug rebate program"),

    ("pharmacy", "caveat",
     "SDUD product_name is truncated to ~10 characters. Use ILIKE '%buprenorph%' not '%buprenorphine%' for MAT drug matching. Full name 'buprenorphine' (14 chars) never matches truncated 'BUPRENORPH'.",
     "Session 30 MAT drug fix"),

    ("quality", "caveat",
     "Census sentinel values (-888888888) appear in poverty/income columns. These are NULL indicators, not real values. Always filter or COALESCE.",
     "Census SAIPE documentation"),

    ("public_health", "caveat",
     "CDC natality public use file (2023) intentionally blanks state FIPS codes. State-level Medicaid birth share comes from KFF, not direct CDC parsing. Clinical rates (C-section, preterm, LBW) are national by payer, not state-specific.",
     "CDC NCHS privacy policy"),

    # -- Failure Modes (reasoning paths that produced wrong answers) --
    ("rates", "failure_mode",
     "Rate-Quality OLS: do NOT include SVI and poverty rate simultaneously with income in the regression. VIF exceeds 10M, causing multicollinearity that inflates p-values. Drop SVI and poverty; use parsimonious model (rate + MC + income, VIF < 1.3).",
     "Session 30 forensic audit"),

    ("rates", "failure_mode",
     "Rate-Quality p=0.044 depends on N=41 (includes AK and CT with MC penetration COALESCE'd to 0). Without those 2 states (N=39), p=0.481. Always disclose this sensitivity.",
     "Session 30 V2 audit Prompt 3"),

    ("pharmacy", "failure_mode",
     "Pharmacy spread analysis: NADAC per_unit for Sodium Chloride is $0.00. Markup ratio = infinity. Always use NULLIF(nadac_per_unit, 0) or WHERE nadac_per_unit > 0.",
     "Session 29 edge case audit"),

    # -- Strategies (effective reasoning patterns) --
    ("expenditure", "strategy",
     "For cross-state spending comparisons, triangulate: CMS-64 (fact_cms64_multiyear) for total/federal/state split, MACPAC (fact_macpac_spending_per_enrollee) for per-enrollee by eligibility group, NHE (fact_nhe_medicaid_aggregate) for 30-year service category trends.",
     "Build Principles"),

    ("rates", "strategy",
     "For states without published fee schedules (TN), use the T-MSIS calibration approach: claims average / Southeast state discount factor (78.5%). Produces simulated fee schedule estimates with ranges.",
     "Session 30 T-MSIS calibration"),

    ("quality", "strategy",
     "Panel fixed effects (within-transformation by state) absorbs all time-invariant state characteristics. For quality trend analysis, use fact_quality_core_set_combined (8 years, 2017-2024) with state FE. The year trend coefficient gives the national quality trajectory.",
     "Research replication methodology"),

    ("nursing", "strategy",
     "For nursing home ownership-quality analysis, use propensity score matching (beds, state, acuity) rather than raw comparison. PSM with 10,737 matched pairs confirms -0.67 star for-profit penalty. Raw Cohen's d = 0.50.",
     "Session 30 advanced methods"),

    # -- Query Patterns (SQL patterns that work) --
    ("rates", "query_pattern",
     "For per-enrollee spending by state: SELECT state_code, SUM(total_computable) / NULLIF(MAX(e.total_enrollment), 0) FROM fact_cms64_multiyear c JOIN fact_enrollment e ON c.state_code = e.state_code AND c.fiscal_year = e.year WHERE c.state_code != 'US' GROUP BY state_code.",
     "Common query pattern"),

    ("pharmacy", "query_pattern",
     "For latest NADAC price per NDC: SELECT ndc, nadac_per_unit FROM fact_nadac QUALIFY ROW_NUMBER() OVER (PARTITION BY ndc ORDER BY effective_date DESC) = 1. This is faster than a correlated subquery on 1.9M rows.",
     "Pharmacy spread analysis"),

    ("rates", "query_pattern",
     "For SVI by state (fact_svi_county uses st_abbr, NOT state_code): SELECT st_abbr AS state_code, AVG(rpl_themes) AS avg_svi FROM fact_svi_county WHERE rpl_themes >= 0 GROUP BY st_abbr.",
     "Session 30 schema fix"),

    ("enrollment", "query_pattern",
     "For Medicaid expansion status: SELECT state_code, expansion_status, expansion_date, expansion_type FROM dim_state WHERE expansion_status = 'Expanded'. 41 expanded states, 10 not expanded (AL, FL, GA, KS, MS, SC, TN, TX, WI, WY).",
     "dim_state expansion columns"),
]


def main():
    print("Initializing Skillbook...")
    ensure_table()

    print(f"Seeding {len(SEEDS)} skills...")
    for domain, category, content, provenance in SEEDS:
        skill_id = add_skill(
            domain=domain,
            category=category,
            content=content,
            source_type="manual",
            provenance=provenance,
        )
        if skill_id:
            print(f"  + [{category}] {domain}: {content[:60]}...")

    print(f"\nDone. {len(SEEDS)} skills seeded.")


if __name__ == "__main__":
    main()
