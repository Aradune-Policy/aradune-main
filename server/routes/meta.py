from fastapi import APIRouter, HTTPException
from server.models import QueryMeta
from server.db import get_cursor
from server.presets import PRESETS
from server.utils.error_handler import safe_route

router = APIRouter()

# ── Table descriptions for the data catalog ────────────────────────────
TABLE_DESCRIPTIONS = {
    "fact_medicaid_rate": "Medicaid fee schedule rates by state, code, and modifier (47 states)",
    "fact_rate_comparison": "Medicaid vs Medicare rate comparison with pct_of_medicare (45 states)",
    "fact_enrollment": "Monthly Medicaid enrollment by state (total, CHIP, FFS, managed care)",
    "fact_claims": "T-MSIS claims aggregated by state, procedure code, and month",
    "fact_hospital_cost": "HCRIS hospital cost reports — financials, beds, payer mix, margins",
    "fact_bls_wage": "BLS OEWS healthcare occupation wages by state (16 occupations)",
    "fact_quality_measure": "Medicaid Adult/Child Core Set quality measures",
    "fact_expenditure": "CMS-64 Medicaid expenditure by state, category, and quarter",
    "fact_hpsa": "HRSA Health Professional Shortage Area designations (3 disciplines)",
    "fact_nsduh_prevalence": "SAMHSA behavioral health prevalence estimates by state",
    "fact_scorecard": "CMS Medicaid Scorecard measures with state and national benchmarks",
    "fact_unwinding": "PHE unwinding redetermination outcomes by state",
    "fact_fmap": "Federal Medical Assistance Percentages (FMAP/eFMAP) by state",
    "fact_drug_utilization": "State Drug Utilization Data (SDUD) — Medicaid prescriptions",
    "fact_nadac": "National Average Drug Acquisition Cost pharmacy pricing",
    "fact_dsh_payment": "Disproportionate Share Hospital payments by state",
    "fact_dsh_hospital": "Hospital-level DSH data (6,103 hospitals)",
    "fact_managed_care": "Managed care plan enrollment by state and plan type",
    "fact_five_star": "CMS Five-Star nursing facility quality ratings",
    "fact_hospital_rating": "Overall hospital quality star ratings from CMS",
    "fact_hospital_vbp": "Hospital Value-Based Purchasing program scores",
    "fact_hospital_hrrp": "Hospital Readmissions Reduction Program data",
    "fact_acs_state": "Census ACS demographics — population, poverty, income, insurance",
    "fact_unemployment": "Monthly state unemployment rates from BLS LAUS",
    "fact_medicaid_opioid_prescribing": "Medicare Part D opioid prescribing rates by state",
    "fact_maternal_health": "Hospital-level maternal health quality measures",
    "fact_telehealth_services": "Telehealth utilization by state and service type",
    "fact_dental_services": "Dental services to Medicaid children under 19",
    "fact_chip_enrollment": "CHIP enrollment counts by state and month",
    "fact_block_grant": "SAMHSA Mental Health Block Grant allotments by state",
    "fact_mh_facility": "SAMHSA treatment facility directory with bed counts",
    "fact_epsdt": "Early and Periodic Screening, Diagnostic, and Treatment (CMS-416)",
    "fact_hospice_quality": "Hospice facility-level quality measures (4,948 hospices)",
    "fact_medicare_enrollment": "Medicare enrollment by state including MA penetration",
    "fact_sdud_2024": "State Drug Utilization Data — 2024 quarterly data",
    "fact_sdud_2025": "State Drug Utilization Data — 2025 Q1-Q2 (2.64M rows, $108.8B reimbursed)",
    "fact_cms372_waiver": "CMS-372 waiver program records with expenditure data",
    "fact_bh_by_condition": "Behavioral health conditions by state from T-MSIS",
    "fact_irf_provider": "Inpatient rehabilitation facility quality measures",
    "fact_ltch_provider": "Long-term care hospital quality measures",
    "fact_home_health_agency": "Home health agency directory with quality ratings",
    "fact_snap_enrollment": "SNAP (food stamps) monthly participation and benefit cost by state (FY2019-2026)",
    "fact_tanf_enrollment": "TANF monthly families and recipients by state (FY2023-2024, 7 measures)",
    "fact_fair_market_rent": "HUD Fair Market Rents by county — 0BR-4BR (FY2025, 4,764 areas)",
    "fact_eligibility_processing": "Medicaid/CHIP renewal and redetermination outcomes by state (3,162 rows)",
    "fact_marketplace_unwinding": "HealthCare.gov Medicaid unwinding marketplace transitions (59,527 rows)",
    "fact_sbm_unwinding": "State-Based Marketplace Medicaid unwinding data (128 rows)",
    "fact_quality_core_set_2023": "FFY 2023 Child/Adult Core Set quality measures — 56 measures, 51 states",
    "fact_quality_core_set_2024": "FFY 2024 Child/Adult Core Set quality measures — 57 measures, 51 states (first mandatory year)",
    "fact_hcbs_waitlist": "HCBS waiting lists by state and population — 606,895 people across 41 states (KFF 2025)",
    "fact_ltss_expenditure": "Medicaid LTSS expenditure by state — institutional vs HCBS breakdown (CY 2022-2023)",
    "fact_ltss_users": "Medicaid LTSS users by state — institutional vs HCBS vs both (CY 2022-2023)",
    "fact_ltss_rebalancing": "LTSS HCBS rebalancing measures — % HCBS by age group and state (CY 2022-2023)",
    "fact_vital_stats_monthly": "CDC VSRR monthly births, deaths, and infant deaths by state (2023-2024)",
    "fact_maternal_mortality_monthly": "CDC provisional maternal mortality — rolling 12-month rates by demographics",
    "fact_fmr_fy2024": "CMS-64 Financial Management Report FY 2024 — 80+ service categories, all states ($909B total)",
    "fact_new_adult_spending": "Medicaid VIII Group (New Adult/Expansion) expenditures by state (Q3 FY 2025)",
    "fact_nsduh_prevalence_2024": "SAMHSA NSDUH 2023-2024 state behavioral health prevalence — 18 measures, 52 states",
    "fact_mc_enrollment_summary": "Managed care enrollment summary by state and year (2016-2024, 57 states, MC penetration %)",
    "fact_saipe_poverty": "Census SAIPE 2023 poverty estimates & median income — state and county level (3,196 rows)",
    "fact_places_county": "CDC PLACES county health estimates — 40 measures (asthma, diabetes, insurance, etc.) for 3,144 counties",
    "fact_health_center_sites": "HRSA FQHC & look-alike health center sites directory (8,121 sites, 56 fields)",
    "fact_marketplace_oep": "ACA Marketplace 2025 Open Enrollment — plan selections, premiums, APTC by state (54 rows, 102 fields)",
    "fact_mua_designation": "HRSA Medically Underserved Areas/Populations designations (19,645 areas across all states)",
    "fact_workforce_projections": "HRSA healthcare workforce supply/demand projections 2023-2038 — 121 professions by state",
    "fact_food_environment": "USDA Food Environment Atlas — 304 variables by county (food access, SNAP stores, insecurity, health)",
    "fact_medicare_telehealth": "Medicare telehealth utilization by state, quarter, demographics (Q1 2020–Q2 2025, 32K rows)",
    "fact_medicare_geo_variation": "Medicare FFS geographic variation — spending, utilization, quality by state/county (2014-2023, 247 columns)",
    "fact_ma_geo_variation": "Medicare Advantage geographic variation by state (2016-2022, 378 rows)",
    "fact_medicaid_drug_spending": "Medicaid spending by drug — brand/generic, 2019-2023, top drugs by total spending",
    "fact_mc_dashboard": "Medicaid managed care dashboard — MCO utilization by county, service category (AZ/MI/NV/NM, 2020+)",
    "fact_nhe_state": "National Health Expenditure by state — Medicaid/Medicare/private/total spending 1991-2020 (117K rows)",
    "fact_mssp_aco": "Medicare Shared Savings Program ACO organizations — 511 ACOs with track, assignment, service area (PY2026)",
    "fact_mssp_participants": "MSSP ACO participant TINs/NPIs — 15,370 provider-ACO linkages (PY2026)",
    "fact_aco_beneficiaries_county": "ACO assigned beneficiaries by county — ESRD, disabled, aged (135K rows, 56 states, 2024)",
    "fact_aco_reach_results": "ACO REACH financial & quality results — savings, quality scores (132 ACOs, PY3)",
    "fact_part_d_geo": "Medicare Part D prescribing by state and drug — claims, spending, opioid flags (116K rows, 2023)",
    "fact_part_d_quarterly_spending": "Medicare Part D quarterly drug spending — brand/generic, manufacturer, trends (28K rows)",
    "fact_nhsc_field_strength": "NHSC clinician counts by state & discipline — primary care, mental health, oral health (FY2025)",
    "fact_fqhc_hypertension": "FQHC hypertension control rates from UDS — health center level (6,866 rows, 2019-2023)",
    "fact_fqhc_quality_badges": "FQHC quality recognition badges — gold/silver/bronze, HIT, access (7,438 rows, 2021-2025)",
    "fact_macpac_enrollment": "MACPAC Exhibit 14 — Medicaid enrollment by state, eligibility group, dual status (FY 2023)",
    "fact_macpac_spending_per_enrollee": "MACPAC Exhibit 22 — Medicaid benefit spending per FYE enrollee by state and group (FY 2023)",
    "fact_nursing_workforce": "NSSRN nursing workforce demographics by state — RN/LPN/APRN counts (2022 survey, 17.6K rows)",
    "fact_nursing_earnings": "NSSRN nursing earnings & hours by state — primary/total earnings distributions (2022 survey, 41.8K rows)",
    "fact_teds_admissions": "SAMHSA TEDS-A substance abuse treatment admissions by state — substance types, demographics, insurance (49 states, 1.6M admissions, 2023)",
    "fact_medicare_program_stats": "CMS Medicare Part A & B program statistics — utilization, payments, cost sharing (national, 2018-2023)",
    "fact_hospital_service_area": "CMS Hospital Service Area — Medicare discharges by hospital and ZIP code (1.16M rows, 2024)",
    "fact_hha_cost_report": "Home Health Agency cost reports — revenues, costs, visits (10,715 agencies, FY2023)",
    "fact_esrd_etc_results": "ESRD Treatment Choices model results — facility aggregation group performance (433 rows)",
    "fact_pac_hha_utilization": "Medicare post-acute care HHA utilization — episodes, spending, outcomes by state (8,519 rows, 2023)",
    "fact_pac_irf_utilization": "Medicare post-acute care IRF utilization — stays, spending, outcomes by state (1,205 rows, 2023)",
    "fact_pac_ltch_utilization": "Medicare post-acute care LTCH utilization — stays, spending, outcomes by state (373 rows, 2023)",
    "fact_market_saturation_county": "CMS Market Saturation & Utilization by county — provider counts, utilization rates, per-capita measures (962K rows)",
    "fact_medicare_physician_geo": "Medicare Physician & Other Practitioners by geography and service — utilization, payments, beneficiaries (269K rows, 2023)",
    "fact_mssp_financial_results": "MSSP ACO financial & quality results — savings/losses, shared savings payments, quality scores (476 ACOs, PY2024)",
    "fact_nh_penalties_v2": "Nursing home penalties — fines and payment denials in last 3 years (17,463 rows, Feb 2026)",
    "fact_nh_survey_summary": "Nursing home survey results summary — deficiencies, scores per facility (43,983 rows, Feb 2026)",
    "fact_dialysis_facility_v2": "Dialysis Facility Compare — all Medicare dialysis facilities with quality measures (7,557 facilities)",
    "fact_cdc_overdose_deaths": "CDC provisional drug overdose death counts by state and drug type — monthly (81,270 rows)",
    "fact_cdc_leading_causes_death": "CDC leading causes of death by state — age-adjusted rates since 1999 (10,868 rows)",
    "fact_part_d_opioid_geo": "Part D opioid prescribing rates by state and county — opioid claims, LA opioid rates (329K rows, 2023)",
    "fact_part_d_spending_by_drug": "Part D total spending per drug — brand/generic, manufacturer, cost trends (14,309 drugs, 2023)",
    "fact_macpac_spending_by_state": "MACPAC Exhibit 16 — Medicaid spending by state, benefits/admin, federal/state split (FY 2024, millions)",
    "fact_macpac_benefit_spending": "MACPAC Exhibit 17 — Medicaid benefit spending by state and category (hospital, physician, drugs, MC, DSH, FY 2024)",
    "dim_state": "State dimension — codes, names, FMAP, methodology, enrollment",
    "dim_procedure": "HCPCS/CPT procedure codes with RVUs and Medicare rates",
    "dim_medicare_locality": "Medicare GPCI values by locality",
}


@router.get("/api/meta", response_model=QueryMeta)
@safe_route(default_response={})
async def meta():
    try:
        with get_cursor() as cur:
            states = [r[0] for r in cur.execute(
                "SELECT DISTINCT state FROM spending WHERE state IS NOT NULL ORDER BY state"
            ).fetchall()]

            categories = [r[0] for r in cur.execute(
                "SELECT DISTINCT category FROM spending WHERE category IS NOT NULL ORDER BY category"
            ).fetchall()]

            date_range = cur.execute(
                "SELECT MIN(CLAIM_FROM_MONTH), MAX(CLAIM_FROM_MONTH) FROM spending"
            ).fetchone()

            columns = [r[0] for r in cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'spending' ORDER BY ordinal_position"
            ).fetchall()]

            total_rows = cur.execute("SELECT COUNT(*) FROM spending").fetchone()[0]

        return QueryMeta(
            states=states,
            categories=categories,
            date_min=str(date_range[0]) if date_range and date_range[0] else None,
            date_max=str(date_range[1]) if date_range and date_range[1] else None,
            columns=columns,
            total_rows=total_rows,
            presets=list(PRESETS.keys()),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Meta query error: {e}")


@router.get("/api/catalog")
@safe_route(default_response={"tables": [], "total_tables": 0, "total_rows": 0})
async def catalog():
    """Return metadata about all available tables — name, row count, columns, description."""
    try:
        with get_cursor() as cur:
            # Get all views (our registered tables)
            views = cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'VIEW' ORDER BY table_name"
            ).fetchall()

            tables = []
            for (view_name,) in views:
                # Skip compat views
                if view_name in ("spending", "spending_providers"):
                    continue
                try:
                    row_count = cur.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
                    cols = cur.execute(
                        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {view_name})"
                    ).fetchall()
                    tables.append({
                        "name": view_name,
                        "rows": row_count,
                        "columns": [{"name": c[0], "type": c[1]} for c in cols],
                        "description": TABLE_DESCRIPTIONS.get(view_name, ""),
                        "category": "dimension" if view_name.startswith("dim_") else "reference" if view_name.startswith("ref_") else "fact",
                    })
                except Exception:
                    continue

            total_rows = sum(t["rows"] for t in tables)
            return {
                "tables": tables,
                "total_tables": len(tables),
                "total_rows": total_rows,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Catalog error: {e}")


@router.get("/api/validation")
@safe_route(default_response={"total": 0, "passed": 0, "failed": 0, "results": []})
async def run_validation():
    """Run core data validation checks and return results."""
    try:
        from server.engines.validator import run_core_checks
        results = run_core_checks()
        passed = sum(1 for r in results if r["passed"])
        failed = sum(1 for r in results if not r["passed"])
        return {
            "total": len(results), "passed": passed, "failed": failed,
            "results": results,
        }
    except Exception as e:
        return {"total": 0, "passed": 0, "failed": 0, "error": str(e), "results": []}
