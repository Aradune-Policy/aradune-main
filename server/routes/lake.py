"""Data lake exploration routes — dimensions, enrollment, quality, expenditure."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/states")
async def states():
    """Get all states with metadata from dim_state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code, state_name, region, methodology,
                conversion_factor, fmap, total_enrollment,
                ffs_enrollment, mc_enrollment, pct_managed_care,
                fee_index, update_frequency
            FROM dim_state
            ORDER BY state_code
        """).fetchall()
        columns = [
            "state_code", "state_name", "region", "methodology",
            "conversion_factor", "fmap", "total_enrollment",
            "ffs_enrollment", "mc_enrollment", "pct_managed_care",
            "fee_index", "update_frequency",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/enrollment/{state_code}")
async def enrollment(state_code: str):
    """Get enrollment history for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT year, month,
                   MAX(total_enrollment) AS total_enrollment,
                   MAX(chip_enrollment) AS chip_enrollment,
                   MAX(ffs_enrollment) AS ffs_enrollment,
                   MAX(mc_enrollment) AS mc_enrollment
            FROM fact_enrollment
            WHERE state_code = $1
            GROUP BY year, month
            ORDER BY year, month
        """, [state_code]).fetchall()
        columns = ["year", "month", "total_enrollment", "chip_enrollment",
                    "ffs_enrollment", "mc_enrollment"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/quality/{state_code}")
async def quality_measures(state_code: str, year: int = Query(None)):
    """Get quality measures for a state."""
    state_code = state_code.upper()
    year_filter = "AND year = $2" if year else ""
    params = [state_code] + ([year] if year else [])

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT measure_id, measure_name, domain, year,
                   rate, numerator, denominator
            FROM fact_quality_measure
            WHERE state_code = $1 {year_filter}
            ORDER BY domain, measure_id, year
        """, params).fetchall()
        columns = ["measure_id", "measure_name", "domain", "year",
                    "rate", "numerator", "denominator"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/expenditure/{state_code}")
async def expenditure(state_code: str):
    """Get CMS-64 expenditure data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT fiscal_year, quarter, category, subcategory,
                   federal_share, total_computable
            FROM fact_expenditure
            WHERE state_code = $1
            ORDER BY fiscal_year, quarter, category
        """, [state_code]).fetchall()
        columns = ["fiscal_year", "quarter", "category", "subcategory",
                    "federal_share", "total_computable"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/lake/stats")
async def lake_stats():
    """Get data lake statistics — row counts, data freshness, etc."""
    with get_cursor() as cur:
        stats = {}
        tables = [
            ("dim_state", "dim_state"),
            ("dim_procedure", "dim_procedure"),
            ("dim_hcpcs", "dim_hcpcs"),
            ("fact_claims", "fact_claims"),
            ("fact_provider", "fact_provider"),
            ("fact_rate_comparison", "fact_rate_comparison"),
            ("fact_dq_flag", "fact_dq_flag"),
            ("fact_enrollment", "fact_enrollment"),
            ("fact_quality_measure", "fact_quality_measure"),
            ("fact_medicaid_rate", "fact_medicaid_rate"),
            ("fact_medicare_rate", "fact_medicare_rate"),
            ("fact_drug_utilization", "fact_drug_utilization"),
            ("fact_nadac", "fact_nadac"),
            ("fact_managed_care", "fact_managed_care"),
            ("fact_dsh_payment", "fact_dsh_payment"),
            ("fact_fmap", "fact_fmap"),
            ("fact_spa", "fact_spa"),
            ("fact_bls_wage", "fact_bls_wage"),
            ("fact_bls_wage_msa", "fact_bls_wage_msa"),
            ("fact_bls_wage_national", "fact_bls_wage_national"),
            ("fact_hospital_cost", "fact_hospital_cost"),
            ("fact_snf_cost", "fact_snf_cost"),
            ("fact_eligibility", "fact_eligibility"),
            ("fact_new_adult", "fact_new_adult"),
            ("fact_unwinding", "fact_unwinding"),
            ("fact_mc_enrollment", "fact_mc_enrollment"),
            ("fact_pbj_nurse_staffing", "fact_pbj_nurse_staffing"),
            ("fact_pbj_nonnurse_staffing", "fact_pbj_nonnurse_staffing"),
            ("fact_five_star", "fact_five_star"),
            ("fact_hac_measure", "fact_hac_measure"),
            ("fact_pos_hospital", "fact_pos_hospital"),
            ("fact_pos_other", "fact_pos_other"),
            ("fact_hospital_rating", "fact_hospital_rating"),
            ("fact_hospital_vbp", "fact_hospital_vbp"),
            ("fact_hospital_hrrp", "fact_hospital_hrrp"),
            ("fact_epsdt", "fact_epsdt"),
            ("fact_mspb_state", "fact_mspb_state"),
            ("fact_timely_effective", "fact_timely_effective"),
            ("fact_complications", "fact_complications"),
            ("fact_unplanned_visits", "fact_unplanned_visits"),
            ("fact_dialysis_state", "fact_dialysis_state"),
            ("fact_home_health_state", "fact_home_health_state"),
            ("fact_mltss", "fact_mltss"),
            ("fact_financial_mgmt", "fact_financial_mgmt"),
            ("fact_eligibility_levels", "fact_eligibility_levels"),
            ("fact_aca_ful", "fact_aca_ful"),
            ("fact_dq_atlas", "fact_dq_atlas"),
            ("fact_cpi", "fact_cpi"),
            ("fact_unemployment", "fact_unemployment"),
            ("fact_median_income", "fact_median_income"),
            ("fact_mspb_hospital", "fact_mspb_hospital"),
            ("ref_drug_rebate", "ref_drug_rebate"),
            ("ref_ncci_edits", "ref_ncci_edits"),
            ("fact_hpsa", "fact_hpsa"),
            ("fact_scorecard", "fact_scorecard"),
            ("fact_elig_group_monthly", "fact_elig_group_monthly"),
            ("fact_elig_group_annual", "fact_elig_group_annual"),
            ("fact_cms64_new_adult", "fact_cms64_new_adult"),
            ("fact_ffcra_fmap", "fact_ffcra_fmap"),
            ("fact_mc_enroll_pop", "fact_mc_enroll_pop"),
            ("fact_mc_enroll_duals", "fact_mc_enroll_duals"),
            ("fact_hai_state", "fact_hai_state"),
            ("fact_hai_hospital", "fact_hai_hospital"),
            ("fact_nh_ownership", "fact_nh_ownership"),
            ("fact_acs_state", "fact_acs_state"),
            ("fact_drug_overdose", "fact_drug_overdose"),
            ("fact_mortality_trend", "fact_mortality_trend"),
            ("fact_state_gdp", "fact_state_gdp"),
            ("fact_state_population", "fact_state_population"),
            ("fact_nh_penalties", "fact_nh_penalties"),
            ("fact_nh_deficiencies", "fact_nh_deficiencies"),
            ("fact_brfss", "fact_brfss"),
            ("fact_hcahps_state", "fact_hcahps_state"),
            ("fact_imaging_hospital", "fact_imaging_hospital"),
            ("ref_1115_waivers", "ref_1115_waivers"),
        ]
        for name, view in tables:
            try:
                count = cur.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
                stats[name] = count
            except Exception:
                stats[name] = None

        return {
            "tables": stats,
            "total_rows": sum(v for v in stats.values() if v),
        }
