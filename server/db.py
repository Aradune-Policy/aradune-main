"""
DuckDB connection backed by the Aradune data lake (Parquet files).

On startup, creates an in-memory DuckDB and registers lake Parquet files
as views. All queries hit these views — no persistent DuckDB file needed.
"""

import duckdb
from contextlib import contextmanager
from pathlib import Path

from server.config import settings

_conn: duckdb.DuckDBPyConnection | None = None


def _latest_snapshot(fact_dir: Path, fact_name: str) -> Path | None:
    """Find the most recent snapshot Parquet for a fact table."""
    fact_path = fact_dir / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


def init_db() -> None:
    """Create in-memory DuckDB and register lake Parquet files as views."""
    global _conn
    _conn = duckdb.connect()

    lake = Path(settings.lake_dir)
    dim_dir = lake / "dimension"
    fact_dir = lake / "fact"

    # Register dimension tables
    for parquet_file in dim_dir.glob("*.parquet"):
        view_name = parquet_file.stem  # dim_state, dim_procedure, etc.
        _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")

    # Register fact tables (latest snapshot)
    fact_names = [
        "medicaid_rate", "medicare_rate", "medicare_rate_state",
        "rate_comparison", "dq_flag",
        "enrollment", "quality_measure", "expenditure",
        "claims", "claims_monthly", "claims_categories", "provider",
        "drug_utilization", "nadac", "managed_care",
        "dsh_payment", "fmap", "spa",
        "bls_wage", "bls_wage_msa", "bls_wage_national",
        "hospital_cost", "snf_cost",
        "eligibility", "new_adult", "unwinding", "mc_enrollment",
        "pbj_nurse_staffing", "pbj_nonnurse_staffing", "pbj_employee",
        "five_star", "hac_measure", "pos_hospital", "pos_other",
        "hospital_rating", "hospital_vbp", "hospital_hrrp", "epsdt",
        "mspb_state", "timely_effective", "complications",
        "unplanned_visits", "dialysis_state", "home_health_state",
        "mltss", "financial_mgmt", "eligibility_levels",
        "aca_ful", "dq_atlas",
        "cpi", "unemployment", "median_income", "mspb_hospital",
        "hpsa",
        "scorecard", "elig_group_monthly", "elig_group_annual",
        "cms64_new_adult", "ffcra_fmap",
        "mc_enroll_pop", "mc_enroll_duals", "hai_state",
        "hai_hospital", "nh_ownership",
        "acs_state", "drug_overdose", "mortality_trend",
        "state_gdp", "state_population", "nh_penalties",
        "nh_deficiencies", "brfss", "hcahps_state",
        "fmr_supplemental", "macpac_supplemental",
        "dsh_hospital", "sdp_preprint",
        "nsduh_prevalence", "mh_facility",
        "ipf_quality_state", "ipf_quality_facility",
        "mds_quality", "nh_provider_info",
        "brfss_behavioral",
        "provider_specific", "block_grant",
        "chip_enrollment", "hcbs_waiver_enrollment",
        "hospice_quality", "chip_eligibility", "continuous_eligibility",
        "hcbs_authority", "mc_quality_features",
        "maternal_health", "hospice_state", "asc_quality_state",
        "home_health_state2", "oas_cahps_state",
        "cms372_waiver", "mc_enrollment_plan", "mltss_enrollment2",
        "mc_enrollment_pop2",
        "bh_by_condition", "mh_sud_recipients", "maternal_morbidity",
        "dental_services", "telehealth_services",
        "irf_provider", "ltch_provider", "home_health_agency",
        "physical_among_mh", "physical_among_sud",
        "bh_services", "integrated_care", "1915c_participants",
        "mc_share", "mc_monthly",
        "dialysis_facility", "ipf_facility", "hospice_provider",
        "health_screenings", "acute_care", "perinatal_care",
        "mc_summary", "asc_facility",
        "hospital_directory",
        "mc_programs",
        "chip_enrollment_unwinding", "chip_program_monthly",
        "medicaid_applications", "vaccinations",
        "blood_lead_screening", "renewal_processing",
        "dual_status_monthly", "benefit_package", "program_annual",
        "nas_rates", "pregnant_postpartum", "smm_extended",
        "caa_fmap",
        "hai_hospital2", "complications_hosp",
        "timely_effective_hosp", "unplanned_visits_hosp",
        "psi90_hospital", "snf_vbp",
        "nh_claims_quality", "snf_quality_provider",
        "nh_state_averages",
        "fqhc_directory", "vital_stats", "maternal_mortality_national",
        "hhcahps_provider", "hhcahps_state",
        "hospice_directory", "hospice_cahps_state",
        "medicare_spending_claim", "vha_provider",
        "pregnancy_outcomes", "chip_program_annual",
        "well_child_visits", "financial_mgmt_national",
        "mspb_hospital_detail", "imaging_hospital", "esrd_qip",
        "ahrf_county", "physician_compare",
        "medicare_enrollment", "cms_impact", "opioid_prescribing",
        "otp_provider", "cms64_ffcra", "contraceptive_care",
        "respiratory_conditions", "esrd_qip_tps",
        "program_monthly", "mc_annual", "mc_info_monthly",
        "chip_monthly", "chip_app_elig", "performance_indicator",
        "new_adult_enrollment", "drug_rebate_products", "sdud_2024",
        "medicare_provider_enrollment",
        "snap_enrollment", "tanf_enrollment",
        "fair_market_rent",
        "sdud_2025",
        "eligibility_processing", "marketplace_unwinding", "sbm_unwinding",
        "quality_core_set_2023", "quality_core_set_2024", "hcbs_waitlist",
        "ltss_expenditure", "ltss_users", "ltss_rebalancing",
        "vital_stats_monthly", "maternal_mortality_monthly",
        "fmr_fy2024", "new_adult_spending",
        "nsduh_prevalence_2024",
        "mc_enrollment_summary",
        "saipe_poverty", "places_county", "health_center_sites",
        "marketplace_oep", "mua_designation", "workforce_projections",
        "food_environment",
        "medicare_telehealth", "medicare_geo_variation", "ma_geo_variation",
        "medicaid_drug_spending", "mc_dashboard",
        "nhe_state",
        "mssp_aco", "mssp_participants", "aco_beneficiaries_county",
        "aco_reach_results", "part_d_geo", "part_d_quarterly_spending",
        "nhsc_field_strength", "fqhc_hypertension", "fqhc_quality_badges",
        "macpac_enrollment", "macpac_spending_per_enrollee",
        "nursing_workforce", "nursing_earnings",
        "teds_admissions", "medicare_program_stats",
        "hospital_service_area", "hha_cost_report",
        "esrd_etc_results", "pac_hha_utilization",
        "pac_irf_utilization", "pac_ltch_utilization",
        "market_saturation_county", "medicare_physician_geo",
        "mssp_financial_results", "nh_penalties_v2", "nh_survey_summary",
        "dialysis_facility_v2", "cdc_overdose_deaths", "cdc_leading_causes_death",
        "part_d_opioid_geo", "part_d_spending_by_drug",
        "macpac_spending_by_state", "macpac_benefit_spending",
    ]
    for fact_name in fact_names:
        p = _latest_snapshot(fact_dir, fact_name)
        if p:
            view_name = f"fact_{fact_name}"
            _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{p}'")

    # Register reference tables
    ref_dir = lake / "reference"
    if ref_dir.exists():
        for parquet_file in ref_dir.glob("*.parquet"):
            view_name = parquet_file.stem  # ref_drug_rebate, ref_ncci_edits, etc.
            _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")
        # Also register snapshot-based reference tables
        for ref_subdir in ref_dir.iterdir():
            if ref_subdir.is_dir():
                p = _latest_snapshot(ref_dir, ref_subdir.name)
                if p:
                    view_name = f"ref_{ref_subdir.name}"
                    _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{p}'")

    # Create a unified 'spending' view for backward compatibility with query_builder
    # Maps the old column names to the new lake schema
    claims_path = _latest_snapshot(fact_dir, "claims")
    provider_path = _latest_snapshot(fact_dir, "provider")
    if claims_path:
        _conn.execute(f"""
            CREATE VIEW spending AS
            SELECT
                state_code            AS state,
                procedure_code        AS HCPCS_CODE,
                category,
                year,
                month,
                total_paid            AS TOTAL_PAID,
                total_claims          AS TOTAL_CLAIMS,
                total_beneficiaries   AS TOTAL_UNIQUE_BENEFICIARIES,
                provider_count,
                avg_paid_per_claim,
                claim_type,
                LPAD(CAST(year AS VARCHAR), 4, '0') || '-' ||
                    LPAD(COALESCE(CAST(month AS VARCHAR), '01'), 2, '0')
                                      AS CLAIM_FROM_MONTH,
                snapshot_date
            FROM '{claims_path}'
        """)

    if provider_path:
        _conn.execute(f"""
            CREATE VIEW spending_providers AS
            SELECT
                npi                   AS BILLING_PROVIDER_NPI_NUM,
                state_code            AS state,
                provider_name,
                zip3,
                taxonomy_code         AS taxonomy,
                total_paid            AS TOTAL_PAID,
                total_claims          AS TOTAL_CLAIMS,
                total_beneficiaries   AS TOTAL_UNIQUE_BENEFICIARIES,
                code_count
            FROM '{provider_path}'
        """)


def close_db() -> None:
    global _conn
    if _conn:
        _conn.close()
        _conn = None


@contextmanager
def get_cursor():
    """Yield a thread-safe DuckDB cursor from the shared connection."""
    if _conn is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    cursor = _conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
