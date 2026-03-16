"""
DuckDB connection backed by the Aradune data lake (Parquet files).

Views are registered lazily on first access. The DuckDB connection is
created immediately (for health checks), but individual Parquet views
are only created when a query first touches them. This keeps startup
under 1 second and lets the health check pass before the full lake is
scanned.
"""

import duckdb
import threading
from contextlib import contextmanager
from pathlib import Path

from server.config import settings

_conn: duckdb.DuckDBPyConnection | None = None
_lake_ready = False          # True once all views have been registered
_registered: set[str] = set()  # Views already created
_lock = threading.Lock()


def _latest_snapshot(fact_dir: Path, fact_name: str) -> Path | None:
    """Find the most recent snapshot Parquet for a fact table."""
    fact_path = fact_dir / fact_name
    if not fact_path.exists():
        return None
    # Check for direct data.parquet first (newer convention)
    direct = fact_path / "data.parquet"
    if direct.exists():
        return direct
    # Fall back to snapshot partitions
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


# ── Canonical list of fact tables ────────────────────────────────────
FACT_NAMES = [
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
    "irf_provider", "ltch_provider", "ltch_general", "home_health_agency",
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
    "quality_core_set_2017", "quality_core_set_2018", "quality_core_set_2019",
    "quality_core_set_2020", "quality_core_set_2021", "quality_core_set_2022",
    "quality_core_set_2023", "quality_core_set_2024", "quality_core_set_combined",
    "hcbs_waitlist",
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
    "mssp_financial_results", "nh_penalties_v2", "nh_survey_summary", "nh_deficiency",
    "dialysis_facility_v2", "cdc_overdose_deaths", "cdc_leading_causes_death",
    "part_d_opioid_geo", "part_d_spending_by_drug",
    "macpac_spending_by_state", "macpac_benefit_spending",
    "nh_mds_quality", "medicaid_opioid_prescribing",
    "hospital_timely_effective",
    "policy_document", "policy_chunk",
    "pfs_rvu", "pfs_opps_cap",
    "leie", "open_payments", "mco_mlr",
    "medicare_provider", "medicare_procedure_summary",
    "order_referring",
    "clfs", "ambulance", "dmepos",
    "federal_register", "mc_programs_by_state",
    "opioid_prescribing_geo", "drug_spending_trend",
    "mh_sud_services", "mc_enrollment_annual", "mc_enrollment_monthly",
    "pace_directory", "presumptive_eligibility",
    "ipps_impact", "viii_group_expenditure",
    "dsh_annual",
    "medicaid_applications_v2",
    "mh_facility_survey",
    "medicare_outpatient_geo", "medicare_outpatient_provider",
    "snf_owners", "medicare_inpatient_geo",
    "pac_snf_utilization", "medicare_inpatient_provider",
    "medicare_inpatient_drg",
    "mc_info_monthly2", "mc_info_annual", "program_info_monthly",
    "bea_state_gdp", "bea_gdp_growth", "scorecard_detail",
    "cdc_maternal_mortality_prov", "cdc_vsrr_vital",
    "mltss_enrollees", "hac_measure_2025", "hospital_general_info",
    "mc_annual_v2", "mc_monthly_v2", "mc_features_population",
    "hcris_hospital", "hcris_snf", "bh_services_detail",
    "bls_medicaid_occupations", "part_d_prescriber_geo",
    "chip_program_type_annual", "chip_program_type_monthly",
    "pos_iqies", "smm_conditions", "well_child_visits_v2",
    "hospital_chow", "hospital_chow_owners",
    "snf_chow", "snf_chow_owners", "mcbs_income_assets",
    "aco_reach_providers", "county_expenditure_risk",
    "hospital_all_owners", "hospital_price_transparency",
    "ltc_facility_characteristics", "part_b_discarded_drugs",
    "nh_chain_performance", "revoked_providers", "optout_providers",
    "quality_measures_2024_full", "food_environment_full",
    "marketplace_oep_2025", "mc_features_qa", "mc_qa_features",
    "mds_quality_full", "nsumhss_2024", "pi_performance",
    "medicare_summary_ab", "epsdt_2024",
    "pbj_daily_nurse", "pbj_daily_nonnurse",
    "bls_oews", "leie_v2", "dsh_annual_v2",
    "pfs_rvu_2026", "mco_mlr_v2",
    "anesthesia_2026", "opps_cap_2026",
    "open_payments_v2", "respiratory_conditions_v2",
    "opioid_prescribing_rates", "provider_specific_v2",
    "nppes_endpoint", "nppes_practice_location",
    "medicare_procedure_summary_2024",
    "nsduh_state_2024", "nsduh_state_2023",
    "physician_compare_v2",
    "aco_snf_affiliates", "aco_advance_investment",
    # Session 16 - Part 1 (from raw files)
    "hpsa_dental", "hpsa_mental_health",
    "medicare_physician_provider", "hospice_quality_national",
    # Session 16 - Part 2 (downloaded from CMS catalog)
    "nh_chain_performance_v2", "mds_frequency",
    "home_infusion_provider", "pac_hospice_utilization",
    "aco_reach_2026", "hospital_price_transparency_v2",
    "medicare_dme_geo", "medicare_cert",
    "hospital_enrollments", "medicare_telehealth_v2",
    "part_b_discarded_drugs_v2", "part_b_spending_by_drug",
    "esrd_etc_facility", "fqhc_enrollments",
    "fqhc_all_owners", "hha_all_owners",
    "hospice_all_owners", "rhc_all_owners",
    "clia", "providers_missing_dci",
    "qpp_experience", "asm_participants",
    "market_saturation_cbsa", "mds_facility_level",
    # Session 16 - Part 3 (enrollments + drug spending)
    "hha_enrollments", "hospice_enrollments",
    "snf_enrollments", "rhc_enrollments",
    "medicaid_drug_spending_v2", "part_d_spending_by_drug_v2",
    "esrd_etc_clinician",
    # Session 16 - Part 4 (PAC + opioid + Medicare enrollment)
    "pac_hha_casemix", "pac_irf_casemix", "pac_snf_casemix",
    "optout_providers_v2", "revoked_providers_v2",
    "part_d_opioid_geo_v2", "medicaid_opioid_geo",
    "medicare_monthly_enrollment",
    # Session 16 - Part 5 (prescriber + OTP + quarterly)
    "part_d_prescriber_geo_v2", "otp_providers_v2",
    "ma_geo_variation_v2", "part_b_quarterly_spending",
    "part_d_quarterly_spending_v2", "innovation_model_summary",
    "order_referring_v2",
    # Session 16 - Part 6 (FISS + HAC + LTC + ACO)
    "fiss_attending_rendering", "hac_measure_provider_2025",
    "ltc_facility_characteristics_v2", "aco_beneficiaries_county_v2",
    "ahrq_psi11", "mdpp_suppliers",
    # Session 16 - Part 7 (batch 9)
    "mc_dashboard_v2", "revalidation_clinic_group",
    # Session 17 - overnight data run
    "pbj_employee_summary",
    "county_health_rankings",
    # KFF Medicaid data (28 tables)
    "kff_total_spending", "kff_spending_per_enrollee",
    "kff_spending_per_full_enrollee", "kff_spending_by_enrollment_group",
    "kff_spending_by_service", "kff_spending_acute_care",
    "kff_spending_ltc", "kff_federal_state_share",
    "kff_fmap", "kff_dsh_allotments",
    "kff_fee_index", "kff_eligibility_adults",
    "kff_eligibility_parents_hist", "kff_mc_penetration",
    "kff_mc_plan_type_enrollment", "kff_mco_count",
    "kff_mco_enrollment", "kff_mco_spending",
    "kff_mco_enrollment_by_plan", "kff_mco_parent_financials",
    "kff_enrollees_by_group", "kff_enrollees_by_race",
    "kff_dual_eligible", "kff_dual_spending",
    "kff_births_medicaid", "kff_chip_spending",
    "kff_chip_enhanced_fmap", "kff_child_participation",
    # HRSA workforce (4 tables)
    "health_center_awards", "bh_workforce_projections",
    "np_pa_supply", "nhsc_scholar_pipeline",
    # Federal/regulatory data (5 tables)
    "federal_register_cms", "mfcu_stats",
    "nhe", "nhe_projections", "perm_rates",
    # CMS catalog mining (8 tables)
    "cps_dual_enrollment", "cps_part_ab_summary",
    "cps_premiums", "cps_providers",
    "cps_part_d_utilization", "medicare_covid_hosp",
    "revalidation_due_date", "innovation_participants",
    # Chronic conditions + MACPAC + MCBS (6 new tables)
    "chronic_conditions_national", "chronic_conditions_all_medicare",
    "mcbs_cost_summary", "macpac_benefit_spending_v2",
    "macpac_spending_by_elig", "macpac_mc_enrollment_pct",
    # Medicaid.gov datasets (17 tables)
    "drug_amp_monthly", "drug_amp_quarterly",
    "nam_cahps", "hcgov_transitions",
    "covid_testing", "pharmacy_releases",
    "benefit_package_yearly", "first_time_nadac",
    "dual_status_yearly", "drug_mfr_contacts",
    "chip_unwinding_separate", "clotting_factor",
    "prematurity_smm", "exclusive_pediatric",
    "medicaid_enterprise", "drug_rebate_state_contacts",
    "express_lane_eligibility",
    # Session 17 - Medicare PUFs
    "part_d_prescriber_provider", "medicare_outpatient_by_provider",
    # Session 18 - Economic v2
    "bea_personal_income", "bea_income_components",
    "bea_transfer_receipts", "safmr_zip",
    # Care Compare quality (provider-level)
    "hcahps_hospital", "hh_quality_provider", "asc_quality_facility",
    "timely_effective_hospital", "oas_cahps_hospital",
    # State fiscal data (11 tables)
    "census_state_finances", "tax_burden",
    "state_tax_collections", "federal_aid_share",
    "state_debt", "pension_funded_ratio",
    "state_tax_rates", "tax_revenue_sources",
    "income_per_capita", "property_tax_rate",
    "fmap_historical",
    # Maternal & child health (10 tables)
    "cdc_natality", "infant_mortality_state", "infant_mortality_quarterly",
    "child_vaccination", "adolescent_vaccination", "teen_birth_rate",
    "wic_nutrition", "wic_participation", "foster_care", "title_v_mch",
    # CMS Program Statistics — utilization & enrollment detail (8 tables)
    "cps_inpatient_utilization", "cps_snf_utilization",
    "cps_hha_utilization", "cps_hospice_utilization",
    "cps_dual_enrollment_detail", "cps_ma_enrollment",
    "cps_part_d_enrollment", "ma_enrollment_plan",
    # Session 19 - SAMHSA v2 (TEDS detail + NSDUH totals + CDC overdose refresh)
    "teds_admissions_detail", "teds_discharges",
    "nsduh_sae_totals_2024",
    # Provider & network data (3 fact tables; provider_affiliation deprecated, use provider_reassignment)
    "pecos_enrollment",
    "critical_access_hospitals", "gme_teaching_hospitals",
    # Insurance market & coverage data (5 tables)
    "mlr_market", "risk_adjustment", "ma_star_ratings",
    "census_health_insurance", "meps_employer_insurance",
    # NPPES full provider registry
    "nppes_provider", "nppes_taxonomy_detail",
    # HHS DOGE Medicaid Provider Spending (190M rows aggregated, 5 tables)
    "doge_state_hcpcs", "doge_state_taxonomy", "doge_state_monthly",
    "doge_state_category", "doge_top_providers",
    # Session 19 - Gap-closing data run
    "provider_reassignment",
    "hrsa_awarded_grants", "hrsa_active_grants",
    "sdud_2020", "sdud_2021", "sdud_2022", "sdud_2023", "sdud_combined",
    "cms64_multiyear", "cms64_historical",
    "macpac_benefit_spending_fy2024", "macpac_spending_by_elig_fy2023",
    "macpac_mc_enrollment_detail",
    # Session 20 - Additional data ingestion
    "promoting_interoperability",
    "quality_measures_2024_detail",
    "bls_oews_msa",
    # MACPAC exhibits + MFCU FY2024
    "macpac_enrollment_v2", "macpac_hcbs_payment_scan", "macpac_fmap_multiyear",
    "mfcu_statistical_chart", "mfcu_open_cases", "mfcu_case_outcomes",
    # Session 21 - CMS March 2026 batch (31 tables)
    "enrollment_feb2026", "mc_enrollment_by_plan_2024",
    "cms64_financial_management", "cms64_financial_management_national",
    "cms64_new_adult_expenditures", "cms64_caa_fmap_expenditures",
    "eligibility_processing_feb2026",
    "major_eligibility_group_annual", "dual_status_annual", "program_info_annual",
    "renewal_outcomes", "chip_enrollment_monthly",
    "continuous_eligibility_v2", "express_lane_eligibility_v2",
    "benefit_package_annual", "medicaid_chip_eligibility_levels",
    "mc_programs_by_state_2023", "mc_features_enrollment_2024",
    "mc_share_enrollees_2024", "mc_enrollment_pop_2024",
    "mltss_enrollment_2024", "mc_enrollment_summary_2024_v2",
    "mlr_summary_dec2025",
    "nadac_mar2026", "nadac_comparison_mar2026",
    "drug_amp_q4_2025", "mdrp_drug_products_q4_2025",
    "dsh_reporting_latest",
    "hcgov_transitions_unwinding", "medicaid_enterprise_system",
    "1915c_waiver_participants_v2",
    # DMEPOS detail + ambulance geographic (7 tables)
    "dmepos_detail", "dmepos_pen", "dmepos_cba",
    "dmepos_cba_mailorder", "dmepos_cba_zipcodes",
    "dmepos_rural_zipcodes", "ambulance_geographic",
    # Session 21: CMS Provider Data API + agent ingestions
    "340b_covered_entities", "aca_effectuated_enrollment",
    "eligibility_enrollment_snapshot", "mhbg_fy23_allotments",
    # Session 21: Previously blocked + new downloads
    "svi_county", "mips_performance",
    "kff_1115_approved_waivers", "kff_1115_pending_waivers", "kff_1115_work_requirements",
    "state_mac_ny", "state_mac_tx",
    "nsduh_2022_state",
    # Section 1115 Medicaid waivers (665 waivers, 54 states)
    "section_1115_waivers", "kff_1115_waivers",
    # Session 22: SDUD historical backfill (1991-2019, 40M+ rows)
    "sdud_1991", "sdud_1992", "sdud_1993", "sdud_1994", "sdud_1995",
    "sdud_1996", "sdud_1997", "sdud_1998", "sdud_1999", "sdud_2000",
    "sdud_2001", "sdud_2002", "sdud_2003", "sdud_2004", "sdud_2005",
    "sdud_2006", "sdud_2007", "sdud_2008", "sdud_2009", "sdud_2010",
    "sdud_2011", "sdud_2012", "sdud_2013", "sdud_2014", "sdud_2015",
    "sdud_2016", "sdud_2017", "sdud_2018", "sdud_2019",
    # Session 22: CMS-64 historical backfill (FY1997-2017)
    # cms64_historical — already listed in Session 19 gap-closing block above
    # Session 22: SDUD historical combined (1991-2019, 41.8M rows)
    "sdud_historical_combined",
    # Session 22: Gap analysis — Census/CDC/SAMHSA new tables
    "acs_disability", "acs_language",
    "sahie_state", "sahie_county", "sahie_county_138fpl",
    "places_county_2025", "provisional_overdose",
    "mc_enrollment_by_plan", "nsumhss_facility",
    "teds_admissions_2023",
    "cdc_chronic_disease",
    "cdc_behavioral_risk", "cdc_underlying_cod",
    # Session 30: Manual data ingestion + T-MSIS analysis
    "adi_block_group", "ahrq_sdoh_county",
    "fee_schedule_ks", "fee_schedule_wi", "fee_schedule_nj",
    "fee_schedule_tx", "fee_schedule_ny", "fee_schedule_va",
    "fmap_kff_historical",
    "mcpar", "meps_hc_2022", "meps_hc_full_2022",
    "tmsis_calibration", "tn_simulated_fee_schedule", "tmsis_effective_rates",
    "rate_comparison_v2",
]


def _register_view(view_name: str, parquet_path: Path) -> None:
    """Register a single Parquet file as a DuckDB view (thread-safe, idempotent)."""
    if view_name in _registered:
        return
    with _lock:
        if view_name in _registered:
            return
        _conn.execute(f"CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM '{parquet_path}'")
        _registered.add(view_name)


def _register_all_views() -> None:
    """Eagerly register every lake Parquet file as a DuckDB view.

    Called in a background thread so the server can start accepting
    health-check requests immediately.
    """
    global _lake_ready

    lake = Path(settings.lake_dir)
    dim_dir = lake / "dimension"
    fact_dir = lake / "fact"

    # Load extensions for RAG (vector search + full-text search)
    try:
        _conn.execute("INSTALL vss; LOAD vss;")
    except Exception:
        pass
    try:
        _conn.execute("INSTALL fts; LOAD fts;")
    except Exception:
        pass

    # Register dimension tables
    if dim_dir.exists():
        for parquet_file in dim_dir.glob("*.parquet"):
            view_name = parquet_file.stem
            _register_view(view_name, parquet_file)

    # Register fact tables from FACT_NAMES (latest snapshot)
    for fact_name in FACT_NAMES:
        p = _latest_snapshot(fact_dir, fact_name)
        if p:
            view_name = f"fact_{fact_name}"
            _register_view(view_name, p)

    # Auto-discover any fact tables on disk not in FACT_NAMES
    if fact_dir.exists():
        fact_name_set = set(FACT_NAMES)
        for subdir in fact_dir.iterdir():
            if subdir.is_dir() and subdir.name not in fact_name_set:
                p = _latest_snapshot(fact_dir, subdir.name)
                if p:
                    _register_view(f"fact_{subdir.name}", p)

    # Register reference tables
    ref_dir = lake / "reference"
    if ref_dir.exists():
        for parquet_file in ref_dir.glob("*.parquet"):
            view_name = parquet_file.stem
            _register_view(view_name, parquet_file)
        for ref_subdir in ref_dir.iterdir():
            if ref_subdir.is_dir():
                p = _latest_snapshot(ref_dir, ref_subdir.name)
                if p:
                    view_name = f"ref_{ref_subdir.name}"
                    _register_view(view_name, p)

    # SDUD standardized schema (all tables now use sdud_2025 column names):
    #   state_code (not 'state'), number_of_prescriptions (not 'num_prescriptions'),
    #   total_amount_reimbursed (not 'total_reimbursed'),
    #   medicaid_amount_reimbursed (not 'medicaid_reimbursed')
    # When ETL is re-run, a UNION ALL view can be created:
    #   CREATE VIEW fact_sdud_all AS
    #     SELECT state_code, year, quarter, ndc, product_name, utilization_type,
    #            units_reimbursed, number_of_prescriptions, total_amount_reimbursed,
    #            medicaid_amount_reimbursed, source, snapshot_date
    #     FROM fact_sdud_2025
    #     UNION ALL SELECT ... FROM fact_sdud_2024
    #     UNION ALL SELECT ... FROM fact_sdud_combined  -- 2020-2023
    #     UNION ALL SELECT ... FROM fact_sdud_historical_combined  -- 1991-2019

    # Create backward-compatibility views
    claims_path = _latest_snapshot(fact_dir, "claims")
    provider_path = _latest_snapshot(fact_dir, "provider")
    if claims_path and "spending" not in _registered:
        with _lock:
            if "spending" not in _registered:
                _conn.execute(f"""
                    CREATE VIEW IF NOT EXISTS spending AS
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
                _registered.add("spending")

    if provider_path and "spending_providers" not in _registered:
        with _lock:
            if "spending_providers" not in _registered:
                _conn.execute(f"""
                    CREATE VIEW IF NOT EXISTS spending_providers AS
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
                _registered.add("spending_providers")

    _lake_ready = True
    print(f"Lake ready: {len(_registered)} views registered.", flush=True)


def is_lake_ready() -> bool:
    """Return True once all views have been registered."""
    return _lake_ready


def _delayed_rescan() -> None:
    """Periodically rescan the lake until enough views are registered.

    On cold start with no pre-baked data, the entrypoint downloads
    Parquet from R2 in the background (takes 2-5 minutes). This thread
    polls every 30 seconds and re-registers views each time new files
    appear, until at least 500 views are found or 10 minutes pass.
    """
    import time
    global _registered
    target = 650  # Expect ~700 views when fully loaded
    max_attempts = 30  # 30 x 30s = 15 minutes max
    for attempt in range(1, max_attempts + 1):
        time.sleep(30)
        current = len(_registered)
        if current >= target:
            print(f"Auto-rescan: {current} views registered (>={target}). Done.", flush=True)
            return
        # Clear registered set and re-scan to pick up newly downloaded files
        with _lock:
            _registered = set()
        _register_all_views()
        new_count = len(_registered)
        if new_count > current:
            print(f"Auto-rescan [{attempt}]: {current} -> {new_count} views.", flush=True)
        if new_count >= target:
            print(f"Auto-rescan: {new_count} views registered. Done.", flush=True)
            return
    print(f"Auto-rescan: stopped after {max_attempts} attempts ({len(_registered)} views).", flush=True)


def init_db() -> None:
    """Create the in-memory DuckDB connection, then register views in background.

    The connection is available immediately for health checks.
    View registration runs in a background thread so the server can
    start accepting requests right away.
    """
    global _conn
    _conn = duckdb.connect()

    # Register views in a background thread to avoid blocking startup
    t = threading.Thread(target=_register_all_views, daemon=True, name="lake-init")
    t.start()

    # Schedule a delayed rescan in case the lake wasn't ready on first pass
    t2 = threading.Thread(target=_delayed_rescan, daemon=True, name="lake-rescan")
    t2.start()


def reload_lake() -> None:
    """Re-scan the lake directory and register any new views.

    Called after the background R2 sync completes so that views become
    available without restarting the server. Clears the registered set
    so all views are re-created from the now-populated lake directory.
    """
    global _lake_ready, _registered
    _lake_ready = False
    with _lock:
        _registered = set()  # Clear so views can be re-registered
    t = threading.Thread(target=_register_all_views, daemon=True, name="lake-reload")
    t.start()


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
