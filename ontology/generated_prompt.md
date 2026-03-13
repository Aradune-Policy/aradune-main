## The Aradune Data Lake

### Behavioral Health & Substance Use
NSDUH prevalence and state estimates, TEDS admissions and discharges, MH/SUD facilities, opioid prescribing, OTP providers, NSUMHSS survey, and IPF quality

- **fact_bh_by_condition**: 4,240 rows
- **fact_bh_services**: 31,800 rows
- **fact_cdc_overdose_deaths**: 81,270 rows
- **fact_drug_overdose**: 2,703 rows
- **fact_ipf_facility**: 1,422 rows
- **fact_ipf_quality_facility**: 1,422 rows
- **fact_medicaid_opioid_prescribing**: 539,181 rows
- **fact_mh_facility**: 27,957 rows
- **fact_nsduh_prevalence**: 5,865 rows
- **fact_nsduh_prevalence_2024**: 4,212 rows
- **fact_opioid_prescribing**: 539,181 rows
- **fact_otp_provider**: 1,533 rows
- **fact_physical_among_mh**: 5,565 rows
- **fact_physical_among_sud**: 5,565 rows
- **fact_teds_admissions_detail**: 83,795 rows
- **fact_teds_discharges**: 91,253 rows
- **fact_nsduh_sae_totals_2024**: 8,120 rows
- **fact_nsduh_state_2023**: 29,736 rows
- **fact_nsduh_state_2024**: 28,112 rows
- **fact_nsumhss_2024**: 27,957 rows
- **fact_nsumhss_facility**: 27,957 rows
- **fact_teds_admissions_2023**: 1,625,833 rows
- **fact_nsduh_2022_state**: 1,600 rows
- **fact_mh_facility_survey**: 27,957 rows
- **fact_mh_sud_services**: 216 rows
- **fact_opioid_prescribing_geo**: 539,181 rows
- **fact_opioid_prescribing_rates**: 539,181 rows
- **fact_medicaid_opioid_geo**: 539,181 rows
- **fact_otp_providers_v2**: 1,533 rows
- **fact_bh_services_detail**: 31,800 rows
- **fact_ipf_quality_state**: 52 rows
- **fact_mhbg_fy23_allotments**: 59 rows
- Supporting: fact_block_grant, fact_mh_sud_recipients, fact_teds_admissions

### Economic & Social Context
BLS CPI/unemployment, Census ACS and health insurance coverage, BEA GDP/income/transfers, SAIPE poverty, HUD FMR/SAFMR, food environment, county health rankings

- **fact_ahrf_county**: 3,235 rows
- **fact_fair_market_rent**: 4,764 rows
- **fact_food_environment**: 957,753 rows
- **fact_saipe_poverty**: 3,196 rows
- **fact_unemployment**: 3,621 rows
- **fact_bea_state_gdp**: 13,440 rows
- **fact_bea_gdp_growth**: 149,040 rows
- **fact_bea_personal_income**: 1,275 rows
- **fact_bea_income_components**: 1,275 rows
- **fact_bea_transfer_receipts**: 1,275 rows
- **fact_safmr_zip**: 51,830 rows
- **fact_county_health_rankings**: 262,687 rows
- **fact_census_health_insurance**: 9,152 rows
- Supporting: fact_acs_state, fact_cpi, fact_median_income, fact_state_gdp, fact_state_population, food_environment_variables, fact_food_environment_full

### Enrollment & Managed Care
Monthly Medicaid enrollment, eligibility groups, managed care programs, CHIP, unwinding, applications, PACE, dual status, marketplace transitions, KFF coverage data

- **dim_pace_organization**: 201 rows
- **dim_state**: 51 rows
- **fact_benefit_package**: 1,484 rows
- **fact_chip_app_elig**: 5,567 rows
- **fact_chip_enrollment**: 10,404 rows
- **fact_chip_monthly**: 1,080 rows
- **fact_chip_program_annual**: 1,113 rows
- **fact_chip_program_monthly**: 13,356 rows
- **fact_cms64_new_adult**: 2,622 rows
- **fact_dental_services**: 3,180 rows
- **fact_dual_status_monthly**: 13,356 rows
- **fact_elig_group_annual**: 2,597 rows
- **fact_elig_group_monthly**: 31,164 rows
- **fact_eligibility**: 10,404 rows
- **fact_eligibility_processing**: 3,162 rows
- **fact_enrollment**: 10,399 rows
- **fact_managed_care**: 7,107 rows
- **fact_marketplace_unwinding**: 59,527 rows
- **fact_mc_annual**: 2,597 rows
- **fact_mc_dashboard**: 19,559 rows
- **fact_mc_enrollment**: 7,804 rows
- **fact_mc_enrollment_plan**: 7,804 rows
- **fact_mc_info_monthly**: 31,164 rows
- **fact_mc_monthly**: 31,164 rows
- **fact_medicaid_applications**: 10,404 rows
- **fact_medicare_enrollment**: 557,084 rows
- **fact_medicare_provider_enrollment**: 2,957,262 rows
- **fact_new_adult**: 7,058 rows
- **fact_new_adult_enrollment**: 7,854 rows
- **fact_program_annual**: 1,113 rows
- **fact_program_monthly**: 13,356 rows
- **fact_renewal_processing**: 3,162 rows
- **fact_snap_enrollment**: 3,920 rows
- **fact_tanf_enrollment**: 9,072 rows
- **fact_telehealth_services**: 12,720 rows
- **fact_unwinding**: 57,759 rows
- **fact_mc_enrollment_annual**: 2,597 rows
- **fact_mc_enrollment_monthly**: 31,164 rows
- **fact_mc_programs_by_state**: 362 rows
- **fact_mc_annual_v2**: 2,597 rows
- **fact_mc_monthly_v2**: 31,164 rows
- **fact_mc_info_monthly2**: 31,164 rows
- **fact_mc_info_annual**: 2,597 rows
- **fact_mc_features_population**: 93 rows
- **fact_mc_features_qa**: 81 rows
- **fact_mc_qa_features**: 81 rows
- **fact_mc_dashboard_v2**: 2,040 rows
- **fact_program_info_monthly**: 13,356 rows
- **fact_chip_program_type_annual**: 1,113 rows
- **fact_chip_program_type_monthly**: 13,356 rows
- **fact_chip_unwinding_separate**: 780 rows
- **fact_dual_status_yearly**: 1,113 rows
- **fact_benefit_package_yearly**: 1,484 rows
- **fact_medicaid_applications_v2**: 10,404 rows
- **fact_marketplace_oep_2025**: 54 rows
- **fact_pace_directory**: 921 rows
- **fact_mltss_enrollees**: 504 rows
- **fact_express_lane_eligibility**: 15 rows
- **fact_hcgov_transitions**: 59,527 rows
- **fact_pi_performance**: 10,404 rows
- **fact_eligibility_enrollment_snapshot**: 5,000 rows
- Supporting: fact_chip_eligibility, fact_chip_enrollment_unwinding, fact_continuous_eligibility, fact_eligibility_levels, fact_hcbs_waiver_enrollment, fact_macpac_enrollment, fact_marketplace_oep, fact_mc_enroll_duals, fact_mc_enroll_pop, fact_mc_enrollment_pop2 (+27 more)

**Named metrics (use these for consistency):**
- `enrollment_change_pct`: Month-over-month or year-over-year enrollment change = `(current_enrollment - prior_enrollment) / prior_enrollment`
- `managed_care_penetration`: Percent of Medicaid enrollees in managed care = `mc_enrollment / total_enrollment`
- `unwinding_disenrollment_rate`: Percent of renewals resulting in disenrollment during PHE unwinding = `disenrollments / total_renewals`

### Expenditure & Fiscal
CMS-64 expenditure (FY2016 + multi-year FY2018-2024 by service category, 117K rows), FMAP (current and historical), supplemental payments, DSH, NHE national health expenditure, MCO medical loss ratios, MACPAC spending, KFF spending data

- **fact_cms64_ffcra**: 1,197 rows
- **fact_expenditure**: 5,379 rows
- **fact_ffcra_fmap**: 1,197 rows
- **fact_financial_mgmt**: 15,511 rows
- **fact_cms64_multiyear**: 117,936 rows
- **fact_cms64_historical**: 175,688 rows
- **fact_fmr_fy2024**: 19,095 rows
- **fact_fmr_supplemental**: 1,553 rows
- **fact_nhe_state**: 117,000 rows
- **fact_dsh_annual**: 49 rows
- **fact_dsh_annual_v2**: 49 rows
- **fact_macpac_benefit_spending_v2**: 56 rows
- **fact_fmap_historical**: 612 rows
- **fact_mco_mlr**: 2,282 rows
- **fact_mco_mlr_v2**: 2,282 rows
- **fact_nhe**: 3,393 rows
- **fact_nhe_projections**: 1,334 rows
- **fact_macpac_benefit_spending_fy2024**: 56 rows
- **fact_macpac_spending_by_elig_fy2023**: 51 rows
- **fact_macpac_mc_enrollment_detail**: 52 rows
- Supporting: fact_caa_fmap, fact_financial_mgmt_national, fact_fmap, fact_macpac_benefit_spending, fact_macpac_spending_by_state, fact_macpac_spending_per_enrollee, fact_macpac_supplemental, fact_sdp_preprint, fact_kff_total_spending, fact_kff_spending_per_enrollee (+12 more)

**Named metrics (use these for consistency):**
- `per_enrollee_spending`: Total Medicaid spending divided by average monthly enrollment = `total_expenditure / avg_monthly_enrollment`
- `federal_share_pct`: Federal share of total Medicaid expenditure = `federal_expenditure / total_expenditure`
- `fmap_rate`: Federal Medical Assistance Percentage for a state = `fmap (direct lookup)`
- `cms64_spending_growth`: Change in total computable expenditure by state between fiscal years = `(SUM(total_computable WHERE fiscal_year={year}) - SUM(total_computable WHERE fiscal_year={year-1})) / SUM(total_computable WHERE fiscal_year={year-1})`

### Hospitals & Acute Care
Hospital cost reports (HCRIS), quality ratings, DSH/VBP/HRRP, AHEAD readiness, HCAHPS patient experience, price transparency, critical access, teaching hospitals, ASC quality

- **fact_cms_impact**: 3,152 rows
- **fact_complications**: 1,120 rows
- **fact_complications_hosp**: 95,780 rows
- **fact_dsh_hospital**: 6,103 rows
- **fact_hac_measure**: 12,120 rows
- **fact_hai_hospital**: 172,404 rows
- **fact_hai_hospital2**: 172,404 rows
- **fact_hai_state**: 1,008 rows
- **fact_hhcahps_provider**: 12,251 rows
- **fact_hospital_cost**: 18,220 rows
- **fact_hospital_directory**: 5,426 rows
- **fact_hospital_hrrp**: 18,330 rows
- **fact_hospital_rating**: 5,426 rows
- **fact_hospital_service_area**: 1,156,701 rows
- **fact_hospital_timely_effective**: 138,129 rows
- **fact_hospital_vbp**: 2,455 rows
- **fact_imaging_hospital**: 18,500 rows
- **fact_mspb_hospital**: 4,625 rows
- **fact_mspb_hospital_detail**: 4,625 rows
- **fact_pos_hospital**: 13,510 rows
- **fact_psi90_hospital**: 52,327 rows
- **fact_snf_vbp**: 13,900 rows
- **fact_timely_effective**: 1,736 rows
- **fact_timely_effective_hosp**: 138,129 rows
- **fact_unplanned_visits_hosp**: 67,046 rows
- **fact_hcahps_hospital**: 325,652 rows
- **fact_timely_effective_hospital**: 138,129 rows
- **fact_oas_cahps_hospital**: 92,500 rows
- **fact_hcris_hospital**: 18,220 rows
- **fact_hospital_general_info**: 5,426 rows
- **fact_hac_measure_2025**: 12,120 rows
- **fact_hac_measure_provider_2025**: 12,120 rows
- **fact_asc_quality_facility**: 5,711 rows
- **fact_hh_quality_provider**: 12,251 rows
- **fact_hospital_price_transparency**: 10,680 rows
- **fact_hospital_price_transparency_v2**: 10,680 rows
- **fact_gme_teaching_hospitals**: 61,818 rows
- **fact_critical_access_hospitals**: 1,376 rows
- **fact_ahrq_psi11**: 3,319 rows
- Supporting: fact_dsh_payment, fact_hcahps_state, fact_hhcahps_state, fact_mspb_state, fact_unplanned_visits, fact_hospital_chow, fact_hospital_chow_owners, fact_hospital_all_owners, fact_hospital_enrollments

### Insurance Market & Coverage
Medical loss ratios by market type, risk adjustment transfers, MA Star ratings, Census health insurance coverage, and employer-sponsored insurance

- **fact_mlr_market**: 12,285 rows
- **fact_risk_adjustment**: 730 rows
- **fact_meps_employer_insurance**: 415 rows
- **fact_aca_effectuated_enrollment**: 254 rows

### LTSS & HCBS
HCBS waitlists, waiver enrollment, LTSS expenditure/rebalancing, 1915(c) waivers, MLTSS

- **fact_ltss_rebalancing**: 1,300 rows
- Supporting: fact_1915c_participants, fact_cms372_waiver, fact_hcbs_authority, fact_hcbs_waitlist, fact_ltss_expenditure, fact_ltss_users, fact_mltss, hcbs_payment_method, ref_1115_waivers

### Medicare & ACOs
Medicare enrollment, geographic variation, ACOs (MSSP/REACH), inpatient/outpatient/DME by provider and geo, CMS Program Statistics, chronic conditions, MCBS, market saturation, innovation models, Part D

- **fact_aco_beneficiaries_county**: 135,203 rows
- **fact_esrd_qip**: 7,557 rows
- **fact_esrd_qip_tps**: 7,558 rows
- **fact_market_saturation_county**: 962,222 rows
- **fact_medicare_geo_variation**: 33,639 rows
- **fact_medicare_physician_geo**: 268,634 rows
- **fact_medicare_program_stats**: 1,644 rows
- **fact_medicare_spending_claim**: 63,646 rows
- **fact_medicare_telehealth**: 32,508 rows
- **fact_mssp_participants**: 15,370 rows
- **fact_medicare_physician_provider**: 1,259,343 rows
- **fact_medicare_provider**: 1,259,343 rows
- **fact_medicare_procedure_summary**: 14,377,293 rows
- **fact_medicare_procedure_summary_2024**: 14,369,525 rows
- **fact_medicare_inpatient_geo**: 26,479 rows
- **fact_medicare_inpatient_provider**: 3,093 rows
- **fact_medicare_inpatient_drg**: 146,427 rows
- **fact_medicare_outpatient_geo**: 19,401 rows
- **fact_medicare_outpatient_provider**: 8,497 rows
- **fact_medicare_outpatient_by_provider**: 116,799 rows
- **fact_medicare_dme_geo**: 38,675 rows
- **fact_medicare_monthly_enrollment**: 557,084 rows
- **fact_medicare_summary_ab**: 2,322 rows
- **fact_medicare_telehealth_v2**: 32,508 rows
- **fact_medicare_covid_hosp**: 62,702 rows
- **fact_medicare_cert**: 163,940 rows
- **fact_ma_geo_variation_v2**: 378 rows
- **fact_ma_star_ratings**: 1,558 rows
- **fact_aco_reach_2026**: 74 rows
- **fact_aco_reach_providers**: 191,493 rows
- **fact_aco_advance_investment**: 202 rows
- **fact_aco_snf_affiliates**: 3,196 rows
- **fact_aco_beneficiaries_county_v2**: 135,203 rows
- **fact_market_saturation_cbsa**: 207,651 rows
- **fact_cps_dual_enrollment**: 58 rows
- **fact_cps_dual_enrollment_detail**: 156 rows
- **fact_cps_part_ab_summary**: 56 rows
- **fact_cps_premiums**: 156 rows
- **fact_cps_providers**: 636 rows
- **fact_cps_inpatient_utilization**: 56 rows
- **fact_cps_snf_utilization**: 56 rows
- **fact_cps_hha_utilization**: 56 rows
- **fact_cps_hospice_utilization**: 56 rows
- **fact_cps_ma_enrollment**: 112 rows
- **fact_cps_part_d_enrollment**: 56 rows
- **fact_cps_part_d_utilization**: 56 rows
- **fact_chronic_conditions_national**: 186 rows
- **fact_chronic_conditions_all_medicare**: 140 rows
- **fact_mcbs_cost_summary**: 143 rows
- **fact_mcbs_income_assets**: 74 rows
- **fact_county_expenditure_risk**: 3,226 rows
- **fact_innovation_model_summary**: 99 rows
- **fact_innovation_participants**: 3,492 rows
- **fact_esrd_etc_clinician**: 907 rows
- **fact_esrd_etc_facility**: 433 rows
- Supporting: fact_aco_reach_results, fact_esrd_etc_results, fact_ma_geo_variation, fact_mssp_aco, fact_mssp_financial_results, fact_ipps_impact, fact_viii_group_expenditure

### Nursing Facilities
Nursing facility Five-Star ratings, PBJ staffing (daily and summary), MDS quality and assessment data, deficiency citations, SNF cost, chain performance, LTC facility characteristics

- **fact_five_star**: 14,710 rows
- **fact_mds_quality**: 250,070 rows
- **fact_nh_deficiencies**: 419,452 rows
- **fact_nh_deficiency**: 419,452 rows
- **fact_nh_mds_quality**: 250,070 rows
- **fact_nh_ownership**: 144,000 rows
- **fact_nh_penalties**: 17,463 rows
- **fact_nh_penalties_v2**: 17,463 rows
- **fact_nh_provider_info**: 14,710 rows
- **fact_nh_survey_summary**: 43,983 rows
- **fact_pbj_employee**: 65,006,704 rows
- **fact_pbj_nonnurse_staffing**: 1,332,804 rows
- **fact_pbj_nurse_staffing**: 1,332,436 rows
- **fact_snf_cost**: 44,979 rows
- **fact_snf_quality_provider**: 838,470 rows
- **fact_mds_facility_level**: 29,224,873 rows
- **fact_mds_frequency**: 133,920 rows
- **fact_mds_quality_full**: 250,070 rows
- **fact_nh_chain_performance**: 619 rows
- **fact_nh_chain_performance_v2**: 619 rows
- **fact_ltc_facility_characteristics**: 14,717 rows
- **fact_ltc_facility_characteristics_v2**: 14,717 rows
- **fact_hcris_snf**: 44,979 rows
- **fact_pbj_daily_nurse**: 1,332,436 rows
- **fact_pbj_daily_nonnurse**: 1,332,436 rows
- **fact_pbj_employee_summary**: 14,487 rows
- Supporting: fact_nh_state_averages, fact_snf_chow, fact_snf_chow_owners, fact_snf_owners, fact_snf_enrollments

### Pharmacy & Drug Spending
State Drug Utilization (SDUD 2020-2025, 28.3M rows, $1.05T pre-rebate), NADAC pricing, drug rebates, AMP reporting, Part B/D spending, prescriber data, Open Payments, opioid prescribing, clotting factors

- **fact_aca_ful**: 2,145,557 rows
- **fact_drug_rebate_products**: 1,907,607 rows
- **fact_drug_utilization**: 2,369,659 rows
- **fact_medicaid_drug_spending**: 16,938 rows
- **fact_nadac**: 1,882,296 rows
- **fact_part_d_geo**: 115,936 rows
- **fact_part_d_opioid_geo**: 328,890 rows
- **fact_part_d_quarterly_spending**: 28,255 rows
- **fact_part_d_spending_by_drug**: 14,309 rows
- **fact_sdud_2020**: 4,920,264 rows
- **fact_sdud_2021**: 5,053,442 rows
- **fact_sdud_2022**: 5,184,004 rows
- **fact_sdud_2023**: 5,311,612 rows
- **fact_sdud_2024**: 5,201,667 rows
- **fact_sdud_2025**: 2,637,009 rows
- **fact_sdud_combined**: 28,307,998 rows
- **fact_sdud_1991**: 1,778,209 rows
- **fact_sdud_1992**: 84,029 rows
- **fact_sdud_1993**: 2,779,620 rows
- **fact_sdud_1994**: 194,447 rows
- **fact_sdud_1995**: 1,602,114 rows
- **fact_sdud_1996**: 1,739,622 rows
- **fact_sdud_1997**: 1,756,324 rows
- **fact_sdud_1998**: 1,370,611 rows
- **fact_sdud_1999**: 1,792,560 rows
- **fact_sdud_2000**: 1,207,428 rows
- **fact_sdud_2001**: 1,427,501 rows
- **fact_sdud_2002**: 1,448,825 rows
- **fact_sdud_2003**: 1,714,785 rows
- **fact_sdud_2004**: 1,170,182 rows
- **fact_sdud_2005**: 1,773,955 rows
- **fact_sdud_2006**: 1,568,821 rows
- **fact_sdud_2007**: 1,278,375 rows
- **fact_sdud_2008**: 321,366 rows
- **fact_sdud_2009**: 1,325,855 rows
- **fact_sdud_2010**: 943,785 rows
- **fact_sdud_2011**: 1,158,886 rows
- **fact_sdud_2012**: 1,175,670 rows
- **fact_sdud_2013**: 1,350,237 rows
- **fact_sdud_2014**: 948,288 rows
- **fact_sdud_2015**: 2,092,261 rows
- **fact_sdud_2016**: 1,135,712 rows
- **fact_sdud_2017**: 1,068,356 rows
- **fact_sdud_2018**: 664,047 rows
- **fact_sdud_2019**: 4,961,100 rows
- **fact_sdud_historical_combined**: 41,832,971 rows
- **ref_drug_rebate**: 1,907,607 rows
- **fact_drug_amp_monthly**: 3,400,944 rows
- **fact_drug_amp_quarterly**: 2,126,520 rows
- **fact_part_d_prescriber_provider**: 1,380,665 rows
- **fact_part_d_prescriber_geo**: 115,936 rows
- **fact_part_d_prescriber_geo_v2**: 115,936 rows
- **fact_part_d_opioid_geo_v2**: 328,890 rows
- **fact_part_d_quarterly_spending_v2**: 28,255 rows
- **fact_part_d_spending_by_drug_v2**: 14,309 rows
- **fact_part_b_spending_by_drug**: 734 rows
- **fact_part_b_quarterly_spending**: 1,730 rows
- **fact_part_b_discarded_drugs**: 824 rows
- **fact_part_b_discarded_drugs_v2**: 824 rows
- **fact_medicaid_drug_spending_v2**: 16,938 rows
- **fact_open_payments**: 36,152 rows
- **fact_open_payments_v2**: 12,224,199 rows
- **fact_first_time_nadac**: 1,269 rows
- **fact_340b_covered_entities**: 26,290 rows
- Supporting: fact_clotting_factor, fact_exclusive_pediatric, fact_pharmacy_releases, fact_drug_mfr_contacts, fact_drug_rebate_state_contacts

**Named metrics (use these for consistency):**
- `total_drug_spending_by_state`: Sum of total amount reimbursed for all NDCs in a state-year-quarter = `SUM(total_reimbursed) WHERE year = {year} AND state = {state}`
- `prescriptions_per_enrollee`: Total prescriptions divided by Medicaid enrollment for a state-year = `SUM(num_prescriptions) / AVG(total_enrollment)`
- `spending_per_prescription`: Total reimbursed divided by number of prescriptions = `SUM(total_reimbursed) / SUM(num_prescriptions)`

### Policy & Regulatory
CMS policy documents (CIBs, SHO letters, SPAs, waivers), Federal Register rulemaking, and Medicaid enterprise system standards

- **fact_policy_chunk**: 6,058 rows
- **fact_policy_document**: 1,039 rows
- **fact_federal_register**: 762 rows
- **fact_federal_register_cms**: 5,982 rows
- **fact_medicaid_enterprise**: 68 rows
- Supporting: fact_spa

**Planned (not yet in lake):**
- fact_section_1115_waivers: Section 1115 waiver tracker by state — approved, pending, expired, with key provisions and effective dates. Source KFF or Medicaid.gov. ~200 rows.

### Program Integrity
OIG LEIE exclusion lists, PERM error rates, MFCU statistics, revoked and opt-out providers

- **fact_leie**: 82,749 rows
- **fact_leie_v2**: 82,749 rows
- **fact_perm_rates**: 12 rows
- **fact_mfcu_stats**: 53 rows

### Providers & Facilities
NPPES full provider registry, PECOS enrollment, physician compare, revalidation, provider affiliations, CLIA, FQHCs, dialysis, hospice, HHA, IRF, LTCH, ASC, post-acute care, QPP, and facility ownership data

- **dim_provider_taxonomy**: 10 rows
- **fact_asc_facility**: 5,711 rows
- **fact_dialysis_facility**: 7,557 rows
- **fact_dialysis_facility_v2**: 7,557 rows
- **fact_fqhc_directory**: 18,808 rows
- **fact_fqhc_hypertension**: 6,866 rows
- **fact_fqhc_quality_badges**: 7,438 rows
- **fact_health_center_sites**: 8,121 rows
- **fact_hha_cost_report**: 10,715 rows
- **fact_home_health_agency**: 12,251 rows
- **fact_hospice_cahps_state**: 1,320 rows
- **fact_hospice_directory**: 6,943 rows
- **fact_hospice_provider**: 465,181 rows
- **fact_hospice_quality**: 465,181 rows
- **fact_hospice_state**: 1,100 rows
- **fact_irf_provider**: 79,365 rows
- **fact_ltch_general**: 319 rows
- **fact_ltch_provider**: 24,882 rows
- **fact_pac_hha_utilization**: 8,519 rows
- **fact_pac_irf_utilization**: 1,205 rows
- **fact_physician_compare**: 31,490 rows
- **fact_pos_other**: 122,013 rows
- **fact_provider**: 584,080 rows
- **fact_provider_specific**: 68,668 rows
- **fact_nppes_provider**: 9,368,082 rows
- **fact_nppes_taxonomy_detail**: 11,732,753 rows
- **fact_nppes_endpoint**: 593,297 rows
- **fact_nppes_practice_location**: 1,146,558 rows
- **fact_pecos_enrollment**: 2,957,252 rows
- **fact_provider_affiliation**: 3,493,819 rows
- **fact_revalidation_clinic_group**: 3,306,178 rows
- **fact_revalidation_due_date**: 2,890,219 rows
- **fact_physician_compare_v2**: 2,843,762 rows
- **fact_fiss_attending_rendering**: 2,039,109 rows
- **fact_order_referring**: 1,996,162 rows
- **fact_order_referring_v2**: 1,996,162 rows
- **fact_providers_missing_dci**: 3,822,698 rows
- **fact_clia**: 671,570 rows
- **fact_qpp_experience**: 524,998 rows
- **fact_provider_specific_v2**: 68,669 rows
- **fact_home_infusion_provider**: 1,918 rows
- **fact_mdpp_suppliers**: 941 rows
- **fact_asm_participants**: 6,637 rows
- **fact_pos_iqies**: 58,001 rows
- **fact_hospice_quality_national**: 331,500 rows
- **fact_pac_hospice_utilization**: 5,824 rows
- **fact_pac_hha_casemix**: 127,100 rows
- **fact_pac_irf_casemix**: 12,840 rows
- **fact_pac_snf_casemix**: 14,214 rows
- **fact_pac_snf_utilization**: 14,214 rows
- **fact_provider_reassignment**: 3,493,820 rows
- Supporting: fact_asc_quality_state, fact_dialysis_state, fact_home_health_state, fact_home_health_state2, fact_oas_cahps_state, fact_pac_ltch_utilization, fact_vha_provider, fact_optout_providers, fact_optout_providers_v2, fact_revoked_providers (+9 more)

### Public Health & Maternal
CDC PLACES, BRFSS, vital statistics, maternal health/mortality, natality, infant mortality, child/adolescent vaccinations, WIC, foster care, Title V MCH, prematurity/SMM, CAHPS surveys, COVID testing

- **fact_acute_care**: 25,440 rows
- **fact_blood_lead_screening**: 3,180 rows
- **fact_brfss**: 1,168,981 rows
- **fact_brfss_behavioral**: 86,141 rows
- **fact_cdc_leading_causes_death**: 10,868 rows
- **fact_contraceptive_care**: 6,360 rows
- **fact_health_screenings**: 3,180 rows
- **fact_maternal_health**: 17,968 rows
- **fact_mortality_trend**: 10,868 rows
- **fact_perinatal_care**: 12,720 rows
- **fact_places_county**: 229,232 rows
- **fact_places_county_2025**: 114,576 rows
- **fact_sahie_state**: 52 rows
- **fact_sahie_county**: 3,144 rows
- **fact_sahie_county_138fpl**: 3,144 rows
- **fact_acs_disability**: 52 rows
- **fact_acs_language**: 52 rows
- **fact_provisional_overdose**: 81,900 rows
- **fact_cdc_chronic_disease**: 309,215 rows
- **fact_svi_county**: 3,144 rows
- **fact_pregnancy_outcomes**: 6,360 rows
- **fact_pregnant_postpartum**: 1,060 rows
- **fact_respiratory_conditions**: 28,620 rows
- **fact_vaccinations**: 47,700 rows
- **fact_vital_stats**: 1,836 rows
- **fact_vital_stats_monthly**: 1,980 rows
- **fact_cdc_natality**: 1,980 rows
- **fact_cdc_vsrr_vital**: 1,980 rows
- **fact_cdc_maternal_mortality_prov**: 810 rows
- **fact_infant_mortality_state**: 1,167 rows
- **fact_infant_mortality_quarterly**: 80 rows
- **fact_child_vaccination**: 128,188 rows
- **fact_adolescent_vaccination**: 28,181 rows
- **fact_teen_birth_rate**: 56,466 rows
- **fact_wic_nutrition**: 12,473 rows
- **fact_wic_participation**: 275 rows
- **fact_foster_care**: 1,200 rows
- **fact_title_v_mch**: 3,360 rows
- **fact_prematurity_smm**: 435 rows
- **fact_smm_conditions**: 530 rows
- **fact_well_child_visits_v2**: 63 rows
- **fact_nam_cahps**: 272,679 rows
- **fact_covid_testing**: 3,180 rows
- Supporting: fact_maternal_morbidity, fact_maternal_mortality_monthly, fact_maternal_mortality_national, fact_smm_extended, fact_well_child_visits, fact_respiratory_conditions_v2

**Planned (not yet in lake):**
- fact_svi_county: CDC/ATSDR Social Vulnerability Index at county level — composite ranking on 16 social factors (poverty, housing, transportation, minority status) grouped into 4 themes. ~3,200 rows.
- fact_svi_tract: CDC SVI at census tract level — same indicators at finer geography. ~74,000 tracts.
- fact_adi_block_group: Area Deprivation Index at block group level — socioeconomic deprivation ranking (1-100 national, 1-10 state). ~220,000 rows.
- fact_brfss_state_indicators: BRFSS behavioral risk factors aggregated to state level — smoking, obesity, diabetes, mental health, exercise, healthcare access. Filter from 1GB raw JSON.

### Quality & Outcomes
CMS Core Set quality measures (2023-2024), Scorecard with detailed stratification, EPSDT, and performance indicators

- **dim_scorecard_measure**: 55 rows
- **fact_performance_indicator**: 10,404 rows
- **fact_quality_core_set_2023**: 5,555 rows
- **fact_quality_core_set_2024**: 10,972 rows
- **fact_quality_core_set_2017**: 2,058 rows
- **fact_quality_core_set_2018**: 2,826 rows
- **fact_quality_core_set_2019**: 3,096 rows
- **fact_quality_core_set_2020**: 3,450 rows
- **fact_quality_core_set_2021**: 3,830 rows
- **fact_quality_core_set_2022**: 4,206 rows
- **fact_quality_core_set_combined**: 35,993 rows
- **fact_quality_measure**: 5,236 rows
- **fact_scorecard**: 90,165 rows
- **fact_quality_measures_2024_full**: 11,100 rows
- **fact_scorecard_detail**: 90,165 rows
- **fact_epsdt_2024**: 22,513 rows
- Supporting: fact_epsdt

**Planned (not yet in lake):**
- fact_quality_child_adult_2024: Child and adult health care quality measures 2024 — may extend or replace quality_core_set_2024. 2024-child-and-adult-health-care-quality-measures.csv (9 MB) in raw.
- fact_mcpar: Managed Care Program Annual Reports — plan-level prior auth, grievances, appeals, quality, network adequacy. High value but PDF extraction needed.

**Named metrics (use these for consistency):**
- `core_set_measure_rate`: State performance rate on a CMS Core Set quality measure = `measure_value (direct)`
- `five_star_avg`: Average CMS overall Five-Star rating for nursing facilities = `AVG(overall_rating)`

### Rates & Fee Schedules
Medicaid provider payment rates, Medicare benchmarks, rate comparisons, PFS RVUs, fee schedules (ambulance, anesthesia, CLFS, DMEPOS, OPPS), and DOGE claims data

- **dim_hcpcs**: 8,623 rows
- **dim_medicare_locality**: 109 rows
- **dim_procedure**: 16,978 rows
- **fact_claims**: 712,793 rows
- **fact_claims_categories**: 8,120 rows
- **fact_claims_monthly**: 6,299,376 rows
- **fact_dq_atlas**: 101,565 rows
- **fact_dq_flag**: 269,475 rows
- **fact_medicaid_rate**: 597,483 rows
- **fact_medicare_rate**: 858,593 rows
- **fact_medicare_rate_state**: 417,481 rows
- **fact_nh_claims_quality**: 58,840 rows
- **fact_rate_comparison**: 302,332 rows
- **fact_pfs_rvu**: 19,277 rows
- **fact_pfs_rvu_2026**: 18,239 rows
- **fact_pfs_opps_cap**: 15,260 rows
- **fact_opps_cap_2026**: 15,260 rows
- **fact_ambulance**: 1,308 rows
- **fact_anesthesia_2026**: 109 rows
- **fact_clfs**: 2,162 rows
- **fact_dmepos**: 222,076 rows
- **fact_drug_spending_trend**: 71,333 rows
- **fact_dmepos_detail**: 372,590 rows
- **fact_dmepos_pen**: 5,300 rows
- **fact_dmepos_cba**: 79,927 rows
- **fact_dmepos_cba_mailorder**: 8 rows
- **fact_dmepos_cba_zipcodes**: 16,124 rows
- **fact_dmepos_rural_zipcodes**: 15,911 rows
- **fact_ambulance_geographic**: 109 rows
- Supporting: fact_integrated_care, fact_nas_rates, fact_doge_state_hcpcs, fact_doge_state_taxonomy, fact_doge_state_monthly, fact_doge_state_category, fact_doge_top_providers, fact_kff_fee_index

**Named metrics (use these for consistency):**
- `pct_of_medicare`: Medicaid FFS rate divided by Medicare non-facility rate = `medicaid_rate / medicare_nonfac_rate`
- `cpra_pct_of_medicare`: Official CPRA calculation per 42 CFR 447.203 = `SUM(medicaid_rate * claim_count) / SUM(medicare_nonfac_rate * claim_count)`
- `rate_decay_index`: How far Medicaid rates have fallen behind Medicare over time = `current_pct_of_medicare / baseline_pct_of_medicare`
- `implied_conversion_factor`: Reverse-engineered state CF from Medicaid rates and RVUs = `medicaid_rate / total_rvu`

### State Fiscal & Tax
State government finances, tax burden, revenue sources, debt, pension funding, and property taxes

- **fact_census_state_finances**: 16,435 rows
- **fact_tax_burden**: 50 rows
- **fact_state_tax_collections**: 271 rows
- **fact_federal_aid_share**: 49 rows
- **fact_state_debt**: 49 rows
- **fact_pension_funded_ratio**: 49 rows
- **fact_state_tax_rates**: 207 rows
- **fact_tax_revenue_sources**: 250 rows
- **fact_property_tax_rate**: 50 rows
- **fact_income_per_capita**: 50 rows

### Workforce & Shortage Areas
BLS healthcare wages (OEWS, MSA), HPSA shortage areas (primary care, dental, mental health), workforce projections, NHSC field strength and scholars, behavioral health workforce, NP/PA supply

- **dim_bls_occupation**: 16 rows
- **fact_bls_wage_msa**: 5,473 rows
- **fact_hpsa**: 68,859 rows
- **fact_mua_designation**: 19,645 rows
- **fact_nursing_earnings**: 41,857 rows
- **fact_nursing_workforce**: 17,640 rows
- **fact_workforce_projections**: 102,528 rows
- **fact_bls_oews**: 38,003 rows
- **fact_bls_medicaid_occupations**: 9,752 rows
- **fact_hpsa_dental**: 30,287 rows
- **fact_hpsa_mental_health**: 24,761 rows
- **fact_bh_workforce_projections**: 12,608 rows
- **fact_np_pa_supply**: 6,961 rows
- **fact_health_center_awards**: 70,902 rows
- **fact_nhsc_scholar_pipeline**: 56 rows
- **fact_hrsa_awarded_grants**: 70,902 rows
- **fact_hrsa_active_grants**: 18,641 rows
- Supporting: fact_bls_wage, fact_bls_wage_national, fact_nhsc_field_strength

**Planned (not yet in lake):**
- fact_bls_oews_msa: BLS OEWS full occupation x MSA/metro area detail for all healthcare occupations. ~400K rows from all_data_M_2024.xlsx (78 MB in raw).

**Named metrics (use these for consistency):**
- `wage_adequacy_ratio`: Medicaid rate implied hourly wage vs BLS market wage = `medicaid_implied_wage / bls_median_wage`
- `hpsa_score`: Health Professional Shortage Area designation score = `hpsa_score (direct)`
- `hcbs_waitlist_per_capita`: People on HCBS waitlists per 1,000 Medicaid enrollees = `waitlist_count / (enrollment / 1000)`

## How entities connect

**Drug Product** (`fact_drug_utilization`, key: `ndc`): 12 fact tables
**Economic Indicator** (`fact_unemployment`, key: `state_code`): 107 fact tables
**Enrollment Record** (`fact_enrollment`, key: `state_code`): 107 fact tables
**Expenditure Record** (`fact_expenditure`, key: `state_code`): 107 fact tables
**Geographic Area** (`fact_places_county`, key: `county_fips`): 22 fact tables
**HCBS Program** (`fact_hcbs_waitlist`, key: `state_code`): 107 fact tables
**Hospital** (`fact_hospital_cost`, key: `provider_ccn`): 88 fact tables
**Nursing Facility** (`fact_nh_provider_info`, key: `ccn`): 67 fact tables
**Healthcare Occupation** (`dim_bls_occupation`, key: `soc_code`): 6 fact tables
**Policy Document** (`fact_policy_document`, key: `doc_id`): 2 fact tables
**Medical Procedure** (`dim_procedure`, key: `cpt_hcpcs_code`): 27 fact tables
**Healthcare Provider** (`fact_provider`, key: `npi`): 38 fact tables
**Quality Measure** (`dim_scorecard_measure`, key: `measure_id`): 49 fact tables
**Rate Cell** (`fact_rate_comparison`, key: `state_code`): 107 fact tables
**State Medicaid Program** (`dim_state`, key: `state_code`): 385 fact tables

## Universal join keys

- `state_code` (VARCHAR 2): links most tables to dim_state
- `cpt_hcpcs_code` / `procedure_code`: links to dim_procedure (RVUs, descriptions)
- `provider_ccn` / `ccn`: links hospital tables to HCRIS/quality data
- `npi`: links provider-level tables
- `ndc`: links drug/pharmacy tables
- `soc_code`: links workforce/occupation tables to dim_bls_occupation
- `county_fips` / `fips_code`: links geographic/county-level data
- `measure_id`: links quality measure tables to dim_scorecard_measure
