# Aradune Dataset Catalog

> 250 fact tables + 9 dimensions + 9 reference + 2 compatibility views = **270 registered views, 115M+ rows**
>
> Last updated: 2026-03-10

---

## Medicaid Core

| Table | Description |
|-------|-------------|
| `fact_medicaid_rate` | Medicaid fee schedule rates by state, code, and modifier (47 states, 597K rows) |
| `fact_rate_comparison` | Medicaid vs Medicare rate comparison with pct_of_medicare (45 states, 302K rows) |
| `fact_enrollment` | Monthly Medicaid enrollment by state (total, CHIP, FFS, managed care) |
| `fact_claims` | T-MSIS claims aggregated by state, procedure code, and month |
| `fact_expenditure` | CMS-64 Medicaid expenditure by state, category, and quarter |
| `fact_fmap` | Federal Medical Assistance Percentages (FMAP/eFMAP) by state |
| `fact_managed_care` | Managed care plan enrollment by state and plan type |
| `fact_mc_enrollment_summary` | Managed care enrollment summary by state and year (2016-2024, MC penetration %) |
| `fact_mc_dashboard` | Managed care MCO utilization by county, service category (AZ/MI/NV/NM, 2020+) |
| `fact_unwinding` | PHE unwinding redetermination outcomes by state |
| `fact_eligibility_processing` | Medicaid/CHIP renewal and redetermination outcomes by state (3,162 rows) |
| `fact_marketplace_unwinding` | HealthCare.gov Medicaid unwinding marketplace transitions (59,527 rows) |
| `fact_sbm_unwinding` | State-Based Marketplace Medicaid unwinding data (128 rows) |
| `fact_new_adult_spending` | Medicaid VIII Group (expansion) expenditures by state (Q3 FY 2025) |
| `fact_fmr_fy2024` | CMS-64 Financial Management Report FY 2024, 80+ service categories ($909B total) |
| `fact_scorecard` | CMS Medicaid Scorecard measures with state and national benchmarks |
| `fact_dsh_payment` | Disproportionate Share Hospital payments by state |
| `fact_dsh_hospital` | Hospital-level DSH data (6,103 hospitals) |
| `fact_eligibility` | Medicaid eligibility thresholds by state and group |
| `fact_eligibility_levels` | Medicaid eligibility income levels by state, category, and population |
| `fact_new_adult` | Medicaid expansion (new adult) enrollment data |
| `fact_medicaid_applications` | Medicaid application volume and processing by state |
| `fact_renewal_processing` | Medicaid renewal processing outcomes by state |
| `fact_sdp_preprint` | Supplemental payment demonstration preprints (34 states) |

## Enrollment Breakdowns

| Table | Description |
|-------|-------------|
| `fact_mc_enrollment` | Managed care enrollment counts by state |
| `fact_mc_enroll_pop` | Managed care enrollment by population group |
| `fact_mc_enroll_pop2` | Managed care enrollment by population (alternate breakdown) |
| `fact_mc_enroll_duals` | Managed care enrollment among dual eligibles |
| `fact_mc_enrollment_plan` | Managed care enrollment by individual plan |
| `fact_mc_share` | Managed care market share by state |
| `fact_mc_monthly` | Managed care monthly enrollment trends |
| `fact_mc_annual` | Managed care annual enrollment |
| `fact_mc_info_monthly` | Managed care plan information, monthly |
| `fact_mc_summary` | Managed care summary statistics by state |
| `fact_mc_programs` | Managed care program types by state |
| `fact_mc_quality_features` | Managed care quality and access features by state |
| `fact_elig_group_monthly` | Medicaid enrollment by eligibility group, monthly |
| `fact_elig_group_annual` | Medicaid enrollment by eligibility group, annual |
| `fact_new_adult_enrollment` | New adult (expansion) enrollment trends |
| `fact_program_monthly` | Medicaid program enrollment, monthly |
| `fact_program_annual` | Medicaid program enrollment, annual |
| `fact_dual_status_monthly` | Dual-eligible enrollment by status, monthly |
| `fact_benefit_package` | Medicaid benefit package details by state |
| `fact_performance_indicator` | Medicaid program performance indicators |

## Quality & Core Sets

| Table | Description |
|-------|-------------|
| `fact_quality_measure` | Medicaid Adult/Child Core Set quality measures |
| `fact_quality_core_set_2023` | FFY 2023 Child/Adult Core Set, 56 measures, 51 states |
| `fact_quality_core_set_2024` | FFY 2024 Child/Adult Core Set, 57 measures, 51 states (first mandatory year) |
| `fact_dq_atlas` | T-MSIS Data Quality Atlas ratings by state and data element |

## CHIP & Children

| Table | Description |
|-------|-------------|
| `fact_chip_enrollment` | CHIP enrollment counts by state and month |
| `fact_chip_eligibility` | CHIP eligibility thresholds by state |
| `fact_continuous_eligibility` | Continuous eligibility policies by state |
| `fact_chip_enrollment_unwinding` | CHIP enrollment during PHE unwinding |
| `fact_chip_program_monthly` | CHIP program enrollment, monthly |
| `fact_chip_program_annual` | CHIP program enrollment, annual |
| `fact_chip_monthly` | CHIP monthly enrollment trends |
| `fact_chip_app_elig` | CHIP applications and eligibility determinations |
| `fact_epsdt` | Early and Periodic Screening, Diagnostic, and Treatment (CMS-416) |
| `fact_dental_services` | Dental services to Medicaid children under 19 |
| `fact_well_child_visits` | Well-child visit rates by state |
| `fact_blood_lead_screening` | Blood lead screening rates for Medicaid children |
| `fact_vaccinations` | Childhood vaccination rates by state |
| `fact_health_screenings` | Health screening completion rates |

## Pharmacy

| Table | Description |
|-------|-------------|
| `fact_drug_utilization` | State Drug Utilization Data (SDUD), Medicaid prescriptions |
| `fact_sdud_2024` | State Drug Utilization Data, 2024 quarterly |
| `fact_sdud_2025` | State Drug Utilization Data, 2025 Q1-Q2 (2.64M rows, $108.8B reimbursed) |
| `fact_nadac` | National Average Drug Acquisition Cost pharmacy pricing |
| `fact_medicaid_drug_spending` | Medicaid spending by drug, brand/generic, 2019-2023 |
| `fact_aca_ful` | ACA Federal Upper Limit drug pricing |
| `fact_drug_rebate_products` | Medicaid Drug Rebate Program product list |
| `fact_medicaid_opioid_prescribing` | Medicaid opioid prescribing patterns by state |

## Hospital Quality & Finance

| Table | Description |
|-------|-------------|
| `fact_hospital_cost` | HCRIS hospital cost reports: financials, beds, payer mix, margins |
| `fact_hospital_rating` | Overall hospital quality star ratings from CMS |
| `fact_hospital_vbp` | Hospital Value-Based Purchasing program scores |
| `fact_hospital_hrrp` | Hospital Readmissions Reduction Program data |
| `fact_hospital_service_area` | Medicare discharges by hospital and ZIP code (1.16M rows) |
| `fact_hospital_directory` | Hospital directory with addresses and characteristics |
| `fact_hac_measure` | Hospital-Acquired Condition Reduction Program measures |
| `fact_pos_hospital` | Provider of Services: hospital facility data |
| `fact_pos_other` | Provider of Services: non-hospital facilities |
| `fact_cms_impact` | CMS Impact File: projected Medicare payment changes by hospital |
| `fact_hcahps_state` | HCAHPS patient experience survey scores by state |
| `fact_mspb_state` | Medicare Spending Per Beneficiary by state |
| `fact_mspb_hospital` | Medicare Spending Per Beneficiary by hospital |
| `fact_mspb_hospital_detail` | Medicare Spending Per Beneficiary hospital detail |
| `fact_timely_effective` | Timely and effective care measures by state |
| `fact_timely_effective_hosp` | Timely and effective care measures by hospital |
| `fact_hospital_timely_effective` | Hospital timely and effective care (alternate view) |
| `fact_complications` | Complication measures by state |
| `fact_complications_hosp` | Complication measures by hospital |
| `fact_unplanned_visits` | Unplanned visit measures by state |
| `fact_unplanned_visits_hosp` | Unplanned visit measures by hospital |
| `fact_psi90_hospital` | Patient Safety Indicator (PSI-90) composite by hospital |
| `fact_imaging_hospital` | Outpatient imaging efficiency measures by hospital |
| `fact_medicare_spending_claim` | Medicare spending per claim by hospital and claim type |
| `fact_provider_specific` | Provider-specific Medicare payment data |

## Healthcare-Associated Infections

| Table | Description |
|-------|-------------|
| `fact_hai_state` | Healthcare-associated infection rates by state |
| `fact_hai_hospital` | Healthcare-associated infection rates by hospital |
| `fact_hai_hospital2` | Healthcare-associated infections by hospital (alternate measures) |

## Nursing Homes

| Table | Description |
|-------|-------------|
| `fact_five_star` | CMS Five-Star nursing facility quality ratings |
| `fact_nh_penalties_v2` | Nursing home penalties: fines and payment denials (17,463 rows) |
| `fact_nh_survey_summary` | Nursing home survey deficiency results by facility (43,983 rows) |
| `fact_nh_deficiencies` | Nursing home deficiency citations |
| `fact_nh_deficiency` | Nursing home deficiency detail (alternate view) |
| `fact_nh_ownership` | Nursing home ownership and organizational structure |
| `fact_nh_provider_info` | Nursing home provider directory and characteristics |
| `fact_nh_claims_quality` | Nursing home claims-based quality measures |
| `fact_nh_state_averages` | Nursing home quality state-level averages |
| `fact_nh_mds_quality` | Nursing home MDS-derived quality measures |
| `fact_snf_cost` | Skilled Nursing Facility cost reports |
| `fact_snf_vbp` | SNF Value-Based Purchasing program scores |
| `fact_snf_quality_provider` | SNF quality measures by provider |
| `fact_pbj_nurse_staffing` | Payroll-Based Journal nurse staffing levels |
| `fact_pbj_nonnurse_staffing` | Payroll-Based Journal non-nurse staffing levels |
| `fact_pbj_employee` | Payroll-Based Journal employee-level data |
| `fact_mds_quality` | Minimum Data Set quality measures |

## LTSS & HCBS

| Table | Description |
|-------|-------------|
| `fact_hcbs_waitlist` | HCBS waiting lists by state and population: 606,895 people, 41 states (KFF 2025) |
| `fact_ltss_expenditure` | Medicaid LTSS expenditure: institutional vs HCBS breakdown (CY 2022-2023) |
| `fact_ltss_users` | Medicaid LTSS users: institutional vs HCBS vs both (CY 2022-2023) |
| `fact_ltss_rebalancing` | LTSS HCBS rebalancing: % HCBS by age group and state (CY 2022-2023) |
| `fact_cms372_waiver` | CMS-372 waiver program records with expenditure data |
| `fact_mltss` | Managed LTSS program data by state |
| `fact_mltss_enrollment2` | Managed LTSS enrollment (alternate breakdown) |
| `fact_hcbs_waiver_enrollment` | HCBS waiver enrollment by state and waiver |
| `fact_hcbs_authority` | HCBS authority types (1915c, 1915i, 1915k, state plan) by state |
| `fact_1915c_participants` | 1915(c) waiver participants by state and waiver |

## Hospice

| Table | Description |
|-------|-------------|
| `fact_hospice_quality` | Hospice facility-level quality measures (4,948 hospices) |
| `fact_hospice_state` | Hospice utilization and quality by state |
| `fact_hospice_provider` | Hospice provider directory |
| `fact_hospice_directory` | Hospice facility directory with addresses |
| `fact_hospice_cahps_state` | Hospice CAHPS patient experience by state |

## Home Health

| Table | Description |
|-------|-------------|
| `fact_home_health_agency` | Home health agency directory with quality ratings |
| `fact_home_health_state` | Home health utilization and quality by state |
| `fact_home_health_state2` | Home health measures by state (alternate view) |
| `fact_hha_cost_report` | Home Health Agency cost reports: revenues, costs, visits (10,715 agencies) |
| `fact_hhcahps_provider` | Home Health CAHPS patient experience by provider |
| `fact_hhcahps_state` | Home Health CAHPS patient experience by state |

## Dialysis & ESRD

| Table | Description |
|-------|-------------|
| `fact_dialysis_facility_v2` | Dialysis Facility Compare: all Medicare facilities with quality measures (7,557) |
| `fact_dialysis_facility` | Dialysis facility data (prior version) |
| `fact_dialysis_state` | Dialysis utilization by state |
| `fact_esrd_etc_results` | ESRD Treatment Choices model results (433 rows) |
| `fact_esrd_qip` | ESRD Quality Incentive Program scores |
| `fact_esrd_qip_tps` | ESRD QIP Total Performance Scores |

## Inpatient Rehab & Long-Term Care Hospitals

| Table | Description |
|-------|-------------|
| `fact_irf_provider` | Inpatient rehabilitation facility quality measures |
| `fact_ltch_provider` | Long-term care hospital quality measures |

## Ambulatory Surgical Centers

| Table | Description |
|-------|-------------|
| `fact_asc_quality_state` | ASC quality measures by state |
| `fact_asc_facility` | ASC quality measures by facility |
| `fact_oas_cahps_state` | Outpatient/ASC CAHPS patient experience by state |

## Post-Acute Care

| Table | Description |
|-------|-------------|
| `fact_pac_hha_utilization` | Medicare post-acute HHA: episodes, spending, outcomes by state (8,519 rows) |
| `fact_pac_irf_utilization` | Medicare post-acute IRF: stays, spending, outcomes by state (1,205 rows) |
| `fact_pac_ltch_utilization` | Medicare post-acute LTCH: stays, spending, outcomes by state (373 rows) |

## Medicare

| Table | Description |
|-------|-------------|
| `fact_medicare_enrollment` | Medicare enrollment by state including MA penetration |
| `fact_medicare_program_stats` | Medicare Part A & B program statistics: utilization, payments (2018-2023) |
| `fact_medicare_telehealth` | Medicare telehealth utilization by state, quarter (Q1 2020-Q2 2025, 32K rows) |
| `fact_medicare_geo_variation` | Medicare FFS geographic variation: spending, utilization, quality (2014-2023) |
| `fact_ma_geo_variation` | Medicare Advantage geographic variation by state (2016-2022) |
| `fact_medicare_physician_geo` | Medicare Physician & Other Practitioners by geography (269K rows) |
| `fact_medicare_provider_enrollment` | Medicare provider enrollment data |
| `fact_opioid_prescribing` | Medicare Part D opioid prescribing rates by state |
| `fact_telehealth_services` | Telehealth utilization by state and service type |
| `fact_maternal_health` | Hospital-level maternal health quality measures |
| `fact_physician_compare` | Physician Compare provider directory |
| `fact_vha_provider` | VA healthcare provider data |
| `fact_acute_care` | Acute care utilization measures |

## Medicare Part D

| Table | Description |
|-------|-------------|
| `fact_part_d_geo` | Part D prescribing by state and drug: claims, spending, opioid flags (116K rows) |
| `fact_part_d_quarterly_spending` | Part D quarterly drug spending: brand/generic, trends (28K rows) |
| `fact_part_d_opioid_geo` | Part D opioid prescribing rates by state and county (329K rows) |
| `fact_part_d_spending_by_drug` | Part D total spending per drug (14,309 drugs) |

## ACO & Value-Based Care

| Table | Description |
|-------|-------------|
| `fact_mssp_aco` | MSSP ACO organizations: 511 ACOs with track, assignment (PY2026) |
| `fact_mssp_participants` | MSSP ACO participant TINs/NPIs: 15,370 linkages (PY2026) |
| `fact_mssp_financial_results` | MSSP ACO financial & quality results (476 ACOs, PY2024) |
| `fact_aco_beneficiaries_county` | ACO assigned beneficiaries by county (135K rows) |
| `fact_aco_reach_results` | ACO REACH financial & quality results (132 ACOs, PY3) |

## MACPAC

| Table | Description |
|-------|-------------|
| `fact_macpac_enrollment` | Exhibit 14: Medicaid enrollment by eligibility group, dual status (FY 2023) |
| `fact_macpac_spending_per_enrollee` | Exhibit 22: Benefit spending per FYE enrollee by state and group (FY 2023) |
| `fact_macpac_spending_by_state` | Exhibit 16: Spending by state, benefits/admin, federal/state split (FY 2024) |
| `fact_macpac_benefit_spending` | Exhibit 17: Benefit spending by state and category (FY 2024) |
| `fact_macpac_supplemental` | MACPAC supplemental payment data |

## Behavioral Health

| Table | Description |
|-------|-------------|
| `fact_nsduh_prevalence` | SAMHSA behavioral health prevalence estimates by state |
| `fact_nsduh_prevalence_2024` | SAMHSA NSDUH 2023-2024 prevalence: 18 measures, 52 states |
| `fact_block_grant` | SAMHSA Mental Health Block Grant allotments by state |
| `fact_mh_facility` | SAMHSA treatment facility directory with bed counts |
| `fact_bh_by_condition` | Behavioral health conditions by state from T-MSIS |
| `fact_mh_sud_recipients` | Mental health and SUD service recipients by state |
| `fact_teds_admissions` | TEDS-A substance abuse treatment admissions (49 states, 1.6M admissions) |
| `fact_physical_among_mh` | Physical health conditions among mental health population |
| `fact_physical_among_sud` | Physical health conditions among SUD population |
| `fact_bh_services` | Behavioral health service utilization by state |
| `fact_integrated_care` | Integrated physical/behavioral health care data |
| `fact_brfss` | CDC BRFSS behavioral risk factor survey data by state |
| `fact_brfss_behavioral` | BRFSS behavioral health-specific measures |
| `fact_ipf_quality_state` | Inpatient psychiatric facility quality by state |
| `fact_ipf_quality_facility` | Inpatient psychiatric facility quality by facility |
| `fact_ipf_facility` | Inpatient psychiatric facility directory |
| `fact_otp_provider` | Opioid treatment program provider directory |
| `fact_respiratory_conditions` | Respiratory conditions prevalence by state |

## Maternal & Reproductive Health

| Table | Description |
|-------|-------------|
| `fact_maternal_morbidity` | Severe maternal morbidity rates by state |
| `fact_pregnancy_outcomes` | Pregnancy outcome measures by state |
| `fact_pregnant_postpartum` | Pregnant and postpartum coverage by state |
| `fact_smm_extended` | Severe maternal morbidity extended measures |
| `fact_nas_rates` | Neonatal Abstinence Syndrome rates by state |
| `fact_perinatal_care` | Perinatal care quality measures |
| `fact_contraceptive_care` | Contraceptive care access and utilization |

## Workforce

| Table | Description |
|-------|-------------|
| `fact_bls_wage` | BLS OEWS healthcare occupation wages by state (16 occupations) |
| `fact_bls_wage_msa` | BLS OEWS healthcare wages by metro area |
| `fact_bls_wage_national` | BLS OEWS healthcare wages, national benchmarks |
| `fact_hpsa` | HRSA Health Professional Shortage Area designations (3 disciplines) |
| `fact_workforce_projections` | HRSA workforce supply/demand projections 2023-2038 (121 professions) |
| `fact_nhsc_field_strength` | NHSC clinician counts by state and discipline (FY2025) |
| `fact_nursing_workforce` | NSSRN nursing demographics by state: RN/LPN/APRN (2022 survey) |
| `fact_nursing_earnings` | NSSRN nursing earnings and hours by state (2022 survey) |

## FQHCs & Safety Net

| Table | Description |
|-------|-------------|
| `fact_health_center_sites` | HRSA FQHC and look-alike sites directory (8,121 sites) |
| `fact_fqhc_hypertension` | FQHC hypertension control rates from UDS (6,866 rows) |
| `fact_fqhc_quality_badges` | FQHC quality recognition badges (7,438 rows) |
| `fact_fqhc_directory` | FQHC facility directory |
| `fact_mua_designation` | Medically Underserved Areas/Populations designations (19,645 areas) |

## Economic & Demographic

| Table | Description |
|-------|-------------|
| `fact_acs_state` | Census ACS demographics: population, poverty, income, insurance |
| `fact_unemployment` | Monthly state unemployment rates from BLS LAUS |
| `fact_cpi` | Consumer Price Index trends |
| `fact_median_income` | Median household income by state |
| `fact_state_gdp` | State GDP data |
| `fact_state_population` | State population counts |
| `fact_saipe_poverty` | Census SAIPE 2023 poverty estimates and median income (3,196 rows) |
| `fact_snap_enrollment` | SNAP monthly participation and benefit cost by state (FY2019-2026) |
| `fact_tanf_enrollment` | TANF monthly families and recipients by state (FY2023-2024) |
| `fact_fair_market_rent` | HUD Fair Market Rents by county, 0BR-4BR (FY2025, 4,764 areas) |
| `fact_nhe_state` | National Health Expenditure by state, 1991-2020 (117K rows) |
| `fact_marketplace_oep` | ACA Marketplace 2025 Open Enrollment: plan selections, premiums, APTC |
| `fact_market_saturation_county` | CMS Market Saturation and Utilization by county (962K rows) |
| `fact_ahrf_county` | Area Health Resources File: county-level health resources |

## Public Health

| Table | Description |
|-------|-------------|
| `fact_places_county` | CDC PLACES county health estimates: 40 measures, 3,144 counties |
| `fact_food_environment` | USDA Food Environment Atlas: 304 variables by county |
| `fact_vital_stats_monthly` | CDC VSRR monthly births, deaths, infant deaths by state (2023-2024) |
| `fact_vital_stats` | Vital statistics summary data |
| `fact_maternal_mortality_monthly` | CDC provisional maternal mortality: rolling 12-month rates |
| `fact_maternal_mortality_national` | CDC maternal mortality national trends |
| `fact_cdc_overdose_deaths` | CDC provisional drug overdose deaths by state and drug type (81,270 rows) |
| `fact_cdc_leading_causes_death` | CDC leading causes of death by state since 1999 (10,868 rows) |
| `fact_drug_overdose` | Drug overdose death rates by state |
| `fact_mortality_trend` | Mortality trends by state |

## Financial Management

| Table | Description |
|-------|-------------|
| `fact_financial_mgmt` | Medicaid financial management data by state |
| `fact_financial_mgmt_national` | Medicaid financial management, national totals |
| `fact_fmr_supplemental` | CMS-64 Financial Management Report supplemental data |
| `fact_cms64_new_adult` | CMS-64 new adult (expansion) expenditure |
| `fact_cms64_ffcra` | CMS-64 FFCRA enhanced FMAP expenditure |
| `fact_caa_fmap` | Consolidated Appropriations Act FMAP adjustments |
| `fact_ffcra_fmap` | Families First Coronavirus Response Act FMAP data |

## Policy Corpus (RAG)

| Table | Description |
|-------|-------------|
| `fact_policy_document` | 1,039 CMS guidance documents (CIBs, SHOs, SMDs) |
| `fact_policy_chunk` | 6,058 searchable text chunks from policy documents |

## Dimensions & Reference

| Table | Description |
|-------|-------------|
| `dim_state` | State codes, names, FMAP, methodology, enrollment |
| `dim_procedure` | HCPCS/CPT procedure codes with RVUs and Medicare rates |
| `dim_medicare_locality` | Medicare GPCI values by locality |
| + 6 additional dimension tables | Time, provider type, eligibility group, payer, geography, diagnosis |
| + 9 reference tables | Crosswalks, code mappings, taxonomy lookups |
| + 2 compatibility views | Legacy view aliases |
