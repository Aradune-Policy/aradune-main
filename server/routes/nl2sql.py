"""
NL2SQL endpoint: natural language → DuckDB SQL → results.

Uses Claude Sonnet to generate SQL from user questions, validates for safety,
executes against the in-memory DuckDB lake, and returns results with explanation.
"""

import re
import time
import os
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import anthropic

from server.db import get_cursor
from server.config import settings
from server.utils.error_handler import safe_route

router = APIRouter(prefix="/api/nl2sql", tags=["nl2sql"])

# ---------------------------------------------------------------------------
# Schema context for Claude — compiled from actual column inspection
# ---------------------------------------------------------------------------

SCHEMA_CONTEXT = """You are a DuckDB SQL expert. You have access to a Medicaid data lake with 185+ fact tables, 9 dimension tables, and 5 reference tables. All tables are DuckDB views over Parquet files.

## Key Dimension Tables (join to these for labels)

### dim_state (51 rows) — state metadata
  state_code VARCHAR (PK, 2-letter), state_name, methodology, conversion_factor DOUBLE,
  pct_managed_care DOUBLE, fmap DOUBLE, efmap DOUBLE,
  total_enrollment BIGINT, ffs_enrollment BIGINT, mc_enrollment BIGINT, region VARCHAR

### dim_procedure (16,978 rows) — HCPCS/CPT codes with RVUs
  procedure_code VARCHAR (PK), description, short_description, category, subcategory,
  is_em_code BOOLEAN, em_category VARCHAR,
  work_rvu DOUBLE, pe_rvu_facility DOUBLE, pe_rvu_nonfacility DOUBLE, mp_rvu DOUBLE,
  total_rvu_facility DOUBLE, total_rvu_nonfac DOUBLE,
  medicare_rate_nonfac DOUBLE, medicare_rate_fac DOUBLE, conversion_factor DECIMAL

## Core Fact Tables

### fact_rate_comparison (302,332 rows) — Medicaid vs Medicare rate comparison (45 states)
  state_code VARCHAR, procedure_code VARCHAR, medicaid_rate DOUBLE,
  medicare_nonfac_rate DOUBLE, medicare_fac_rate DOUBLE, pct_of_medicare DOUBLE,
  em_category VARCHAR, modifier VARCHAR, rate_effective_date VARCHAR

### fact_medicaid_rate (597,483 rows) — raw Medicaid fee schedule rates (47 states)
  state_code VARCHAR, procedure_code VARCHAR, modifier VARCHAR,
  rate DOUBLE, rate_facility DOUBLE, rate_nonfacility DOUBLE,
  effective_date DATE, end_date DATE, billing_unit VARCHAR, source_file VARCHAR

### fact_enrollment (10,399 rows) — monthly Medicaid enrollment
  state_code VARCHAR, year BIGINT, month BIGINT,
  total_enrollment BIGINT, chip_enrollment BIGINT, ffs_enrollment BIGINT, mc_enrollment BIGINT

### fact_claims (712,793 rows) — T-MSIS claims aggregated by state/code/month
  state_code VARCHAR, procedure_code VARCHAR, category VARCHAR,
  year INTEGER, month INTEGER, total_paid DOUBLE, total_claims DOUBLE,
  total_beneficiaries DOUBLE, provider_count BIGINT, avg_paid_per_claim DOUBLE, claim_type VARCHAR

### fact_hospital_cost (6,103 rows) — HCRIS hospital cost reports
  provider_ccn VARCHAR, hospital_name VARCHAR, state_code VARCHAR, city VARCHAR,
  facility_type VARCHAR, bed_count INTEGER, bed_days_available BIGINT,
  medicare_days BIGINT, medicaid_days BIGINT, total_days BIGINT,
  medicare_discharges INTEGER, medicaid_discharges INTEGER, total_discharges INTEGER,
  total_costs DOUBLE, net_patient_revenue DOUBLE, net_income DOUBLE,
  cost_to_charge_ratio DOUBLE, uncompensated_care_cost DOUBLE,
  dsh_adjustment DOUBLE, ime_payment DOUBLE, medicaid_day_pct DOUBLE, report_year INTEGER

### fact_bls_wage (812 rows) — BLS healthcare occupation wages by state
  state_code VARCHAR, soc_code VARCHAR, occupation_title VARCHAR, data_year INTEGER,
  total_employment INTEGER, hourly_mean DOUBLE, annual_mean DOUBLE,
  hourly_median DOUBLE, annual_median DOUBLE, hourly_p90 DOUBLE, annual_p90 DOUBLE

### fact_quality_measure (5,236 rows) — Medicaid quality measures
  state_code VARCHAR, measure_id VARCHAR, year BIGINT, rate DOUBLE,
  numerator BIGINT, denominator BIGINT, measure_name VARCHAR, domain VARCHAR

### fact_expenditure (5,379 rows) — CMS-64 Medicaid expenditure
  state_code VARCHAR, fiscal_year BIGINT, quarter BIGINT, category VARCHAR,
  subcategory VARCHAR, federal_share DOUBLE, total_computable DOUBLE

### fact_hpsa (68,859 rows) — Health Professional Shortage Areas
  hpsa_name VARCHAR, discipline VARCHAR, designation_type VARCHAR,
  hpsa_score INTEGER, state_code VARCHAR, metro_indicator VARCHAR,
  designation_population DOUBLE, pct_poverty DOUBLE, county_name VARCHAR

### fact_nsduh_prevalence (5,865 rows) — SAMHSA behavioral health prevalence
  state_code VARCHAR, measure_id VARCHAR, measure_name VARCHAR,
  age_group VARCHAR, estimate_pct DOUBLE, ci_lower_pct DOUBLE, ci_upper_pct DOUBLE,
  survey_years VARCHAR

### fact_fmap_historical — Federal Medical Assistance Percentages (MACPAC, authoritative)
  state_code VARCHAR, fiscal_year BIGINT, rate_type VARCHAR, rate DOUBLE
  -- rate_type: 'fmap' or 'efmap'. Use WHERE rate_type = 'fmap' for standard FMAP.

### fact_unwinding (57,759 rows) — Medicaid unwinding/redetermination outcomes
  state_code VARCHAR, metric VARCHAR, time_period BIGINT,
  terminated_count BIGINT, terminated_pct DOUBLE,
  cumulative_terminated BIGINT, cumulative_terminated_pct DOUBLE

### fact_scorecard (90,165 rows) — Medicaid Scorecard measures
  measure_id VARCHAR, state_code VARCHAR, measure_value DOUBLE,
  values_direction VARCHAR, median_value DOUBLE, mean_value DOUBLE

### fact_dsh_hospital (6,103 rows) — hospital-level DSH data
  provider_ccn VARCHAR, hospital_name VARCHAR, state_code VARCHAR,
  total_days BIGINT, medicaid_days BIGINT, dsh_pct DOUBLE

### fact_acs_state (51 rows) — Census ACS demographics
  state_code VARCHAR, total_population BIGINT, median_household_income DOUBLE,
  poverty_rate DOUBLE, uninsured_rate DOUBLE, medicaid_pct DOUBLE

### fact_unemployment (varies) — monthly state unemployment rates
  state_code VARCHAR, period_date DATE, value DOUBLE

### fact_drug_utilization (large) — State Drug Utilization Data (all years)
  state_code VARCHAR, ndc VARCHAR, product_name VARCHAR, year BIGINT, quarter BIGINT,
  units_reimbursed DOUBLE, total_amount_reimbursed DOUBLE, number_of_prescriptions BIGINT

### fact_sdud_2025 (2,637,009 rows) — State Drug Utilization Data 2025 (Q1-Q2)
  state_code VARCHAR, utilization_type VARCHAR, ndc VARCHAR, labeler_code VARCHAR,
  product_code VARCHAR, product_name VARCHAR, year INTEGER, quarter INTEGER,
  suppression_used BOOLEAN, units_reimbursed DOUBLE, number_of_prescriptions INTEGER,
  total_amount_reimbursed DOUBLE, medicaid_amount_reimbursed DOUBLE,
  non_medicaid_amount_reimbursed DOUBLE

### fact_maternal_health (17,968 rows) — hospital maternal health measures
  state_code VARCHAR, hospital_name VARCHAR, measure_id VARCHAR, score DOUBLE

### fact_telehealth_services (12,720 rows) — telehealth utilization
  state_code VARCHAR, service_type VARCHAR, year INTEGER, total_services BIGINT

### fact_dental_services (3,180 rows) — dental services to children
  state_code VARCHAR, measure_name VARCHAR, year INTEGER, value DOUBLE

### fact_snap_enrollment — SNAP (food stamps) monthly participation by state
  state_code VARCHAR, year INTEGER, month INTEGER,
  households INTEGER, persons INTEGER, benefit_cost DOUBLE

### fact_tanf_enrollment — TANF monthly enrollment by state (7 measures)
  state_code VARCHAR, year INTEGER, month INTEGER,
  measure VARCHAR (total_families|total_recipients|adult_recipients|child_recipients|two_parent_families|one_parent_families|no_parent_families),
  value INTEGER

### fact_eligibility_processing (3,162 rows) — Medicaid renewal/redetermination outcomes
  state_code VARCHAR, reporting_period VARCHAR (YYYYMM), original_or_updated VARCHAR,
  renewals_initiated BIGINT, renewals_due BIGINT, renewals_completed BIGINT,
  renewals_ex_parte BIGINT, renewals_form_based BIGINT,
  disenrolled_total BIGINT, disenrolled_ineligible BIGINT, disenrolled_procedural BIGINT,
  renewals_pending BIGINT

### fact_marketplace_unwinding (59,527 rows) — Marketplace transitions during Medicaid unwinding
  state VARCHAR, metric VARCHAR, time_period VARCHAR (YYYYMM),
  individual_count BIGINT, individual_pct VARCHAR, cumulative_count BIGINT, cumulative_pct VARCHAR

### fact_hcbs_waitlist (51 rows) — HCBS waiting lists by state (KFF 2025 survey)
  state_code VARCHAR, state_name VARCHAR, screens_eligibility VARCHAR,
  idd_waiting INTEGER, autism_waiting INTEGER, seniors_physical_waiting INTEGER,
  medically_fragile_waiting INTEGER, mental_health_waiting INTEGER,
  tbi_sci_waiting INTEGER, other_waiting INTEGER, total_waiting INTEGER, survey_year INTEGER

### fact_quality_core_set_2024 (5,555 rows) — 2024 Core Set quality measures
  state_code VARCHAR, domain VARCHAR, measure_name VARCHAR, measure_id VARCHAR,
  core_set_year INTEGER, state_rate DOUBLE, median_rate DOUBLE,
  bottom_quartile DOUBLE, top_quartile DOUBLE, states_reporting INTEGER

### fact_fair_market_rent (4,764 rows) — HUD FMR by county
  state_code VARCHAR, county_name VARCHAR, hud_area_name VARCHAR,
  is_metro BOOLEAN, population_2022 INTEGER,
  fmr_efficiency INTEGER, fmr_1br INTEGER, fmr_2br INTEGER, fmr_3br INTEGER, fmr_4br INTEGER

## Additional Useful Tables
- fact_chip_enrollment: state_code, year, month, enrollment_count
- fact_managed_care: state_code, year, plan_name, enrollment, plan_type
- fact_five_star: provider_ccn, state_code, overall_rating, health_inspection_rating
- fact_hospice_quality: provider_ccn, state_code, measure_id, score
- fact_medicare_enrollment: state_code, year, total_enrolled, ma_enrolled
- fact_opioid_prescribing: state_code, year, opioid_claim_count, opioid_prescribing_rate
- fact_block_grant: state_code, allotment DOUBLE (MHBG mental health block grants)
- fact_ltss_expenditure: state_code, year, ltss_total, institutional_total, institutional_pct, hcbs_total, hcbs_pct (LTSS spending by state, CY 2022-2023)
- fact_ltss_users: state_code, year, ltss_total, institutional_total, hcbs_total, both_total (LTSS users by state, CY 2022-2023)
- fact_ltss_rebalancing: state_code, year, demographic_group, subgroup, hcbs_pct (HCBS rebalancing by demographics)
- fact_vital_stats_monthly: state_code, year, month_name, indicator, value (CDC births/deaths/infant deaths by state)
- fact_maternal_mortality_monthly: jurisdiction, demographic_group, subgroup, year, month, maternal_deaths, live_births, maternal_mortality_rate
- fact_fmr_fy2024: state_code, fiscal_year, report_type (MAP=services, ADM=admin), service_category, total_computable, federal_share, state_share (CMS-64 FMR FY 2024, 80+ categories including MCO, drugs, hospitals, HCBS, nursing facilities. Use "Total Net Expenditures" for state totals.)
- fact_nsduh_prevalence_2024: state_code, measure_id, measure_name, age_group, estimate_pct, ci_lower_pct, ci_upper_pct, survey_years (NSDUH 2023-2024, 18 BH measures incl. SUD, opioid, mental illness, depression, suicidal thoughts)
- fact_mc_enrollment_summary: state_code, year, total_enrollees, total_mc_enrollment, comprehensive_mc_enrollment, new_adults_mc_enrollment, mc_penetration_pct (2016-2024, 57 states, managed care enrollment trends. mc_penetration_pct = total_mc / total_enrollees * 100. 'US' row = national totals.)
- fact_saipe_poverty: state_code, state_fips, county_fips, name, geo_level (state/county), year, poverty_estimate_all, poverty_pct_all, poverty_estimate_0_17, poverty_pct_0_17, poverty_estimate_5_17_families, poverty_pct_5_17_families, median_household_income, poverty_estimate_0_4, poverty_pct_0_4 (Census SAIPE 2023, 52 states + 3,144 counties. county_fips='000' for state rows.)
- fact_places_county: year, state_code, county_name, category, measure_id, measure_name, data_value, value_unit, value_type, ci_lower, ci_upper, total_population, county_fips (CDC PLACES 2025 release. 40 health measures at county level: ACCESS2=uninsured, DIABETES=diabetes, BPHIGH=high BP, CASTHMA=asthma, OBESITY=obesity, MHLTH=poor mental health days, etc.)
- fact_workforce_projections: year, profession_group, profession, state, rurality, supply_fte, demand_fte, pct_adequacy, region (HRSA 2023-2038 projections. 121 professions incl. physicians, NPs, RNs, psychiatry. pct_adequacy = supply/demand ratio. state='Total' for national.)
- fact_food_environment: FIPS, State, County, Variable_Code, Value (USDA Food Environment Atlas 2025. 304 variables at county level. Key codes: PCT_LACCESS_POP19=% low food access, FOODINSEC_22_24=food insecurity rate, PCT_SNAP23=% SNAP participation, PCT_DIABETES_ADULTS13=diabetes rate, PCT_OBESE_ADULTS20=obesity rate. JOIN ref_food_environment_variables for variable descriptions.)
- fact_medicare_telehealth: year, quarter, state_name, enrollment_status, race, sex, entitlement_status, age_group, rurality, eligible_beneficiaries, part_b_enrolled, telehealth_users, telehealth_pct (Q1 2020-Q2 2025. 56 geographic areas. Filter enrollment_status='All' for overall. quarter='Overall' for annual.)
- fact_medicare_geo_variation: YEAR, BENE_GEO_LVL (National/State/County), BENE_GEO_DESC, BENE_GEO_CD, BENE_AGE_LVL, BENES_FFS_CNT, TOT_MDCR_STDZD_PYMT_PC (standardized per-capita), IP_MDCR_PYMT_PC, etc. (247 columns. 2014-2023. State/county/national. Per-capita standardized Medicare spending, utilization rates, chronic conditions, quality.)
- fact_nhe_state: payer (medicaid/medicare/private_insurance/total), metric (aggregate/per_enrollee/per_capita/enrollment/population), unit, item_desc, geo_level (state/region/national), geo_name, year, value (CMS NHE 1991-2020. 30 years of health spending by state. payer='medicaid' AND metric='aggregate' for total Medicaid spending. metric='per_enrollee' for per-enrollee cost.)
- fact_medicaid_drug_spending: Brnd_Name, Gnrc_Name, Mftr_Name, Tot_Spndng_2023, Tot_Clms_2023, Avg_Spnd_Per_Dsg_Unt_Wghtd_2023, etc. (wide format 2019-2023, brand-level drug spending, top Medicaid drugs)
- fact_mssp_aco: ACO_ID, ACO_Name, ACO_Service_Area, Agreement_Period_Num, BASIC_Track, ENHANCED_Track, High_Revenue_ACO, Low_Revenue_ACO, ACO_Address, ACO_State, N_AB (assigned beneficiaries), etc. (511 ACOs, PY2026)
- fact_aco_beneficiaries_county: Year, ACO_ID, State_Name, County_Name, State_ID, County_ID, AB_Psn_Yrs_ESRD, AB_Psn_Yrs_DIS, AB_Psn_Yrs_AGDU, AB_Psn_Yrs_AGND, Tot_AB_Psn_Yrs, Tot_AB (135K rows, 2024)
- fact_part_d_geo: Prscrbr_Geo_Lvl, Prscrbr_Geo_Cd, Prscrbr_Geo_Desc, Brnd_Name, Gnrc_Name, Tot_Prscrbrs, Tot_Clms, Tot_30day_Fills, Tot_Drug_Cst, Tot_Benes, Opioid_Drug_Flag, Antpsyct_Drug_Flag (116K rows, state-level Part D drug prescribing 2023)
- fact_part_d_quarterly_spending: Brnd_Name, Gnrc_Name, Tot_Mftr, Mftr_Name, Year, Tot_Benes, Tot_Clms, Tot_Spndng, Avg_Spnd_Per_Bene (28K rows, quarterly Part D drug spending)
- fact_nhsc_field_strength: state_name, discipline (All/Primary Care/Mental Health), fiscal_year, total_clinicians, nhsc_lrp, nhsc_sp, non_rural, rural (222 rows, FY2025)
- fact_macpac_enrollment: state_name, fiscal_year, total_enrollment, child, new_adult_group, other_adult, disabled, aged, dual_total, full_benefit (62 rows, FY2023)
- fact_macpac_spending_per_enrollee: state_name, fiscal_year, total_all, total_full_benefit, child_all, new_adult_all, disabled_all, aged_all (63 rows, FY2023, dollars per FYE enrollee)
- fact_teds_admissions: year, state_fips, total_admissions, alcohol_admissions, heroin_admissions, opioid_synth_admissions, meth_admissions, cocaine_admissions, marijuana_admissions, medicaid_insurance, no_insurance, injection_drug_use, criminal_justice_referral (49 states, 2023 SAMHSA TEDS-A. state_fips is numeric FIPS code.)
- fact_mssp_financial_results: ACO_Num, ACO_Name, ACO_State, N_AB (assigned benes), QualScore, GenSaveLoss (generated savings/losses $), EarnSaveLoss (earned savings $), etc. (476 ACOs, PY2024)
- fact_cdc_overdose_deaths: State, Year, Month, Period, Indicator, Data_Value, Predicted_Value (81K rows. Indicator has drug categories like 'Heroin', 'Synthetic opioids'. Monthly provisional counts.)
- fact_macpac_spending_by_state: state_name, fiscal_year, benefits_total, benefits_federal, benefits_state, admin_total, total_medicaid_total (66 rows, FY2024. Amounts in millions.)
- fact_macpac_benefit_spending: state_name, fiscal_year, total_benefits, ffs_hospital, ffs_physician, ffs_dental, ffs_nursing_facility, ffs_prescribed_drugs, managed_care, dsh_adjustments (72 rows, FY2024. Amounts in millions.)
- fact_part_d_opioid_geo: Prscrbr_Geo_Lvl (State/National), Prscrbr_Geo_Desc, Tot_Opioid_Clms, Tot_LA_Opioid_Clms, Opioid_Prscrbng_Rate, LA_Opioid_Prscrbng_Rate (329K rows by state/county, 2023)

## Rules for SQL Generation

1. ALWAYS add LIMIT (max 500 rows). If user wants "all states", LIMIT 60 is sufficient.
2. Only generate SELECT statements. Never INSERT/UPDATE/DELETE/DROP/ALTER.
3. Use state_code (2-letter) not state_name in WHERE/GROUP BY. JOIN dim_state for names.
4. For Medicaid rate lookups: COALESCE(rate, rate_nonfacility, rate_facility) from fact_medicaid_rate.
5. For rate adequacy: use pct_of_medicare from fact_rate_comparison (pre-computed, most reliable).
6. Filter out bad data: medicaid_rate > 0, pct_of_medicare > 0 AND pct_of_medicare < 10.
7. HAVING COUNT(*) >= 11 for any aggregate with beneficiary/utilization counts (minimum cell size).
8. Dates: use DATE comparisons. Months are stored as integers (1-12), years as integers.
9. For cross-state comparisons, fact_rate_comparison is the go-to table.
10. Use descriptive column aliases (e.g., avg_pct_of_medicare, total_hospitals).
11. Format output for readability: ROUND() dollar amounts to 2 decimals, percentages to 1.
12. If a question is ambiguous, make a reasonable assumption and note it in the explanation.
13. Every column referenced must exist in the schema above. Do not guess column names.
14. DuckDB syntax: use ILIKE for case-insensitive LIKE, || for string concat, :: for casts.
15. Avoid selecting snapshot, snapshot_date, pipeline_run_id, source, source_file columns — they are metadata, not useful to users.
"""

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class NL2SQLRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=1000, description="Natural language question")
    limit: int = Field(default=100, ge=1, le=500)


class NL2SQLResponse(BaseModel):
    sql: str
    explanation: str
    rows: list[dict[str, Any]]
    total_rows: int
    query_ms: int
    model: str = "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# SQL safety validation
# ---------------------------------------------------------------------------

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|ATTACH|COPY|EXPORT|IMPORT|LOAD|INSTALL)\b",
    re.IGNORECASE,
)

_SEMICOLONS = re.compile(r";.+", re.DOTALL)


def validate_sql(sql: str) -> str:
    """Validate and sanitize generated SQL. Returns cleaned SQL or raises."""
    sql = sql.strip().rstrip(";")

    # Strip anything after first semicolon (prevent multi-statement injection)
    sql = _SEMICOLONS.sub("", sql)

    if not sql.upper().startswith("SELECT") and not sql.upper().startswith("WITH"):
        raise ValueError("Only SELECT/WITH queries are allowed")

    if _FORBIDDEN.search(sql):
        raise ValueError("Query contains forbidden SQL keywords")

    # Ensure LIMIT exists
    if "LIMIT" not in sql.upper():
        sql += " LIMIT 500"

    return sql


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.post("", response_model=NL2SQLResponse)
@safe_route(default_response={"sql": "", "explanation": "", "rows": [], "total_rows": 0, "query_ms": 0, "model": ""})
async def nl2sql(req: NL2SQLRequest):
    """Translate natural language to SQL, execute against DuckDB, return results."""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="NL2SQL not configured (missing API key)")

    client = anthropic.Anthropic(api_key=api_key)

    # Step 1: Ask Claude to generate SQL
    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SCHEMA_CONTEXT,
            messages=[
                {
                    "role": "user",
                    "content": f"Generate a DuckDB SQL query to answer this question. Return ONLY a JSON object with two keys: \"sql\" (the SQL query) and \"explanation\" (1-2 sentence explanation of what the query does and any assumptions made). Limit results to {req.limit} rows.\n\nQuestion: {req.query}",
                }
            ],
        )
    except anthropic.APIError as e:
        raise HTTPException(status_code=502, detail=f"Claude API error: {e}")

    # Step 2: Parse Claude's response
    text = response.content[0].text.strip()

    # Extract JSON from markdown code blocks if present
    json_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if json_match:
        text = json_match.group(1)

    import json
    try:
        parsed = json.loads(text)
        sql = parsed["sql"]
        explanation = parsed["explanation"]
    except (json.JSONDecodeError, KeyError):
        # Fallback: try to extract SQL from the response
        sql_match = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1)
            explanation = "Query generated from your question."
        else:
            raise HTTPException(status_code=422, detail="Could not parse SQL from Claude's response")

    # Step 3: Validate SQL
    try:
        sql = validate_sql(sql)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"Unsafe SQL rejected: {e}")

    # Step 4: Execute against DuckDB
    t0 = time.time()
    try:
        with get_cursor() as cur:
            result = cur.execute(sql).fetchall()
            columns = [desc[0] for desc in cur.description]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Query execution failed: {e}")
    query_ms = int((time.time() - t0) * 1000)

    # Step 5: Format results
    rows = [dict(zip(columns, row)) for row in result]

    # Convert any non-JSON-serializable types
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif v is not None and not isinstance(v, (str, int, float, bool)):
                row[k] = str(v)

    return NL2SQLResponse(
        sql=sql,
        explanation=explanation,
        rows=rows,
        total_rows=len(rows),
        query_ms=query_ms,
    )


# ---------------------------------------------------------------------------
# Example queries endpoint (for frontend "Try these" buttons)
# ---------------------------------------------------------------------------

EXAMPLES = [
    "What 10 states pay the lowest primary care rates as a percentage of Medicare?",
    "Which states have the most Health Professional Shortage Areas?",
    "Show me the top 10 hospitals by Medicaid days",
    "What is the average Medicaid-to-Medicare rate by state for E&M codes?",
    "Which states spend the most on prescription drugs per capita?",
    "Compare mental health prevalence rates across all states",
    "What states have the highest FMAP rates?",
    "Show me enrollment trends — total Medicaid enrollment by year",
    "Which states have the most SNAP recipients relative to Medicaid enrollment?",
    "What are the most expensive counties for 2-bedroom rent by state?",
]


@router.get("/examples")
@safe_route(default_response={"examples": []})
async def get_examples():
    """Return example queries for the frontend."""
    return {"examples": EXAMPLES}
