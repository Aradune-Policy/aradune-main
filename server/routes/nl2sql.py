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

### fact_fmap (51 rows) — Federal Medical Assistance Percentages
  state_code VARCHAR, fiscal_year BIGINT, fmap_rate DOUBLE, efmap_rate DOUBLE

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

### fact_drug_utilization (large) — State Drug Utilization Data
  state_code VARCHAR, ndc VARCHAR, product_name VARCHAR, year BIGINT, quarter BIGINT,
  units_reimbursed DOUBLE, total_amount_reimbursed DOUBLE, number_of_prescriptions BIGINT

### fact_maternal_health (17,968 rows) — hospital maternal health measures
  state_code VARCHAR, hospital_name VARCHAR, measure_id VARCHAR, score DOUBLE

### fact_telehealth_services (12,720 rows) — telehealth utilization
  state_code VARCHAR, service_type VARCHAR, year INTEGER, total_services BIGINT

### fact_dental_services (3,180 rows) — dental services to children
  state_code VARCHAR, measure_name VARCHAR, year INTEGER, value DOUBLE

## Additional Useful Tables
- fact_chip_enrollment: state_code, year, month, enrollment_count
- fact_managed_care: state_code, year, plan_name, enrollment, plan_type
- fact_five_star: provider_ccn, state_code, overall_rating, health_inspection_rating
- fact_hospice_quality: provider_ccn, state_code, measure_id, score
- fact_medicare_enrollment: state_code, year, total_enrolled, ma_enrolled
- fact_opioid_prescribing: state_code, year, opioid_claim_count, opioid_prescribing_rate
- fact_block_grant: state_code, allotment DOUBLE (MHBG mental health block grants)

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
    model: str = "claude-sonnet-4-20250514"


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
async def nl2sql(req: NL2SQLRequest):
    """Translate natural language to SQL, execute against DuckDB, return results."""

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="NL2SQL not configured (missing API key)")

    client = anthropic.Anthropic(api_key=api_key)

    # Step 1: Ask Claude to generate SQL
    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
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
    "Which hospitals have the worst cost-to-charge ratios?",
    "What are the top behavioral health measures by state?",
]


@router.get("/examples")
async def get_examples():
    """Return example queries for the frontend."""
    return {"examples": EXAMPLES}
