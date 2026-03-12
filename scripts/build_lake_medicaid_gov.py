#!/usr/bin/env python3
"""
build_lake_medicaid_gov.py — Download and ingest datasets from data.medicaid.gov
that are NOT yet in the Aradune data lake.

Datasets ingested:
  fact/drug_amp_monthly         — 3.4M rows, Drug AMP reporting status by NDC/month (2017+)
  fact/drug_amp_quarterly       — 2.1M rows, Drug AMP reporting status by NDC/quarter (2013+)
  fact/covid_testing            — 3,180 rows, COVID-19 testing services by state/month
  fact/prematurity_smm          — 435 rows, Prematurity & SMM among Medicaid deliveries
  fact/clotting_factor          — 500 rows, Clotting factor drug report by NDC
  fact/exclusive_pediatric      — 262 rows, Exclusive pediatric drug list
  fact/medicaid_enterprise      — 68 rows, MES compliance datatable (policy deadlines)
  fact/first_time_nadac         — 1,269 rows, First-time NADAC rates
  fact/drug_mfr_contacts        — 839 rows, Drug rebate program manufacturer contacts
  fact/hcgov_transitions        — 59,527 rows, HC.gov transitions during Medicaid unwinding
  fact/chip_unwinding_separate  — 780 rows, Separate CHIP enrollment unwinding
  fact/dual_status_yearly       — 1,113 rows, Dual status info yearly
  fact/benefit_package_yearly   — 1,484 rows, Benefit package yearly
  fact/express_lane_eligibility — 15 rows, Express lane eligibility by state

Usage:
  python3 scripts/build_lake_medicaid_gov.py
  python3 scripts/build_lake_medicaid_gov.py --dry-run
  python3 scripts/build_lake_medicaid_gov.py --only drug_amp_monthly,covid_testing
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
SNAPSHOT_DATE = date.today().isoformat()

# State name → 2-letter code mapping
STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Guam": "GU", "Virgin Islands": "VI",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
    "United States": "US", "National": "US",
}

# ──────────────────────────────────────────────────────────────────────
# Dataset registry: dataset_identifier → (table_name, description)
# ──────────────────────────────────────────────────────────────────────
DATASETS = {
    "drug_amp_monthly": {
        "dataset_id": "91d4309d-3ca8-5a1e-8f78-79984027392d",
        "desc": "Drug AMP Reporting - Monthly (status by NDC, 2017+)",
        "total": 3_400_944,
    },
    "drug_amp_quarterly": {
        "dataset_id": "80956a7d-e343-54f3-94a7-45d41b34fc0b",
        "desc": "Drug AMP Reporting - Quarterly (status by NDC, 2013+)",
        "total": 2_126_520,
    },
    "covid_testing": {
        "dataset_id": "c1c4b0cf-c957-4b00-bcf4-c0905045a5b3",
        "desc": "COVID-19 Testing Services by state/month",
        "total": 3_180,
    },
    "prematurity_smm": {
        "dataset_id": "ee3b9534-0d19-4c1b-bf74-43f898d5de7c",
        "desc": "Prematurity & SMM among Medicaid/CHIP deliveries",
        "total": 435,
    },
    "clotting_factor": {
        "dataset_id": "f45f35c5-7aa4-4500-b196-ae7833717add",
        "desc": "Clotting Factor Drug Report",
        "total": 500,
    },
    "exclusive_pediatric": {
        "dataset_id": "a54d7605-b780-4cf0-b53d-50313798f528",
        "desc": "Exclusive Pediatric Drugs",
        "total": 262,
    },
    "medicaid_enterprise": {
        "dataset_id": "13a06cdb-6bbb-4f86-bba7-9d6f3db41090",
        "desc": "Medicaid Enterprise System Datatable (policy compliance deadlines)",
        "total": 68,
    },
    "first_time_nadac": {
        "dataset_id": "e3af839d-8175-5be0-b94e-4a302ed7a035",
        "desc": "First Time NADAC Rates",
        "total": 1_269,
    },
    "drug_mfr_contacts": {
        "dataset_id": "9fcb14ec-d5f0-536e-9938-3f0024531e5b",
        "desc": "Drug Manufacturer Contacts (Medicaid Drug Rebate Program)",
        "total": 839,
    },
    "hcgov_transitions": {
        "dataset_id": "5636a78c-fe18-4229-aee1-e40fa910a8a0",
        "desc": "HealthCare.gov Transitions Marketplace Medicaid Unwinding Report",
        "total": 59_527,
    },
    "chip_unwinding_separate": {
        "dataset_id": "d30cfc7c-4b32-4df1-b2bf-e0a850befd77",
        "desc": "Separate CHIP Enrollment by Month and State (Unwinding Period)",
        "total": 780,
    },
    "dual_status_yearly": {
        "dataset_id": "93b36a8e-4dd5-4ff4-9a8b-8c6537684705",
        "desc": "Dual Status Information for Medicaid/CHIP Beneficiaries by Year",
        "total": 1_113,
    },
    "benefit_package_yearly": {
        "dataset_id": "50f83c5a-6fa9-4e91-b36c-3d3e225c905f",
        "desc": "Benefit Package for Medicaid/CHIP Beneficiaries by Year",
        "total": 1_484,
    },
    "express_lane_eligibility": {
        "dataset_id": "601a8897-1453-5282-81cd-be49d7ec7503",
        "desc": "Express Lane Eligibility for Medicaid and CHIP Coverage",
        "total": 15,
    },
    "nam_cahps": {
        "dataset_id": "2b18f2f7-d0f3-5efe-afc4-4881fcbdf200",
        "desc": "NAM CAHPS 2014 Public Use File (Medicaid enrollee experience survey)",
        "total": 272_679,
    },
    "pharmacy_releases": {
        "dataset_id": "0d425780-16be-4ded-8420-69def8f4ee29",
        "desc": "Division of Pharmacy Releases Index (CMS pharmacy policy releases)",
        "total": 2_416,
    },
    "drug_rebate_state_contacts": {
        "dataset_id": "46a5d780-feef-521a-af7b-25119ec3dc09",
        "desc": "Medicaid Drug Rebate Program State Contact Information",
        "total": 52,
    },
}

PAGE_SIZE = 5000
MAX_RETRIES = 3


def fetch_all_rows(dataset_id: str, expected_total: int) -> list[dict]:
    """Fetch all rows from a data.medicaid.gov dataset using paginated curl requests."""
    all_rows = []
    offset = 0
    while True:
        url = f"https://data.medicaid.gov/api/1/datastore/query/{dataset_id}/0?limit={PAGE_SIZE}&offset={offset}"
        for attempt in range(MAX_RETRIES):
            try:
                result = subprocess.run(
                    ["curl", "-s", "--max-time", "120", url],
                    capture_output=True, text=True, timeout=180
                )
                data = json.loads(result.stdout)
                rows = data.get("results", [])
                break
            except (json.JSONDecodeError, subprocess.TimeoutExpired) as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"    Retry {attempt + 1}/{MAX_RETRIES} (offset={offset}): {e}")
                    time.sleep(2 ** attempt)
                else:
                    print(f"    FAILED after {MAX_RETRIES} retries at offset={offset}")
                    return all_rows

        if not rows:
            break
        all_rows.extend(rows)

        if len(all_rows) >= expected_total or len(rows) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        # Progress for large datasets
        if expected_total > 10000 and offset % 50000 == 0:
            pct = min(100, len(all_rows) * 100 // expected_total)
            print(f"    ... {len(all_rows):,} / {expected_total:,} ({pct}%)")

    return all_rows


def add_state_code(rows: list[dict], state_col: str = "state") -> list[dict]:
    """Add state_code column by mapping state names to 2-letter codes."""
    for row in rows:
        raw = row.get(state_col, "").strip()
        code = STATE_NAME_TO_CODE.get(raw)
        if not code:
            # Try stripping footnote numbers
            import re
            clean = re.sub(r'\d+$', '', raw).strip()
            code = STATE_NAME_TO_CODE.get(clean, "")
        row["state_code"] = code
    return rows


def write_parquet(con: duckdb.DuckDBPyConnection, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to ZSTD Parquet. Returns row count."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
        return count
    if count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    return count


def snapshot_path(table_name: str) -> Path:
    return FACT_DIR / table_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


# ──────────────────────────────────────────────────────────────────────
# Individual table builders
# ──────────────────────────────────────────────────────────────────────

def build_drug_amp_monthly(con, rows, dry_run) -> int:
    """Drug AMP Reporting Monthly: NDC + labeler + status + year/month."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _drug_amp_monthly AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE drug_amp_monthly AS
        SELECT
            TRIM(labeler_name) AS labeler_name,
            TRIM(ndc) AS ndc,
            TRIM(fda_product_name) AS product_name,
            TRIM(status) AS amp_status,
            TRY_CAST(year AS INTEGER) AS year,
            TRY_CAST(month AS INTEGER) AS month,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _drug_amp_monthly
        WHERE ndc IS NOT NULL AND TRIM(ndc) != ''
    """)
    return write_parquet(con, "drug_amp_monthly", snapshot_path("drug_amp_monthly"), dry_run)


def build_drug_amp_quarterly(con, rows, dry_run) -> int:
    """Drug AMP Reporting Quarterly: NDC + labeler + status + year/quarter."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _drug_amp_quarterly AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE drug_amp_quarterly AS
        SELECT
            TRIM(labeler_name) AS labeler_name,
            TRIM(ndc) AS ndc,
            TRIM(fda_product_name) AS product_name,
            TRIM(status) AS amp_status,
            TRY_CAST(year AS INTEGER) AS year,
            TRY_CAST(quarter AS INTEGER) AS quarter,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _drug_amp_quarterly
        WHERE ndc IS NOT NULL AND TRIM(ndc) != ''
    """)
    return write_parquet(con, "drug_amp_quarterly", snapshot_path("drug_amp_quarterly"), dry_run)


def build_covid_testing(con, rows, dry_run) -> int:
    """COVID-19 testing services by state/month."""
    print(f"  Fetched {len(rows):,} rows")
    rows = add_state_code(rows)
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _covid_testing AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE covid_testing AS
        SELECT
            state_code,
            TRIM(state) AS state_name,
            TRY_CAST(year AS INTEGER) AS year,
            TRIM(month) AS month_str,
            TRIM(covidscreeningtype) AS screening_type,
            TRY_CAST(REPLACE(servicecount, ',', '') AS BIGINT) AS service_count,
            TRY_CAST(REPLACE(rateper1000beneficiaries, ',', '') AS DOUBLE) AS rate_per_1000,
            TRIM(dataquality) AS data_quality,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _covid_testing
    """)
    return write_parquet(con, "covid_testing", snapshot_path("covid_testing"), dry_run)


def build_prematurity_smm(con, rows, dry_run) -> int:
    """Prematurity & SMM among Medicaid/CHIP deliveries."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _prematurity AS SELECT * FROM df")
    # Add state_code mapping for geography column
    rows = add_state_code(rows, state_col="geography")
    df2 = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _prematurity2 AS SELECT * FROM df2")
    con.execute("""
        CREATE OR REPLACE TABLE prematurity_smm AS
        SELECT
            state_code,
            TRIM(geography) AS geography,
            TRY_CAST(year AS INTEGER) AS year,
            TRIM(subpopulation_topic) AS subpopulation_topic,
            TRIM(subpopulation) AS subpopulation,
            TRIM(category) AS category,
            TRY_CAST(REPLACE(count_of_deliveries, ',', '') AS BIGINT) AS delivery_count,
            TRY_CAST(REPLACE(denominator_count_of_deliveries, ',', '') AS BIGINT) AS denominator,
            TRY_CAST(rate_of_deliveries AS DOUBLE) AS rate_per_1000,
            TRIM(data_version) AS data_version,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _prematurity2
    """)
    return write_parquet(con, "prematurity_smm", snapshot_path("prematurity_smm"), dry_run)


def build_clotting_factor(con, rows, dry_run) -> int:
    """Clotting factor drug report by NDC."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _clotting AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE clotting_factor AS
        SELECT
            TRIM(CAST(ndc_1 AS VARCHAR)) AS ndc_segment1,
            TRIM(CAST(ndc_2 AS VARCHAR)) AS ndc_segment2,
            TRIM(CAST(ndc_3 AS VARCHAR)) AS ndc_segment3,
            CONCAT(TRIM(CAST(ndc_1 AS VARCHAR)), TRIM(CAST(ndc_2 AS VARCHAR)), TRIM(CAST(ndc_3 AS VARCHAR))) AS ndc,
            TRIM(CAST(labeler_name AS VARCHAR)) AS labeler_name,
            TRIM(CAST(product_name AS VARCHAR)) AS product_name,
            TRIM(CAST(effective_quarter AS VARCHAR)) AS effective_quarter,
            TRIM(CAST(termination_date AS VARCHAR)) AS termination_date,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _clotting
    """)
    return write_parquet(con, "clotting_factor", snapshot_path("clotting_factor"), dry_run)


def build_exclusive_pediatric(con, rows, dry_run) -> int:
    """Exclusive pediatric drug list."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _excl_ped AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE exclusive_pediatric AS
        SELECT
            TRIM(CAST(ndc_1 AS VARCHAR)) AS ndc_segment1,
            TRIM(CAST(ndc_2 AS VARCHAR)) AS ndc_segment2,
            TRIM(CAST(ndc_3 AS VARCHAR)) AS ndc_segment3,
            CONCAT(TRIM(CAST(ndc_1 AS VARCHAR)), TRIM(CAST(ndc_2 AS VARCHAR)), TRIM(CAST(ndc_3 AS VARCHAR))) AS ndc,
            TRIM(CAST(labeler_name AS VARCHAR)) AS labeler_name,
            TRIM(CAST(product_name AS VARCHAR)) AS product_name,
            TRIM(CAST(effective_quarter AS VARCHAR)) AS effective_quarter,
            TRIM(CAST(termination_date AS VARCHAR)) AS termination_date,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _excl_ped
    """)
    return write_parquet(con, "exclusive_pediatric", snapshot_path("exclusive_pediatric"), dry_run)


def build_medicaid_enterprise(con, rows, dry_run) -> int:
    """MES compliance datatable with policy deadlines."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _mes AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE medicaid_enterprise AS
        SELECT
            TRY_CAST(implementation_year AS INTEGER) AS implementation_year,
            TRIM(policyupdate_type) AS policy_type,
            TRIM(policyupdate_title) AS policy_title,
            TRIM(potential_mes_impact) AS mes_impact,
            TRIM(statute_or_regulation_citation) AS regulation_citation,
            TRIM(compliance_month) AS compliance_month,
            TRIM(compliance_day) AS compliance_day,
            TRIM(type_of_potential_system_impact) AS system_impact_type,
            TRIM(category_of_potential_impact) AS impact_category,
            TRIM(link_to__source_document) AS source_url,
            TRIM(impact_to_medicaid_chip_andor_bhp) AS program_impact,
            TRIM(tshirt_size) AS complexity_size,
            TRIM(certification_activities_required) AS cert_required,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _mes
    """)
    return write_parquet(con, "medicaid_enterprise", snapshot_path("medicaid_enterprise"), dry_run)


def build_first_time_nadac(con, rows, dry_run) -> int:
    """First-time NADAC rates."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _ft_nadac AS SELECT * FROM df")
    # Get columns dynamically since we don't know exact schema
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _ft_nadac)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE first_time_nadac AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _ft_nadac
    """)
    return write_parquet(con, "first_time_nadac", snapshot_path("first_time_nadac"), dry_run)


def build_drug_mfr_contacts(con, rows, dry_run) -> int:
    """Drug manufacturer contacts for Medicaid Drug Rebate Program."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _mfr_contacts AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _mfr_contacts)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE drug_mfr_contacts AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _mfr_contacts
    """)
    return write_parquet(con, "drug_mfr_contacts", snapshot_path("drug_mfr_contacts"), dry_run)


def build_hcgov_transitions(con, rows, dry_run) -> int:
    """Healthcare.gov transitions during Medicaid unwinding."""
    print(f"  Fetched {len(rows):,} rows")
    rows = add_state_code(rows)
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _hcgov AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE hcgov_transitions AS
        SELECT
            state_code,
            TRIM(state) AS state_name,
            TRIM(data_record_note) AS record_note,
            TRIM(metric) AS metric,
            TRIM(CAST(time_period AS VARCHAR)) AS time_period,
            TRIM(CAST(release_through AS VARCHAR)) AS release_through,
            TRY_CAST(REPLACE(count_of_individuals_whose_medicaid_or_chip_coverage_was_te_dd11, ',', '') AS BIGINT) AS count_terminated,
            TRIM(percentage_of_individuals_whose_medicaid_or_chip_coverage_w_da6e) AS pct_terminated,
            TRY_CAST(REPLACE(cumulative_count_of_individuals_whose_medicaid_or_chip_cove_28d9, ',', '') AS BIGINT) AS cumulative_count,
            TRIM(cumulative_percentage_of_individuals_whose_medicaid_or_chip_6ac2) AS cumulative_pct,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _hcgov
    """)
    return write_parquet(con, "hcgov_transitions", snapshot_path("hcgov_transitions"), dry_run)


def build_chip_unwinding_separate(con, rows, dry_run) -> int:
    """Separate CHIP enrollment by month/state during unwinding."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _chip_unwind AS SELECT * FROM df")
    # Add state_code
    rows = add_state_code(rows)
    df2 = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _chip_unwind2 AS SELECT * FROM df2")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _chip_unwind2)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE chip_unwinding_separate AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source_system,
            CURRENT_DATE AS snapshot_date_val
        FROM _chip_unwind2
    """)
    return write_parquet(con, "chip_unwinding_separate", snapshot_path("chip_unwinding_separate"), dry_run)


def build_dual_status_yearly(con, rows, dry_run) -> int:
    """Dual status information by year."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _dual_yr AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _dual_yr)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE dual_status_yearly AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _dual_yr
    """)
    return write_parquet(con, "dual_status_yearly", snapshot_path("dual_status_yearly"), dry_run)


def build_benefit_package_yearly(con, rows, dry_run) -> int:
    """Benefit package for Medicaid/CHIP by year."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _bp_yr AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _bp_yr)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE benefit_package_yearly AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _bp_yr
    """)
    return write_parquet(con, "benefit_package_yearly", snapshot_path("benefit_package_yearly"), dry_run)


def build_express_lane_eligibility(con, rows, dry_run) -> int:
    """Express lane eligibility by state."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _express AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _express)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE express_lane_eligibility AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _express
    """)
    return write_parquet(con, "express_lane_eligibility", snapshot_path("express_lane_eligibility"), dry_run)


def build_nam_cahps(con, rows, dry_run) -> int:
    """NAM CAHPS 2014 Public Use File - Medicaid enrollee experience survey."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _nam_cahps AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _nam_cahps)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE nam_cahps AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _nam_cahps
    """)
    return write_parquet(con, "nam_cahps", snapshot_path("nam_cahps"), dry_run)


def build_pharmacy_releases(con, rows, dry_run) -> int:
    """Division of Pharmacy Releases Index."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _pharm_rel AS SELECT * FROM df")
    con.execute("""
        CREATE OR REPLACE TABLE pharmacy_releases AS
        SELECT
            TRIM(CAST(type_of_release AS VARCHAR)) AS release_type,
            TRIM(CAST(release_no AS VARCHAR)) AS release_number,
            TRIM(CAST(release_date AS VARCHAR)) AS release_date,
            TRIM(CAST(description AS VARCHAR)) AS description,
            TRIM(CAST(keywords AS VARCHAR)) AS keywords,
            'data_medicaid_gov' AS source,
            CURRENT_DATE AS snapshot_date
        FROM _pharm_rel
    """)
    return write_parquet(con, "pharmacy_releases", snapshot_path("pharmacy_releases"), dry_run)


def build_drug_rebate_state_contacts(con, rows, dry_run) -> int:
    """Drug Rebate Program State Contact Info."""
    print(f"  Fetched {len(rows):,} rows")
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _rebate_contacts AS SELECT * FROM df")
    cols = con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _rebate_contacts)").fetchall()
    col_names = [c[0] for c in cols]
    select_parts = []
    for c in col_names:
        safe = c.replace('"', '""')
        select_parts.append(f'TRIM(CAST("{safe}" AS VARCHAR)) AS "{safe}"')
    select_sql = ", ".join(select_parts)
    con.execute(f"""
        CREATE OR REPLACE TABLE drug_rebate_state_contacts AS
        SELECT {select_sql},
            'data_medicaid_gov' AS source_system,
            CURRENT_DATE AS snapshot_date_val
        FROM _rebate_contacts
    """)
    return write_parquet(con, "drug_rebate_state_contacts", snapshot_path("drug_rebate_state_contacts"), dry_run)


# ──────────────────────────────────────────────────────────────────────
# Builder dispatch
# ──────────────────────────────────────────────────────────────────────
BUILDERS = {
    "drug_amp_monthly": build_drug_amp_monthly,
    "drug_amp_quarterly": build_drug_amp_quarterly,
    "covid_testing": build_covid_testing,
    "prematurity_smm": build_prematurity_smm,
    "clotting_factor": build_clotting_factor,
    "exclusive_pediatric": build_exclusive_pediatric,
    "medicaid_enterprise": build_medicaid_enterprise,
    "first_time_nadac": build_first_time_nadac,
    "drug_mfr_contacts": build_drug_mfr_contacts,
    "hcgov_transitions": build_hcgov_transitions,
    "chip_unwinding_separate": build_chip_unwinding_separate,
    "dual_status_yearly": build_dual_status_yearly,
    "benefit_package_yearly": build_benefit_package_yearly,
    "express_lane_eligibility": build_express_lane_eligibility,
    "nam_cahps": build_nam_cahps,
    "pharmacy_releases": build_pharmacy_releases,
    "drug_rebate_state_contacts": build_drug_rebate_state_contacts,
}


def main():
    parser = argparse.ArgumentParser(description="Download and ingest data.medicaid.gov datasets")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing files")
    parser.add_argument("--only", type=str, default="", help="Comma-separated list of table names to build")
    parser.add_argument("--skip-large", action="store_true", help="Skip datasets > 100K rows (drug_amp_monthly/quarterly)")
    args = parser.parse_args()

    tables_to_build = list(BUILDERS.keys())
    if args.only:
        tables_to_build = [t.strip() for t in args.only.split(",")]
    if args.skip_large:
        tables_to_build = [t for t in tables_to_build if DATASETS[t]["total"] < 100_000]

    con = duckdb.connect()

    results = {}
    total_rows = 0
    start_time = time.time()

    for table_name in tables_to_build:
        if table_name not in DATASETS:
            print(f"Unknown table: {table_name}")
            continue

        info = DATASETS[table_name]
        print(f"\n{'='*60}")
        print(f"Building {table_name}: {info['desc']}")
        print(f"  Expected: ~{info['total']:,} rows")

        # Check if already exists
        out_path = snapshot_path(table_name)
        if out_path.exists() and not args.dry_run:
            existing = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
            print(f"  Already exists with {existing:,} rows, skipping (delete to rebuild)")
            results[table_name] = existing
            total_rows += existing
            continue

        # Fetch data
        print(f"  Downloading from data.medicaid.gov...")
        rows = fetch_all_rows(info["dataset_id"], info["total"])
        if not rows:
            print(f"  ERROR: No rows fetched!")
            results[table_name] = 0
            continue

        # Build
        builder = BUILDERS[table_name]
        count = builder(con, rows, args.dry_run)
        results[table_name] = count
        total_rows += count

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"SUMMARY — {len(results)} tables, {total_rows:,} total rows, {elapsed:.1f}s")
    print(f"{'='*60}")
    for name, count in results.items():
        status = "[OK]" if count > 0 else "[EMPTY]"
        print(f"  {status} {name}: {count:,} rows")


if __name__ == "__main__":
    main()
