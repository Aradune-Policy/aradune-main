#!/usr/bin/env python3
"""
build_lake_data_gaps_r3.py - Round 3 data gap ingestion (Session 20)

NEW TABLES:
  1. fact_cdc_underlying_cod    - CDC underlying cause of death by state (2012-2022, 10.8K rows)
  2. fact_cdc_behavioral_risk   - CDC BRFSS nutrition/physical/mental health by state (148K rows)

Usage:
  python3 scripts/build_lake_data_gaps_r3.py
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent.parent
LAKE = PROJECT / "data" / "lake"
RAW = PROJECT / "data" / "raw"
SNAP = str(date.today())

STATE_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}


def _write_parquet(con, table_name, fact_name, dry_run=False):
    cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    out_dir = LAKE / "fact" / fact_name / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    if not dry_run and cnt > 0:
        con.execute(
            f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  {fact_name}: {cnt:,} rows ({size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {fact_name}: {cnt:,} rows")
    return cnt


def build_cdc_underlying_cod(con, dry_run=False):
    """CDC underlying cause of death by state (NCHS, 2012-2022)."""
    print("Building fact_cdc_underlying_cod...")
    path = RAW / "cdc_underlying_cod_state.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _cod")
    con.execute(f"""
        CREATE TABLE _cod AS
        SELECT
            CAST("year" AS INTEGER) AS year,
            "state" AS state_name,
            "cause_name" AS cause_name,
            "_113_cause_name" AS icd10_cause_detail,
            TRY_CAST("deaths" AS INTEGER) AS deaths,
            TRY_CAST("aadr" AS DOUBLE) AS age_adjusted_death_rate,
            '{SNAP}' AS snapshot
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true)
        WHERE "state" IS NOT NULL AND "state" != 'United States'
    """)

    # Add state_code
    con.execute("ALTER TABLE _cod ADD COLUMN state_code VARCHAR")
    for name, code in STATE_MAP.items():
        con.execute(f"UPDATE _cod SET state_code = '{code}' WHERE state_name = '{name}'")

    return _write_parquet(con, "_cod", "cdc_underlying_cod", dry_run)


def build_cdc_behavioral_risk(con, dry_run=False):
    """CDC BRFSS behavioral risk factor data by state (nutrition, physical activity,
    obesity, mental health, smoking, etc.)."""
    print("Building fact_cdc_behavioral_risk...")
    path = RAW / "cdc_mental_health_brfss.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    # First check what columns exist
    con.execute(f"""
        CREATE OR REPLACE TABLE _brfss_raw AS
        SELECT * FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true,
                      sample_size=100000) LIMIT 0
    """)
    raw_cols = [c[0] for c in con.execute("SELECT * FROM _brfss_raw").description]

    # Build select list with only existing columns
    select_parts = [
        'CAST("yearstart" AS INTEGER) AS year_start',
        'CAST("yearend" AS INTEGER) AS year_end',
        '"locationabbr" AS state_code',
        '"locationdesc" AS state_name',
        '"class" AS topic_class',
        '"topic" AS topic',
        '"question" AS question',
        '"data_value_unit" AS value_unit',
        '"data_value_type" AS value_type',
        'TRY_CAST("data_value" AS DOUBLE) AS data_value',
        'TRY_CAST("low_confidence_limit" AS DOUBLE) AS ci_lower',
        'TRY_CAST("high_confidence_limit" AS DOUBLE) AS ci_upper',
        'TRY_CAST("sample_size" AS INTEGER) AS sample_size',
    ]
    # Add optional columns
    optional = {
        "total": "stratification_total",
        "age_years": "age_group",
        "education": "education_level",
        "gender": "sex",
        "income": "income_level",
        "race_ethnicity": "race_ethnicity",
    }
    for orig, renamed in optional.items():
        if orig in raw_cols:
            select_parts.append(f'"{orig}" AS {renamed}')

    select_parts.append(f"'{SNAP}' AS snapshot")

    con.execute("DROP TABLE IF EXISTS _brfss")
    con.execute(f"""
        CREATE TABLE _brfss AS
        SELECT {', '.join(select_parts)}
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true,
                      sample_size=100000)
        WHERE "locationabbr" IS NOT NULL AND LENGTH("locationabbr") = 2
    """)
    con.execute("DROP TABLE IF EXISTS _brfss_raw")

    return _write_parquet(con, "_brfss", "cdc_behavioral_risk", dry_run)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect()
    total_rows = 0
    total_tables = 0

    for name, builder in [
        ("cdc_underlying_cod", build_cdc_underlying_cod),
        ("cdc_behavioral_risk", build_cdc_behavioral_risk),
    ]:
        try:
            rows = builder(con, args.dry_run)
            if rows > 0:
                total_tables += 1
                total_rows += rows
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    print(f"\nDone: {total_tables} tables, {total_rows:,} total rows")
    con.close()


if __name__ == "__main__":
    main()
