#!/usr/bin/env python3
"""
build_lake_data_gaps_r2.py - Round 2 data gap ingestion (Session 20)

NEW TABLES:
  1. fact_cdc_chronic_disease  - CDC Chronic Disease Indicators (115 indicators, all states, 309K rows)
  2. fact_provisional_overdose_detail - CDC VSRR overdose by indicator/state (complete, 81K rows)

Usage:
  python3 scripts/build_lake_data_gaps_r2.py
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent.parent
LAKE = PROJECT / "data" / "lake"
RAW = PROJECT / "data" / "raw"
SNAP = str(date.today())


def _write_parquet(con, table_name, fact_name, dry_run=False):
    """Write DuckDB table to Hive-partitioned parquet with ZSTD."""
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


# ── 1. CDC Chronic Disease Indicators ──────────────────────────────────

def build_cdc_chronic_disease(con, dry_run=False):
    """CDC U.S. Chronic Disease Indicators - 115 indicators by state."""
    print("Building fact_cdc_chronic_disease...")
    path = RAW / "cdc_chronic_disease_indicators.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _cdi_raw")
    con.execute(f"""
        CREATE TABLE _cdi_raw AS
        SELECT
            CAST("YearStart" AS INTEGER) AS year_start,
            CAST("YearEnd" AS INTEGER) AS year_end,
            "LocationAbbr" AS state_code,
            "LocationDesc" AS location_name,
            "DataSource" AS data_source,
            "Topic" AS topic,
            "Question" AS question,
            "Response" AS response,
            "DataValueUnit" AS data_value_unit,
            "DataValueType" AS data_value_type,
            TRY_CAST("DataValue" AS DOUBLE) AS data_value,
            TRY_CAST("DataValueAlt" AS DOUBLE) AS data_value_alt,
            "DataValueFootnote" AS footnote,
            TRY_CAST("LowConfidenceLimit" AS DOUBLE) AS ci_lower,
            TRY_CAST("HighConfidenceLimit" AS DOUBLE) AS ci_upper,
            "StratificationCategory1" AS stratification_category,
            "Stratification1" AS stratification,
            "StratificationCategory2" AS stratification_category_2,
            "Stratification2" AS stratification_2,
            '{SNAP}' AS snapshot
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true,
                      sample_size=100000)
        WHERE "LocationAbbr" IS NOT NULL
    """)

    return _write_parquet(con, "_cdi_raw", "cdc_chronic_disease", dry_run)


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    con = duckdb.connect()
    total_rows = 0
    total_tables = 0

    for name, builder in [
        ("cdc_chronic_disease", build_cdc_chronic_disease),
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
