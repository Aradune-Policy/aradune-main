#!/usr/bin/env python3
"""
build_lake_chip_hcbs.py — Ingest CHIP enrollment and HCBS waiver data into the lake.

Sources:
  1. data.medicaid.gov CHIP enrollment (monthly, 10K rows)
  2. CMS 1915(c) waiver participant counts by state (T-MSIS derived, 451 rows)

Tables built:
  fact_chip_enrollment    — Monthly Medicaid/CHIP enrollment + applications by state
  fact_hcbs_waiver_enrollment — 1915(c) waiver participant counts by state

Usage:
  python3 scripts/build_lake_chip_hcbs.py
  python3 scripts/build_lake_chip_hcbs.py --dry-run
"""

import argparse
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def build_chip_enrollment(con, dry_run: bool) -> int:
    """Build fact_chip_enrollment from data.medicaid.gov monthly data."""
    print("Building fact_chip_enrollment...")
    csv_path = RAW_DIR / "chip_enrollment.csv"
    if not csv_path.exists():
        print("  SKIPPED — chip_enrollment.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_chip AS
        SELECT
            "State Abbreviation" AS state_code,
            "Reporting Period" AS reporting_period,
            "State Expanded Medicaid" AS expansion_state,
            "Preliminary or Updated" AS data_status,
            TRY_CAST(REPLACE("New Applications Submitted to Medicaid and CHIP Agencies", ',', '') AS INTEGER) AS new_applications,
            TRY_CAST(REPLACE("Individuals Determined Eligible for Medicaid at Application", ',', '') AS INTEGER) AS medicaid_determinations,
            TRY_CAST(REPLACE("Individuals Determined Eligible for CHIP at Application", ',', '') AS INTEGER) AS chip_determinations,
            TRY_CAST(REPLACE("Medicaid and CHIP Child Enrollment", ',', '') AS INTEGER) AS child_enrollment,
            TRY_CAST(REPLACE("Total Medicaid and CHIP Enrollment", ',', '') AS INTEGER) AS total_enrollment,
            TRY_CAST(REPLACE("Total Medicaid Enrollment", ',', '') AS INTEGER) AS medicaid_enrollment,
            TRY_CAST(REPLACE("Total CHIP Enrollment", ',', '') AS INTEGER) AS chip_enrollment,
            TRY_CAST(REPLACE("Total Adult Medicaid Enrollment", ',', '') AS INTEGER) AS adult_enrollment,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State Abbreviation" IS NOT NULL
          AND LENGTH("State Abbreviation") = 2
    """)

    count = write_parquet(con, "_fact_chip", _snapshot_path("chip_enrollment"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_chip").fetchone()[0]
    periods = con.execute("SELECT MIN(reporting_period), MAX(reporting_period) FROM _fact_chip").fetchone()

    # National totals for latest period
    latest = con.execute("""
        SELECT reporting_period,
               SUM(chip_enrollment) AS total_chip,
               SUM(medicaid_enrollment) AS total_medicaid
        FROM _fact_chip
        WHERE chip_enrollment IS NOT NULL
        GROUP BY reporting_period
        ORDER BY reporting_period DESC
        LIMIT 1
    """).fetchone()
    print(f"  {count:,} rows, {states} states, periods {periods[0]}-{periods[1]}")
    if latest:
        print(f"  Latest ({latest[0]}): CHIP {latest[1]:,} | Medicaid {latest[2]:,}")

    con.execute("DROP TABLE IF EXISTS _fact_chip")
    return count


def build_hcbs_waiver_enrollment(con, dry_run: bool) -> int:
    """Build fact_hcbs_waiver_enrollment from CMS 1915(c) data."""
    print("Building fact_hcbs_waiver_enrollment...")
    csv_path = RAW_DIR / "waiver_1915c_participants.csv"
    if not csv_path.exists():
        print("  SKIPPED — waiver_1915c_participants.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hcbs AS
        SELECT
            TRY_CAST("Year" AS INTEGER) AS year,
            "Geography" AS state_name,
            "Subpopulation topic" AS subpopulation_topic,
            "Subpopulation" AS subpopulation,
            "Category" AS category,
            TRY_CAST(REPLACE("Count of enrollees", ',', '') AS INTEGER) AS enrollee_count,
            TRY_CAST(REPLACE("Denominator count of enrollees", ',', '') AS INTEGER) AS denominator_count,
            TRY_CAST("Percentage of enrollees" AS DOUBLE) AS enrollee_pct,
            "Data version" AS data_version,
            'cms_1915c_waiver_participants' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "Geography" IS NOT NULL
          AND "Geography" != 'National'
    """)

    count = write_parquet(con, "_fact_hcbs", _snapshot_path("hcbs_waiver_enrollment"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_hcbs").fetchone()[0]
    years = con.execute("SELECT DISTINCT year FROM _fact_hcbs ORDER BY year").fetchall()

    # Top waiver states
    top = con.execute("""
        SELECT state_name, enrollee_count
        FROM _fact_hcbs
        WHERE category = 'Enrolled in 1915(c) waiver'
          AND year = (SELECT MAX(year) FROM _fact_hcbs)
        ORDER BY enrollee_count DESC
        LIMIT 5
    """).fetchall()
    print(f"  {count:,} rows, {states} states, years: {[y[0] for y in years]}")
    if top:
        print(f"  Top waiver states: {', '.join(f'{s[0]} ({s[1]:,})' for s in top)}")

    con.execute("DROP TABLE IF EXISTS _fact_hcbs")
    return count


ALL_TABLES = {
    "chip_enrollment": ("fact_chip_enrollment", build_chip_enrollment),
    "hcbs_waiver": ("fact_hcbs_waiver_enrollment", build_hcbs_waiver_enrollment),
}


def main():
    parser = argparse.ArgumentParser(description="Ingest CHIP and HCBS data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"CHIP / HCBS Data Ingestion — {SNAPSHOT_DATE}")
    print(f"{'='*60}")
    print(f"Run ID: {RUN_ID}\n")

    con = duckdb.connect()
    totals = {}

    tables_to_build = ALL_TABLES if args.table == "all" else {args.table: ALL_TABLES[args.table]}
    for key, (fact_name, builder) in tables_to_build.items():
        totals[fact_name] = builder(con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("CHIP / HCBS LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_chip_hcbs_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
