#!/usr/bin/env python3
"""
build_lake_round4.py — Additional T-MSIS utilization + CMS quality datasets.

Tables built:
  fact_health_screenings    — Health screenings for children under 19 (3,180 rows)
  fact_acute_care           — Acute care services by condition (25,440 rows)
  fact_perinatal_care       — Perinatal care services ages 15-44 (12,720 rows)
  fact_mc_summary           — Managed care enrollment summary (513 rows)
  fact_asc_facility         — ASC quality measures by facility (5,712 rows)

Usage:
  python3 scripts/build_lake_round4.py
"""

import argparse
import csv as csvmod
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


def build_health_screenings(con, dry_run: bool) -> int:
    """Health screenings for Medicaid/CHIP children under 19."""
    print("Building fact_health_screenings...")
    csv_path = RAW_DIR / "medicaid_health_screenings.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_screen AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            ScreeningService AS screening_service,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_screen", _snapshot_path("health_screenings"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_screen").fetchone()[0]
    services = con.execute("SELECT COUNT(DISTINCT screening_service) FROM _fact_screen").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {services} screening types")
    con.execute("DROP TABLE IF EXISTS _fact_screen")
    return count


def build_acute_care(con, dry_run: bool) -> int:
    """Acute care services by condition for Medicaid/CHIP population."""
    print("Building fact_acute_care...")
    csv_path = RAW_DIR / "medicaid_acute_care.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_acute AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            Condition AS condition,
            AcuteCareService AS service_type,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_acute", _snapshot_path("acute_care"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_acute").fetchone()[0]
    conditions = con.execute("SELECT COUNT(DISTINCT condition) FROM _fact_acute").fetchone()[0]
    services = con.execute("SELECT COUNT(DISTINCT service_type) FROM _fact_acute").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {conditions} conditions, {services} service types")
    con.execute("DROP TABLE IF EXISTS _fact_acute")
    return count


def build_perinatal_care(con, dry_run: bool) -> int:
    """Perinatal care services for Medicaid/CHIP beneficiaries ages 15-44."""
    print("Building fact_perinatal_care...")
    csv_path = RAW_DIR / "medicaid_perinatal_care.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_perinatal AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            PerinatalCareType AS care_type,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_perinatal", _snapshot_path("perinatal_care"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_perinatal").fetchone()[0]
    types = con.execute("SELECT COUNT(DISTINCT care_type) FROM _fact_perinatal").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {types} care types")
    con.execute("DROP TABLE IF EXISTS _fact_perinatal")
    return count


def build_mc_summary(con, dry_run: bool) -> int:
    """Managed care enrollment summary by state."""
    print("Building fact_mc_summary...")
    csv_path = RAW_DIR / "medicaid_mc_summary.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    # CSV has quoted numbers with commas — use Python csv module
    con.execute("""
        CREATE OR REPLACE TABLE _fact_mc_sum (
            state_name VARCHAR, year INTEGER,
            total_enrollees INTEGER,
            any_mc_enrollment INTEGER,
            comprehensive_mc_enrollment INTEGER,
            new_adult_comprehensive_mco INTEGER,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        for row in reader:
            state = row.get('State', '').strip()
            if not state or len(state) < 2 or state == 'TOTALS':
                continue
            def parse_int(v):
                v = v.replace(',', '').replace(' ', '').strip() if v else ''
                return int(v) if v and v.isdigit() else None
            year = row.get('Year', '').strip()
            if not year or not year.isdigit():
                continue
            con.execute("INSERT INTO _fact_mc_sum VALUES (?,?,?,?,?,?,?,?)", [
                state, int(year),
                parse_int(row.get('Total Medicaid Enrollees', '')),
                parse_int(row.get('Total Medicaid Enrollment in Any Type of Managed Care', '')),
                parse_int(row.get('Medicaid Enrollment in Comprehensive Managed Care', '')),
                parse_int(row.get('Medicaid Newly Eligible Adults Enrolled in Comprehensive MCOs', '')),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ])

    count = write_parquet(con, "_fact_mc_sum", _snapshot_path("mc_summary"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_mc_sum").fetchone()[0]
    years = con.execute("SELECT DISTINCT year FROM _fact_mc_sum ORDER BY year").fetchall()
    print(f"  {count} rows, {states} states, years: {[y[0] for y in years]}")
    con.execute("DROP TABLE IF EXISTS _fact_mc_sum")
    return count


def build_asc_facility(con, dry_run: bool) -> int:
    """Ambulatory Surgical Center quality measures by facility."""
    print("Building fact_asc_facility...")
    csv_path = RAW_DIR / "asc_facility.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_asc AS
        SELECT
            "Facility Name" AS facility_name,
            "Facility ID" AS facility_id,
            "NPI" AS npi,
            "City/Town" AS city,
            "State" AS state,
            "ZIP Code" AS zip_code,
            TRY_CAST("Year" AS INTEGER) AS year,
            TRY_CAST(CASE WHEN "ASC-1 Rate*" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "ASC-1 Rate*" END AS DOUBLE) AS asc1_rate,
            TRY_CAST(CASE WHEN "ASC-2 Rate*" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "ASC-2 Rate*" END AS DOUBLE) AS asc2_rate,
            TRY_CAST(CASE WHEN "ASC-3 Rate*" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "ASC-3 Rate*" END AS DOUBLE) AS asc3_rate,
            TRY_CAST(CASE WHEN "ASC-4 Rate*" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "ASC-4 Rate*" END AS DOUBLE) AS asc4_rate,
            'cms_care_compare_asc' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_fact_asc", _snapshot_path("asc_facility"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT facility_id) FROM _fact_asc").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_asc").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} ASCs, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_asc")
    return count


ALL_TABLES = {
    "screenings": ("fact_health_screenings", build_health_screenings),
    "acute": ("fact_acute_care", build_acute_care),
    "perinatal": ("fact_perinatal_care", build_perinatal_care),
    "mc_summary": ("fact_mc_summary", build_mc_summary),
    "asc": ("fact_asc_facility", build_asc_facility),
}


def main():
    parser = argparse.ArgumentParser(description="Round 4 lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 4 — T-MSIS Utilization + CMS Quality — {SNAPSHOT_DATE}")
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
    print("ROUND 4 LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_round4_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
