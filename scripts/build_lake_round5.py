#!/usr/bin/env python3
"""
build_lake_round5.py — CHIP enrollment, children's health, and renewal processing.

Tables built:
  fact_chip_enrollment_unwinding — CHIP enrollment during CAA/unwinding (779 rows)
  fact_chip_program_monthly      — Medicaid vs CHIP enrollment by month/state (13K rows)
  fact_medicaid_applications     — Applications, determinations, enrollment (10K rows)
  fact_vaccinations              — Vaccinations for children under 19 (47K rows)
  fact_blood_lead_screening      — Blood lead screening ages 1-2 (3K rows)
  fact_renewal_processing        — Eligibility renewal processing during unwinding (3K rows)

Usage:
  python3 scripts/build_lake_round5.py
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


def build_chip_enrollment_unwinding(con, dry_run: bool) -> int:
    """CHIP enrollment by month and state during CAA/unwinding period."""
    print("Building fact_chip_enrollment_unwinding...")
    csv_path = RAW_DIR / "chip_enrollment_unwinding.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    # CSV has BOM and quoted numbers with commas — use Python csv
    con.execute("""
        CREATE OR REPLACE TABLE _fact_chip_unwind (
            state_name VARCHAR, reporting_period VARCHAR,
            release_date VARCHAR, enrollment INTEGER,
            data_notes VARCHAR,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csvmod.DictReader(f)
        for row in reader:
            state = row.get('State', '').strip()
            if not state or len(state) < 2:
                continue
            val = row.get('Value ', row.get('Value', '')).replace(',', '').replace(' ', '').strip()
            enrollment = int(val) if val.isdigit() else None
            con.execute("INSERT INTO _fact_chip_unwind VALUES (?,?,?,?,?,?,?)", [
                state,
                row.get('Reporting period', '').strip(),
                row.get('Release date', '').strip(),
                enrollment,
                row.get('Data notes', '').strip(),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ])

    count = write_parquet(con, "_fact_chip_unwind", _snapshot_path("chip_enrollment_unwinding"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_chip_unwind").fetchone()[0]
    periods = con.execute("SELECT MIN(reporting_period), MAX(reporting_period) FROM _fact_chip_unwind").fetchone()
    print(f"  {count:,} rows, {states} states, period: {periods[0]} to {periods[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_chip_unwind")
    return count


def build_chip_program_monthly(con, dry_run: bool) -> int:
    """Medicaid vs CHIP enrollment by program type, month, and state."""
    print("Building fact_chip_program_monthly...")
    csv_path = RAW_DIR / "medicaid_chip_program_monthly.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute("""
        CREATE OR REPLACE TABLE _fact_chip_prog (
            state_name VARCHAR, reporting_month VARCHAR,
            program_type VARCHAR, enrolled_count INTEGER,
            data_quality VARCHAR,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        batch = []
        for row in reader:
            state = row.get('State', '').strip()
            if not state or len(state) < 2:
                continue
            enrolled = row.get('CountEnrolled', '').replace(',', '').replace(' ', '').strip()
            enrolled_int = int(enrolled) if enrolled.isdigit() else None
            batch.append((
                state,
                row.get('Month', '').strip(),
                row.get('ProgramType', '').strip(),
                enrolled_int,
                row.get('DQUnusable', '').strip(),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ))
            if len(batch) >= 1000:
                con.executemany("INSERT INTO _fact_chip_prog VALUES (?,?,?,?,?,?,?)", batch)
                batch = []
        if batch:
            con.executemany("INSERT INTO _fact_chip_prog VALUES (?,?,?,?,?,?,?)", batch)

    count = write_parquet(con, "_fact_chip_prog", _snapshot_path("chip_program_monthly"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_chip_prog").fetchone()[0]
    programs = con.execute("SELECT DISTINCT program_type FROM _fact_chip_prog ORDER BY program_type").fetchall()
    print(f"  {count:,} rows, {states} states, programs: {[p[0] for p in programs]}")
    con.execute("DROP TABLE IF EXISTS _fact_chip_prog")
    return count


def build_medicaid_applications(con, dry_run: bool) -> int:
    """Medicaid/CHIP applications, eligibility determinations, and enrollment."""
    print("Building fact_medicaid_applications...")
    csv_path = RAW_DIR / "medicaid_chip_applications.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    # Wide CSV with many columns — extract key metrics
    con.execute("""
        CREATE OR REPLACE TABLE _fact_apps (
            state_code VARCHAR, state_name VARCHAR,
            reporting_period VARCHAR, expansion_state VARCHAR,
            preliminary_or_updated VARCHAR, final_report VARCHAR,
            new_applications INTEGER,
            total_medicaid_enrollment INTEGER,
            total_chip_enrollment INTEGER,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        for row in reader:
            state_code = row.get('State Abbreviation', '').strip()
            if not state_code or len(state_code) != 2:
                continue

            def parse_int(v):
                if not v:
                    return None
                v = v.replace(',', '').replace(' ', '').strip()
                return int(v) if v.isdigit() else None

            con.execute("INSERT INTO _fact_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
                state_code,
                row.get('State Name', '').strip(),
                row.get('Reporting Period', '').strip(),
                row.get('State Expanded Medicaid', '').strip(),
                row.get('Preliminary or Updated', '').strip(),
                row.get('Final Report', '').strip(),
                parse_int(row.get('New Applications Submitted to Medicaid and CHIP Agencies', '')),
                parse_int(row.get('Total Medicaid Enrollment', '')),
                parse_int(row.get('Total CHIP Enrollment', '')),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ])

    count = write_parquet(con, "_fact_apps", _snapshot_path("medicaid_applications"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_apps").fetchone()[0]
    periods = con.execute("SELECT MIN(reporting_period), MAX(reporting_period) FROM _fact_apps").fetchone()
    print(f"  {count:,} rows, {states} states, period: {periods[0]} to {periods[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_apps")
    return count


def build_vaccinations(con, dry_run: bool) -> int:
    """Vaccinations provided to Medicaid/CHIP children under 19."""
    print("Building fact_vaccinations...")
    csv_path = RAW_DIR / "medicaid_vaccinations.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_vax AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            VaccineType AS vaccine_type,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_vax", _snapshot_path("vaccinations"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_vax").fetchone()[0]
    types = con.execute("SELECT COUNT(DISTINCT vaccine_type) FROM _fact_vax").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {types} vaccine types")
    con.execute("DROP TABLE IF EXISTS _fact_vax")
    return count


def build_blood_lead_screening(con, dry_run: bool) -> int:
    """Blood lead screening services for Medicaid/CHIP beneficiaries ages 1-2."""
    print("Building fact_blood_lead_screening...")
    csv_path = RAW_DIR / "medicaid_blood_lead_screening.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_lead AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_lead", _snapshot_path("blood_lead_screening"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_lead").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_lead")
    return count


def build_renewal_processing(con, dry_run: bool) -> int:
    """Medicaid/CHIP eligibility renewal processing during unwinding."""
    print("Building fact_renewal_processing...")
    csv_path = RAW_DIR / "medicaid_renewal_processing.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute("""
        CREATE OR REPLACE TABLE _fact_renewal (
            state_code VARCHAR, state_name VARCHAR,
            reporting_period VARCHAR,
            original_or_updated VARCHAR,
            renewals_initiated INTEGER,
            renewals_due INTEGER,
            renewals_completed INTEGER,
            determined_eligible INTEGER,
            determined_ineligible INTEGER,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        for row in reader:
            state_code = row.get('State Abbreviation', '').strip()
            if not state_code or len(state_code) != 2:
                continue

            def parse_int(v):
                if not v:
                    return None
                v = v.replace(',', '').replace(' ', '').strip()
                return int(v) if v.isdigit() else None

            con.execute("INSERT INTO _fact_renewal VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
                state_code,
                row.get('State Name', '').strip(),
                row.get('Reporting Period', '').strip(),
                row.get('Original or Updated', '').strip(),
                parse_int(row.get('Beneficiaries with a Renewal Initiated', '')),
                parse_int(row.get('Beneficiaries with a Renewal Due', '')),
                parse_int(row.get('Beneficiaries with a Renewal Completed', '')),
                parse_int(row.get('Beneficiaries Determined Eligible at Renewal', '')),
                parse_int(row.get('Beneficiaries Determined Ineligible at Renewal', '')),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ])

    count = write_parquet(con, "_fact_renewal", _snapshot_path("renewal_processing"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_renewal").fetchone()[0]
    periods = con.execute("SELECT MIN(reporting_period), MAX(reporting_period) FROM _fact_renewal").fetchone()
    print(f"  {count:,} rows, {states} states, period: {periods[0]} to {periods[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_renewal")
    return count


ALL_TABLES = {
    "chip_unwinding": ("fact_chip_enrollment_unwinding", build_chip_enrollment_unwinding),
    "chip_monthly": ("fact_chip_program_monthly", build_chip_program_monthly),
    "applications": ("fact_medicaid_applications", build_medicaid_applications),
    "vaccinations": ("fact_vaccinations", build_vaccinations),
    "lead_screening": ("fact_blood_lead_screening", build_blood_lead_screening),
    "renewals": ("fact_renewal_processing", build_renewal_processing),
}


def main():
    parser = argparse.ArgumentParser(description="Round 5 lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 5 — CHIP + Children's Health + Renewals — {SNAPSHOT_DATE}")
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
    print("ROUND 5 LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round5_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
