#!/usr/bin/env python3
"""
build_lake_enrollment.py — Ingest Medicaid enrollment, eligibility, and unwinding data.

Reads from: data/raw/medicaid_eligibility_feb2026.csv
             data/raw/medicaid_new_adult_enrollment.csv
             data/raw/unwinding_hcgov.csv
             data/raw/managed_care_enrollment_2024.csv
Writes to:  data/lake/

Tables built:
  Facts:
    fact_eligibility       — Monthly applications, determinations, enrollment by state
    fact_new_adult         — ACA expansion (VIII group) enrollment by state/month
    fact_unwinding         — Post-PHE redetermination outcomes by state/month
    fact_mc_enrollment     — Managed care enrollment by plan, program, state (2024)

Usage:
  python3 scripts/build_lake_enrollment.py
  python3 scripts/build_lake_enrollment.py --dry-run
  python3 scripts/build_lake_enrollment.py --only fact_eligibility,fact_unwinding
"""

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STATE_NAME_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Puerto Rico': 'PR',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY',
}
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def _register_state_map(con):
    """Create a temp state name → code mapping table if it doesn't exist."""
    try:
        con.execute("SELECT 1 FROM _state_map LIMIT 1")
        return  # already exists
    except Exception:
        pass
    con.execute("CREATE TABLE _state_map (state_name VARCHAR, state_code VARCHAR)")
    con.executemany(
        "INSERT INTO _state_map VALUES (?, ?)",
        [(name, code) for name, code in STATE_NAME_TO_CODE.items()],
    )


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


# ---------------------------------------------------------------------------
# Eligibility & Enrollment (PI dataset)
# ---------------------------------------------------------------------------

def build_fact_eligibility(con, dry_run: bool) -> int:
    csv_path = RAW_DIR / "medicaid_eligibility_feb2026.csv"
    print("Building fact_eligibility...")
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_eligibility AS
        SELECT
            "State Abbreviation" AS state_code,
            "Reporting Period" AS reporting_period,
            CASE WHEN "State Expanded Medicaid" = 'Yes' THEN TRUE ELSE FALSE END AS expansion_state,
            "Preliminary or Updated" AS data_status,
            TRY_CAST("New Applications Submitted to Medicaid and CHIP Agencies" AS BIGINT) AS new_applications,
            TRY_CAST("Individuals Determined Eligible for Medicaid at Application" AS BIGINT) AS medicaid_eligible_at_app,
            TRY_CAST("Individuals Determined Eligible for CHIP at Application" AS BIGINT) AS chip_eligible_at_app,
            TRY_CAST("Total Medicaid and CHIP Determinations" AS BIGINT) AS total_determinations,
            TRY_CAST("Total Medicaid and CHIP Enrollment" AS BIGINT) AS total_medicaid_chip_enrollment,
            TRY_CAST("Total Medicaid Enrollment" AS BIGINT) AS total_medicaid_enrollment,
            TRY_CAST("Total CHIP Enrollment" AS BIGINT) AS total_chip_enrollment,
            TRY_CAST("Total Adult Medicaid Enrollment" AS BIGINT) AS adult_medicaid_enrollment,
            TRY_CAST("Medicaid and CHIP Child Enrollment" AS BIGINT) AS child_enrollment,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}')
        WHERE "State Abbreviation" IS NOT NULL
          AND LENGTH("State Abbreviation") = 2
    """)
    count = write_parquet(con, "_fact_eligibility", _snapshot_path("eligibility"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_eligibility").fetchone()[0]
    periods = con.execute("SELECT MIN(reporting_period), MAX(reporting_period) FROM _fact_eligibility").fetchone()
    print(f"  {count:,} rows, {states} states, {periods[0]} to {periods[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_eligibility")
    return count


# ---------------------------------------------------------------------------
# New Adult Group Enrollment (ACA Expansion)
# ---------------------------------------------------------------------------

def build_fact_new_adult(con, dry_run: bool) -> int:
    csv_path = RAW_DIR / "medicaid_new_adult_enrollment.csv"
    print("Building fact_new_adult...")
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    _register_state_map(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_new_adult AS
        SELECT
            sm.state_code,
            TRY_CAST("Enrollment Year" AS INTEGER) AS enrollment_year,
            TRY_CAST("Enrollment Month" AS INTEGER) AS enrollment_month,
            TRY_CAST("Total Medicaid Enrollees" AS BIGINT) AS total_medicaid_enrollees,
            TRY_CAST("Total VIII Group Enrollees" AS BIGINT) AS viii_group_enrollees,
            TRY_CAST("Total VIII Group Newly Eligible Enrollees" AS BIGINT) AS viii_newly_eligible,
            TRY_CAST("Total VIII Group Not Newly Eligible Enrollees" AS BIGINT) AS viii_not_newly_eligible,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}') r
        JOIN _state_map sm ON TRIM(r."State") = sm.state_name
    """)
    count = write_parquet(con, "_fact_new_adult", _snapshot_path("new_adult"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_new_adult").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_new_adult")
    return count


# ---------------------------------------------------------------------------
# Unwinding / Redetermination Outcomes
# ---------------------------------------------------------------------------

def build_fact_unwinding(con, dry_run: bool) -> int:
    csv_path = RAW_DIR / "unwinding_hcgov.csv"
    print("Building fact_unwinding...")
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    _register_state_map(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_unwinding AS
        SELECT
            sm.state_code,
            "Metric" AS metric,
            "Time Period" AS time_period,
            TRY_CAST("Count of individuals whose Medicaid or CHIP coverage was terminated in the month" AS BIGINT) AS terminated_count,
            TRY_CAST("Percentage of individuals whose Medicaid or CHIP coverage was terminated in the month" AS DOUBLE) AS terminated_pct,
            TRY_CAST("Cumulative Count of individuals whose Medicaid or CHIP coverage was terminated in all months" AS BIGINT) AS cumulative_terminated,
            TRY_CAST("Cumulative Percentage of individuals whose Medicaid or CHIP coverage was terminated in all months" AS DOUBLE) AS cumulative_terminated_pct,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}') r
        JOIN _state_map sm ON TRIM(r."State") = sm.state_name
    """)
    count = write_parquet(con, "_fact_unwinding", _snapshot_path("unwinding"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_unwinding").fetchone()[0]
    periods = con.execute("SELECT COUNT(DISTINCT time_period) FROM _fact_unwinding").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {periods} time periods")
    con.execute("DROP TABLE IF EXISTS _fact_unwinding")
    return count


# ---------------------------------------------------------------------------
# Managed Care Enrollment by Plan (2024)
# ---------------------------------------------------------------------------

def build_fact_mc_enrollment(con, dry_run: bool) -> int:
    csv_path = RAW_DIR / "managed_care_enrollment_2024.csv"
    print("Building fact_mc_enrollment...")
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mc_enrollment AS
        SELECT
            "State" AS state_code,
            "Program Name" AS program_name,
            "Plan Name" AS plan_name,
            "Parent Organization" AS parent_org,
            "Geographic Region" AS geographic_region,
            TRY_CAST("Medicaid-Only Enrollment" AS BIGINT) AS medicaid_only_enrollment,
            TRY_CAST("Dual Enrollment" AS BIGINT) AS dual_enrollment,
            TRY_CAST("Total Enrollment" AS BIGINT) AS total_enrollment,
            TRY_CAST("Year" AS INTEGER) AS year,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}')
        WHERE "State" IS NOT NULL
    """)
    count = write_parquet(con, "_fact_mc_enrollment", _snapshot_path("mc_enrollment"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_mc_enrollment").fetchone()[0]
    plans = con.execute("SELECT COUNT(DISTINCT plan_name) FROM _fact_mc_enrollment").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {plans:,} plans")
    con.execute("DROP TABLE IF EXISTS _fact_mc_enrollment")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_eligibility": build_fact_eligibility,
    "fact_new_adult": build_fact_new_adult,
    "fact_unwinding": build_fact_unwinding,
    "fact_mc_enrollment": build_fact_mc_enrollment,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest Medicaid enrollment/unwinding data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]
        invalid = [t for t in tables if t not in ALL_TABLES]
        if invalid:
            print(f"ERROR: Unknown tables: {invalid}", file=sys.stderr)
            sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("ENROLLMENT DATA LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>10,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source": "data.medicaid.gov",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_enrollment_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
