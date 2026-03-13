#!/usr/bin/env python3
"""
build_lake_kff_1115.py -- Ingest KFF Section 1115 waiver tracking data.

Reads from:
  data/raw/kff_1115_approved_waivers.csv  (66 approved waivers, tab-delimited)
  data/raw/kff_1115_pending_waivers.csv   (34 pending waivers, tab-delimited)
  data/raw/kff_1115_work_requirements.csv (51 states, tab-delimited)

Writes to:
  data/lake/fact/kff_1115_waivers/data.parquet          -- Combined approved + pending
  data/lake/fact/kff_1115_work_requirements/data.parquet -- Work requirement status by state

KFF tracks 1115 waiver provisions (eligibility, benefits, SDOH, work requirements)
as a policy overlay -- complementary to the medicaid.gov waiver list.

Usage:
  python3 scripts/build_lake_kff_1115.py
  python3 scripts/build_lake_kff_1115.py --dry-run
"""

import argparse
import json
import re
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

STATE_CODES = {
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
}


def to_snake_case(name: str) -> str:
    s = name.strip().lstrip("\ufeff")
    s = re.sub(r"[\s\-/,]+", "_", s)
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def clean_state_name(raw: str) -> str:
    """Remove asterisks and extra whitespace from state names."""
    return raw.strip().rstrip("*").strip()


def state_to_code(name: str) -> str:
    """Map full state name to 2-letter code."""
    cleaned = clean_state_name(name)
    return STATE_CODES.get(cleaned, "")


def write_parquet(con, table: str, path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if not dry_run and count > 0:
        path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    elif dry_run:
        print(f"  [dry-run] ({count:,} rows)")
    return count


def build_waivers(con, dry_run: bool) -> int:
    """Combine approved + pending waivers into single table with status column."""
    print("Building fact_kff_1115_waivers...")

    approved_path = RAW_DIR / "kff_1115_approved_waivers.csv"
    pending_path = RAW_DIR / "kff_1115_pending_waivers.csv"

    if not approved_path.exists() or not pending_path.exists():
        print("  SKIPPED -- KFF waiver CSVs not found")
        return 0

    # Read approved (tab-delimited)
    con.execute(f"""
        CREATE TABLE _approved_raw AS
        SELECT * FROM read_csv_auto('{approved_path}', delim='\t', header=true, all_varchar=true)
    """)

    # Read pending (tab-delimited)
    con.execute(f"""
        CREATE TABLE _pending_raw AS
        SELECT * FROM read_csv_auto('{pending_path}', delim='\t', header=true, all_varchar=true)
    """)

    # Check columns
    approved_cols = [r[0] for r in con.execute("PRAGMA table_info('_approved_raw')").fetchall()]
    pending_cols = [r[0] for r in con.execute("PRAGMA table_info('_pending_raw')").fetchall()]
    print(f"  Approved columns: {approved_cols}")
    print(f"  Pending columns:  {pending_cols}")

    # Build approved table with normalized columns
    # Approved has: State, Waiver Name, Waiver Expiration Date, + provision flags
    con.execute("""
        CREATE TABLE _approved AS
        SELECT
            State AS state_raw,
            "Waiver Name" AS waiver_name,
            "Waiver Expiration Date" AS expiration_date_raw,
            NULL AS request_type,
            'approved' AS status,
            CASE WHEN "Expanded Eligibility Groups" = 'X' THEN true ELSE false END AS expanded_eligibility_groups,
            CASE WHEN "Other Eligibility/ Enrollment Expansions" = 'X' THEN true ELSE false END AS other_eligibility_expansions,
            CASE WHEN "Eligibility/ Enrollment Restrictions" = 'X' THEN true ELSE false END AS eligibility_restrictions,
            CASE WHEN COALESCE("Select Benefit Expansions", '') = 'X' THEN true ELSE false END AS benefit_expansions,
            CASE WHEN "Benefit Restrictions, Copays, Healthy Behaviors" = 'X' THEN true ELSE false END AS benefit_restrictions_copays,
            CASE WHEN "SDOH Provisions" = 'X' THEN true ELSE false END AS sdoh_provisions,
            CASE WHEN "Other Select DSRs" = 'X' THEN true ELSE false END AS other_select_dsrs
        FROM _approved_raw
        WHERE State != 'TOTAL' AND State IS NOT NULL AND TRIM(State) != ''
    """)

    # Build pending table with normalized columns
    # Pending has: State, Waiver Name, New/Amendment/Extension, + provision flags
    con.execute("""
        CREATE TABLE _pending AS
        SELECT
            State AS state_raw,
            "Waiver Name" AS waiver_name,
            NULL AS expiration_date_raw,
            "New, Amendment, Extension" AS request_type,
            'pending' AS status,
            CASE WHEN "Expanded Eligibility Groups" = 'X' THEN true ELSE false END AS expanded_eligibility_groups,
            CASE WHEN "Other Eligibility/ Enrollment Expansions" = 'X' THEN true ELSE false END AS other_eligibility_expansions,
            CASE WHEN "Eligibility/ Enrollment Restrictions" = 'X' THEN true ELSE false END AS eligibility_restrictions,
            CASE WHEN "Benefit Expansions" = 'X' THEN true ELSE false END AS benefit_expansions,
            CASE WHEN "Benefit Restrictions, Copays, Healthy Behaviors" = 'X' THEN true ELSE false END AS benefit_restrictions_copays,
            CASE WHEN "SDOH Provisions" = 'X' THEN true ELSE false END AS sdoh_provisions,
            CASE WHEN "Other Select DSRs" = 'X' THEN true ELSE false END AS other_select_dsrs
        FROM _pending_raw
        WHERE State != 'TOTAL' AND State IS NOT NULL AND TRIM(State) != ''
    """)

    # Union
    con.execute("""
        CREATE TABLE fact_kff_1115_waivers AS
        SELECT * FROM _approved
        UNION ALL
        SELECT * FROM _pending
    """)

    # Add state_code using Python UDF approach: update via CASE
    # First get distinct raw state names
    states_raw = [r[0] for r in con.execute("SELECT DISTINCT state_raw FROM fact_kff_1115_waivers").fetchall()]
    case_parts = []
    for s in states_raw:
        code = state_to_code(s)
        if code:
            case_parts.append(f"WHEN state_raw = '{s}' THEN '{code}'")

    case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

    con.execute(f"""
        CREATE TABLE _kff_final AS
        SELECT
            {case_expr} AS state_code,
            REPLACE(REPLACE(state_raw, '*', ''), '  ', ' ') AS state_name,
            waiver_name,
            status,
            request_type,
            TRY_STRPTIME(expiration_date_raw, '%m/%d/%Y')::DATE AS expiration_date,
            expanded_eligibility_groups,
            other_eligibility_expansions,
            eligibility_restrictions,
            benefit_expansions,
            benefit_restrictions_copays,
            sdoh_provisions,
            other_select_dsrs,
            'KFF Section 1115 Waiver Tracker' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM fact_kff_1115_waivers
    """)

    con.execute("DROP TABLE fact_kff_1115_waivers")
    con.execute("ALTER TABLE _kff_final RENAME TO fact_kff_1115_waivers")

    count = con.execute("SELECT COUNT(*) FROM fact_kff_1115_waivers").fetchone()[0]
    approved_ct = con.execute("SELECT COUNT(*) FROM fact_kff_1115_waivers WHERE status='approved'").fetchone()[0]
    pending_ct = con.execute("SELECT COUNT(*) FROM fact_kff_1115_waivers WHERE status='pending'").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_kff_1115_waivers WHERE state_code IS NOT NULL").fetchone()[0]

    print(f"  Total: {count:,} waivers ({approved_ct} approved, {pending_ct} pending)")
    print(f"  States: {states}")

    # Sample
    print("\n  Sample (5 rows):")
    sample = con.execute("""
        SELECT state_code, waiver_name, status, expiration_date
        FROM fact_kff_1115_waivers
        LIMIT 5
    """).fetchall()
    for row in sample:
        print(f"    {row[0]} | {row[1]:50s} | {row[2]:8s} | {row[3]}")

    out_path = FACT_DIR / "kff_1115_waivers" / "data.parquet"
    row_count = write_parquet(con, "fact_kff_1115_waivers", out_path, dry_run)

    # Cleanup
    for t in ("_approved_raw", "_pending_raw", "_approved", "_pending"):
        con.execute(f"DROP TABLE IF EXISTS {t}")

    return row_count


def build_work_requirements(con, dry_run: bool) -> int:
    """Build work requirements status table."""
    print("\nBuilding fact_kff_1115_work_requirements...")

    raw_path = RAW_DIR / "kff_1115_work_requirements.csv"
    if not raw_path.exists():
        print("  SKIPPED -- work requirements CSV not found")
        return 0

    con.execute(f"""
        CREATE TABLE _wr_raw AS
        SELECT * FROM read_csv_auto('{raw_path}', delim='\t', header=true, all_varchar=true)
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM _wr_raw").fetchone()[0]
    print(f"  Raw rows: {raw_count}")

    con.execute(f"""
        CREATE TABLE fact_kff_1115_work_requirements AS
        SELECT
            "State" AS state_code,
            COALESCE(NULLIF(TRIM("Status"), ''), 'No Action') AS work_requirement_status,
            CASE
                WHEN TRIM(COALESCE("Status", '')) = '' THEN false
                ELSE true
            END AS has_work_requirement_activity,
            'KFF Section 1115 Work Requirements Tracker' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _wr_raw
        WHERE "State" IS NOT NULL AND TRIM("State") != ''
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_kff_1115_work_requirements").fetchone()[0]
    active = con.execute("SELECT COUNT(*) FROM fact_kff_1115_work_requirements WHERE has_work_requirement_activity").fetchone()[0]
    print(f"  States: {count}, with activity: {active}")

    # Sample
    print("\n  Sample (active states):")
    sample = con.execute("""
        SELECT state_code, work_requirement_status
        FROM fact_kff_1115_work_requirements
        WHERE has_work_requirement_activity
        ORDER BY state_code
    """).fetchall()
    for row in sample:
        print(f"    {row[0]} | {row[1]}")

    out_path = FACT_DIR / "kff_1115_work_requirements" / "data.parquet"
    row_count = write_parquet(con, "fact_kff_1115_work_requirements", out_path, dry_run)

    con.execute("DROP TABLE IF EXISTS _wr_raw")
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Ingest KFF 1115 waiver tracking data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("KFF Section 1115 Waiver Tracker Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}

    totals["fact_kff_1115_waivers"] = build_waivers(con, args.dry_run)
    totals["fact_kff_1115_work_requirements"] = build_work_requirements(con, args.dry_run)

    con.close()

    # Manifest
    if not args.dry_run and sum(totals.values()) > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "script": "build_lake_kff_1115.py",
            "source": "https://www.kff.org/medicaid/issue-brief/section-1115-waiver-tracker/",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "completed_at": datetime.now().isoformat() + "Z",
        }
        manifest_path = META_DIR / f"manifest_kff_1115_{SNAPSHOT_DATE}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"\n  Manifest: {manifest_path.name}")

    print("\n" + "=" * 60)
    print("KFF 1115 INGESTION COMPLETE")
    for name, count in totals.items():
        print(f"  {name:45s} {count:>8,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
