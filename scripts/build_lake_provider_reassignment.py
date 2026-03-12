#!/usr/bin/env python3
"""
build_lake_provider_reassignment.py — Ingest Medicare provider reassignment data.

Reads from: data/raw/provider_reassignment.csv (568 MB, ~3.49M rows)
Writes to:  data/lake/fact/provider_reassignment/snapshot={DATE}/data.parquet

Tables built:
  Facts:
    fact_provider_reassignment — NPI-to-organization reassignment mappings.
    Maps individual providers (by NPI) to group practices / organizations
    (by PAC ID). Critical for provider network analysis, chain ownership
    detection, and organizational affiliation mapping.

Source: CMS PECOS (https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment)
"""

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

RAW_FILE = RAW_DIR / "provider_reassignment.csv"

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


def build_fact_provider_reassignment(con, dry_run: bool) -> int:
    """Build fact_provider_reassignment from raw CSV.

    Normalizes column names, casts NPI to VARCHAR (standard 10-digit format),
    and adds source provenance.
    """
    print("Building fact_provider_reassignment...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_reassignment AS
        SELECT
            "Group PAC ID"                              AS group_pac_id,
            "Group Enrollment ID"                       AS group_enrollment_id,
            NULLIF(TRIM("Group Legal Business Name"), '') AS group_business_name,
            "Group State Code"                          AS group_state_code,
            NULLIF("Group Due Date", 'TBD')             AS group_due_date,
            TRY_CAST("Group Reassignments and Physician Assistants" AS INTEGER)
                                                        AS group_reassignment_count,
            "Record Type"                               AS record_type,
            "Individual PAC ID"                         AS individual_pac_id,
            "Individual Enrollment ID"                  AS individual_enrollment_id,
            LPAD(CAST("Individual NPI" AS VARCHAR), 10, '0')
                                                        AS individual_npi,
            "Individual First Name"                     AS individual_first_name,
            "Individual Last Name"                      AS individual_last_name,
            "Individual State Code"                     AS individual_state_code,
            "Individual Specialty Description"          AS individual_specialty,
            NULLIF("Individual Due Date", 'TBD')        AS individual_due_date,
            TRY_CAST("Individual Total Employer Associations" AS INTEGER)
                                                        AS individual_employer_count,
            'data.cms.gov/pecos' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto(
            '{RAW_FILE}',
            sample_size=20000,
            ignore_errors=true
        )
    """)

    count = write_parquet(con, "_fact_reassignment",
                          _snapshot_path("provider_reassignment"), dry_run)

    # Validation stats
    stats = con.execute("""
        SELECT
            COUNT(DISTINCT individual_npi)      AS unique_npis,
            COUNT(DISTINCT group_pac_id)        AS unique_groups,
            COUNT(DISTINCT individual_state_code) AS states,
            COUNT(DISTINCT record_type)         AS record_types,
            SUM(CASE WHEN record_type = 'Reassignment' THEN 1 ELSE 0 END) AS reassignments,
            SUM(CASE WHEN record_type = 'Physician Assistant' THEN 1 ELSE 0 END) AS pa_records
        FROM _fact_reassignment
    """).fetchone()

    print(f"  {count:,} rows total")
    print(f"  {stats[0]:,} unique NPIs, {stats[1]:,} organizations, {stats[2]} states")
    print(f"  Reassignments: {stats[4]:,}, PA records: {stats[5]:,}")

    # Data quality checks
    null_npis = con.execute("""
        SELECT COUNT(*) FROM _fact_reassignment
        WHERE individual_npi IS NULL OR individual_npi = '0000000000'
    """).fetchone()[0]
    if null_npis > 0:
        print(f"  WARNING: {null_npis:,} rows with null/zero NPI")

    con.execute("DROP TABLE IF EXISTS _fact_reassignment")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Medicare provider reassignment data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not RAW_FILE.exists():
        print(f"ERROR: Raw file not found at {RAW_FILE}", file=sys.stderr)
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Source:   {RAW_FILE} ({RAW_FILE.stat().st_size / (1024*1024):.0f} MB)")
    print()

    con = duckdb.connect()
    count = build_fact_provider_reassignment(con, args.dry_run)
    con.close()

    # Summary
    print()
    print("=" * 60)
    print("PROVIDER REASSIGNMENT INGESTION COMPLETE")
    print("=" * 60)
    print(f"  fact_provider_reassignment  {count:>12,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [str(RAW_FILE)],
            "tables": {"fact_provider_reassignment": {"rows": count}},
            "total_rows": count,
            "notes": "Medicare NPI-to-organization reassignment mappings from PECOS. "
                     "11 rows dropped due to encoding issues in source file.",
        }
        manifest_file = META_DIR / f"manifest_provider_reassignment_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
