#!/usr/bin/env python3
"""
build_lake_hrsa_grants.py — Ingest HRSA grant data into the Aradune data lake.

Reads from: data/raw/hrsa_awarded_grants.csv  (83 MB, ~114K rows — all historical awards)
            data/raw/hrsa_active_grants.csv   (12 MB, ~25K rows — currently active grants)
Writes to:  data/lake/fact/hrsa_awarded_grants/snapshot={DATE}/data.parquet
            data/lake/fact/hrsa_active_grants/snapshot={DATE}/data.parquet

These datasets are broader than the existing fact_health_center_awards, which covers
only health center-specific awards. These cover ALL HRSA grant programs.

Source: HRSA Data Warehouse (https://data.hrsa.gov/data/download)
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

AWARDED_CSV = RAW_DIR / "hrsa_awarded_grants_clean.csv"
ACTIVE_CSV = RAW_DIR / "hrsa_active_grants_clean.csv"

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


def build_fact_hrsa_awarded_grants(con, dry_run: bool) -> int:
    """Build fact_hrsa_awarded_grants — all historical HRSA grant awards."""
    print("Building fact_hrsa_awarded_grants...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_awarded AS
        SELECT
            TRY_CAST("Award Year" AS INTEGER)           AS award_year,
            "Grantee Name"                              AS grantee_name,
            "Grantee Address"                           AS grantee_address,
            "Grantee City"                              AS grantee_city,
            "Grantee State Abbreviation"                AS state_code,
            "Grantee ZIP Code"                          AS zip_code,
            "Grant Activity Code"                       AS activity_code,
            "Grant Number"                              AS grant_number,
            "Grant Serial Number"                       AS serial_number,
            TRY_CAST("Project Period Start Date" AS DATE) AS project_start_date,
            TRY_CAST("Grant Project Period End Date" AS DATE) AS project_end_date,
            "HRSA Program Area Code"                    AS program_area_code,
            "HRSA Program Area Name"                    AS program_area_name,
            "Grant Program Name"                        AS grant_program_name,
            "Grantee Type Description"                  AS grantee_type,
            "Complete County Name"                      AS county_name,
            "State and County Federal Information Processing Standard Code" AS county_fips,
            "State FIPS Code"                           AS state_fips,
            TRY_CAST("Financial Assistance" AS DOUBLE)  AS financial_assistance,
            "CCN"                                       AS ccn,
            "Unique Entity Identifier"                  AS uei,
            TRY_CAST("Geocoding Artifact Address Primary X Coordinate" AS DOUBLE) AS longitude,
            TRY_CAST("Geocoding Artifact Address Primary Y Coordinate" AS DOUBLE) AS latitude,
            'data.hrsa.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{AWARDED_CSV}', sample_size=20000)
    """)

    count = write_parquet(con, "_fact_awarded",
                          _snapshot_path("hrsa_awarded_grants"), dry_run)

    stats = con.execute("""
        SELECT
            COUNT(DISTINCT state_code)       AS states,
            COUNT(DISTINCT grant_program_name) AS programs,
            MIN(award_year)                  AS min_year,
            MAX(award_year)                  AS max_year,
            SUM(financial_assistance)        AS total_funding
        FROM _fact_awarded
    """).fetchone()

    print(f"  {count:,} rows, {stats[0]} states, {stats[1]} grant programs")
    print(f"  Year range: {stats[2]}-{stats[3]}")
    if stats[4]:
        print(f"  Total funding: ${stats[4]:,.0f}")

    # Top 5 program areas
    top = con.execute("""
        SELECT program_area_name, COUNT(*) as cnt
        FROM _fact_awarded
        WHERE program_area_name IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """).fetchall()
    print("  Top program areas:")
    for row in top:
        print(f"    {row[0]}: {row[1]:,}")

    con.execute("DROP TABLE IF EXISTS _fact_awarded")
    return count


def build_fact_hrsa_active_grants(con, dry_run: bool) -> int:
    """Build fact_hrsa_active_grants — currently active HRSA grants."""
    print("Building fact_hrsa_active_grants...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_active AS
        SELECT
            TRY_CAST("Award Year" AS INTEGER)           AS award_year,
            "Grantee Name"                              AS grantee_name,
            "Grantee Address"                           AS grantee_address,
            "Grantee City"                              AS grantee_city,
            "Grantee State Abbreviation"                AS state_code,
            "Grantee ZIP Code"                          AS zip_code,
            "Grant Activity Code"                       AS activity_code,
            "Grant Number"                              AS grant_number,
            "Grant Serial Number"                       AS serial_number,
            TRY_CAST("Project Period Start Date" AS DATE) AS project_start_date,
            TRY_CAST("Grant Project Period End Date" AS DATE) AS project_end_date,
            "HRSA Program Area Code"                    AS program_area_code,
            "HRSA Program Area Name"                    AS program_area_name,
            "Grant Program Name"                        AS grant_program_name,
            "Uniform Data System Grant Program Description" AS uds_program_description,
            "Grantee Type Description"                  AS grantee_type,
            "Complete County Name"                      AS county_name,
            "U.S. - Mexico Border 100 Kilometer Indicator" AS border_100km,
            "U.S. - Mexico Border County Indicator"     AS border_county,
            "Unique Entity Identifier"                  AS uei,
            "Abstract"                                  AS abstract,
            TRY_CAST("Geocoding Artifact Address Primary X Coordinate" AS DOUBLE) AS longitude,
            TRY_CAST("Geocoding Artifact Address Primary Y Coordinate" AS DOUBLE) AS latitude,
            'data.hrsa.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{ACTIVE_CSV}', sample_size=20000)
    """)

    count = write_parquet(con, "_fact_active",
                          _snapshot_path("hrsa_active_grants"), dry_run)

    stats = con.execute("""
        SELECT
            COUNT(DISTINCT state_code)       AS states,
            COUNT(DISTINCT grant_program_name) AS programs,
            MIN(award_year)                  AS min_year,
            MAX(award_year)                  AS max_year
        FROM _fact_active
    """).fetchone()

    print(f"  {count:,} rows, {stats[0]} states, {stats[1]} grant programs")
    print(f"  Award years: {stats[2]}-{stats[3]}")

    con.execute("DROP TABLE IF EXISTS _fact_active")
    return count


ALL_TABLES = {
    "fact_hrsa_awarded_grants": build_fact_hrsa_awarded_grants,
    "fact_hrsa_active_grants": build_fact_hrsa_active_grants,
}


def main():
    parser = argparse.ArgumentParser(
        description="Ingest HRSA grant data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

    missing = []
    if "fact_hrsa_awarded_grants" in tables and not AWARDED_CSV.exists():
        missing.append(str(AWARDED_CSV))
    if "fact_hrsa_active_grants" in tables and not ACTIVE_CSV.exists():
        missing.append(str(ACTIVE_CSV))
    if missing:
        print(f"ERROR: Raw files not found: {missing}", file=sys.stderr)
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

    # Summary
    print("=" * 60)
    print("HRSA GRANTS INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>10,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [
                str(AWARDED_CSV) if AWARDED_CSV.exists() else None,
                str(ACTIVE_CSV) if ACTIVE_CSV.exists() else None,
            ],
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_hrsa_grants_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
