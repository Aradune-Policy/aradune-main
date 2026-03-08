#!/usr/bin/env python3
"""
build_lake_round7.py — CMS Tier 2 + HRSA facility data.

Tables built:
  fact_fqhc_directory         — HRSA health center sites (~18.8K rows)
  fact_hhcahps_provider       — HHCAHPS patient experience by home health agency (~12.2K rows)
  fact_hhcahps_state          — HHCAHPS state averages (~54 rows)
  fact_hospice_directory      — Hospice facility directory (~6.9K rows)
  fact_hospice_cahps_state    — Hospice CAHPS state measures (~1.3K rows)
  fact_medicare_spending_claim — Medicare spending by claim type per hospital (~63.6K rows)
  fact_vha_provider           — VHA hospital directory with ratings (~132 rows)

Usage:
  python3 scripts/build_lake_round7.py
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


def build_fqhc_directory(con, dry_run: bool) -> int:
    """HRSA FQHC health center sites. Uses Python csv due to trailing comma in header."""
    print("Building fact_fqhc_directory...")
    csv_path = RAW_DIR / "hrsa_health_centers.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute("""
        CREATE OR REPLACE TABLE _fact_fqhc (
            site_name VARCHAR, health_center_name VARCHAR,
            state_code VARCHAR, state_name VARCHAR,
            city VARCHAR, zip_code VARCHAR, address VARCHAR,
            telephone VARCHAR, web_address VARCHAR,
            operating_hours_per_week DOUBLE,
            location_setting VARCHAR, site_status VARCHAR,
            medicare_billing_number VARCHAR, npi VARCHAR,
            center_type VARCHAR, operator_type VARCHAR,
            schedule_type VARCHAR, operating_calendar VARCHAR,
            organization_type VARCHAR,
            longitude DOUBLE, latitude DOUBLE,
            county_name VARCHAR, hhs_region_code VARCHAR,
            hhs_region_name VARCHAR, congressional_district VARCHAR,
            source VARCHAR, snapshot_date DATE
        )
    """)

    def safe_float(v):
        if not v:
            return None
        try:
            return float(v.strip())
        except ValueError:
            return None

    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csvmod.DictReader(f)
        batch = []
        for row in reader:
            sc = row.get('Site State Abbreviation', '').strip()
            if not sc or len(sc) != 2:
                continue
            batch.append((
                row.get('Site Name', '').strip(),
                row.get('Health Center Name', '').strip(),
                sc,
                row.get('State Name', '').strip(),
                row.get('Site City', '').strip(),
                row.get('Site Postal Code', '').strip(),
                row.get('Site Address', '').strip(),
                row.get('Site Telephone Number', '').strip(),
                row.get('Site Web Address', '').strip(),
                safe_float(row.get('Operating Hours per Week', '')),
                row.get('Health Center Service Delivery Site Location Setting Description', '').strip(),
                row.get('Site Status Description', '').strip(),
                row.get('FQHC Site Medicare Billing Number', '').strip(),
                row.get('FQHC Site NPI Number', '').strip(),
                row.get('Health Center Type Description', '').strip(),
                row.get('Health Center Operator Description', '').strip(),
                row.get('Health Center Operational Schedule Description', '').strip(),
                row.get('Health Center Operating Calendar', '').strip(),
                row.get('Grantee Organization Type Description', '').strip(),
                safe_float(row.get('Geocoding Artifact Address Primary X Coordinate', '')),
                safe_float(row.get('Geocoding Artifact Address Primary Y Coordinate', '')),
                row.get('Complete County Name', '').strip(),
                row.get('HHS Region Code', '').strip(),
                row.get('HHS Region Name', '').strip(),
                row.get('Congressional District Name', '').strip(),
                'hrsa_bphc', SNAPSHOT_DATE,
            ))
            if len(batch) >= 2000:
                con.executemany("INSERT INTO _fact_fqhc VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)
                batch = []
        if batch:
            con.executemany("INSERT INTO _fact_fqhc VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", batch)

    count = write_parquet(con, "_fact_fqhc", _snapshot_path("fqhc_directory"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_fqhc").fetchone()[0]
    types = con.execute("SELECT DISTINCT center_type FROM _fact_fqhc ORDER BY center_type").fetchall()
    print(f"  {count:,} rows, {states} states, types: {[t[0] for t in types]}")
    con.execute("DROP TABLE IF EXISTS _fact_fqhc")
    return count


def build_hhcahps_provider(con, dry_run: bool) -> int:
    """HHCAHPS patient experience surveys by home health agency."""
    print("Building fact_hhcahps_provider...")
    csv_path = RAW_DIR / "hhcahps_provider.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hhcahps_prov AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            TRY_CAST("HHCAHPS Survey Summary Star Rating" AS INTEGER) AS summary_star_rating,
            TRY_CAST("Star Rating for health team gave care in a professional way" AS INTEGER) AS star_professional_care,
            TRY_CAST("Percent of patients who reported that their home health team gave care in a professional way" AS DOUBLE) AS pct_professional_care,
            TRY_CAST("Star Rating for health team communicated well with them" AS INTEGER) AS star_communication,
            TRY_CAST("Percent of patients who reported that their home health team communicated well with them" AS DOUBLE) AS pct_communication,
            TRY_CAST("Star Rating team discussed medicines, pain, and home safety" AS INTEGER) AS star_medicines_safety,
            TRY_CAST("Percent of patients who reported that their home health team discussed medicines, pain, and home safety with them" AS DOUBLE) AS pct_medicines_safety,
            TRY_CAST("Star Rating for how patients rated overall care from agency" AS INTEGER) AS star_overall_care,
            TRY_CAST("Percent of patients who gave their home health agency a rating of 9 or 10 on a scale from 0 (lowest) to 10 (highest)" AS DOUBLE) AS pct_high_rating,
            TRY_CAST("Percent of patients who reported YES, they would definitely recommend the home health agency to friends and family" AS DOUBLE) AS pct_would_recommend,
            TRY_CAST("Number of completed Surveys" AS INTEGER) AS completed_surveys,
            TRY_CAST("Survey response rate" AS DOUBLE) AS response_rate,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "CMS Certification Number (CCN)" IS NOT NULL
    """)

    count = write_parquet(con, "_fact_hhcahps_prov", _snapshot_path("hhcahps_provider"), dry_run)
    avg_star = con.execute("SELECT ROUND(AVG(summary_star_rating), 2) FROM _fact_hhcahps_prov WHERE summary_star_rating IS NOT NULL").fetchone()[0]
    print(f"  {count:,} rows, avg summary star: {avg_star}")
    con.execute("DROP TABLE IF EXISTS _fact_hhcahps_prov")
    return count


def build_hhcahps_state(con, dry_run: bool) -> int:
    """HHCAHPS state-level averages."""
    print("Building fact_hhcahps_state...")
    csv_path = RAW_DIR / "hhcahps_state.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hhcahps_st AS
        SELECT
            State AS state_code,
            TRY_CAST("Percent of patients who reported that their home health team gave care in a professional way" AS DOUBLE) AS pct_professional_care,
            TRY_CAST("Percent of patients who reported that their home health team communicated well with them" AS DOUBLE) AS pct_communication,
            TRY_CAST("Percent of patients who reported that their home health team discussed medicines, pain, and home safety with them" AS DOUBLE) AS pct_medicines_safety,
            TRY_CAST("Percent of patients who gave their home health agency a rating of 9 or 10 on a scale from 0 (lowest) to 10 (highest)" AS DOUBLE) AS pct_high_rating,
            TRY_CAST("Percent of patients who reported YES, they would definitely recommend the home health agency to friends and family" AS DOUBLE) AS pct_would_recommend,
            TRY_CAST("Number of completed Surveys" AS INTEGER) AS completed_surveys,
            TRY_CAST("Survey response rate" AS DOUBLE) AS response_rate,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) = 2
    """)

    count = write_parquet(con, "_fact_hhcahps_st", _snapshot_path("hhcahps_state"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _fact_hhcahps_st")
    return count


def build_hospice_directory(con, dry_run: bool) -> int:
    """Hospice facility directory."""
    print("Building fact_hospice_directory...")
    csv_path = RAW_DIR / "hospice_directory.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hospice_dir AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Facility Name" AS facility_name,
            "Address Line 1" AS address,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Telephone Number" AS telephone,
            "CMS Region" AS cms_region,
            "Ownership Type" AS ownership_type,
            "Certification Date" AS certification_date,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") = 2
    """)

    count = write_parquet(con, "_fact_hospice_dir", _snapshot_path("hospice_directory"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hospice_dir").fetchone()[0]
    ownership = con.execute("SELECT ownership_type, COUNT(*) FROM _fact_hospice_dir GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
    print(f"  {count:,} rows, {states} states, top ownership: {[(o[0], o[1]) for o in ownership]}")
    con.execute("DROP TABLE IF EXISTS _fact_hospice_dir")
    return count


def build_hospice_cahps_state(con, dry_run: bool) -> int:
    """Hospice CAHPS state-level quality measures."""
    print("Building fact_hospice_cahps_state...")
    csv_path = RAW_DIR / "hospice_cahps_state.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hospice_cahps AS
        SELECT
            "State" AS state_code,
            "Measure Code" AS measure_code,
            "Measure Name" AS measure_name,
            TRY_CAST("Score" AS DOUBLE) AS score,
            "Footnote" AS footnote,
            "Date" AS measure_period,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") >= 2
    """)

    count = write_parquet(con, "_fact_hospice_cahps", _snapshot_path("hospice_cahps_state"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hospice_cahps").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_hospice_cahps").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _fact_hospice_cahps")
    return count


def build_medicare_spending_claim(con, dry_run: bool) -> int:
    """Medicare spending by claim type per hospital."""
    print("Building fact_medicare_spending_claim...")
    csv_path = RAW_DIR / "medicare_spending_by_claim.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mspd AS
        SELECT
            "Facility Name" AS facility_name,
            "Facility ID" AS facility_id,
            "State" AS state_code,
            "Period" AS period,
            "Claim Type" AS claim_type,
            TRY_CAST("Avg Spndg Per EP Hospital" AS DOUBLE) AS avg_spending_hospital,
            TRY_CAST("Avg Spndg Per EP State" AS DOUBLE) AS avg_spending_state,
            TRY_CAST("Avg Spndg Per EP National" AS DOUBLE) AS avg_spending_national,
            REPLACE("Percent of Spndg Hospital", '%', '') AS pct_spending_hospital,
            REPLACE("Percent of Spndg State", '%', '') AS pct_spending_state,
            REPLACE("Percent of Spndg National", '%', '') AS pct_spending_national,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") = 2
    """)

    count = write_parquet(con, "_fact_mspd", _snapshot_path("medicare_spending_claim"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_mspd").fetchone()[0]
    claim_types = con.execute("SELECT COUNT(DISTINCT claim_type) FROM _fact_mspd").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {claim_types} claim types")
    con.execute("DROP TABLE IF EXISTS _fact_mspd")
    return count


def build_vha_provider(con, dry_run: bool) -> int:
    """VHA (Veterans Health Administration) hospital directory with ratings."""
    print("Building fact_vha_provider...")
    csv_path = RAW_DIR / "vha_providers.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_vha AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "Address" AS address,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Telephone Number" AS telephone,
            "Hospital Type" AS hospital_type,
            "Hospital Ownership" AS ownership,
            "Emergency Services" AS emergency_services,
            TRY_CAST("Hospital overall rating" AS INTEGER) AS overall_rating,
            "Hospital overall rating footnote" AS rating_footnote,
            'cms_care_compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") = 2
    """)

    count = write_parquet(con, "_fact_vha", _snapshot_path("vha_provider"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_vha").fetchone()[0]
    avg_rating = con.execute("SELECT ROUND(AVG(overall_rating), 2) FROM _fact_vha WHERE overall_rating IS NOT NULL").fetchone()[0]
    print(f"  {count:,} rows, {states} states, avg rating: {avg_rating}")
    con.execute("DROP TABLE IF EXISTS _fact_vha")
    return count


ALL_TABLES = {
    "fqhc": ("fact_fqhc_directory", build_fqhc_directory),
    "hhcahps_provider": ("fact_hhcahps_provider", build_hhcahps_provider),
    "hhcahps_state": ("fact_hhcahps_state", build_hhcahps_state),
    "hospice_dir": ("fact_hospice_directory", build_hospice_directory),
    "hospice_cahps": ("fact_hospice_cahps_state", build_hospice_cahps_state),
    "spending_claim": ("fact_medicare_spending_claim", build_medicare_spending_claim),
    "vha": ("fact_vha_provider", build_vha_provider),
}


def main():
    parser = argparse.ArgumentParser(description="Round 7 lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 7 — CMS Tier 2 + HRSA — {SNAPSHOT_DATE}")
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
    print("ROUND 7 LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round7_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
