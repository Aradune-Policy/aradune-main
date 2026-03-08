#!/usr/bin/env python3
"""
build_lake_round3c.py — CMS Care Compare provider datasets.

Tables built:
  fact_dialysis_facility    — Dialysis facility listing with Five Star ratings (7.5K rows)
  fact_ipf_facility         — Inpatient Psychiatric Facility quality by facility (1.4K rows)
  fact_hospice_provider     — Hospice provider quality measures (465K rows)

Usage:
  python3 scripts/build_lake_round3c.py
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


def build_dialysis_facility(con, dry_run: bool) -> int:
    """Dialysis facility directory with Five Star quality ratings."""
    print("Building fact_dialysis_facility...")
    csv_path = RAW_DIR / "dialysis_facility.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dialysis AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Facility Name" AS facility_name,
            "State" AS state,
            "City/Town" AS city,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Network" AS network,
            TRY_CAST("Five Star" AS INTEGER) AS five_star,
            "Five Star Data Availability Code" AS five_star_availability,
            "Profit or Non-Profit" AS ownership_type,
            "Chain Owned" AS chain_owned,
            TRY_CAST("# of Dialysis Stations" AS INTEGER) AS dialysis_stations,
            "Offers in-center hemodialysis" AS offers_incenter_hd,
            "Offers peritoneal dialysis" AS offers_pd,
            "Offers home hemodialysis training" AS offers_home_hd,
            "Late Shift" AS offers_late_shift,
            TRY_CAST("Mortality Rate (Facility)" AS DOUBLE) AS mortality_rate,
            TRY_CAST("Hospitalization Rate (Facility)" AS DOUBLE) AS hospitalization_rate,
            TRY_CAST("Readmission Rate (Facility)" AS DOUBLE) AS readmission_rate,
            TRY_CAST("Fistula Rate (Facility)" AS DOUBLE) AS fistula_rate,
            "Patient Survival Category Text" AS survival_category,
            "Patient hospitalization category text" AS hospitalization_category,
            'cms_care_compare_dialysis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_fact_dialysis", _snapshot_path("dialysis_facility"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_dialysis").fetchone()[0]
    avg_star = con.execute("SELECT ROUND(AVG(five_star), 2) FROM _fact_dialysis WHERE five_star IS NOT NULL").fetchone()[0]
    print(f"  {count:,} facilities, {states} states, avg Five Star: {avg_star}")
    con.execute("DROP TABLE IF EXISTS _fact_dialysis")
    return count


def build_ipf_facility(con, dry_run: bool) -> int:
    """Inpatient Psychiatric Facility quality measures by facility."""
    print("Building fact_ipf_facility...")
    csv_path = RAW_DIR / "ipf_facility.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_ipf AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "State" AS state,
            "City/Town" AS city,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            TRY_CAST(CASE WHEN "HBIPS-2 Overall Rate Per 1000" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "HBIPS-2 Overall Rate Per 1000" END AS DOUBLE) AS hbips2_rate,
            TRY_CAST(CASE WHEN "HBIPS-3 Overall Rate Per 1000" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "HBIPS-3 Overall Rate Per 1000" END AS DOUBLE) AS hbips3_rate,
            TRY_CAST(CASE WHEN "SMD %" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "SMD %" END AS DOUBLE) AS smd_pct,
            TRY_CAST(CASE WHEN "SUB-2 %" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "SUB-2 %" END AS DOUBLE) AS sub2_pct,
            TRY_CAST(CASE WHEN "TOB-3 %" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "TOB-3 %" END AS DOUBLE) AS tob3_pct,
            TRY_CAST(CASE WHEN "IMM-2 %" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "IMM-2 %" END AS DOUBLE) AS imm2_pct,
            TRY_CAST(CASE WHEN "READM-30-IPF Rate" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "READM-30-IPF Rate" END AS DOUBLE) AS readm30_rate,
            "READM-30-IPF Category" AS readm30_category,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'cms_care_compare_ipf' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_fact_ipf", _snapshot_path("ipf_facility"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_ipf").fetchone()[0]
    print(f"  {count:,} facilities, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_ipf")
    return count


def build_hospice_provider(con, dry_run: bool) -> int:
    """Hospice provider quality measures from CMS Care Compare."""
    print("Building fact_hospice_provider...")
    csv_path = RAW_DIR / "hospice_providers.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hospice_prov AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Facility Name" AS facility_name,
            "State" AS state,
            "City/Town" AS city,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Measure Code" AS measure_code,
            "Measure Name" AS measure_name,
            TRY_CAST(CASE WHEN "Score" IN ('Not Available', '--', 'N/A', '') THEN NULL
                ELSE "Score" END AS DOUBLE) AS score,
            "Footnote" AS footnote,
            "Measure Date Range" AS measure_date_range,
            'cms_care_compare_hospice' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_fact_hospice_prov", _snapshot_path("hospice_provider"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _fact_hospice_prov").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_hospice_prov").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_hospice_prov").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} hospices, {states} states, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _fact_hospice_prov")
    return count


ALL_TABLES = {
    "dialysis": ("fact_dialysis_facility", build_dialysis_facility),
    "ipf": ("fact_ipf_facility", build_ipf_facility),
    "hospice": ("fact_hospice_provider", build_hospice_provider),
}


def main():
    parser = argparse.ArgumentParser(description="Round 3c lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 3c — CMS Care Compare Provider Data — {SNAPSHOT_DATE}")
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
    print("ROUND 3c LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round3c_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
