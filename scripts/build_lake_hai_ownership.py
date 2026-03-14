#!/usr/bin/env python3
"""
build_lake_hai_ownership.py — Ingest HAI hospital + NH ownership data.

Reads from: data/raw/hai_hospital.json (172K rows)
             data/raw/nh_ownership.json (144K rows)
Writes to:  data/lake/fact/hai_hospital/snapshot=YYYY-MM-DD/data.parquet
             data/lake/fact/nh_ownership/snapshot=YYYY-MM-DD/data.parquet

Usage:
  python3 scripts/build_lake_hai_ownership.py
  python3 scripts/build_lake_hai_ownership.py --dry-run
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


def build_hai_hospital(con, dry_run: bool) -> int:
    """Build HAI hospital fact table."""
    print("Building fact_hai_hospital...")
    json_path = RAW_DIR / "hai_hospital.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hai_hosp AS
        SELECT
            facility_id AS provider_id,
            facility_name,
            state AS state_code,
            zip_code,
            countyparish AS county,
            measure_id,
            measure_name,
            compared_to_national,
            TRY_CAST(score AS DOUBLE) AS score,
            footnote,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """)

    count = write_parquet(con, "_fact_hai_hosp", _snapshot_path("hai_hospital"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hai_hosp").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_hai_hosp").fetchone()[0]
    hospitals = con.execute("SELECT COUNT(DISTINCT provider_id) FROM _fact_hai_hosp").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {hospitals:,} hospitals, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _fact_hai_hosp")
    return count


def build_nh_ownership(con, dry_run: bool) -> int:
    """Build NH ownership fact table."""
    print("Building fact_nh_ownership...")
    json_path = RAW_DIR / "nh_ownership.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_nh_own AS
        SELECT
            cms_certification_number_ccn AS ccn,
            provider_name,
            state AS state_code,
            zip_code,
            role_played_by_owner_or_manager_in_facility AS role,
            owner_type,
            owner_name,
            TRY_CAST(REPLACE(ownership_percentage, '%', '') AS DOUBLE) AS ownership_pct,
            association_date,
            processing_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """)

    count = write_parquet(con, "_fact_nh_own", _snapshot_path("nh_ownership"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_nh_own").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _fact_nh_own").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {facilities:,} facilities")
    con.execute("DROP TABLE IF EXISTS _fact_nh_own")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest HAI + NH Ownership into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}
    totals['fact_hai_hospital'] = build_hai_hospital(con, args.dry_run)
    print()
    totals['fact_nh_ownership'] = build_nh_ownership(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("HAI + NH OWNERSHIP LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_hai_ownership_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
