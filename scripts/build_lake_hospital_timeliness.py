#!/usr/bin/env python3
"""
build_lake_hospital_timeliness.py — Ingest hospital timely & effective care measures into the Aradune data lake.

Reads from: data/raw/timely_effective_hospital.csv (CMS Hospital Compare)
Writes to:  data/lake/fact/hospital_timely_effective/snapshot={today}/data.parquet

Tables built:
  Facts:
    fact_hospital_timely_effective — Hospital timely & effective care quality measures
                                     (ED throughput, stroke, sepsis, immunization, etc.)

Usage:
  python3 scripts/build_lake_hospital_timeliness.py
  python3 scripts/build_lake_hospital_timeliness.py --dry-run
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

CSV_FILE = RAW_DIR / "timely_effective_hospital.csv"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


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


def build_fact_hospital_timely_effective(con, dry_run: bool) -> int:
    print("Building fact_hospital_timely_effective...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hospital_timely_effective AS
        SELECT
            "Facility ID"                       AS provider_ccn,
            "Facility Name"                     AS hospital_name,
            "State"                             AS state_code,
            "City/Town"                         AS city,
            "ZIP Code"                          AS zip_code,
            "County/Parish"                     AS county,
            "Condition"                         AS condition,
            "Measure ID"                        AS measure_id,
            "Measure Name"                      AS measure_name,
            TRY_CAST("Score" AS DOUBLE)         AS score,
            TRY_CAST("Sample" AS INTEGER)       AS sample_size,
            "Footnote"                          AS footnote,
            TRY_CAST("Start Date" AS DATE)      AS start_date,
            TRY_CAST("End Date" AS DATE)        AS end_date,
            'data.cms.gov/hospital-compare'     AS source,
            DATE '{SNAPSHOT_DATE}'              AS snapshot_date
        FROM read_csv_auto('{CSV_FILE}', all_varchar=true)
        WHERE "State" IS NOT NULL
          AND LENGTH(TRIM("State")) = 2
    """)

    count = write_parquet(
        con, "_fact_hospital_timely_effective",
        _snapshot_path("hospital_timely_effective"), dry_run
    )

    # Summary stats
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_hospital_timely_effective"
    ).fetchone()[0]
    hospitals = con.execute(
        "SELECT COUNT(DISTINCT provider_ccn) FROM _fact_hospital_timely_effective"
    ).fetchone()[0]
    measures = con.execute(
        "SELECT COUNT(DISTINCT measure_id) FROM _fact_hospital_timely_effective"
    ).fetchone()[0]
    conditions = con.execute(
        "SELECT COUNT(DISTINCT condition) FROM _fact_hospital_timely_effective"
    ).fetchone()[0]
    scored = con.execute(
        "SELECT COUNT(*) FROM _fact_hospital_timely_effective WHERE score IS NOT NULL"
    ).fetchone()[0]
    scored_pct = round(scored / count * 100, 1) if count > 0 else 0

    print(f"  {count:,} rows, {states} states, {hospitals:,} hospitals, "
          f"{measures} measures, {conditions} conditions")
    print(f"  {scored:,} rows with numeric score ({scored_pct}%)")

    # Top conditions
    top_conditions = con.execute("""
        SELECT condition, COUNT(*) AS n
        FROM _fact_hospital_timely_effective
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """).fetchall()
    print("  Top conditions:")
    for cond, n in top_conditions:
        print(f"    {cond}: {n:,}")

    con.execute("DROP TABLE IF EXISTS _fact_hospital_timely_effective")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Ingest hospital timely & effective care measures into Aradune lake"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not CSV_FILE.exists():
        print(f"ERROR: {CSV_FILE} not found", file=sys.stderr)
        print("Download from: https://data.cms.gov/provider-data/dataset/yv7e-xc69")
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Source:   {CSV_FILE.name}")
    print()

    con = duckdb.connect()
    totals = {}
    totals["fact_hospital_timely_effective"] = build_fact_hospital_timely_effective(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("HOSPITAL TIMELY & EFFECTIVE CARE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>10,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [str(CSV_FILE)],
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_hospital_timeliness_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
