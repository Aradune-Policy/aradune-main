#!/usr/bin/env python3
"""
build_lake_nh_mds_quality.py — Ingest nursing home MDS quality measures into the Aradune data lake.

Reads from: data/raw/nh_quality_mds_feb2026.csv (250,071 rows)
Writes to:  data/lake/fact/nh_mds_quality/snapshot={today}/data.parquet

Tables built:
  Facts:
    fact_nh_mds_quality — Nursing home MDS quality measures: per-facility quarterly
                          and average scores across CMS quality measure codes

Usage:
  python3 scripts/build_lake_nh_mds_quality.py
  python3 scripts/build_lake_nh_mds_quality.py --dry-run
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

CSV_FILE = RAW_DIR / "nh_quality_mds_feb2026.csv"

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


def build_fact_nh_mds_quality(con, dry_run: bool) -> int:
    print("Building fact_nh_mds_quality...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_nh_mds_quality AS
        SELECT
            "CMS Certification Number (CCN)" AS provider_ccn,
            "Provider Name" AS provider_name,
            "State" AS state_code,
            "City/Town" AS city,
            "ZIP Code" AS zip_code,
            "Measure Code" AS measure_code,
            "Measure Description" AS measure_description,
            "Resident type" AS resident_type,
            TRY_CAST("Q1 Measure Score" AS DOUBLE) AS q1_score,
            TRY_CAST("Q2 Measure Score" AS DOUBLE) AS q2_score,
            TRY_CAST("Q3 Measure Score" AS DOUBLE) AS q3_score,
            TRY_CAST("Q4 Measure Score" AS DOUBLE) AS q4_score,
            TRY_CAST("Four Quarter Average Score" AS DOUBLE) AS avg_score,
            "Used in Quality Measure Five Star Rating" AS used_in_five_star,
            'data.cms.gov/nh-mds-quality' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{CSV_FILE}', all_varchar=true)
        WHERE "State" IS NOT NULL
          AND LENGTH(TRIM("State")) = 2
    """)

    count = write_parquet(con, "_fact_nh_mds_quality", _snapshot_path("nh_mds_quality"), dry_run)

    # Summary stats
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_nh_mds_quality").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT provider_ccn) FROM _fact_nh_mds_quality").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_nh_mds_quality").fetchone()[0]
    avg_avg = con.execute("SELECT ROUND(AVG(avg_score), 2) FROM _fact_nh_mds_quality WHERE avg_score IS NOT NULL").fetchone()[0]
    five_star_count = con.execute("SELECT COUNT(*) FROM _fact_nh_mds_quality WHERE used_in_five_star = 'Y'").fetchone()[0]

    print(f"  {count:,} rows, {states} states, {facilities:,} facilities, {measures} measures")
    print(f"  Mean avg_score: {avg_avg}, Five-star rows: {five_star_count:,}")

    # Top measures by row count
    top_measures = con.execute("""
        SELECT measure_code, measure_description, COUNT(*) AS cnt
        FROM _fact_nh_mds_quality
        GROUP BY 1, 2
        ORDER BY cnt DESC
        LIMIT 5
    """).fetchall()
    print("  Top measures by row count:")
    for code, desc, cnt in top_measures:
        print(f"    {code}: {cnt:,} rows — {desc[:60]}")

    con.execute("DROP TABLE IF EXISTS _fact_nh_mds_quality")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest nursing home MDS quality measures into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not CSV_FILE.exists():
        print(f"ERROR: CSV not found at {CSV_FILE}", file=sys.stderr)
        print("Expected: data/raw/nh_quality_mds_feb2026.csv")
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Source:   {CSV_FILE.name}")
    print()

    con = duckdb.connect()
    totals = {}
    totals["fact_nh_mds_quality"] = build_fact_nh_mds_quality(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("NH MDS QUALITY LAKE INGESTION COMPLETE")
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
            "source_files": [str(CSV_FILE)],
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_nh_mds_quality_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
