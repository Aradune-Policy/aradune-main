#!/usr/bin/env python3
"""
build_lake_round2b.py — Ingest CMS state/facility quality datasets into the lake.

Sources:
  1. Maternal Health - Hospital (CMS Care Compare, 17,968 rows)
  2. Hospice - State Data (1,100 rows, 67 measures × 55 states)
  3. ASC Quality State-level (54 rows)
  4. Home Health - State Data (55 rows)
  5. OAS CAHPS State-level (840 rows)

Tables built:
  fact_maternal_health      — Hospital-level maternal health quality measures
  fact_hospice_state        — State-level hospice quality measures
  fact_asc_quality_state    — Ambulatory surgical center quality by state
  fact_home_health_state2   — Home health quality ratings by state (Care Compare)
  fact_oas_cahps_state      — Outpatient surgery CAHPS by state

Usage:
  python3 scripts/build_lake_round2b.py
  python3 scripts/build_lake_round2b.py --dry-run
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


def build_maternal_health(con, dry_run: bool) -> int:
    """Build fact_maternal_health from CMS Care Compare maternal health data."""
    print("Building fact_maternal_health...")
    json_path = RAW_DIR / "maternal_health_hospital.json"
    if not json_path.exists():
        print("  SKIPPED — maternal_health_hospital.json not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_maternal AS
        SELECT
            facility_id,
            facility_name,
            state,
            citytown AS city,
            zip_code,
            countyparish AS county,
            measure_id,
            measure_name,
            TRY_CAST(CASE WHEN score = 'Not Available' THEN NULL ELSE score END AS DOUBLE) AS score,
            TRY_CAST(CASE WHEN sample = 'Not Available' THEN NULL ELSE sample END AS INTEGER) AS sample_size,
            footnote,
            start_date,
            end_date,
            'cms_care_compare_maternal' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) <= 2
    """)

    count = write_parquet(con, "_fact_maternal", _snapshot_path("maternal_health"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT facility_id) FROM _fact_maternal").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_maternal").fetchone()[0]

    # Score availability
    scored = con.execute("SELECT COUNT(*) FROM _fact_maternal WHERE score IS NOT NULL").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {states} states, {scored:,} with scores")

    # Mean scores by measure
    avgs = con.execute("""
        SELECT measure_id, measure_name, ROUND(AVG(score), 1) AS avg_score, COUNT(*) AS n
        FROM _fact_maternal WHERE score IS NOT NULL
        GROUP BY measure_id, measure_name ORDER BY measure_id
    """).fetchall()
    for m in avgs:
        print(f"    {m[0]}: {m[1]} — avg {m[2]}, n={m[3]:,}")

    con.execute("DROP TABLE IF EXISTS _fact_maternal")
    return count


def build_hospice_state(con, dry_run: bool) -> int:
    """Build fact_hospice_state from CMS Care Compare hospice state data."""
    print("Building fact_hospice_state...")
    json_path = RAW_DIR / "hospice_state.json"
    if not json_path.exists():
        print("  SKIPPED — hospice_state.json not found")
        return 0

    # This file may be wrapped in {"results": [...]}
    with open(json_path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "results" in data:
        records = data["results"]
        with open(json_path, "w") as f:
            json.dump(records, f)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hosp_state AS
        SELECT
            state,
            measure_code,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            footnote,
            measure_date_range,
            'cms_care_compare_hospice_state' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_hosp_state", _snapshot_path("hospice_state"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_hosp_state").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_hosp_state").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {measures} measures")

    con.execute("DROP TABLE IF EXISTS _fact_hosp_state")
    return count


def build_asc_quality_state(con, dry_run: bool) -> int:
    """Build fact_asc_quality_state from CMS ASC quality state data."""
    print("Building fact_asc_quality_state...")
    json_path = RAW_DIR / "asc_quality_state.json"
    if not json_path.exists():
        print("  SKIPPED — asc_quality_state.json not found")
        return 0

    with open(json_path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "results" in data:
        records = data["results"]
        with open(json_path, "w") as f:
            json.dump(records, f)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_asc AS
        SELECT *,
            'cms_care_compare_asc' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_asc", _snapshot_path("asc_quality_state"), dry_run)
    print(f"  {count} state ASC quality records")

    con.execute("DROP TABLE IF EXISTS _fact_asc")
    return count


def build_home_health_state2(con, dry_run: bool) -> int:
    """Build fact_home_health_state2 from CMS Care Compare home health state data."""
    print("Building fact_home_health_state2...")
    json_path = RAW_DIR / "home_health_state_data.json"
    if not json_path.exists():
        print("  SKIPPED — home_health_state_data.json not found")
        return 0

    with open(json_path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "results" in data:
        records = data["results"]
        with open(json_path, "w") as f:
            json.dump(records, f)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hh_state AS
        SELECT *,
            'cms_care_compare_home_health' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_hh_state", _snapshot_path("home_health_state2"), dry_run)
    print(f"  {count} state home health records")

    con.execute("DROP TABLE IF EXISTS _fact_hh_state")
    return count


def build_oas_cahps_state(con, dry_run: bool) -> int:
    """Build fact_oas_cahps_state from CMS OAS CAHPS state-level data."""
    print("Building fact_oas_cahps_state...")
    json_path = RAW_DIR / "oas_cahps_state.json"
    if not json_path.exists():
        print("  SKIPPED — oas_cahps_state.json not found")
        return 0

    with open(json_path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "results" in data:
        records = data["results"]
        with open(json_path, "w") as f:
            json.dump(records, f)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_oas AS
        SELECT
            state,
            oas_cahps_measure_id AS measure_id,
            oas_cahps_question AS question,
            oas_cahps_answer_description AS answer_description,
            TRY_CAST(oas_cahps_answer_percent AS DOUBLE) AS answer_pct,
            start_date,
            end_date,
            'cms_oas_cahps_state' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_oas", _snapshot_path("oas_cahps_state"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_oas").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_oas").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {measures} measures")

    con.execute("DROP TABLE IF EXISTS _fact_oas")
    return count


ALL_TABLES = {
    "maternal_health": ("fact_maternal_health", build_maternal_health),
    "hospice_state": ("fact_hospice_state", build_hospice_state),
    "asc_quality": ("fact_asc_quality_state", build_asc_quality_state),
    "home_health_state": ("fact_home_health_state2", build_home_health_state2),
    "oas_cahps": ("fact_oas_cahps_state", build_oas_cahps_state),
}


def main():
    parser = argparse.ArgumentParser(description="Ingest CMS quality datasets into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 2B — CMS Quality Data Ingestion — {SNAPSHOT_DATE}")
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
    print("ROUND 2B LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round2b_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
