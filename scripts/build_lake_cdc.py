#!/usr/bin/env python3
"""
build_lake_cdc.py — Ingest CDC health outcome data into the lake.

Reads from: data/raw/cdc_drug_overdose_state.json (2.7K rows)
             data/raw/cdc_mortality_trends.json (10.9K rows)
             data/raw/cdc_infant_mortality.json (53 rows — actually drug OD rates by age)
Writes to:  data/lake/

Tables built:
  fact_drug_overdose    — Drug poisoning mortality by state, age, sex, race
  fact_mortality_trend  — Leading causes of death by state, 1999-2017

Usage:
  python3 scripts/build_lake_cdc.py
  python3 scripts/build_lake_cdc.py --dry-run
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


def build_drug_overdose(con, dry_run: bool) -> int:
    print("Building fact_drug_overdose...")
    json_path = RAW_DIR / "cdc_drug_overdose_state.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_od AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            sex,
            age AS age_group,
            race_hispanic_origin AS race,
            state AS state_name,
            TRY_CAST(deaths AS INTEGER) AS deaths,
            TRY_CAST(population AS BIGINT) AS population,
            TRY_CAST(crude_death_rate AS DOUBLE) AS crude_death_rate,
            TRY_CAST(age_adjusted_rate AS DOUBLE) AS age_adjusted_rate,
            'data.cdc.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_od", _snapshot_path("drug_overdose"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_od").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_od").fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_od")
    return count


def build_mortality_trends(con, dry_run: bool) -> int:
    print("Building fact_mortality_trend...")
    json_path = RAW_DIR / "cdc_mortality_trends.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mort AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            cause_name,
            state AS state_name,
            TRY_CAST(deaths AS INTEGER) AS deaths,
            TRY_CAST(aadr AS DOUBLE) AS age_adjusted_death_rate,
            'data.cdc.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL
    """)

    count = write_parquet(con, "_fact_mort", _snapshot_path("mortality_trend"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_mort").fetchone()[0]
    causes = con.execute("SELECT COUNT(DISTINCT cause_name) FROM _fact_mort").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {causes} causes of death")
    con.execute("DROP TABLE IF EXISTS _fact_mort")
    return count


ALL_TABLES = {
    "fact_drug_overdose": build_drug_overdose,
    "fact_mortality_trend": build_mortality_trends,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest CDC health data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}
    for name, builder in ALL_TABLES.items():
        totals[name] = builder(con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("CDC HEALTH DATA LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_cdc_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
