#!/usr/bin/env python3
"""
build_lake_opioid_prescribing.py — Ingest Medicaid opioid prescribing rates into the Aradune data lake.

Reads from: data/raw/medicaid_opioid_prescribing_rates.csv
Writes to:  data/lake/fact/medicaid_opioid_prescribing/snapshot={today}/data.parquet

Tables built:
  Facts:
    fact_medicaid_opioid_prescribing — Medicaid opioid prescribing rates by state, year, plan type
                                       Includes long-acting opioid rates and 1Y/5Y change metrics

Usage:
  python3 scripts/build_lake_opioid_prescribing.py
  python3 scripts/build_lake_opioid_prescribing.py --dry-run
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

CSV_FILE = RAW_DIR / "medicaid_opioid_prescribing_rates.csv"

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


def build_fact_medicaid_opioid_prescribing(con, dry_run: bool) -> int:
    print("Building fact_medicaid_opioid_prescribing...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_medicaid_opioid_prescribing AS
        SELECT
            TRY_CAST("Year" AS INTEGER) AS year,
            "Geo_Lvl" AS geo_level,
            "Geo_Cd" AS state_code,
            "Geo_Desc" AS state_name,
            "Plan_Type" AS plan_type,
            TRY_CAST("Tot_Opioid_Clms" AS DOUBLE) AS total_opioid_claims,
            TRY_CAST("Tot_Clms" AS DOUBLE) AS total_claims,
            TRY_CAST("Opioid_Prscrbng_Rate" AS DOUBLE) AS opioid_prescribing_rate,
            TRY_CAST("Opioid_Prscrbng_Rate_5Y_Chg" AS DOUBLE) AS opioid_rate_5y_change,
            TRY_CAST("Opioid_Prscrbng_Rate_1Y_Chg" AS DOUBLE) AS opioid_rate_1y_change,
            TRY_CAST("LA_Tot_Opioid_Clms" AS DOUBLE) AS la_total_opioid_claims,
            TRY_CAST("LA_Opioid_Prscrbng_Rate" AS DOUBLE) AS la_opioid_prescribing_rate,
            TRY_CAST("LA_Opioid_Prscrbng_Rate_5Y_Chg" AS DOUBLE) AS la_rate_5y_change,
            TRY_CAST("LA_Opioid_Prscrbng_Rate_1Y_Chg" AS DOUBLE) AS la_rate_1y_change,
            'data.cms.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{CSV_FILE}', all_varchar=true)
        WHERE "Year" IS NOT NULL
    """)

    count = write_parquet(con, "_fact_medicaid_opioid_prescribing",
                          _snapshot_path("medicaid_opioid_prescribing"), dry_run)

    # Summary stats
    states = con.execute("""
        SELECT COUNT(DISTINCT state_code)
        FROM _fact_medicaid_opioid_prescribing
        WHERE geo_level = 'State'
    """).fetchone()[0]

    years = con.execute("""
        SELECT MIN(year), MAX(year)
        FROM _fact_medicaid_opioid_prescribing
        WHERE year IS NOT NULL
    """).fetchone()

    plan_types = con.execute("""
        SELECT DISTINCT plan_type
        FROM _fact_medicaid_opioid_prescribing
        ORDER BY plan_type
    """).fetchall()

    avg_rate = con.execute("""
        SELECT ROUND(AVG(opioid_prescribing_rate), 2)
        FROM _fact_medicaid_opioid_prescribing
        WHERE opioid_prescribing_rate IS NOT NULL
          AND geo_level = 'National'
          AND year = (SELECT MAX(year) FROM _fact_medicaid_opioid_prescribing)
    """).fetchone()[0]

    geo_levels = con.execute("""
        SELECT geo_level, COUNT(*)
        FROM _fact_medicaid_opioid_prescribing
        GROUP BY geo_level
        ORDER BY geo_level
    """).fetchall()

    print(f"  {count:,} total rows, {states} states, years {years[0]}-{years[1]}")
    print(f"  Plan types: {', '.join(pt[0] for pt in plan_types)}")
    print(f"  National avg opioid prescribing rate (latest year): {avg_rate}")
    for gl, cnt in geo_levels:
        print(f"    {gl}: {cnt:,} rows")

    con.execute("DROP TABLE IF EXISTS _fact_medicaid_opioid_prescribing")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Ingest Medicaid opioid prescribing rates into Aradune lake"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not CSV_FILE.exists():
        print(f"ERROR: CSV not found at {CSV_FILE}", file=sys.stderr)
        print("Download from: https://data.cms.gov/summary-statistics-on-use-and-payments/"
              "medicare-medicaid-opioid-prescribing-rates/")
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Source:   {CSV_FILE.name}")
    print()

    con = duckdb.connect()
    totals = {}
    totals["fact_medicaid_opioid_prescribing"] = build_fact_medicaid_opioid_prescribing(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("OPIOID PRESCRIBING LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:45s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':45s} {total_rows:>10,} rows")

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
        manifest_file = META_DIR / f"manifest_opioid_prescribing_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
