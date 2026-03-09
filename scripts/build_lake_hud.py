#!/usr/bin/env python3
"""
build_lake_hud.py — Ingest HUD Fair Market Rent data into the lake.

Reads from: data/raw/hud/FY25_FMRs_revised.xlsx
Writes to:  data/lake/fact/fair_market_rent/

Tables built:
  fact_fair_market_rent — HUD Fair Market Rents by county/metro area (FY2025)

Usage:
  python3 scripts/build_lake_hud.py
  python3 scripts/build_lake_hud.py --dry-run
"""

import argparse
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "hud"
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


def build_fact_fair_market_rent(con, dry_run: bool) -> int:
    print("Building fact_fair_market_rent...")
    xlsx_path = RAW_DIR / "FY25_FMRs_revised.xlsx"
    if not xlsx_path.exists():
        print(f"  SKIPPED — {xlsx_path.name} not found")
        return 0

    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_fmr AS
        SELECT
            stusps AS state_code,
            LPAD(CAST(state AS VARCHAR), 2, '0') AS state_fips,
            hud_area_code,
            countyname AS county_name,
            county_town_name,
            CASE WHEN metro = '1' THEN true ELSE false END AS is_metro,
            hud_area_name,
            fips AS county_fips,
            CAST(pop2022 AS INTEGER) AS population_2022,
            CAST(fmr_0 AS INTEGER) AS fmr_efficiency,
            CAST(fmr_1 AS INTEGER) AS fmr_1br,
            CAST(fmr_2 AS INTEGER) AS fmr_2br,
            CAST(fmr_3 AS INTEGER) AS fmr_3br,
            CAST(fmr_4 AS INTEGER) AS fmr_4br,
            2025 AS fiscal_year,
            'huduser.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM st_read('{xlsx_path}')
    """)

    count = write_parquet(con, "_fact_fmr", _snapshot_path("fair_market_rent"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_fmr").fetchone()[0]
    avg_2br = con.execute("SELECT ROUND(AVG(fmr_2br)) FROM _fact_fmr").fetchone()[0]
    print(f"  {count:,} rows, {states} states, avg 2BR FMR: ${avg_2br:,.0f}/mo")
    con.execute("DROP TABLE IF EXISTS _fact_fmr")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest HUD data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    count = build_fact_fair_market_rent(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("HUD LAKE INGESTION COMPLETE")
    print("=" * 60)
    print(f"  fact_fair_market_rent          {count:>12,} rows")

    if not args.dry_run and count > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {"fact_fair_market_rent": {"rows": count}},
            "total_rows": count,
        }
        manifest_file = META_DIR / f"manifest_hud_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
