#!/usr/bin/env python3
"""
build_lake_economic.py — Ingest economic context data into the lake.

Reads from: data/raw/bls_economic_indicators.json (CPI, national unemployment)
             data/raw/fred_state_unemployment.json (state unemployment from FRED)
             data/raw/fred_state_income.json (state median household income)
             data/raw/mspb_hospital.json (Medicare Spending Per Beneficiary, hospital-level)
Writes to:  data/lake/

Tables built:
  fact_cpi                   — CPI indices (medical care, all items, services, drugs)
  fact_unemployment          — State-level monthly unemployment rates
  fact_median_income         — State-level annual median household income
  fact_mspb_hospital         — Medicare Spending Per Beneficiary by hospital

Usage:
  python3 scripts/build_lake_economic.py
  python3 scripts/build_lake_economic.py --dry-run
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


# ---------------------------------------------------------------------------
# CPI Indicators
# ---------------------------------------------------------------------------

def build_fact_cpi(con, dry_run: bool) -> int:
    print("Building fact_cpi...")
    json_path = RAW_DIR / "bls_economic_indicators.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_cpi AS
        SELECT
            series_id,
            series_name,
            CAST(year AS INTEGER) AS year,
            period,
            period_name,
            CAST(value AS DOUBLE) AS value,
            'api.bls.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
    """)

    count = write_parquet(con, "_fact_cpi", _snapshot_path("cpi"), dry_run)
    series = con.execute("SELECT COUNT(DISTINCT series_name) FROM _fact_cpi").fetchone()[0]
    print(f"  {count:,} rows, {series} series")
    con.execute("DROP TABLE IF EXISTS _fact_cpi")
    return count


# ---------------------------------------------------------------------------
# State Unemployment
# ---------------------------------------------------------------------------

def build_fact_unemployment(con, dry_run: bool) -> int:
    print("Building fact_unemployment...")
    json_path = RAW_DIR / "fred_state_unemployment.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_unemp AS
        SELECT
            state_code,
            CAST(date AS DATE) AS observation_date,
            EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
            EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,
            CAST(unemployment_rate AS DOUBLE) AS unemployment_rate,
            'fred.stlouisfed.org' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state_code IS NOT NULL
    """)

    count = write_parquet(con, "_fact_unemp", _snapshot_path("unemployment"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_unemp").fetchone()[0]
    latest = con.execute("SELECT MAX(observation_date) FROM _fact_unemp").fetchone()[0]
    print(f"  {count:,} rows, {states} states, latest: {latest}")
    con.execute("DROP TABLE IF EXISTS _fact_unemp")
    return count


# ---------------------------------------------------------------------------
# State Median Household Income
# ---------------------------------------------------------------------------

def build_fact_median_income(con, dry_run: bool) -> int:
    print("Building fact_median_income...")
    json_path = RAW_DIR / "fred_state_income.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_income AS
        SELECT
            state_code,
            CAST(date AS DATE) AS observation_date,
            EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
            CAST(median_household_income AS DOUBLE) AS median_household_income,
            'fred.stlouisfed.org' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state_code IS NOT NULL
    """)

    count = write_parquet(con, "_fact_income", _snapshot_path("median_income"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_income").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_income")
    return count


# ---------------------------------------------------------------------------
# Medicare Spending Per Beneficiary — Hospital Level
# ---------------------------------------------------------------------------

def build_fact_mspb_hospital(con, dry_run: bool) -> int:
    print("Building fact_mspb_hospital...")
    json_path = RAW_DIR / "mspb_hospital.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mspb_hosp AS
        SELECT
            facility_id AS provider_id,
            facility_name,
            state AS state_code,
            measure_id,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """)

    count = write_parquet(con, "_fact_mspb_hosp", _snapshot_path("mspb_hospital"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_mspb_hosp").fetchone()[0]
    hospitals = con.execute("SELECT COUNT(DISTINCT provider_id) FROM _fact_mspb_hosp").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {hospitals:,} hospitals")
    con.execute("DROP TABLE IF EXISTS _fact_mspb_hosp")
    return count


# ---------------------------------------------------------------------------
# State GDP (FRED)
# ---------------------------------------------------------------------------

def build_fact_state_gdp(con, dry_run: bool) -> int:
    print("Building fact_state_gdp...")
    json_path = RAW_DIR / "fred_state_gdp.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_gdp AS
        SELECT
            state_code,
            CAST(date AS DATE) AS observation_date,
            EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
            CAST(real_gdp AS DOUBLE) AS real_gdp_millions,
            'fred.stlouisfed.org' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state_code IS NOT NULL
    """)

    count = write_parquet(con, "_fact_gdp", _snapshot_path("state_gdp"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_gdp").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_gdp")
    return count


# ---------------------------------------------------------------------------
# State Population (FRED)
# ---------------------------------------------------------------------------

def build_fact_state_population(con, dry_run: bool) -> int:
    print("Building fact_state_population...")
    json_path = RAW_DIR / "fred_state_population.json"
    if not json_path.exists():
        print(f"  SKIPPED — {json_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_pop AS
        SELECT
            state_code,
            CAST(date AS DATE) AS observation_date,
            EXTRACT(YEAR FROM CAST(date AS DATE)) AS year,
            CAST(population_thousands AS DOUBLE) AS population_thousands,
            'fred.stlouisfed.org' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state_code IS NOT NULL
    """)

    count = write_parquet(con, "_fact_pop", _snapshot_path("state_population"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_pop").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_pop")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_cpi": build_fact_cpi,
    "fact_unemployment": build_fact_unemployment,
    "fact_median_income": build_fact_median_income,
    "fact_mspb_hospital": build_fact_mspb_hospital,
    "fact_state_gdp": build_fact_state_gdp,
    "fact_state_population": build_fact_state_population,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest economic data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None)
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

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

    print("=" * 60)
    print("ECONOMIC DATA LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_economic_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
