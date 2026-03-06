#!/usr/bin/env python3
"""
build_lake_care_compare.py — Ingest CMS Care Compare state-level quality data.

Reads from: data/raw/mspb_state.json
             data/raw/timely_effective_state.json
             data/raw/complications_state.json
             data/raw/unplanned_visits_state.json
             data/raw/dialysis_state.json
             data/raw/home_health_state.json
Writes to:  data/lake/

Tables built:
  fact_mspb_state             — Medicare Spending Per Beneficiary by state (56 rows)
  fact_timely_effective       — Timely and Effective Care measures by state (1,736 rows)
  fact_complications          — Complications and Deaths by state (1,120 rows)
  fact_unplanned_visits       — Unplanned Hospital Visits by state (784 rows)
  fact_dialysis_state         — Dialysis facility quality averages by state (56 rows)
  fact_home_health_state      — Home health quality by state (55 rows)

Usage:
  python3 scripts/build_lake_care_compare.py
  python3 scripts/build_lake_care_compare.py --dry-run
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


def _build_json_table(con, table_name: str, json_file: Path, sql: str, dry_run: bool) -> int:
    print(f"Building {table_name}...")
    if not json_file.exists():
        print(f"  SKIPPED — {json_file.name} not found")
        return 0
    con.execute(sql.format(json_path=json_file, snapshot_date=SNAPSHOT_DATE))
    count = write_parquet(con, f"_{table_name}", _snapshot_path(table_name.replace('fact_', '')), dry_run)
    states = con.execute(f"SELECT COUNT(DISTINCT state_code) FROM _{table_name}").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute(f"DROP TABLE IF EXISTS _{table_name}")
    return count


def build_all(con, dry_run: bool) -> dict:
    totals = {}

    # Medicare Spending Per Beneficiary
    totals["fact_mspb_state"] = _build_json_table(con, "fact_mspb_state",
        RAW_DIR / "mspb_state.json", """
        CREATE OR REPLACE TABLE _fact_mspb_state AS
        SELECT
            state AS state_code,
            measure_id,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    # Timely and Effective Care
    totals["fact_timely_effective"] = _build_json_table(con, "fact_timely_effective",
        RAW_DIR / "timely_effective_state.json", """
        CREATE OR REPLACE TABLE _fact_timely_effective AS
        SELECT
            state AS state_code,
            _condition AS condition,
            measure_id,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    # Complications and Deaths
    totals["fact_complications"] = _build_json_table(con, "fact_complications",
        RAW_DIR / "complications_state.json", """
        CREATE OR REPLACE TABLE _fact_complications AS
        SELECT
            state AS state_code,
            measure_id,
            measure_name,
            TRY_CAST(number_of_hospitals_worse AS INTEGER) AS hospitals_worse,
            TRY_CAST(number_of_hospitals_same AS INTEGER) AS hospitals_same,
            TRY_CAST(number_of_hospitals_better AS INTEGER) AS hospitals_better,
            TRY_CAST(number_of_hospitals_too_few AS INTEGER) AS hospitals_too_few,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    # Unplanned Hospital Visits
    totals["fact_unplanned_visits"] = _build_json_table(con, "fact_unplanned_visits",
        RAW_DIR / "unplanned_visits_state.json", """
        CREATE OR REPLACE TABLE _fact_unplanned_visits AS
        SELECT
            state AS state_code,
            measure_id,
            measure_name,
            TRY_CAST(number_of_hospitals_worse AS INTEGER) AS hospitals_worse,
            TRY_CAST(number_of_hospitals_same AS INTEGER) AS hospitals_same,
            TRY_CAST(number_of_hospitals_better AS INTEGER) AS hospitals_better,
            TRY_CAST(number_of_hospitals_too_few AS INTEGER) AS hospitals_too_few,
            start_date,
            end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    # Dialysis State Averages
    totals["fact_dialysis_state"] = _build_json_table(con, "fact_dialysis_state",
        RAW_DIR / "dialysis_state.json", """
        CREATE OR REPLACE TABLE _fact_dialysis_state AS
        SELECT
            state AS state_code,
            TRY_CAST(survival_better_than_expected_state AS INTEGER) AS survival_better,
            TRY_CAST(survival_as_expected_state AS INTEGER) AS survival_expected,
            TRY_CAST(survival_worse_than_expected_state AS INTEGER) AS survival_worse,
            TRY_CAST(hospitalizations_better_than_expected_state AS INTEGER) AS hosp_better,
            TRY_CAST(hospitalizations_as_expected_state AS INTEGER) AS hosp_expected,
            TRY_CAST(hospitalizations_worse_than_expected_state AS INTEGER) AS hosp_worse,
            TRY_CAST(hospital_readmission__better_than_expected_state AS INTEGER) AS readm_better,
            TRY_CAST(hospital_readmission__as_expected_state AS INTEGER) AS readm_expected,
            TRY_CAST(hospital__readmission__worse_than_expected_state AS INTEGER) AS readm_worse,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    # Home Health State
    totals["fact_home_health_state"] = _build_json_table(con, "fact_home_health_state",
        RAW_DIR / "home_health_state.json", """
        CREATE OR REPLACE TABLE _fact_home_health_state AS
        SELECT
            state AS state_code,
            TRY_CAST(quality_of_patient_care_star_rating AS DOUBLE) AS quality_star_rating,
            TRY_CAST(how_often_patients_got_better_at_walking_or_moving_around AS DOUBLE) AS pct_better_walking,
            TRY_CAST(how_often_patients_got_better_at_getting_in_and_out_of_bed AS DOUBLE) AS pct_better_bed_transfer,
            TRY_CAST(how_often_patients_breathing_improved AS DOUBLE) AS pct_breathing_improved,
            TRY_CAST(how_often_patients_got_better_at_bathing AS DOUBLE) AS pct_better_bathing,
            TRY_CAST(how_often_patients_got_better_at_taking_their_drugs_correct_bd88 AS DOUBLE) AS pct_better_medications,
            TRY_CAST(discharge_function_score AS DOUBLE) AS discharge_function_score,
            TRY_CAST(how_much_medicare_spends_on_an_episode_of_care_by_agencies__e8d7 AS DOUBLE) AS medicare_spending_per_episode,
            'data.cms.gov/care-compare' AS source,
            DATE '{snapshot_date}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """, dry_run)

    return totals


def main():
    parser = argparse.ArgumentParser(description="Ingest Care Compare state-level quality data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = build_all(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("CARE COMPARE STATE QUALITY DATA INGESTION COMPLETE")
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
            "source": "data.cms.gov/care-compare",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_care_compare_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
