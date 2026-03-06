#!/usr/bin/env python3
"""
build_lake_scorecard.py — Ingest CMS Medicaid & CHIP Scorecard + supplemental data.

Reads from: data/raw/scorecard_measure_value.csv (74MB, ~90K rows)
             data/raw/scorecard_measure.csv (measure definitions)
             data/raw/eligibility_group_monthly.csv (31K rows)
             data/raw/eligibility_group_annual.csv (2.6K rows)
             data/raw/cms64_new_adult_expenditures.csv (2.6K rows)
             data/raw/ffcra_fmap_expenditure.csv (1.2K rows)
             data/raw/mc_enrollment_population.csv (515 rows)
             data/raw/mc_enrollment_duals.csv (513 rows)
             data/raw/hai_state.json (1K rows)

Tables built:
  fact_scorecard         — Medicaid & CHIP Scorecard measure values by state
  dim_scorecard_measure  — Scorecard measure definitions
  fact_elig_group_monthly — Major eligibility group enrollment by month
  fact_elig_group_annual  — Major eligibility group enrollment by year
  fact_cms64_new_adult   — CMS-64 new adult group (expansion) expenditures
  fact_ffcra_fmap        — FFCRA increased FMAP expenditure
  fact_mc_enroll_pop     — Managed care enrollment by program/population
  fact_mc_enroll_duals   — Managed care enrollment for duals
  fact_hai_state         — Healthcare-associated infections by state

Usage:
  python3 scripts/build_lake_scorecard.py
  python3 scripts/build_lake_scorecard.py --dry-run
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
DIM_DIR = LAKE_DIR / "dimension"
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


def _dim_path(dim_name: str) -> Path:
    return DIM_DIR / f"{dim_name}.parquet"


# ---------------------------------------------------------------------------
# Scorecard
# ---------------------------------------------------------------------------

def build_scorecard(con, dry_run: bool) -> dict:
    print("Building fact_scorecard + dim_scorecard_measure...")
    mv_path = RAW_DIR / "scorecard_measure_value.csv"
    m_path = RAW_DIR / "scorecard_measure.csv"

    totals = {}

    if mv_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_scorecard AS
            SELECT
                measure_id,
                state_abbreviation AS state_code,
                strat_tier1_label, strat_tier1_value,
                strat_tier2_label, strat_tier2_value,
                strat_tier3_label, strat_tier3_value,
                data_period,
                population,
                methodology,
                TRY_CAST(measure_value AS DOUBLE) AS measure_value,
                values_direction,
                value_type,
                TRY_CAST(number_of_states_reporting AS INTEGER) AS states_reporting,
                TRY_CAST("median" AS DOUBLE) AS median_value,
                TRY_CAST("mean" AS DOUBLE) AS mean_value,
                TRY_CAST(q1 AS DOUBLE) AS q1,
                TRY_CAST(q3 AS DOUBLE) AS q3,
                TRY_CAST("min" AS DOUBLE) AS min_value,
                TRY_CAST("max" AS DOUBLE) AS max_value,
                dataset,
                'medicaid.gov/scorecard' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{mv_path}', sample_size=5000)
            WHERE state_abbreviation IS NOT NULL
        """)
        totals['fact_scorecard'] = write_parquet(
            con, "_fact_scorecard", _snapshot_path("scorecard"), dry_run
        )
        states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_scorecard").fetchone()[0]
        measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_scorecard").fetchone()[0]
        print(f"  {totals['fact_scorecard']:,} rows, {states} states, {measures} measures")
        con.execute("DROP TABLE IF EXISTS _fact_scorecard")
    else:
        print(f"  SKIPPED — {mv_path.name} not found")

    if m_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _dim_scorecard_measure AS
            SELECT
                id AS measure_id,
                data_source_link,
                data_period_type,
                data_range,
                data_source,
                keywords,
                TRY_CAST(has_national_view AS BOOLEAN) AS has_national_view,
                TRY_CAST(has_state_view AS BOOLEAN) AS has_state_view,
                'medicaid.gov/scorecard' AS source
            FROM read_csv_auto('{m_path}', sample_size=100)
        """)
        totals['dim_scorecard_measure'] = write_parquet(
            con, "_dim_scorecard_measure", _dim_path("dim_scorecard_measure"), dry_run
        )
        print(f"  dim_scorecard_measure: {totals['dim_scorecard_measure']} definitions")
        con.execute("DROP TABLE IF EXISTS _dim_scorecard_measure")
    else:
        print(f"  SKIPPED — {m_path.name} not found")

    return totals


# ---------------------------------------------------------------------------
# Eligibility Group Enrollment
# ---------------------------------------------------------------------------

def build_elig_groups(con, dry_run: bool) -> dict:
    print("Building eligibility group tables...")
    totals = {}

    monthly_path = RAW_DIR / "eligibility_group_monthly.csv"
    if monthly_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_elig_monthly AS
            SELECT
                "State" AS state_name,
                "Month" AS month_str,
                "majoreligibility group" AS eligibility_group,
                TRY_CAST("CountEnrolled" AS BIGINT) AS count_enrolled,
                "dunusable" AS dq_unusable,
                'data.medicaid.gov' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{monthly_path}', sample_size=2000)
            WHERE "State" IS NOT NULL
        """)
        totals['fact_elig_group_monthly'] = write_parquet(
            con, "_fact_elig_monthly", _snapshot_path("elig_group_monthly"), dry_run
        )
        states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_elig_monthly").fetchone()[0]
        print(f"  Monthly: {totals['fact_elig_group_monthly']:,} rows, {states} states")
        con.execute("DROP TABLE IF EXISTS _fact_elig_monthly")
    else:
        print(f"  SKIPPED — {monthly_path.name} not found")

    annual_path = RAW_DIR / "eligibility_group_annual.csv"
    if annual_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_elig_annual AS
            SELECT
                "State" AS state_name,
                TRY_CAST("Year" AS INTEGER) AS year,
                "majoreligibility group" AS eligibility_group,
                TRY_CAST("countever enrolled" AS BIGINT) AS count_ever_enrolled,
                TRY_CAST("countlast month enrollment" AS BIGINT) AS count_last_month,
                TRY_CAST("AverageEnrollmentPerMonth" AS DOUBLE) AS avg_monthly_enrollment,
                "dunusable" AS dq_unusable,
                'data.medicaid.gov' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{annual_path}', sample_size=500)
            WHERE "State" IS NOT NULL
        """)
        totals['fact_elig_group_annual'] = write_parquet(
            con, "_fact_elig_annual", _snapshot_path("elig_group_annual"), dry_run
        )
        print(f"  Annual: {totals['fact_elig_group_annual']:,} rows")
        con.execute("DROP TABLE IF EXISTS _fact_elig_annual")
    else:
        print(f"  SKIPPED — {annual_path.name} not found")

    return totals


# ---------------------------------------------------------------------------
# CMS-64 New Adult + FFCRA
# ---------------------------------------------------------------------------

def build_expenditure_supplemental(con, dry_run: bool) -> dict:
    print("Building expenditure supplemental tables...")
    totals = {}

    na_path = RAW_DIR / "cms64_new_adult_expenditures.csv"
    if na_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_cms64_na AS
            SELECT *,
                'data.medicaid.gov' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{na_path}', sample_size=500)
        """)
        totals['fact_cms64_new_adult'] = write_parquet(
            con, "_fact_cms64_na", _snapshot_path("cms64_new_adult"), dry_run
        )
        print(f"  CMS-64 New Adult: {totals['fact_cms64_new_adult']:,} rows")
        con.execute("DROP TABLE IF EXISTS _fact_cms64_na")
    else:
        print(f"  SKIPPED — {na_path.name} not found")

    ffcra_path = RAW_DIR / "ffcra_fmap_expenditure.csv"
    if ffcra_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_ffcra AS
            SELECT *,
                'data.medicaid.gov' AS source_url,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{ffcra_path}', sample_size=500)
        """)
        totals['fact_ffcra_fmap'] = write_parquet(
            con, "_fact_ffcra", _snapshot_path("ffcra_fmap"), dry_run
        )
        print(f"  FFCRA FMAP: {totals['fact_ffcra_fmap']:,} rows")
        con.execute("DROP TABLE IF EXISTS _fact_ffcra")
    else:
        print(f"  SKIPPED — {ffcra_path.name} not found")

    return totals


# ---------------------------------------------------------------------------
# Managed Care Enrollment by Population
# ---------------------------------------------------------------------------

def build_mc_enrollment_detail(con, dry_run: bool) -> dict:
    print("Building managed care enrollment detail tables...")
    totals = {}

    pop_path = RAW_DIR / "mc_enrollment_population.csv"
    if pop_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_mc_pop AS
            SELECT *,
                'data.medicaid.gov' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{pop_path}', sample_size=200)
        """)
        totals['fact_mc_enroll_pop'] = write_parquet(
            con, "_fact_mc_pop", _snapshot_path("mc_enroll_pop"), dry_run
        )
        print(f"  MC Enrollment (All): {totals['fact_mc_enroll_pop']:,} rows")
        con.execute("DROP TABLE IF EXISTS _fact_mc_pop")
    else:
        print(f"  SKIPPED — {pop_path.name} not found")

    duals_path = RAW_DIR / "mc_enrollment_duals.csv"
    if duals_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_mc_duals AS
            SELECT *,
                'data.medicaid.gov' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{duals_path}', sample_size=200)
        """)
        totals['fact_mc_enroll_duals'] = write_parquet(
            con, "_fact_mc_duals", _snapshot_path("mc_enroll_duals"), dry_run
        )
        print(f"  MC Enrollment (Duals): {totals['fact_mc_enroll_duals']:,} rows")
        con.execute("DROP TABLE IF EXISTS _fact_mc_duals")
    else:
        print(f"  SKIPPED — {duals_path.name} not found")

    return totals


# ---------------------------------------------------------------------------
# HAI State
# ---------------------------------------------------------------------------

def build_hai_state(con, dry_run: bool) -> dict:
    print("Building fact_hai_state...")
    totals = {}

    json_path = RAW_DIR / "hai_state.json"
    if json_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_hai AS
            SELECT
                state AS state_code,
                measure_id,
                measure_name,
                TRY_CAST(score AS DOUBLE) AS score,
                footnote,
                start_date,
                end_date,
                'data.cms.gov/care-compare' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_json_auto('{json_path}')
            WHERE state IS NOT NULL AND LENGTH(state) = 2
        """)
        totals['fact_hai_state'] = write_parquet(
            con, "_fact_hai", _snapshot_path("hai_state"), dry_run
        )
        states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hai").fetchone()[0]
        measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_hai").fetchone()[0]
        print(f"  {totals['fact_hai_state']:,} rows, {states} states, {measures} measures")
        con.execute("DROP TABLE IF EXISTS _fact_hai")
    else:
        print(f"  SKIPPED — {json_path.name} not found")

    return totals


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_BUILDERS = [
    build_scorecard,
    build_elig_groups,
    build_expenditure_supplemental,
    build_mc_enrollment_detail,
    build_hai_state,
]


def main():
    parser = argparse.ArgumentParser(description="Ingest Scorecard + supplemental data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    all_totals = {}
    for builder in ALL_BUILDERS:
        result = builder(con, args.dry_run)
        all_totals.update(result)
        print()

    con.close()

    print("=" * 60)
    print("SCORECARD + SUPPLEMENTAL LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(all_totals.values())
    for name, count in all_totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in all_totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_scorecard_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
