#!/usr/bin/env python3
"""
build_lake_round6.py — CMS Care Compare hospital + SNF quality datasets (facility-level).

Tables built:
  fact_hai_hospital2         — Hospital-acquired infections by hospital (172K rows)
  fact_complications_hosp    — Complications and deaths by hospital (96K rows)
  fact_timely_effective_hosp — Timely & effective care by hospital (138K rows)
  fact_unplanned_visits_hosp — Unplanned hospital visits by hospital (67K rows)
  fact_psi90_hospital        — Patient Safety Indicator PSI-90 by hospital (52K rows)
  fact_snf_vbp               — SNF Value-Based Purchasing performance (14K rows)
  fact_nh_claims_quality     — NH claims-based quality measures (59K rows)
  fact_snf_quality_provider  — SNF quality reporting by provider (838K rows)
  fact_nh_state_averages     — NH state/national quality averages (55 rows)

Usage:
  python3 scripts/build_lake_round6.py
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


def _build_hospital_measures(con, dry_run, csv_file, table_name, fact_name, label,
                              has_denominator=False, has_compared=False,
                              has_condition=False, has_lower_upper=False,
                              has_sample=False, score_col="Score", rate_col=None):
    """Generic builder for CMS hospital measure CSVs with similar structure."""
    print(f"Building {fact_name}...")
    csv_path = RAW_DIR / csv_file
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    # Build SELECT columns dynamically
    cols = [
        '"Facility ID" AS facility_id',
        '"Facility Name" AS facility_name',
        '"State" AS state',
        '"ZIP Code" AS zip_code',
    ]
    if has_condition:
        cols.append('"Condition" AS condition')
    cols.append('"Measure ID" AS measure_id')
    cols.append('"Measure Name" AS measure_name')
    if has_compared:
        cols.append('"Compared to National" AS compared_to_national')
    if has_denominator:
        cols.append(f'TRY_CAST(CASE WHEN "Denominator" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "Denominator" END AS INTEGER) AS denominator')
    if rate_col:
        cols.append(f'TRY_CAST(CASE WHEN "{rate_col}" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "{rate_col}" END AS DOUBLE) AS score')
    else:
        cols.append(f'TRY_CAST(CASE WHEN "{score_col}" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "{score_col}" END AS DOUBLE) AS score')
    if has_lower_upper:
        cols.append('TRY_CAST(CASE WHEN "Lower Estimate" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "Lower Estimate" END AS DOUBLE) AS lower_estimate')
        cols.append('TRY_CAST(CASE WHEN "Higher Estimate" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "Higher Estimate" END AS DOUBLE) AS higher_estimate')
    if has_sample:
        cols.append('TRY_CAST(CASE WHEN "Sample" IN (\'Not Available\', \'--\', \'N/A\', \'\') THEN NULL ELSE "Sample" END AS INTEGER) AS sample_size')
    cols.append('"Footnote" AS footnote')
    cols.append('"Start Date" AS start_date')
    cols.append('"End Date" AS end_date')
    cols.append(f"'cms_care_compare' AS source")
    cols.append(f"DATE '{SNAPSHOT_DATE}' AS snapshot_date")

    select = ",\n            ".join(cols)
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            {select}
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, table_name, _snapshot_path(fact_name), dry_run)
    facilities = con.execute(f"SELECT COUNT(DISTINCT facility_id) FROM {table_name}").fetchone()[0]
    states = con.execute(f"SELECT COUNT(DISTINCT state) FROM {table_name}").fetchone()[0]
    measures = con.execute(f"SELECT COUNT(DISTINCT measure_id) FROM {table_name}").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {states} states, {measures} measures")
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    return count


def build_hai_hospital(con, dry_run):
    return _build_hospital_measures(con, dry_run,
        "hai_hospital_feb2026.csv", "_hai2", "hai_hospital2", "HAI Hospital",
        has_compared=True)


def build_complications(con, dry_run):
    return _build_hospital_measures(con, dry_run,
        "complications_hospital.csv", "_comp", "complications_hosp", "Complications",
        has_compared=True, has_denominator=True, has_lower_upper=True)


def build_timely_effective(con, dry_run):
    return _build_hospital_measures(con, dry_run,
        "timely_effective_hospital.csv", "_te", "timely_effective_hosp", "Timely & Effective",
        has_condition=True, has_sample=True)


def build_unplanned_visits(con, dry_run):
    return _build_hospital_measures(con, dry_run,
        "unplanned_visits_hospital.csv", "_uv", "unplanned_visits_hosp", "Unplanned Visits",
        has_compared=True, has_denominator=True, has_lower_upper=True)


def build_psi90(con, dry_run):
    return _build_hospital_measures(con, dry_run,
        "psi90_hospital.csv", "_psi", "psi90_hospital", "PSI-90",
        rate_col="Rate")


def build_snf_vbp(con, dry_run):
    """SNF Value-Based Purchasing facility performance."""
    print("Building fact_snf_vbp...")
    csv_path = RAW_DIR / "snf_vbp_facility.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _snf_vbp AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Provider Name" AS provider_name,
            "State" AS state,
            "ZIP Code" AS zip_code,
            TRY_CAST("SNF VBP Program Ranking" AS INTEGER) AS vbp_ranking,
            TRY_CAST(CASE WHEN "Baseline Period: FY 2022 Risk-Standardized Readmission Rate" IN ('', '--', 'N/A') THEN NULL
                ELSE "Baseline Period: FY 2022 Risk-Standardized Readmission Rate" END AS DOUBLE) AS baseline_readmission_rate,
            TRY_CAST(CASE WHEN "Performance Period: FY 2024 Risk-Standardized Readmission Rate" IN ('', '--', 'N/A') THEN NULL
                ELSE "Performance Period: FY 2024 Risk-Standardized Readmission Rate" END AS DOUBLE) AS performance_readmission_rate,
            TRY_CAST(CASE WHEN "SNFRM Achievement Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "SNFRM Achievement Score" END AS DOUBLE) AS snfrm_achievement_score,
            TRY_CAST(CASE WHEN "SNFRM Improvement Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "SNFRM Improvement Score" END AS DOUBLE) AS snfrm_improvement_score,
            TRY_CAST(CASE WHEN "Performance Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "Performance Score" END AS DOUBLE) AS performance_score,
            TRY_CAST(CASE WHEN "Incentive Payment Multiplier" IN ('', '--', 'N/A') THEN NULL
                ELSE "Incentive Payment Multiplier" END AS DOUBLE) AS incentive_multiplier,
            'cms_care_compare_snf_vbp' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_snf_vbp", _snapshot_path("snf_vbp"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _snf_vbp").fetchone()[0]
    avg_mult = con.execute("SELECT ROUND(AVG(incentive_multiplier), 4) FROM _snf_vbp WHERE incentive_multiplier IS NOT NULL").fetchone()[0]
    print(f"  {count:,} SNFs, {states} states, avg incentive multiplier: {avg_mult}")
    con.execute("DROP TABLE IF EXISTS _snf_vbp")
    return count


def build_nh_claims_quality(con, dry_run):
    """Nursing home claims-based quality measures."""
    print("Building fact_nh_claims_quality...")
    csv_path = RAW_DIR / "nh_claims_quality.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _nhcq AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Provider Name" AS provider_name,
            "State" AS state,
            "ZIP Code" AS zip_code,
            "Measure Code" AS measure_code,
            "Measure Description" AS measure_description,
            "Resident type" AS resident_type,
            TRY_CAST(CASE WHEN "Adjusted Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "Adjusted Score" END AS DOUBLE) AS adjusted_score,
            TRY_CAST(CASE WHEN "Observed Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "Observed Score" END AS DOUBLE) AS observed_score,
            TRY_CAST(CASE WHEN "Expected Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "Expected Score" END AS DOUBLE) AS expected_score,
            "Footnote for Score" AS footnote,
            "Measure Period" AS measure_period,
            'cms_care_compare_nh' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_nhcq", _snapshot_path("nh_claims_quality"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _nhcq").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _nhcq").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _nhcq")
    return count


def build_snf_quality_provider(con, dry_run):
    """SNF Quality Reporting Program provider-level data."""
    print("Building fact_snf_quality_provider...")
    csv_path = RAW_DIR / "snf_quality_provider.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _snfq AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Provider Name" AS provider_name,
            "State" AS state,
            "ZIP Code" AS zip_code,
            "CMS Region" AS cms_region,
            "Measure Code" AS measure_code,
            TRY_CAST(CASE WHEN "Score" IN ('', '--', 'N/A') THEN NULL
                ELSE "Score" END AS DOUBLE) AS score,
            "Footnote" AS footnote,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'cms_care_compare_snf' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") <= 2
    """)

    count = write_parquet(con, "_snfq", _snapshot_path("snf_quality_provider"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _snfq").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _snfq").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _snfq")
    return count


def build_nh_state_averages(con, dry_run):
    """Nursing home state and national quality averages."""
    print("Building fact_nh_state_averages...")
    csv_path = RAW_DIR / "nh_state_averages.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _nhavg AS
        SELECT
            "State or Nation" AS state_or_nation,
            TRY_CAST("Overall Rating" AS DOUBLE) AS overall_rating,
            TRY_CAST("Health Inspection Rating" AS DOUBLE) AS health_inspection_rating,
            TRY_CAST("QM Rating" AS DOUBLE) AS qm_rating,
            TRY_CAST("Staffing Rating" AS DOUBLE) AS staffing_rating,
            TRY_CAST("Cycle 1 Total Number of Health Deficiencies" AS DOUBLE) AS c1_health_deficiencies,
            TRY_CAST("Cycle 1 Total Number of Fire Safety Deficiencies" AS DOUBLE) AS c1_fire_deficiencies,
            TRY_CAST("Average Number of Residents per Day" AS DOUBLE) AS avg_residents_per_day,
            TRY_CAST("Reported Total Nurse Staffing Hours per Resident per Day" AS DOUBLE) AS total_nurse_hrs_per_res_day,
            TRY_CAST("Reported RN Staffing Hours per Resident per Day" AS DOUBLE) AS rn_hrs_per_res_day,
            TRY_CAST("Number of Fines" AS DOUBLE) AS num_fines,
            TRY_CAST("Fine Amount in Dollars" AS DOUBLE) AS fine_amount_dollars,
            TRY_CAST("Total nursing staff turnover" AS DOUBLE) AS nursing_turnover_pct,
            'cms_care_compare_nh' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE "State or Nation" IS NOT NULL
    """)

    count = write_parquet(con, "_nhavg", _snapshot_path("nh_state_averages"), dry_run)
    print(f"  {count} rows (states + national)")
    con.execute("DROP TABLE IF EXISTS _nhavg")
    return count


ALL_TABLES = {
    "hai": ("fact_hai_hospital2", build_hai_hospital),
    "complications": ("fact_complications_hosp", build_complications),
    "timely": ("fact_timely_effective_hosp", build_timely_effective),
    "unplanned": ("fact_unplanned_visits_hosp", build_unplanned_visits),
    "psi90": ("fact_psi90_hospital", build_psi90),
    "snf_vbp": ("fact_snf_vbp", build_snf_vbp),
    "nh_claims": ("fact_nh_claims_quality", build_nh_claims_quality),
    "snf_quality": ("fact_snf_quality_provider", build_snf_quality_provider),
    "nh_averages": ("fact_nh_state_averages", build_nh_state_averages),
}


def main():
    parser = argparse.ArgumentParser(description="Round 6 lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 6 — CMS Hospital + SNF Quality (Facility) — {SNAPSHOT_DATE}")
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
    print("ROUND 6 LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round6_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
