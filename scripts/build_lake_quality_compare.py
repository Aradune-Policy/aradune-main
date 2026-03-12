#!/usr/bin/env python3
"""
build_lake_quality_compare.py — Ingest CMS Care Compare provider-level quality datasets.

Downloads CSVs from data.cms.gov/provider-data and writes Parquet to the data lake.
Focuses on datasets where we have state-level only or limited measures, upgrading to
full provider-level quality data.

Tables built:
  fact_hcahps_hospital           — HCAHPS patient experience by hospital (~500K+ rows)
  fact_hh_quality_provider       — Home health OASIS quality measures by agency (~12K rows)
  fact_asc_quality_facility      — ASC quality measures, full (ASC-1 through ASC-19, ~5.7K rows)
  fact_timely_effective_hospital — Timely & Effective Care by hospital (~200K+ rows)
  fact_mspb_hospital             — Medicare Spending Per Beneficiary by hospital (~5K rows)
  fact_oas_cahps_hospital        — OAS CAHPS Hospital Outpatient by facility (~100K+ rows)

Usage:
  python3 scripts/build_lake_quality_compare.py
  python3 scripts/build_lake_quality_compare.py --dry-run
  python3 scripts/build_lake_quality_compare.py --only fact_hcahps_hospital
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
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

# ── Download URLs (from data.cms.gov provider-data catalog) ──────────────

DATASETS = {
    "hcahps_hospital": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/78a50346fbe828ea0ce2837847af6a7c_1770163580/HCAHPS-Hospital.csv",
        "filename": "hcahps_hospital.csv",
        "description": "Patient survey (HCAHPS) - Hospital",
    },
    "hh_quality_provider": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/d6258a04bfe1a4492ad2e80ca05572aa_1767204345/HH_Provider_Jan2026.csv",
        "filename": "hh_provider_quality.csv",
        "description": "Home Health Care Agencies (100 cols, OASIS quality)",
    },
    "asc_quality_facility": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/dd03994fc93e296bb0297f1cd43cc987_1770163552/ASC_Facility.csv",
        "filename": "asc_facility_quality.csv",
        "description": "Ambulatory Surgical Center Quality Measures - Facility",
    },
    "timely_effective_hospital": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/0437b5494ac61507ad90f2af6b8085a7_1770163650/Timely_and_Effective_Care-Hospital.csv",
        "filename": "timely_effective_hospital.csv",
        "description": "Timely and Effective Care - Hospital",
    },
    "mspb_hospital": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/69874ce604586980ac088283c1b35095_1770163639/Medicare_Hospital_Spending_Per_Patient-Hospital.csv",
        "filename": "mspb_hospital.csv",
        "description": "Medicare Spending Per Beneficiary - Hospital",
    },
    "oas_cahps_hospital": {
        "url": "https://data.cms.gov/provider-data/sites/default/files/resources/9189f27bb8ab7a4ff8919bdc682bf79a_1770163592/OQR_OAS_CAHPS_BY_HOSPITAL.csv",
        "filename": "oas_cahps_hospital.csv",
        "description": "OAS CAHPS Hospital Outpatient Departments - Facility",
    },
}


def download_csv(url: str, dest: Path) -> bool:
    """Download a CSV using curl (more reliable than urllib on macOS)."""
    if dest.exists() and dest.stat().st_size > 100:
        print(f"  Using cached {dest.name} ({dest.stat().st_size / 1024 / 1024:.1f} MB)")
        return True
    print(f"  Downloading {dest.name}...")
    result = subprocess.run(
        ["curl", "-sL", "-o", str(dest), url],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(f"  ERROR: curl failed: {result.stderr}")
        return False
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"  Downloaded {size_mb:.1f} MB")
    return size_mb > 0.001


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to Parquet with ZSTD compression."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


# ── Table builders ───────────────────────────────────────────────────────


def build_hcahps_hospital(con, csv_path: Path, dry_run: bool) -> int:
    """HCAHPS patient experience survey scores by hospital."""
    print("Building fact_hcahps_hospital...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hcahps_hospital AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "HCAHPS Measure ID" AS measure_id,
            "HCAHPS Question" AS question,
            "HCAHPS Answer Description" AS answer_description,
            TRY_CAST("Patient Survey Star Rating" AS INTEGER) AS star_rating,
            TRY_CAST("HCAHPS Answer Percent" AS DOUBLE) AS answer_pct,
            TRY_CAST("HCAHPS Linear Mean Value" AS DOUBLE) AS linear_mean_value,
            TRY_CAST("Number of Completed Surveys" AS INTEGER) AS completed_surveys,
            TRY_CAST("Survey Response Rate Percent" AS DOUBLE) AS response_rate_pct,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con, "_fact_hcahps_hospital", _snapshot_path("hcahps_hospital"), dry_run
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_hcahps_hospital"
    ).fetchone()[0]
    facilities = con.execute(
        "SELECT COUNT(DISTINCT facility_id) FROM _fact_hcahps_hospital"
    ).fetchone()[0]
    measures = con.execute(
        "SELECT COUNT(DISTINCT measure_id) FROM _fact_hcahps_hospital"
    ).fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {measures} measures, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_hcahps_hospital")
    return count


def build_hh_quality_provider(con, csv_path: Path, dry_run: bool) -> int:
    """Home Health OASIS quality measures by agency (100 columns)."""
    print("Building fact_hh_quality_provider...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hh_quality_provider AS
        SELECT
            "CMS Certification Number (CCN)" AS ccn,
            "Provider Name" AS facility_name,
            "State" AS state_code,
            "City/Town" AS city,
            "ZIP Code" AS zip_code,
            "Type of Ownership" AS ownership_type,
            TRY_CAST("Quality of patient care star rating" AS DOUBLE) AS quality_star_rating,

            -- Timely care
            TRY_CAST("How often the home health team began their patients' care in a timely manner" AS DOUBLE) AS pct_timely_care,

            -- Flu shot
            TRY_CAST("How often the home health team determined whether patients received a flu shot for the current flu season" AS DOUBLE) AS pct_flu_shot,

            -- Functional outcomes
            TRY_CAST("How often patients got better at walking or moving around" AS DOUBLE) AS pct_better_walking,
            TRY_CAST("How often patients got better at getting in and out of bed" AS DOUBLE) AS pct_better_bed_transfer,
            TRY_CAST("How often patients got better at bathing" AS DOUBLE) AS pct_better_bathing,
            TRY_CAST("How often patients' breathing improved" AS DOUBLE) AS pct_breathing_improved,
            TRY_CAST("How often patients got better at taking their drugs correctly by mouth" AS DOUBLE) AS pct_better_medications,

            -- Skin integrity
            TRY_CAST("Changes in skin integrity post-acute care: pressure ulcer/injury" AS DOUBLE) AS pct_pressure_ulcer,

            -- Medication reconciliation
            TRY_CAST("How often physician-recommended actions to address medication issues were completely timely" AS DOUBLE) AS pct_medication_issues_timely,

            -- Falls
            TRY_CAST("Percent of Residents Experiencing One or More Falls with Major Injury" AS DOUBLE) AS pct_falls_major_injury,

            -- Discharge function
            TRY_CAST("Discharge Function Score" AS DOUBLE) AS discharge_function_score,

            -- Transfer of health info
            TRY_CAST("Transfer of Health Information to the Provider" AS DOUBLE) AS transfer_info_to_provider,
            TRY_CAST("Transfer of Health Information to the Patient" AS DOUBLE) AS transfer_info_to_patient,

            -- COVID vaccine
            TRY_CAST("COVID-19 Vaccine: Percent of Patients Who are Up to Date" AS DOUBLE) AS pct_covid_vaccine,

            -- DTC (Discharge to Community)
            TRY_CAST("DTC Observed Rate" AS DOUBLE) AS dtc_observed_rate,
            TRY_CAST("DTC Risk-Standardized Rate" AS DOUBLE) AS dtc_risk_std_rate,
            "DTC Performance Categorization" AS dtc_performance,

            -- PPR (Potentially Preventable Readmissions)
            TRY_CAST("PPR Observed Rate" AS DOUBLE) AS ppr_observed_rate,
            TRY_CAST("PPR Risk-Standardized Rate" AS DOUBLE) AS ppr_risk_std_rate,
            "PPR Performance Categorization" AS ppr_performance,

            -- PPH (Potentially Preventable Hospitalizations)
            TRY_CAST("PPH Observed Rate" AS DOUBLE) AS pph_observed_rate,
            TRY_CAST("PPH Risk-Standardized Rate" AS DOUBLE) AS pph_risk_std_rate,
            "PPH Performance Categorization" AS pph_performance,

            -- Medicare spending
            TRY_CAST("How much Medicare spends on an episode of care at this agency, compared to Medicare spending across all agencies nationally" AS DOUBLE) AS medicare_spending_ratio,

            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con, "_fact_hh_quality_provider", _snapshot_path("hh_quality_provider"), dry_run
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_hh_quality_provider"
    ).fetchone()[0]
    providers = con.execute(
        "SELECT COUNT(DISTINCT ccn) FROM _fact_hh_quality_provider"
    ).fetchone()[0]
    print(f"  {count:,} rows, {providers:,} providers, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_hh_quality_provider")
    return count


def build_asc_quality_facility(con, csv_path: Path, dry_run: bool) -> int:
    """ASC quality measures, full set (ASC-1 through ASC-19)."""
    print("Building fact_asc_quality_facility...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_asc_quality_facility AS
        SELECT
            "Facility Name" AS facility_name,
            "Facility ID" AS facility_id,
            "NPI" AS npi,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            TRY_CAST("Year" AS INTEGER) AS year,

            -- Patient burn measures
            TRY_CAST("ASC-1 Rate*" AS DOUBLE) AS asc1_rate,
            TRY_CAST("ASC-2 Rate*" AS DOUBLE) AS asc2_rate,
            TRY_CAST("ASC-3 Rate*" AS DOUBLE) AS asc3_rate,
            TRY_CAST("ASC-4 Rate*" AS DOUBLE) AS asc4_rate,

            -- Falls and wrong-site
            TRY_CAST("ASC-9 Rate*" AS DOUBLE) AS asc9_rate,
            TRY_CAST("ASC-11 Rate*" AS DOUBLE) AS asc11_rate,

            -- Hospital visits after colonoscopy
            TRY_CAST("ASC-12 Total Cases" AS INTEGER) AS asc12_total_cases,
            "ASC-12 Performance Category" AS asc12_performance,
            TRY_CAST("ASC-12 RSHV Rate" AS DOUBLE) AS asc12_rshv_rate,
            TRY_CAST("ASC-12 Interval Lower Limit" AS DOUBLE) AS asc12_lower,
            TRY_CAST("ASC-12 Interval Upper Limit" AS DOUBLE) AS asc12_upper,

            -- Normothermia
            TRY_CAST("ASC-13 Rate*" AS DOUBLE) AS asc13_rate,

            -- Unplanned anterior vitrectomy
            TRY_CAST("ASC-14 Rate*" AS DOUBLE) AS asc14_rate,

            -- Hospital visits after orthopedic
            TRY_CAST("ASC-17 Total Cases" AS INTEGER) AS asc17_total_cases,
            "ASC-17 Performance Category" AS asc17_performance,
            TRY_CAST("ASC-17 RSHV Rate" AS DOUBLE) AS asc17_rshv_rate,
            TRY_CAST("ASC-17 Interval Lower Limit" AS DOUBLE) AS asc17_lower,
            TRY_CAST("ASC-17 Interval Upper Limit" AS DOUBLE) AS asc17_upper,

            -- Hospital visits after urology
            TRY_CAST("ASC-18 Total Cases" AS INTEGER) AS asc18_total_cases,
            "ASC-18 Performance Category" AS asc18_performance,
            TRY_CAST("ASC-18 RSHV Rate" AS DOUBLE) AS asc18_rshv_rate,
            TRY_CAST("ASC-18 Interval Lower Limit" AS DOUBLE) AS asc18_lower,
            TRY_CAST("ASC-18 Interval Upper Limit" AS DOUBLE) AS asc18_upper,

            -- Facility-level risk-standardized hospital visit after outpatient surgery
            TRY_CAST("ASC-19 Total Cases" AS INTEGER) AS asc19_total_cases,
            "ASC-19 Performance Category" AS asc19_performance,
            TRY_CAST("ASC-19 RSHV Rate" AS DOUBLE) AS asc19_rshv_rate,
            TRY_CAST("ASC-19 Interval Lower Limit" AS DOUBLE) AS asc19_lower,
            TRY_CAST("ASC-19 Interval Upper Limit" AS DOUBLE) AS asc19_upper,

            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con, "_fact_asc_quality_facility", _snapshot_path("asc_quality_facility"), dry_run
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_asc_quality_facility"
    ).fetchone()[0]
    facilities = con.execute(
        "SELECT COUNT(DISTINCT facility_id) FROM _fact_asc_quality_facility"
    ).fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_asc_quality_facility")
    return count


def build_timely_effective_hospital(con, csv_path: Path, dry_run: bool) -> int:
    """Timely and Effective Care measures by hospital."""
    print("Building fact_timely_effective_hospital...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_timely_effective_hospital AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Condition" AS condition,
            "Measure ID" AS measure_id,
            "Measure Name" AS measure_name,
            TRY_CAST("Score" AS DOUBLE) AS score,
            TRY_CAST("Sample" AS INTEGER) AS sample_size,
            "Footnote" AS footnote,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con,
        "_fact_timely_effective_hospital",
        _snapshot_path("timely_effective_hospital"),
        dry_run,
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_timely_effective_hospital"
    ).fetchone()[0]
    facilities = con.execute(
        "SELECT COUNT(DISTINCT facility_id) FROM _fact_timely_effective_hospital"
    ).fetchone()[0]
    measures = con.execute(
        "SELECT COUNT(DISTINCT measure_id) FROM _fact_timely_effective_hospital"
    ).fetchone()[0]
    print(
        f"  {count:,} rows, {facilities:,} facilities, {measures} measures, {states} states"
    )
    con.execute("DROP TABLE IF EXISTS _fact_timely_effective_hospital")
    return count


def build_mspb_hospital(con, csv_path: Path, dry_run: bool) -> int:
    """Medicare Spending Per Beneficiary by hospital."""
    print("Building fact_mspb_hospital...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mspb_hospital AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "Measure ID" AS measure_id,
            "Measure Name" AS measure_name,
            TRY_CAST("Score" AS DOUBLE) AS score,
            "Footnote" AS footnote,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con, "_fact_mspb_hospital", _snapshot_path("mspb_hospital"), dry_run
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_mspb_hospital"
    ).fetchone()[0]
    facilities = con.execute(
        "SELECT COUNT(DISTINCT facility_id) FROM _fact_mspb_hospital"
    ).fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} facilities, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_mspb_hospital")
    return count


def build_oas_cahps_hospital(con, csv_path: Path, dry_run: bool) -> int:
    """OAS CAHPS survey for hospital outpatient departments by facility."""
    print("Building fact_oas_cahps_hospital...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_oas_cahps_hospital AS
        SELECT
            "Facility ID" AS facility_id,
            "Facility Name" AS facility_name,
            "City/Town" AS city,
            "State" AS state_code,
            "ZIP Code" AS zip_code,
            "County/Parish" AS county,
            "OAS CAHPS Measure ID" AS measure_id,
            "OAS CAHPS Question" AS question,
            "OAS CAHPS Answer Description" AS answer_description,
            TRY_CAST("OAS CAHPS Answer Percent" AS DOUBLE) AS answer_pct,
            TRY_CAST("OAS CAHPS Linear Mean Value" AS DOUBLE) AS linear_mean_value,
            TRY_CAST("Number of Completed Surveys" AS INTEGER) AS completed_surveys,
            TRY_CAST("Survey Response Rate Percent" AS DOUBLE) AS response_rate_pct,
            "Start Date" AS start_date,
            "End Date" AS end_date,
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
        WHERE "State" IS NOT NULL AND LENGTH(TRIM("State")) = 2
    """)
    count = write_parquet(
        con, "_fact_oas_cahps_hospital", _snapshot_path("oas_cahps_hospital"), dry_run
    )
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM _fact_oas_cahps_hospital"
    ).fetchone()[0]
    facilities = con.execute(
        "SELECT COUNT(DISTINCT facility_id) FROM _fact_oas_cahps_hospital"
    ).fetchone()[0]
    measures = con.execute(
        "SELECT COUNT(DISTINCT measure_id) FROM _fact_oas_cahps_hospital"
    ).fetchone()[0]
    print(
        f"  {count:,} rows, {facilities:,} facilities, {measures} measures, {states} states"
    )
    con.execute("DROP TABLE IF EXISTS _fact_oas_cahps_hospital")
    return count


# ── Main ─────────────────────────────────────────────────────────────────

BUILDERS = {
    "fact_hcahps_hospital": ("hcahps_hospital", build_hcahps_hospital),
    "fact_hh_quality_provider": ("hh_quality_provider", build_hh_quality_provider),
    "fact_asc_quality_facility": ("asc_quality_facility", build_asc_quality_facility),
    "fact_timely_effective_hospital": (
        "timely_effective_hospital",
        build_timely_effective_hospital,
    ),
    "fact_mspb_hospital": ("mspb_hospital", build_mspb_hospital),
    "fact_oas_cahps_hospital": ("oas_cahps_hospital", build_oas_cahps_hospital),
}


def main():
    parser = argparse.ArgumentParser(
        description="Ingest CMS Care Compare provider-level quality data"
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--only",
        type=str,
        help="Build only this table (e.g. fact_hcahps_hospital)",
    )
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Determine which tables to build
    if args.only:
        if args.only not in BUILDERS:
            print(f"ERROR: Unknown table '{args.only}'. Options: {list(BUILDERS.keys())}")
            sys.exit(1)
        build_list = {args.only: BUILDERS[args.only]}
    else:
        build_list = BUILDERS

    # Download all needed CSVs
    print("=" * 60)
    print("DOWNLOADING CSVs")
    print("=" * 60)
    csv_paths = {}
    for fact_name, (ds_key, _) in build_list.items():
        ds = DATASETS[ds_key]
        csv_path = RAW_DIR / ds["filename"]
        print(f"\n{ds['description']}:")
        if download_csv(ds["url"], csv_path):
            csv_paths[fact_name] = csv_path
        else:
            print(f"  SKIPPED — download failed")

    # Build tables
    print()
    print("=" * 60)
    print("BUILDING PARQUET TABLES")
    print("=" * 60)

    con = duckdb.connect()
    totals = {}

    for fact_name, (ds_key, builder_fn) in build_list.items():
        if fact_name not in csv_paths:
            print(f"\nSkipping {fact_name} — no CSV available")
            continue
        print()
        try:
            totals[fact_name] = builder_fn(con, csv_paths[fact_name], args.dry_run)
        except Exception as e:
            print(f"  ERROR building {fact_name}: {e}")
            totals[fact_name] = 0

    con.close()

    # Summary
    print()
    print("=" * 60)
    print("CARE COMPARE QUALITY DATA INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>12,} rows")

    # Write manifest
    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source": "data.cms.gov/provider-data (Care Compare)",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_quality_compare_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    # Remind about db.py
    if not args.dry_run and total_rows > 0:
        new_facts = [
            name.replace("fact_", "")
            for name, count in totals.items()
            if count > 0
        ]
        print(f"\n  REMINDER: Add these to server/db.py fact_names list:")
        for f in new_facts:
            print(f'    "{f}",')

    print("\nDone.")


if __name__ == "__main__":
    main()
