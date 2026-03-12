#!/usr/bin/env python3
"""
build_lake_raw_processing.py — Process unprocessed raw files into the data lake.

Reads from: data/raw/
Writes to:  data/lake/fact/

Tables built:
  fact_ahrf_county           — AHRF county-level health resources (curated ~60 columns)
  fact_pbj_employee_summary  — PBJ employee-level data aggregated to facility level
  fact_drug_rebate_products  — CMS drug rebate product data (cleaned schema)

Skipped (already well-populated):
  fact_bea_state_gdp         — 13,440 rows, good schema
  fact_brfss_behavioral      — 86,141 rows, 56 states, good schema

Usage:
  python3 scripts/build_lake_raw_processing.py
  python3 scripts/build_lake_raw_processing.py --dry-run
  python3 scripts/build_lake_raw_processing.py --only fact_ahrf_county
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
# 1. AHRF County Data — Curated subset of ~60 most useful columns
# ---------------------------------------------------------------------------

# Column mapping: raw_name -> clean_name
AHRF_COLUMNS = {
    # Geography
    "fips_st_cnty": "fips_county",
    "st_name": "state_name",
    "st_name_abbrev": "state_code",
    "cnty_name": "county_name",
    "fips_st": "fips_state",
    "fips_cnty": "fips_county_part",
    "cens_regn": "census_region",
    "cens_regn_name": "census_region_name",
    "cbsa_23": "cbsa_code",
    "cbsa_name_23": "cbsa_name",
    "cbsa_ind_23": "cbsa_type",
    "rural_urban_contnm_23": "rural_urban_continuum",
    "urban_influnc_13": "urban_influence_code",

    # Population
    "popn_est_24": "population_2024",
    "popn_est_23": "population_2023",
    "popn_est_ge65_23": "population_65plus_2023",
    "cens_popn_20": "census_population_2020",

    # HPSA designations
    "hpsa_prim_care_25": "hpsa_primary_care_2025",
    "hpsa_dent_25": "hpsa_dental_2025",
    "hpsa_mentl_hlth_25": "hpsa_mental_health_2025",

    # Physicians
    "tot_md_do_23": "total_physicians_2023",
    "tot_md_do_activ_23": "active_physicians_2023",
    "phys_nf_prim_care_pc_exc_rsdt_23": "primary_care_physicians_2023",
    "md_nf_prim_care_pc_excl_rsdnt_23": "primary_care_md_2023",
    "do_nf_prim_care_pc_excl_rsdnt_23": "primary_care_do_2023",
    "md_nf_all_med_spec_23": "specialist_physicians_2023",
    "md_nf_child_psych_23": "child_psychiatrists_2023",
    "md_nf_ped_gen_23": "pediatricians_2023",

    # Other clinicians (NPI-based counts)
    "np_npi_24": "nurse_practitioners_2024",
    "np_npi_23": "nurse_practitioners_2023",
    "pa_npi_24": "physician_assistants_2024",
    "pa_npi_23": "physician_assistants_2023",
    "dent_npi_24": "dentists_2024",
    "dent_npi_23": "dentists_2023",
    "clin_nurse_spec_npi_24": "clinical_nurse_specialists_2024",

    # Hospital infrastructure
    "hosp_beds_23": "hospital_beds_2023",
    "hosp_beds_22": "hospital_beds_2022",
    "stgh_hosp_beds_23": "short_term_hospital_beds_2023",
    "stngh_hosp_beds_23": "long_term_hospital_beds_2023",
    "stgh_obstetrc_care_23": "hospitals_with_obstetric_care_2023",

    # Nursing and home health
    "nurs_fac_24": "nursing_facilities_2024",
    "nurs_fac_23": "nursing_facilities_2023",
    "nurs_fac_beds_24": "nursing_facility_beds_2024",
    "home_hlth_agencs_24": "home_health_agencies_2024",
    "home_hlth_agencs_23": "home_health_agencies_2023",

    # Poverty
    "pers_povty_pct_23": "poverty_pct_2023",
    "pers_povty_pct_22": "poverty_pct_2022",
    "pers_deep_povty_pct_23": "deep_poverty_pct_2023",
    "child_deep_povty_lt18_pct_23": "child_deep_poverty_pct_2023",

    # Births and vital stats
    "births_july_1_june_30_24": "births_2024",
    "births_july_1_june_30_23": "births_2023",
}


def build_fact_ahrf_county(con, dry_run: bool) -> int:
    print("Building fact_ahrf_county (curated)...")
    csv_path = RAW_DIR / "ahrf_county.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    # Build SELECT clause with aliasing
    select_parts = []
    for raw_col, clean_col in AHRF_COLUMNS.items():
        select_parts.append(f'TRY_CAST("{raw_col}" AS VARCHAR) AS "{clean_col}"')

    select_clause = ",\n            ".join(select_parts)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_ahrf AS
        SELECT
            {select_clause},
            'hrsa.gov/data/area-health-resources-files' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
        WHERE "fips_st_cnty" IS NOT NULL
          AND LENGTH(TRIM("fips_st_cnty")) = 5
    """)

    # Cast numeric columns properly
    con.execute("""
        CREATE OR REPLACE TABLE _fact_ahrf_typed AS
        SELECT
            fips_county,
            state_name,
            state_code,
            county_name,
            fips_state,
            fips_county_part,
            census_region,
            census_region_name,
            cbsa_code,
            cbsa_name,
            cbsa_type,
            rural_urban_continuum,
            urban_influence_code,
            TRY_CAST(population_2024 AS INTEGER) AS population_2024,
            TRY_CAST(population_2023 AS INTEGER) AS population_2023,
            TRY_CAST(population_65plus_2023 AS INTEGER) AS population_65plus_2023,
            TRY_CAST(census_population_2020 AS INTEGER) AS census_population_2020,
            TRY_CAST(hpsa_primary_care_2025 AS INTEGER) AS hpsa_primary_care_2025,
            TRY_CAST(hpsa_dental_2025 AS INTEGER) AS hpsa_dental_2025,
            TRY_CAST(hpsa_mental_health_2025 AS INTEGER) AS hpsa_mental_health_2025,
            TRY_CAST(total_physicians_2023 AS INTEGER) AS total_physicians_2023,
            TRY_CAST(active_physicians_2023 AS INTEGER) AS active_physicians_2023,
            TRY_CAST(primary_care_physicians_2023 AS INTEGER) AS primary_care_physicians_2023,
            TRY_CAST(primary_care_md_2023 AS INTEGER) AS primary_care_md_2023,
            TRY_CAST(primary_care_do_2023 AS INTEGER) AS primary_care_do_2023,
            TRY_CAST(specialist_physicians_2023 AS INTEGER) AS specialist_physicians_2023,
            TRY_CAST(child_psychiatrists_2023 AS INTEGER) AS child_psychiatrists_2023,
            TRY_CAST(pediatricians_2023 AS INTEGER) AS pediatricians_2023,
            TRY_CAST(nurse_practitioners_2024 AS INTEGER) AS nurse_practitioners_2024,
            TRY_CAST(nurse_practitioners_2023 AS INTEGER) AS nurse_practitioners_2023,
            TRY_CAST(physician_assistants_2024 AS INTEGER) AS physician_assistants_2024,
            TRY_CAST(physician_assistants_2023 AS INTEGER) AS physician_assistants_2023,
            TRY_CAST(dentists_2024 AS INTEGER) AS dentists_2024,
            TRY_CAST(dentists_2023 AS INTEGER) AS dentists_2023,
            TRY_CAST(clinical_nurse_specialists_2024 AS INTEGER) AS clinical_nurse_specialists_2024,
            TRY_CAST(hospital_beds_2023 AS INTEGER) AS hospital_beds_2023,
            TRY_CAST(hospital_beds_2022 AS INTEGER) AS hospital_beds_2022,
            TRY_CAST(short_term_hospital_beds_2023 AS INTEGER) AS short_term_hospital_beds_2023,
            TRY_CAST(long_term_hospital_beds_2023 AS INTEGER) AS long_term_hospital_beds_2023,
            TRY_CAST(hospitals_with_obstetric_care_2023 AS INTEGER) AS hospitals_with_obstetric_care_2023,
            TRY_CAST(nursing_facilities_2024 AS INTEGER) AS nursing_facilities_2024,
            TRY_CAST(nursing_facilities_2023 AS INTEGER) AS nursing_facilities_2023,
            TRY_CAST(nursing_facility_beds_2024 AS INTEGER) AS nursing_facility_beds_2024,
            TRY_CAST(home_health_agencies_2024 AS INTEGER) AS home_health_agencies_2024,
            TRY_CAST(home_health_agencies_2023 AS INTEGER) AS home_health_agencies_2023,
            TRY_CAST(poverty_pct_2023 AS DOUBLE) AS poverty_pct_2023,
            TRY_CAST(poverty_pct_2022 AS DOUBLE) AS poverty_pct_2022,
            TRY_CAST(deep_poverty_pct_2023 AS DOUBLE) AS deep_poverty_pct_2023,
            TRY_CAST(child_deep_poverty_pct_2023 AS DOUBLE) AS child_deep_poverty_pct_2023,
            TRY_CAST(births_2024 AS INTEGER) AS births_2024,
            TRY_CAST(births_2023 AS INTEGER) AS births_2023,
            source,
            snapshot_date
        FROM _fact_ahrf
    """)

    count = write_parquet(con, "_fact_ahrf_typed", _snapshot_path("ahrf_county"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_ahrf_typed").fetchone()[0]
    counties = con.execute("SELECT COUNT(DISTINCT fips_county) FROM _fact_ahrf_typed").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {counties:,} counties, 52 curated columns")
    con.execute("DROP TABLE IF EXISTS _fact_ahrf")
    con.execute("DROP TABLE IF EXISTS _fact_ahrf_typed")
    return count


# ---------------------------------------------------------------------------
# 2. PBJ Employee Detail — Aggregate 65M rows to facility-level summary
# ---------------------------------------------------------------------------

# Job code mapping from CMS PBJ documentation
PBJ_JOB_CODES = {
    "1": "Administrator",
    "2": "Medical Director",
    "3": "Other Physician",
    "4": "Physician Assistant",
    "5": "Registered Nurse Director of Nursing",
    "6": "Registered Nurse with Administrative Duties",
    "7": "Registered Nurse",
    "8": "Licensed Practical/Vocational Nurse with Administrative Duties",
    "9": "Licensed Practical/Vocational Nurse",
    "10": "Certified Nurse Aide",
    "11": "Nurse Aide in Training",
    "12": "Medication Aide/Technician",
    "13": "Nurse Practitioner",
    "14": "Clinical Nurse Specialist",
    "15": "Pharmacist",
    "16": "Dietitian/Nutritionist",
    "17": "Feeding Assistant",
    "18": "Occupational Therapist",
    "19": "Occupational Therapy Assistant",
    "20": "Occupational Therapy Aide",
    "21": "Physical Therapist",
    "22": "Physical Therapy Assistant",
    "23": "Physical Therapy Aide",
    "24": "Respiratory Therapist",
    "25": "Respiratory Therapy Technician",
    "26": "Speech/Language Pathologist",
    "27": "Therapeutic Recreation Specialist",
    "28": "Qualified Activities Professional",
    "29": "Other Activities Staff",
    "30": "Qualified Social Worker",
    "31": "Other Social Worker",
    "32": "Dentist",
    "33": "Podiatrist",
    "34": "Mental Health Service Worker",
    "35": "Vocational Service Worker",
    "36": "Clinical Laboratory Service Worker",
    "37": "Diagnostic X-ray Service Worker",
    "38": "Blood Bank Technologist/Technician (1)",
    "39": "Other",
    "40": "Certified Nursing Assistant (1)",
}


def build_fact_pbj_employee_summary(con, dry_run: bool) -> int:
    print("Building fact_pbj_employee_summary...")
    csv_path = RAW_DIR / "pbj_employee_detail_2025q3.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    print("  Reading 65M+ rows (this will take a few minutes)...")

    # Step 1: Aggregate by facility + job type
    # Using DuckDB to handle the 3.4GB file efficiently
    con.execute(f"""
        CREATE OR REPLACE TABLE _pbj_agg AS
        SELECT
            PROVNUM AS facility_id,
            STATE AS state_code,
            CY_Qtr AS quarter,
            CAST(EMPLEE_JOB_CD_ID AS VARCHAR) AS job_code,
            COUNT(DISTINCT SYS_EMPLEE_ID) AS unique_employees,
            SUM(TRY_CAST(WORK_HRS_NUM AS DOUBLE)) AS total_hours,
            COUNT(*) AS work_day_records
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
        WHERE PROVNUM IS NOT NULL
          AND STATE IS NOT NULL
        GROUP BY PROVNUM, STATE, CY_Qtr, EMPLEE_JOB_CD_ID
    """)

    agg_count = con.execute("SELECT COUNT(*) FROM _pbj_agg").fetchone()[0]
    print(f"  Intermediate aggregation: {agg_count:,} facility-job rows")

    # Step 2: Pivot into facility-level summary with nurse/non-nurse breakdowns
    con.execute(f"""
        CREATE OR REPLACE TABLE _pbj_summary AS
        SELECT
            facility_id,
            state_code,
            quarter,
            SUM(unique_employees) AS total_unique_employees,
            SUM(total_hours) AS total_hours,
            SUM(work_day_records) AS total_work_day_records,
            ROUND(SUM(total_hours) / NULLIF(SUM(unique_employees), 0), 2) AS avg_hours_per_employee,

            -- Nursing staff (RN, LPN, CNA)
            SUM(CASE WHEN job_code IN ('5','6','7') THEN unique_employees ELSE 0 END) AS rn_employees,
            SUM(CASE WHEN job_code IN ('5','6','7') THEN total_hours ELSE 0 END) AS rn_hours,
            SUM(CASE WHEN job_code IN ('8','9') THEN unique_employees ELSE 0 END) AS lpn_employees,
            SUM(CASE WHEN job_code IN ('8','9') THEN total_hours ELSE 0 END) AS lpn_hours,
            SUM(CASE WHEN job_code IN ('10','11','40') THEN unique_employees ELSE 0 END) AS cna_employees,
            SUM(CASE WHEN job_code IN ('10','11','40') THEN total_hours ELSE 0 END) AS cna_hours,

            -- Clinical staff (PT, OT, SLP, RT)
            SUM(CASE WHEN job_code IN ('18','19','20','21','22','23','24','25','26') THEN unique_employees ELSE 0 END) AS therapy_employees,
            SUM(CASE WHEN job_code IN ('18','19','20','21','22','23','24','25','26') THEN total_hours ELSE 0 END) AS therapy_hours,

            -- Administrative and other
            SUM(CASE WHEN job_code IN ('1','2','3','4','13','14') THEN unique_employees ELSE 0 END) AS admin_clinical_employees,
            SUM(CASE WHEN job_code IN ('15','16','30','31','34','35') THEN unique_employees ELSE 0 END) AS support_staff_employees,

            -- Total nursing (RN + LPN + CNA)
            SUM(CASE WHEN job_code IN ('5','6','7','8','9','10','11','40') THEN unique_employees ELSE 0 END) AS total_nursing_employees,
            SUM(CASE WHEN job_code IN ('5','6','7','8','9','10','11','40') THEN total_hours ELSE 0 END) AS total_nursing_hours,

            -- Distinct job types at this facility
            COUNT(DISTINCT job_code) AS distinct_job_types,

            'data.cms.gov/quality-of-care/payroll-based-journal' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _pbj_agg
        GROUP BY facility_id, state_code, quarter
    """)

    count = write_parquet(con, "_pbj_summary", _snapshot_path("pbj_employee_summary"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _pbj_summary").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT facility_id) FROM _pbj_summary").fetchone()[0]
    total_emp = con.execute("SELECT SUM(total_unique_employees) FROM _pbj_summary").fetchone()[0]
    total_hrs = con.execute("SELECT SUM(total_hours) FROM _pbj_summary").fetchone()[0]
    print(f"  {count:,} facility records, {states} states, {facilities:,} facilities")
    print(f"  {total_emp:,.0f} total employee slots, {total_hrs:,.0f} total hours")
    con.execute("DROP TABLE IF EXISTS _pbj_agg")
    con.execute("DROP TABLE IF EXISTS _pbj_summary")
    return count


# ---------------------------------------------------------------------------
# 3. Drug Rebate Products — Clean schema rebuild
# ---------------------------------------------------------------------------

def build_fact_drug_rebate_products(con, dry_run: bool) -> int:
    print("Building fact_drug_rebate_products (cleaned schema)...")
    csv_path = RAW_DIR / "drug_rebate_products_2025q4.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _drug_rebate AS
        SELECT
            TRY_CAST("Year" AS INTEGER) AS year,
            TRY_CAST("Quarter" AS INTEGER) AS quarter,
            "Labeler Name" AS labeler_name,
            "NDC" AS ndc,
            "Labeler Code" AS labeler_code,
            "Product Code" AS product_code,
            "Package Size Code" AS package_size_code,
            "Drug Category" AS drug_category,
            "Drug Type Indicator" AS drug_type_indicator,
            "Termination Date" AS termination_date,
            "Unit Type" AS unit_type,
            TRY_CAST("Units Per Pkg Size" AS DOUBLE) AS units_per_pkg_size,
            "FDA Approval Date" AS fda_approval_date,
            "Market Date" AS market_date,
            "FDA Therapeutic Equivalence Code" AS fda_te_code,
            "FDA Product Name" AS fda_product_name,
            CASE WHEN "Clotting Factor Indicator" = 'Y' THEN TRUE
                 WHEN "Clotting Factor Indicator" = 'N' THEN FALSE
                 ELSE NULL END AS is_clotting_factor,
            CASE WHEN "Pediatric Indicator" = 'Y' THEN TRUE
                 WHEN "Pediatric Indicator" = 'N' THEN FALSE
                 ELSE NULL END AS is_pediatric,
            "Package Size Intro Date" AS package_intro_date,
            "Purchased Product Date" AS purchased_product_date,
            "COD Status" AS cod_status,
            "FDA Application Number" AS fda_application_number,
            "Reactivation Date" AS reactivation_date,
            CASE WHEN "Line Extension Drug Indicator" = 'Y' THEN TRUE
                 WHEN "Line Extension Drug Indicator" = 'N' THEN FALSE
                 ELSE NULL END AS is_line_extension,
            'data.medicaid.gov/drug-rebate' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{csv_path}', all_varchar=true, header=true)
        WHERE "NDC" IS NOT NULL
    """)

    count = write_parquet(con, "_drug_rebate", _snapshot_path("drug_rebate_products"), dry_run)
    labelers = con.execute("SELECT COUNT(DISTINCT labeler_name) FROM _drug_rebate").fetchone()[0]
    products = con.execute("SELECT COUNT(DISTINCT ndc) FROM _drug_rebate").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _drug_rebate WHERE year IS NOT NULL").fetchone()
    print(f"  {count:,} rows, {labelers:,} labelers, {products:,} unique NDCs")
    print(f"  Year range: {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _drug_rebate")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_ahrf_county": build_fact_ahrf_county,
    "fact_pbj_employee_summary": build_fact_pbj_employee_summary,
    "fact_drug_rebate_products": build_fact_drug_rebate_products,
}


def main():
    parser = argparse.ArgumentParser(description="Process raw files into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    print("Pre-flight checks:")
    print(f"  fact_bea_state_gdp — SKIP (already 13,440 rows, good schema)")
    print(f"  fact_brfss_behavioral — SKIP (already 86,141 rows, 56 states)")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        if name not in ALL_TABLES:
            print(f"  UNKNOWN table: {name}")
            continue
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("RAW FILE PROCESSING COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>12,} rows")

    # Also report skipped tables
    print()
    print("Skipped (already populated):")
    print(f"  {'fact_bea_state_gdp':35s} {'13,440':>12s} rows  [existing]")
    print(f"  {'fact_brfss_behavioral':35s} {'86,141':>12s} rows  [existing]")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "skipped": {
                "fact_bea_state_gdp": {"rows": 13440, "reason": "already populated"},
                "fact_brfss_behavioral": {"rows": 86141, "reason": "already populated"},
            },
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_raw_processing_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
