#!/usr/bin/env python3
"""
build_lake_round3.py — Ingest T-MSIS derived Medicaid data + CMS provider data.

Sources:
  1. T-MSIS BH by condition (4,241 rows from data.medicaid.gov)
  2. T-MSIS MH/SUD service recipients (217 rows)
  3. T-MSIS maternal morbidity (435 rows)
  4. T-MSIS dental services (3,180 rows)
  5. T-MSIS telehealth services (12,720 rows)
  6. IRF provider data (79,365 rows from CMS)
  7. LTCH provider data (24,882 rows from CMS)
  8. Home Health Agencies (12,251 rows from CMS)

Tables built:
  fact_bh_by_condition       — Behavioral health conditions among Medicaid beneficiaries
  fact_mh_sud_recipients     — MH/SUD service recipients by state/subpopulation
  fact_maternal_morbidity    — Preterm birth and severe maternal morbidity
  fact_dental_services       — Dental services to children under 19
  fact_telehealth_services   — Telehealth utilization by state and type
  fact_irf_provider          — Inpatient Rehab Facility quality measures
  fact_ltch_provider         — Long-Term Care Hospital quality measures
  fact_home_health_agency    — Home health agencies with services and quality

Usage:
  python3 scripts/build_lake_round3.py
  python3 scripts/build_lake_round3.py --dry-run
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


def build_bh_by_condition(con, dry_run: bool) -> int:
    """BH conditions among Medicaid beneficiaries by state."""
    print("Building fact_bh_by_condition...")
    csv_path = RAW_DIR / "medicaid_bh_by_condition.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bh_cond AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Condition AS condition,
            TRY_CAST(REPLACE(REPLACE("Number of Beneficiaries", ',', ''), ' ', '') AS INTEGER) AS beneficiaries,
            TRY_CAST(REPLACE("Percent of beneficiaries with specific condition (out of those with any behavioral health condition in each state, by year)", '%', '') AS DOUBLE) AS pct_of_bh,
            "Data Quality" AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_bh_cond", _snapshot_path("bh_by_condition"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_bh_cond").fetchone()[0]
    conditions = con.execute("SELECT COUNT(DISTINCT condition) FROM _fact_bh_cond").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {conditions} conditions")

    con.execute("DROP TABLE IF EXISTS _fact_bh_cond")
    return count


def build_mh_sud_recipients(con, dry_run: bool) -> int:
    """MH/SUD service recipients from T-MSIS."""
    print("Building fact_mh_sud_recipients...")
    csv_path = RAW_DIR / "medicaid_mh_sud_recipients.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mh_sud AS
        SELECT
            TRY_CAST(Year AS INTEGER) AS year,
            Geography AS geography,
            "Subpopulation topic" AS subpop_topic,
            Subpopulation AS subpopulation,
            Category AS category,
            TRY_CAST(REPLACE("Count of enrollees", ',', '') AS INTEGER) AS enrollee_count,
            TRY_CAST(REPLACE("Denominator count of enrollees", ',', '') AS INTEGER) AS denominator,
            TRY_CAST("Percentage of enrollees" AS DOUBLE) AS pct_enrollees,
            "Data version" AS data_version,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE Year IS NOT NULL
    """)

    count = write_parquet(con, "_fact_mh_sud", _snapshot_path("mh_sud_recipients"), dry_run)
    years = con.execute("SELECT DISTINCT year FROM _fact_mh_sud ORDER BY year").fetchall()
    print(f"  {count} rows, years: {[y[0] for y in years]}")

    con.execute("DROP TABLE IF EXISTS _fact_mh_sud")
    return count


def build_maternal_morbidity(con, dry_run: bool) -> int:
    """Preterm birth and severe maternal morbidity among Medicaid births."""
    print("Building fact_maternal_morbidity...")
    csv_path = RAW_DIR / "medicaid_maternal_morbidity.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_maternal AS
        SELECT
            TRY_CAST(Year AS INTEGER) AS year,
            Geography AS geography,
            "Subpopulation topic" AS subpop_topic,
            Subpopulation AS subpopulation,
            Category AS category,
            TRY_CAST(REPLACE("Count of deliveries", ',', '') AS INTEGER) AS delivery_count,
            TRY_CAST(REPLACE("Denominator count of deliveries", ',', '') AS INTEGER) AS denominator,
            TRY_CAST("Rate of deliveries" AS DOUBLE) AS rate,
            "Data version" AS data_version,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE Year IS NOT NULL
    """)

    count = write_parquet(con, "_fact_maternal", _snapshot_path("maternal_morbidity"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT geography) FROM _fact_maternal WHERE geography != 'National'").fetchone()[0]
    print(f"  {count} rows, {states} states")

    # Key stats
    stats = con.execute("""
        SELECT category, ROUND(AVG(rate), 1), COUNT(DISTINCT geography)
        FROM _fact_maternal WHERE geography != 'National' AND year = 2022
        GROUP BY category
    """).fetchall()
    for s in stats:
        print(f"    {s[0]}: avg rate {s[1]}%, {s[2]} states")

    con.execute("DROP TABLE IF EXISTS _fact_maternal")
    return count


def build_dental_services(con, dry_run: bool) -> int:
    """Dental services to Medicaid/CHIP children under 19."""
    print("Building fact_dental_services...")
    csv_path = RAW_DIR / "medicaid_dental_services.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dental AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            DentalService AS dental_service,
            TRY_CAST(REPLACE(ServiceCount, ',', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_dental", _snapshot_path("dental_services"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_dental").fetchone()[0]
    services = con.execute("SELECT COUNT(DISTINCT dental_service) FROM _fact_dental").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {services} service types")

    con.execute("DROP TABLE IF EXISTS _fact_dental")
    return count


def build_telehealth_services(con, dry_run: bool) -> int:
    """Telehealth utilization by state, type, and month."""
    print("Building fact_telehealth_services...")
    csv_path = RAW_DIR / "medicaid_telehealth_services.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_telehealth AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            TelehealthType AS telehealth_type,
            ServiceType AS service_type,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_telehealth", _snapshot_path("telehealth_services"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_telehealth").fetchone()[0]
    types = con.execute("SELECT COUNT(DISTINCT telehealth_type) FROM _fact_telehealth").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {types} telehealth types")

    con.execute("DROP TABLE IF EXISTS _fact_telehealth")
    return count


def build_irf_provider(con, dry_run: bool) -> int:
    """Inpatient Rehabilitation Facility quality measures."""
    print("Building fact_irf_provider...")
    json_path = RAW_DIR / "irf_provider.json"
    if not json_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_irf AS
        SELECT
            cms_certification_number_ccn AS ccn,
            provider_name AS facility_name,
            state,
            citytown AS city,
            zip_code,
            countyparish AS county,
            measure_code,
            TRY_CAST(CASE WHEN score IN ('Not Available', '--', '') THEN NULL ELSE score END AS DOUBLE) AS score,
            footnote,
            start_date,
            end_date,
            'cms_care_compare_irf' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) <= 2
    """)

    count = write_parquet(con, "_fact_irf", _snapshot_path("irf_provider"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _fact_irf").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_irf").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_irf").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} IRFs, {states} states, {measures} measures")

    con.execute("DROP TABLE IF EXISTS _fact_irf")
    return count


def build_ltch_provider(con, dry_run: bool) -> int:
    """Long-Term Care Hospital quality measures."""
    print("Building fact_ltch_provider...")
    json_path = RAW_DIR / "ltch_provider.json"
    if not json_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_ltch AS
        SELECT
            cms_certification_number_ccn AS ccn,
            provider_name AS facility_name,
            state,
            citytown AS city,
            zip_code,
            countyparish AS county,
            measure_code,
            TRY_CAST(CASE WHEN score IN ('Not Available', '--', '') THEN NULL ELSE score END AS DOUBLE) AS score,
            footnote,
            start_date,
            end_date,
            'cms_care_compare_ltch' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) <= 2
    """)

    count = write_parquet(con, "_fact_ltch", _snapshot_path("ltch_provider"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _fact_ltch").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_ltch").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_ltch").fetchone()[0]
    print(f"  {count:,} rows, {facilities:,} LTCHs, {states} states, {measures} measures")

    con.execute("DROP TABLE IF EXISTS _fact_ltch")
    return count


def build_home_health_agency(con, dry_run: bool) -> int:
    """Home health agencies with services offered and quality ratings."""
    print("Building fact_home_health_agency...")
    json_path = RAW_DIR / "home_health_agencies.json"
    if not json_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hha AS
        SELECT
            cms_certification_number_ccn AS ccn,
            provider_name AS facility_name,
            state,
            citytown AS city,
            zip_code,
            type_of_ownership AS ownership_type,
            offers_nursing_care_services AS offers_nursing,
            offers_physical_therapy_services AS offers_pt,
            offers_occupational_therapy_services AS offers_ot,
            offers_speech_pathology_services AS offers_speech,
            offers_medical_social_services AS offers_social,
            offers_home_health_aide_services AS offers_hha,
            TRY_CAST(quality_of_patient_care_star_rating AS DOUBLE) AS quality_star_rating,
            certification_date,
            'cms_care_compare_hha' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{json_path}')
        WHERE state IS NOT NULL AND LENGTH(state) <= 2
    """)

    count = write_parquet(con, "_fact_hha", _snapshot_path("home_health_agency"), dry_run)
    facilities = con.execute("SELECT COUNT(DISTINCT ccn) FROM _fact_hha").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_hha").fetchone()[0]
    avg_quality = con.execute("SELECT ROUND(AVG(quality_star_rating), 2) FROM _fact_hha WHERE quality_star_rating IS NOT NULL").fetchone()[0]
    print(f"  {count:,} agencies, {states} states, avg quality rating: {avg_quality}")

    con.execute("DROP TABLE IF EXISTS _fact_hha")
    return count


def build_physical_among_mh(con, dry_run: bool) -> int:
    """Physical health conditions among Medicaid beneficiaries with MH conditions."""
    print("Building fact_physical_among_mh...")
    csv_path = RAW_DIR / "medicaid_physical_among_mh.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_phys_mh AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Condition AS condition,
            TRY_CAST(REPLACE(REPLACE("Number of Beneficiaries", ',', ''), ' ', '') AS INTEGER) AS beneficiaries,
            TRY_CAST(REPLACE("Percent of beneficiaries with specific condition (out of those with any mental health condition in each state, by year)", '%', '') AS DOUBLE) AS pct_of_mh,
            "Data Quality" AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_phys_mh", _snapshot_path("physical_among_mh"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_phys_mh").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_phys_mh")
    return count


def build_physical_among_sud(con, dry_run: bool) -> int:
    """Physical health conditions among Medicaid beneficiaries with SUD conditions."""
    print("Building fact_physical_among_sud...")
    csv_path = RAW_DIR / "medicaid_physical_among_sud.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_phys_sud AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Category AS sud_category,
            "Category value" AS sud_category_value,
            Condition AS condition,
            TRY_CAST(REPLACE(REPLACE("Number of Beneficiaries", ',', ''), ' ', '') AS INTEGER) AS beneficiaries,
            TRY_CAST(REPLACE("Percent of beneficiaries with specific condition (out of those with any SUD condition in each state, by year)", '%', '') AS DOUBLE) AS pct_of_sud,
            "Data Quality" AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_phys_sud", _snapshot_path("physical_among_sud"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_phys_sud").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_phys_sud")
    return count


ALL_TABLES = {
    "bh_condition": ("fact_bh_by_condition", build_bh_by_condition),
    "mh_sud": ("fact_mh_sud_recipients", build_mh_sud_recipients),
    "maternal": ("fact_maternal_morbidity", build_maternal_morbidity),
    "dental": ("fact_dental_services", build_dental_services),
    "telehealth": ("fact_telehealth_services", build_telehealth_services),
    "irf": ("fact_irf_provider", build_irf_provider),
    "ltch": ("fact_ltch_provider", build_ltch_provider),
    "hha": ("fact_home_health_agency", build_home_health_agency),
    "phys_mh": ("fact_physical_among_mh", build_physical_among_mh),
    "phys_sud": ("fact_physical_among_sud", build_physical_among_sud),
}


def main():
    parser = argparse.ArgumentParser(description="Ingest T-MSIS + CMS provider data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 3 — T-MSIS + CMS Provider Data — {SNAPSHOT_DATE}")
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
    print("ROUND 3 LAKE INGESTION COMPLETE")
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
        manifest_file = META_DIR / f"manifest_round3_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
