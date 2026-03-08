#!/usr/bin/env python3
"""
build_lake_providers_demographics.py — Ingest HRSA FQHCs, CDC vital stats,
CDC maternal mortality (national), and federal poverty guidelines into the lake.

Tables built:
  fact_fqhc_directory               — HRSA Health Center sites (18,809 rows)
  fact_vital_stats                  — CDC VSRR provisional births/deaths/infant deaths (1,980 rows)
  fact_maternal_mortality_national   — CDC provisional maternal mortality (810 rows)
  ref_poverty_guidelines            — Federal Poverty Guidelines 2025 (24 rows, reference table)

Usage:
  python3 scripts/build_lake_providers_demographics.py
  python3 scripts/build_lake_providers_demographics.py --dry-run
  python3 scripts/build_lake_providers_demographics.py --table fqhc
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
REF_DIR = LAKE_DIR / "reference"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

# Upper-case version for CDC VSRR (uses UPPERCASE state names)
STATE_NAME_UPPER_TO_CODE = {k.upper(): v for k, v in STATE_NAME_TO_CODE.items()}


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


def build_fqhc_directory(con, dry_run: bool) -> int:
    """Ingest HRSA Health Center (FQHC) site directory."""
    csv_path = RAW_DIR / "hrsa_health_centers.csv"
    if not csv_path.exists():
        print("  [skip] hrsa_health_centers.csv not found")
        return 0

    print("\n[1/4] HRSA Health Center Directory (FQHCs)")
    # HRSA CSV has unquoted header but quoted data rows + trailing comma;
    # must use read_csv with explicit options instead of read_csv_auto
    con.execute(f"""
        CREATE TABLE fqhc_directory AS
        SELECT
            TRIM("Health Center Type") AS health_center_type,
            TRIM("Health Center Number") AS health_center_number,
            TRIM("Site Name") AS site_name,
            TRIM("Site Address") AS site_address,
            TRIM("Site City") AS site_city,
            TRIM("Site State Abbreviation") AS state_code,
            TRIM("Site Postal Code") AS site_zip,
            TRIM("Site Telephone Number") AS site_phone,
            TRIM("Site Web Address") AS site_web_address,
            TRY_CAST("Operating Hours per Week" AS DOUBLE) AS operating_hours_per_week,
            TRIM("Health Center Service Delivery Site Location Setting Description") AS location_setting,
            TRIM("Site Status Description") AS site_status,
            TRIM("FQHC Site Medicare Billing Number") AS medicare_billing_number,
            TRIM("FQHC Site NPI Number") AS npi,
            TRIM("Health Center Location Type Description") AS location_type,
            TRIM("Health Center Type Description") AS type_description,
            TRIM("Health Center Operator Description") AS operator_description,
            TRIM("Health Center Operational Schedule Description") AS operational_schedule,
            TRIM("Health Center Operating Calendar") AS operating_calendar,
            TRIM("Health Center Name") AS health_center_name,
            TRIM("Health Center Organization Street Address") AS org_address,
            TRIM("Health Center Organization City") AS org_city,
            TRIM("Health Center Organization State") AS org_state,
            TRIM("Health Center Organization ZIP Code") AS org_zip,
            TRIM("Grantee Organization Type Description") AS grantee_org_type,
            TRY_CAST("Geocoding Artifact Address Primary X Coordinate" AS DOUBLE) AS longitude,
            TRY_CAST("Geocoding Artifact Address Primary Y Coordinate" AS DOUBLE) AS latitude,
            TRIM("State and County Federal Information Processing Standard Code") AS county_fips,
            TRIM("Complete County Name") AS county_name,
            TRIM("HHS Region Code") AS hhs_region_code,
            TRIM("HHS Region Name") AS hhs_region_name,
            TRIM("State FIPS Code") AS state_fips,
            TRIM("State Name") AS state_name,
            TRIM("Congressional District Number") AS congressional_district,
            TRIM("Congressional District Name") AS congressional_district_name,
            TRIM("Congressional District Code") AS congressional_district_code,
            TRIM("U.S. Congressional Representative Name") AS us_representative,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{csv_path}', all_varchar=true, header=true,
                      quote='"', delim=',', escape='"', null_padding=true)
        WHERE TRIM("Site Status Description") = 'Active'
    """)

    return write_parquet(con, "fqhc_directory", _snapshot_path("fqhc_directory"), dry_run)


def build_vital_stats(con, dry_run: bool) -> int:
    """Ingest CDC VSRR provisional births, deaths, and infant deaths by state."""
    csv_path = RAW_DIR / "cdc_vsrr_births_deaths_infant.csv"
    if not csv_path.exists():
        print("  [skip] cdc_vsrr_births_deaths_infant.csv not found")
        return 0

    print("\n[2/4] CDC Vital Statistics (VSRR)")

    # Build state name mapping as a CASE statement for upper-case state names
    case_parts = [f"WHEN TRIM(UPPER(state)) = '{name}' THEN '{code}'"
                  for name, code in STATE_NAME_UPPER_TO_CODE.items()]
    case_stmt = "CASE " + " ".join(case_parts) + " ELSE NULL END"

    con.execute(f"""
        CREATE TABLE vital_stats AS
        SELECT
            {case_stmt} AS state_code,
            TRIM(state) AS state_name,
            TRY_CAST(year AS INTEGER) AS year,
            TRIM(month) AS month,
            TRIM(period) AS period,
            TRIM(indicator) AS indicator,
            TRY_CAST(REPLACE(data_value, ',', '') AS BIGINT) AS data_value,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true)
        WHERE TRIM(state) != 'UNITED STATES'
          AND TRIM(data_value) IS NOT NULL
          AND TRIM(data_value) != ''
    """)

    # Also remove rows where state_code is null (territories, etc.)
    con.execute("""
        DELETE FROM vital_stats WHERE state_code IS NULL
    """)

    return write_parquet(con, "vital_stats", _snapshot_path("vital_stats"), dry_run)


def build_maternal_mortality_national(con, dry_run: bool) -> int:
    """Ingest CDC provisional maternal mortality data (national only)."""
    csv_path = RAW_DIR / "cdc_maternal_mortality_provisional.csv"
    if not csv_path.exists():
        print("  [skip] cdc_maternal_mortality_provisional.csv not found")
        return 0

    print("\n[3/4] CDC Maternal Mortality (National, Provisional)")
    con.execute(f"""
        CREATE TABLE maternal_mortality_national AS
        SELECT
            TRIM(jurisdiction) AS jurisdiction,
            TRIM("group") AS demographic_group,
            TRIM(subgroup) AS subgroup,
            TRY_CAST(year_of_death AS INTEGER) AS year_of_death,
            TRY_CAST(month_of_death AS INTEGER) AS month_of_death,
            TRIM(time_period) AS time_period,
            TRIM(month_ending_date) AS month_ending_date,
            TRY_CAST(maternal_deaths AS INTEGER) AS maternal_deaths,
            TRY_CAST(REPLACE(live_births, ',', '') AS BIGINT) AS live_births,
            TRY_CAST(maternal_mortality_rate AS DOUBLE) AS maternal_mortality_rate,
            TRIM(footnote) AS footnote,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true)
        WHERE TRIM(maternal_deaths) IS NOT NULL
          AND TRIM(maternal_deaths) != ''
    """)

    return write_parquet(con, "maternal_mortality_national",
                         _snapshot_path("maternal_mortality_national"), dry_run)


def build_poverty_guidelines(con, dry_run: bool) -> int:
    """Ingest federal poverty guidelines as a reference table."""
    csv_path = RAW_DIR / "federal_poverty_guidelines_2025.csv"
    if not csv_path.exists():
        print("  [skip] federal_poverty_guidelines_2025.csv not found")
        return 0

    print("\n[4/4] Federal Poverty Guidelines 2025 (Reference)")
    con.execute(f"""
        CREATE TABLE poverty_guidelines AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            TRIM(region) AS region,
            TRY_CAST(family_size AS INTEGER) AS family_size,
            TRY_CAST(REPLACE(poverty_guideline, ',', '') AS INTEGER) AS poverty_guideline,
            TRY_CAST(REPLACE(increment_per_person, ',', '') AS INTEGER) AS increment_per_person,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true)
    """)

    out_path = REF_DIR / "ref_poverty_guidelines.parquet"
    return write_parquet(con, "poverty_guidelines", out_path, dry_run)


def main():
    parser = argparse.ArgumentParser(description="Ingest HRSA/CDC/poverty data into lake")
    parser.add_argument("--dry-run", action="store_true", help="Show counts but don't write")
    parser.add_argument("--table", type=str, default=None,
                        help="Build only one table: fqhc|vital|maternal|poverty")
    args = parser.parse_args()

    con = duckdb.connect()
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_providers_demographics.py",
        "started_at": datetime.now().isoformat(),
        "tables": {},
    }

    tables = {
        "fqhc": ("fqhc_directory", build_fqhc_directory),
        "vital": ("vital_stats", build_vital_stats),
        "maternal": ("maternal_mortality_national", build_maternal_mortality_national),
        "poverty": ("poverty_guidelines", build_poverty_guidelines),
    }

    total_rows = 0
    for key, (table_name, builder) in tables.items():
        if args.table and args.table != key:
            continue
        count = builder(con, args.dry_run)
        manifest["tables"][table_name] = count
        total_rows += count

    manifest["finished_at"] = datetime.now().isoformat()
    manifest["total_rows"] = total_rows

    # Write manifest
    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = META_DIR / f"manifest_providers_demographics_{SNAPSHOT_DATE}.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\nManifest: {manifest_path.relative_to(LAKE_DIR)}")

    print(f"\nTotal: {total_rows:,} rows ingested")
    con.close()


if __name__ == "__main__":
    main()
