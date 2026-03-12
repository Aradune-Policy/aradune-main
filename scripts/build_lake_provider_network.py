#!/usr/bin/env python3
"""
build_lake_provider_network.py — Ingest provider and network datasets into the lake.

Datasets:
  1. PECOS Provider Enrollment (v2) — 2.96M rows, adds PECOS ID, enrollment ID, org name
  2. Provider Affiliation/Reassignment — 3.49M provider-to-organization links
  3. Critical Access Hospitals — extracted from Hospital General Info (1,376 CAHs)
  4. GME Teaching Hospitals — HCRIS GME data (FY2010-2020, ~62K hospital-year rows)
  5. GME Update Factors — annual DGME inflation factors

Tables built:
  fact_pecos_enrollment        — Full PECOS enrollment with org names and enrollment IDs
  fact_provider_affiliation    — Which providers are affiliated with which organizations
  fact_critical_access_hospitals — CAH-designated hospitals with ratings and bed counts
  fact_gme_teaching_hospitals  — GME/IME slots and FTE counts per teaching hospital
  ref_gme_update_factors       — Annual DGME inflation update factors

Skipped:
  fact_optout_detail       — optout_providers_v2 already has NPI-level detail (54K rows)
  fact_rhc_quality         — No CMS dataset exists for RHC quality measures
  fact_fqhc_financial      — HRSA UDS data blocked by WAF, not downloadable

Usage:
  python3 scripts/build_lake_provider_network.py
  python3 scripts/build_lake_provider_network.py --dry-run
"""

import argparse
import json
import os
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


def _fact_path(name: str) -> Path:
    return FACT_DIR / name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _ref_path(name: str) -> Path:
    return REF_DIR / f"{name}.parquet"


# ── 1. PECOS Provider Enrollment (v2) ────────────────────────────────

def build_pecos_enrollment(con, dry_run: bool) -> int:
    """Full PECOS enrollment extract with org names, PECOS IDs, enrollment IDs."""
    print("Building fact_pecos_enrollment...")
    path = RAW_DIR / "pecos_enrollment.csv"
    if not path.exists():
        print("  SKIPPED — pecos_enrollment.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _pecos AS
        SELECT
            TRIM(NPI) AS npi,
            TRIM(MULTIPLE_NPI_FLAG) AS multiple_npi_flag,
            TRIM(PECOS_ASCT_CNTL_ID) AS pecos_associate_id,
            TRIM(ENRLMT_ID) AS enrollment_id,
            TRIM(PROVIDER_TYPE_CD) AS provider_type_code,
            TRIM(PROVIDER_TYPE_DESC) AS provider_type,
            TRIM(STATE_CD) AS state_code,
            TRIM(FIRST_NAME) AS first_name,
            TRIM(MDL_NAME) AS middle_name,
            TRIM(LAST_NAME) AS last_name,
            TRIM(ORG_NAME) AS org_name,
            'data.cms.gov/PPEF' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{path}', all_varchar=true, auto_detect=true, ignore_errors=true)
        WHERE NPI IS NOT NULL AND LENGTH(TRIM(NPI)) = 10
    """)

    count = write_parquet(con, "_pecos", _fact_path("pecos_enrollment"), dry_run)
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT npi) as unique_npis,
                COUNT(DISTINCT state_code) as states,
                COUNT(DISTINCT provider_type_code) as provider_types,
                SUM(CASE WHEN org_name IS NOT NULL AND org_name != '' THEN 1 ELSE 0 END) as with_org
            FROM _pecos
        """).fetchone()
        print(f"  {count:,} enrollments, {stats[0]:,} unique NPIs, {stats[1]} states, {stats[2]} provider types, {stats[3]:,} with org name")
    con.execute("DROP TABLE IF EXISTS _pecos")
    return count


# ── 2. Provider Affiliation (Reassignment) ───────────────────────────

def build_provider_affiliation(con, dry_run: bool) -> int:
    """Provider-to-organization affiliation via CMS reassignment data."""
    print("Building fact_provider_affiliation...")
    path = RAW_DIR / "provider_reassignment.csv"
    if not path.exists():
        print("  SKIPPED — provider_reassignment.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _affil AS
        SELECT
            TRIM("Group PAC ID") AS group_pac_id,
            TRIM("Group Enrollment ID") AS group_enrollment_id,
            TRIM("Group Legal Business Name") AS group_name,
            TRIM("Group State Code") AS group_state_code,
            TRIM("Group Due Date") AS group_due_date,
            TRY_CAST("Group Reassignments and Physician Assistants" AS INTEGER) AS group_reassignment_count,
            TRIM("Record Type") AS record_type,
            TRIM("Individual PAC ID") AS individual_pac_id,
            TRIM("Individual Enrollment ID") AS individual_enrollment_id,
            TRIM("Individual NPI") AS individual_npi,
            TRIM("Individual First Name") AS individual_first_name,
            TRIM("Individual Last Name") AS individual_last_name,
            TRIM("Individual State Code") AS individual_state_code,
            TRIM("Individual Specialty Description") AS individual_specialty,
            TRIM("Individual Due Date") AS individual_due_date,
            TRY_CAST("Individual Total Employer Associations" AS INTEGER) AS individual_employer_count,
            'data.cms.gov/revalidation_reassignment' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{path}', all_varchar=true, auto_detect=true, ignore_errors=true)
        WHERE "Individual NPI" IS NOT NULL
    """)

    count = write_parquet(con, "_affil", _fact_path("provider_affiliation"), dry_run)
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT individual_npi) as unique_providers,
                COUNT(DISTINCT group_pac_id) as unique_groups,
                COUNT(DISTINCT individual_state_code) as states,
                COUNT(DISTINCT individual_specialty) as specialties
            FROM _affil
        """).fetchone()
        print(f"  {count:,} affiliations, {stats[0]:,} unique providers, {stats[1]:,} unique groups, {stats[2]} states, {stats[3]} specialties")
    con.execute("DROP TABLE IF EXISTS _affil")
    return count


# ── 3. Critical Access Hospitals ─────────────────────────────────────

def build_critical_access_hospitals(con, dry_run: bool) -> int:
    """Extract CAH-designated hospitals from Hospital General Information file."""
    print("Building fact_critical_access_hospitals...")
    path = RAW_DIR / "hospital_general_info.csv"
    if not path.exists():
        print("  SKIPPED — hospital_general_info.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _cah AS
        SELECT
            TRIM("Facility ID") AS facility_id,
            TRIM("Facility Name") AS facility_name,
            TRIM("Address") AS address,
            TRIM("City/Town") AS city,
            TRIM("State") AS state_code,
            TRIM("ZIP Code") AS zip_code,
            TRIM("County/Parish") AS county,
            TRIM("Telephone Number") AS phone,
            TRIM("Hospital Type") AS hospital_type,
            TRIM("Hospital Ownership") AS ownership,
            TRIM("Emergency Services") AS emergency_services,
            TRIM("Meets criteria for birthing friendly designation") AS birthing_friendly,
            TRY_CAST("Hospital overall rating" AS INTEGER) AS overall_rating,
            'data.cms.gov/provider-data/Hospital_General_Information' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv('{path}', auto_detect=true, all_varchar=true)
        WHERE TRIM("Hospital Type") = 'Critical Access Hospitals'
    """)

    count = write_parquet(con, "_cah", _fact_path("critical_access_hospitals"), dry_run)
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT state_code) as states,
                AVG(overall_rating) as avg_rating,
                SUM(CASE WHEN emergency_services = 'Yes' THEN 1 ELSE 0 END) as with_emergency
            FROM _cah
        """).fetchone()
        print(f"  {count:,} CAHs across {stats[0]} states, avg rating {stats[1]:.1f}, {stats[2]:,} with emergency services")
    con.execute("DROP TABLE IF EXISTS _cah")
    return count


# ── 4. GME Teaching Hospitals ────────────────────────────────────────

def build_gme_teaching_hospitals(con, dry_run: bool) -> int:
    """GME/IME teaching hospital data from HCRIS cost reports (FY2010-2020)."""
    print("Building fact_gme_teaching_hospitals...")
    gme_dir = RAW_DIR / "gme_data"
    if not gme_dir.exists():
        print("  SKIPPED — gme_data/ directory not found")
        return 0

    # Find all FY txt files
    fy_files = sorted([f for f in gme_dir.iterdir() if f.suffix == '.txt' and 'FY' in f.name and 'ColumnName' not in f.name])
    if not fy_files:
        print("  SKIPPED — no FY data files found")
        return 0

    # Build UNION ALL across all fiscal years
    unions = []
    for f in fy_files:
        # Extract year from filename like HCRISFYs10thru20_FY2020.txt
        fy = f.stem.split('_FY')[-1] if '_FY' in f.stem else 'unknown'
        unions.append(f"""
            SELECT
                TRIM(PROV) AS provider_ccn,
                TRIM(NAME) AS facility_name,
                TRIM(FYB) AS fiscal_year_begin,
                TRIM(FYE) AS fiscal_year_end,
                '{fy}' AS fiscal_year,
                TRY_CAST("S3 RESIDENTS IN FACILITY" AS DOUBLE) AS residents_in_facility,
                TRY_CAST("DGME 96 CAP" AS DOUBLE) AS dgme_96_cap,
                TRY_CAST("DGME NEW PGM CAP" AS DOUBLE) AS dgme_new_program_cap,
                TRY_CAST("DGME AFFILIATION ADJ" AS DOUBLE) AS dgme_affiliation_adj,
                TRY_CAST("DGME CURRENT YR ALLO & OSTEO FTES" AS DOUBLE) AS dgme_current_yr_ftes,
                TRY_CAST("DGME DENT & POD FTES" AS DOUBLE) AS dgme_dental_podiatric_ftes,
                TRY_CAST("PRIMARY CARE OBGYN PRA" AS DOUBLE) AS primary_care_obgyn_pra,
                TRY_CAST("NON PRIMARY CARE PRA" AS DOUBLE) AS non_primary_care_pra,
                TRY_CAST("IME 96 CAP" AS DOUBLE) AS ime_96_cap,
                TRY_CAST("IME NEW PGM CAP" AS DOUBLE) AS ime_new_program_cap,
                TRY_CAST("IME AFFILIATION ADJ" AS DOUBLE) AS ime_affiliation_adj,
                TRY_CAST("IME CURRENT YR ALLO & OSTEO FTES" AS DOUBLE) AS ime_current_yr_ftes,
                TRY_CAST("IME DENT & POD FTES" AS DOUBLE) AS ime_dental_podiatric_ftes
            FROM read_csv('{f}', delim='\\t', all_varchar=true, auto_detect=true)
            WHERE TRIM(PROV) IS NOT NULL AND LENGTH(TRIM(PROV)) >= 6
        """)

    union_sql = " UNION ALL ".join(unions)

    con.execute(f"""
        CREATE OR REPLACE TABLE _gme AS
        SELECT
            *,
            -- Derive state from CCN (first 2 digits map to state)
            CASE LEFT(provider_ccn, 2)
                WHEN '01' THEN 'AL' WHEN '02' THEN 'AK' WHEN '03' THEN 'AZ' WHEN '04' THEN 'AR'
                WHEN '05' THEN 'CA' WHEN '06' THEN 'CO' WHEN '07' THEN 'CT' WHEN '08' THEN 'DE'
                WHEN '09' THEN 'DC' WHEN '10' THEN 'FL' WHEN '11' THEN 'GA' WHEN '12' THEN 'HI'
                WHEN '13' THEN 'ID' WHEN '14' THEN 'IL' WHEN '15' THEN 'IN' WHEN '16' THEN 'IA'
                WHEN '17' THEN 'KS' WHEN '18' THEN 'KY' WHEN '19' THEN 'LA' WHEN '20' THEN 'ME'
                WHEN '21' THEN 'MD' WHEN '22' THEN 'MA' WHEN '23' THEN 'MI' WHEN '24' THEN 'MN'
                WHEN '25' THEN 'MS' WHEN '26' THEN 'MO' WHEN '27' THEN 'MT' WHEN '28' THEN 'NE'
                WHEN '29' THEN 'NV' WHEN '30' THEN 'NH' WHEN '31' THEN 'NJ' WHEN '32' THEN 'NM'
                WHEN '33' THEN 'NY' WHEN '34' THEN 'NC' WHEN '35' THEN 'ND' WHEN '36' THEN 'OH'
                WHEN '37' THEN 'OK' WHEN '38' THEN 'OR' WHEN '39' THEN 'PA' WHEN '40' THEN 'RI'
                WHEN '41' THEN 'SC' WHEN '42' THEN 'SD' WHEN '43' THEN 'TN' WHEN '44' THEN 'TX'
                WHEN '45' THEN 'UT' WHEN '46' THEN 'VT' WHEN '47' THEN 'VA' WHEN '48' THEN 'WA'
                WHEN '49' THEN 'WV' WHEN '50' THEN 'WI' WHEN '51' THEN 'WY'
                WHEN '64' THEN 'AS' WHEN '65' THEN 'GU' WHEN '66' THEN 'MP' WHEN '67' THEN 'MH'
                WHEN '68' THEN 'VI' WHEN '69' THEN 'PR'
                ELSE NULL
            END AS state_code,
            'cms.gov/HCRIS-GME' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    count = write_parquet(con, "_gme", _fact_path("gme_teaching_hospitals"), dry_run)
    if count > 0:
        stats = con.execute("""
            SELECT
                COUNT(DISTINCT provider_ccn) as unique_hospitals,
                COUNT(DISTINCT fiscal_year) as fiscal_years,
                COUNT(DISTINCT state_code) as states,
                SUM(CASE WHEN residents_in_facility > 0 THEN 1 ELSE 0 END) as with_residents,
                SUM(CASE WHEN dgme_96_cap > 0 OR dgme_new_program_cap > 0 THEN 1 ELSE 0 END) as with_dgme,
                SUM(CASE WHEN ime_96_cap > 0 OR ime_new_program_cap > 0 THEN 1 ELSE 0 END) as with_ime
            FROM _gme
        """).fetchone()
        print(f"  {count:,} hospital-year rows, {stats[0]:,} unique hospitals, {stats[1]} fiscal years, {stats[2]} states")
        print(f"  {stats[3]:,} with residents, {stats[4]:,} with DGME caps, {stats[5]:,} with IME caps")
    con.execute("DROP TABLE IF EXISTS _gme")
    return count


# ── 5. GME Update Factors ────────────────────────────────────────────

def build_gme_update_factors(con, dry_run: bool) -> int:
    """Annual Direct GME inflation/update factors reference table."""
    print("Building ref_gme_update_factors...")
    csv_path = RAW_DIR / "gme_factors" / "HAPGFrontOffice" / "DAC" / "GME" / "Update Factors" / "Inflation Factor Automation" / "Files for Web"

    # Find the CSV file
    csv_file = None
    if csv_path.exists():
        for f in csv_path.iterdir():
            if f.suffix == '.csv':
                csv_file = f
                break

    if csv_file is None:
        print("  SKIPPED — GME update factors CSV not found")
        return 0

    # The CSV has a header row "Annual Direct GME Update Factors" before the actual data
    # Read with Python csv module to handle the non-standard format
    import csv as csv_mod
    rows = []
    with open(csv_file, 'r') as f:
        reader = csv_mod.reader(f)
        header_found = False
        for row in reader:
            if not row or len(row) < 3:
                continue
            # Skip the title row
            if 'Annual Direct GME Update Factors' in str(row[0]):
                continue
            # Skip header-like rows
            if 'Updating to' in str(row[0]) or 'midpoint' in str(row[0]):
                continue
            # Data rows have dates and a factor
            if len(row) >= 4:
                try:
                    factor = float(row[3].strip()) if row[3].strip() else None
                    if factor is not None:
                        rows.append({
                            'update_period_begin': row[0].strip().strip('"'),
                            'update_period_end': row[1].strip().strip('"'),
                            'base_period_begin': row[2].strip().strip('"'),
                            'update_factor': factor,
                            'factor_type': row[4].strip().strip('"') if len(row) > 4 and row[4].strip() else 'Actual'
                        })
                except (ValueError, IndexError):
                    continue

    if not rows:
        print("  SKIPPED — no valid rows parsed from GME update factors")
        return 0

    import pandas as pd
    df = pd.DataFrame(rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _gme_factors AS
        SELECT
            update_period_begin,
            update_period_end,
            base_period_begin,
            CAST(update_factor AS DOUBLE) AS update_factor,
            factor_type,
            'cms.gov/DGME-update-factors' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM df
    """)

    count = write_parquet(con, "_gme_factors", _ref_path("gme_update_factors"), dry_run)
    if count > 0:
        stats = con.execute("""
            SELECT MIN(update_factor), MAX(update_factor), AVG(update_factor)
            FROM _gme_factors
        """).fetchone()
        print(f"  {count:,} update factor rows, factor range {stats[0]:.4f} - {stats[1]:.4f}, avg {stats[2]:.4f}")
    con.execute("DROP TABLE IF EXISTS _gme_factors")
    return count


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build provider & network lake tables")
    parser.add_argument("--dry-run", action="store_true", help="Count rows without writing")
    args = parser.parse_args()

    con = duckdb.connect()
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "started_at": datetime.utcnow().isoformat(),
        "tables": {},
    }

    builders = [
        ("pecos_enrollment", build_pecos_enrollment),
        ("provider_affiliation", build_provider_affiliation),
        ("critical_access_hospitals", build_critical_access_hospitals),
        ("gme_teaching_hospitals", build_gme_teaching_hospitals),
        ("gme_update_factors", build_gme_update_factors),
    ]

    total_rows = 0
    for name, builder in builders:
        try:
            count = builder(con, args.dry_run)
            manifest["tables"][name] = {"rows": count, "status": "ok"}
            total_rows += count
        except Exception as e:
            print(f"  ERROR building {name}: {e}")
            import traceback
            traceback.print_exc()
            manifest["tables"][name] = {"rows": 0, "status": f"error: {e}"}

    manifest["finished_at"] = datetime.utcnow().isoformat()
    manifest["total_rows"] = total_rows

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        meta_path = META_DIR / f"provider_network_{SNAPSHOT_DATE}.json"
        with open(meta_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\nManifest: {meta_path.relative_to(LAKE_DIR)}")

    print(f"\nDone. {total_rows:,} total rows across {len(manifest['tables'])} tables.")
    con.close()


if __name__ == "__main__":
    main()
