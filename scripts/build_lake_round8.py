#!/usr/bin/env python3
"""
Round 8: Ingest newly downloaded CMS provider and quality datasets.
- MSPB Hospital Detail (4,625 hospitals)
- Imaging Hospital Detail (18,500 measure rows)
- ESRD QIP (7,557 dialysis facilities)
- AHRF County (3,235 counties — health resource indicators)
- Physician Compare (10,924 rows — top doctors/clinicians)
"""

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
SNAP = str(date.today())


def _write_parquet(con, table: str, fact_name: str) -> int:
    out_dir = LAKE / "fact" / fact_name / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  -> {fact_name}: {n:,} rows -> {out}")
    return n


def build_mspb_hospital_detail(con, dry_run: bool) -> int:
    """Medicare Spending Per Beneficiary by hospital."""
    csv_path = RAW / "mspb_hospital_detail.csv"
    if not csv_path.exists():
        print("  SKIP: mspb_hospital_detail.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            facility_id AS ccn,
            facility_name,
            state,
            zip_code,
            measure_id,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            footnote,
            start_date,
            end_date,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE state IS NOT NULL AND state != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  mspb_hospital_detail: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "mspb_hospital_detail")


def build_imaging_hospital(con, dry_run: bool) -> int:
    """Outpatient Imaging Efficiency — hospital-level measures."""
    csv_path = RAW / "imaging_hospital_detail.csv"
    if not csv_path.exists():
        print("  SKIP: imaging_hospital_detail.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            facility_id AS ccn,
            facility_name,
            state,
            zip_code,
            measure_id,
            measure_name,
            TRY_CAST(score AS DOUBLE) AS score,
            footnote,
            start_date,
            end_date,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE state IS NOT NULL AND state != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  imaging_hospital: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "imaging_hospital")


def build_esrd_qip(con, dry_run: bool) -> int:
    """ESRD Quality Incentive Program — dialysis facility quality."""
    csv_path = RAW / "esrd_qip.csv"
    if not csv_path.exists():
        print("  SKIP: esrd_qip.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            cms_certification_number_ccn AS ccn,
            facility_name,
            state,
            zip_code,
            TRY_CAST(five_star AS INTEGER) AS five_star,
            TRY_CAST(of_dialysis_stations AS INTEGER) AS dialysis_stations,
            profit_or_nonprofit,
            chain_organization,
            TRY_CAST(mortality_rate_facility AS DOUBLE) AS mortality_rate,
            patient_survival_category_text AS survival_category,
            TRY_CAST(hospitalization_rate_facility AS DOUBLE) AS hospitalization_rate,
            patient_hospitalization_category_text AS hospitalization_category,
            TRY_CAST(readmission_rate_facility AS DOUBLE) AS readmission_rate,
            TRY_CAST(transfusion_rate_facility AS DOUBLE) AS transfusion_rate,
            TRY_CAST(fistula_rate_facility AS DOUBLE) AS fistula_rate,
            TRY_CAST(percentage_of_adult_patients_with_long_term_catheter_in_use AS DOUBLE) AS catheter_rate,
            TRY_CAST(percentage_of_adult_patients_with_hypercalcemia_serum_calci_044d AS DOUBLE) AS hypercalcemia_rate,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE state IS NOT NULL AND state != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  esrd_qip: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "esrd_qip")


def build_ahrf_county(con, dry_run: bool) -> int:
    """Area Health Resources File — county-level health resources."""
    csv_path = RAW / "ahrf_county.csv"
    if not csv_path.exists():
        print("  SKIP: ahrf_county.csv not found")
        return 0
    # Check actual columns first
    r = con.execute(f"SELECT * FROM read_csv_auto('{csv_path}', header=true) LIMIT 0")
    print(f"  ahrf_county: {len(r.description)} columns, loading...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT *,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  ahrf_county: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "ahrf_county")


def build_physician_compare(con, dry_run: bool) -> int:
    """Physician/Clinician directory (Doctors and Clinicians dataset)."""
    csv_path = RAW / "physician_compare.csv"
    if not csv_path.exists():
        print("  SKIP: physician_compare.csv not found")
        return 0
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact AS
        SELECT
            npi,
            provider_last_name,
            provider_first_name,
            gndr AS gender,
            cred AS credential,
            pri_spec AS primary_specialty,
            sec_spec_all AS secondary_specialties,
            facility_name,
            state,
            zip_code,
            citytown AS city,
            TRY_CAST(telehlth AS VARCHAR) AS telehealth,
            ind_assgn AS accepts_assignment,
            '{SNAP}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, ignore_errors=true)
        WHERE state IS NOT NULL AND state != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM _fact").fetchone()[0]
    print(f"  physician_compare: {n:,} rows")
    if dry_run:
        return n
    return _write_parquet(con, "_fact", "physician_compare")


def main():
    parser = argparse.ArgumentParser(description="Round 8: CMS provider/quality datasets")
    parser.add_argument("--dry-run", action="store_true", help="Count rows without writing Parquet")
    args = parser.parse_args()

    con = duckdb.connect()
    total = 0

    builders = [
        ("MSPB Hospital Detail", build_mspb_hospital_detail),
        ("Imaging Hospital Detail", build_imaging_hospital),
        ("ESRD QIP", build_esrd_qip),
        ("AHRF County", build_ahrf_county),
        ("Physician Compare", build_physician_compare),
    ]

    for name, builder in builders:
        print(f"\n{'='*60}\n{name}\n{'='*60}")
        n = builder(con, args.dry_run)
        total += n

    # Write manifest
    if not args.dry_run:
        manifest = {
            "script": "build_lake_round8.py",
            "snapshot": SNAP,
            "tables": {name: {"rows": builder(con, True)} for name, builder in builders},
            "total_rows": total,
        }
        manifest_path = LAKE / "metadata" / f"manifest_round8_{SNAP}.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"\nManifest: {manifest_path}")

    con.close()
    print(f"\n{'='*60}")
    print(f"Round 8 complete: {total:,} total rows")
    if args.dry_run:
        print("(DRY RUN — no files written)")


if __name__ == "__main__":
    main()
