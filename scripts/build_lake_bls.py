#!/usr/bin/env python3
"""
build_lake_bls.py — Ingest BLS OEWS wage data into the Aradune data lake.

Reads from: data/raw/bls_medicaid_occupations.csv (pre-filtered 16 SOC codes)
             data/raw/all_data_M_2024.xlsx (full OEWS — optional, for MSA-level)
Writes to:  data/lake/

Tables built:
  Dimensions:
    dim_bls_occupation     — 16 Medicaid-relevant SOC codes + service category mapping
  Facts:
    fact_bls_wage          — State-level wages for 16 Medicaid occupations (~800 rows)
    fact_bls_wage_msa      — MSA-level wages for 16 Medicaid occupations (~5,400 rows)
    fact_bls_wage_national — National-level wages for all 854 occupations (~850 rows)

Usage:
  python3 scripts/build_lake_bls.py
  python3 scripts/build_lake_bls.py --dry-run
  python3 scripts/build_lake_bls.py --only fact_bls_wage
  python3 scripts/build_lake_bls.py --skip-xlsx   # skip the 81MB file
"""

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

MEDICAID_CSV = RAW_DIR / "bls_medicaid_occupations.csv"
FULL_XLSX = RAW_DIR / "all_data_M_2024.xlsx"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# SOC code → Medicaid service category mapping
SOC_CATEGORY = {
    "21-1021": "behavioral_health",     # Child, Family, and School Social Workers
    "21-1023": "behavioral_health",     # MH/SUD Social Workers
    "29-1122": "therapy",               # Occupational Therapists
    "29-1123": "therapy",               # Physical Therapists
    "29-1127": "therapy",               # Speech-Language Pathologists
    "29-1141": "nursing",               # Registered Nurses
    "29-1151": "nursing",               # Nurse Anesthetists
    "29-1171": "primary_care",          # Nurse Practitioners
    "29-1215": "primary_care",          # Family Medicine Physicians
    "29-1292": "dental",                # Dental Hygienists
    "29-2061": "nursing",               # LPN/LVNs
    "31-1120": "hcbs",                  # Home Health and Personal Care Aides
    "31-1131": "nursing",               # Nursing Assistants
    "31-2011": "therapy",               # OT Assistants
    "31-2021": "therapy",               # PT Assistants
    "31-9091": "dental",                # Dental Assistants
}

DATA_YEAR = 2024  # OEWS May 2024 release


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _load_medicaid_csv(con):
    """Load the pre-filtered Medicaid occupations CSV into a temp table."""
    con.execute(f"""
        CREATE OR REPLACE TABLE _bls_raw AS
        SELECT * FROM read_csv_auto('{MEDICAID_CSV}')
    """)


def _load_full_xlsx(con):
    """Load the full OEWS XLSX using DuckDB spatial extension."""
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"""
        CREATE OR REPLACE TABLE _bls_full AS
        SELECT * FROM st_read('{FULL_XLSX}')
    """)


# ---------------------------------------------------------------------------
# DIMENSION: BLS Occupations
# ---------------------------------------------------------------------------

def build_dim_bls_occupation(con, dry_run: bool) -> int:
    print("Building dim_bls_occupation...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _dim_bls_occ AS
        SELECT DISTINCT
            OCC_CODE AS soc_code,
            OCC_TITLE AS occupation_title,
            CAST(NULL AS VARCHAR) AS medicaid_category,
            {DATA_YEAR} AS data_year
        FROM _bls_raw
        WHERE AREA_TYPE = '2'
        ORDER BY OCC_CODE
    """)
    for soc, cat in SOC_CATEGORY.items():
        con.execute(f"UPDATE _dim_bls_occ SET medicaid_category = '{cat}' WHERE soc_code = '{soc}'")

    out = DIM_DIR / "dim_bls_occupation.parquet"
    count = write_parquet(con, "_dim_bls_occ", out, dry_run)
    print(f"  {count} occupations mapped to Medicaid categories")
    con.execute("DROP TABLE IF EXISTS _dim_bls_occ")
    return count


# ---------------------------------------------------------------------------
# FACT: State-level wages for Medicaid occupations
# ---------------------------------------------------------------------------

def build_fact_bls_wage(con, dry_run: bool) -> int:
    print("Building fact_bls_wage (state-level, 16 Medicaid occupations)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bls_wage AS
        SELECT
            PRIM_STATE AS state_code,
            OCC_CODE AS soc_code,
            OCC_TITLE AS occupation_title,
            {DATA_YEAR} AS data_year,
            TRY_CAST(TOT_EMP AS INTEGER) AS total_employment,
            TRY_CAST(H_MEAN AS DOUBLE) AS hourly_mean,
            TRY_CAST(A_MEAN AS DOUBLE) AS annual_mean,
            TRY_CAST(H_MEDIAN AS DOUBLE) AS hourly_median,
            TRY_CAST(A_MEDIAN AS DOUBLE) AS annual_median,
            TRY_CAST(H_PCT10 AS DOUBLE) AS hourly_p10,
            TRY_CAST(H_PCT25 AS DOUBLE) AS hourly_p25,
            TRY_CAST(H_PCT75 AS DOUBLE) AS hourly_p75,
            TRY_CAST(H_PCT90 AS DOUBLE) AS hourly_p90,
            TRY_CAST(A_PCT10 AS DOUBLE) AS annual_p10,
            TRY_CAST(A_PCT25 AS DOUBLE) AS annual_p25,
            TRY_CAST(A_PCT75 AS DOUBLE) AS annual_p75,
            TRY_CAST(A_PCT90 AS DOUBLE) AS annual_p90,
            TRY_CAST(JOBS_1000 AS DOUBLE) AS jobs_per_1000,
            TRY_CAST(LOC_QUOTIENT AS DOUBLE) AS location_quotient,
            'bls.gov/oews' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bls_raw
        WHERE AREA_TYPE = '2'
          AND PRIM_STATE != 'US'
          AND OCC_CODE NOT LIKE '%-0000'
    """)
    count = write_parquet(con, "_fact_bls_wage", _snapshot_path("bls_wage"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_bls_wage").fetchone()[0]
    occs = con.execute("SELECT COUNT(DISTINCT soc_code) FROM _fact_bls_wage").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {occs} occupations")
    con.execute("DROP TABLE IF EXISTS _fact_bls_wage")
    return count


# ---------------------------------------------------------------------------
# FACT: MSA-level wages for Medicaid occupations
# ---------------------------------------------------------------------------

def build_fact_bls_wage_msa(con, dry_run: bool) -> int:
    print("Building fact_bls_wage_msa (MSA-level, 16 Medicaid occupations)...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bls_wage_msa AS
        SELECT
            CAST(AREA AS VARCHAR) AS msa_code,
            AREA_TITLE AS msa_title,
            PRIM_STATE AS state_code,
            OCC_CODE AS soc_code,
            OCC_TITLE AS occupation_title,
            {DATA_YEAR} AS data_year,
            TRY_CAST(TOT_EMP AS INTEGER) AS total_employment,
            TRY_CAST(H_MEAN AS DOUBLE) AS hourly_mean,
            TRY_CAST(A_MEAN AS DOUBLE) AS annual_mean,
            TRY_CAST(H_MEDIAN AS DOUBLE) AS hourly_median,
            TRY_CAST(A_MEDIAN AS DOUBLE) AS annual_median,
            TRY_CAST(H_PCT10 AS DOUBLE) AS hourly_p10,
            TRY_CAST(H_PCT25 AS DOUBLE) AS hourly_p25,
            TRY_CAST(H_PCT75 AS DOUBLE) AS hourly_p75,
            TRY_CAST(H_PCT90 AS DOUBLE) AS hourly_p90,
            TRY_CAST(A_PCT10 AS DOUBLE) AS annual_p10,
            TRY_CAST(A_PCT25 AS DOUBLE) AS annual_p25,
            TRY_CAST(A_PCT75 AS DOUBLE) AS annual_p75,
            TRY_CAST(A_PCT90 AS DOUBLE) AS annual_p90,
            TRY_CAST(JOBS_1000 AS DOUBLE) AS jobs_per_1000,
            TRY_CAST(LOC_QUOTIENT AS DOUBLE) AS location_quotient,
            'bls.gov/oews' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bls_raw
        WHERE AREA_TYPE = '4'
          AND OCC_CODE NOT LIKE '%-0000'
    """)
    count = write_parquet(con, "_fact_bls_wage_msa", _snapshot_path("bls_wage_msa"), dry_run)
    msas = con.execute("SELECT COUNT(DISTINCT msa_code) FROM _fact_bls_wage_msa").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_bls_wage_msa").fetchone()[0]
    print(f"  {count:,} rows, {msas} MSAs across {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_bls_wage_msa")
    return count


# ---------------------------------------------------------------------------
# FACT: National-level wages for ALL occupations (from full XLSX)
# ---------------------------------------------------------------------------

def build_fact_bls_wage_national(con, dry_run: bool) -> int:
    print("Building fact_bls_wage_national (national, all occupations)...")
    if not FULL_XLSX.exists():
        print(f"  SKIPPED — {FULL_XLSX.name} not found")
        return 0

    _load_full_xlsx(con)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bls_wage_national AS
        SELECT
            OCC_CODE AS soc_code,
            OCC_TITLE AS occupation_title,
            O_GROUP AS occ_group,
            {DATA_YEAR} AS data_year,
            TRY_CAST(TOT_EMP AS INTEGER) AS total_employment,
            TRY_CAST(H_MEAN AS DOUBLE) AS hourly_mean,
            TRY_CAST(A_MEAN AS DOUBLE) AS annual_mean,
            TRY_CAST(H_MEDIAN AS DOUBLE) AS hourly_median,
            TRY_CAST(A_MEDIAN AS DOUBLE) AS annual_median,
            TRY_CAST(H_PCT10 AS DOUBLE) AS hourly_p10,
            TRY_CAST(H_PCT25 AS DOUBLE) AS hourly_p25,
            TRY_CAST(H_PCT75 AS DOUBLE) AS hourly_p75,
            TRY_CAST(H_PCT90 AS DOUBLE) AS hourly_p90,
            TRY_CAST(A_PCT10 AS DOUBLE) AS annual_p10,
            TRY_CAST(A_PCT25 AS DOUBLE) AS annual_p25,
            TRY_CAST(A_PCT75 AS DOUBLE) AS annual_p75,
            TRY_CAST(A_PCT90 AS DOUBLE) AS annual_p90,
            'bls.gov/oews' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bls_full
        WHERE AREA_TYPE = '1'
          AND PRIM_STATE = 'US'
          AND I_GROUP = 'cross-industry'
          AND OWN_CODE = '1235'
          AND O_GROUP = 'detailed'
    """)
    count = write_parquet(con, "_fact_bls_wage_national", _snapshot_path("bls_wage_national"), dry_run)
    print(f"  {count:,} national occupation rows")
    con.execute("DROP TABLE IF EXISTS _fact_bls_wage_national")
    con.execute("DROP TABLE IF EXISTS _bls_full")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "dim_bls_occupation": build_dim_bls_occupation,
    "fact_bls_wage": build_fact_bls_wage,
    "fact_bls_wage_msa": build_fact_bls_wage_msa,
    "fact_bls_wage_national": build_fact_bls_wage_national,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest BLS OEWS wage data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    parser.add_argument("--skip-xlsx", action="store_true",
                        help="Skip the 81MB full OEWS XLSX")
    args = parser.parse_args()

    if not MEDICAID_CSV.exists():
        print(f"ERROR: BLS CSV not found at {MEDICAID_CSV}", file=sys.stderr)
        sys.exit(1)

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]
        invalid = [t for t in tables if t not in ALL_TABLES]
        if invalid:
            print(f"ERROR: Unknown tables: {invalid}", file=sys.stderr)
            print(f"Valid: {list(ALL_TABLES.keys())}", file=sys.stderr)
            sys.exit(1)
    if args.skip_xlsx and "fact_bls_wage_national" in tables:
        tables.remove("fact_bls_wage_national")

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    con = duckdb.connect()
    _load_medicaid_csv(con)

    totals = {}
    for name in tables:
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    # Summary
    print("=" * 60)
    print("BLS WAGE DATA LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>10,} rows")

    if not args.dry_run:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source_files": [
                str(MEDICAID_CSV),
                str(FULL_XLSX) if FULL_XLSX.exists() else None,
            ],
            "data_year": DATA_YEAR,
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_bls_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
