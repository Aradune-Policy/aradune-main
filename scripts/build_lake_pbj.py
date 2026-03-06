#!/usr/bin/env python3
"""
build_lake_pbj.py — Ingest CMS Payroll-Based Journal (PBJ) staffing data into the lake.

Reads from: data/raw/pbj_daily_nurse_staffing_2025q3.csv
             data/raw/pbj_daily_nonnurse_staffing_2025q3.csv
             data/raw/pbj_employee_detail_2025q3.csv (optional, large)
Writes to:  data/lake/

Tables built:
  Facts:
    fact_pbj_nurse_staffing     — Daily nurse staffing hours per facility (RN, LPN, CNA)
    fact_pbj_nonnurse_staffing  — Daily non-nurse staffing hours per facility (therapy, social work, etc.)
    fact_pbj_employee           — Employee-level staffing detail (optional, ~1GB+)

Usage:
  python3 scripts/build_lake_pbj.py
  python3 scripts/build_lake_pbj.py --dry-run
  python3 scripts/build_lake_pbj.py --only fact_pbj_nurse_staffing
  python3 scripts/build_lake_pbj.py --skip-employee   # skip the large employee detail file
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
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

NURSE_CSV = RAW_DIR / "pbj_daily_nurse_staffing_2025q3.csv"
NONNURSE_CSV = RAW_DIR / "pbj_daily_nonnurse_staffing_2025q3.csv"
EMPLOYEE_CSV = RAW_DIR / "pbj_employee_detail_2025q3.csv"


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
# Daily Nurse Staffing
# ---------------------------------------------------------------------------

def build_fact_pbj_nurse_staffing(con, dry_run: bool) -> int:
    print("Building fact_pbj_nurse_staffing...")
    if not NURSE_CSV.exists():
        print(f"  SKIPPED — {NURSE_CSV.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_pbj_nurse AS
        SELECT
            PROVNUM AS provider_ccn,
            PROVNAME AS facility_name,
            CITY AS city,
            STATE AS state_code,
            COUNTY_NAME AS county,
            COUNTY_FIPS AS county_fips,
            CY_Qtr AS quarter,
            CAST(WorkDate AS VARCHAR) AS work_date,
            TRY_CAST(MDScensus AS INTEGER) AS mds_census,
            -- RN hours
            TRY_CAST(Hrs_RN AS DOUBLE) AS hrs_rn,
            TRY_CAST(Hrs_RN_emp AS DOUBLE) AS hrs_rn_employee,
            TRY_CAST(Hrs_RN_ctr AS DOUBLE) AS hrs_rn_contract,
            -- RN admin/DON
            TRY_CAST(Hrs_RNadmin AS DOUBLE) AS hrs_rn_admin,
            TRY_CAST(Hrs_RNDON AS DOUBLE) AS hrs_rn_don,
            -- LPN hours
            TRY_CAST(Hrs_LPN AS DOUBLE) AS hrs_lpn,
            TRY_CAST(Hrs_LPN_emp AS DOUBLE) AS hrs_lpn_employee,
            TRY_CAST(Hrs_LPN_ctr AS DOUBLE) AS hrs_lpn_contract,
            -- CNA hours
            TRY_CAST(Hrs_CNA AS DOUBLE) AS hrs_cna,
            TRY_CAST(Hrs_CNA_emp AS DOUBLE) AS hrs_cna_employee,
            TRY_CAST(Hrs_CNA_ctr AS DOUBLE) AS hrs_cna_contract,
            -- Nurse aide trainee
            TRY_CAST(Hrs_NAtrn AS DOUBLE) AS hrs_na_trainee,
            -- Med aide
            TRY_CAST(Hrs_MedAide AS DOUBLE) AS hrs_med_aide,
            -- Derived: total nursing hours and hours per resident day
            TRY_CAST(Hrs_RN AS DOUBLE) + TRY_CAST(Hrs_LPN AS DOUBLE) + TRY_CAST(Hrs_CNA AS DOUBLE)
                AS total_nursing_hrs,
            CASE WHEN TRY_CAST(MDScensus AS INTEGER) > 0
                THEN ROUND((TRY_CAST(Hrs_RN AS DOUBLE) + TRY_CAST(Hrs_LPN AS DOUBLE)
                     + TRY_CAST(Hrs_CNA AS DOUBLE)) / TRY_CAST(MDScensus AS DOUBLE), 4)
            END AS nursing_hprd,
            'data.cms.gov/pbj' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{NURSE_CSV}', ignore_errors=true)
        WHERE STATE IS NOT NULL AND LENGTH(STATE) = 2
    """)
    count = write_parquet(con, "_fact_pbj_nurse", _snapshot_path("pbj_nurse_staffing"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_pbj_nurse").fetchone()[0]
    facilities = con.execute("SELECT COUNT(DISTINCT provider_ccn) FROM _fact_pbj_nurse").fetchone()[0]
    avg_hprd = con.execute("SELECT ROUND(AVG(nursing_hprd), 2) FROM _fact_pbj_nurse WHERE nursing_hprd > 0").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {facilities:,} facilities, avg HPRD: {avg_hprd}")
    con.execute("DROP TABLE IF EXISTS _fact_pbj_nurse")
    return count


# ---------------------------------------------------------------------------
# Daily Non-Nurse Staffing
# ---------------------------------------------------------------------------

def build_fact_pbj_nonnurse_staffing(con, dry_run: bool) -> int:
    print("Building fact_pbj_nonnurse_staffing...")
    if not NONNURSE_CSV.exists():
        print(f"  SKIPPED — {NONNURSE_CSV.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_pbj_nonnurse AS
        SELECT
            PROVNUM AS provider_ccn,
            STATE AS state_code,
            CY_Qtr AS quarter,
            CAST(WorkDate AS VARCHAR) AS work_date,
            TRY_CAST(MDScensus AS INTEGER) AS mds_census,
            -- Key non-nurse roles
            TRY_CAST(Hrs_Admin AS DOUBLE) AS hrs_admin,
            TRY_CAST(Hrs_MedDir AS DOUBLE) AS hrs_medical_director,
            TRY_CAST(Hrs_NP AS DOUBLE) AS hrs_np,
            TRY_CAST(Hrs_PA AS DOUBLE) AS hrs_pa,
            TRY_CAST(Hrs_Pharmacist AS DOUBLE) AS hrs_pharmacist,
            TRY_CAST(Hrs_Dietician AS DOUBLE) AS hrs_dietician,
            -- Therapy
            TRY_CAST(Hrs_OT AS DOUBLE) AS hrs_ot,
            TRY_CAST(Hrs_OTasst AS DOUBLE) AS hrs_ot_asst,
            TRY_CAST(Hrs_PT AS DOUBLE) AS hrs_pt,
            TRY_CAST(Hrs_PTasst AS DOUBLE) AS hrs_pt_asst,
            TRY_CAST(Hrs_SpcLangPath AS DOUBLE) AS hrs_slp,
            TRY_CAST(Hrs_RespTher AS DOUBLE) AS hrs_resp_therapist,
            -- Social work / mental health
            TRY_CAST(Hrs_QualSocWrk AS DOUBLE) AS hrs_social_work,
            TRY_CAST(Hrs_MHSvc AS DOUBLE) AS hrs_mental_health,
            -- Activities
            TRY_CAST(Hrs_QualActvProf AS DOUBLE) AS hrs_activities,
            'data.cms.gov/pbj' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{NONNURSE_CSV}', ignore_errors=true)
        WHERE STATE IS NOT NULL AND LENGTH(STATE) = 2
    """)
    count = write_parquet(con, "_fact_pbj_nonnurse", _snapshot_path("pbj_nonnurse_staffing"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_pbj_nonnurse").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_pbj_nonnurse")
    return count


# ---------------------------------------------------------------------------
# Employee Detail
# ---------------------------------------------------------------------------

def build_fact_pbj_employee(con, dry_run: bool) -> int:
    print("Building fact_pbj_employee...")
    if not EMPLOYEE_CSV.exists():
        print(f"  SKIPPED — {EMPLOYEE_CSV.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_pbj_employee AS
        SELECT
            PROVNUM AS provider_ccn,
            STATE AS state_code,
            CY_Qtr AS quarter,
            CAST(WorkDate AS VARCHAR) AS work_date,
            CAST(SYS_EMPLEE_ID AS BIGINT) AS employee_id,
            TRY_CAST(EMPLEE_JOB_CD_ID AS INTEGER) AS job_code,
            TRY_CAST(EMP_CTR AS INTEGER) AS is_contractor,
            TRY_CAST(WORK_HRS_NUM AS DOUBLE) AS work_hours,
            'data.cms.gov/pbj' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{EMPLOYEE_CSV}', ignore_errors=true)
        WHERE STATE IS NOT NULL AND LENGTH(STATE) = 2
    """)
    count = write_parquet(con, "_fact_pbj_employee", _snapshot_path("pbj_employee"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _fact_pbj_employee")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_pbj_nurse_staffing": build_fact_pbj_nurse_staffing,
    "fact_pbj_nonnurse_staffing": build_fact_pbj_nonnurse_staffing,
    "fact_pbj_employee": build_fact_pbj_employee,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest PBJ staffing data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None)
    parser.add_argument("--skip-employee", action="store_true",
                        help="Skip the large employee detail file")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]
    if args.skip_employee and "fact_pbj_employee" in tables:
        tables.remove("fact_pbj_employee")

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("PBJ STAFFING DATA LAKE INGESTION COMPLETE")
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
            "source": "data.cms.gov/pbj",
            "quarter": "2025Q3",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_pbj_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
