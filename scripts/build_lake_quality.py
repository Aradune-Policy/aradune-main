#!/usr/bin/env python3
"""
build_lake_quality.py — Ingest quality, facility, and provider characteristic data into the lake.

Reads from: data/raw/nh_five_star_jan2026.json
             data/raw/hac_measures_2025.csv
             data/raw/pos_hospital_q4_2025.csv
             data/raw/pos_iqies_q4_2025.csv
Writes to:  data/lake/

Tables built:
  Facts:
    fact_five_star            — Nursing home Five-Star quality ratings (14,710 facilities)
    fact_hac_measure          — Hospital-acquired condition measures (12,120 rows)
    fact_pos_hospital         — Provider of Services: hospitals (13,510 facilities)
    fact_pos_other            — Provider of Services: HHA, hospice, ESRD, RHC, FQHC (58,001 rows)

Usage:
  python3 scripts/build_lake_quality.py
  python3 scripts/build_lake_quality.py --dry-run
  python3 scripts/build_lake_quality.py --only fact_five_star
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

FIVE_STAR_JSON = RAW_DIR / "nh_five_star_jan2026.json"
HAC_CSV = RAW_DIR / "hac_measures_2025.csv"
POS_HOSPITAL_CSV = RAW_DIR / "pos_hospital_q4_2025.csv"
POS_IQIES_CSV = RAW_DIR / "pos_iqies_q4_2025.csv"


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
# Five-Star Nursing Home Ratings
# ---------------------------------------------------------------------------

def build_fact_five_star(con, dry_run: bool) -> int:
    print("Building fact_five_star...")
    if not FIVE_STAR_JSON.exists():
        print(f"  SKIPPED — {FIVE_STAR_JSON.name} not found")
        return 0

    with open(FIVE_STAR_JSON) as f:
        data = json.load(f)

    print(f"  Loaded {len(data):,} records from JSON")

    # Insert JSON into DuckDB via temp table
    con.execute("CREATE OR REPLACE TABLE _raw_five_star AS SELECT * FROM read_json_auto(?)", [str(FIVE_STAR_JSON)])

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_five_star AS
        SELECT
            cms_certification_number_ccn AS provider_ccn,
            provider_name AS facility_name,
            citytown AS city,
            state AS state_code,
            countyparish AS county,
            TRY_CAST(zip_code AS VARCHAR) AS zip_code,
            ownership_type,
            provider_type,
            TRY_CAST(number_of_certified_beds AS INTEGER) AS certified_beds,
            TRY_CAST(average_number_of_residents_per_day AS DOUBLE) AS avg_residents_per_day,
            provider_resides_in_hospital = 'Y' AS in_hospital,
            continuing_care_retirement_community = 'Y' AS is_ccrc,
            -- Ratings
            TRY_CAST(overall_rating AS INTEGER) AS overall_rating,
            TRY_CAST(health_inspection_rating AS INTEGER) AS health_inspection_rating,
            TRY_CAST(qm_rating AS INTEGER) AS qm_rating,
            TRY_CAST(longstay_qm_rating AS INTEGER) AS longstay_qm_rating,
            TRY_CAST(shortstay_qm_rating AS INTEGER) AS shortstay_qm_rating,
            TRY_CAST(staffing_rating AS INTEGER) AS staffing_rating,
            -- Staffing HPRD (reported)
            TRY_CAST(reported_nurse_aide_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_cna,
            TRY_CAST(reported_lpn_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_lpn,
            TRY_CAST(reported_rn_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_rn,
            TRY_CAST(reported_total_nurse_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_total,
            TRY_CAST(reported_physical_therapist_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_pt,
            -- Casemix-adjusted staffing
            TRY_CAST(adjusted_total_nurse_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_total_adjusted,
            TRY_CAST(adjusted_rn_staffing_hours_per_resident_per_day AS DOUBLE) AS hprd_rn_adjusted,
            -- Turnover
            TRY_CAST(total_nursing_staff_turnover AS DOUBLE) AS turnover_total_pct,
            TRY_CAST(registered_nurse_turnover AS DOUBLE) AS turnover_rn_pct,
            TRY_CAST(number_of_administrators_who_have_left_the_nursing_home AS INTEGER) AS admin_departures,
            -- Deficiencies & penalties
            TRY_CAST(rating_cycle_1_total_number_of_health_deficiencies AS INTEGER) AS deficiency_count,
            TRY_CAST(rating_cycle_1_health_deficiency_score AS DOUBLE) AS deficiency_score,
            TRY_CAST(total_weighted_health_survey_score AS DOUBLE) AS weighted_health_score,
            TRY_CAST(number_of_fines AS INTEGER) AS fine_count,
            TRY_CAST(total_amount_of_fines_in_dollars AS DOUBLE) AS fine_total_dollars,
            TRY_CAST(number_of_payment_denials AS INTEGER) AS payment_denial_count,
            TRY_CAST(total_number_of_penalties AS INTEGER) AS total_penalties,
            TRY_CAST(number_of_citations_from_infection_control_inspections AS INTEGER) AS infection_control_citations,
            -- Special status
            special_focus_status,
            abuse_icon = 'Y' AS abuse_flag,
            provider_changed_ownership_in_last_12_months = 'Y' AS recent_ownership_change,
            -- Chain info
            chain_name,
            TRY_CAST(number_of_facilities_in_chain AS INTEGER) AS chain_size,
            -- Location
            TRY_CAST(latitude AS DOUBLE) AS latitude,
            TRY_CAST(longitude AS DOUBLE) AS longitude,
            -- Metadata
            'data.cms.gov/care-compare' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _raw_five_star
        WHERE state IS NOT NULL AND LENGTH(state) = 2
    """)

    count = write_parquet(con, "_fact_five_star", _snapshot_path("five_star"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_five_star").fetchone()[0]
    avg_rating = con.execute("SELECT ROUND(AVG(overall_rating), 2) FROM _fact_five_star WHERE overall_rating > 0").fetchone()[0]
    avg_hprd = con.execute("SELECT ROUND(AVG(hprd_total), 2) FROM _fact_five_star WHERE hprd_total > 0").fetchone()[0]
    print(f"  {count:,} facilities, {states} states, avg rating: {avg_rating}, avg HPRD: {avg_hprd}")
    con.execute("DROP TABLE IF EXISTS _raw_five_star")
    con.execute("DROP TABLE IF EXISTS _fact_five_star")
    return count


# ---------------------------------------------------------------------------
# Hospital-Acquired Condition (HAC) Measures
# ---------------------------------------------------------------------------

def build_fact_hac_measure(con, dry_run: bool) -> int:
    print("Building fact_hac_measure...")
    if not HAC_CSV.exists():
        print(f"  SKIPPED — {HAC_CSV.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hac AS
        SELECT
            Provider_ID AS provider_id,
            Measure AS measure_name,
            TRY_CAST(Rate AS DOUBLE) AS rate,
            Footnote AS footnote,
            Start_Quarter AS start_quarter,
            End_Quarter AS end_quarter,
            'data.cms.gov/hac' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{HAC_CSV}', ignore_errors=true)
        WHERE Provider_ID IS NOT NULL
    """)

    count = write_parquet(con, "_fact_hac", _snapshot_path("hac_measure"), dry_run)
    providers = con.execute("SELECT COUNT(DISTINCT provider_id) FROM _fact_hac").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_name) FROM _fact_hac").fetchone()[0]
    print(f"  {count:,} rows, {providers:,} providers, {measures} measures")
    con.execute("DROP TABLE IF EXISTS _fact_hac")
    return count


# ---------------------------------------------------------------------------
# Provider of Services — Hospitals (Category 01)
# ---------------------------------------------------------------------------

def build_fact_pos_hospital(con, dry_run: bool) -> int:
    print("Building fact_pos_hospital...")
    if not POS_HOSPITAL_CSV.exists():
        print(f"  SKIPPED — {POS_HOSPITAL_CSV.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_pos_hosp AS
        SELECT
            PRVDR_NUM AS provider_id,
            FAC_NAME AS facility_name,
            STATE_CD AS state_code,
            CITY_NAME AS city,
            TRY_CAST(SSA_CNTY_CD AS VARCHAR) AS county_ssa,
            TRY_CAST(ZIP_CD AS VARCHAR) AS zip_code,
            TRY_CAST(FIPS_STATE_CD AS VARCHAR) AS fips_state,
            TRY_CAST(FIPS_CNTY_CD AS VARCHAR) AS fips_county,
            TRY_CAST(CBSA_CD AS VARCHAR) AS cbsa_code,
            CBSA_URBN_RRL_IND AS urban_rural,
            PRVDR_CTGRY_SBTYP_CD AS provider_subtype,
            TRY_CAST(GNRL_FAC_TYPE_CD AS VARCHAR) AS facility_type,
            TRY_CAST(GNRL_CNTL_TYPE_CD AS VARCHAR) AS control_type,
            TRY_CAST(PGM_PRTCPTN_CD AS INTEGER) AS program_participation,
            TRY_CAST(BED_CNT AS INTEGER) AS total_beds,
            TRY_CAST(CRTFD_BED_CNT AS INTEGER) AS certified_beds,
            TRY_CAST(MDCR_SNF_BED_CNT AS INTEGER) AS medicare_snf_beds,
            TRY_CAST(MDCD_NF_BED_CNT AS INTEGER) AS medicaid_nf_beds,
            TRY_CAST(MDCR_MDCD_SNF_BED_CNT AS INTEGER) AS dual_snf_beds,
            TRY_CAST(PSYCH_UNIT_BED_CNT AS INTEGER) AS psych_beds,
            TRY_CAST(REHAB_UNIT_BED_CNT AS INTEGER) AS rehab_beds,
            TRY_CAST(ICFIID_BED_CNT AS INTEGER) AS icf_iid_beds,
            TRY_CAST(AIDS_BED_CNT AS INTEGER) AS aids_beds,
            TRY_CAST(ALZHMR_BED_CNT AS INTEGER) AS alzheimer_beds,
            TRY_CAST(HOSPC_BED_CNT AS INTEGER) AS hospice_beds,
            TRY_CAST(VNTLTR_BED_CNT AS INTEGER) AS ventilator_beds,
            TRY_CAST(OPRTG_ROOM_CNT AS INTEGER) AS operating_rooms,
            -- Staffing counts
            TRY_CAST(RN_CNT AS DOUBLE) AS rn_count,
            TRY_CAST(LPN_LVN_CNT AS DOUBLE) AS lpn_count,
            TRY_CAST(EMPLEE_CNT AS DOUBLE) AS total_employees,
            TRY_CAST(PHYSN_CNT AS DOUBLE) AS physician_count,
            -- Services
            ICU_SRVC_CD IS NOT NULL AND ICU_SRVC_CD != '0' AS has_icu,
            EMER_PSYCH_SRVC_CD IS NOT NULL AND EMER_PSYCH_SRVC_CD != '0' AS has_er_psych,
            OB_SRVC_CD IS NOT NULL AND OB_SRVC_CD != '0' AS has_ob,
            -- Teaching
            MDCL_SCHL_AFLTN_CD AS medical_school_affiliation,
            INTRN_RSDNT_SRVC_CD AS teaching_status,
            -- Dates
            ORGNL_PRTCPTN_DT AS original_participation_date,
            CRTFCTN_DT AS certification_date,
            -- Metadata
            'data.cms.gov/pos' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{POS_HOSPITAL_CSV}', ignore_errors=true)
        WHERE PRVDR_CTGRY_CD = '01'
          AND STATE_CD IS NOT NULL AND LENGTH(STATE_CD) = 2
    """)

    count = write_parquet(con, "_fact_pos_hosp", _snapshot_path("pos_hospital"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_pos_hosp").fetchone()[0]
    beds = con.execute("SELECT SUM(total_beds) FROM _fact_pos_hosp WHERE total_beds > 0").fetchone()[0]
    print(f"  {count:,} hospitals, {states} states, {beds:,} total beds")
    con.execute("DROP TABLE IF EXISTS _fact_pos_hosp")
    return count


# ---------------------------------------------------------------------------
# Provider of Services — Other (HHA, Hospice, ESRD, RHC, FQHC from both files)
# ---------------------------------------------------------------------------

def build_fact_pos_other(con, dry_run: bool) -> int:
    print("Building fact_pos_other...")

    parts = []

    # From POS Hospital file — non-hospital categories
    if POS_HOSPITAL_CSV.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _pos_other_hosp AS
            SELECT
                PRVDR_NUM AS provider_id,
                FAC_NAME AS facility_name,
                STATE_CD AS state_code,
                CITY_NAME AS city,
                TRY_CAST(ZIP_CD AS VARCHAR) AS zip_code,
                TRY_CAST(FIPS_STATE_CD AS VARCHAR) AS fips_state,
                TRY_CAST(FIPS_CNTY_CD AS VARCHAR) AS fips_county,
                TRY_CAST(CBSA_CD AS VARCHAR) AS cbsa_code,
                CBSA_URBN_RRL_IND AS urban_rural,
                PRVDR_CTGRY_CD AS provider_category,
                CASE PRVDR_CTGRY_CD
                    WHEN '07' THEN 'Portable X-Ray'
                    WHEN '08' THEN 'HHA'
                    WHEN '09' THEN 'Hospice'
                    WHEN '11' THEN 'ESRD'
                    WHEN '12' THEN 'RHC'
                    WHEN '14' THEN 'CORF'
                    WHEN '19' THEN 'FQHC'
                    WHEN '21' THEN 'CMHC'
                    ELSE 'Other'
                END AS provider_type_name,
                TRY_CAST(GNRL_CNTL_TYPE_CD AS VARCHAR) AS control_type,
                TRY_CAST(BED_CNT AS INTEGER) AS bed_count,
                TRY_CAST(EMPLEE_CNT AS DOUBLE) AS employee_count,
                ORGNL_PRTCPTN_DT AS original_participation_date,
                'data.cms.gov/pos' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{POS_HOSPITAL_CSV}', ignore_errors=true)
            WHERE PRVDR_CTGRY_CD != '01'
              AND STATE_CD IS NOT NULL AND LENGTH(STATE_CD) = 2
        """)
        parts.append("_pos_other_hosp")

    # From POS iQIES file
    if POS_IQIES_CSV.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _pos_other_iqies AS
            SELECT
                prvdr_num AS provider_id,
                fac_name AS facility_name,
                state_cd AS state_code,
                city_name AS city,
                TRY_CAST(zip_cd AS VARCHAR) AS zip_code,
                TRY_CAST(fips_state_cd AS VARCHAR) AS fips_state,
                TRY_CAST(fips_cnty_cd AS VARCHAR) AS fips_county,
                TRY_CAST(cbsa_cd AS VARCHAR) AS cbsa_code,
                cbsa_urbn_rrl_ind AS urban_rural,
                CAST(prvdr_type_id AS VARCHAR) AS provider_category,
                CASE CAST(prvdr_type_id AS VARCHAR)
                    WHEN '3' THEN 'HHA'
                    WHEN '11' THEN 'ESRD'
                    WHEN '12' THEN 'SNF/NF'
                    WHEN '13' THEN 'ICF/IID'
                    WHEN '20' THEN 'Hospice'
                    ELSE 'Other'
                END AS provider_type_name,
                control_type AS control_type,
                TRY_CAST(bed_cnt AS INTEGER) AS bed_count,
                TRY_CAST(emplee_cnt AS DOUBLE) AS employee_count,
                orgnl_prtcptn_dt AS original_participation_date,
                'data.cms.gov/pos-iqies' AS source,
                DATE '{SNAPSHOT_DATE}' AS snapshot_date
            FROM read_csv_auto('{POS_IQIES_CSV}', ignore_errors=true)
            WHERE state_cd IS NOT NULL AND LENGTH(state_cd) = 2
        """)
        parts.append("_pos_other_iqies")

    if not parts:
        print("  SKIPPED — no POS files found")
        return 0

    union_sql = " UNION ALL ".join(f"SELECT * FROM {p}" for p in parts)
    con.execute(f"CREATE OR REPLACE TABLE _fact_pos_other AS {union_sql}")

    count = write_parquet(con, "_fact_pos_other", _snapshot_path("pos_other"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_pos_other").fetchone()[0]
    by_type = con.execute("""
        SELECT provider_type_name, COUNT(*)
        FROM _fact_pos_other
        GROUP BY provider_type_name
        ORDER BY COUNT(*) DESC
    """).fetchall()
    print(f"  {count:,} providers, {states} states")
    for t, c in by_type:
        print(f"    {t}: {c:,}")

    for p in parts:
        con.execute(f"DROP TABLE IF EXISTS {p}")
    con.execute("DROP TABLE IF EXISTS _fact_pos_other")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_five_star": build_fact_five_star,
    "fact_hac_measure": build_fact_hac_measure,
    "fact_pos_hospital": build_fact_pos_hospital,
    "fact_pos_other": build_fact_pos_other,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest quality/facility data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None)
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

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
    print("QUALITY & FACILITY DATA LAKE INGESTION COMPLETE")
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
            "source": "data.cms.gov",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_quality_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
