#!/usr/bin/env python3
"""
build_lake_medicaid_supplemental.py — Ingest supplemental Medicaid datasets.

Reads from: data/raw/mltss_enrollees_2024.csv
             data/raw/medicaid_financial_mgmt.csv
             data/raw/eligibility_levels.csv
             data/raw/dsh_annual_reporting.csv
             data/raw/aca_ful_feb2026.csv
             data/raw/dq_atlas_states_measures.csv
Writes to:  data/lake/

Tables built:
  fact_mltss                 — Managed LTSS enrollees by state/year
  fact_financial_mgmt        — Medicaid financial management (CMS-64 detail)
  fact_eligibility_levels    — Medicaid/CHIP eligibility income thresholds
  fact_dsh_reporting         — DSH annual reporting requirements
  fact_aca_ful               — ACA Federal Upper Limits (pharmacy)
  fact_dq_atlas              — T-MSIS DQ Atlas assessments by state/topic

Usage:
  python3 scripts/build_lake_medicaid_supplemental.py
  python3 scripts/build_lake_medicaid_supplemental.py --dry-run
"""

import argparse
import csv
import io
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

STATE_NAME_TO_CODE = {
    'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
    'Colorado':'CO','Connecticut':'CT','Delaware':'DE','District of Columbia':'DC',
    'Florida':'FL','Georgia':'GA','Hawaii':'HI','Idaho':'ID','Illinois':'IL',
    'Indiana':'IN','Iowa':'IA','Kansas':'KS','Kentucky':'KY','Louisiana':'LA',
    'Maine':'ME','Maryland':'MD','Massachusetts':'MA','Michigan':'MI','Minnesota':'MN',
    'Mississippi':'MS','Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV',
    'New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY',
    'North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK','Oregon':'OR',
    'Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD',
    'Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT','Virginia':'VA',
    'Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY',
    'Puerto Rico':'PR','Guam':'GU','Virgin Islands':'VI',
    'American Samoa':'AS','Northern Mariana Islands':'MP',
}


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


def _register_state_map(con):
    con.execute("CREATE OR REPLACE TABLE _state_map (state_name VARCHAR, state_code VARCHAR(2))")
    for name, code in STATE_NAME_TO_CODE.items():
        con.execute("INSERT INTO _state_map VALUES (?, ?)", [name, code])


# ---------------------------------------------------------------------------
# MLTSS Enrollees
# ---------------------------------------------------------------------------

def build_fact_mltss(con, dry_run: bool) -> int:
    print("Building fact_mltss...")
    csv_path = RAW_DIR / "mltss_enrollees_2024.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    _register_state_map(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mltss AS
        SELECT
            sm.state_code,
            TRY_CAST(r.Year AS INTEGER) AS year,
            TRY_CAST(REPLACE(r."Total Any Managed Care Enrollees", ',', '') AS BIGINT) AS total_mc_enrollees,
            TRY_CAST(REPLACE(r."Comprehensive Managed Care LTSS Enrollees", ',', '') AS BIGINT) AS comprehensive_mltss_enrollees,
            TRY_CAST(REPLACE(REPLACE(r."Comprehensive Managed Care LTSS Percent", '%', ''), ',', '') AS DOUBLE) AS comprehensive_mltss_pct,
            TRY_CAST(REPLACE(r."Managed LTSS Only Enrollees", ',', '') AS BIGINT) AS mltss_only_enrollees,
            TRY_CAST(REPLACE(REPLACE(r."Managed LTSS Only Percent", '%', ''), ',', '') AS DOUBLE) AS mltss_only_pct,
            'data.medicaid.gov/mltss' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', ignore_errors=true) r
        JOIN _state_map sm ON TRIM(r.State) = sm.state_name
        WHERE r.State != 'TOTALS'
    """)

    count = write_parquet(con, "_fact_mltss", _snapshot_path("mltss"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_mltss").fetchone()[0]
    years = con.execute("SELECT COUNT(DISTINCT year) FROM _fact_mltss").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {years} years")
    con.execute("DROP TABLE IF EXISTS _fact_mltss")
    return count


# ---------------------------------------------------------------------------
# Medicaid Financial Management
# ---------------------------------------------------------------------------

def build_fact_financial_mgmt(con, dry_run: bool) -> int:
    print("Building fact_financial_mgmt...")
    csv_path = RAW_DIR / "medicaid_financial_mgmt.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    _register_state_map(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_fin_mgmt AS
        SELECT
            sm.state_code,
            r.Program AS program,
            r."Service Category" AS service_category,
            TRY_CAST(r."Total Computable" AS DOUBLE) AS total_computable,
            TRY_CAST(r."Federal Share" AS DOUBLE) AS federal_share,
            TRY_CAST(r."Federal Share Medicaid" AS DOUBLE) AS federal_share_medicaid,
            TRY_CAST(r."Federal Share ARRA" AS DOUBLE) AS federal_share_arra,
            TRY_CAST(r."Federal Share BIPP" AS DOUBLE) AS federal_share_bipp,
            TRY_CAST(r."State Share" AS DOUBLE) AS state_share,
            TRY_CAST(r.Year AS INTEGER) AS fiscal_year,
            'data.medicaid.gov/financial-mgmt' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', ignore_errors=true) r
        JOIN _state_map sm ON TRIM(r.State) = sm.state_name
    """)

    count = write_parquet(con, "_fact_fin_mgmt", _snapshot_path("financial_mgmt"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_fin_mgmt").fetchone()[0]
    total = con.execute("SELECT SUM(total_computable) FROM _fact_fin_mgmt").fetchone()[0]
    print(f"  {count:,} rows, {states} states, ${total/1e9:.1f}B total computable")
    con.execute("DROP TABLE IF EXISTS _fact_fin_mgmt")
    return count


# ---------------------------------------------------------------------------
# Eligibility Levels
# ---------------------------------------------------------------------------

def build_fact_eligibility_levels(con, dry_run: bool) -> int:
    print("Building fact_eligibility_levels...")
    csv_path = RAW_DIR / "eligibility_levels.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    _register_state_map(con)
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_elig_levels AS
        SELECT
            sm.state_code,
            REPLACE(r."Medicaid Ages 0-1", '%', '') AS medicaid_0_1,
            REPLACE(r."Medicaid Ages 1-5", '%', '') AS medicaid_1_5,
            REPLACE(r."Medicaid Ages 6-18", '%', '') AS medicaid_6_18,
            REPLACE(r."Separate CHIP", '%', '') AS separate_chip,
            REPLACE(r."Pregnant Women Medicaid", '%', '') AS pregnant_medicaid,
            REPLACE(r."Pregnant Women CHIP", '%', '') AS pregnant_chip,
            REPLACE(r."Parent/Caretaker", '%', '') AS parents_caretakers,
            REPLACE(r."Expansion to Adults", '%', '') AS expansion_adults,
            r."Parent/Caretaker Income Standard" AS parent_income_standard,
            r."Separate CHIP Ages" AS separate_chip_ages,
            'data.medicaid.gov/eligibility' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', ignore_errors=true) r
        JOIN _state_map sm ON TRIM(r.State) = sm.state_name
    """)

    count = write_parquet(con, "_fact_elig_levels", _snapshot_path("eligibility_levels"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_elig_levels").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_elig_levels")
    return count


# ---------------------------------------------------------------------------
# ACA Federal Upper Limits (pharmacy)
# ---------------------------------------------------------------------------

def build_fact_aca_ful(con, dry_run: bool) -> int:
    print("Building fact_aca_ful...")
    csv_path = RAW_DIR / "aca_ful_feb2026.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_aca_ful AS
        SELECT
            TRY_CAST("Product Group" AS INTEGER) AS product_group,
            Ingredient AS ingredient,
            Strength AS strength,
            Dosage AS dosage_form,
            Route AS route,
            "MDR Unit Type" AS unit_type,
            TRY_CAST("Weighted Average of AMPs" AS DOUBLE) AS weighted_avg_amp,
            TRY_CAST("ACA FUL" AS DOUBLE) AS aca_ful,
            TRY_CAST("Package Size" AS DOUBLE) AS package_size,
            NDC AS ndc,
            "A-Rated" AS a_rated,
            TRY_CAST(Year AS INTEGER) AS year,
            TRY_CAST(Month AS INTEGER) AS month,
            'data.medicaid.gov/aca-ful' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', ignore_errors=true)
        WHERE Ingredient IS NOT NULL
    """)

    count = write_parquet(con, "_fact_aca_ful", _snapshot_path("aca_ful"), dry_run)
    ingredients = con.execute("SELECT COUNT(DISTINCT ingredient) FROM _fact_aca_ful").fetchone()[0]
    print(f"  {count:,} rows, {ingredients:,} unique ingredients")
    con.execute("DROP TABLE IF EXISTS _fact_aca_ful")
    return count


# ---------------------------------------------------------------------------
# DQ Atlas (T-MSIS Data Quality)
# ---------------------------------------------------------------------------

def build_fact_dq_atlas(con, dry_run: bool) -> int:
    print("Building fact_dq_atlas...")
    csv_path = RAW_DIR / "dq_atlas_states_measures.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    # The DQ Atlas file has embedded CSVs in a JSON payload field.
    # Parse them out into structured rows.
    records = []

    raw_data = con.execute(f"""
        SELECT tafVersionId, stateId, payload
        FROM read_csv_auto('{csv_path}', ignore_errors=true)
    """).fetchall()

    for taf_version, state_id, payload_str in raw_data:
        if not payload_str:
            continue
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            continue

        file_content = payload.get('fileContent', '')
        if not file_content:
            continue

        reader = csv.reader(io.StringIO(file_content))
        header = None
        for row in reader:
            if not row or len(row) < 4:
                continue
            if row[0].startswith('"Title') or row[0].startswith('Title') or row[0].startswith('"Source') or row[0].startswith('Source'):
                continue
            if row[0] == 'State' or row[0] == '"State"':
                header = [c.strip('"') for c in row]
                continue
            if header and len(row) >= len(header):
                rec = dict(zip(header, [c.strip('"') for c in row]))
                state_name = rec.get('State', '').strip()
                state_code = STATE_NAME_TO_CODE.get(state_name)
                if state_code:
                    records.append({
                        'state_code': state_code,
                        'topic_area': rec.get('Topic Area', ''),
                        'topic': rec.get('Topic', ''),
                        'data_year': rec.get('Data Year', ''),
                        'dq_assessment': rec.get('DQ Assessment', ''),
                        'assessment_basis': rec.get('Assessment Basis', ''),
                        'assessment_value': rec.get('Assessment Basis Value', ''),
                        'data_version': rec.get('Data Version', ''),
                    })

    if not records:
        print("  SKIPPED — no records parsed from DQ Atlas")
        return 0

    print(f"  Parsed {len(records):,} DQ assessments")

    con.execute("""
        CREATE OR REPLACE TABLE _fact_dq_atlas (
            state_code VARCHAR, topic_area VARCHAR, topic VARCHAR,
            data_year VARCHAR, dq_assessment VARCHAR, assessment_basis VARCHAR,
            assessment_value VARCHAR, data_version VARCHAR
        )
    """)

    for rec in records:
        con.execute("INSERT INTO _fact_dq_atlas VALUES (?,?,?,?,?,?,?,?)", [
            rec['state_code'], rec['topic_area'], rec['topic'],
            rec['data_year'], rec['dq_assessment'], rec['assessment_basis'],
            rec['assessment_value'], rec['data_version'],
        ])

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dq_atlas2 AS
        SELECT *,
            'medicaid.gov/dq-atlas' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _fact_dq_atlas
    """)

    count = write_parquet(con, "_fact_dq_atlas2", _snapshot_path("dq_atlas"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_dq_atlas2").fetchone()[0]
    years = con.execute("SELECT COUNT(DISTINCT data_year) FROM _fact_dq_atlas2").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {years} data years")
    con.execute("DROP TABLE IF EXISTS _fact_dq_atlas")
    con.execute("DROP TABLE IF EXISTS _fact_dq_atlas2")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_mltss": build_fact_mltss,
    "fact_financial_mgmt": build_fact_financial_mgmt,
    "fact_eligibility_levels": build_fact_eligibility_levels,
    "fact_aca_ful": build_fact_aca_ful,
    "fact_dq_atlas": build_fact_dq_atlas,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest supplemental Medicaid data into Aradune lake")
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
    print("SUPPLEMENTAL MEDICAID DATA INGESTION COMPLETE")
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
            "source": "data.medicaid.gov",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_supplemental_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
