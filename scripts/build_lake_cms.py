#!/usr/bin/env python3
"""
build_lake_cms.py — Migrate CMS supplemental data from SQLite into the Aradune data lake.

Reads from: tools/mfs_scraper/aradune.db
Writes to:  data/lake/

Tables migrated:
  Dimensions:
    dim_hcpcs              — 8.6K HCPCS Level II code definitions
  Facts:
    fact_drug_utilization  — 2.4M rows, State Drug Utilization Data (SDUD)
    fact_nadac             — 1.9M rows, National Average Drug Acquisition Cost
    fact_managed_care      — 7.1K rows, MCO plan enrollment by state/year
    fact_dsh_payment       — 49 rows, Disproportionate Share Hospital payments
    fact_spa               — 962+144 rows, State Plan Amendments + CF extracts
    fact_fmap              — 51 rows, Federal Medical Assistance Percentages
  Reference:
    ref_drug_rebate        — 1.9M rows, drug rebate product catalog
    ref_ncci_edits         — 2.5M rows, NCCI code pair edits
    ref_1115_waivers       — 647 rows, Section 1115 waiver approvals

Usage:
  python3 scripts/build_lake_cms.py
  python3 scripts/build_lake_cms.py --dry-run
  python3 scripts/build_lake_cms.py --only fact_drug_utilization,fact_nadac
"""

import argparse
import json
import sqlite3
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQLITE_DB = PROJECT_ROOT / "tools" / "mfs_scraper" / "aradune.db"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"
REF_DIR = LAKE_DIR / "reference"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to Parquet. Returns row count."""
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


# ---------------------------------------------------------------------------
# DIMENSION: HCPCS Level II codes
# ---------------------------------------------------------------------------

def build_dim_hcpcs(con, dry_run: bool) -> int:
    print("Building dim_hcpcs...")
    con.execute("""
        CREATE OR REPLACE TABLE _dim_hcpcs AS
        SELECT
            hcpcs_code,
            long_description AS description,
            short_description,
            pricing_indicator,
            coverage_code,
            type_of_service
        FROM adb.hcpcs_codes
    """)
    out = DIM_DIR / "dim_hcpcs.parquet"
    count = write_parquet(con, "_dim_hcpcs", out, dry_run)
    con.execute("DROP TABLE IF EXISTS _dim_hcpcs")
    return count


# ---------------------------------------------------------------------------
# FACT: State Drug Utilization Data (SDUD)
# ---------------------------------------------------------------------------

def build_fact_drug_utilization(con, dry_run: bool) -> int:
    print("Building fact_drug_utilization...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_drug_utilization AS
        SELECT
            state_code,
            year,
            quarter,
            ndc,
            product_name,
            CAST(units_reimbursed AS DOUBLE) AS units_reimbursed,
            CAST(number_of_prescriptions AS INTEGER) AS prescription_count,
            CAST(total_amount_reimbursed AS DOUBLE) AS total_reimbursed,
            CAST(medicaid_amount_reimbursed AS DOUBLE) AS medicaid_reimbursed,
            'data.medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM adb.drug_utilization
        WHERE state_code IS NOT NULL
    """)
    count = write_parquet(con, "_fact_drug_utilization", _snapshot_path("drug_utilization"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_drug_utilization").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_drug_utilization").fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_drug_utilization")
    return count


# ---------------------------------------------------------------------------
# FACT: NADAC pharmacy pricing
# ---------------------------------------------------------------------------

def build_fact_nadac(con, dry_run: bool) -> int:
    print("Building fact_nadac...")
    # Read via Python sqlite3 to avoid date parsing issues
    scon = sqlite3.connect(str(SQLITE_DB))
    rows = scon.execute("""
        SELECT ndc, ndc_description, nadac_per_unit, effective_date,
               pricing_unit, pharmacy_type, otc
        FROM nadac_pricing
    """).fetchall()
    scon.close()

    con.execute("""
        CREATE OR REPLACE TABLE _nadac_raw (
            ndc VARCHAR, ndc_description VARCHAR, nadac_per_unit DOUBLE,
            effective_date_raw VARCHAR, pricing_unit VARCHAR,
            pharmacy_type VARCHAR, otc VARCHAR
        )
    """)
    con.executemany("INSERT INTO _nadac_raw VALUES (?,?,?,?,?,?,?)", rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_nadac AS
        SELECT
            ndc,
            ndc_description,
            nadac_per_unit,
            TRY_STRPTIME(effective_date_raw, '%m/%d/%Y')::DATE AS effective_date,
            pricing_unit,
            pharmacy_type,
            CASE WHEN otc = 'Y' THEN TRUE ELSE FALSE END AS is_otc,
            'medicaid.gov/nadac' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _nadac_raw
    """)
    con.execute("DROP TABLE IF EXISTS _nadac_raw")
    count = write_parquet(con, "_fact_nadac", _snapshot_path("nadac"), dry_run)
    ndcs = con.execute("SELECT COUNT(DISTINCT ndc) FROM _fact_nadac").fetchone()[0]
    print(f"  {count:,} rows, {ndcs:,} distinct NDCs")
    con.execute("DROP TABLE IF EXISTS _fact_nadac")
    return count


# ---------------------------------------------------------------------------
# FACT: Managed Care Enrollment
# ---------------------------------------------------------------------------

def build_fact_managed_care(con, dry_run: bool) -> int:
    print("Building fact_managed_care...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_managed_care AS
        SELECT
            state_code,
            year,
            plan_name,
            plan_type,
            CAST(enrollment AS INTEGER) AS enrollment,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM adb.managed_care_enrollment
        WHERE state_code IS NOT NULL
    """)
    count = write_parquet(con, "_fact_managed_care", _snapshot_path("managed_care"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_managed_care").fetchone()[0]
    plans = con.execute("SELECT COUNT(DISTINCT plan_name) FROM _fact_managed_care").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {plans:,} plans")
    con.execute("DROP TABLE IF EXISTS _fact_managed_care")
    return count


# ---------------------------------------------------------------------------
# FACT: DSH Payments
# ---------------------------------------------------------------------------

def build_fact_dsh_payment(con, dry_run: bool) -> int:
    print("Building fact_dsh_payment...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dsh_payment AS
        SELECT
            state_code,
            year,
            hospital_name,
            CAST(dsh_payment AS DOUBLE) AS dsh_payment,
            CAST(uncompensated_care_cost AS DOUBLE) AS uncompensated_care_cost,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM adb.dsh_payments
        WHERE state_code IS NOT NULL
    """)
    count = write_parquet(con, "_fact_dsh_payment", _snapshot_path("dsh_payment"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _fact_dsh_payment")
    return count


# ---------------------------------------------------------------------------
# FACT: FMAP Rates
# ---------------------------------------------------------------------------

def build_fact_fmap(con, dry_run: bool) -> int:
    print("Building fact_fmap...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_fmap AS
        SELECT
            state_code,
            fiscal_year,
            CAST(fmap_rate AS DOUBLE) AS fmap_rate,
            CAST(efmap_rate AS DOUBLE) AS efmap_rate,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM adb.fmap_rates
        WHERE state_code IS NOT NULL
    """)
    count = write_parquet(con, "_fact_fmap", _snapshot_path("fmap"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _fact_fmap")
    return count


# ---------------------------------------------------------------------------
# FACT: State Plan Amendments
# ---------------------------------------------------------------------------

def build_fact_spa(con, dry_run: bool) -> int:
    print("Building fact_spa...")
    # spa_tracker has text dates — read via Python
    scon = sqlite3.connect(str(SQLITE_DB))

    # SPA tracker
    tracker_rows = scon.execute("""
        SELECT state_code, spa_number, title, effective_date, approval_date,
               topic, affects_419b, pdf_url, summary
        FROM spa_tracker
    """).fetchall()

    # SPA extracts (conversion factors from PDFs)
    extract_rows = scon.execute("""
        SELECT state_code, spa_number, conversion_factor, cf_effective_date,
               rvu_year, methodology_keywords, anesthesia_cf
        FROM spa_extracts
    """).fetchall()
    scon.close()

    con.execute("""
        CREATE OR REPLACE TABLE _spa_tracker (
            state_code VARCHAR, spa_number VARCHAR, title VARCHAR,
            effective_date VARCHAR, approval_date VARCHAR,
            topic VARCHAR, affects_419b INTEGER, pdf_url VARCHAR, summary VARCHAR
        )
    """)
    con.executemany("INSERT INTO _spa_tracker VALUES (?,?,?,?,?,?,?,?,?)", tracker_rows)

    con.execute("""
        CREATE OR REPLACE TABLE _spa_extracts (
            state_code VARCHAR, spa_number VARCHAR, conversion_factor VARCHAR,
            cf_effective_date VARCHAR, rvu_year VARCHAR,
            methodology_keywords VARCHAR, anesthesia_cf VARCHAR
        )
    """)
    con.executemany("INSERT INTO _spa_extracts VALUES (?,?,?,?,?,?,?)", extract_rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_spa AS
        SELECT
            t.state_code,
            t.spa_number,
            t.title,
            t.effective_date,
            t.approval_date,
            t.topic,
            CASE WHEN t.affects_419b = 1 THEN TRUE ELSE FALSE END AS affects_rate_setting,
            t.pdf_url,
            t.summary,
            TRY_CAST(e.conversion_factor AS DOUBLE) AS conversion_factor,
            TRY_CAST(e.rvu_year AS INTEGER) AS rvu_year,
            e.methodology_keywords,
            TRY_CAST(e.anesthesia_cf AS DOUBLE) AS anesthesia_cf,
            'medicaid.gov' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _spa_tracker t
        LEFT JOIN _spa_extracts e ON t.state_code = e.state_code AND t.spa_number = e.spa_number
    """)
    con.execute("DROP TABLE IF EXISTS _spa_tracker")
    con.execute("DROP TABLE IF EXISTS _spa_extracts")
    count = write_parquet(con, "_fact_spa", _snapshot_path("spa"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_spa").fetchone()[0]
    print(f"  {count:,} SPAs across {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_spa")
    return count


# ---------------------------------------------------------------------------
# REFERENCE: Drug Rebate Products
# ---------------------------------------------------------------------------

def build_ref_drug_rebate(con, dry_run: bool) -> int:
    print("Building ref_drug_rebate...")
    scon = sqlite3.connect(str(SQLITE_DB))
    rows = scon.execute("""
        SELECT ndc, labeler_name, product_name, drug_category,
               unit_type, units_per_pkg, effective_date, termination_date, source
        FROM drug_rebate_products
    """).fetchall()
    scon.close()

    con.execute("""
        CREATE OR REPLACE TABLE _rebate_raw (
            ndc VARCHAR, labeler_name VARCHAR, product_name VARCHAR,
            drug_category VARCHAR, unit_type VARCHAR, units_per_pkg BIGINT,
            effective_date_raw VARCHAR, termination_date_raw VARCHAR, source VARCHAR
        )
    """)
    con.executemany("INSERT INTO _rebate_raw VALUES (?,?,?,?,?,?,?,?,?)", rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_drug_rebate AS
        SELECT
            ndc,
            labeler_name,
            product_name,
            drug_category,
            unit_type,
            units_per_pkg,
            TRY_STRPTIME(effective_date_raw, '%m/%d/%Y')::DATE AS effective_date,
            TRY_STRPTIME(termination_date_raw, '%m/%d/%Y')::DATE AS termination_date,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _rebate_raw
    """)
    con.execute("DROP TABLE IF EXISTS _rebate_raw")
    out = REF_DIR / "ref_drug_rebate.parquet"
    count = write_parquet(con, "_ref_drug_rebate", out, dry_run)
    labelers = con.execute("SELECT COUNT(DISTINCT labeler_name) FROM _ref_drug_rebate").fetchone()[0]
    print(f"  {count:,} products, {labelers:,} labelers")
    con.execute("DROP TABLE IF EXISTS _ref_drug_rebate")
    return count


# ---------------------------------------------------------------------------
# REFERENCE: NCCI Edits
# ---------------------------------------------------------------------------

def build_ref_ncci_edits(con, dry_run: bool) -> int:
    print("Building ref_ncci_edits...")
    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_ncci_edits AS
        SELECT
            column1_code,
            column2_code,
            effective_date,
            deletion_date,
            edit_type,
            modifier_indicator,
            ptp_edit_rationale AS rationale,
            source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM adb.ncci_edits
    """)
    out = REF_DIR / "ref_ncci_edits.parquet"
    count = write_parquet(con, "_ref_ncci_edits", out, dry_run)
    print(f"  {count:,} edit pairs")
    con.execute("DROP TABLE IF EXISTS _ref_ncci_edits")
    return count


# ---------------------------------------------------------------------------
# REFERENCE: 1115 Waivers
# ---------------------------------------------------------------------------

def build_ref_1115_waivers(con, dry_run: bool) -> int:
    print("Building ref_1115_waivers...")
    scon = sqlite3.connect(str(SQLITE_DB))
    rows = scon.execute("""
        SELECT state_code, waiver_name, waiver_number, approval_date,
               expiration_date, description, waiver_type, status, source
        FROM section_1115_waivers
    """).fetchall()
    scon.close()

    con.execute("""
        CREATE OR REPLACE TABLE _ref_waivers (
            state_code VARCHAR, waiver_name VARCHAR, waiver_number VARCHAR,
            approval_date VARCHAR, expiration_date VARCHAR, description VARCHAR,
            waiver_type VARCHAR, status VARCHAR, source VARCHAR
        )
    """)
    con.executemany("INSERT INTO _ref_waivers VALUES (?,?,?,?,?,?,?,?,?)", rows)

    con.execute(f"""
        ALTER TABLE _ref_waivers ADD COLUMN snapshot_date DATE;
    """)
    con.execute(f"UPDATE _ref_waivers SET snapshot_date = DATE '{SNAPSHOT_DATE}'")
    out = REF_DIR / "ref_1115_waivers.parquet"
    count = write_parquet(con, "_ref_waivers", out, dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _ref_waivers").fetchone()[0]
    print(f"  {count:,} waivers across {states} states")
    con.execute("DROP TABLE IF EXISTS _ref_waivers")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    # Dimensions
    "dim_hcpcs": build_dim_hcpcs,
    # Facts
    "fact_drug_utilization": build_fact_drug_utilization,
    "fact_nadac": build_fact_nadac,
    "fact_managed_care": build_fact_managed_care,
    "fact_dsh_payment": build_fact_dsh_payment,
    "fact_fmap": build_fact_fmap,
    "fact_spa": build_fact_spa,
    # Reference
    "ref_drug_rebate": build_ref_drug_rebate,
    "ref_ncci_edits": build_ref_ncci_edits,
    "ref_1115_waivers": build_ref_1115_waivers,
}


def main():
    parser = argparse.ArgumentParser(description="Migrate CMS data from SQLite to Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    args = parser.parse_args()

    if not SQLITE_DB.exists():
        print(f"ERROR: SQLite not found at {SQLITE_DB}", file=sys.stderr)
        sys.exit(1)

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]
        invalid = [t for t in tables if t not in ALL_TABLES]
        if invalid:
            print(f"ERROR: Unknown tables: {invalid}", file=sys.stderr)
            print(f"Valid: {list(ALL_TABLES.keys())}", file=sys.stderr)
            sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{SQLITE_DB}' AS adb (TYPE sqlite, READ_ONLY)")

    totals = {}
    for name in tables:
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    # Summary
    print("=" * 60)
    print("CMS LAKE MIGRATION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>10,} rows")

    if not args.dry_run:
        # Update manifest
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source": str(SQLITE_DB),
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_cms_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
