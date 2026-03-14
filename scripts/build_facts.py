#!/usr/bin/env python3
"""
build_facts.py — Build unified fact Parquet files for the Aradune data lake.

Reads from:
  - SQLite: tools/mfs_scraper/aradune.db (rates, medicaid_enrollment, quality_measures, cms64_expenditures)
  - DuckDB: tools/mfs_scraper/aradune_cpra.duckdb (fact_medicare_rate, fact_rate_comparison, fact_dq_flags)
  - Parquet: data/lake/dimension/ (shared dimensions for joins & enrichment)

Writes to:
  data/lake/fact/
    medicaid_rate/snapshot=YYYY-MM-DD/data.parquet
    medicare_rate/snapshot=YYYY-MM-DD/data.parquet
    medicare_rate_state/snapshot=YYYY-MM-DD/data.parquet
    rate_comparison/snapshot=YYYY-MM-DD/data.parquet
    dq_flag/snapshot=YYYY-MM-DD/data.parquet
    enrollment/snapshot=YYYY-MM-DD/data.parquet
    quality_measure/snapshot=YYYY-MM-DD/data.parquet
    expenditure/snapshot=YYYY-MM-DD/data.parquet

Usage:
  python3 scripts/build_facts.py
  python3 scripts/build_facts.py --dry-run
  python3 scripts/build_facts.py --only medicaid_rate,rate_comparison
"""

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQLITE_DB = PROJECT_ROOT / "tools" / "mfs_scraper" / "aradune.db"
CPRA_DUCKDB = PROJECT_ROOT / "tools" / "mfs_scraper" / "aradune_cpra.duckdb"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

ALL_FACTS = [
    "medicaid_rate",
    "medicare_rate",
    "medicare_rate_state",
    "rate_comparison",
    "dq_flag",
    "enrollment",
    "quality_measure",
    "expenditure",
]


def write_parquet(con, table_name: str, fact_name: str, dry_run: bool) -> int:
    """Write a DuckDB table to a snapshot-partitioned Parquet file."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    if not dry_run and count > 0:
        out_dir = FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"
        con.execute(f"COPY {table_name} TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_file.stat().st_size / 1024
        print(f"  Wrote {out_file} ({size_kb:.1f} KB)")

    return count


def build_fact_medicaid_rate(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_medicaid_rate from SQLite rates table.

    Handles the date format issue (SQLite stores "10/1/2025" not "2025-10-01").
    """
    print("Building fact_medicaid_rate...")

    # The SQLite rates table has effective_date as text in M/D/YYYY format.
    # We use TRY_STRPTIME to parse it safely.
    # SQLite dates are stored as text (M/D/YYYY). DuckDB's SQLite scanner auto-casts
    # them to DATE and fails. Bypass: read via Python's sqlite3, insert into DuckDB.
    import sqlite3
    scon = sqlite3.connect(str(SQLITE_DB))
    scon.row_factory = sqlite3.Row
    cursor = scon.execute("""
        SELECT state_code, procedure_code, modifier,
               rate, rate_facility, rate_nonfacility,
               effective_date, end_date,
               billing_unit, place_of_service, prior_auth, source_file
        FROM rates
        WHERE rate IS NOT NULL OR rate_facility IS NOT NULL OR rate_nonfacility IS NOT NULL
    """)
    rows = cursor.fetchall()
    scon.close()

    con.execute("""
        CREATE OR REPLACE TABLE _rates_raw (
            state_code VARCHAR, procedure_code VARCHAR, modifier VARCHAR,
            rate DOUBLE, rate_facility DOUBLE, rate_nonfacility DOUBLE,
            effective_date_raw VARCHAR, end_date_raw VARCHAR,
            billing_unit VARCHAR, place_of_service VARCHAR,
            prior_auth VARCHAR, source_file VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO _rates_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11]) for r in rows]
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_medicaid_rate AS
        SELECT
            state_code,
            procedure_code,
            COALESCE(modifier, '')             AS modifier,
            rate,
            rate_facility,
            rate_nonfacility,
            TRY_STRPTIME(effective_date_raw, '%m/%d/%Y')::DATE AS effective_date,
            TRY_STRPTIME(end_date_raw, '%m/%d/%Y')::DATE       AS end_date,
            billing_unit,
            place_of_service,
            CASE WHEN prior_auth IS NOT NULL AND prior_auth != '' AND prior_auth != '0'
                 THEN TRUE ELSE FALSE END      AS prior_auth,
            source_file,
            DATE '{SNAPSHOT_DATE}'             AS snapshot_date,
            '{RUN_ID}'                         AS pipeline_run_id
        FROM _rates_raw
    """)
    con.execute("DROP TABLE IF EXISTS _rates_raw")

    # --- Backfill NULL effective_date from source_file and fee schedule directory ---
    null_before = con.execute(
        "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
    ).fetchone()[0]

    if null_before > 0:
        print(f"  Backfilling effective_date for {null_before:,} rows with NULL dates...")

        # Strategy 1: Extract date from source_file name patterns.
        # Common patterns in source filenames:
        #   "Fee_Schedule_20240304.pdf" -> YYYYMMDD
        #   "7.3G_Physician_Fee_Schedule_10-1-25.xlsx" -> M-D-YY
        #   "2025 Physician Fee Schedule.xlsx" -> YYYY (use Jan 1)
        #   "FY 25-26 CHP+ Fee Schedule.xlsx" -> FY prefix (use July 1 of first year)
        #   "01_CO_Fee Schedule_07012025.xlsx" -> MMDDYYYY embedded

        # Step 1a: YYYYMMDD pattern (e.g., "20240304" in filename)
        con.execute(f"""
            UPDATE _fact_medicaid_rate
            SET effective_date = TRY_CAST(
                SUBSTRING(regexp_extract(source_file, '(20\\d{{6}})', 1), 1, 4) || '-' ||
                SUBSTRING(regexp_extract(source_file, '(20\\d{{6}})', 1), 5, 2) || '-' ||
                SUBSTRING(regexp_extract(source_file, '(20\\d{{6}})', 1), 7, 2)
                AS DATE
            )
            WHERE effective_date IS NULL
              AND regexp_matches(source_file, '20\\d{{6}}')
        """)

        filled_1a = null_before - con.execute(
            "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
        ).fetchone()[0]
        if filled_1a > 0:
            print(f"    Step 1a (YYYYMMDD in filename): filled {filled_1a:,} rows")

        # Step 1b: 4-digit year in filename (e.g., "2025" or "2022")
        # Use January 1 of that year as a reasonable default
        remaining_null = con.execute(
            "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
        ).fetchone()[0]
        con.execute(f"""
            UPDATE _fact_medicaid_rate
            SET effective_date = TRY_CAST(
                regexp_extract(source_file, '(20[12]\\d)', 1) || '-01-01'
                AS DATE
            )
            WHERE effective_date IS NULL
              AND regexp_matches(source_file, '20[12]\\d')
        """)

        filled_1b = remaining_null - con.execute(
            "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
        ).fetchone()[0]
        if filled_1b > 0:
            print(f"    Step 1b (YYYY in filename):     filled {filled_1b:,} rows")

        # Step 2: Use snapshot_date as ultimate fallback for any remaining NULLs
        still_null = con.execute(
            "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
        ).fetchone()[0]
        if still_null > 0:
            con.execute(f"""
                UPDATE _fact_medicaid_rate
                SET effective_date = DATE '{SNAPSHOT_DATE}'
                WHERE effective_date IS NULL
            """)
            print(f"    Step 2 (snapshot_date fallback): filled {still_null:,} rows")

        null_after = con.execute(
            "SELECT COUNT(*) FROM _fact_medicaid_rate WHERE effective_date IS NULL"
        ).fetchone()[0]
        total_filled = null_before - null_after
        print(f"  effective_date backfill complete: {total_filled:,} of {null_before:,} NULL rows filled")
    # --- End backfill ---

    count = write_parquet(con, "_fact_medicaid_rate", "medicaid_rate", dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_medicaid_rate").fetchone()[0]
    print(f"  fact_medicaid_rate: {count:,} rows across {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_medicaid_rate")
    return count


def build_fact_medicare_rate(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_medicare_rate from CPRA DuckDB (locality-level)."""
    print("Building fact_medicare_rate...")

    # Export CPRA table to temp Parquet (can't query across DuckDB connections)
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    cpra = duckdb.connect(str(CPRA_DUCKDB), read_only=True)
    cpra.execute(f"COPY fact_medicare_rate TO '{tmp_path}' (FORMAT PARQUET)")
    cpra.close()

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_medicare_rate AS
        SELECT
            procedure_code,
            locality_id,
            state_code,
            mac_locality,
            locality_name,
            nonfac_rate,
            fac_rate,
            work_rvu,
            pe_rvu_nonfacility   AS pe_rvu_nonfac,
            pe_rvu_facility      AS pe_rvu_fac,
            mp_rvu,
            gpci_work,
            gpci_pe,
            gpci_mp,
            conversion_factor,
            2026                 AS pfs_year,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM '{tmp_path}'
    """)

    count = write_parquet(con, "_fact_medicare_rate", "medicare_rate", dry_run)
    print(f"  fact_medicare_rate: {count:,} rows (locality-level)")
    con.execute("DROP TABLE IF EXISTS _fact_medicare_rate")
    os.unlink(tmp_path)
    return count


def _export_cpra_table(cpra_table: str, tmp_dir: str) -> str:
    """Export a CPRA DuckDB table to a temp Parquet file."""
    tmp_path = os.path.join(tmp_dir, f"{cpra_table}.parquet")
    cpra = duckdb.connect(str(CPRA_DUCKDB), read_only=True)
    cpra.execute(f"COPY {cpra_table} TO '{tmp_path}' (FORMAT PARQUET)")
    cpra.close()
    return tmp_path


def build_fact_medicare_rate_state(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_medicare_rate_state (weighted state-level averages)."""
    print("Building fact_medicare_rate_state...")

    import tempfile
    tmp_dir = tempfile.mkdtemp()
    tmp_path = _export_cpra_table("fact_medicare_rate_state", tmp_dir)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_medicare_rate_state AS
        SELECT
            procedure_code,
            state_code,
            nonfac_rate,
            fac_rate,
            2026                   AS pfs_year,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM '{tmp_path}'
    """)

    count = write_parquet(con, "_fact_medicare_rate_state", "medicare_rate_state", dry_run)
    print(f"  fact_medicare_rate_state: {count:,} rows (state-level weighted averages)")
    con.execute("DROP TABLE IF EXISTS _fact_medicare_rate_state")
    os.unlink(tmp_path)
    os.rmdir(tmp_dir)
    return count


def build_fact_rate_comparison(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_rate_comparison from CPRA DuckDB, enriched with dim_procedure."""
    print("Building fact_rate_comparison...")

    import tempfile
    tmp_dir = tempfile.mkdtemp()
    tmp_path = _export_cpra_table("fact_rate_comparison", tmp_dir)

    dim_proc = DIM_DIR / "dim_procedure.parquet"

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_rate_comparison AS
        SELECT
            rc.state_code,
            rc.procedure_code,
            COALESCE(rc.modifier, '')     AS modifier,
            rc.medicaid_rate,
            rc.medicare_nonfac_rate,
            rc.medicare_fac_rate,
            rc.pct_of_medicare,
            -- Classification from shared dimension
            rc.em_category,
            dp.category,
            -- Rate metadata
            rc.rate_effective_date        AS medicaid_rate_date,
            2025                          AS comparison_year,
            -- Metadata
            DATE '{SNAPSHOT_DATE}'         AS snapshot_date,
            '{RUN_ID}'                     AS pipeline_run_id
        FROM '{tmp_path}' rc
        LEFT JOIN '{dim_proc}' dp ON rc.procedure_code = dp.procedure_code
    """)

    count = write_parquet(con, "_fact_rate_comparison", "rate_comparison", dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_rate_comparison").fetchone()[0]
    em_count = con.execute("SELECT COUNT(*) FROM _fact_rate_comparison WHERE em_category IS NOT NULL").fetchone()[0]
    print(f"  fact_rate_comparison: {count:,} rows across {states} states ({em_count:,} E/M)")
    con.execute("DROP TABLE IF EXISTS _fact_rate_comparison")
    os.unlink(tmp_path)
    os.rmdir(tmp_dir)
    return count


def build_fact_dq_flag(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_dq_flag from CPRA DuckDB flags."""
    print("Building fact_dq_flag...")

    import tempfile
    tmp_dir = tempfile.mkdtemp()
    tmp_path = _export_cpra_table("fact_dq_flags", tmp_dir)

    # Map flag types to severity levels
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_dq_flag AS
        SELECT
            state_code,
            CASE WHEN procedure_code = '*' THEN 'state' ELSE 'procedure' END AS entity_type,
            procedure_code                AS entity_id,
            flag                          AS flag_type,
            CASE
                WHEN flag IN ('HIGH_RATE', 'ZERO_RATE')              THEN 'error'
                WHEN flag IN ('BELOW_50PCT', 'ABOVE_MEDICARE', 'STALE_RATE', 'METHODOLOGY_RISK') THEN 'warning'
                WHEN flag IN ('MISSING_MEDICARE', 'MODIFIER_ONLY', 'LOW_COVERAGE', 'MANAGED_CARE_HIGH', 'FEE_SCHEDULE_STALE') THEN 'info'
                ELSE 'info'
            END                           AS severity,
            detail,
            'cpra'                        AS source_pipeline,
            DATE '{SNAPSHOT_DATE}'        AS snapshot_date,
            '{RUN_ID}'                    AS pipeline_run_id
        FROM '{tmp_path}'
    """)

    count = write_parquet(con, "_fact_dq_flag", "dq_flag", dry_run)
    # Flag distribution
    flags = con.execute("""
        SELECT flag_type, severity, COUNT(*) c
        FROM _fact_dq_flag
        GROUP BY flag_type, severity
        ORDER BY c DESC
    """).fetchall()
    for f in flags:
        print(f"    {f[0]:25s} [{f[1]:7s}] {f[2]:>7,}")
    print(f"  fact_dq_flag: {count:,} rows total")
    con.execute("DROP TABLE IF EXISTS _fact_dq_flag")
    os.unlink(tmp_path)
    os.rmdir(tmp_dir)
    return count


def build_fact_enrollment(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_enrollment from SQLite medicaid_enrollment."""
    print("Building fact_enrollment...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_enrollment AS
        SELECT
            state_code,
            year,
            month,
            total_enrollment,
            chip_enrollment,
            ffs_enrollment,
            managed_care_enrollment        AS mc_enrollment,
            source,
            DATE '{SNAPSHOT_DATE}'         AS snapshot_date
        FROM adb.medicaid_enrollment
        WHERE total_enrollment IS NOT NULL
    """)

    count = write_parquet(con, "_fact_enrollment", "enrollment", dry_run)
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_enrollment").fetchone()
    print(f"  fact_enrollment: {count:,} rows ({years[0]}-{years[1]})")
    con.execute("DROP TABLE IF EXISTS _fact_enrollment")
    return count


def build_fact_quality_measure(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_quality_measure from SQLite quality_measures."""
    print("Building fact_quality_measure...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_quality_measure AS
        SELECT
            state_code,
            measure_id,
            year,
            rate,
            numerator,
            denominator,
            measure_name,
            domain,
            source,
            DATE '{SNAPSHOT_DATE}'         AS snapshot_date
        FROM adb.quality_measures
    """)

    count = write_parquet(con, "_fact_quality_measure", "quality_measure", dry_run)
    measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_quality_measure").fetchone()[0]
    print(f"  fact_quality_measure: {count:,} rows ({measures} distinct measures)")
    con.execute("DROP TABLE IF EXISTS _fact_quality_measure")
    return count


def build_fact_expenditure(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build fact_expenditure from SQLite cms64_expenditures."""
    print("Building fact_expenditure...")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_expenditure AS
        SELECT
            state_code,
            fiscal_year,
            quarter,
            category,
            subcategory,
            federal_share,
            total_computable,
            source,
            DATE '{SNAPSHOT_DATE}'         AS snapshot_date
        FROM adb.cms64_expenditures
    """)

    count = write_parquet(con, "_fact_expenditure", "expenditure", dry_run)
    years = con.execute("SELECT MIN(fiscal_year), MAX(fiscal_year) FROM _fact_expenditure").fetchone()
    print(f"  fact_expenditure: {count:,} rows (FY{years[0]}-FY{years[1]})")
    con.execute("DROP TABLE IF EXISTS _fact_expenditure")
    return count


def write_manifest(totals: dict[str, int]):
    """Write a snapshot manifest with row counts and metadata."""
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "snapshot_date": SNAPSHOT_DATE,
        "pipeline_run_id": RUN_ID,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "sources": {
            "sqlite": str(SQLITE_DB),
            "cpra_duckdb": str(CPRA_DUCKDB),
        },
        "facts": {name: {"rows": count} for name, count in totals.items()},
        "total_rows": sum(totals.values()),
    }

    manifest_file = META_DIR / f"manifest_{SNAPSHOT_DATE}.json"
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  Manifest: {manifest_file}")


def main():
    parser = argparse.ArgumentParser(description="Build Aradune fact Parquet files")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without writing files")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of facts to build (default: all)")
    args = parser.parse_args()

    facts_to_build = ALL_FACTS
    if args.only:
        facts_to_build = [f.strip() for f in args.only.split(",")]
        invalid = [f for f in facts_to_build if f not in ALL_FACTS]
        if invalid:
            print(f"ERROR: Unknown facts: {invalid}. Valid: {ALL_FACTS}", file=sys.stderr)
            sys.exit(1)

    # Validate inputs
    if not SQLITE_DB.exists():
        print(f"ERROR: SQLite database not found at {SQLITE_DB}", file=sys.stderr)
        sys.exit(1)
    if not CPRA_DUCKDB.exists():
        print(f"ERROR: CPRA DuckDB not found at {CPRA_DUCKDB}", file=sys.stderr)
        sys.exit(1)
    if not DIM_DIR.exists():
        print(f"ERROR: Dimension directory not found at {DIM_DIR}", file=sys.stderr)
        print("  Run build_dimensions.py first.", file=sys.stderr)
        sys.exit(1)

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(facts_to_build)}")
    print()

    # Connect to in-memory DuckDB, attach SQLite
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{SQLITE_DB}' AS adb (TYPE sqlite, READ_ONLY)")

    # Build each requested fact
    builders = {
        "medicaid_rate": build_fact_medicaid_rate,
        "medicare_rate": build_fact_medicare_rate,
        "medicare_rate_state": build_fact_medicare_rate_state,
        "rate_comparison": build_fact_rate_comparison,
        "dq_flag": build_fact_dq_flag,
        "enrollment": build_fact_enrollment,
        "quality_measure": build_fact_quality_measure,
        "expenditure": build_fact_expenditure,
    }

    totals = {}
    for fact_name in facts_to_build:
        totals[fact_name] = builders[fact_name](con, args.dry_run)
        print()

    con.close()

    # Summary
    print("=" * 60)
    print("FACT BUILD COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>10,} rows")

    if not args.dry_run:
        write_manifest(totals)

        # List all output files
        print(f"\nOutput files under {FACT_DIR}/:")
        total_kb = 0
        for f in sorted(FACT_DIR.rglob("*.parquet")):
            rel = f.relative_to(FACT_DIR)
            size_kb = f.stat().st_size / 1024
            total_kb += size_kb
            print(f"  {str(rel):60s} {size_kb:>10.1f} KB")
        print(f"  {'TOTAL':60s} {total_kb:>10.1f} KB")

    print("\nDone.")


if __name__ == "__main__":
    main()
