#!/usr/bin/env python3
"""
build_dimensions.py — Build unified dimension Parquet files for the Aradune data lake.

Reads from:
  - SQLite: tools/mfs_scraper/aradune.db (states, procedure_codes, gpci_values, hcpcs_codes, fmap_rates, medicaid_enrollment)
  - DuckDB: tools/mfs_scraper/aradune_cpra.duckdb (dim_em_447_codes)

Writes to:
  - data/lake/dimension/dim_state.parquet
  - data/lake/dimension/dim_procedure.parquet
  - data/lake/dimension/dim_medicare_locality.parquet
  - data/lake/dimension/dim_provider_taxonomy.parquet
  - data/lake/dimension/dim_time.parquet

Usage:
  python3 scripts/build_dimensions.py
  python3 scripts/build_dimensions.py --dry-run   # print stats without writing
"""

import argparse
import os
import sys
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

# Correct Medicare conversion factor per CLAUDE.md (non-QPP CY2026)
MEDICARE_CF = 33.4009
# The SQLite procedure_codes table has $32.3465 (QPP-adjusted) — we override it.

REGIONS = {
    "CT": "Northeast", "ME": "Northeast", "MA": "Northeast", "NH": "Northeast",
    "RI": "Northeast", "VT": "Northeast", "NJ": "Northeast", "NY": "Northeast",
    "PA": "Northeast",
    "IL": "Midwest", "IN": "Midwest", "MI": "Midwest", "OH": "Midwest",
    "WI": "Midwest", "IA": "Midwest", "KS": "Midwest", "MN": "Midwest",
    "MO": "Midwest", "NE": "Midwest", "ND": "Midwest", "SD": "Midwest",
    "DE": "South", "FL": "South", "GA": "South", "MD": "South", "NC": "South",
    "SC": "South", "VA": "South", "DC": "South", "WV": "South", "AL": "South",
    "KY": "South", "MS": "South", "TN": "South", "AR": "South", "LA": "South",
    "OK": "South", "TX": "South",
    "AZ": "West", "CO": "West", "ID": "West", "MT": "West", "NV": "West",
    "NM": "West", "UT": "West", "WY": "West", "AK": "West", "CA": "West",
    "HI": "West", "OR": "West", "WA": "West",
}


def build_dim_state(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build dim_state from SQLite states + fmap_rates + enrollment."""
    print("Building dim_state...")

    con.execute("""
        CREATE OR REPLACE TABLE _dim_state AS
        SELECT
            s.state_code,
            s.state_name,
            s.methodology,
            s.conversion_factor,
            s.cf_effective_date,
            s.rvu_source,
            s.update_frequency,
            s.fee_schedule_url,
            s.pct_managed_care,
            s.ffs_relevance_note,
            s.has_upl_supplement::BOOLEAN   AS has_upl_supplement,
            s.has_directed_payments::BOOLEAN AS has_directed_payments,
            s.notes                         AS dq_notes,
            -- FMAP (latest fiscal year)
            f.fmap_rate                     AS fmap,
            f.efmap_rate                    AS efmap,
            -- Enrollment (latest year, max across months)
            e.total_enrollment,
            e.ffs_enrollment,
            e.mc_enrollment,
            -- Fee index
            fi.fee_index,
            -- Metadata
            CURRENT_DATE                    AS last_updated
        FROM adb.states s
        LEFT JOIN (
            SELECT state_code, fmap_rate, efmap_rate
            FROM adb.fmap_rates
            WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM adb.fmap_rates)
        ) f ON s.state_code = f.state_code
        LEFT JOIN (
            SELECT
                state_code,
                MAX(total_enrollment) AS total_enrollment,
                MAX(ffs_enrollment)   AS ffs_enrollment,
                MAX(managed_care_enrollment) AS mc_enrollment
            FROM adb.medicaid_enrollment
            WHERE year = (SELECT MAX(year) FROM adb.medicaid_enrollment)
            GROUP BY state_code
        ) e ON s.state_code = e.state_code
        LEFT JOIN adb.fee_index fi ON s.state_code = fi.state_code
    """)

    # Add region column (DuckDB doesn't have a nice CASE for 50 values, use a temp table)
    region_values = ", ".join(
        f"('{k}', '{v}')" for k, v in REGIONS.items()
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE _regions AS
        SELECT * FROM (VALUES {region_values}) AS t(state_code, region)
    """)
    con.execute("""
        CREATE OR REPLACE TABLE dim_state AS
        SELECT
            d.*,
            r.region
        FROM _dim_state d
        LEFT JOIN _regions r ON d.state_code = r.state_code
    """)
    con.execute("DROP TABLE IF EXISTS _dim_state")
    con.execute("DROP TABLE IF EXISTS _regions")

    count = con.execute("SELECT COUNT(*) FROM dim_state").fetchone()[0]
    print(f"  dim_state: {count} rows")

    if not dry_run:
        out = DIM_DIR / "dim_state.parquet"
        con.execute(f"COPY dim_state TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  Wrote {out}")

    return count


def build_dim_procedure(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build dim_procedure from SQLite procedure_codes + CPRA E/M codes + HCPCS descriptions."""
    print("Building dim_procedure...")

    # Load E/M category mapping from CPRA DuckDB
    cpra_con = duckdb.connect(str(CPRA_DUCKDB), read_only=True)
    em_codes = cpra_con.execute(
        "SELECT cpt_code, category, description FROM dim_em_447_codes"
    ).fetchall()
    cpra_con.close()

    # Create temp table for E/M mapping
    con.execute("CREATE OR REPLACE TABLE _em_codes (cpt_code VARCHAR, em_category VARCHAR, em_description VARCHAR)")
    con.executemany(
        "INSERT INTO _em_codes VALUES (?, ?, ?)",
        em_codes
    )

    # Merge HCPCS Level II descriptions with procedure_codes
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_procedure AS
        SELECT
            p.procedure_code,
            COALESCE(h.long_description, p.short_description) AS description,
            COALESCE(h.short_description, p.short_description) AS short_description,
            -- Classification
            p.category,
            p.subcategory,
            p.is_add_on::BOOLEAN           AS is_add_on,
            CASE WHEN em.cpt_code IS NOT NULL THEN TRUE ELSE FALSE END AS is_em_code,
            em.em_category,
            -- Medicare RVU components
            p.medicare_rvu_work            AS work_rvu,
            p.medicare_rvu_pe_fac          AS pe_rvu_facility,
            p.medicare_rvu_pe_nonfac       AS pe_rvu_nonfacility,
            p.medicare_rvu_mp              AS mp_rvu,
            -- Total RVUs
            COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_fac, 0) + COALESCE(p.medicare_rvu_mp, 0)
                                           AS total_rvu_facility,
            COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_nonfac, 0) + COALESCE(p.medicare_rvu_mp, 0)
                                           AS total_rvu_nonfac,
            -- Compute correct Medicare rates using $33.4009 (not the $32.3465 in SQLite)
            CASE WHEN (COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_nonfac, 0) + COALESCE(p.medicare_rvu_mp, 0)) > 0
                 THEN ROUND((COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_nonfac, 0) + COALESCE(p.medicare_rvu_mp, 0)) * {MEDICARE_CF}, 2)
                 ELSE 0.0
            END                            AS medicare_rate_nonfac,
            CASE WHEN (COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_fac, 0) + COALESCE(p.medicare_rvu_mp, 0)) > 0
                 THEN ROUND((COALESCE(p.medicare_rvu_work, 0) + COALESCE(p.medicare_rvu_pe_fac, 0) + COALESCE(p.medicare_rvu_mp, 0)) * {MEDICARE_CF}, 2)
                 ELSE 0.0
            END                            AS medicare_rate_fac,
            {MEDICARE_CF}                  AS conversion_factor,
            -- Code metadata from HCPCS codes table
            h.pricing_indicator,
            h.coverage_code,
            h.type_of_service,
            -- Metadata
            2026                           AS pfs_year,
            CURRENT_DATE                   AS last_updated
        FROM adb.procedure_codes p
        LEFT JOIN _em_codes em ON p.procedure_code = em.cpt_code
        LEFT JOIN adb.hcpcs_codes h ON p.procedure_code = h.hcpcs_code
    """)

    con.execute("DROP TABLE IF EXISTS _em_codes")

    count = con.execute("SELECT COUNT(*) FROM dim_procedure").fetchone()[0]
    em_count = con.execute("SELECT COUNT(*) FROM dim_procedure WHERE is_em_code").fetchone()[0]
    print(f"  dim_procedure: {count} rows ({em_count} E/M codes)")

    if not dry_run:
        out = DIM_DIR / "dim_procedure.parquet"
        con.execute(f"COPY dim_procedure TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  Wrote {out}")

    return count


def build_dim_medicare_locality(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build dim_medicare_locality from SQLite gpci_values with state weights."""
    print("Building dim_medicare_locality...")

    con.execute("""
        CREATE OR REPLACE TABLE dim_medicare_locality AS
        SELECT
            g.id                            AS locality_id,
            g.mac_locality,
            g.locality_name,
            g.state_code,
            g.gpci_work,
            g.gpci_pe,
            g.gpci_mp,
            -- Pre-compute weight for state-level aggregation
            1.0 / COUNT(*) OVER (PARTITION BY g.state_code) AS state_weight,
            g.year                          AS pfs_year,
            CURRENT_DATE                    AS last_updated
        FROM adb.gpci_values g
    """)

    count = con.execute("SELECT COUNT(*) FROM dim_medicare_locality").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM dim_medicare_locality").fetchone()[0]
    print(f"  dim_medicare_locality: {count} rows across {states} states")

    if not dry_run:
        out = DIM_DIR / "dim_medicare_locality.parquet"
        con.execute(f"COPY dim_medicare_locality TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  Wrote {out}")

    return count


def build_dim_time(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build dim_time covering 2014-2030 (T-MSIS era through planning horizon)."""
    print("Building dim_time...")

    con.execute("""
        CREATE OR REPLACE TABLE dim_time AS
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            d                               AS full_date,
            YEAR(d)                         AS year,
            QUARTER(d)                      AS quarter,
            MONTH(d)                        AS month,
            strftime(d, '%B')               AS month_name,
            -- Federal fiscal year: Oct-Sep, so FY2025 = Oct 2024 - Sep 2025
            CASE WHEN MONTH(d) >= 10 THEN YEAR(d) + 1 ELSE YEAR(d) END AS fiscal_year,
            CASE
                WHEN MONTH(d) IN (10,11,12) THEN 1
                WHEN MONTH(d) IN (1,2,3)    THEN 2
                WHEN MONTH(d) IN (4,5,6)    THEN 3
                ELSE 4
            END                             AS fiscal_quarter,
            DAY(d) = 1                      AS is_month_start,
            DAY(d) = 1 AND MONTH(d) IN (1,4,7,10) AS is_quarter_start,
            DAY(d) = 1 AND MONTH(d) = 10    AS is_fy_start
        FROM (
            SELECT UNNEST(generate_series(DATE '2014-01-01', DATE '2030-12-31', INTERVAL 1 DAY))::DATE AS d
        )
    """)

    count = con.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    print(f"  dim_time: {count} rows (2014-01-01 to 2030-12-31)")

    if not dry_run:
        out = DIM_DIR / "dim_time.parquet"
        con.execute(f"COPY dim_time TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  Wrote {out}")

    return count


def build_dim_provider_taxonomy(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Build dim_provider_taxonomy. Currently a stub — we populate from NPPES when available."""
    print("Building dim_provider_taxonomy...")

    # For now, extract distinct taxonomies from the NPPES data if it's been processed,
    # otherwise create a minimal taxonomy from the bootstrap_db.py category logic.
    # This is a placeholder that will be enriched when the T-MSIS pipeline runs.

    # Core taxonomy categories used in bootstrap_db.py and the R pipeline
    taxonomies = [
        ("207Q00000X", "Family Medicine", None, "physician", "Family Medicine"),
        ("207R00000X", "Internal Medicine", None, "physician", "Internal Medicine"),
        ("208D00000X", "General Practice", None, "physician", "General Practice"),
        ("363L00000X", "Nurse Practitioner", None, "non_physician", "Nurse Practitioner"),
        ("363A00000X", "Physician Assistant", None, "non_physician", "Physician Assistant"),
        ("261QM1300X", "Federally Qualified Health Center", None, "facility", "FQHC"),
        ("282N00000X", "General Acute Care Hospital", None, "facility", "Hospital"),
        ("251E00000X", "Home Health", None, "facility", "Home Health"),
        ("253Z00000X", "Nursing Facility", None, "facility", "Nursing Facility"),
        ("261QR0405X", "Rural Health Clinic", None, "facility", "Rural Health Clinic"),
    ]

    con.execute("""
        CREATE OR REPLACE TABLE dim_provider_taxonomy (
            taxonomy_code VARCHAR,
            classification VARCHAR,
            specialization VARCHAR,
            category VARCHAR,
            display_name VARCHAR,
            last_updated DATE
        )
    """)
    con.executemany(
        "INSERT INTO dim_provider_taxonomy VALUES (?, ?, ?, ?, ?, CURRENT_DATE)",
        taxonomies
    )

    count = con.execute("SELECT COUNT(*) FROM dim_provider_taxonomy").fetchone()[0]
    print(f"  dim_provider_taxonomy: {count} rows (stub — will be enriched from NPPES)")

    if not dry_run:
        out = DIM_DIR / "dim_provider_taxonomy.parquet"
        con.execute(f"COPY dim_provider_taxonomy TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f"  Wrote {out}")

    return count


def main():
    parser = argparse.ArgumentParser(description="Build Aradune dimension Parquet files")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without writing files")
    args = parser.parse_args()

    # Validate inputs exist
    if not SQLITE_DB.exists():
        print(f"ERROR: SQLite database not found at {SQLITE_DB}", file=sys.stderr)
        sys.exit(1)
    if not CPRA_DUCKDB.exists():
        print(f"ERROR: CPRA DuckDB not found at {CPRA_DUCKDB}", file=sys.stderr)
        sys.exit(1)

    # Create output directory
    if not args.dry_run:
        DIM_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Output directory: {DIM_DIR}")

    # Connect to in-memory DuckDB for building, attach SQLite as read-only source
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{SQLITE_DB}' AS adb (TYPE sqlite, READ_ONLY)")

    print(f"\nSources:")
    print(f"  SQLite: {SQLITE_DB}")
    print(f"  CPRA:   {CPRA_DUCKDB}")
    print(f"  Medicare CF: ${MEDICARE_CF} (non-QPP CY2026)")
    print()

    # Build each dimension
    totals = {}
    totals["dim_state"] = build_dim_state(con, args.dry_run)
    print()
    totals["dim_procedure"] = build_dim_procedure(con, args.dry_run)
    print()
    totals["dim_medicare_locality"] = build_dim_medicare_locality(con, args.dry_run)
    print()
    totals["dim_time"] = build_dim_time(con, args.dry_run)
    print()
    totals["dim_provider_taxonomy"] = build_dim_provider_taxonomy(con, args.dry_run)

    # Summary
    print("\n" + "=" * 60)
    print("DIMENSION BUILD COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>8,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total_rows:>8,} rows")

    if not args.dry_run:
        # Verify all files exist
        print(f"\nOutput files in {DIM_DIR}/:")
        for f in sorted(DIM_DIR.glob("*.parquet")):
            size_kb = f.stat().st_size / 1024
            print(f"  {f.name:40s} {size_kb:>8.1f} KB")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
