#!/usr/bin/env python3
"""
build_lake_doge_spending.py — Build aggregated lake tables from HHS DOGE Medicaid
Provider Spending dataset (190M rows).

Source: data/raw/medicaid-provider-spending.duckdb (16 GB, 190,657,008 rows)
        Originally from HHS DOGE Medicaid Provider Spending release.
        Covers 2018-01 through 2024-12, all 50 states + DC + territories.

Tables built:
  fact_doge_state_hcpcs     — State x HCPCS x year x category aggregates
  fact_doge_state_taxonomy  — State x taxonomy x year aggregates
  fact_doge_state_monthly   — State x month x category time series
  fact_doge_state_category  — State x category x year high-level summary
  fact_doge_top_providers   — Top providers by state (billing NPI, >$100K total paid)

Usage:
  python3 scripts/build_lake_doge_spending.py
  python3 scripts/build_lake_doge_spending.py --dry-run
  python3 scripts/build_lake_doge_spending.py --only fact_doge_state_hcpcs,fact_doge_state_monthly
"""

import argparse
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DB = PROJECT_ROOT / "data" / "raw" / "medicaid-provider-spending.duckdb"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

SOURCE_URL = "https://doge.gov/savings/medicaid-provider-spending"


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    """Write a DuckDB table to Parquet with ZSTD compression. Returns row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_state_hcpcs(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """State x HCPCS x year x category aggregates."""
    print("\nBuilding fact_doge_state_hcpcs...")
    con.execute("""
        CREATE OR REPLACE TABLE _doge_state_hcpcs AS
        SELECT
            state,
            HCPCS_CODE AS hcpcs_code,
            SUBSTRING(CLAIM_FROM_MONTH, 1, 4) AS year,
            category,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_PAID) AS total_paid,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            COUNT(DISTINCT SERVICING_PROVIDER_NPI_NUM) AS provider_count
        FROM spending
        GROUP BY 1, 2, 3, 4
    """)
    count = con.execute("SELECT COUNT(*) FROM _doge_state_hcpcs").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] fact_doge_state_hcpcs ({count:,} rows)")
        return count
    return write_parquet(con, "_doge_state_hcpcs", _snapshot_path("doge_state_hcpcs"))


def build_state_taxonomy(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """State x taxonomy x year aggregates."""
    print("\nBuilding fact_doge_state_taxonomy...")
    con.execute("""
        CREATE OR REPLACE TABLE _doge_state_taxonomy AS
        SELECT
            state,
            taxonomy,
            SUBSTRING(CLAIM_FROM_MONTH, 1, 4) AS year,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_PAID) AS total_paid,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            COUNT(DISTINCT SERVICING_PROVIDER_NPI_NUM) AS provider_count
        FROM spending
        GROUP BY 1, 2, 3
    """)
    count = con.execute("SELECT COUNT(*) FROM _doge_state_taxonomy").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] fact_doge_state_taxonomy ({count:,} rows)")
        return count
    return write_parquet(con, "_doge_state_taxonomy", _snapshot_path("doge_state_taxonomy"))


def build_state_monthly(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """State x month x category time series."""
    print("\nBuilding fact_doge_state_monthly...")
    con.execute("""
        CREATE OR REPLACE TABLE _doge_state_monthly AS
        SELECT
            state,
            CLAIM_FROM_MONTH AS month,
            category,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_PAID) AS total_paid,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            COUNT(DISTINCT SERVICING_PROVIDER_NPI_NUM) AS provider_count
        FROM spending
        GROUP BY 1, 2, 3
    """)
    count = con.execute("SELECT COUNT(*) FROM _doge_state_monthly").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] fact_doge_state_monthly ({count:,} rows)")
        return count
    return write_parquet(con, "_doge_state_monthly", _snapshot_path("doge_state_monthly"))


def build_state_category(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """State x category x year high-level summary."""
    print("\nBuilding fact_doge_state_category...")
    con.execute("""
        CREATE OR REPLACE TABLE _doge_state_category AS
        SELECT
            state,
            category,
            SUBSTRING(CLAIM_FROM_MONTH, 1, 4) AS year,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_PAID) AS total_paid,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            COUNT(DISTINCT BILLING_PROVIDER_NPI_NUM) AS billing_provider_count,
            COUNT(DISTINCT SERVICING_PROVIDER_NPI_NUM) AS servicing_provider_count
        FROM spending
        GROUP BY 1, 2, 3
    """)
    count = con.execute("SELECT COUNT(*) FROM _doge_state_category").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] fact_doge_state_category ({count:,} rows)")
        return count
    return write_parquet(con, "_doge_state_category", _snapshot_path("doge_state_category"))


def build_top_providers(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Top providers by state (billing NPI level, >$100K total paid)."""
    print("\nBuilding fact_doge_top_providers...")
    con.execute("""
        CREATE OR REPLACE TABLE _doge_top_providers AS
        SELECT
            state,
            BILLING_PROVIDER_NPI_NUM AS billing_npi,
            provider_name,
            taxonomy,
            SUM(TOTAL_CLAIMS) AS total_claims,
            SUM(TOTAL_PAID) AS total_paid,
            SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
            COUNT(DISTINCT HCPCS_CODE) AS code_count,
            MIN(CLAIM_FROM_MONTH) AS first_month,
            MAX(CLAIM_FROM_MONTH) AS last_month
        FROM spending
        GROUP BY 1, 2, 3, 4
        HAVING SUM(TOTAL_PAID) > 100000
    """)
    count = con.execute("SELECT COUNT(*) FROM _doge_top_providers").fetchone()[0]
    if dry_run:
        print(f"  [dry-run] fact_doge_top_providers ({count:,} rows)")
        return count
    return write_parquet(con, "_doge_top_providers", _snapshot_path("doge_top_providers"))


# ── Table registry ────────────────────────────────────────────────────

BUILDERS = {
    "fact_doge_state_hcpcs":    build_state_hcpcs,
    "fact_doge_state_taxonomy": build_state_taxonomy,
    "fact_doge_state_monthly":  build_state_monthly,
    "fact_doge_state_category": build_state_category,
    "fact_doge_top_providers":  build_top_providers,
}


def main():
    parser = argparse.ArgumentParser(description="Build DOGE spending lake tables")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only, don't write Parquet")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build (e.g. fact_doge_state_hcpcs,fact_doge_state_monthly)")
    args = parser.parse_args()

    if not SOURCE_DB.exists():
        raise FileNotFoundError(f"Source DuckDB not found: {SOURCE_DB}")

    print(f"HHS DOGE Medicaid Provider Spending — Lake Ingestion")
    print(f"  Source: {SOURCE_DB.name} ({SOURCE_DB.stat().st_size / 1e9:.1f} GB)")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID: {RUN_ID}")

    # Connect in-memory and attach source as read-only
    con = duckdb.connect()
    con.execute(f"ATTACH '{SOURCE_DB}' AS src (READ_ONLY)")
    # Create a view so all queries can reference 'spending' directly
    con.execute("CREATE VIEW spending AS SELECT * FROM src.spending")

    selected = set()
    if args.only:
        selected = {t.strip() for t in args.only.split(",")}
        # Validate
        for t in selected:
            if t not in BUILDERS:
                raise ValueError(f"Unknown table: {t}. Options: {', '.join(BUILDERS.keys())}")

    results = {}
    start = datetime.now()

    for name, builder in BUILDERS.items():
        if selected and name not in selected:
            continue
        t0 = datetime.now()
        count = builder(con, args.dry_run)
        elapsed = (datetime.now() - t0).total_seconds()
        results[name] = {"rows": count, "seconds": round(elapsed, 1)}
        print(f"  ({elapsed:.1f}s)")

    total_elapsed = (datetime.now() - start).total_seconds()

    # Write manifest
    if not args.dry_run:
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "source": str(SOURCE_DB.name),
            "source_url": SOURCE_URL,
            "source_rows": 190_657_008,
            "tables": results,
            "total_seconds": round(total_elapsed, 1),
            "created_at": datetime.now().isoformat(),
        }
        manifest_path = META_DIR / "doge_spending_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_path.relative_to(PROJECT_ROOT)}")

    # Summary
    print(f"\n{'='*60}")
    print(f"  DOGE Spending Lake Build Complete")
    print(f"  Total time: {total_elapsed:.1f}s")
    total_rows = sum(r["rows"] for r in results.values())
    print(f"  Total rows: {total_rows:,}")
    for name, info in results.items():
        print(f"    {name}: {info['rows']:,} rows ({info['seconds']}s)")
    print(f"{'='*60}")

    con.close()


if __name__ == "__main__":
    main()
