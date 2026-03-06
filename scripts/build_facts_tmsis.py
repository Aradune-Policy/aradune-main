#!/usr/bin/env python3
"""
build_facts_tmsis.py — Ingest existing T-MSIS Parquet files into the data lake.

Reads from:
  - public/data/claims.parquet (712K rows, yearly aggregates)
  - public/data/claims_monthly.parquet (6.3M rows, monthly granularity)
  - public/data/categories.parquet (8K rows, state x category rollup)
  - public/data/providers.parquet (584K rows, provider-level)

Writes to:
  data/lake/fact/
    claims/snapshot=YYYY-MM-DD/data.parquet
    claims_monthly/snapshot=YYYY-MM-DD/data.parquet
    claims_categories/snapshot=YYYY-MM-DD/data.parquet
    provider/snapshot=YYYY-MM-DD/data.parquet

These files already exist in public/data/ from a previous R pipeline run.
This script normalizes column names to match the unified schema and copies
them into the lake with snapshot versioning.

Usage:
  python3 scripts/build_facts_tmsis.py
  python3 scripts/build_facts_tmsis.py --dry-run
"""

import argparse
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PUBLIC_DATA = PROJECT_ROOT / "public" / "data"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# Source files from previous R pipeline run
SOURCES = {
    "claims": PUBLIC_DATA / "claims.parquet",
    "claims_monthly": PUBLIC_DATA / "claims_monthly.parquet",
    "categories": PUBLIC_DATA / "categories.parquet",
    "providers": PUBLIC_DATA / "providers.parquet",
}


def write_parquet(con, table_name: str, fact_name: str, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_dir = FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "data.parquet"
        con.execute(f"COPY {table_name} TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_file.stat().st_size / 1024
        print(f"  Wrote {out_file} ({size_kb:.1f} KB)")
    return count


def build_fact_claims(con, dry_run: bool) -> int:
    """Normalize claims.parquet into lake schema."""
    print("Building fact_claims (yearly)...")
    src = SOURCES["claims"]
    if not src.exists():
        print(f"  SKIP: {src} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_claims AS
        SELECT
            state              AS state_code,
            hcpcs_code         AS procedure_code,
            category,
            year,
            NULL::INTEGER      AS month,
            total_paid,
            total_claims,
            total_beneficiaries,
            provider_count,
            CASE WHEN total_claims > 0
                 THEN ROUND(total_paid / total_claims, 2)
                 ELSE NULL
            END                AS avg_paid_per_claim,
            'FFS'              AS claim_type,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date,
            '{RUN_ID}'         AS pipeline_run_id
        FROM '{src}'
    """)

    count = write_parquet(con, "_fact_claims", "claims", dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_claims").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_claims").fetchone()
    print(f"  fact_claims: {count:,} rows, {states} states, {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_claims")
    return count


def build_fact_claims_monthly(con, dry_run: bool) -> int:
    """Normalize claims_monthly.parquet into lake schema."""
    print("Building fact_claims_monthly...")
    src = SOURCES["claims_monthly"]
    if not src.exists():
        print(f"  SKIP: {src} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_claims_monthly AS
        SELECT
            state              AS state_code,
            hcpcs_code         AS procedure_code,
            category,
            year,
            CASE WHEN claim_month IS NOT NULL
                 THEN TRY_CAST(SPLIT_PART(claim_month, '-', 2) AS INTEGER)
                 ELSE NULL
            END                AS month,
            claim_month,
            total_paid,
            total_claims,
            total_beneficiaries,
            provider_count,
            CASE WHEN total_claims > 0
                 THEN ROUND(total_paid / total_claims, 2)
                 ELSE NULL
            END                AS avg_paid_per_claim,
            'FFS'              AS claim_type,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date,
            '{RUN_ID}'         AS pipeline_run_id
        FROM '{src}'
    """)

    count = write_parquet(con, "_fact_claims_monthly", "claims_monthly", dry_run)
    print(f"  fact_claims_monthly: {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _fact_claims_monthly")
    return count


def build_fact_claims_categories(con, dry_run: bool) -> int:
    """Normalize categories.parquet into lake schema."""
    print("Building fact_claims_categories...")
    src = SOURCES["categories"]
    if not src.exists():
        print(f"  SKIP: {src} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_claims_categories AS
        SELECT
            state              AS state_code,
            category,
            year,
            total_paid,
            total_claims,
            total_beneficiaries,
            code_count,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date,
            '{RUN_ID}'         AS pipeline_run_id
        FROM '{src}'
    """)

    count = write_parquet(con, "_fact_claims_categories", "claims_categories", dry_run)
    cats = con.execute("SELECT COUNT(DISTINCT category) FROM _fact_claims_categories").fetchone()[0]
    print(f"  fact_claims_categories: {count:,} rows ({cats} categories)")
    con.execute("DROP TABLE IF EXISTS _fact_claims_categories")
    return count


def build_fact_provider(con, dry_run: bool) -> int:
    """Normalize providers.parquet into lake schema."""
    print("Building fact_provider...")
    src = SOURCES["providers"]
    if not src.exists():
        print(f"  SKIP: {src} not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_provider AS
        SELECT
            npi,
            state              AS state_code,
            provider_name,
            zip3,
            taxonomy           AS taxonomy_code,
            total_paid,
            total_claims,
            total_beneficiaries,
            code_count,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date,
            '{RUN_ID}'         AS pipeline_run_id
        FROM '{src}'
    """)

    count = write_parquet(con, "_fact_provider", "provider", dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_provider").fetchone()[0]
    print(f"  fact_provider: {count:,} rows, {states} states")
    con.execute("DROP TABLE IF EXISTS _fact_provider")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest T-MSIS Parquet files into data lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Check sources exist
    missing = [k for k, v in SOURCES.items() if not v.exists()]
    if missing:
        print(f"WARNING: Missing source files: {missing}")

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}
    totals["claims"] = build_fact_claims(con, args.dry_run)
    print()
    totals["claims_monthly"] = build_fact_claims_monthly(con, args.dry_run)
    print()
    totals["claims_categories"] = build_fact_claims_categories(con, args.dry_run)
    print()
    totals["provider"] = build_fact_provider(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("T-MSIS LAKE INGESTION COMPLETE")
    print("=" * 60)
    total = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:30s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':30s} {total:>10,} rows")

    if not args.dry_run:
        # Append to manifest
        manifest_file = META_DIR / f"manifest_{SNAPSHOT_DATE}.json"
        if manifest_file.exists():
            with open(manifest_file) as f:
                manifest = json.load(f)
        else:
            manifest = {"snapshot_date": SNAPSHOT_DATE, "facts": {}}

        for name, count in totals.items():
            manifest["facts"][f"tmsis_{name}"] = {"rows": count}
        manifest["total_rows"] = sum(v["rows"] for v in manifest["facts"].values())

        META_DIR.mkdir(parents=True, exist_ok=True)
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Updated manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
