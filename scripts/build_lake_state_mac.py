#!/usr/bin/env python3
"""
build_lake_state_mac.py -- Ingest state Medicaid drug pricing / MAC lists.

Reads from:
  data/raw/ny_medicaid_reimbursable_drugs.csv  (37K entries, comma-delimited)
  data/raw/tx_medicaid_formulary_drug.txt      (17K entries, pipe-delimited)

Writes to:
  data/lake/fact/state_mac_ny/data.parquet
  data/lake/fact/state_mac_tx/data.parquet

State MAC (Maximum Allowable Cost) / formulary files show what each state
pays for specific drugs, complementing federal NADAC and AMP data.

Usage:
  python3 scripts/build_lake_state_mac.py
  python3 scripts/build_lake_state_mac.py --dry-run
"""

import argparse
import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())


def to_snake_case(name) -> str:
    name = str(name).strip().lstrip("\ufeff")
    s = re.sub(r"[\s\-/]+", "_", name)
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def write_parquet(con, table: str, path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if not dry_run and count > 0:
        path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    elif dry_run:
        print(f"  [dry-run] ({count:,} rows)")
    return count


def build_ny(con, dry_run: bool) -> int:
    """Ingest NY Medicaid Reimbursable Drug List."""
    print("Building fact_state_mac_ny...")

    raw_path = RAW_DIR / "ny_medicaid_reimbursable_drugs.csv"
    if not raw_path.exists():
        print("  SKIPPED -- NY drug file not found")
        return 0

    print(f"  Source: {raw_path.name} ({raw_path.stat().st_size / (1024*1024):.1f} MB)")

    # Read with pandas (DuckDB has header detection issues with spaces in column names)
    print("  Reading CSV with pandas...")
    df = pd.read_csv(raw_path, encoding="utf-8-sig", low_memory=False)
    print(f"  Raw rows: {len(df):,}, columns: {len(df.columns)}")
    print(f"  Columns: {list(df.columns)}")

    # Rename columns to snake_case
    col_map = {col: to_snake_case(col) for col in df.columns}
    df.rename(columns=col_map, inplace=True)

    # Add metadata
    df["state_code"] = "NY"
    df["source_system"] = "NY Medicaid Reimbursable Drug List"
    df["snapshot_date"] = SNAPSHOT_DATE

    # Register in DuckDB
    con.execute("CREATE TABLE fact_state_mac_ny AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM fact_state_mac_ny").fetchone()[0]
    cols = con.execute("PRAGMA table_info('fact_state_mac_ny')").fetchall()
    print(f"  Cleaned: {count:,} rows, {len(cols)} columns")

    # Summary
    types = con.execute("SELECT type, COUNT(*) n FROM fact_state_mac_ny GROUP BY 1 ORDER BY n DESC").fetchall()
    print("\n  Drug types:")
    for t, n in types:
        print(f"    {t}: {n:,}")

    # Sample
    print("\n  Sample (5 rows):")
    sample = con.execute("""
        SELECT ndc, description, generic_name, mra_cost, type
        FROM fact_state_mac_ny
        WHERE mra_cost IS NOT NULL
        ORDER BY mra_cost DESC
        LIMIT 5
    """).fetchall()
    for row in sample:
        print(f"    NDC={row[0]} | {row[1][:40]:40s} | ${row[3]:>10.2f} | {row[4]}")

    out_path = FACT_DIR / "state_mac_ny" / "data.parquet"
    row_count = write_parquet(con, "fact_state_mac_ny", out_path, dry_run)
    con.execute("DROP TABLE IF EXISTS _ny_raw")
    return row_count


def build_tx(con, dry_run: bool) -> int:
    """Ingest TX Medicaid Formulary Drug List (pipe-delimited)."""
    print("\nBuilding fact_state_mac_tx...")

    raw_path = RAW_DIR / "tx_medicaid_formulary_drug.txt"
    if not raw_path.exists():
        print("  SKIPPED -- TX drug file not found")
        return 0

    print(f"  Source: {raw_path.name} ({raw_path.stat().st_size / (1024*1024):.1f} MB)")

    # Read with pandas (pipe-delimited)
    print("  Reading pipe-delimited file with pandas...")
    df = pd.read_csv(raw_path, sep="|", encoding="utf-8-sig", low_memory=False)
    print(f"  Raw rows: {len(df):,}, columns: {len(df.columns)}")
    print(f"  Columns (first 20): {list(df.columns)[:20]}")

    # Rename columns to snake_case
    col_map = {col: to_snake_case(col) for col in df.columns}
    df.rename(columns=col_map, inplace=True)

    # Add metadata
    df["state_code"] = "TX"
    df["source_system"] = "TX Medicaid Formulary"
    df["snapshot_date"] = SNAPSHOT_DATE

    # Register in DuckDB — cast drug_ndc to VARCHAR and pad to 11 digits
    con.execute("""
        CREATE TABLE fact_state_mac_tx AS
        SELECT * REPLACE (
            LPAD(CAST(drug_ndc AS VARCHAR), 11, '0') AS drug_ndc
        )
        FROM df
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_state_mac_tx").fetchone()[0]
    cols = con.execute("PRAGMA table_info('fact_state_mac_tx')").fetchall()
    print(f"  Cleaned: {count:,} rows, {len(cols)} columns")

    # Summary
    print("\n  Column names (snake_case):")
    for c in cols[:15]:
        print(f"    {c[1]}: {c[2]}")

    # Sample
    print("\n  Sample (5 rows):")
    sample = con.execute("""
        SELECT drug_ndc, drug_descr, drug_generic, drug_retail, drug_manufacturer
        FROM fact_state_mac_tx
        LIMIT 5
    """).fetchall()
    for row in sample:
        print(f"    NDC={row[0]} | {str(row[1])[:40]:40s} | ${row[3] if row[3] else 'N/A':>10} | {row[4]}")

    out_path = FACT_DIR / "state_mac_tx" / "data.parquet"
    row_count = write_parquet(con, "fact_state_mac_tx", out_path, dry_run)
    con.execute("DROP TABLE IF EXISTS _tx_raw")
    return row_count


def main():
    parser = argparse.ArgumentParser(description="Ingest state MAC drug pricing data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("State MAC Drug Pricing Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}

    totals["fact_state_mac_ny"] = build_ny(con, args.dry_run)
    totals["fact_state_mac_tx"] = build_tx(con, args.dry_run)

    con.close()

    # Manifest
    if not args.dry_run and sum(totals.values()) > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_id": RUN_ID,
            "snapshot_date": SNAPSHOT_DATE,
            "script": "build_lake_state_mac.py",
            "sources": [
                "https://www.health.ny.gov/health_care/medicaid/program/pharmacy.htm",
                "https://www.txvendordrug.com/formulary",
            ],
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "completed_at": datetime.now().isoformat() + "Z",
        }
        manifest_path = META_DIR / f"manifest_state_mac_{SNAPSHOT_DATE}.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        print(f"\n  Manifest: {manifest_path.name}")

    print("\n" + "=" * 60)
    print("STATE MAC DRUG PRICING INGESTION COMPLETE")
    for name, count in totals.items():
        print(f"  {name:35s} {count:>10,} rows")
    print(f"  {'TOTAL':35s} {sum(totals.values()):>10,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
