#!/usr/bin/env python3
"""
build_lake_open_payments.py — Ingest CMS Open Payments (General Payments).

Source: https://openpaymentsdata.cms.gov
Program Year 2024 General Payments. 15.4M individual payment records, 6.3GB raw CSV.
Aggregated to state × specialty × payment-nature level for the lake.

Tables built:
  fact_open_payments — State-level aggregates of industry payments to physicians.
                       Includes payment counts, total/avg amounts, unique physicians.

Usage:
  python3 scripts/build_lake_open_payments.py
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "open_payments"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

SOURCE_URL = "https://openpaymentsdata.cms.gov"
RAW_FILE = "OP_DTL_GNRL_PGYR2024.csv"


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_open_payments():
    csv_path = RAW_DIR / RAW_FILE
    if not csv_path.exists():
        raise FileNotFoundError(f"Open Payments CSV not found: {csv_path}")

    print(f"\nCMS Open Payments Ingestion (PGYR 2024)")
    print(f"  Source file: {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")
    print(f"  Snapshot: {SNAPSHOT_DATE}")

    con = duckdb.connect()

    # Aggregate directly from CSV — never load 15M rows into memory
    print("\n  Aggregating 15M+ records to state × specialty × payment type...")
    print("  (This may take a minute...)")

    con.execute(f"""
        CREATE TABLE fact_open_payments AS
        SELECT
            Recipient_State AS state_code,
            Covered_Recipient_Type AS recipient_type,
            Covered_Recipient_Specialty_1 AS specialty,
            Nature_Of_Payment_Or_Transfer_Of_Value AS payment_nature,
            CAST(Program_Year AS INTEGER) AS program_year,
            COUNT(*) AS payment_count,
            COUNT(DISTINCT Covered_Recipient_NPI) AS unique_physicians,
            COUNT(DISTINCT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) AS unique_companies,
            ROUND(SUM(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS total_amount,
            ROUND(AVG(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS avg_amount,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS median_amount,
            ROUND(MAX(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS max_amount,
            '{SOURCE_URL}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true, sample_size=20000)
        WHERE Recipient_State IS NOT NULL
          AND Recipient_State != ''
          AND LENGTH(TRIM(Recipient_State)) = 2
        GROUP BY
            Recipient_State,
            Covered_Recipient_Type,
            Covered_Recipient_Specialty_1,
            Nature_Of_Payment_Or_Transfer_Of_Value,
            Program_Year
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_open_payments").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_open_payments").fetchone()[0]
    total_amt = con.execute("SELECT ROUND(SUM(total_amount)/1e9, 2) FROM fact_open_payments").fetchone()[0]
    total_payments = con.execute("SELECT SUM(payment_count) FROM fact_open_payments").fetchone()[0]
    unique_docs = con.execute("SELECT SUM(unique_physicians) FROM fact_open_payments").fetchone()[0]

    print(f"\n  {count:,} aggregate rows")
    print(f"  {states} states, {total_payments:,} total payments, ${total_amt}B total")

    # Top payment types
    print("\n  Top payment types by total amount:")
    top = con.execute("""
        SELECT payment_nature,
               SUM(payment_count) as payments,
               ROUND(SUM(total_amount)/1e9, 2) as total_B
        FROM fact_open_payments
        GROUP BY payment_nature ORDER BY total_B DESC LIMIT 8
    """).fetchall()
    for row in top:
        print(f"    {row[0]}: {row[1]:,} payments, ${row[2]}B")

    # Top states
    print("\n  Top states by total amount:")
    top_states = con.execute("""
        SELECT state_code,
               SUM(payment_count) as payments,
               ROUND(SUM(total_amount)/1e6, 1) as total_M
        FROM fact_open_payments
        GROUP BY state_code ORDER BY total_M DESC LIMIT 10
    """).fetchall()
    for row in top_states:
        print(f"    {row[0]}: {row[1]:,} payments, ${row[2]}M")

    out_path = FACT_DIR / "open_payments" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_open_payments", out_path)

    con.close()
    return row_count


def write_manifest(row_count: int):
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_open_payments.py",
        "source": SOURCE_URL,
        "raw_file": RAW_FILE,
        "raw_rows": "~15.4M",
        "tables": {
            "fact_open_payments": {
                "rows": row_count,
                "path": f"fact/open_payments/snapshot={SNAPSHOT_DATE}/data.parquet",
                "note": "Aggregated from 15.4M raw records to state x specialty x payment type",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_open_payments_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    row_count = build_open_payments()
    write_manifest(row_count)
    print("\n" + "=" * 60)
    print("OPEN PAYMENTS INGESTION COMPLETE")
    print(f"  fact_open_payments: {row_count:,} rows")
    print(f"  (aggregated from ~15.4M raw payment records)")
    print("=" * 60)


if __name__ == "__main__":
    main()
