#!/usr/bin/env python3
"""
build_lake_open_payments.py — Ingest CMS Open Payments (All Categories).

Source: https://openpaymentsdata.cms.gov
Program Year 2024. Three payment categories:
  - General Payments (~12M records, ~$2.2B)
  - Research Payments (~650K records, ~$10.5B)
  - Ownership/Investment (~70K records, ~$0.3B)
Total: ~13M records, ~$13B

Aggregated to state x specialty x payment-nature level for the lake.

Tables built:
  fact_open_payments — State-level aggregates of industry payments to physicians
                       across all three CMS Open Payments categories.

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

# All three CMS Open Payments file types
RAW_FILES = {
    "general": "OP_DTL_GNRL_PGYR2024.csv",
    "research": "OP_DTL_RSRCH_PGYR2024.csv",
    "ownership": "OP_DTL_OWNRSHP_PGYR2024.csv",
}


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def _ingest_general(con: duckdb.DuckDBPyConnection, csv_path: Path):
    """Ingest General Payments (~12M records, ~$2.2B)."""
    print(f"\n  [General] {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")
    con.execute(f"""
        CREATE TABLE _general AS
        SELECT
            Recipient_State AS state_code,
            Covered_Recipient_Type AS recipient_type,
            Covered_Recipient_Specialty_1 AS specialty,
            Nature_Of_Payment_Or_Transfer_Of_Value AS payment_nature,
            'General' AS payment_category,
            CAST(Program_Year AS INTEGER) AS program_year,
            COUNT(*) AS payment_count,
            COUNT(DISTINCT Covered_Recipient_NPI) AS unique_physicians,
            COUNT(DISTINCT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) AS unique_companies,
            ROUND(SUM(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS total_amount,
            ROUND(AVG(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS avg_amount,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS median_amount,
            ROUND(MAX(CAST(Total_Amount_Of_Payment_USDollars AS DOUBLE)), 2) AS max_amount
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
    cnt = con.execute("SELECT COUNT(*) FROM _general").fetchone()[0]
    amt = con.execute("SELECT ROUND(SUM(total_amount)/1e9, 2) FROM _general").fetchone()[0]
    print(f"    {cnt:,} aggregate rows, ${amt}B total")


def _ingest_research(con: duckdb.DuckDBPyConnection, csv_path: Path):
    """Ingest Research Payments (~650K records, ~$10.5B)."""
    print(f"\n  [Research] {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")
    con.execute(f"""
        CREATE TABLE _research AS
        SELECT
            Recipient_State AS state_code,
            Covered_Recipient_Type AS recipient_type,
            Covered_Recipient_Specialty_1 AS specialty,
            Form_of_Payment_or_Transfer_of_Value AS payment_nature,
            'Research' AS payment_category,
            CAST(Program_Year AS INTEGER) AS program_year,
            COUNT(*) AS payment_count,
            COUNT(DISTINCT Covered_Recipient_NPI) AS unique_physicians,
            COUNT(DISTINCT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) AS unique_companies,
            ROUND(SUM(CAST(Total_Amount_of_Payment_USDollars AS DOUBLE)), 2) AS total_amount,
            ROUND(AVG(CAST(Total_Amount_of_Payment_USDollars AS DOUBLE)), 2) AS avg_amount,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(Total_Amount_of_Payment_USDollars AS DOUBLE)), 2) AS median_amount,
            ROUND(MAX(CAST(Total_Amount_of_Payment_USDollars AS DOUBLE)), 2) AS max_amount
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true, sample_size=20000)
        WHERE Recipient_State IS NOT NULL
          AND Recipient_State != ''
          AND LENGTH(TRIM(Recipient_State)) = 2
        GROUP BY
            Recipient_State,
            Covered_Recipient_Type,
            Covered_Recipient_Specialty_1,
            Form_of_Payment_or_Transfer_of_Value,
            Program_Year
    """)
    cnt = con.execute("SELECT COUNT(*) FROM _research").fetchone()[0]
    amt = con.execute("SELECT ROUND(SUM(total_amount)/1e9, 2) FROM _research").fetchone()[0]
    print(f"    {cnt:,} aggregate rows, ${amt}B total")


def _ingest_ownership(con: duckdb.DuckDBPyConnection, csv_path: Path):
    """Ingest Ownership/Investment (~70K records, ~$0.3B)."""
    print(f"\n  [Ownership] {csv_path.name} ({csv_path.stat().st_size / 1e9:.1f} GB)")
    con.execute(f"""
        CREATE TABLE _ownership AS
        SELECT
            Recipient_State AS state_code,
            'Physician Owner/Investor' AS recipient_type,
            Physician_Specialty AS specialty,
            'Ownership/Investment Interest' AS payment_nature,
            'Ownership' AS payment_category,
            CAST(Program_Year AS INTEGER) AS program_year,
            COUNT(*) AS payment_count,
            COUNT(DISTINCT Physician_NPI) AS unique_physicians,
            COUNT(DISTINCT Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name) AS unique_companies,
            ROUND(SUM(CAST(Total_Amount_Invested_USDollars AS DOUBLE)), 2) AS total_amount,
            ROUND(AVG(CAST(Total_Amount_Invested_USDollars AS DOUBLE)), 2) AS avg_amount,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(Total_Amount_Invested_USDollars AS DOUBLE)), 2) AS median_amount,
            ROUND(MAX(CAST(Total_Amount_Invested_USDollars AS DOUBLE)), 2) AS max_amount
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true, sample_size=20000)
        WHERE Recipient_State IS NOT NULL
          AND Recipient_State != ''
          AND LENGTH(TRIM(Recipient_State)) = 2
        GROUP BY
            Recipient_State,
            Physician_Specialty,
            Program_Year
    """)
    cnt = con.execute("SELECT COUNT(*) FROM _ownership").fetchone()[0]
    amt = con.execute("SELECT ROUND(SUM(total_amount)/1e9, 2) FROM _ownership").fetchone()[0]
    print(f"    {cnt:,} aggregate rows, ${amt}B total")


def build_open_payments():
    print(f"\nCMS Open Payments Ingestion (PGYR 2024) — All Categories")
    print(f"  Snapshot: {SNAPSHOT_DATE}")

    con = duckdb.connect()

    # Ingest each available category
    general_path = RAW_DIR / RAW_FILES["general"]
    research_path = RAW_DIR / RAW_FILES["research"]
    ownership_path = RAW_DIR / RAW_FILES["ownership"]

    categories_loaded = []

    if general_path.exists():
        _ingest_general(con, general_path)
        categories_loaded.append("general")
    else:
        print(f"\n  [General] SKIPPED — {general_path.name} not found")

    if research_path.exists():
        _ingest_research(con, research_path)
        categories_loaded.append("research")
    else:
        print(f"\n  [Research] SKIPPED — {research_path.name} not found")

    if ownership_path.exists():
        _ingest_ownership(con, ownership_path)
        categories_loaded.append("ownership")
    else:
        print(f"\n  [Ownership] SKIPPED — {ownership_path.name} not found")

    if not categories_loaded:
        raise FileNotFoundError("No Open Payments CSV files found in " + str(RAW_DIR))

    # UNION ALL available categories into fact_open_payments
    union_parts = []
    if "general" in categories_loaded:
        union_parts.append("SELECT * FROM _general")
    if "research" in categories_loaded:
        union_parts.append("SELECT * FROM _research")
    if "ownership" in categories_loaded:
        union_parts.append("SELECT * FROM _ownership")

    union_sql = " UNION ALL ".join(union_parts)
    con.execute(f"""
        CREATE TABLE fact_open_payments AS
        SELECT *, '{SOURCE_URL}' AS source, '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    # Cleanup temp tables
    for cat in categories_loaded:
        con.execute(f"DROP TABLE IF EXISTS _{cat}")

    count = con.execute("SELECT COUNT(*) FROM fact_open_payments").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_open_payments").fetchone()[0]
    total_amt = con.execute("SELECT ROUND(SUM(total_amount)/1e9, 2) FROM fact_open_payments").fetchone()[0]
    total_payments = con.execute("SELECT SUM(payment_count) FROM fact_open_payments").fetchone()[0]

    print(f"\n  COMBINED: {count:,} aggregate rows")
    print(f"  {states} states, {total_payments:,} total payments, ${total_amt}B total")
    print(f"  Categories: {', '.join(categories_loaded)}")

    # Top payment categories
    print("\n  By category:")
    cats = con.execute("""
        SELECT payment_category,
               SUM(payment_count) as payments,
               ROUND(SUM(total_amount)/1e9, 2) as total_B
        FROM fact_open_payments
        GROUP BY payment_category ORDER BY total_B DESC
    """).fetchall()
    for row in cats:
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
    return row_count, categories_loaded


def write_manifest(row_count: int, categories: list[str]):
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_open_payments.py",
        "source": SOURCE_URL,
        "raw_files": {k: v for k, v in RAW_FILES.items() if k in categories},
        "categories": categories,
        "tables": {
            "fact_open_payments": {
                "rows": row_count,
                "path": f"fact/open_payments/snapshot={SNAPSHOT_DATE}/data.parquet",
                "note": f"Aggregated from all payment categories: {', '.join(categories)}",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_open_payments_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    row_count, categories = build_open_payments()
    write_manifest(row_count, categories)
    print("\n" + "=" * 60)
    print("OPEN PAYMENTS INGESTION COMPLETE")
    print(f"  fact_open_payments: {row_count:,} rows")
    print(f"  Categories: {', '.join(categories)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
