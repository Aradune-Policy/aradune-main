#!/usr/bin/env python3
"""
build_lake_leie.py — Ingest OIG List of Excluded Individuals/Entities (LEIE).

Source: https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv
Updated weekly. ~82K currently excluded providers/entities.

Tables built:
  fact_leie — Currently excluded individuals/entities with exclusion type,
              specialty, state, NPI, dates.

Usage:
  python3 scripts/build_lake_leie.py
"""

import csv
import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "leie"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

SOURCE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"

# Exclusion type descriptions per OIG
EXCL_TYPE_DESC = {
    "1128a1": "Conviction of program-related crimes",
    "1128a2": "Conviction relating to patient abuse or neglect",
    "1128a3": "Felony conviction relating to health care fraud",
    "1128a4": "Felony conviction relating to controlled substance",
    "1128b1": "Misdemeanor conviction relating to health care fraud",
    "1128b2": "Misdemeanor conviction relating to controlled substance",
    "1128b4": "License revocation or suspension",
    "1128b5": "Exclusion or suspension under federal or state program",
    "1128b6": "Claims for excessive charges or unnecessary services",
    "1128b7": "Fraud, kickbacks, and other prohibited activities",
    "1128b8": "Entities controlled by a sanctioned individual",
    "1128b9": "Failure to disclose required information",
    "1128b10": "Failure to supply claimed services",
    "1128b11": "Failure to supply payment information",
    "1128b12": "Failure to grant immediate access",
    "1128b13": "Failure to take corrective action",
    "1128b14": "Default on health education loan or scholarship obligations",
    "1128b15": "Individuals controlling a sanctioned entity",
    "1128b16": "Making false statements or misrepresentations",
}


def _parse_date(s: str) -> str | None:
    """Parse LEIE date format YYYYMMDD to ISO date, or None."""
    if not s or s == "00000000" or len(s) != 8:
        return None
    try:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    except Exception:
        return None


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    """Write a DuckDB table to ZSTD-compressed Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_leie():
    """Build fact_leie from OIG LEIE CSV."""
    csv_path = RAW_DIR / "UPDATED.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"LEIE CSV not found: {csv_path}")

    print(f"\nOIG LEIE Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    con = duckdb.connect()

    # Read raw CSV
    print("\nReading LEIE CSV...")
    con.execute(f"""
        CREATE TABLE raw AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
    """)
    raw_count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    print(f"  {raw_count:,} raw records")

    # Transform
    print("\nBuilding fact_leie...")
    con.execute("""
        CREATE TABLE fact_leie AS
        SELECT
            CASE WHEN TRIM(COALESCE(LASTNAME,'')) != '' THEN 'individual' ELSE 'entity' END AS entity_type,
            NULLIF(TRIM(COALESCE(LASTNAME,'')), '') AS last_name,
            NULLIF(TRIM(COALESCE(FIRSTNAME,'')), '') AS first_name,
            NULLIF(TRIM(COALESCE(MIDNAME,'')), '') AS middle_name,
            NULLIF(TRIM(COALESCE(BUSNAME,'')), '') AS business_name,
            NULLIF(TRIM(COALESCE(GENERAL,'')), '') AS general_type,
            NULLIF(TRIM(COALESCE(SPECIALTY,'')), '') AS specialty,
            NULLIF(TRIM(COALESCE(UPIN,'')), '') AS upin,
            CASE
                WHEN TRIM(COALESCE(NPI,'')) IN ('', '0000000000') THEN NULL
                ELSE TRIM(NPI)
            END AS npi,
            NULLIF(TRIM(COALESCE(ADDRESS,'')), '') AS address,
            NULLIF(TRIM(COALESCE(CITY,'')), '') AS city,
            NULLIF(TRIM(COALESCE(STATE,'')), '') AS state_code,
            NULLIF(TRIM(COALESCE(ZIP,'')), '') AS zip_code,
            LOWER(TRIM(COALESCE(EXCLTYPE,''))) AS exclusion_type,
            TRIM(COALESCE(EXCLDATE,'')) AS exclusion_date_raw,
            TRIM(COALESCE(REINDATE,'')) AS reinstatement_date_raw,
            TRIM(COALESCE(WAIVERDATE,'')) AS waiver_date_raw,
            NULLIF(TRIM(COALESCE(WVRSTATE,'')), '') AS waiver_state
        FROM raw
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_leie").fetchone()[0]
    individuals = con.execute(
        "SELECT COUNT(*) FROM fact_leie WHERE entity_type = 'individual'"
    ).fetchone()[0]
    entities = con.execute(
        "SELECT COUNT(*) FROM fact_leie WHERE entity_type = 'entity'"
    ).fetchone()[0]
    with_npi = con.execute(
        "SELECT COUNT(*) FROM fact_leie WHERE npi IS NOT NULL"
    ).fetchone()[0]
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM fact_leie WHERE state_code IS NOT NULL"
    ).fetchone()[0]

    print(f"  {count:,} total exclusions ({individuals:,} individuals, {entities:,} entities)")
    print(f"  {with_npi:,} with valid NPI, {states} states/territories")

    # Parse dates in Python (LEIE uses YYYYMMDD format)
    rows = con.execute("SELECT * FROM fact_leie").fetchdf()
    rows["exclusion_date"] = rows["exclusion_date_raw"].apply(_parse_date)
    rows["reinstatement_date"] = rows["reinstatement_date_raw"].apply(_parse_date)
    rows["waiver_date"] = rows["waiver_date_raw"].apply(_parse_date)
    rows = rows.drop(columns=["exclusion_date_raw", "reinstatement_date_raw", "waiver_date_raw"])

    # Add metadata columns
    rows["source"] = SOURCE_URL
    rows["snapshot_date"] = SNAPSHOT_DATE

    # Re-register in DuckDB
    con.execute("DROP TABLE fact_leie")
    con.execute("CREATE TABLE fact_leie AS SELECT * FROM rows")

    # Write parquet
    out_path = FACT_DIR / "leie" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_leie", out_path)

    # Top exclusion types
    print("\n  Top exclusion types:")
    top_types = con.execute("""
        SELECT exclusion_type, COUNT(*) as n
        FROM fact_leie GROUP BY exclusion_type ORDER BY n DESC LIMIT 5
    """).fetchall()
    for t, n in top_types:
        desc = EXCL_TYPE_DESC.get(t, "Unknown")
        print(f"    {t}: {n:,} — {desc}")

    con.close()
    return row_count


def write_manifest(row_count: int):
    """Write manifest JSON."""
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_leie.py",
        "source": SOURCE_URL,
        "tables": {
            "fact_leie": {
                "rows": row_count,
                "path": f"fact/leie/snapshot={SNAPSHOT_DATE}/data.parquet",
            }
        },
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_leie_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    row_count = build_leie()
    write_manifest(row_count)
    print("\n" + "=" * 60)
    print("LEIE INGESTION COMPLETE")
    print(f"  fact_leie: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
