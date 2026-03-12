#!/usr/bin/env python3
"""
build_lake_mco_mlr.py — Ingest Medicaid Managed Care MLR Summary Reports.

Source: https://data.medicaid.gov (MLR Summary Reports)
Plan-specific Medical Loss Ratios for MCOs, PIHPs, PAHPs (42 CFR 438.74).

Tables built:
  fact_mco_mlr — Plan-level MLR data with numerator, denominator, member months,
                 adjusted MLR, and remittance amounts.

Usage:
  python3 scripts/build_lake_mco_mlr.py
"""

import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "mco_mlr"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

SOURCE_URL = "https://download.medicaid.gov/data/mlr-public-use-file-UPDATED12162025.csv"

# State name to code mapping for any that need it
STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Guam": "GU", "Virgin Islands": "VI",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
}


def _clean_money(s) -> float | None:
    """Parse dollar strings like '$2,254,490 ' to float."""
    if s is None or (isinstance(s, float) and s != s):
        return None
    s = str(s).strip().replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_number(s) -> int | None:
    """Parse number strings like '1,493' to int."""
    if s is None or (isinstance(s, float) and s != s):
        return None
    s = str(s).strip().replace(",", "").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _clean_pct(s) -> float | None:
    """Parse percentage strings to float."""
    if s is None or (isinstance(s, float) and s != s):
        return None
    s = str(s).strip().replace("%", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_mco_mlr():
    csv_path = RAW_DIR / "mlr_summary.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"MLR CSV not found: {csv_path}")

    print(f"\nMCO MLR Summary Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    con = duckdb.connect()

    # Read raw
    print("\nReading MLR CSV...")
    con.execute(f"""
        CREATE TABLE raw AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, header=true)
    """)
    raw_count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
    print(f"  {raw_count:,} raw records")

    # Pull into Python for cleaning
    import pandas as pd
    df = con.execute("SELECT * FROM raw").fetchdf()

    # Normalize state codes
    df["state_code"] = df["State"].map(lambda s: STATE_NAME_TO_CODE.get(s.strip(), s.strip()) if isinstance(s, str) else s)

    # Clean monetary and numeric fields
    df["mlr_numerator"] = df["MLR Numerator"].apply(_clean_money) if "MLR Numerator" in df.columns else None
    # Handle column name with leading space
    num_col = [c for c in df.columns if "numerator" in c.lower() and "mlr" in c.lower()]
    den_col = [c for c in df.columns if "denominator" in c.lower() and "mlr" in c.lower()]
    mm_col = [c for c in df.columns if "member month" in c.lower()]
    mlr_col = [c for c in df.columns if "adjusted mlr" in c.lower()]
    rem_col = [c for c in df.columns if "remittance" in c.lower()]

    df["mlr_numerator"] = df[num_col[0]].apply(_clean_money) if num_col else None
    df["mlr_denominator"] = df[den_col[0]].apply(_clean_money) if den_col else None
    df["member_months"] = df[mm_col[0]].apply(_clean_number) if mm_col else None
    df["adjusted_mlr"] = df[mlr_col[0]].apply(_clean_pct) if mlr_col else None
    df["remittance_amount"] = df[rem_col[0]].apply(_clean_money) if rem_col else None

    # Build clean table
    clean = pd.DataFrame({
        "state_code": df["state_code"],
        "program_name": df["Program Name"].str.strip() if "Program Name" in df.columns else None,
        "program_type": df["Program Type"].str.strip() if "Program Type" in df.columns else None,
        "eligibility_group": df["Eligibility Group"].str.strip() if "Eligibility Group" in df.columns else None,
        "plan_name": df["MCO, PIHP, or PAHP Name"].str.strip() if "MCO, PIHP, or PAHP Name" in df.columns else None,
        "report_year": pd.to_numeric(df["Report Year"].str.strip(), errors="coerce").astype("Int64") if "Report Year" in df.columns else None,
        "period_start": df["MLR Reporting Period Start Date"].str.strip() if "MLR Reporting Period Start Date" in df.columns else None,
        "period_end": df["MLR Reporting Period End Date"].str.strip() if "MLR Reporting Period End Date" in df.columns else None,
        "mlr_numerator": df["mlr_numerator"],
        "mlr_denominator": df["mlr_denominator"],
        "member_months": df["member_months"],
        "adjusted_mlr": df["adjusted_mlr"],
        "remittance_amount": df["remittance_amount"],
        "source": SOURCE_URL,
        "snapshot_date": SNAPSHOT_DATE,
    })

    con.execute("CREATE TABLE fact_mco_mlr AS SELECT * FROM clean")

    count = con.execute("SELECT COUNT(*) FROM fact_mco_mlr").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_mco_mlr").fetchone()[0]
    years = con.execute("SELECT MIN(report_year), MAX(report_year) FROM fact_mco_mlr").fetchone()
    has_mlr = con.execute("SELECT COUNT(*) FROM fact_mco_mlr WHERE adjusted_mlr IS NOT NULL").fetchone()[0]

    print(f"\n  {count:,} MLR records, {states} states, {years[0]}-{years[1]}")
    print(f"  {has_mlr:,} with reported adjusted MLR")

    # Summary stats
    print("\n  MLR by program type:")
    pt = con.execute("""
        SELECT program_type, COUNT(*) as n,
               ROUND(AVG(adjusted_mlr), 1) as avg_mlr,
               ROUND(SUM(mlr_numerator)/1e9, 2) as total_num_B
        FROM fact_mco_mlr
        WHERE adjusted_mlr IS NOT NULL
        GROUP BY program_type ORDER BY n DESC LIMIT 8
    """).fetchall()
    for row in pt:
        print(f"    {row[0]}: {row[1]:,} plans, avg MLR {row[2]}%, ${row[3]}B")

    out_path = FACT_DIR / "mco_mlr" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_mco_mlr", out_path)

    con.close()
    return row_count


def write_manifest(row_count: int):
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_mco_mlr.py",
        "source": SOURCE_URL,
        "tables": {
            "fact_mco_mlr": {
                "rows": row_count,
                "path": f"fact/mco_mlr/snapshot={SNAPSHOT_DATE}/data.parquet",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_mco_mlr_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    row_count = build_mco_mlr()
    write_manifest(row_count)
    print("\n" + "=" * 60)
    print("MCO MLR INGESTION COMPLETE")
    print(f"  fact_mco_mlr: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
