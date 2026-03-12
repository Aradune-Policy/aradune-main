#!/usr/bin/env python3
"""
build_lake_fee_schedules.py — Ingest CMS non-physician fee schedules.

Sources (CMS, CY2026):
  - Clinical Laboratory Fee Schedule (CLFS) Q1 2026: ~2,100 test codes
  - Ambulance Fee Schedule (AFS) PUF 2026: ~1,300 locality × HCPCS entries
  - DMEPOS Fee Schedule Jan 2026: ~3,500 codes × 56 state jurisdictions

Tables built:
  fact_clfs       — Clinical lab test codes with national limit rates
  fact_ambulance  — Ambulance services by carrier/locality with GPCIs
  fact_dmepos     — DME/prosthetics/orthotics rates by state (long format)

Usage:
  python3 scripts/build_lake_fee_schedules.py
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

results = {}


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def build_clfs():
    """Clinical Laboratory Fee Schedule."""
    csv_path = RAW_DIR / "clfs" / "CLFS 2026 Q1V1.csv"
    if not csv_path.exists():
        print("  CLFS CSV not found, skipping")
        return 0

    print("\n--- CLFS (Clinical Laboratory Fee Schedule) ---")
    con = duckdb.connect()

    # Skip header rows (4 rows of title/copyright), data starts at row 5
    con.execute(f"""
        CREATE TABLE fact_clfs AS
        SELECT
            CAST(YEAR AS INTEGER) AS year,
            TRIM(HCPCS) AS hcpcs_code,
            NULLIF(TRIM(MOD), '') AS modifier,
            TRIM(EFF_DATE) AS effective_date,
            TRIM(INDICATOR) AS indicator,
            TRY_CAST(REPLACE(RATE, ',', '') AS DOUBLE) AS rate,
            TRIM(SHORTDESC) AS short_description,
            TRIM(LONGDESC) AS long_description,
            'https://www.cms.gov/medicare/payment/fee-schedules/clinical-laboratory' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
             skip=4, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != '' AND YEAR IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_clfs").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_clfs").fetchone()[0]
    avg_rate = con.execute("SELECT ROUND(AVG(rate), 2) FROM fact_clfs WHERE rate > 0").fetchone()[0]
    print(f"  {count:,} rows, {codes:,} unique codes, avg rate ${avg_rate}")

    out_path = FACT_DIR / "clfs" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_clfs", out_path)
    con.close()
    return row_count


def build_ambulance():
    """Ambulance Fee Schedule PUF."""
    txt_path = RAW_DIR / "ambulance" / "508-compliant-version-of-AFS2026_PUF_ext.txt"
    if not txt_path.exists():
        print("  Ambulance PUF not found, skipping")
        return 0

    print("\n--- Ambulance Fee Schedule PUF ---")
    con = duckdb.connect()

    con.execute(f"""
        CREATE TABLE fact_ambulance AS
        SELECT
            TRIM("CONTRACTOR/CARRIER") AS carrier,
            TRIM(LOCALITY) AS locality,
            TRIM(HCPCS) AS hcpcs_code,
            TRY_CAST(RVU AS DOUBLE) AS rvu,
            TRY_CAST(GPCI AS DOUBLE) AS gpci,
            TRY_CAST(REPLACE(REPLACE("BASE RATE", '$', ''), ',', '') AS DOUBLE) AS base_rate,
            TRY_CAST(REPLACE(REPLACE("URBAN BASE RATE / URBAN MILEAGE", '$', ''), ',', '') AS DOUBLE) AS urban_rate,
            TRY_CAST(REPLACE(REPLACE("RURAL BASE RATE / RURAL MILEAGE", '$', ''), ',', '') AS DOUBLE) AS rural_rate,
            TRY_CAST(REPLACE(REPLACE("RURAL BASE RATE / LOWEST QUARTILE", '$', ''), ',', '') AS DOUBLE) AS rural_lowest_quartile_rate,
            TRY_CAST(REPLACE(REPLACE("RURAL GROUND MILES 1-17*", '$', ''), ',', '') AS DOUBLE) AS rural_ground_miles_1_17,
            2026 AS pfs_year,
            'https://www.cms.gov/medicare/payment/fee-schedules/ambulance' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{txt_path}', all_varchar=true, ignore_errors=true)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_ambulance").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_ambulance").fetchone()[0]
    localities = con.execute("SELECT COUNT(DISTINCT carrier || '-' || locality) FROM fact_ambulance").fetchone()[0]
    print(f"  {count:,} rows, {codes:,} HCPCS codes, {localities} carrier-localities")

    out_path = FACT_DIR / "ambulance" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_ambulance", out_path)
    con.close()
    return row_count


def build_dmepos():
    """DMEPOS Fee Schedule — pivot from wide (state columns) to long format."""
    csv_path = RAW_DIR / "dmepos" / "DMEPOS26_JAN.csv"
    if not csv_path.exists():
        print("  DMEPOS CSV not found, skipping")
        return 0

    print("\n--- DMEPOS Fee Schedule ---")
    con = duckdb.connect()

    # Read with skip for header rows (6 rows), actual header at row 7
    con.execute(f"""
        CREATE TABLE raw_dmepos AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=6, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_dmepos").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_dmepos").fetchall()]
    print(f"  {raw_count:,} raw rows, {len(cols)} columns")

    # Find state columns (pattern: "XX (NR)" or "XX (R)")
    state_cols = [c for c in cols if "(NR)" in c or "(R)" in c]
    states = sorted(set(c.split(" ")[0] for c in state_cols))
    print(f"  {len(states)} state jurisdictions: {', '.join(states[:10])}...")

    # Unpivot to long format: one row per HCPCS × state × rural_flag
    unpivot_parts = []
    for st in states:
        nr_col = f'"{st} (NR)"'
        r_col = f'"{st} (R)"'
        # Non-rural
        if f"{st} (NR)" in cols:
            unpivot_parts.append(f"""
                SELECT HCPCS AS hcpcs_code, Mod AS modifier, Mod2 AS modifier_2,
                       JURIS AS jurisdiction, CATG AS category,
                       TRY_CAST(Ceiling AS DOUBLE) AS ceiling,
                       TRY_CAST(Floor AS DOUBLE) AS floor,
                       '{st}' AS state_code, false AS rural,
                       TRY_CAST({nr_col} AS DOUBLE) AS rate,
                       Description AS description
                FROM raw_dmepos WHERE TRY_CAST({nr_col} AS DOUBLE) IS NOT NULL AND TRY_CAST({nr_col} AS DOUBLE) > 0
            """)
        # Rural
        if f"{st} (R)" in cols:
            unpivot_parts.append(f"""
                SELECT HCPCS AS hcpcs_code, Mod AS modifier, Mod2 AS modifier_2,
                       JURIS AS jurisdiction, CATG AS category,
                       TRY_CAST(Ceiling AS DOUBLE) AS ceiling,
                       TRY_CAST(Floor AS DOUBLE) AS floor,
                       '{st}' AS state_code, true AS rural,
                       TRY_CAST({r_col} AS DOUBLE) AS rate,
                       Description AS description
                FROM raw_dmepos WHERE TRY_CAST({r_col} AS DOUBLE) IS NOT NULL AND TRY_CAST({r_col} AS DOUBLE) > 0
            """)

    union_sql = "\nUNION ALL\n".join(unpivot_parts)
    con.execute(f"""
        CREATE TABLE fact_dmepos AS
        SELECT *, 2026 AS fee_schedule_year,
            'https://www.cms.gov/medicare/payment/fee-schedules/dmepos' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_dmepos").fetchone()[0]
    st_count = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_dmepos").fetchone()[0]
    rural = con.execute("SELECT COUNT(*) FROM fact_dmepos WHERE rural").fetchone()[0]
    print(f"  {count:,} long-format rows, {codes:,} codes, {st_count} states")
    print(f"  {rural:,} rural rates, {count - rural:,} non-rural rates")

    out_path = FACT_DIR / "dmepos" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_dmepos", out_path)
    con.close()
    return row_count


def write_manifest():
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_fee_schedules.py",
        "tables": results,
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_fee_schedules_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


def main():
    print("=" * 60)
    print("CMS Non-Physician Fee Schedule Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    clfs_rows = build_clfs()
    results["fact_clfs"] = {"rows": clfs_rows, "path": f"fact/clfs/snapshot={SNAPSHOT_DATE}/data.parquet"}

    amb_rows = build_ambulance()
    results["fact_ambulance"] = {"rows": amb_rows, "path": f"fact/ambulance/snapshot={SNAPSHOT_DATE}/data.parquet"}

    dmepos_rows = build_dmepos()
    results["fact_dmepos"] = {"rows": dmepos_rows, "path": f"fact/dmepos/snapshot={SNAPSHOT_DATE}/data.parquet"}

    write_manifest()

    total = clfs_rows + amb_rows + dmepos_rows
    print("\n" + "=" * 60)
    print("FEE SCHEDULE INGESTION COMPLETE")
    print(f"  fact_clfs:      {clfs_rows:,} rows")
    print(f"  fact_ambulance: {amb_rows:,} rows")
    print(f"  fact_dmepos:    {dmepos_rows:,} rows")
    print(f"  TOTAL:          {total:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
