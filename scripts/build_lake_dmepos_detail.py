#!/usr/bin/env python3
"""
build_lake_dmepos_detail.py — Ingest detailed DMEPOS fee schedule files into the data lake.

The existing fact_dmepos (222K rows) contains only the main DMEPOS fee schedule
unpivoted to long format. This script ingests ALL DMEPOS-related files as separate
fact tables for richer analysis:

  fact_dmepos_detail      — Main DMEPOS fee schedule (long format, ~222K rows)
  fact_dmepos_pen         — PEN (Parenteral & Enteral Nutrition) fee schedule (~2.7K rows)
  fact_dmepos_cba         — Former CBA (Competitive Bidding Area) fees (~80K rows)
  fact_dmepos_cba_mailorder — Former CBA National Mail-Order DTS fees (~14 rows)
  fact_dmepos_cba_zipcodes  — Former CBA ZIP code mapping (~16K rows)
  fact_dmepos_rural_zipcodes — DME Rural ZIP codes (~15.9K rows)

Source: CMS DMEPOS Fee Schedule, January 2026
        https://www.cms.gov/medicare/payment/fee-schedules/dmepos

Also ingests ambulance geographic area data:
  fact_ambulance_geographic — Ambulance Fee Schedule geographic areas (~109 rows)

Source: CMS Ambulance Fee Schedule, CY 2026
        https://www.cms.gov/medicare/payment/fee-schedules/ambulance

Usage:
  python3 scripts/build_lake_dmepos_detail.py
  python3 scripts/build_lake_dmepos_detail.py --dry-run
"""

import argparse
import json
import re
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

DMEPOS_RAW = RAW_DIR / "dmepos"
AMBULANCE_RAW = RAW_DIR / "ambulance"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())
SOURCE_DMEPOS = "https://www.cms.gov/medicare/payment/fee-schedules/dmepos"
SOURCE_AMBULANCE = "https://www.cms.gov/medicare/payment/fee-schedules/ambulance"

results = {}


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to Parquet with ZSTD compression."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if count == 0:
        print(f"  [skip] {table_name} — 0 rows")
        return 0
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
        )
        size_mb = out_path.stat().st_size / 1_048_576
        print(f"  -> {out_path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    else:
        print(f"  [dry-run] {table_name} ({count:,} rows)")
    return count


def snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def print_columns(con, table_name: str):
    """Print column names and types for a table."""
    cols = con.execute(f"DESCRIBE {table_name}").fetchall()
    for name, dtype, *_ in cols:
        print(f"    {name}: {dtype}")


# ---------------------------------------------------------------------------
# 1. DMEPOS Main Fee Schedule (wide -> long)
# ---------------------------------------------------------------------------

def build_dmepos_detail(con, dry_run: bool) -> int:
    """Main DMEPOS fee schedule — unpivot wide state columns to long format."""
    csv_path = DMEPOS_RAW / "DMEPOS26_JAN.csv"
    if not csv_path.exists():
        print("  DMEPOS CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_detail (Main DMEPOS Fee Schedule) ---")

    # Read with skip=6 for header rows, actual header at row 7
    con.execute(f"""
        CREATE TABLE raw_dmepos AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=6, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_dmepos").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_dmepos").fetchall()]
    print(f"  Raw: {raw_count:,} rows, {len(cols)} columns")

    # Identify state columns: pattern "XX (NR)" or "XX (R)"
    state_cols = [c for c in cols if "(NR)" in c or "(R)" in c]
    states = sorted(set(c.split(" ")[0].split("(")[0] for c in state_cols))
    print(f"  States: {len(states)} jurisdictions: {', '.join(states[:10])}...")

    # Unpivot to long format
    unpivot_parts = []
    for st in states:
        nr_col = f"{st} (NR)" if f"{st} (NR)" in cols else f"{st}(NR)" if f"{st}(NR)" in cols else None
        r_col = f"{st} (R)" if f"{st} (R)" in cols else f"{st}(R)" if f"{st}(R)" in cols else None

        if nr_col and nr_col in cols:
            unpivot_parts.append(f"""
                SELECT TRIM(HCPCS) AS hcpcs_code,
                       NULLIF(TRIM(Mod), '') AS modifier,
                       NULLIF(TRIM(Mod2), '') AS modifier_2,
                       TRIM(JURIS) AS jurisdiction,
                       TRIM(CATG) AS category,
                       TRY_CAST(REPLACE(Ceiling, ',', '') AS DOUBLE) AS ceiling,
                       TRY_CAST(REPLACE(Floor, ',', '') AS DOUBLE) AS floor,
                       '{st}' AS state_code,
                       false AS rural,
                       TRY_CAST(REPLACE("{nr_col}", ',', '') AS DOUBLE) AS rate,
                       TRIM(Description) AS description
                FROM raw_dmepos
                WHERE TRY_CAST(REPLACE("{nr_col}", ',', '') AS DOUBLE) IS NOT NULL
            """)

        if r_col and r_col in cols:
            unpivot_parts.append(f"""
                SELECT TRIM(HCPCS) AS hcpcs_code,
                       NULLIF(TRIM(Mod), '') AS modifier,
                       NULLIF(TRIM(Mod2), '') AS modifier_2,
                       TRIM(JURIS) AS jurisdiction,
                       TRIM(CATG) AS category,
                       TRY_CAST(REPLACE(Ceiling, ',', '') AS DOUBLE) AS ceiling,
                       TRY_CAST(REPLACE(Floor, ',', '') AS DOUBLE) AS floor,
                       '{st}' AS state_code,
                       true AS rural,
                       TRY_CAST(REPLACE("{r_col}", ',', '') AS DOUBLE) AS rate,
                       TRIM(Description) AS description
                FROM raw_dmepos
                WHERE TRY_CAST(REPLACE("{r_col}", ',', '') AS DOUBLE) IS NOT NULL
            """)

    union_sql = "\nUNION ALL\n".join(unpivot_parts)
    con.execute(f"""
        CREATE TABLE fact_dmepos_detail AS
        SELECT *,
            'dmepos' AS fee_schedule_type,
            2026 AS fee_schedule_year,
            'January' AS fee_schedule_quarter,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_detail").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_dmepos_detail").fetchone()[0]
    st_count = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_dmepos_detail").fetchone()[0]
    rural = con.execute("SELECT COUNT(*) FROM fact_dmepos_detail WHERE rural").fetchone()[0]
    print(f"  Long format: {count:,} rows, {codes:,} codes, {st_count} states")
    print(f"  Rural: {rural:,} | Non-rural: {count - rural:,}")
    print_columns(con, "fact_dmepos_detail")

    out_path = snapshot_path("dmepos_detail")
    row_count = write_parquet(con, "fact_dmepos_detail", out_path, dry_run)
    con.execute("DROP TABLE raw_dmepos")
    return row_count


# ---------------------------------------------------------------------------
# 2. PEN Fee Schedule (wide -> long)
# ---------------------------------------------------------------------------

def build_dmepos_pen(con, dry_run: bool) -> int:
    """PEN (Parenteral & Enteral Nutrition) fee schedule — unpivot wide to long."""
    csv_path = DMEPOS_RAW / "DMEPEN26_JAN.csv"
    if not csv_path.exists():
        print("  PEN CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_pen (PEN Fee Schedule) ---")

    # PEN has 4 header rows, data header at row 5
    con.execute(f"""
        CREATE TABLE raw_pen AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=4, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_pen").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_pen").fetchall()]
    print(f"  Raw: {raw_count:,} rows, {len(cols)} columns")

    # PEN columns: HCPCS, MOD, MOD2, then state(NR)/state(R) pairs, then Desc
    state_cols = [c for c in cols if "(NR)" in c or "(R)" in c]
    states = sorted(set(re.sub(r'\(.*\)', '', c).strip() for c in state_cols))
    print(f"  States: {len(states)} jurisdictions")

    unpivot_parts = []
    for st in states:
        nr_col = f"{st}(NR)" if f"{st}(NR)" in cols else f"{st} (NR)" if f"{st} (NR)" in cols else None
        r_col = f"{st}(R)" if f"{st}(R)" in cols else f"{st} (R)" if f"{st} (R)" in cols else None

        if nr_col and nr_col in cols:
            unpivot_parts.append(f"""
                SELECT TRIM(HCPCS) AS hcpcs_code,
                       NULLIF(TRIM(MOD), '') AS modifier,
                       NULLIF(TRIM(MOD2), '') AS modifier_2,
                       '{st}' AS state_code,
                       false AS rural,
                       TRY_CAST(REPLACE("{nr_col}", ',', '') AS DOUBLE) AS rate,
                       TRIM("Desc") AS description
                FROM raw_pen
                WHERE TRY_CAST(REPLACE("{nr_col}", ',', '') AS DOUBLE) IS NOT NULL
            """)

        if r_col and r_col in cols:
            unpivot_parts.append(f"""
                SELECT TRIM(HCPCS) AS hcpcs_code,
                       NULLIF(TRIM(MOD), '') AS modifier,
                       NULLIF(TRIM(MOD2), '') AS modifier_2,
                       '{st}' AS state_code,
                       true AS rural,
                       TRY_CAST(REPLACE("{r_col}", ',', '') AS DOUBLE) AS rate,
                       TRIM("Desc") AS description
                FROM raw_pen
                WHERE TRY_CAST(REPLACE("{r_col}", ',', '') AS DOUBLE) IS NOT NULL
            """)

    union_sql = "\nUNION ALL\n".join(unpivot_parts)
    con.execute(f"""
        CREATE TABLE fact_dmepos_pen AS
        SELECT *,
            'pen' AS fee_schedule_type,
            2026 AS fee_schedule_year,
            'January' AS fee_schedule_quarter,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_pen").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_dmepos_pen").fetchone()[0]
    st_count = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_dmepos_pen").fetchone()[0]
    print(f"  Long format: {count:,} rows, {codes:,} codes, {st_count} states")
    print_columns(con, "fact_dmepos_pen")

    out_path = snapshot_path("dmepos_pen")
    row_count = write_parquet(con, "fact_dmepos_pen", out_path, dry_run)
    con.execute("DROP TABLE raw_pen")
    return row_count


# ---------------------------------------------------------------------------
# 3. Former CBA Fee Schedule (wide -> long by CBA area)
# ---------------------------------------------------------------------------

def build_dmepos_cba(con, dry_run: bool) -> int:
    """Former CBA (Competitive Bidding Area) fee schedule — unpivot by CBA area."""
    csv_path = DMEPOS_RAW / "Former CBA Fee schedule File- JAN2026.csv"
    if not csv_path.exists():
        print("  Former CBA Fee CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_cba (Former CBA Fee Schedule) ---")

    # 6 header rows, actual header at row 7
    con.execute(f"""
        CREATE TABLE raw_cba AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=6, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_cba").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_cba").fetchall()]
    print(f"  Raw: {raw_count:,} rows, {len(cols)} columns")

    # CBA columns: HCPCS, Mod, Mod2, Mod3, CATG, then CBA area names, then Description
    meta_cols = {"HCPCS", "Mod", "Mod2", "Mod3", "CATG", "Description"}
    cba_areas = [c for c in cols if c not in meta_cols]
    print(f"  CBA areas: {len(cba_areas)}")

    # Unpivot each CBA area
    unpivot_parts = []
    for area in cba_areas:
        safe_area = area.replace("'", "''")
        unpivot_parts.append(f"""
            SELECT TRIM(HCPCS) AS hcpcs_code,
                   NULLIF(TRIM(Mod), '') AS modifier,
                   NULLIF(TRIM(Mod2), '') AS modifier_2,
                   NULLIF(TRIM(Mod3), '') AS modifier_3,
                   TRIM(CATG) AS category,
                   '{safe_area}' AS cba_area,
                   TRY_CAST(REPLACE("{area}", ',', '') AS DOUBLE) AS rate,
                   TRIM(Description) AS description
            FROM raw_cba
            WHERE TRY_CAST(REPLACE("{area}", ',', '') AS DOUBLE) IS NOT NULL
        """)

    union_sql = "\nUNION ALL\n".join(unpivot_parts)
    con.execute(f"""
        CREATE TABLE fact_dmepos_cba AS
        SELECT *,
            'cba' AS fee_schedule_type,
            2026 AS fee_schedule_year,
            'January' AS fee_schedule_quarter,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM ({union_sql})
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_cba").fetchone()[0]
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM fact_dmepos_cba").fetchone()[0]
    areas = con.execute("SELECT COUNT(DISTINCT cba_area) FROM fact_dmepos_cba").fetchone()[0]
    print(f"  Long format: {count:,} rows, {codes:,} codes, {areas} CBA areas")
    print_columns(con, "fact_dmepos_cba")

    out_path = snapshot_path("dmepos_cba")
    row_count = write_parquet(con, "fact_dmepos_cba", out_path, dry_run)
    con.execute("DROP TABLE raw_cba")
    return row_count


# ---------------------------------------------------------------------------
# 4. Former CBA National Mail-Order DTS Fee Schedule
# ---------------------------------------------------------------------------

def build_dmepos_cba_mailorder(con, dry_run: bool) -> int:
    """Former CBA National Mail-Order Diabetic Testing Supply fee schedule."""
    csv_path = DMEPOS_RAW / "Former CBA National Mail-Order DTS Fee Schedule- JAN2026.csv"
    if not csv_path.exists():
        print("  CBA Mail-Order DTS CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_cba_mailorder (CBA Mail-Order DTS) ---")

    # 5 header rows, header at row 6
    con.execute(f"""
        CREATE TABLE raw_cba_mo AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=5, header=true, sample_size=5000)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
          AND HCPCS NOT LIKE 'Note%'
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_cba_mo").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_cba_mo").fetchall()]
    print(f"  Raw: {raw_count:,} rows, {len(cols)} columns: {cols}")

    # Determine the rate column name (could be "National Mail-Order" or similar)
    rate_col = [c for c in cols if c not in {"HCPCS", "Mod", "Mod2", "Mod3", "CATG", "Description"}]
    rate_col_name = rate_col[0] if rate_col else "National Mail-Order"

    con.execute(f"""
        CREATE TABLE fact_dmepos_cba_mailorder AS
        SELECT
            TRIM(HCPCS) AS hcpcs_code,
            NULLIF(TRIM(Mod), '') AS modifier,
            NULLIF(TRIM(Mod2), '') AS modifier_2,
            NULLIF(TRIM(Mod3), '') AS modifier_3,
            TRIM(CATG) AS category,
            TRY_CAST(REPLACE("{rate_col_name}", ',', '') AS DOUBLE) AS rate,
            TRIM(Description) AS description,
            'cba_mailorder' AS fee_schedule_type,
            2026 AS fee_schedule_year,
            'January' AS fee_schedule_quarter,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM raw_cba_mo
        WHERE TRY_CAST(REPLACE("{rate_col_name}", ',', '') AS DOUBLE) IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_cba_mailorder").fetchone()[0]
    print(f"  {count:,} rows")
    print_columns(con, "fact_dmepos_cba_mailorder")

    out_path = snapshot_path("dmepos_cba_mailorder")
    row_count = write_parquet(con, "fact_dmepos_cba_mailorder", out_path, dry_run)
    con.execute("DROP TABLE raw_cba_mo")
    return row_count


# ---------------------------------------------------------------------------
# 5. Former CBA ZIP Code File
# ---------------------------------------------------------------------------

def build_dmepos_cba_zipcodes(con, dry_run: bool) -> int:
    """Former CBA ZIP code-to-area mapping."""
    csv_path = DMEPOS_RAW / "Former CBA ZIP Code File- JAN2026.csv"
    if not csv_path.exists():
        print("  CBA ZIP Code CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_cba_zipcodes (CBA ZIP Code Mapping) ---")

    # 5 header rows, header at row 6
    con.execute(f"""
        CREATE TABLE raw_cba_zip AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            skip=5, header=true, sample_size=5000)
        WHERE "CBA State" IS NOT NULL AND TRIM("CBA State") != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_cba_zip").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_cba_zip").fetchall()]
    print(f"  Raw: {raw_count:,} rows, columns: {cols}")

    con.execute(f"""
        CREATE TABLE fact_dmepos_cba_zipcodes AS
        SELECT
            TRIM("CBA State") AS state_code,
            TRIM("CBA ZIP Code") AS zip_code,
            TRIM("CBA Name Short") AS cba_name_short,
            TRIM("CBA Name") AS cba_name,
            TRIM("Year/Qtr") AS year_quarter,
            'cba_zipcodes' AS fee_schedule_type,
            2026 AS fee_schedule_year,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM raw_cba_zip
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_cba_zipcodes").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_dmepos_cba_zipcodes").fetchone()[0]
    areas = con.execute("SELECT COUNT(DISTINCT cba_name_short) FROM fact_dmepos_cba_zipcodes").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {areas} CBA areas")
    print_columns(con, "fact_dmepos_cba_zipcodes")

    out_path = snapshot_path("dmepos_cba_zipcodes")
    row_count = write_parquet(con, "fact_dmepos_cba_zipcodes", out_path, dry_run)
    con.execute("DROP TABLE raw_cba_zip")
    return row_count


# ---------------------------------------------------------------------------
# 6. DME Rural ZIP Codes
# ---------------------------------------------------------------------------

def build_dmepos_rural_zipcodes(con, dry_run: bool) -> int:
    """DME Rural ZIP code list by state and quarter."""
    csv_path = DMEPOS_RAW / "DME Rural ZIP Code Quarter 1 2026.csv"
    if not csv_path.exists():
        print("  Rural ZIP Code CSV not found, skipping")
        return 0

    print("\n--- fact_dmepos_rural_zipcodes (Rural ZIP Codes) ---")

    con.execute(f"""
        CREATE TABLE raw_rural_zip AS
        SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true, ignore_errors=true,
            header=true, sample_size=5000)
        WHERE STATE IS NOT NULL AND TRIM(STATE) != ''
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_rural_zip").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_rural_zip").fetchall()]
    print(f"  Raw: {raw_count:,} rows, columns: {cols}")

    con.execute(f"""
        CREATE TABLE fact_dmepos_rural_zipcodes AS
        SELECT
            TRIM(STATE) AS state_code,
            TRIM("DMEPOS RURAL ZIP CODE") AS zip_code,
            TRIM("YEAR/QTR") AS year_quarter,
            2026 AS fee_schedule_year,
            1 AS quarter,
            '{SOURCE_DMEPOS}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM raw_rural_zip
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_dmepos_rural_zipcodes").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_dmepos_rural_zipcodes").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    print_columns(con, "fact_dmepos_rural_zipcodes")

    out_path = snapshot_path("dmepos_rural_zipcodes")
    row_count = write_parquet(con, "fact_dmepos_rural_zipcodes", out_path, dry_run)
    con.execute("DROP TABLE raw_rural_zip")
    return row_count


# ---------------------------------------------------------------------------
# 7. Ambulance Geographic Areas
# ---------------------------------------------------------------------------

def build_ambulance_geographic(con, dry_run: bool) -> int:
    """Ambulance Fee Schedule geographic area mapping (MAC -> state -> locality)."""
    xlsx_path = AMBULANCE_RAW / "Geographic_Area_2026.xlsx"
    if not xlsx_path.exists():
        print("  Ambulance Geographic Area Excel not found, skipping")
        return 0

    print("\n--- fact_ambulance_geographic (Ambulance Geographic Areas) ---")

    # st_read may use generic field names; use DuckDB's excel reader instead
    con.execute("INSTALL excel; LOAD excel;")
    con.execute(f"""
        CREATE TABLE raw_amb_geo AS
        SELECT * FROM read_xlsx('{xlsx_path}', header=true)
    """)

    raw_count = con.execute("SELECT COUNT(*) FROM raw_amb_geo").fetchone()[0]
    cols = [r[0] for r in con.execute("DESCRIBE raw_amb_geo").fetchall()]
    print(f"  Raw: {raw_count:,} rows, columns: {cols}")

    # Determine actual column names (may vary depending on reader)
    # Expected: MAC code, State, Locality Name
    mac_col = cols[0]
    state_col = cols[1]
    locality_col = cols[2]

    con.execute(f"""
        CREATE TABLE fact_ambulance_geographic AS
        SELECT
            TRIM(CAST("{mac_col}" AS VARCHAR)) AS mac_code,
            TRIM(CAST("{state_col}" AS VARCHAR)) AS state_code,
            TRIM(CAST("{locality_col}" AS VARCHAR)) AS locality_name,
            2026 AS fee_schedule_year,
            '{SOURCE_AMBULANCE}' AS source,
            '{SNAPSHOT_DATE}' AS snapshot_date
        FROM raw_amb_geo
        WHERE "{state_col}" IS NOT NULL AND TRIM(CAST("{state_col}" AS VARCHAR)) != ''
    """)

    count = con.execute("SELECT COUNT(*) FROM fact_ambulance_geographic").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_ambulance_geographic").fetchone()[0]
    print(f"  {count:,} rows, {states} states")
    print_columns(con, "fact_ambulance_geographic")

    out_path = snapshot_path("ambulance_geographic")
    row_count = write_parquet(con, "fact_ambulance_geographic", out_path, dry_run)
    con.execute("DROP TABLE raw_amb_geo")
    return row_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def write_manifest():
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_dmepos_detail.py",
        "tables": results,
        "completed_at": datetime.now().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_dmepos_detail_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path.relative_to(PROJECT_ROOT)}")


def main():
    parser = argparse.ArgumentParser(description="Ingest DMEPOS detail + ambulance geographic data")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing files")
    args = parser.parse_args()

    print("=" * 65)
    print("DMEPOS Detail + Ambulance Geographic Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print(f"  Mode:     {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("=" * 65)

    con = duckdb.connect()
    # Load spatial extension for Excel reading
    con.execute("INSTALL spatial; LOAD spatial;")

    # DMEPOS tables
    r1 = build_dmepos_detail(con, args.dry_run)
    results["fact_dmepos_detail"] = {"rows": r1, "path": str(snapshot_path("dmepos_detail").relative_to(LAKE_DIR))}

    r2 = build_dmepos_pen(con, args.dry_run)
    results["fact_dmepos_pen"] = {"rows": r2, "path": str(snapshot_path("dmepos_pen").relative_to(LAKE_DIR))}

    r3 = build_dmepos_cba(con, args.dry_run)
    results["fact_dmepos_cba"] = {"rows": r3, "path": str(snapshot_path("dmepos_cba").relative_to(LAKE_DIR))}

    r4 = build_dmepos_cba_mailorder(con, args.dry_run)
    results["fact_dmepos_cba_mailorder"] = {"rows": r4, "path": str(snapshot_path("dmepos_cba_mailorder").relative_to(LAKE_DIR))}

    r5 = build_dmepos_cba_zipcodes(con, args.dry_run)
    results["fact_dmepos_cba_zipcodes"] = {"rows": r5, "path": str(snapshot_path("dmepos_cba_zipcodes").relative_to(LAKE_DIR))}

    r6 = build_dmepos_rural_zipcodes(con, args.dry_run)
    results["fact_dmepos_rural_zipcodes"] = {"rows": r6, "path": str(snapshot_path("dmepos_rural_zipcodes").relative_to(LAKE_DIR))}

    # Ambulance
    r7 = build_ambulance_geographic(con, args.dry_run)
    results["fact_ambulance_geographic"] = {"rows": r7, "path": str(snapshot_path("ambulance_geographic").relative_to(LAKE_DIR))}

    con.close()

    if not args.dry_run:
        write_manifest()

    # Summary
    total = r1 + r2 + r3 + r4 + r5 + r6 + r7
    print("\n" + "=" * 65)
    print("INGESTION COMPLETE")
    print(f"  fact_dmepos_detail:         {r1:>10,} rows")
    print(f"  fact_dmepos_pen:            {r2:>10,} rows")
    print(f"  fact_dmepos_cba:            {r3:>10,} rows")
    print(f"  fact_dmepos_cba_mailorder:  {r4:>10,} rows")
    print(f"  fact_dmepos_cba_zipcodes:   {r5:>10,} rows")
    print(f"  fact_dmepos_rural_zipcodes: {r6:>10,} rows")
    print(f"  fact_ambulance_geographic:  {r7:>10,} rows")
    print(f"  TOTAL:                      {total:>10,} rows")
    print("=" * 65)


if __name__ == "__main__":
    main()
