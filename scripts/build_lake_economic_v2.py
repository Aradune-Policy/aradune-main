#!/usr/bin/env python3
"""
build_lake_economic_v2.py — Ingest additional economic/contextual data into the lake.

Downloads and processes:
  1. BEA SAINC1  — State personal income, population, per capita income (1929-2024)
  2. BEA SAINC4  — Personal income components (wages, transfers, proprietors, etc.)
  3. BEA SAINC35 — Transfer receipts by type (Medicaid, Medicare, SSI, SNAP, etc.)
  4. HUD SAFMR   — Small Area Fair Market Rents by ZIP code (FY2025)

Tables built:
  fact_bea_personal_income    — State-level personal income, per capita, population
  fact_bea_income_components  — Personal income decomposed: wages, supplements, transfers, etc.
  fact_bea_transfer_receipts  — Government transfer payments: Medicaid, Medicare, SSI, SNAP, UI
  fact_safmr_zip              — HUD Small Area Fair Market Rents by ZIP (0-4BR)

Usage:
  python3 scripts/build_lake_economic_v2.py
  python3 scripts/build_lake_economic_v2.py --dry-run
  python3 scripts/build_lake_economic_v2.py --only fact_bea_personal_income,fact_safmr_zip
"""

import argparse
import csv
import io
import json
import os
import subprocess
import tempfile
import uuid
import zipfile
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BEA_DIR = RAW_DIR / "bea"
HUD_DIR = RAW_DIR / "hud"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# FIPS code -> state code mapping (50 states + DC + territories)
FIPS_TO_STATE = {
    "01000": "AL", "02000": "AK", "04000": "AZ", "05000": "AR", "06000": "CA",
    "08000": "CO", "09000": "CT", "10000": "DE", "11000": "DC", "12000": "FL",
    "13000": "GA", "15000": "HI", "16000": "ID", "17000": "IL", "18000": "IN",
    "19000": "IA", "20000": "KS", "21000": "KY", "22000": "LA", "23000": "ME",
    "24000": "MD", "25000": "MA", "26000": "MI", "27000": "MN", "28000": "MS",
    "29000": "MO", "30000": "MT", "31000": "NE", "32000": "NV", "33000": "NH",
    "34000": "NJ", "35000": "NM", "36000": "NY", "37000": "NC", "38000": "ND",
    "39000": "OH", "40000": "OK", "41000": "OR", "42000": "PA", "44000": "RI",
    "45000": "SC", "46000": "SD", "47000": "TN", "48000": "TX", "49000": "UT",
    "50000": "VT", "51000": "VA", "53000": "WA", "54000": "WV", "55000": "WI",
    "56000": "WY",
    # Territories
    "60000": "AS", "66000": "GU", "69000": "MP", "72000": "PR", "78000": "VI",
}

# Reverse for looking up state names from BEA GeoName
STATE_NAME_TO_CODE = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "Puerto Rico": "PR",
}


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    """Write a DuckDB table to Parquet with ZSTD compression."""
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _ensure_bea_downloaded():
    """Download BEA SAINC ZIP if not already present."""
    zip_path = BEA_DIR / "SAINC.zip"
    if zip_path.exists():
        print(f"  BEA SAINC.zip already downloaded ({zip_path.stat().st_size / 1e6:.1f} MB)")
        return True

    BEA_DIR.mkdir(parents=True, exist_ok=True)
    url = "https://apps.bea.gov/regional/zip/SAINC.zip"
    print(f"  Downloading BEA SAINC data from {url}...")
    result = subprocess.run(
        ["curl", "-sL", "-o", str(zip_path), url],
        capture_output=True, text=True, timeout=120
    )
    if result.returncode != 0 or not zip_path.exists() or zip_path.stat().st_size < 1000:
        print(f"  ERROR downloading BEA data: {result.stderr}")
        if zip_path.exists():
            zip_path.unlink()
        return False

    print(f"  Downloaded {zip_path.stat().st_size / 1e6:.1f} MB")
    return True


def _extract_bea_csv(filename: str) -> Path:
    """Extract a specific CSV from the BEA SAINC ZIP."""
    zip_path = BEA_DIR / "SAINC.zip"
    csv_path = BEA_DIR / filename
    if csv_path.exists():
        return csv_path

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the matching file
        matches = [n for n in zf.namelist() if n.startswith(filename.split("__")[0]) and "ALL_AREAS" in n]
        if not matches:
            raise FileNotFoundError(f"No ALL_AREAS file for {filename} in SAINC.zip")
        zf.extract(matches[0], BEA_DIR)
        extracted = BEA_DIR / matches[0]
        if extracted != csv_path:
            extracted.rename(csv_path)
        return csv_path


def _parse_bea_wide_csv(csv_path: Path, min_year: int = 2000) -> list[dict]:
    """Parse BEA wide-format CSV (years as columns) into long format rows.

    Returns list of dicts with: geo_fips, geo_name, state_code, region, table_name,
    line_code, description, unit, year, value.
    """
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)

        # Find year columns (columns after the metadata columns)
        year_cols = {}
        for i, col in enumerate(header):
            col = col.strip()
            if col.isdigit() and int(col) >= min_year:
                year_cols[i] = int(col)

        for row in reader:
            if len(row) < 8:
                continue

            geo_fips = row[0].strip().strip('"').strip()
            geo_name = row[1].strip().strip('"').strip()

            # Skip non-state rows (US total, BEA regions, etc.)
            # State FIPS are 5-digit codes like "01000", "06000"
            state_code = FIPS_TO_STATE.get(geo_fips)
            if not state_code:
                # Try matching by name (some entries have asterisks)
                clean_name = geo_name.replace("*", "").strip()
                state_code = STATE_NAME_TO_CODE.get(clean_name)
            if not state_code:
                continue

            table_name = row[3].strip()
            line_code = row[4].strip()
            description = row[6].strip().rstrip("/").strip()
            # Clean footnote markers from description
            import re
            description = re.sub(r'\s*\d+/$', '', description).strip()
            unit = row[7].strip() if len(row) > 7 else ""

            for col_idx, year in year_cols.items():
                if col_idx >= len(row):
                    continue
                val_str = row[col_idx].strip().strip('"').strip()
                if not val_str or val_str == "(NA)" or val_str == "(D)" or val_str == "(L)":
                    continue
                try:
                    # Remove commas in numbers
                    value = float(val_str.replace(",", ""))
                    rows.append({
                        "geo_fips": geo_fips,
                        "state_code": state_code,
                        "line_code": line_code,
                        "description": description,
                        "unit": unit,
                        "year": year,
                        "value": value,
                    })
                except ValueError:
                    continue

    return rows


# ---------------------------------------------------------------------------
# 1. BEA Personal Income (SAINC1)
# ---------------------------------------------------------------------------

def build_fact_bea_personal_income(con, dry_run: bool) -> int:
    """SAINC1: Personal income, population, per capita income by state."""
    print("Building fact_bea_personal_income...")

    if not _ensure_bea_downloaded():
        return 0

    # Extract SAINC1 ALL_AREAS CSV
    zip_path = BEA_DIR / "SAINC.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        sainc1_files = [n for n in zf.namelist() if "SAINC1__ALL_AREAS" in n]
        if not sainc1_files:
            print("  SKIPPED - SAINC1 ALL_AREAS not found in ZIP")
            return 0
        csv_name = sainc1_files[0]
        csv_path = BEA_DIR / csv_name
        if not csv_path.exists():
            zf.extract(csv_name, BEA_DIR)

    print(f"  Parsing {csv_name}...")
    rows = _parse_bea_wide_csv(csv_path, min_year=2000)
    print(f"  Parsed {len(rows):,} raw records")

    if not rows:
        print("  SKIPPED - no rows parsed")
        return 0

    # Load into DuckDB and pivot to one row per state-year
    import pandas as pd
    df = pd.DataFrame(rows)
    con.register("_bea_raw", df)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bea_pi AS
        SELECT
            state_code,
            year,
            MAX(CASE WHEN line_code = '1' THEN value END) AS personal_income_millions,
            MAX(CASE WHEN line_code = '2' THEN value END) AS population,
            MAX(CASE WHEN line_code = '3' THEN value END) AS per_capita_personal_income,
            'apps.bea.gov/SAINC1' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bea_raw
        GROUP BY state_code, year
        HAVING personal_income_millions IS NOT NULL
        ORDER BY state_code, year
    """)

    count = write_parquet(con, "_fact_bea_pi", _snapshot_path("bea_personal_income"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_bea_pi").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_bea_pi").fetchone()
    latest_us = con.execute("""
        SELECT per_capita_personal_income
        FROM _fact_bea_pi
        WHERE year = (SELECT MAX(year) FROM _fact_bea_pi) AND state_code = 'AL'
    """).fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_bea_pi")
    con.unregister("_bea_raw")
    return count


# ---------------------------------------------------------------------------
# 2. BEA Income Components (SAINC4)
# ---------------------------------------------------------------------------

def build_fact_bea_income_components(con, dry_run: bool) -> int:
    """SAINC4: Personal income decomposed into wages, supplements, transfers, etc."""
    print("Building fact_bea_income_components...")

    if not _ensure_bea_downloaded():
        return 0

    zip_path = BEA_DIR / "SAINC.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        sainc4_files = [n for n in zf.namelist() if "SAINC4__ALL_AREAS" in n]
        if not sainc4_files:
            print("  SKIPPED - SAINC4 ALL_AREAS not found in ZIP")
            return 0
        csv_name = sainc4_files[0]
        csv_path = BEA_DIR / csv_name
        if not csv_path.exists():
            zf.extract(csv_name, BEA_DIR)

    print(f"  Parsing {csv_name}...")
    rows = _parse_bea_wide_csv(csv_path, min_year=2000)
    print(f"  Parsed {len(rows):,} raw records")

    if not rows:
        print("  SKIPPED - no rows parsed")
        return 0

    import pandas as pd
    df = pd.DataFrame(rows)
    con.register("_bea_raw4", df)

    # Key line codes for SAINC4:
    # 10=Personal income, 35=Earnings by place of work, 45=Net earnings,
    # 46=Dividends/interest/rent, 47=Transfer receipts,
    # 50=Wages/salaries, 60=Supplements, 70=Proprietors income,
    # 7010=Total employment, 7020=Wage/salary employment
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bea_components AS
        SELECT
            state_code,
            year,
            MAX(CASE WHEN line_code = '10' THEN value END) AS personal_income_millions,
            MAX(CASE WHEN line_code = '35' THEN value END) AS earnings_by_place_of_work,
            MAX(CASE WHEN line_code = '45' THEN value END) AS net_earnings_by_residence,
            MAX(CASE WHEN line_code = '46' THEN value END) AS dividends_interest_rent,
            MAX(CASE WHEN line_code = '47' THEN value END) AS personal_transfer_receipts,
            MAX(CASE WHEN line_code = '50' THEN value END) AS wages_and_salaries,
            MAX(CASE WHEN line_code = '60' THEN value END) AS supplements_to_wages,
            MAX(CASE WHEN line_code = '70' THEN value END) AS proprietors_income,
            MAX(CASE WHEN line_code = '71' THEN value END) AS farm_proprietors_income,
            MAX(CASE WHEN line_code = '72' THEN value END) AS nonfarm_proprietors_income,
            MAX(CASE WHEN line_code = '7010' THEN value END) AS total_employment,
            MAX(CASE WHEN line_code = '7020' THEN value END) AS wage_salary_employment,
            MAX(CASE WHEN line_code = '7040' THEN value END) AS proprietors_employment,
            MAX(CASE WHEN line_code = '20' THEN value END) AS population,
            MAX(CASE WHEN line_code = '30' THEN value END) AS per_capita_personal_income,
            'apps.bea.gov/SAINC4' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bea_raw4
        GROUP BY state_code, year
        HAVING personal_income_millions IS NOT NULL
        ORDER BY state_code, year
    """)

    count = write_parquet(con, "_fact_bea_components", _snapshot_path("bea_income_components"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_bea_components").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_bea_components").fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")

    # Show a sample: transfer receipts as % of personal income for latest year
    sample = con.execute("""
        SELECT state_code,
               ROUND(personal_transfer_receipts / personal_income_millions * 100, 1) AS transfer_pct
        FROM _fact_bea_components
        WHERE year = (SELECT MAX(year) FROM _fact_bea_components)
              AND personal_income_millions > 0
        ORDER BY transfer_pct DESC
        LIMIT 5
    """).fetchall()
    print(f"  Top states by transfer receipts as % of income ({years[1]}):")
    for sc, pct in sample:
        print(f"    {sc}: {pct}%")

    con.execute("DROP TABLE IF EXISTS _fact_bea_components")
    con.unregister("_bea_raw4")
    return count


# ---------------------------------------------------------------------------
# 3. BEA Transfer Receipts (SAINC35) — Medicaid, Medicare, SSI, SNAP, etc.
# ---------------------------------------------------------------------------

def build_fact_bea_transfer_receipts(con, dry_run: bool) -> int:
    """SAINC35: Transfer payments by type, including Medicaid and Medicare by state."""
    print("Building fact_bea_transfer_receipts...")

    if not _ensure_bea_downloaded():
        return 0

    zip_path = BEA_DIR / "SAINC.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        sainc35_files = [n for n in zf.namelist() if "SAINC35__ALL_AREAS" in n]
        if not sainc35_files:
            print("  SKIPPED - SAINC35 ALL_AREAS not found in ZIP")
            return 0
        csv_name = sainc35_files[0]
        csv_path = BEA_DIR / csv_name
        if not csv_path.exists():
            zf.extract(csv_name, BEA_DIR)

    print(f"  Parsing {csv_name}...")
    rows = _parse_bea_wide_csv(csv_path, min_year=2000)
    print(f"  Parsed {len(rows):,} raw records")

    if not rows:
        print("  SKIPPED - no rows parsed")
        return 0

    import pandas as pd
    df = pd.DataFrame(rows)
    con.register("_bea_raw35", df)

    # Note: SAINC35 values are in THOUSANDS of dollars
    # Key line codes:
    # 1000 = Total personal current transfer receipts
    # 2000 = Gov transfer receipts
    # 2200 = Medical benefits
    # 2210 = Medicare
    # 2220 = Public assistance medical care (Medicaid + CHIP + other)
    # 2221 = Medicaid
    # 2300 = Income maintenance
    # 2310 = SSI
    # 2320 = EITC
    # 2330 = SNAP
    # 2400 = Unemployment insurance
    # 2500 = Veterans' benefits
    # 2600 = Education/training assistance
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bea_transfers AS
        SELECT
            state_code,
            year,
            MAX(CASE WHEN line_code = '1000' THEN value END) AS total_transfer_receipts_thousands,
            MAX(CASE WHEN line_code = '2000' THEN value END) AS gov_transfer_receipts_thousands,
            MAX(CASE WHEN line_code = '2200' THEN value END) AS medical_benefits_thousands,
            MAX(CASE WHEN line_code = '2210' THEN value END) AS medicare_benefits_thousands,
            MAX(CASE WHEN line_code = '2220' THEN value END) AS public_assistance_medical_thousands,
            MAX(CASE WHEN line_code = '2221' THEN value END) AS medicaid_thousands,
            MAX(CASE WHEN line_code = '2222' THEN value END) AS other_medical_care_thousands,
            MAX(CASE WHEN line_code = '2230' THEN value END) AS military_medical_thousands,
            MAX(CASE WHEN line_code = '2300' THEN value END) AS income_maintenance_thousands,
            MAX(CASE WHEN line_code = '2310' THEN value END) AS ssi_benefits_thousands,
            MAX(CASE WHEN line_code = '2320' THEN value END) AS eitc_thousands,
            MAX(CASE WHEN line_code = '2330' THEN value END) AS snap_benefits_thousands,
            MAX(CASE WHEN line_code = '2340' THEN value END) AS other_income_maintenance_thousands,
            MAX(CASE WHEN line_code = '2400' THEN value END) AS unemployment_insurance_thousands,
            MAX(CASE WHEN line_code = '2500' THEN value END) AS veterans_benefits_thousands,
            MAX(CASE WHEN line_code = '2600' THEN value END) AS education_training_thousands,
            'apps.bea.gov/SAINC35' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _bea_raw35
        GROUP BY state_code, year
        HAVING total_transfer_receipts_thousands IS NOT NULL
        ORDER BY state_code, year
    """)

    count = write_parquet(con, "_fact_bea_transfers", _snapshot_path("bea_transfer_receipts"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_bea_transfers").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_bea_transfers").fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")

    # Show Medicaid spending top states
    sample = con.execute("""
        SELECT state_code,
               ROUND(medicaid_thousands / 1e6, 1) AS medicaid_billions
        FROM _fact_bea_transfers
        WHERE year = (SELECT MAX(year) FROM _fact_bea_transfers)
              AND medicaid_thousands IS NOT NULL
        ORDER BY medicaid_thousands DESC
        LIMIT 5
    """).fetchall()
    print(f"  Top states by Medicaid transfer receipts ({years[1]}):")
    for sc, bil in sample:
        print(f"    {sc}: ${bil}B")

    con.execute("DROP TABLE IF EXISTS _fact_bea_transfers")
    con.unregister("_bea_raw35")
    return count


# ---------------------------------------------------------------------------
# 4. HUD Small Area Fair Market Rents (SAFMR) by ZIP
# ---------------------------------------------------------------------------

def build_fact_safmr_zip(con, dry_run: bool) -> int:
    """HUD SAFMR: Small Area Fair Market Rents by ZIP code (FY2025)."""
    print("Building fact_safmr_zip...")

    xlsx_path = HUD_DIR / "fy2025_safmrs_revised.xlsx"
    if not xlsx_path.exists():
        print(f"  SKIPPED - {xlsx_path.name} not found")
        print("  Download from: https://www.huduser.gov/portal/datasets/fmr.html")
        return 0

    print(f"  Reading {xlsx_path.name} ({xlsx_path.stat().st_size / 1e6:.1f} MB)...")

    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    # The SAFMR file has newlines in column headers, so we use the spatial extension
    # Column mapping (from inspection):
    # ZIP Code, HUD Area Code, HUD Fair Market Rent Area Name,
    # SAFMR 0BR, SAFMR 0BR 90% Payment Standard, SAFMR 0BR 110% Payment Standard,
    # SAFMR 1BR, ..., SAFMR 2BR, ..., SAFMR 3BR, ..., SAFMR 4BR, ...

    # First, read raw with st_read to handle the messy headers
    con.execute(f"""
        CREATE OR REPLACE TABLE _raw_safmr AS
        SELECT * FROM st_read('{xlsx_path}')
    """)

    # Get column names
    cols = con.execute("SELECT * FROM _raw_safmr LIMIT 0").description
    col_names = [c[0] for c in cols]

    # Build the mapping based on position/content
    # Column 0: ZIP Code, 1: HUD Area Code, 2: HUD Area Name
    # Columns 3,4,5: 0BR (base, 90%, 110%)
    # Columns 6,7,8: 1BR
    # Columns 9,10,11: 2BR
    # Columns 12,13,14: 3BR
    # Columns 15,16,17: 4BR

    # We need to derive state code from the ZIP code.
    # Use a ZIP-to-state prefix mapping for the first 3 digits.
    # Alternatively, we extract state from the HUD area name.

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_safmr AS
        SELECT
            LPAD(CAST("{col_names[0]}" AS VARCHAR), 5, '0') AS zip_code,
            CAST("{col_names[1]}" AS VARCHAR) AS hud_area_code,
            CAST("{col_names[2]}" AS VARCHAR) AS hud_area_name,
            TRY_CAST("{col_names[3]}" AS INTEGER) AS safmr_0br,
            TRY_CAST("{col_names[4]}" AS INTEGER) AS safmr_0br_90pct,
            TRY_CAST("{col_names[5]}" AS INTEGER) AS safmr_0br_110pct,
            TRY_CAST("{col_names[6]}" AS INTEGER) AS safmr_1br,
            TRY_CAST("{col_names[7]}" AS INTEGER) AS safmr_1br_90pct,
            TRY_CAST("{col_names[8]}" AS INTEGER) AS safmr_1br_110pct,
            TRY_CAST("{col_names[9]}" AS INTEGER) AS safmr_2br,
            TRY_CAST("{col_names[10]}" AS INTEGER) AS safmr_2br_90pct,
            TRY_CAST("{col_names[11]}" AS INTEGER) AS safmr_2br_110pct,
            TRY_CAST("{col_names[12]}" AS INTEGER) AS safmr_3br,
            TRY_CAST("{col_names[13]}" AS INTEGER) AS safmr_3br_90pct,
            TRY_CAST("{col_names[14]}" AS INTEGER) AS safmr_3br_110pct,
            TRY_CAST("{col_names[15]}" AS INTEGER) AS safmr_4br,
            TRY_CAST("{col_names[16]}" AS INTEGER) AS safmr_4br_90pct,
            TRY_CAST("{col_names[17]}" AS INTEGER) AS safmr_4br_110pct,
            2025 AS fiscal_year,
            'huduser.gov/SAFMR' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _raw_safmr
        WHERE "{col_names[0]}" IS NOT NULL
    """)

    # Add state_code by extracting the state abbreviation from the HUD area name
    # Most area names end with ", XX" where XX is state abbreviation.
    # For multi-state MSAs we'll use the first state listed.
    # Better approach: use ZIP prefix to state mapping.

    # ZIP prefix to state code (3-digit ZIP prefixes)
    zip_state_sql = _build_zip_state_case()

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_safmr2 AS
        SELECT
            {zip_state_sql} AS state_code,
            *
        FROM _fact_safmr
    """)

    # Drop rows where we couldn't determine state
    con.execute("""
        DELETE FROM _fact_safmr2 WHERE state_code IS NULL OR state_code = ''
    """)

    count = write_parquet(con, "_fact_safmr2", _snapshot_path("safmr_zip"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_safmr2").fetchone()[0]
    avg_2br = con.execute("SELECT ROUND(AVG(safmr_2br)) FROM _fact_safmr2").fetchone()[0]
    max_2br = con.execute("SELECT MAX(safmr_2br), zip_code FROM _fact_safmr2 GROUP BY zip_code ORDER BY 1 DESC LIMIT 1").fetchone()
    print(f"  {count:,} rows, {states} states/territories")
    print(f"  Avg 2BR SAFMR: ${avg_2br:,.0f}/mo, Max: ${max_2br[0]:,}/mo (ZIP {max_2br[1]})")

    con.execute("DROP TABLE IF EXISTS _raw_safmr")
    con.execute("DROP TABLE IF EXISTS _fact_safmr")
    con.execute("DROP TABLE IF EXISTS _fact_safmr2")
    return count


def _build_zip_state_case() -> str:
    """Build a SQL CASE statement mapping 3-digit ZIP prefixes to state codes."""
    # ZIP prefix ranges to state mapping
    # Source: USPS Publication 65
    zip_ranges = {
        "AL": [(350, 369)],
        "AK": [(995, 999)],
        "AZ": [(850, 865)],
        "AR": [(716, 729)],
        "CA": [(900, 961)],
        "CO": [(800, 816)],
        "CT": [(60, 69)],
        "DE": [(197, 199)],
        "DC": [(200, 205)],
        "FL": [(320, 349)],
        "GA": [(300, 319)],
        "HI": [(967, 968)],
        "ID": [(832, 838)],
        "IL": [(600, 629)],
        "IN": [(460, 479)],
        "IA": [(500, 528)],
        "KS": [(660, 679)],
        "KY": [(400, 427)],
        "LA": [(700, 714)],
        "ME": [(39, 49)],
        "MD": [(206, 219)],
        "MA": [(10, 27)],
        "MI": [(480, 499)],
        "MN": [(550, 567)],
        "MS": [(386, 397)],
        "MO": [(630, 658)],
        "MT": [(590, 599)],
        "NE": [(680, 693)],
        "NV": [(889, 898)],
        "NH": [(30, 38)],
        "NJ": [(70, 89)],
        "NM": [(870, 884)],
        "NY": [(100, 149)],
        "NC": [(270, 289)],
        "ND": [(580, 588)],
        "OH": [(430, 459)],
        "OK": [(730, 749)],
        "OR": [(970, 979)],
        "PA": [(150, 196)],
        "RI": [(28, 29)],
        "SC": [(290, 299)],
        "SD": [(570, 577)],
        "TN": [(370, 385)],
        "TX": [(750, 799)],
        "UT": [(840, 847)],
        "VT": [(50, 59)],
        "VA": [(220, 246)],
        "WA": [(980, 994)],
        "WV": [(247, 268)],
        "WI": [(530, 549)],
        "WY": [(820, 831)],
        "PR": [(6, 7), (9, 9)],
        "GU": [(969, 969)],
        "VI": [(8, 8)],
    }

    cases = []
    for state, ranges in zip_ranges.items():
        for lo, hi in ranges:
            cases.append(
                f"WHEN CAST(SUBSTRING(LPAD(CAST(zip_code AS VARCHAR), 5, '0'), 1, 3) AS INTEGER) "
                f"BETWEEN {lo} AND {hi} THEN '{state}'"
            )

    return "CASE\n" + "\n".join(cases) + "\nELSE NULL END"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_bea_personal_income": build_fact_bea_personal_income,
    "fact_bea_income_components": build_fact_bea_income_components,
    "fact_bea_transfer_receipts": build_fact_bea_transfer_receipts,
    "fact_safmr_zip": build_fact_safmr_zip,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest economic v2 data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of tables to build")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]
        invalid = [t for t in tables if t not in ALL_TABLES]
        if invalid:
            print(f"ERROR: Unknown tables: {invalid}")
            print(f"Valid: {list(ALL_TABLES.keys())}")
            return

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        try:
            totals[name] = ALL_TABLES[name](con, args.dry_run)
        except Exception as e:
            print(f"  ERROR building {name}: {e}")
            import traceback
            traceback.print_exc()
            totals[name] = 0
        print()

    con.close()

    print("=" * 60)
    print("ECONOMIC V2 DATA LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>10,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>10,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_economic_v2_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
