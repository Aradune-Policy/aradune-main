#!/usr/bin/env python3
"""
build_lake_aca_effectuated_enrollment.py — Ingest ACA Marketplace Enrollment by State.

Source: CMS 2025 Open Enrollment Period State-Level Public Use File
  https://www.cms.gov/data-research/statistics-trends-reports/marketplace-products/
  2025-marketplace-open-enrollment-period-public-use-files

Also fetches historical OEP data (2014-2025) for trend analysis.

Tables built:
  fact_aca_effectuated_enrollment — ACA marketplace enrollment by state.

Usage:
  python3 scripts/build_lake_aca_effectuated_enrollment.py
"""

import json
import re
import uuid
import zipfile
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen, Request

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "insurance_market"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# CMS OEP state-level download URLs by year
OEP_URLS = {
    2025: "https://www.cms.gov/files/zip/2025-oep-state-level-public-use-file.zip",
    2024: "https://www.cms.gov/files/zip/2024-oep-state-level-public-use-file.zip",
    2023: "https://www.cms.gov/files/zip/2023-oep-state-level-public-use-file.zip",
    2022: "https://www.cms.gov/files/zip/2022-oep-state-level-public-use-file.zip",
    2021: "https://www.cms.gov/files/zip/2021-oep-state-level-public-use-file.zip",
}

# Column name mapping from abbreviated to readable snake_case
COLUMN_MAP = {
    "State_Abrvtn": "state_code",
    "Pltfrm": "platform",
    "Aplctn_Sbmtd": "applications_submitted",
    "Indvdl_Aplctn_Sbmtd": "individual_applications_submitted",
    "QHP_Elgbl": "qhp_eligible",
    "FA_Elgbl": "financial_assistance_eligible",
    "MC_Elgbl": "medicaid_chip_eligible",
    "Cnsmr": "total_consumers",
    "New_Cnsmr": "new_consumers",
    "Tot_Renrl": "total_reenrolled",
    "Actv_Renrl": "active_reenrolled",
    "Auto_Renrl": "auto_reenrolled",
    "Actv_Renrl_Sw": "active_reenrolled_switched",
    "Actv_Renrl_Nsw": "active_reenrolled_not_switched",
    "Avg_Prm": "avg_premium",
    "Avg_Prm_Aftr_APTC": "avg_premium_after_aptc",
    "Cnsmr_Prm_Aftr_APTC_LTEQ10": "consumers_premium_lte_10",
    "Cnsmr_Wth_APTC_CSR": "consumers_with_aptc_csr",
    "CSR_Cnsmr": "csr_consumers",
    "CSR_Cnsmr_73": "csr_consumers_73",
    "CSR_Cnsmr_87": "csr_consumers_87",
    "CSR_Cnsmr_94": "csr_consumers_94",
    "CSR_Cnsmr_AIAN": "csr_consumers_aian",
    "APTC_Cnsmr": "aptc_consumers",
    "APTC_Cnsmr_Avg_APTC": "aptc_consumers_avg_aptc",
    "APTC_Cnsmr_Avg_Prm_Aftr_APTC": "aptc_consumers_avg_premium_after_aptc",
    "Age_0_17": "age_0_17",
    "Age_18_25": "age_18_25",
    "Age_26_34": "age_26_34",
    "Age_35_44": "age_35_44",
    "Age_45_54": "age_45_54",
    "Age_55_64": "age_55_64",
    "Age_GE65": "age_65_plus",
    "Male": "male",
    "Female": "female",
    "Hspnc_Yes": "hispanic",
    "Hspnc_No": "non_hispanic",
    "Unk_Ethncty": "unknown_ethnicity",
    "AIAN_NonHspnc": "aian_non_hispanic",
    "ASN_NonHspnc": "asian_non_hispanic",
    "NHPI_NonHspnc": "nhpi_non_hispanic",
    "BLACK_NonHspnc": "black_non_hispanic",
    "WHT_NonHspnc": "white_non_hispanic",
    "Othr_Race_NonHspnc": "other_race_non_hispanic",
    "Mlt_Race_NonHspnc": "multi_race_non_hispanic",
    "Unk_Race_NonHspnc": "unknown_race_non_hispanic",
    "AIAN": "aian",
    "ASN": "asian",
    "NHPI": "nhpi",
    "BLACK": "black",
    "WHT": "white",
    "Othr_Race": "other_race",
    "Mlt_Race": "multi_race",
    "Unk_Race": "unknown_race",
    "Rrl": "rural",
    "Non_Rrl": "non_rural",
    "Ctstrphc": "catastrophic",
    "Brnz": "bronze",
    "Slvr": "silver",
    "Gld": "gold",
    "Pltnm": "platinum",
    "FPL_LT100": "fpl_lt_100",
    "FPL_100_138": "fpl_100_138",
    "FPL_100_150": "fpl_100_150",
    "FPL_150_200": "fpl_150_200",
    "FPL_200_250": "fpl_200_250",
    "FPL_250_300": "fpl_250_300",
    "FPL_300_400": "fpl_300_400",
    "FPL_400_500": "fpl_400_500",
    "FPL_GT500": "fpl_gt_500",
    "FPL_OTHR": "fpl_other",
    "Dntl_Cnsmr": "dental_consumers",
    "BHP_Enrlmnt": "bhp_enrollment",
    "BHP_New_Enrl": "bhp_new_enrollment",
    "BHP_Renrl": "bhp_reenrollment",
}

# Weekly columns to aggregate
WEEK_COLS = {f"Wk_{i}": f"week_{i}" for i in range(1, 20)}
COLUMN_MAP.update(WEEK_COLS)


def clean_numeric(val):
    """Clean numeric values from CMS formatting."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s in ("", "+", "*", "NR", "N/A", "**"):
        return None
    # Remove dollar signs, commas, spaces
    s = re.sub(r'[\$,\s]', '', s)
    try:
        return float(s) if '.' in s else int(s)
    except (ValueError, TypeError):
        return None


def fetch_and_extract_csv(url: str, year: int) -> pd.DataFrame | None:
    """Download ZIP, extract CSV, return DataFrame."""
    print(f"  Fetching {year} OEP data...")
    req = Request(url, headers={
        "User-Agent": "Aradune/1.0 (Medicaid intelligence platform)",
    })
    try:
        with urlopen(req, timeout=60) as resp:
            zip_data = BytesIO(resp.read())

        with zipfile.ZipFile(zip_data) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
                if xlsx_files:
                    with zf.open(xlsx_files[0]) as f:
                        df = pd.read_excel(BytesIO(f.read()))
                        return df
                return None

            with zf.open(csv_files[0]) as f:
                df = pd.read_csv(f, encoding='utf-8-sig')
                return df

    except Exception as e:
        print(f"    Error fetching {year}: {e}")
        return None


def process_oep_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Clean and normalize OEP DataFrame."""
    # Rename columns
    rename_map = {}
    for old_col in df.columns:
        clean = old_col.strip()
        if clean in COLUMN_MAP:
            rename_map[old_col] = COLUMN_MAP[clean]
        else:
            # Convert to snake_case
            snake = re.sub(r'([A-Z])', r'_\1', clean).lower().strip('_')
            snake = re.sub(r'[^a-z0-9]+', '_', snake).strip('_')
            rename_map[old_col] = snake

    df = df.rename(columns=rename_map)

    # Add year
    df["enrollment_year"] = year

    # Clean numeric columns (everything except state_code and platform)
    text_cols = {"state_code", "platform", "enrollment_year"}
    for col in df.columns:
        if col not in text_cols:
            df[col] = df[col].apply(clean_numeric)

    # Filter out non-state rows (totals, etc.)
    if "state_code" in df.columns:
        valid_states = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
            "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
            "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
            "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
            "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
            "WY", "AS", "GU", "MP", "PR", "VI",
        }
        df = df[df["state_code"].isin(valid_states)].copy()

    return df


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    return count


def main():
    print("=" * 60)
    print("ACA Marketplace Enrollment Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    all_dfs = []

    # Try local file first (2025 already downloaded)
    local_csv = RAW_DIR / "oep_state_level_2025.csv"
    if local_csv.exists():
        print(f"  Using local file: {local_csv.name}")
        df = pd.read_csv(local_csv, encoding='utf-8-sig')
        df = process_oep_dataframe(df, 2025)
        all_dfs.append(df)
        print(f"    2025: {len(df)} state rows")

    # Fetch historical years
    for year, url in sorted(OEP_URLS.items()):
        if year == 2025 and all_dfs:  # Already have 2025
            continue
        df = fetch_and_extract_csv(url, year)
        if df is not None:
            df = process_oep_dataframe(df, year)
            all_dfs.append(df)
            print(f"    {year}: {len(df)} state rows")
        else:
            print(f"    {year}: FAILED to fetch")

    if not all_dfs:
        print("  No data fetched! Exiting.")
        return

    # Combine all years
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"\n  Combined: {len(combined):,} rows across {combined['enrollment_year'].nunique()} years")

    # Build DuckDB table
    con = duckdb.connect()
    combined["source"] = "https://www.cms.gov/marketplace-products"
    combined["snapshot_date"] = SNAPSHOT_DATE

    con.execute("CREATE TABLE fact_aca_effectuated_enrollment AS SELECT * FROM combined")

    # Stats
    by_year = con.execute("""
        SELECT enrollment_year, COUNT(*) as states,
               SUM(CAST(total_consumers AS BIGINT)) as total_enrollment
        FROM fact_aca_effectuated_enrollment
        GROUP BY enrollment_year ORDER BY enrollment_year
    """).fetchall()
    print("\n  By year:")
    for yr, states, total in by_year:
        total_str = f"{total:,.0f}" if total else "N/A"
        print(f"    {yr}: {states} states, {total_str} total consumers")

    top_states = con.execute("""
        SELECT state_code, CAST(total_consumers AS BIGINT) as enrollment
        FROM fact_aca_effectuated_enrollment
        WHERE enrollment_year = (SELECT MAX(enrollment_year) FROM fact_aca_effectuated_enrollment)
        ORDER BY enrollment DESC LIMIT 10
    """).fetchall()
    print(f"\n  Top 10 states (latest year):")
    for s, n in top_states:
        print(f"    {s}: {n:,}" if n else f"    {s}: N/A")

    # Write parquet
    out_path = FACT_DIR / "aca_effectuated_enrollment" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_aca_effectuated_enrollment", out_path)

    # Manifest
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_aca_effectuated_enrollment.py",
        "source": "https://www.cms.gov/marketplace-products",
        "tables": {
            "fact_aca_effectuated_enrollment": {
                "rows": row_count,
                "path": f"fact/aca_effectuated_enrollment/snapshot={SNAPSHOT_DATE}/data.parquet",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    (META_DIR / f"manifest_aca_effectuated_enrollment_{SNAPSHOT_DATE}.json").write_text(
        json.dumps(manifest, indent=2)
    )

    con.close()
    print("\n" + "=" * 60)
    print("ACA MARKETPLACE ENROLLMENT INGESTION COMPLETE")
    print(f"  fact_aca_effectuated_enrollment: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
