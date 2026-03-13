#!/usr/bin/env python3
"""
build_lake_bls_oews_msa.py — Ingest BLS OEWS full occupation x area data (May 2024)
filtered to healthcare-related SOC codes at MSA level.

Reads from: data/raw/all_data_M_2024.xlsx (78 MB)
Writes to:  data/lake/fact/bls_oews_msa/

Filters to healthcare-related SOC codes:
  29-xxxx  Healthcare Practitioners and Technical
  31-xxxx  Healthcare Support
  21-1xxx  Counselors, Social Workers, and Other Community and Social Service

The lake already has fact_bls_oews (38K rows, state-level, all area types).
This adds MSA-level detail (AREA_TYPE=4) for healthcare occupations only.

Usage:
  python3 scripts/build_lake_bls_oews_msa.py
"""

import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
FACT_DIR = PROJECT_ROOT / "data" / "lake" / "fact"

RAW_FILE = RAW_DIR / "all_data_M_2024.xlsx"
OUT_DIR = FACT_DIR / "bls_oews_msa"
SNAPSHOT_DATE = date.today().isoformat()
DATA_YEAR = 2024

# Healthcare SOC code prefixes
HEALTHCARE_SOC_PREFIXES = ('29-', '31-', '21-1')


def is_healthcare_soc(code: str) -> bool:
    """Check if SOC code is healthcare-related."""
    if not isinstance(code, str):
        return False
    return any(code.startswith(prefix) for prefix in HEALTHCARE_SOC_PREFIXES)


def clean_col(name: str) -> str:
    """Convert column name to snake_case."""
    name = name.strip()
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_').lower()
    return name


def safe_numeric(series, as_int=False):
    """Convert series to numeric, handling '#' and '*' BLS suppression markers."""
    cleaned = series.astype(str).str.replace(',', '', regex=False)
    cleaned = cleaned.replace({'#': None, '*': None, '**': None, 'nan': None, '': None})
    result = pd.to_numeric(cleaned, errors='coerce')
    if as_int:
        result = result.astype('Int64')
    return result


def main():
    if not RAW_FILE.exists():
        print(f"ERROR: Raw file not found: {RAW_FILE}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {RAW_FILE.name} (78 MB, may take 30-60 seconds)...")

    # Read with openpyxl, first sheet only
    df = pd.read_excel(RAW_FILE, sheet_name='All May 2024 data', dtype=str,
                       engine='openpyxl')
    print(f"  Raw: {len(df):,} rows, {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")

    # Filter to MSA-level data (AREA_TYPE = 4)
    df_msa = df[df['AREA_TYPE'] == '4'].copy()
    print(f"  MSA-level (AREA_TYPE=4): {len(df_msa):,} rows")

    # Filter to healthcare SOC codes
    df_hc = df_msa[df_msa['OCC_CODE'].apply(is_healthcare_soc)].copy()
    print(f"  Healthcare SOC (29-/31-/21-1): {len(df_hc):,} rows")

    # Exclude summary/major group codes (ending in -0000)
    df_hc = df_hc[~df_hc['OCC_CODE'].str.endswith('-0000')].copy()
    print(f"  After excluding major groups: {len(df_hc):,} rows")

    # Filter to cross-industry, all ownership
    df_hc = df_hc[(df_hc['I_GROUP'] == 'cross-industry') & (df_hc['OWN_CODE'] == '1235')].copy()
    print(f"  Cross-industry, all ownership: {len(df_hc):,} rows")

    # Clean column names
    df_hc.columns = [clean_col(c) for c in df_hc.columns]

    # Rename columns for consistency with existing bls_oews table
    rename_map = {
        'area': 'msa_code',
        'area_title': 'msa_title',
        'prim_state': 'state_code',
        'occ_code': 'soc_code',
        'occ_title': 'occupation_title',
        'o_group': 'occ_group',
        'tot_emp': 'total_employment',
        'emp_prse': 'employment_rse',
        'jobs_1000': 'jobs_per_1000',
        'loc_quotient': 'location_quotient',
        'h_mean': 'hourly_mean',
        'a_mean': 'annual_mean',
        'mean_prse': 'mean_rse',
        'h_pct10': 'hourly_p10',
        'h_pct25': 'hourly_p25',
        'h_median': 'hourly_median',
        'h_pct75': 'hourly_p75',
        'h_pct90': 'hourly_p90',
        'a_pct10': 'annual_p10',
        'a_pct25': 'annual_p25',
        'a_median': 'annual_median',
        'a_pct75': 'annual_p75',
        'a_pct90': 'annual_p90',
    }
    df_hc = df_hc.rename(columns={k: v for k, v in rename_map.items() if k in df_hc.columns})

    # Convert numeric columns
    int_cols = ['total_employment']
    for col in int_cols:
        if col in df_hc.columns:
            df_hc[col] = safe_numeric(df_hc[col], as_int=True)

    float_cols = ['employment_rse', 'jobs_per_1000', 'location_quotient',
                  'hourly_mean', 'annual_mean', 'mean_rse',
                  'hourly_p10', 'hourly_p25', 'hourly_median', 'hourly_p75', 'hourly_p90',
                  'annual_p10', 'annual_p25', 'annual_median', 'annual_p75', 'annual_p90']
    for col in float_cols:
        if col in df_hc.columns:
            df_hc[col] = safe_numeric(df_hc[col])

    # Select and order final columns
    keep_cols = ['msa_code', 'msa_title', 'state_code', 'soc_code', 'occupation_title',
                 'occ_group', 'total_employment', 'employment_rse',
                 'jobs_per_1000', 'location_quotient',
                 'hourly_mean', 'annual_mean', 'mean_rse',
                 'hourly_p10', 'hourly_p25', 'hourly_median', 'hourly_p75', 'hourly_p90',
                 'annual_p10', 'annual_p25', 'annual_median', 'annual_p75', 'annual_p90']
    keep_cols = [c for c in keep_cols if c in df_hc.columns]
    df_hc = df_hc[keep_cols].copy()

    # Add metadata
    df_hc['data_year'] = DATA_YEAR
    df_hc['source'] = 'bls.gov/oews'
    df_hc['snapshot_date'] = SNAPSHOT_DATE

    # Strip whitespace from string columns
    str_cols = df_hc.select_dtypes(include=['object', 'string']).columns
    for col in str_cols:
        df_hc[col] = df_hc[col].str.strip()

    # Drop fully empty columns
    empty_cols = [c for c in df_hc.columns if df_hc[c].isna().all()]
    if empty_cols:
        print(f"  Dropping {len(empty_cols)} empty columns: {empty_cols}")
        df_hc = df_hc.drop(columns=empty_cols)

    # Validation
    print("\n--- Validation ---")
    print(f"  Rows: {len(df_hc):,}")
    print(f"  Columns: {len(df_hc.columns)}")
    print(f"  MSAs: {df_hc['msa_code'].nunique()}")
    print(f"  States: {df_hc['state_code'].nunique()}")
    print(f"  SOC codes: {df_hc['soc_code'].nunique()}")
    print(f"  Occupations: {df_hc['occupation_title'].nunique()}")

    # Show top occupations by row count
    top_occs = df_hc.groupby('soc_code')['msa_code'].count().sort_values(ascending=False).head(10)
    print(f"\n  Top 10 SOC codes by coverage:")
    for soc, count in top_occs.items():
        title = df_hc[df_hc['soc_code'] == soc]['occupation_title'].iloc[0]
        print(f"    {soc}: {count:,} MSAs — {title}")

    # Check null rates for key columns
    key_cols = ['total_employment', 'hourly_mean', 'annual_mean', 'hourly_median']
    print(f"\n  Null rates (key columns):")
    for col in key_cols:
        if col in df_hc.columns:
            null_pct = df_hc[col].isna().mean() * 100
            print(f"    {col}: {null_pct:.1f}%")

    # Compare with existing bls_oews
    try:
        existing = pq.read_table(str(FACT_DIR / "bls_oews"))
        print(f"\n  Existing bls_oews: {existing.num_rows:,} rows (all area types)")
        print(f"  This MSA table: {len(df_hc):,} rows (MSA-level, healthcare only)")
    except Exception:
        pass

    # Write parquet
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "data.parquet"

    table = pa.Table.from_pandas(df_hc, preserve_index=False)
    pq.write_table(table, out_path, compression='zstd')

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"\n--- Output ---")
    print(f"  File: {out_path.relative_to(PROJECT_ROOT)}")
    print(f"  Rows: {len(df_hc):,}")
    print(f"  Columns: {len(df_hc.columns)}")
    print(f"  Size: {size_mb:.2f} MB")
    print("\nDone.")


if __name__ == "__main__":
    main()
