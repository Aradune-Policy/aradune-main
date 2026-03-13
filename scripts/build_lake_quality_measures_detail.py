#!/usr/bin/env python3
"""
build_lake_quality_measures_detail.py — Ingest 2024 Child and Adult Health Care
Quality Measures detail dataset into the Aradune data lake.

Reads from: data/raw/2024-child-and-adult-health-care-quality-measures.csv
Writes to:  data/lake/fact/quality_measures_2024_detail/

Note: The lake already has:
  - fact_quality_core_set_2024 (10,972 rows) — normalized, no state-specific comments
  - fact_quality_measures_2024_full (11,100 rows) — broader, no state-specific comments
This adds fact_quality_measures_2024_detail with:
  - State-Specific Comments (100% populated)
  - Rate Definition detail
  - Full provenance

Usage:
  python3 scripts/build_lake_quality_measures_detail.py
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

RAW_FILE = RAW_DIR / "2024-child-and-adult-health-care-quality-measures.csv"
OUT_DIR = FACT_DIR / "quality_measures_2024_detail"
SNAPSHOT_DATE = date.today().isoformat()

# State name to code mapping
STATE_CODES = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI',
    'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
    'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'Puerto Rico': 'PR', 'Guam': 'GU', 'U.S. Virgin Islands': 'VI',
    'American Samoa': 'AS', 'Northern Mariana Islands': 'MP',
    'Dist. of Col.': 'DC',
}


def clean_col(name: str) -> str:
    """Convert column name to snake_case."""
    name = name.strip()
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_').lower()
    return name


def main():
    if not RAW_FILE.exists():
        print(f"ERROR: Raw file not found: {RAW_FILE}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {RAW_FILE.name}...")
    df = pd.read_csv(RAW_FILE, dtype=str)
    print(f"  Raw: {len(df):,} rows, {len(df.columns)} columns")

    # Clean column names
    df.columns = [clean_col(c) for c in df.columns]
    print(f"  Columns: {list(df.columns)}")

    # Rename for consistency
    rename_map = {
        'state': 'state_name',
        'measure_abbreviation': 'measure_id',
        'number_of_states_reporting': 'states_reporting',
        'mean': 'mean_value',
        'median': 'median_value',
        'bottom_quartile': 'q1_value',
        'top_quartile': 'q3_value',
        'source': 'data_source',
        'notes': 'notes',
        'state_specific_comments': 'state_comments',
        'rate_used_in_calculating_state_mean_and_median': 'rate_in_calculation',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Add state_code
    df['state_code'] = df['state_name'].map(STATE_CODES)
    unmapped = df[df['state_code'].isna()]['state_name'].unique()
    if len(unmapped) > 0:
        print(f"  WARNING: Unmapped states: {unmapped}")

    # Convert numeric columns
    numeric_int = ['core_set_year', 'states_reporting']
    for col in numeric_int:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')

    numeric_float = ['state_rate', 'mean_value', 'median_value', 'q1_value', 'q3_value']
    for col in numeric_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop fully empty columns
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        print(f"  Dropping {len(empty_cols)} empty columns: {empty_cols}")
        df = df.drop(columns=empty_cols)

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Add metadata
    df['source'] = 'mathematica/cms_quality_measures'
    df['snapshot_date'] = SNAPSHOT_DATE

    # Reorder columns for readability
    priority_cols = ['state_code', 'state_name', 'domain', 'reporting_program',
                     'measure_name', 'measure_id', 'measure_type', 'rate_definition',
                     'core_set_year', 'population', 'methodology',
                     'state_rate', 'states_reporting', 'mean_value', 'median_value',
                     'q1_value', 'q3_value', 'state_comments', 'notes',
                     'rate_in_calculation', 'data_source', 'source', 'snapshot_date']
    final_cols = [c for c in priority_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in final_cols]
    df = df[final_cols + remaining]

    # Validation
    print("\n--- Validation ---")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  States: {df['state_code'].nunique()} (incl territories)")
    print(f"  Core set year: {df['core_set_year'].unique().tolist()}")
    print(f"  Measures: {df['measure_id'].nunique()}")
    print(f"  Domains: {df['domain'].nunique()}")
    print(f"  State comments populated: {df['state_comments'].notna().sum():,}/{len(df):,}")

    # Compare with existing tables
    for table_name in ['quality_core_set_2024', 'quality_measures_2024_full']:
        try:
            existing = pq.read_table(str(FACT_DIR / table_name))
            print(f"\n  Existing {table_name}: {existing.num_rows:,} rows, {existing.num_columns} cols")
        except Exception:
            pass

    print(f"\n  This detail table adds: state_comments, rate_definition columns")

    # Write parquet
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "data.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression='zstd')

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"\n--- Output ---")
    print(f"  File: {out_path.relative_to(PROJECT_ROOT)}")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Size: {size_mb:.2f} MB")
    print("\nDone.")


if __name__ == "__main__":
    main()
