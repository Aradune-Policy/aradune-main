#!/usr/bin/env python3
"""
build_lake_promoting_interoperability.py — Ingest CMS Promoting Interoperability
(Performance Indicator) dataset into the Aradune data lake.

Reads from: data/raw/pi_dataset_feb2026.csv
Writes to:  data/lake/fact/promoting_interoperability/

Note: The lake already has fact_pi_performance with the same source data (10,404 rows).
This script creates fact_promoting_interoperability as a cleaner re-ingest that:
  - Includes all footnote columns for full provenance
  - Uses consistent snake_case naming
  - Adds data vintage metadata

Usage:
  python3 scripts/build_lake_promoting_interoperability.py
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

RAW_FILE = RAW_DIR / "pi_dataset_feb2026.csv"
OUT_DIR = FACT_DIR / "promoting_interoperability"
SNAPSHOT_DATE = date.today().isoformat()


def clean_col(name: str) -> str:
    """Convert column name to snake_case."""
    name = name.strip()
    # Remove parenthetical content
    name = re.sub(r'\s*\([^)]*\)', '', name)
    # Remove " - footnotes" suffix
    name = re.sub(r'\s*-\s*footnotes$', '_footnotes', name)
    # Common abbreviations
    name = name.replace("Medicaid and CHIP", "medicaid_chip")
    name = name.replace("CHIP", "chip")
    name = name.replace("Medicaid", "medicaid")
    # Convert to snake_case
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
    print(f"  Cleaned columns: {list(df.columns)[:10]}...")

    # Identify footnote columns vs data columns
    footnote_cols = [c for c in df.columns if c.endswith('_footnotes')]
    data_cols = [c for c in df.columns if not c.endswith('_footnotes')]
    print(f"  Data columns: {len(data_cols)}, Footnote columns: {len(footnote_cols)}")

    # Rename key columns for consistency
    rename_map = {
        'state_abbreviation': 'state_code',
        'state_name': 'state_name',
        'reporting_period': 'reporting_period',
        'state_expanded_medicaid': 'expansion_status',
        'preliminary_or_updated': 'data_status',
        'final_report': 'is_final',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Parse reporting_period to year/month
    df['year'] = pd.to_numeric(df['reporting_period'].str[:4], errors='coerce').astype('Int32')
    df['month'] = pd.to_numeric(df['reporting_period'].str[4:6], errors='coerce').astype('Int32')

    # Convert numeric columns
    numeric_cols = [
        'new_applications_submitted_to_medicaid_chip_agencies',
        'applications_for_financial_assistance_submitted_to_the_state_based_marketplace',
        'total_applications_for_financial_assistance_submitted_at_state_level',
        'individuals_determined_eligible_for_medicaid_at_application',
        'individuals_determined_eligible_for_chip_at_application',
        'total_medicaid_chip_determinations',
        'medicaid_chip_child_enrollment',
        'total_medicaid_chip_enrollment',
        'total_medicaid_enrollment',
        'total_chip_enrollment',
        'total_adult_medicaid_enrollment',
        'total_medicaid_chip_determinations_processed_in_less_than_24_hours',
        'total_medicaid_chip_determinations_processed_between_24_hours_and_7_days',
        'total_medicaid_chip_determinations_processed_between_8_days_and_30_days',
        'total_medicaid_chip_determinations_processed_between_31_days_and_45_days',
        'total_medicaid_chip_determinations_processed_in_more_than_45_days',
        'total_call_center_volume',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').astype('Int64')

    # Float columns
    float_cols = [
        'average_call_center_wait_time',
        'average_call_center_abandonment_rate',
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace('%', '').str.replace(',', ''), errors='coerce')

    # Drop fully empty columns
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        print(f"  Dropping {len(empty_cols)} empty columns: {empty_cols}")
        df = df.drop(columns=empty_cols)

    # Drop footnote columns that are entirely empty
    empty_footnotes = [c for c in footnote_cols if c in df.columns and df[c].isna().all()]
    if empty_footnotes:
        print(f"  Dropping {len(empty_footnotes)} empty footnote columns")
        df = df.drop(columns=[c for c in empty_footnotes if c in df.columns])

    # Add metadata
    df['source'] = 'medicaid.gov/pi'
    df['snapshot_date'] = SNAPSHOT_DATE

    # Strip whitespace from string columns
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Validation
    print("\n--- Validation ---")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    print(f"  States: {df['state_code'].nunique()}")
    print(f"  Period range: {df['reporting_period'].min()} - {df['reporting_period'].max()}")
    print(f"  Years: {sorted(df['year'].dropna().unique().tolist())}")

    null_rates = df.isnull().mean()
    high_null = null_rates[null_rates > 0.5]
    if len(high_null) > 0:
        print(f"  High null columns (>50%): {list(high_null.index)}")

    # Check overlap with existing pi_performance
    try:
        existing = pq.read_table(str(FACT_DIR / "pi_performance"))
        print(f"\n  Existing pi_performance: {existing.num_rows:,} rows")
        print(f"  This file: {len(df):,} rows")
        if existing.num_rows == len(df):
            print("  NOTE: Same row count - this is likely the same source data")
            print("  New table adds footnote columns and cleaner column names")
    except Exception:
        print("  No existing pi_performance table found")

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
