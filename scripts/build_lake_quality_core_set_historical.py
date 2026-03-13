#!/usr/bin/env python3
"""
build_lake_quality_core_set_historical.py — Download and ingest Medicaid/CHIP
Quality Core Set data for reporting years 2017-2022 from data.medicaid.gov API.

Existing lake tables: quality_core_set_2023 (5,555 rows), quality_core_set_2024 (10,972 rows)
This script adds: quality_core_set_2017 through quality_core_set_2022 + quality_core_set_combined

Source: data.medicaid.gov API
  API pattern: https://data.medicaid.gov/api/1/datastore/query/{dataset_id}/0?limit=500&offset=N

Output:
  data/lake/fact/quality_core_set_{year}/snapshot=YYYY-MM-DD/data.parquet  (one per year)
  data/lake/fact/quality_core_set_combined/snapshot=YYYY-MM-DD/data.parquet (all years 2017-2024)

Usage:
  python3 scripts/build_lake_quality_core_set_historical.py
  python3 scripts/build_lake_quality_core_set_historical.py --download-only
  python3 scripts/build_lake_quality_core_set_historical.py --ingest-only
"""

import json
import re
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
FACT_DIR = PROJECT_ROOT / "data" / "lake" / "fact"
SNAPSHOT_DATE = date.today().isoformat()

# Dataset UUIDs on data.medicaid.gov
DATASETS = {
    2017: "c1028fdf-2e43-5d5e-990b-51ed03428625",
    2018: "229d6279-e614-5353-9226-f6a6f37d06c3",
    2019: "e36d89c0-f62e-56d5-bc7e-b0adf89262b8",
    2020: "fbbe1734-b448-4e5a-bc94-3f8688534741",
    2021: "a058ef78-e18b-4435-94aa-b70ab6ce5904",
    2022: "dfd13757-d763-4f7a-9641-3f06ce21b4c6",
}

API_BASE = "https://data.medicaid.gov/api/1/datastore/query"
PAGE_LIMIT = 500  # CMS API max

# State name -> code mapping
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


def download_dataset(year: int, dataset_id: str) -> Path:
    """Download all pages of a dataset from data.medicaid.gov API."""
    import urllib.request

    raw_file = RAW_DIR / f"quality_core_set_{year}.json"

    if raw_file.exists():
        with open(raw_file) as f:
            existing = json.load(f)
        print(f"  {year}: Already downloaded ({len(existing):,} records) -> {raw_file.name}")
        return raw_file

    print(f"  {year}: Downloading from data.medicaid.gov (dataset {dataset_id})...")

    all_records = []
    offset = 0
    total_count = None

    while True:
        url = f"{API_BASE}/{dataset_id}/0?limit={PAGE_LIMIT}&offset={offset}"
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'Aradune-DataLake/1.0')
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f"    ERROR at offset {offset}: {e}")
            if all_records:
                print(f"    Saving {len(all_records)} records collected so far...")
                break
            raise

        results = data.get("results", [])
        if total_count is None:
            total_count = data.get("count", 0)
            print(f"    Total records expected: {total_count:,}")

        if not results:
            break

        all_records.extend(results)
        offset += PAGE_LIMIT

        if offset % 2000 == 0 or len(all_records) >= total_count:
            print(f"    Downloaded {len(all_records):,}/{total_count:,} records...")

        if len(all_records) >= total_count:
            break

        # Brief pause between requests
        time.sleep(0.3)

    print(f"    Total downloaded: {len(all_records):,} records")

    # Save raw JSON
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with open(raw_file, "w") as f:
        json.dump(all_records, f)

    size_kb = raw_file.stat().st_size / 1024
    print(f"    Saved: {raw_file.name} ({size_kb:.0f} KB)")

    return raw_file


def normalize_dataframe(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Normalize a raw quality core set dataframe to match the existing schema.

    Target schema (from quality_core_set_2023/2024):
      state_code, domain, reporting_program, measure_name, measure_id,
      measure_type, core_set_year, population, methodology,
      state_rate, states_reporting, median_rate, bottom_quartile, top_quartile,
      source, rate_in_calculation, snapshot_date
    """
    # Clean column names
    df.columns = [clean_col(c) for c in df.columns]

    # Handle schema variations across years
    # 2017: state_specific_comments, location, no measure_type, no rate_used_in_*
    # 2018-2019: state_specific_comments
    # 2020-2022: statespecific_comments

    # Rename to canonical schema
    rename_map = {
        'state': 'state_name',
        'measure_abbreviation': 'measure_id',
        'ffy': 'core_set_year',
        'number_of_states_reporting': 'states_reporting',
        'median': 'median_rate',
        'rate_used_in_calculating_state_mean_and_median': 'rate_in_calculation',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Normalize state comments field name variations
    for variant in ['statespecific_comments', 'state_specific_comments']:
        if variant in df.columns:
            df = df.rename(columns={variant: 'state_comments'})
            break

    # Add state_code from state name
    df['state_code'] = df['state_name'].map(STATE_CODES)
    unmapped = df[df['state_code'].isna()]['state_name'].unique()
    if len(unmapped) > 0:
        print(f"    WARNING: Unmapped states: {unmapped}")

    # Convert core_set_year
    df['core_set_year'] = pd.to_numeric(df['core_set_year'], errors='coerce').astype('Int32')

    # Convert numeric columns
    # state_rate can have 'DS', '#', 'NR', 'NM', etc. -- keep as numeric, non-numeric -> NaN
    df['state_rate'] = pd.to_numeric(df['state_rate'], errors='coerce')
    df['states_reporting'] = pd.to_numeric(df['states_reporting'], errors='coerce').astype('Int32')
    df['median_rate'] = pd.to_numeric(df['median_rate'], errors='coerce')
    df['bottom_quartile'] = pd.to_numeric(df['bottom_quartile'], errors='coerce')
    df['top_quartile'] = pd.to_numeric(df['top_quartile'], errors='coerce')

    # Ensure measure_type exists (2017 may not have it)
    if 'measure_type' not in df.columns:
        df['measure_type'] = None

    # Normalize rate_in_calculation to lowercase boolean string
    if 'rate_in_calculation' in df.columns:
        df['rate_in_calculation'] = df['rate_in_calculation'].str.strip().str.lower()
        # Map Yes/No to true/false for consistency with 2023/2024
        df['rate_in_calculation'] = df['rate_in_calculation'].map(
            {'yes': 'true', 'no': 'false'}
        ).fillna(df['rate_in_calculation'])
    else:
        df['rate_in_calculation'] = None

    # Add metadata
    df['source'] = f'data.medicaid.gov/quality-core-set-{year}'
    df['snapshot_date'] = pd.to_datetime(SNAPSHOT_DATE).date()

    # Drop columns not in the canonical schema
    # Keep: state_code, domain, reporting_program, measure_name, measure_id,
    #        measure_type, core_set_year, population, methodology,
    #        state_rate, states_reporting, median_rate, bottom_quartile, top_quartile,
    #        source, rate_in_calculation, snapshot_date
    canonical_cols = [
        'state_code', 'domain', 'reporting_program', 'measure_name', 'measure_id',
        'measure_type', 'core_set_year', 'population', 'methodology',
        'state_rate', 'states_reporting', 'median_rate', 'bottom_quartile', 'top_quartile',
        'source', 'rate_in_calculation', 'snapshot_date',
    ]
    final_cols = [c for c in canonical_cols if c in df.columns]
    df = df[final_cols]

    # Strip whitespace from string columns (exclude non-string types like date)
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        df[col] = df[col].astype(str).where(df[col].notna(), None)
        if df[col].notna().any():
            df[col] = df[col].str.strip()

    # Filter out rows with no state_code (unmapped territories, etc.)
    before = len(df)
    df = df[df['state_code'].notna()].copy()
    if len(df) < before:
        print(f"    Filtered {before - len(df)} rows with unmapped state names")

    return df


def ingest_year(year: int) -> pd.DataFrame:
    """Load raw JSON and normalize to canonical schema."""
    raw_file = RAW_DIR / f"quality_core_set_{year}.json"

    if not raw_file.exists():
        print(f"  {year}: Raw file not found: {raw_file.name}")
        return pd.DataFrame()

    with open(raw_file) as f:
        records = json.load(f)

    print(f"  {year}: Loaded {len(records):,} raw records")
    df = pd.DataFrame(records)
    df = normalize_dataframe(df, year)

    # Write per-year parquet
    out_dir = FACT_DIR / f"quality_core_set_{year}" / f"snapshot={SNAPSHOT_DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    # Ensure snapshot_date is proper date type for parquet
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date
    table = pa.Table.from_pandas(df, preserve_index=False)
    # Cast snapshot_date to date32 for consistency with existing tables
    idx = table.schema.get_field_index('snapshot_date')
    if idx >= 0 and table.schema.field(idx).type != pa.date32():
        table = table.set_column(idx, 'snapshot_date',
                                 table.column('snapshot_date').cast(pa.date32()))
    pq.write_table(table, out_path, compression='zstd')

    size_kb = out_path.stat().st_size / 1024
    states = df['state_code'].nunique()
    measures = df['measure_id'].nunique()
    programs = df['reporting_program'].unique().tolist()
    print(f"    -> {out_path.relative_to(PROJECT_ROOT)}")
    print(f"       {len(df):,} rows, {states} states, {measures} measures, programs: {programs}")
    print(f"       Size: {size_kb:.0f} KB")

    return df


def build_combined(all_dfs: list[pd.DataFrame]) -> int:
    """Build the combined table from all years (2017-2024)."""
    # Also include existing 2023 and 2024 data
    for year in [2023, 2024]:
        try:
            existing_path = FACT_DIR / f"quality_core_set_{year}"
            t = pq.read_table(str(existing_path))
            edf = t.to_pandas()
            # Drop partition column if present
            if 'snapshot' in edf.columns:
                edf = edf.drop(columns=['snapshot'])
            print(f"  Including existing quality_core_set_{year}: {len(edf):,} rows")
            all_dfs.append(edf)
        except Exception as e:
            print(f"  WARNING: Could not read quality_core_set_{year}: {e}")

    if not all_dfs:
        print("  No data to combine!")
        return 0

    # Ensure snapshot_date is consistently date type across all dataframes
    for i, df in enumerate(all_dfs):
        if 'snapshot_date' in df.columns:
            all_dfs[i]['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date

    combined = pd.concat(all_dfs, ignore_index=True)

    # Ensure snapshot_date is date type after concat
    if 'snapshot_date' in combined.columns:
        combined['snapshot_date'] = pd.to_datetime(combined['snapshot_date']).dt.date

    # Sort for consistent ordering
    combined = combined.sort_values(
        ['core_set_year', 'state_code', 'reporting_program', 'domain', 'measure_id', 'population'],
        na_position='last'
    ).reset_index(drop=True)

    # Write combined parquet
    out_dir = FACT_DIR / "quality_core_set_combined" / f"snapshot={SNAPSHOT_DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    table = pa.Table.from_pandas(combined, preserve_index=False)
    pq.write_table(table, out_path, compression='zstd')

    size_mb = out_path.stat().st_size / (1024 * 1024)
    years = sorted(combined['core_set_year'].dropna().unique().tolist())
    states = combined['state_code'].nunique()
    measures = combined['measure_id'].nunique()
    print(f"\n  Combined table:")
    print(f"    -> {out_path.relative_to(PROJECT_ROOT)}")
    print(f"       {len(combined):,} rows, years: {years}")
    print(f"       {states} states, {measures} unique measures")
    print(f"       Size: {size_mb:.2f} MB")

    # Year breakdown
    print(f"\n    Year breakdown:")
    for yr, grp in combined.groupby('core_set_year'):
        print(f"      {yr}: {len(grp):,} rows, {grp['state_code'].nunique()} states, {grp['measure_id'].nunique()} measures")

    return len(combined)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Download and ingest Quality Core Set historical data")
    parser.add_argument("--download-only", action="store_true", help="Only download raw data")
    parser.add_argument("--ingest-only", action="store_true", help="Only ingest from existing raw files")
    args = parser.parse_args()

    print("=" * 70)
    print("QUALITY CORE SET HISTORICAL DATA INGESTION (2017-2022)")
    print("=" * 70)
    print(f"Snapshot: {SNAPSHOT_DATE}")
    print()

    # Step 1: Download
    if not args.ingest_only:
        print("--- STEP 1: Download from data.medicaid.gov API ---")
        for year, dataset_id in sorted(DATASETS.items()):
            download_dataset(year, dataset_id)
        print()

    if args.download_only:
        print("Download complete. Run with --ingest-only to process.")
        return

    # Step 2: Ingest per-year
    print("--- STEP 2: Normalize and write per-year parquet ---")
    year_dfs = []
    year_counts = {}
    for year in sorted(DATASETS.keys()):
        df = ingest_year(year)
        if not df.empty:
            year_dfs.append(df)
            year_counts[year] = len(df)
        print()

    # Step 3: Build combined
    print("--- STEP 3: Build combined table (all years 2017-2024) ---")
    combined_count = build_combined(year_dfs)

    # Summary
    print()
    print("=" * 70)
    print("QUALITY CORE SET HISTORICAL INGESTION COMPLETE")
    print("=" * 70)
    print()
    print("  Per-year tables created:")
    for year, count in sorted(year_counts.items()):
        print(f"    quality_core_set_{year}: {count:,} rows")
    if combined_count > 0:
        print(f"\n  Combined table: quality_core_set_combined: {combined_count:,} rows")
    print(f"\n  Total new rows: {sum(year_counts.values()):,}")
    print(f"  Total combined rows (with 2023+2024): {combined_count:,}")
    print()
    print("  Data sources:")
    for year, dataset_id in sorted(DATASETS.items()):
        print(f"    {year}: https://data.medicaid.gov/dataset/{dataset_id}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
