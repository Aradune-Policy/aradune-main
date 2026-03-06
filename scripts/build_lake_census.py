#!/usr/bin/env python3
"""
build_lake_census.py — Ingest Census ACS demographic data into the lake.

Reads from: data/raw/acs_poverty_state.json
             data/raw/acs_insurance_state.json
             data/raw/acs_demographics_state.json
             data/raw/acs_medicaid_disability.json
             data/raw/acs_child_poverty.json
Writes to:  data/lake/fact/acs_state/snapshot=YYYY-MM-DD/data.parquet

Tables built:
  fact_acs_state — Census ACS state-level demographics (poverty, insurance, population, disability)

Usage:
  python3 scripts/build_lake_census.py
  python3 scripts/build_lake_census.py --dry-run
"""

import argparse
import csv
import json
import tempfile
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

# FIPS to state code mapping
FIPS_TO_STATE = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY', '72': 'PR',
}


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
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


def _try_float(val):
    if val is None or val == '' or val == 'null' or val == '-':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _try_int(val):
    f = _try_float(val)
    return int(f) if f is not None else None


def _load_acs_json(filename):
    """Load a Census API JSON response (header row + data rows)."""
    path = RAW_DIR / filename
    if not path.exists():
        return None, None
    data = json.load(open(path))
    header = data[0]
    rows = data[1:]
    return header, rows


def build_fact_acs_state(con, dry_run: bool) -> int:
    """Build unified ACS state-level demographics table."""
    print("Building fact_acs_state...")

    # Build a dict keyed by state FIPS
    state_data = {}

    # 1. Poverty data
    hdr, rows = _load_acs_json("acs_poverty_state.json")
    if hdr:
        for row in rows:
            fips = row[hdr.index('state')]
            sc = FIPS_TO_STATE.get(fips)
            if not sc:
                continue
            state_data.setdefault(sc, {})['state_name'] = row[hdr.index('NAME')]
            state_data[sc]['pct_poverty'] = _try_float(row[hdr.index('S1701_C03_001E')])
            state_data[sc]['total_pop_poverty_universe'] = _try_int(row[hdr.index('S1701_C01_001E')])
        print(f"  Poverty: {len(rows)} rows loaded")

    # 2. Insurance data
    hdr, rows = _load_acs_json("acs_insurance_state.json")
    if hdr:
        for row in rows:
            fips = row[hdr.index('state')]
            sc = FIPS_TO_STATE.get(fips)
            if not sc:
                continue
            state_data.setdefault(sc, {})
            state_data[sc]['pct_uninsured'] = _try_float(row[hdr.index('S2701_C05_001E')])
            state_data[sc]['total_pop_insurance_universe'] = _try_int(row[hdr.index('S2701_C01_001E')])
        print(f"  Insurance: {len(rows)} rows loaded")

    # 3. Demographics
    hdr, rows = _load_acs_json("acs_demographics_state.json")
    if hdr:
        for row in rows:
            fips = row[hdr.index('state')]
            sc = FIPS_TO_STATE.get(fips)
            if not sc:
                continue
            state_data.setdefault(sc, {})
            state_data[sc]['total_population'] = _try_int(row[hdr.index('B01003_001E')])
            state_data[sc]['male_population'] = _try_int(row[hdr.index('B01001_002E')])
            state_data[sc]['female_population'] = _try_int(row[hdr.index('B01001_026E')])
            state_data[sc]['white_alone'] = _try_int(row[hdr.index('B02001_002E')])
            state_data[sc]['black_alone'] = _try_int(row[hdr.index('B02001_003E')])
            state_data[sc]['hispanic_latino'] = _try_int(row[hdr.index('B03003_003E')])
        print(f"  Demographics: {len(rows)} rows loaded")

    # 4. Medicaid/Disability
    hdr, rows = _load_acs_json("acs_medicaid_disability.json")
    if hdr:
        for row in rows:
            fips = row[hdr.index('state')]
            sc = FIPS_TO_STATE.get(fips)
            if not sc:
                continue
            state_data.setdefault(sc, {})
            # B27010: health insurance type by age
            state_data[sc]['insured_under19'] = _try_int(row[hdr.index('B27010_002E')])
            state_data[sc]['insured_19_34'] = _try_int(row[hdr.index('B27010_017E')])
            state_data[sc]['insured_35_64'] = _try_int(row[hdr.index('B27010_033E')])
            state_data[sc]['insured_65plus'] = _try_int(row[hdr.index('B27010_050E')])
            # B18101: disability status by age
            state_data[sc]['civilian_noninst_pop'] = _try_int(row[hdr.index('B18101_001E')])
            state_data[sc]['disability_male_under5'] = _try_int(row[hdr.index('B18101_004E')])
            state_data[sc]['disability_male_5_17'] = _try_int(row[hdr.index('B18101_007E')])
        print(f"  Medicaid/Disability: {len(rows)} rows loaded")

    # 5. Child poverty
    hdr, rows = _load_acs_json("acs_child_poverty.json")
    if hdr:
        for row in rows:
            fips = row[hdr.index('state')]
            sc = FIPS_TO_STATE.get(fips)
            if not sc:
                continue
            state_data.setdefault(sc, {})
            state_data[sc]['children_in_households'] = _try_int(row[hdr.index('S0901_C01_001E')])
            state_data[sc]['pct_poverty_under18'] = _try_float(row[hdr.index('S1701_C03_002E')])
            state_data[sc]['pct_poverty_under5'] = _try_float(row[hdr.index('S1701_C03_003E')])
            state_data[sc]['pct_poverty_65plus'] = _try_float(row[hdr.index('S1701_C03_042E')])
        print(f"  Child Poverty: {len(rows)} rows loaded")

    if not state_data:
        print("  SKIPPED — no ACS data found")
        return 0

    # Convert to rows
    all_rows = []
    for sc, d in sorted(state_data.items()):
        all_rows.append({
            'state_code': sc,
            'state_name': d.get('state_name', ''),
            'total_population': d.get('total_population'),
            'male_population': d.get('male_population'),
            'female_population': d.get('female_population'),
            'white_alone': d.get('white_alone'),
            'black_alone': d.get('black_alone'),
            'hispanic_latino': d.get('hispanic_latino'),
            'pct_poverty': d.get('pct_poverty'),
            'pct_poverty_under18': d.get('pct_poverty_under18'),
            'pct_poverty_under5': d.get('pct_poverty_under5'),
            'pct_poverty_65plus': d.get('pct_poverty_65plus'),
            'pct_uninsured': d.get('pct_uninsured'),
            'children_in_households': d.get('children_in_households'),
            'insured_under19': d.get('insured_under19'),
            'insured_19_34': d.get('insured_19_34'),
            'insured_35_64': d.get('insured_35_64'),
            'insured_65plus': d.get('insured_65plus'),
            'civilian_noninst_pop': d.get('civilian_noninst_pop'),
            'source': 'api.census.gov/acs5/2023',
            'data_year': 2023,
            'snapshot_date': SNAPSHOT_DATE,
        })

    # Write to temp CSV for DuckDB
    tmp = Path(tempfile.mktemp(suffix='.csv'))
    fieldnames = list(all_rows[0].keys())
    with open(tmp, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_acs AS
        SELECT
            state_code, state_name,
            TRY_CAST(total_population AS BIGINT) AS total_population,
            TRY_CAST(male_population AS BIGINT) AS male_population,
            TRY_CAST(female_population AS BIGINT) AS female_population,
            TRY_CAST(white_alone AS BIGINT) AS white_alone,
            TRY_CAST(black_alone AS BIGINT) AS black_alone,
            TRY_CAST(hispanic_latino AS BIGINT) AS hispanic_latino,
            TRY_CAST(pct_poverty AS DOUBLE) AS pct_poverty,
            TRY_CAST(pct_poverty_under18 AS DOUBLE) AS pct_poverty_under18,
            TRY_CAST(pct_poverty_under5 AS DOUBLE) AS pct_poverty_under5,
            TRY_CAST(pct_poverty_65plus AS DOUBLE) AS pct_poverty_65plus,
            TRY_CAST(pct_uninsured AS DOUBLE) AS pct_uninsured,
            TRY_CAST(children_in_households AS BIGINT) AS children_in_households,
            TRY_CAST(insured_under19 AS BIGINT) AS insured_under19,
            TRY_CAST(insured_19_34 AS BIGINT) AS insured_19_34,
            TRY_CAST(insured_35_64 AS BIGINT) AS insured_35_64,
            TRY_CAST(insured_65plus AS BIGINT) AS insured_65plus,
            TRY_CAST(civilian_noninst_pop AS BIGINT) AS civilian_noninst_pop,
            source,
            TRY_CAST(data_year AS INTEGER) AS data_year,
            TRY_CAST(snapshot_date AS DATE) AS snapshot_date
        FROM read_csv_auto('{tmp}', header=true, all_varchar=true)
    """)
    tmp.unlink()

    count = write_parquet(con, "_fact_acs", _snapshot_path("acs_state"), dry_run)
    print(f"  {count} states with demographic data")
    con.execute("DROP TABLE IF EXISTS _fact_acs")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest Census ACS data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    count = build_fact_acs_state(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("CENSUS ACS LAKE INGESTION COMPLETE")
    print("=" * 60)
    status = "written" if not args.dry_run else "dry-run"
    print(f"  {'fact_acs_state':35s} {count:>12,} rows  [{status}]")

    if not args.dry_run and count > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {"fact_acs_state": {"rows": count}},
            "total_rows": count,
        }
        manifest_file = META_DIR / f"manifest_census_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
