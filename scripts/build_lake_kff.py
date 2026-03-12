#!/usr/bin/env python3
"""
build_lake_kff.py — Ingest KFF (Kaiser Family Foundation) Medicaid data into the lake.

KFF data is backed by Google Sheets. Download CSV via:
  https://docs.google.com/spreadsheets/d/{KEY}/export?format=csv

Tables built:
  kff_total_spending              — Total Medicaid spending by state
  kff_spending_per_enrollee       — Medicaid spending per enrollee by enrollment group
  kff_spending_per_full_enrollee  — Medicaid spending per full-benefit enrollee
  kff_spending_by_enrollment_group — Spending breakdown by enrollment group
  kff_spending_by_service         — Distribution of Medicaid spending by service category
  kff_spending_acute_care         — Acute care spending by state
  kff_spending_ltc                — Long-term care spending by state
  kff_federal_state_share         — Federal/state share of Medicaid spending
  kff_fmap                        — Federal Medical Assistance Percentage + multiplier
  kff_dsh_allotments              — Federal DSH allotments by state
  kff_fee_index                   — Medicaid-to-Medicare fee index by state
  kff_eligibility_adults          — Adult income eligibility limits (% FPL)
  kff_eligibility_parents_hist    — Parent income eligibility historical (2002-2025)
  kff_mc_penetration              — Managed care penetration rates by eligibility group
  kff_mc_plan_type_enrollment     — Enrollment by MC plan type (MCO, PCCM, PACE, Other)
  kff_mco_count                   — Total MCOs by state
  kff_mco_enrollment              — Total MCO enrollment by state
  kff_mco_spending                — Total MCO spending by state
  kff_mco_enrollment_by_plan      — MCO enrollment by individual plan + parent firm
  kff_mco_parent_financials       — MCO parent firm financial information
  kff_enrollees_by_group          — Distribution of enrollees by enrollment group
  kff_enrollees_by_race           — Enrollees by race/ethnicity (counts + percentages)
  kff_dual_eligible               — Number of dual-eligible individuals
  kff_dual_spending               — Medicaid spending per dual-eligible individual
  kff_births_medicaid             — Births financed by Medicaid (metro/nonmetro)
  kff_expansion_enrollment        — Medicaid expansion group enrollment (metadata)
  kff_expansion_spending          — Medicaid expansion spending (metadata)
  kff_chip_spending               — Total CHIP spending by state
  kff_chip_enhanced_fmap          — Enhanced CHIP FMAP by state
  kff_child_participation         — Medicaid/CHIP child participation rates

Usage:
  python3 scripts/build_lake_kff.py
  python3 scripts/build_lake_kff.py --dry-run
  python3 scripts/build_lake_kff.py --only kff_fee_index,kff_fmap
"""

import argparse
import csv
import io
import json
import os
import re
import subprocess
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "kff"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

STATE_NAME_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Puerto Rico': 'PR',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Virgin Islands': 'VI', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'Guam': 'GU', 'American Samoa': 'AS', 'Northern Mariana Islands': 'MP',
    'United States': 'US',
}

# Google Sheets keys for KFF datasets
KFF_SHEETS = {
    'total_spending':           '1sWIFxWvT4RMTd-N-01STIAAvKgICH9MgdoAsReiXFkk',
    'spending_per_enrollee':    '1CXA1QwHFgTngA4ikMJRmP7pe1qW2dYNbMLy4g4k7rvs',
    'spending_per_full_enrollee': '1ZoVzUrIBrb_1m8gTS6CTXbs2jfi7H2lFSuycFm4Q8cU',
    'spending_by_enrollment_group': '1XgU8gfuOoQoPJZuJ4cWDcZqDHQSkJQa0SeUWNt_eUzk',
    'spending_by_service':      '1xzo78T8_Par2-6qU_7DmMIgX0f3fbhLeD2dD0-eSUwc',
    'spending_acute_care':      '1LNKKz8JTDYbUzbezRt-Edrf5VaffLcWUrizqIXO0Shg',
    'spending_ltc':             '1KX-HUXzzVbdqt07LEJNAZ0x1DLyv5ZXtFN7e4-2delQ',
    'federal_state_share':      '1nnOqzbb1hhyCzrmLNfMvJDIPW1lC5o4CyuGNc8QwLMk',
    'fmap':                     '1woOCUjyucWbz5oY8z5w1MzPE2i0c0tpNWG4a2JEBSbQ',
    'dsh_allotments':           '1ldBqY_GSVjAN9P9pvqhFk8FygE_01sT0_oy0X3YHmgA',
    'fee_index':                '13HN-M0ip23XkIiLYZrZ3MMuBjp08Yte9WQEvTCLMit4',
    'eligibility_adults':       '1yqnXue0vO6QETZa97_Zgvqwyj3ucrDHb_KWXDYWuWGE',
    'eligibility_parents_hist': '1QbbD0L1HonQ9v8DTUnc36cDufH0_XxUKuatNWaltdqU',
    'mc_penetration':           '1oClQ7_Zq5w_g4YiKnXuqigFsUINKWdhYcbD-ekymRf8',
    'mc_plan_type_enrollment':  '1FJqSOFUCsXNXoEN4zFntDM5_BMvwWFiD53Rp1ec9UqU',
    'mco_count':                '1zlBoE7Mc2gEzoEnNs2_4HJBtWISVDF-nnJwPfhmBfqc',
    'mco_enrollment':           '1q8Te-8urbWXB3f7pQTGrfnQ3rflLHTSx6RNxAH4w30I',
    'mco_spending':             '1RBxePs2PhI7VdLc-DE8KdRZbaJaiQRUn3716Bbkud44',
    'mco_enrollment_by_plan':   '12Gwuv_gArFx5xPVWnPZNfz1La_aHvF6DSZgtl27YjpE',
    'mco_parent_financials':    '1sK53l-pki5EgPWdyak8oNOjx5001x0inkRSh687jdUg',
    'enrollees_by_group':       '14M01Tq8ad9dCRKbAU-0xAttjuZyq7_d9K6l8bQ1MU58',
    'enrollees_by_race':        '1DsHea6zJ1iEFb0DQF-AZUFmKtj4ASOsQzLrvkCsTiPs',
    'dual_eligible':            '1Hw1Sermfv1fOBw0y7kwWCfuKqIIMlS70B29F2YBbLqo',
    'dual_spending':            '1GhhEeZDC_ROeTz5OMwrq9JmrbL9IkTDSjjtocHROU9E',
    'births_medicaid':          '1tXDvh-seCngx4ziLgC49m3-63bV7UHkWifcee_Z7sts',
    'chip_spending':            '1FrV15Xf-SXWCjHisFCwKdeAe3kUSZNOYMDOIXG2Y968',
    'chip_enhanced_fmap':       '15ufYbgoiUvxanokG9SHBKKocx_MQcZr6EWG2jrFugMU',
    'child_participation':      '1pdqUHAyCIpWeSF_a95ON9mkv0wK_Le2arkiKMCF8D88',
    'monthly_enrollment':       '1K_rLRwctn_wM3iol4nPRsRQ-SxLNH45CP2RM_aF5UfM',
    'expansion_enrollment':     '1AGr8hDomn2ryg8sY8CF-EBQBviqvF53hnT_vj9d7ly4',
    'expansion_spending':       '13xtD31fJYwHctk79S3E_XaeSLPj8kvRyZ_6nVxpa4E8',
}


def download_csv(sheet_key: str, name: str) -> Path:
    """Download a Google Sheets CSV to raw directory. Returns path."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"{name}.csv"
    url = f"https://docs.google.com/spreadsheets/d/{sheet_key}/export?format=csv"
    result = subprocess.run(
        ['curl', '-sL', url],
        capture_output=True, timeout=60
    )
    if result.returncode != 0:
        print(f"  FAILED to download {name}: curl error {result.returncode}")
        return None
    content = result.stdout.decode('utf-8', errors='replace')
    # Check for HTML error pages
    if content.strip().startswith('<!DOCTYPE') or content.strip().startswith('<html'):
        print(f"  FAILED to download {name}: got HTML instead of CSV")
        return None
    # Check for metadata-only sheets (KFF time-series format)
    if content.startswith('Timeframe,Distribution,Data Format'):
        print(f"  SKIPPED {name}: metadata-only sheet (time-series interactive format)")
        return None
    with open(out_path, 'w') as f:
        f.write(content)
    lines = content.count('\n')
    print(f"  Downloaded {name}.csv ({lines} lines)")
    return out_path


def write_parquet(con, table_name: str, out_path: Path, dry_run: bool) -> int:
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = out_path.stat().st_size / 1024
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_kb:.1f} KB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")
    return count


def _snapshot_path(fact_name: str) -> Path:
    return FACT_DIR / fact_name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _clean_currency(val: str) -> float:
    """Parse KFF currency strings like '$918,687,295,218' to float."""
    if not val or val in ('N/A', '-', 'NR', 'NSD', '*'):
        return None
    val = val.strip().replace('$', '').replace(',', '').replace('"', '')
    try:
        return float(val)
    except ValueError:
        return None


def _clean_number(val: str) -> float:
    """Parse KFF number strings (may have commas, spaces)."""
    if not val or val.strip() in ('N/A', '-', 'NR', 'NSD', '*', ''):
        return None
    val = val.strip().replace(',', '').replace('"', '').replace(' ', '')
    try:
        return float(val)
    except ValueError:
        return None


def _clean_percent(val: str) -> float:
    """Parse KFF percent values (already decimal, e.g. 0.75)."""
    if not val or val.strip() in ('N/A', '-', 'NR', 'NSD', '*', ''):
        return None
    val = val.strip().replace(',', '').replace('"', '')
    try:
        return float(val)
    except ValueError:
        return None


def _state_code(name: str) -> str:
    """Map state name to 2-letter code. Strip footnote numbers."""
    if not name:
        return None
    name = name.strip()
    name = re.sub(r'\d+$', '', name).strip()
    return STATE_NAME_TO_CODE.get(name, None)


def _parse_kff_csv(csv_path: Path, skip_format_row: bool = True):
    """
    Parse KFF CSV. First column is state name, row 1 is headers,
    row 2 is format descriptors (Currency, Percent, Number, etc).
    Returns (headers, rows) where rows are lists of strings.
    """
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return [], []
    headers = rows[0]
    if skip_format_row and len(rows) > 1:
        # Row 2 is format descriptors like "Currency", "Percent", etc
        format_row = rows[1]
        data_rows = rows[2:]
    else:
        data_rows = rows[1:]
    return headers, data_rows


def _register_state_map(con):
    """Create a temp state name -> code mapping table."""
    try:
        con.execute("SELECT 1 FROM _state_map LIMIT 1")
        return
    except Exception:
        pass
    con.execute("CREATE TABLE _state_map (state_name VARCHAR, state_code VARCHAR)")
    con.executemany(
        "INSERT INTO _state_map VALUES (?, ?)",
        [(name, code) for name, code in STATE_NAME_TO_CODE.items()],
    )


# ---------------------------------------------------------------------------
# Dataset builders
# ---------------------------------------------------------------------------

def build_kff_total_spending(con, dry_run: bool) -> int:
    """Total Medicaid spending by state."""
    print("Building kff_total_spending...")
    csv_path = download_csv(KFF_SHEETS['total_spending'], 'total_spending')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        state_name = row[0].strip() if row else ''
        sc = _state_code(state_name)
        if not sc:
            continue
        total = _clean_currency(row[1]) if len(row) > 1 else None
        if total is not None:
            records.append((sc, state_name if sc != 'US' else 'United States', total))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_total_spending (state_code VARCHAR, state_name VARCHAR, total_spending DOUBLE)")
    con.executemany("INSERT INTO _kff_total_spending VALUES (?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_total_spending_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_total_spending
    """)
    count = write_parquet(con, "_kff_total_spending_final", _snapshot_path("kff_total_spending"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_total_spending; DROP TABLE IF EXISTS _kff_total_spending_final")
    return count


def build_kff_spending_per_enrollee(con, dry_run: bool) -> int:
    """Medicaid spending per enrollee by enrollment group."""
    print("Building kff_spending_per_enrollee...")
    csv_path = download_csv(KFF_SHEETS['spending_per_enrollee'], 'spending_per_enrollee')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,All Full or Partial Benefit Enrollees,Seniors,People with Disabilities,Adults,Children,ACA Expansion Adults
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_currency(row[1]) if len(row) > 1 else None,  # all enrollees
            _clean_currency(row[2]) if len(row) > 2 else None,  # seniors
            _clean_currency(row[3]) if len(row) > 3 else None,  # disabled
            _clean_currency(row[4]) if len(row) > 4 else None,  # adults
            _clean_currency(row[5]) if len(row) > 5 else None,  # children
            _clean_currency(row[6]) if len(row) > 6 else None,  # expansion
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_spend_pe (
        state_code VARCHAR, all_enrollees DOUBLE, seniors DOUBLE,
        disabled DOUBLE, adults DOUBLE, children DOUBLE, expansion_adults DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_spend_pe VALUES (?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_spend_pe_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_spend_pe
    """)
    count = write_parquet(con, "_kff_spend_pe_final", _snapshot_path("kff_spending_per_enrollee"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_spend_pe; DROP TABLE IF EXISTS _kff_spend_pe_final")
    return count


def build_kff_spending_per_full_enrollee(con, dry_run: bool) -> int:
    """Medicaid spending per full-benefit enrollee."""
    print("Building kff_spending_per_full_enrollee...")
    csv_path = download_csv(KFF_SHEETS['spending_per_full_enrollee'], 'spending_per_full_enrollee')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_currency(row[1]) if len(row) > 1 else None,
            _clean_currency(row[2]) if len(row) > 2 else None,
            _clean_currency(row[3]) if len(row) > 3 else None,
            _clean_currency(row[4]) if len(row) > 4 else None,
            _clean_currency(row[5]) if len(row) > 5 else None,
            _clean_currency(row[6]) if len(row) > 6 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_spend_full (
        state_code VARCHAR, all_full_enrollees DOUBLE, seniors DOUBLE,
        disabled DOUBLE, adults DOUBLE, children DOUBLE, expansion_adults DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_spend_full VALUES (?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_spend_full_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_spend_full
    """)
    count = write_parquet(con, "_kff_spend_full_final", _snapshot_path("kff_spending_per_full_enrollee"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_spend_full; DROP TABLE IF EXISTS _kff_spend_full_final")
    return count


def build_kff_spending_by_enrollment_group(con, dry_run: bool) -> int:
    """Spending breakdown by enrollment group."""
    print("Building kff_spending_by_enrollment_group...")
    csv_path = download_csv(KFF_SHEETS['spending_by_enrollment_group'], 'spending_by_enrollment_group')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        vals = [_clean_currency(row[i]) if len(row) > i else None for i in range(1, len(row))]
        records.append(tuple([sc] + vals))
    if not records:
        print("  No data parsed")
        return 0
    # Dynamic columns based on headers
    col_names = [h.strip() for h in headers[1:]]
    col_defs = ", ".join([f'col_{i} DOUBLE' for i in range(len(col_names))])
    con.execute(f"CREATE OR REPLACE TABLE _kff_spend_group (state_code VARCHAR, {col_defs})")
    placeholders = ", ".join(["?"] * (1 + len(col_names)))
    # Pad records to match column count
    n_cols = 1 + len(col_names)
    padded = [r + (None,) * (n_cols - len(r)) if len(r) < n_cols else r[:n_cols] for r in records]
    con.executemany(f"INSERT INTO _kff_spend_group VALUES ({placeholders})", padded)

    # Rename columns to meaningful names
    rename_map = {}
    for i, name in enumerate(col_names):
        clean = name.lower().strip().replace(' ', '_').replace('-', '_').replace('/', '_')
        clean = re.sub(r'[^a-z0-9_]', '', clean)
        if not clean:
            clean = f'col_{i}'
        rename_map[f'col_{i}'] = clean

    select_parts = ['state_code'] + [f'col_{i} AS {rename_map[f"col_{i}"]}' for i in range(len(col_names))]
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_spend_group_final AS
        SELECT {', '.join(select_parts)}, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_spend_group
    """)
    count = write_parquet(con, "_kff_spend_group_final", _snapshot_path("kff_spending_by_enrollment_group"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_spend_group; DROP TABLE IF EXISTS _kff_spend_group_final")
    return count


def build_kff_spending_by_service(con, dry_run: bool) -> int:
    """Distribution of Medicaid spending by service category."""
    print("Building kff_spending_by_service...")
    csv_path = download_csv(KFF_SHEETS['spending_by_service'], 'spending_by_service')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,FFS Acute,FFS LTC,MC & Health Plans,Medicare Payments,DSH,Total,
    #          FFS Acute %,FFS LTC %,MC %,Medicare %,DSH %,Total %
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_currency(row[1]) if len(row) > 1 else None,   # ffs_acute
            _clean_currency(row[2]) if len(row) > 2 else None,   # ffs_ltc
            _clean_currency(row[3]) if len(row) > 3 else None,   # mc_health_plans
            _clean_currency(row[4]) if len(row) > 4 else None,   # medicare_payments
            _clean_currency(row[5]) if len(row) > 5 else None,   # dsh
            _clean_currency(row[6]) if len(row) > 6 else None,   # total
            _clean_percent(row[7]) if len(row) > 7 else None,    # pct_ffs_acute
            _clean_percent(row[8]) if len(row) > 8 else None,    # pct_ffs_ltc
            _clean_percent(row[9]) if len(row) > 9 else None,    # pct_mc
            _clean_percent(row[10]) if len(row) > 10 else None,  # pct_medicare
            _clean_percent(row[11]) if len(row) > 11 else None,  # pct_dsh
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_svc (
        state_code VARCHAR,
        ffs_acute_care DOUBLE, ffs_long_term_care DOUBLE, managed_care DOUBLE,
        medicare_payments DOUBLE, dsh_payments DOUBLE, total_spending DOUBLE,
        pct_ffs_acute DOUBLE, pct_ffs_ltc DOUBLE, pct_managed_care DOUBLE,
        pct_medicare DOUBLE, pct_dsh DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_svc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_svc_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_svc
    """)
    count = write_parquet(con, "_kff_svc_final", _snapshot_path("kff_spending_by_service"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_svc; DROP TABLE IF EXISTS _kff_svc_final")
    return count


def build_kff_spending_acute_care(con, dry_run: bool) -> int:
    """Acute care spending by state."""
    print("Building kff_spending_acute_care...")
    csv_path = download_csv(KFF_SHEETS['spending_acute_care'], 'spending_acute_care')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_acute (state_code VARCHAR, acute_care_spending DOUBLE)")
    con.executemany("INSERT INTO _kff_acute VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_acute_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_acute
    """)
    count = write_parquet(con, "_kff_acute_final", _snapshot_path("kff_spending_acute_care"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_acute; DROP TABLE IF EXISTS _kff_acute_final")
    return count


def build_kff_spending_ltc(con, dry_run: bool) -> int:
    """Long-term care spending by state."""
    print("Building kff_spending_ltc...")
    csv_path = download_csv(KFF_SHEETS['spending_ltc'], 'spending_ltc')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_ltc (state_code VARCHAR, ltc_spending DOUBLE)")
    con.executemany("INSERT INTO _kff_ltc VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_ltc_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_ltc
    """)
    count = write_parquet(con, "_kff_ltc_final", _snapshot_path("kff_spending_ltc"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_ltc; DROP TABLE IF EXISTS _kff_ltc_final")
    return count


def build_kff_federal_state_share(con, dry_run: bool) -> int:
    """Federal/state share of Medicaid spending."""
    print("Building kff_federal_state_share...")
    csv_path = download_csv(KFF_SHEETS['federal_state_share'], 'federal_state_share')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_currency(row[1]) if len(row) > 1 else None,  # federal
            _clean_currency(row[2]) if len(row) > 2 else None,  # state
            _clean_currency(row[3]) if len(row) > 3 else None,  # total
            _clean_percent(row[4]) if len(row) > 4 else None,   # pct_federal
            _clean_percent(row[5]) if len(row) > 5 else None,   # pct_state
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_share (
        state_code VARCHAR, federal_spending DOUBLE, state_spending DOUBLE,
        total_spending DOUBLE, pct_federal DOUBLE, pct_state DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_share VALUES (?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_share_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_share
    """)
    count = write_parquet(con, "_kff_share_final", _snapshot_path("kff_federal_state_share"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_share; DROP TABLE IF EXISTS _kff_share_final")
    return count


def build_kff_fmap(con, dry_run: bool) -> int:
    """Federal Medical Assistance Percentage + multiplier."""
    print("Building kff_fmap...")
    csv_path = download_csv(KFF_SHEETS['fmap'], 'fmap')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_percent(row[1]) if len(row) > 1 else None,
            _clean_number(row[2]) if len(row) > 2 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_fmap (state_code VARCHAR, fmap_pct DOUBLE, multiplier DOUBLE)")
    con.executemany("INSERT INTO _kff_fmap VALUES (?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_fmap_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_fmap
    """)
    count = write_parquet(con, "_kff_fmap_final", _snapshot_path("kff_fmap"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_fmap; DROP TABLE IF EXISTS _kff_fmap_final")
    return count


def build_kff_dsh_allotments(con, dry_run: bool) -> int:
    """Federal DSH allotments by state."""
    print("Building kff_dsh_allotments...")
    csv_path = download_csv(KFF_SHEETS['dsh_allotments'], 'dsh_allotments')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_dsh (state_code VARCHAR, dsh_allotment DOUBLE)")
    con.executemany("INSERT INTO _kff_dsh VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_dsh_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_dsh
    """)
    count = write_parquet(con, "_kff_dsh_final", _snapshot_path("kff_dsh_allotments"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_dsh; DROP TABLE IF EXISTS _kff_dsh_final")
    return count


def build_kff_fee_index(con, dry_run: bool) -> int:
    """Medicaid-to-Medicare fee index by state and service type."""
    print("Building kff_fee_index...")
    csv_path = download_csv(KFF_SHEETS['fee_index'], 'fee_index')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,All Services,Primary Care,Obstetric Care,Other Services
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_number(row[1]) if len(row) > 1 else None,
            _clean_number(row[2]) if len(row) > 2 else None,
            _clean_number(row[3]) if len(row) > 3 else None,
            _clean_number(row[4]) if len(row) > 4 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_fee (
        state_code VARCHAR, all_services DOUBLE, primary_care DOUBLE,
        obstetric_care DOUBLE, other_services DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_fee VALUES (?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_fee_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_fee
    """)
    count = write_parquet(con, "_kff_fee_final", _snapshot_path("kff_fee_index"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_fee; DROP TABLE IF EXISTS _kff_fee_final")
    return count


def build_kff_eligibility_adults(con, dry_run: bool) -> int:
    """Adult income eligibility limits (% FPL)."""
    print("Building kff_eligibility_adults...")
    csv_path = download_csv(KFF_SHEETS['eligibility_adults'], 'eligibility_adults')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,Parents (in a family of three),Other Adults (for an individual)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_percent(row[1]) if len(row) > 1 else None,
            _clean_percent(row[2]) if len(row) > 2 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_elig (
        state_code VARCHAR, parent_fpl_pct DOUBLE, other_adult_fpl_pct DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_elig VALUES (?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_elig_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_elig
    """)
    count = write_parquet(con, "_kff_elig_final", _snapshot_path("kff_eligibility_adults"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_elig; DROP TABLE IF EXISTS _kff_elig_final")
    return count


def build_kff_eligibility_parents_hist(con, dry_run: bool) -> int:
    """Historical parent income eligibility limits (2002-2025), wide to long."""
    print("Building kff_eligibility_parents_hist...")
    csv_path = download_csv(KFF_SHEETS['eligibility_parents_hist'], 'eligibility_parents_hist')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,January 2002,April 2003,...,January 2025
    # Values are FPL percentages as decimals
    records = []
    date_cols = headers[1:]
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        for i, date_str in enumerate(date_cols):
            date_str = date_str.strip()
            if not date_str:
                continue
            val = _clean_percent(row[i + 1]) if len(row) > i + 1 else None
            if val is not None:
                # Parse "January 2002" etc
                try:
                    dt = datetime.strptime(date_str, '%B %Y')
                    records.append((sc, dt.strftime('%Y-%m-%d'), dt.year, val))
                except ValueError:
                    # Try "May 2024" format
                    try:
                        dt = datetime.strptime(date_str.strip(), '%B %Y')
                        records.append((sc, dt.strftime('%Y-%m-%d'), dt.year, val))
                    except ValueError:
                        continue
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_elig_hist (
        state_code VARCHAR, effective_date VARCHAR, year INTEGER, parent_fpl_pct DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_elig_hist VALUES (?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_elig_hist_final AS
        SELECT state_code, CAST(effective_date AS DATE) AS effective_date, year, parent_fpl_pct,
               'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_elig_hist
    """)
    count = write_parquet(con, "_kff_elig_hist_final", _snapshot_path("kff_eligibility_parents_hist"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _kff_elig_hist_final").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _kff_elig_hist_final").fetchone()
    print(f"  {count:,} rows, {states} states, {years[0]}-{years[1]}")
    con.execute("DROP TABLE IF EXISTS _kff_elig_hist; DROP TABLE IF EXISTS _kff_elig_hist_final")
    return count


def build_kff_mc_penetration(con, dry_run: bool) -> int:
    """Managed care penetration rates by eligibility group."""
    print("Building kff_mc_penetration...")
    csv_path = download_csv(KFF_SHEETS['mc_penetration'], 'mc_penetration')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,Children,Expansion Adult,Aged & Disabled,All Other Adults
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_percent(row[1]) if len(row) > 1 else None,
            _clean_percent(row[2]) if len(row) > 2 else None,
            _clean_percent(row[3]) if len(row) > 3 else None,
            _clean_percent(row[4]) if len(row) > 4 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_mc (
        state_code VARCHAR, children DOUBLE, expansion_adult DOUBLE,
        aged_disabled DOUBLE, other_adults DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_mc VALUES (?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mc_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mc
    """)
    count = write_parquet(con, "_kff_mc_final", _snapshot_path("kff_mc_penetration"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mc; DROP TABLE IF EXISTS _kff_mc_final")
    return count


def build_kff_mc_plan_type_enrollment(con, dry_run: bool) -> int:
    """Enrollment by MC plan type (MCO, PCCM, PACE, Other)."""
    print("Building kff_mc_plan_type_enrollment...")
    csv_path = download_csv(KFF_SHEETS['mc_plan_type_enrollment'], 'mc_plan_type_enrollment')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_number(row[1]) if len(row) > 1 else None,  # MCO
            _clean_number(row[2]) if len(row) > 2 else None,  # PCCM
            _clean_number(row[3]) if len(row) > 3 else None,  # PACE
            _clean_number(row[4]) if len(row) > 4 else None,  # Other
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_mctype (
        state_code VARCHAR, mco_enrollment DOUBLE, pccm_enrollment DOUBLE,
        pace_enrollment DOUBLE, other_enrollment DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_mctype VALUES (?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mctype_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mctype
    """)
    count = write_parquet(con, "_kff_mctype_final", _snapshot_path("kff_mc_plan_type_enrollment"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mctype; DROP TABLE IF EXISTS _kff_mctype_final")
    return count


def build_kff_mco_count(con, dry_run: bool) -> int:
    """Total MCOs by state."""
    print("Building kff_mco_count...")
    csv_path = download_csv(KFF_SHEETS['mco_count'], 'mco_count')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_number(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_mcocount (state_code VARCHAR, mco_count DOUBLE)")
    con.executemany("INSERT INTO _kff_mcocount VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mcocount_final AS
        SELECT state_code, CAST(mco_count AS INTEGER) AS mco_count,
               'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mcocount
    """)
    count = write_parquet(con, "_kff_mcocount_final", _snapshot_path("kff_mco_count"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mcocount; DROP TABLE IF EXISTS _kff_mcocount_final")
    return count


def build_kff_mco_enrollment(con, dry_run: bool) -> int:
    """Total MCO enrollment by state."""
    print("Building kff_mco_enrollment...")
    csv_path = download_csv(KFF_SHEETS['mco_enrollment'], 'mco_enrollment')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_number(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_mcoenr (state_code VARCHAR, mco_enrollment DOUBLE)")
    con.executemany("INSERT INTO _kff_mcoenr VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mcoenr_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mcoenr
    """)
    count = write_parquet(con, "_kff_mcoenr_final", _snapshot_path("kff_mco_enrollment"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mcoenr; DROP TABLE IF EXISTS _kff_mcoenr_final")
    return count


def build_kff_mco_spending(con, dry_run: bool) -> int:
    """Total MCO spending by state."""
    print("Building kff_mco_spending...")
    csv_path = download_csv(KFF_SHEETS['mco_spending'], 'mco_spending')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_mcosp (state_code VARCHAR, mco_spending DOUBLE)")
    con.executemany("INSERT INTO _kff_mcosp VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mcosp_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mcosp
    """)
    count = write_parquet(con, "_kff_mcosp_final", _snapshot_path("kff_mco_spending"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mcosp; DROP TABLE IF EXISTS _kff_mcosp_final")
    return count


def build_kff_mco_enrollment_by_plan(con, dry_run: bool) -> int:
    """MCO enrollment by individual plan + parent firm."""
    print("Building kff_mco_enrollment_by_plan...")
    csv_path = download_csv(KFF_SHEETS['mco_enrollment_by_plan'], 'mco_enrollment_by_plan')
    if not csv_path:
        return 0
    # This CSV has: state header rows, then plan rows
    # Format: State (All),N Plans,total_medicaid_only,total_dual,total_enrollment
    # Then: State,Plan Name,medicaid_only,dual,total,Parent Firm
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    current_state = None
    for row in rows:
        if not row or len(row) < 5:
            continue
        name_field = row[0].strip()
        # Check if this is a state aggregate row (contains "(All)")
        if '(All)' in name_field:
            state_name = name_field.replace('(All)', '').strip().rstrip(',')
            current_state = _state_code(state_name)
            continue
        # It's a plan row
        if current_state is None:
            # Try to detect state from the name field
            sc = _state_code(name_field)
            if sc:
                current_state = sc
        plan_name = row[1].strip() if len(row) > 1 else ''
        if not plan_name or plan_name in ('Text', 'Plan Name'):
            continue
        medicaid_only = _clean_number(row[2]) if len(row) > 2 else None
        dual = _clean_number(row[3]) if len(row) > 3 else None
        total = _clean_number(row[4]) if len(row) > 4 else None
        parent_firm = row[5].strip() if len(row) > 5 else None
        if parent_firm in ('', 'Text'):
            parent_firm = None
        if current_state and (medicaid_only is not None or total is not None):
            records.append((current_state, plan_name, medicaid_only, dual, total, parent_firm))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_mcoplan (
        state_code VARCHAR, plan_name VARCHAR, medicaid_only_enrollment DOUBLE,
        dual_enrollment DOUBLE, total_enrollment DOUBLE, parent_firm VARCHAR
    )""")
    con.executemany("INSERT INTO _kff_mcoplan VALUES (?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mcoplan_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mcoplan
    """)
    count = write_parquet(con, "_kff_mcoplan_final", _snapshot_path("kff_mco_enrollment_by_plan"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _kff_mcoplan_final").fetchone()[0]
    plans = con.execute("SELECT COUNT(DISTINCT plan_name) FROM _kff_mcoplan_final").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {plans} plans")
    con.execute("DROP TABLE IF EXISTS _kff_mcoplan; DROP TABLE IF EXISTS _kff_mcoplan_final")
    return count


def build_kff_mco_parent_financials(con, dry_run: bool) -> int:
    """MCO parent firm financial information."""
    print("Building kff_mco_parent_financials...")
    csv_path = download_csv(KFF_SHEETS['mco_parent_financials'], 'mco_parent_financials')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,Medicaid Rev 2023,Medicaid Rev 2024,% Change,As % Total Rev,
    #          Total Rev 2023,Total Rev 2024,% Change,Profits 2023,Profits 2024,% Change,Fortune 500
    records = []
    for row in rows:
        firm = row[0].strip() if row else ''
        if not firm or firm in ('', 'Notes, Sources, and Definitions'):
            continue
        records.append((
            firm,
            _clean_currency(row[1]) if len(row) > 1 else None,   # med_rev_2023
            _clean_currency(row[2]) if len(row) > 2 else None,   # med_rev_2024
            _clean_percent(row[3]) if len(row) > 3 else None,    # med_rev_change
            _clean_percent(row[4]) if len(row) > 4 else None,    # med_as_pct_total
            _clean_currency(row[5]) if len(row) > 5 else None,   # total_rev_2023
            _clean_currency(row[6]) if len(row) > 6 else None,   # total_rev_2024
            _clean_percent(row[7]) if len(row) > 7 else None,    # total_rev_change
            _clean_currency(row[8]) if len(row) > 8 else None,   # profits_2023
            _clean_currency(row[9]) if len(row) > 9 else None,   # profits_2024
            _clean_percent(row[10]) if len(row) > 10 else None,  # profits_change
            _clean_number(row[11]) if len(row) > 11 else None,   # fortune_500
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_mcofin (
        parent_firm VARCHAR,
        medicaid_rev_2023_millions DOUBLE, medicaid_rev_2024_millions DOUBLE,
        medicaid_rev_pct_change DOUBLE, medicaid_as_pct_total_rev DOUBLE,
        total_rev_2023_millions DOUBLE, total_rev_2024_millions DOUBLE,
        total_rev_pct_change DOUBLE,
        profits_2023_millions DOUBLE, profits_2024_millions DOUBLE,
        profits_pct_change DOUBLE, fortune_500_rank DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_mcofin VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_mcofin_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_mcofin
    """)
    count = write_parquet(con, "_kff_mcofin_final", _snapshot_path("kff_mco_parent_financials"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_mcofin; DROP TABLE IF EXISTS _kff_mcofin_final")
    return count


def build_kff_enrollees_by_group(con, dry_run: bool) -> int:
    """Distribution of enrollees by enrollment group."""
    print("Building kff_enrollees_by_group...")
    csv_path = download_csv(KFF_SHEETS['enrollees_by_group'], 'enrollees_by_group')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    # Parse columns dynamically
    col_names = [h.strip() for h in headers[1:] if h.strip()]
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        vals = []
        for i in range(1, len(col_names) + 1):
            v = _clean_number(row[i]) if len(row) > i else None
            vals.append(v)
        records.append(tuple([sc] + vals))
    if not records:
        print("  No data parsed")
        return 0
    n_cols = len(col_names)
    col_defs = ", ".join([f'col_{i} DOUBLE' for i in range(n_cols)])
    con.execute(f"CREATE OR REPLACE TABLE _kff_enrgroup (state_code VARCHAR, {col_defs})")
    placeholders = ", ".join(["?"] * (1 + n_cols))
    padded = [r + (None,) * (1 + n_cols - len(r)) if len(r) < 1 + n_cols else r[:1 + n_cols] for r in records]
    con.executemany(f"INSERT INTO _kff_enrgroup VALUES ({placeholders})", padded)

    # Create final with clean names
    rename_parts = ['state_code']
    for i, name in enumerate(col_names):
        clean = name.lower().strip().replace(' ', '_').replace('/', '_').replace('-', '_')
        clean = re.sub(r'[^a-z0-9_]', '', clean)
        if not clean:
            clean = f'group_{i}'
        rename_parts.append(f'col_{i} AS {clean}')
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_enrgroup_final AS
        SELECT {', '.join(rename_parts)}, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_enrgroup
    """)
    count = write_parquet(con, "_kff_enrgroup_final", _snapshot_path("kff_enrollees_by_group"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_enrgroup; DROP TABLE IF EXISTS _kff_enrgroup_final")
    return count


def build_kff_enrollees_by_race(con, dry_run: bool) -> int:
    """Enrollees by race/ethnicity (counts + percentages)."""
    print("Building kff_enrollees_by_race...")
    csv_path = download_csv(KFF_SHEETS['enrollees_by_race'], 'enrollees_by_race')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers have count columns then percentage columns
    # White,Hispanic,Black,Asian,AIAN,NHPI,Other,Unknown,Total (counts)
    # White,Hispanic,Black,Asian,AIAN,NHPI,Other,Unknown,Total (pcts)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_number(row[1]) if len(row) > 1 else None,   # white
            _clean_number(row[2]) if len(row) > 2 else None,   # hispanic
            _clean_number(row[3]) if len(row) > 3 else None,   # black
            _clean_number(row[4]) if len(row) > 4 else None,   # asian
            _clean_number(row[5]) if len(row) > 5 else None,   # aian
            _clean_number(row[6]) if len(row) > 6 else None,   # nhpi
            _clean_number(row[7]) if len(row) > 7 else None,   # other
            _clean_number(row[8]) if len(row) > 8 else None,   # unknown
            _clean_number(row[9]) if len(row) > 9 else None,   # total
            _clean_percent(row[10]) if len(row) > 10 else None, # pct_white
            _clean_percent(row[11]) if len(row) > 11 else None, # pct_hispanic
            _clean_percent(row[12]) if len(row) > 12 else None, # pct_black
            _clean_percent(row[13]) if len(row) > 13 else None, # pct_asian
            _clean_percent(row[14]) if len(row) > 14 else None, # pct_aian
            _clean_percent(row[15]) if len(row) > 15 else None, # pct_nhpi
            _clean_percent(row[16]) if len(row) > 16 else None, # pct_other
            _clean_percent(row[17]) if len(row) > 17 else None, # pct_unknown
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_race (
        state_code VARCHAR,
        white DOUBLE, hispanic DOUBLE, black DOUBLE, asian DOUBLE,
        aian DOUBLE, nhpi DOUBLE, other DOUBLE, unknown DOUBLE, total DOUBLE,
        pct_white DOUBLE, pct_hispanic DOUBLE, pct_black DOUBLE, pct_asian DOUBLE,
        pct_aian DOUBLE, pct_nhpi DOUBLE, pct_other DOUBLE, pct_unknown DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_race VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_race_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_race
    """)
    count = write_parquet(con, "_kff_race_final", _snapshot_path("kff_enrollees_by_race"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_race; DROP TABLE IF EXISTS _kff_race_final")
    return count


def build_kff_dual_eligible(con, dry_run: bool) -> int:
    """Number of dual-eligible individuals."""
    print("Building kff_dual_eligible...")
    csv_path = download_csv(KFF_SHEETS['dual_eligible'], 'dual_eligible')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    # Headers: ,Full-benefit,Partial-benefit,Total,Full-benefit %,Partial-benefit %,Total %
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_number(row[1]) if len(row) > 1 else None,
            _clean_number(row[2]) if len(row) > 2 else None,
            _clean_number(row[3]) if len(row) > 3 else None,
            _clean_percent(row[4]) if len(row) > 4 else None,
            _clean_percent(row[5]) if len(row) > 5 else None,
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_dual (
        state_code VARCHAR, full_benefit DOUBLE, partial_benefit DOUBLE,
        total DOUBLE, pct_full_benefit DOUBLE, pct_partial_benefit DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_dual VALUES (?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_dual_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_dual
    """)
    count = write_parquet(con, "_kff_dual_final", _snapshot_path("kff_dual_eligible"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_dual; DROP TABLE IF EXISTS _kff_dual_final")
    return count


def build_kff_dual_spending(con, dry_run: bool) -> int:
    """Medicaid spending per dual-eligible individual."""
    print("Building kff_dual_spending...")
    csv_path = download_csv(KFF_SHEETS['dual_spending'], 'dual_spending')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_dualsp (state_code VARCHAR, spending_per_dual DOUBLE)")
    con.executemany("INSERT INTO _kff_dualsp VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_dualsp_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_dualsp
    """)
    count = write_parquet(con, "_kff_dualsp_final", _snapshot_path("kff_dual_spending"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_dualsp; DROP TABLE IF EXISTS _kff_dualsp_final")
    return count


def build_kff_births_medicaid(con, dry_run: bool) -> int:
    """Births financed by Medicaid (metro/nonmetro)."""
    print("Building kff_births_medicaid...")
    csv_path = download_csv(KFF_SHEETS['births_medicaid'], 'births_medicaid')
    if not csv_path:
        return 0
    # This CSV has an extra format row AND a second header row for Number/Percent subtypes
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        all_rows = list(reader)
    # Row 0: top-level headers (repeated group names)
    # Row 1: sub-headers (Number, Percent of All Births, etc.)
    # Row 2: format row (Number--narrow, Percent, etc.)
    # Row 3+: data
    if len(all_rows) < 4:
        print("  Not enough rows")
        return 0
    data_rows = all_rows[3:]
    records = []
    for row in data_rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((
            sc,
            _clean_number(row[1]) if len(row) > 1 else None,   # all_births_count
            _clean_percent(row[2]) if len(row) > 2 else None,   # all_births_pct
            _clean_number(row[3]) if len(row) > 3 else None,   # metro_count
            _clean_percent(row[4]) if len(row) > 4 else None,   # metro_pct
            _clean_number(row[5]) if len(row) > 5 else None,   # nonmetro_count
            _clean_percent(row[6]) if len(row) > 6 else None,   # nonmetro_pct
        ))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("""CREATE OR REPLACE TABLE _kff_births (
        state_code VARCHAR,
        all_births_count DOUBLE, all_births_pct DOUBLE,
        metro_births_count DOUBLE, metro_births_pct DOUBLE,
        nonmetro_births_count DOUBLE, nonmetro_births_pct DOUBLE
    )""")
    con.executemany("INSERT INTO _kff_births VALUES (?, ?, ?, ?, ?, ?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_births_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_births
    """)
    count = write_parquet(con, "_kff_births_final", _snapshot_path("kff_births_medicaid"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_births; DROP TABLE IF EXISTS _kff_births_final")
    return count


def build_kff_chip_spending(con, dry_run: bool) -> int:
    """Total CHIP spending by state."""
    print("Building kff_chip_spending...")
    csv_path = download_csv(KFF_SHEETS['chip_spending'], 'chip_spending')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_currency(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_chip (state_code VARCHAR, chip_spending DOUBLE)")
    con.executemany("INSERT INTO _kff_chip VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_chip_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_chip
    """)
    count = write_parquet(con, "_kff_chip_final", _snapshot_path("kff_chip_spending"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_chip; DROP TABLE IF EXISTS _kff_chip_final")
    return count


def build_kff_chip_enhanced_fmap(con, dry_run: bool) -> int:
    """Enhanced CHIP FMAP by state."""
    print("Building kff_chip_enhanced_fmap...")
    csv_path = download_csv(KFF_SHEETS['chip_enhanced_fmap'], 'chip_enhanced_fmap')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        records.append((sc, _clean_percent(row[1]) if len(row) > 1 else None))
    if not records:
        print("  No data parsed")
        return 0
    con.execute("CREATE OR REPLACE TABLE _kff_chipfmap (state_code VARCHAR, enhanced_fmap DOUBLE)")
    con.executemany("INSERT INTO _kff_chipfmap VALUES (?, ?)", records)
    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_chipfmap_final AS
        SELECT *, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_chipfmap
    """)
    count = write_parquet(con, "_kff_chipfmap_final", _snapshot_path("kff_chip_enhanced_fmap"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_chipfmap; DROP TABLE IF EXISTS _kff_chipfmap_final")
    return count


def build_kff_child_participation(con, dry_run: bool) -> int:
    """Medicaid/CHIP child participation rates."""
    print("Building kff_child_participation...")
    csv_path = download_csv(KFF_SHEETS['child_participation'], 'child_participation')
    if not csv_path:
        return 0
    headers, rows = _parse_kff_csv(csv_path)
    records = []
    for row in rows:
        sc = _state_code(row[0]) if row else None
        if not sc:
            continue
        # May have multiple columns (rate, CI lower, CI upper)
        vals = [_clean_percent(row[i]) if len(row) > i else None for i in range(1, min(len(row), 6))]
        records.append(tuple([sc] + vals))
    if not records:
        print("  No data parsed")
        return 0

    max_cols = max(len(r) for r in records)
    col_defs = ", ".join([f'col_{i} DOUBLE' for i in range(max_cols - 1)])
    con.execute(f"CREATE OR REPLACE TABLE _kff_childpart (state_code VARCHAR, {col_defs})")
    placeholders = ", ".join(["?"] * max_cols)
    padded = [r + (None,) * (max_cols - len(r)) if len(r) < max_cols else r[:max_cols] for r in records]
    con.executemany(f"INSERT INTO _kff_childpart VALUES ({placeholders})", padded)

    # Name columns based on headers
    col_labels = [h.strip() for h in headers[1:max_cols]]
    rename_parts = ['state_code']
    for i in range(max_cols - 1):
        if i < len(col_labels) and col_labels[i]:
            clean = col_labels[i].lower().strip().replace(' ', '_').replace('-', '_').replace('/', '_')
            clean = re.sub(r'[^a-z0-9_]', '', clean)
        else:
            clean = f'value_{i}'
        rename_parts.append(f'col_{i} AS {clean}')

    con.execute(f"""
        CREATE OR REPLACE TABLE _kff_childpart_final AS
        SELECT {', '.join(rename_parts)}, 'kff.org' AS source, DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _kff_childpart
    """)
    count = write_parquet(con, "_kff_childpart_final", _snapshot_path("kff_child_participation"), dry_run)
    print(f"  {count:,} rows")
    con.execute("DROP TABLE IF EXISTS _kff_childpart; DROP TABLE IF EXISTS _kff_childpart_final")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "kff_total_spending": build_kff_total_spending,
    "kff_spending_per_enrollee": build_kff_spending_per_enrollee,
    "kff_spending_per_full_enrollee": build_kff_spending_per_full_enrollee,
    "kff_spending_by_enrollment_group": build_kff_spending_by_enrollment_group,
    "kff_spending_by_service": build_kff_spending_by_service,
    "kff_spending_acute_care": build_kff_spending_acute_care,
    "kff_spending_ltc": build_kff_spending_ltc,
    "kff_federal_state_share": build_kff_federal_state_share,
    "kff_fmap": build_kff_fmap,
    "kff_dsh_allotments": build_kff_dsh_allotments,
    "kff_fee_index": build_kff_fee_index,
    "kff_eligibility_adults": build_kff_eligibility_adults,
    "kff_eligibility_parents_hist": build_kff_eligibility_parents_hist,
    "kff_mc_penetration": build_kff_mc_penetration,
    "kff_mc_plan_type_enrollment": build_kff_mc_plan_type_enrollment,
    "kff_mco_count": build_kff_mco_count,
    "kff_mco_enrollment": build_kff_mco_enrollment,
    "kff_mco_spending": build_kff_mco_spending,
    "kff_mco_enrollment_by_plan": build_kff_mco_enrollment_by_plan,
    "kff_mco_parent_financials": build_kff_mco_parent_financials,
    "kff_enrollees_by_group": build_kff_enrollees_by_group,
    "kff_enrollees_by_race": build_kff_enrollees_by_race,
    "kff_dual_eligible": build_kff_dual_eligible,
    "kff_dual_spending": build_kff_dual_spending,
    "kff_births_medicaid": build_kff_births_medicaid,
    "kff_chip_spending": build_kff_chip_spending,
    "kff_chip_enhanced_fmap": build_kff_chip_enhanced_fmap,
    "kff_child_participation": build_kff_child_participation,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest KFF Medicaid data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None,
                        help="Comma-separated list of table names to build")
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {len(tables)} tables")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        if name not in ALL_TABLES:
            print(f"  UNKNOWN TABLE: {name}")
            continue
        totals[name] = ALL_TABLES[name](con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("KFF DATA LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>8,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>8,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source": "kff.org (Kaiser Family Foundation)",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_kff_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
