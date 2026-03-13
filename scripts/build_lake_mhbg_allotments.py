#!/usr/bin/env python3
"""
build_lake_mhbg_allotments.py — Parse SAMHSA MHBG FY2023 allotments from HTML.

Source: data/raw/samhsa_mhbg_fy23_allotments.html
  (SAMHSA Mental Health Block Grant state/territory allotments page)

NOTE: The existing fact_block_grant table already contains MHBG FY2023 data
for 55 states/territories. This script creates a separate table with the
same data parsed directly from the SAMHSA HTML page, including territories
that may not be in the existing table. If the existing table already has
complete coverage, this table is redundant.

Table built:
  fact_mhbg_fy23_allotments — MHBG FY2023 allotments by state/territory

Usage:
  python3 scripts/build_lake_mhbg_allotments.py
"""

import re
import uuid
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

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
    "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Guam": "GU", "Virgin Islands": "VI",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
    "Northern Marianas": "MP",
    "Marshall Islands": "MH", "Micronesia": "FM", "Palau": "PW",
    "District Of Columbia": "DC",
}


def parse_html_table(html_path: Path) -> list[dict]:
    """Parse the MHBG allotments table from the SAMHSA HTML page."""
    with open(html_path) as f:
        content = f.read()

    tables = re.findall(r'<table[^>]*>(.*?)</table>', content, re.DOTALL)
    if not tables:
        raise ValueError("No <table> found in HTML")

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tables[0], re.DOTALL)
    records = []

    for row in rows[1:]:  # Skip header
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
        if len(cells) < 2:
            continue

        state_name = re.sub(r'<[^>]+>', '', cells[0]).strip()
        amount_str = re.sub(r'<[^>]+>', '', cells[1]).strip()

        # Skip totals or empty
        if not state_name or state_name.lower() in ('total', 'totals', 'grand total'):
            continue

        # Parse dollar amount: "$14,051,192" -> 14051192
        amount_clean = amount_str.replace('$', '').replace(',', '').strip()
        try:
            allotment = int(amount_clean)
        except (ValueError, TypeError):
            continue

        state_code = STATE_NAME_TO_CODE.get(state_name, "")

        records.append({
            "state_code": state_code,
            "state_name": state_name,
            "program": "MHBG",
            "fiscal_year": 2023,
            "allotment": allotment,
            "source": "SAMHSA MHBG FY2023 Final Allotments",
            "snapshot_date": SNAPSHOT_DATE,
        })

    return records


def main():
    print("=" * 60)
    print("SAMHSA MHBG FY2023 Allotments Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    html_path = RAW_DIR / "samhsa_mhbg_fy23_allotments.html"
    if not html_path.exists():
        print(f"  ERROR: {html_path} not found")
        return

    records = parse_html_table(html_path)
    print(f"  Parsed {len(records)} state/territory allotment records")

    if not records:
        print("  No records parsed. Exiting.")
        return

    df = pd.DataFrame(records)
    con = duckdb.connect()
    con.execute("CREATE TABLE fact_mhbg_fy23_allotments AS SELECT * FROM df")

    # Stats
    total = con.execute("SELECT SUM(allotment) FROM fact_mhbg_fy23_allotments").fetchone()[0]
    with_code = con.execute("SELECT COUNT(*) FROM fact_mhbg_fy23_allotments WHERE state_code != ''").fetchone()[0]
    print(f"  Total allotment: ${total:,.0f}")
    print(f"  Records with state_code: {with_code}")
    print(f"  Records without state_code: {len(records) - with_code}")

    # Top 5
    top5 = con.execute("""
        SELECT state_name, allotment
        FROM fact_mhbg_fy23_allotments
        ORDER BY allotment DESC LIMIT 5
    """).fetchall()
    print("\n  Top 5 allotments:")
    for name, amt in top5:
        print(f"    {name}: ${amt:,.0f}")

    # Write parquet
    out_path = FACT_DIR / "mhbg_fy23_allotments" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY fact_mhbg_fy23_allotments TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"\n  -> {out_path.relative_to(PROJECT_ROOT)} ({len(records)} rows, {size_mb:.3f} MB)")

    con.close()
    print("\n" + "=" * 60)
    print("MHBG FY2023 ALLOTMENTS INGESTION COMPLETE")
    print(f"  fact_mhbg_fy23_allotments: {len(records)} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
