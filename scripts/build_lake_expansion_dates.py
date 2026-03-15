#!/usr/bin/env python3
"""
build_lake_expansion_dates.py — Add Medicaid expansion dates to dim_state.

Reads a CSV/text file of expansion dates from KFF and updates dim_state.parquet
with expansion_date, expansion_status, and expansion_type columns.

Usage:
    python3 scripts/build_lake_expansion_dates.py

Input:  data/raw/policy/medicaid_expansion_dates.csv
Output: Updates data/lake/dimension/dim_state.parquet in place

CSV format (flexible — will try to auto-detect columns):
    State, Status, Date, Type
    e.g.: "Arizona, Adopted, 2014-01-01, ACA"
"""

import sys
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW = PROJECT_ROOT / "data" / "raw" / "policy"
LAKE = PROJECT_ROOT / "data" / "lake" / "dimension"
DIM_STATE = LAKE / "dim_state.parquet"

# Hardcoded expansion data from KFF (as of March 2026)
# This serves as fallback if no CSV is provided
EXPANSION_DATA = {
    # State: (date, type)  — date is effective date of expansion
    "AK": ("2015-09-01", "ACA"), "AZ": ("2014-01-01", "ACA"), "AR": ("2014-01-01", "1115"),
    "CA": ("2014-01-01", "ACA"), "CO": ("2014-01-01", "ACA"), "CT": ("2010-04-01", "ACA early"),
    "DE": ("2014-01-01", "ACA"), "DC": ("2010-07-01", "ACA early"), "HI": ("2014-01-01", "ACA"),
    "ID": ("2020-01-01", "Ballot"), "IL": ("2014-01-01", "ACA"), "IN": ("2015-02-01", "1115"),
    "IA": ("2014-01-01", "1115"), "KY": ("2014-01-01", "ACA"), "LA": ("2016-07-01", "ACA"),
    "ME": ("2019-01-10", "Ballot"), "MD": ("2014-01-01", "ACA"), "MA": ("2014-01-01", "ACA"),
    "MI": ("2014-04-01", "1115"), "MN": ("2014-01-01", "ACA early"), "MO": ("2021-10-01", "Ballot"),
    "MT": ("2016-01-01", "1115"), "NE": ("2020-10-01", "Ballot"), "NV": ("2014-01-01", "ACA"),
    "NH": ("2014-08-15", "1115"), "NJ": ("2014-01-01", "ACA"), "NM": ("2014-01-01", "ACA"),
    "NY": ("2014-01-01", "ACA"), "NC": ("2023-12-01", "ACA"), "ND": ("2014-01-01", "ACA"),
    "OH": ("2014-01-01", "ACA"), "OK": ("2021-07-01", "Ballot"), "OR": ("2014-01-01", "ACA"),
    "PA": ("2015-01-01", "ACA"), "RI": ("2014-01-01", "ACA"), "SD": ("2023-07-01", "Ballot"),
    "VA": ("2019-01-01", "ACA"), "VT": ("2014-01-01", "ACA"), "WA": ("2014-01-01", "ACA"),
    "WV": ("2014-01-01", "ACA"), "WI": (None, "Partial"),  # WI covers adults to 100% FPL, not 138%
}

# Non-expansion states (as of March 2026)
NON_EXPANSION = ["AL", "FL", "GA", "KS", "MS", "SC", "TN", "TX", "WY"]


def main():
    if not DIM_STATE.exists():
        print(f"ERROR: {DIM_STATE} not found")
        sys.exit(1)

    # Read current dim_state
    con = duckdb.connect()
    dim = con.execute(f"SELECT * FROM read_parquet('{DIM_STATE}')").fetchdf()
    print(f"Read dim_state: {len(dim)} rows, columns: {list(dim.columns)}")

    # Try to read CSV override
    csv_path = RAW / "medicaid_expansion_dates.csv"
    if csv_path.exists():
        print(f"Reading expansion dates from {csv_path}")
        csv_df = pd.read_csv(csv_path)
        # Auto-detect columns (flexible matching)
        col_map = {}
        for col in csv_df.columns:
            cl = col.lower().strip()
            if "state" in cl and "code" not in cl:
                col_map["state_name"] = col
            elif "code" in cl or "abbr" in cl:
                col_map["state_code"] = col
            elif "date" in cl or "effective" in cl:
                col_map["date"] = col
            elif "status" in cl or "adopted" in cl:
                col_map["status"] = col
            elif "type" in cl or "method" in cl:
                col_map["type"] = col
        print(f"  Detected columns: {col_map}")
        # TODO: merge CSV data into EXPANSION_DATA dict
        # For now, use the hardcoded data

    # Add expansion columns
    expansion_dates = []
    expansion_statuses = []
    expansion_types = []

    for _, row in dim.iterrows():
        sc = row["state_code"]
        if sc in EXPANSION_DATA:
            date_str, exp_type = EXPANSION_DATA[sc]
            expansion_dates.append(date_str)
            expansion_statuses.append("Expanded")
            expansion_types.append(exp_type)
        elif sc in NON_EXPANSION:
            expansion_dates.append(None)
            expansion_statuses.append("Not Expanded")
            expansion_types.append(None)
        else:
            # Territories or unknown
            expansion_dates.append(None)
            expansion_statuses.append("N/A")
            expansion_types.append(None)

    dim["expansion_date"] = expansion_dates
    dim["expansion_status"] = expansion_statuses
    dim["expansion_type"] = expansion_types

    # Count
    expanded = dim[dim["expansion_status"] == "Expanded"]
    not_expanded = dim[dim["expansion_status"] == "Not Expanded"]
    print(f"\nExpanded: {len(expanded)} states")
    print(f"Not Expanded: {len(not_expanded)} states")
    print(f"N/A (territories): {len(dim) - len(expanded) - len(not_expanded)}")

    # Write back
    con.execute(f"COPY (SELECT * FROM dim) TO '{DIM_STATE}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    print(f"\nUpdated {DIM_STATE}")
    print("Columns now:", list(dim.columns))

    con.close()


if __name__ == "__main__":
    main()
