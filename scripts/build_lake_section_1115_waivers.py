#!/usr/bin/env python3
"""
build_lake_section_1115_waivers.py — Ingest Section 1115 and other Medicaid waivers.

Source: https://www.medicaid.gov/medicaid/section-1115-demo/demonstration-and-waiver-list
Reads pre-scraped raw JSON (665 waivers across all states).

Tables built:
  fact_section_1115_waivers — All Medicaid demonstration waivers by state.

Usage:
  python3 scripts/build_lake_section_1115_waivers.py
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

STATE_CODES = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "American Samoa": "AS", "Guam": "GU", "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR", "Virgin Islands": "VI", "U.S. Virgin Islands": "VI",
}

# Reverse lookup
CODE_TO_NAME = {v: k for k, v in STATE_CODES.items()}


def parse_date(raw):
    if not raw or str(raw).strip() in ("", "None", "N/A", "No Approval Date", "No Effective Date"):
        return None
    raw = str(raw).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def classify_authority(authority):
    if not authority:
        return "unknown"
    auth = str(authority).lower()
    if "1115" in auth:
        return "1115"
    if "1915" in auth and "c" in auth and "b" not in auth:
        return "1915c"
    if "1915" in auth and "b" in auth and "c" not in auth:
        return "1915b"
    if "1915" in auth and "b" in auth and "c" in auth:
        return "1915b_1915c"
    if "1915" in auth:
        return "1915_other"
    return "other"


def write_parquet(con, table, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)")
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    return count


def main():
    print("=" * 60)
    print("Section 1115 & Medicaid Waivers Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    raw_path = RAW_DIR / "section_1115_waivers.json"
    waivers = json.loads(raw_path.read_text())
    print(f"\n  Loaded {len(waivers):,} waivers from {raw_path.name}")

    # Enrich
    for w in waivers:
        sn = w.get("state_name", "")
        sc = w.get("state_code", "")
        if not sc and sn:
            w["state_code"] = STATE_CODES.get(sn)
        if not sn and sc:
            w["state_name"] = CODE_TO_NAME.get(sc, "")
        w["approval_date"] = parse_date(w.get("approval_date") or w.get("approval_date_raw"))
        w["effective_date"] = parse_date(w.get("effective_date") or w.get("effective_date_raw"))
        w["authority_type"] = classify_authority(w.get("authority"))
        for k in ("approval_date_raw", "effective_date_raw"):
            w.pop(k, None)

    con = duckdb.connect()
    df = pd.DataFrame(waivers)
    df["source"] = "https://www.medicaid.gov/medicaid/section-1115-demo/demonstration-and-waiver-list"
    df["snapshot_date"] = SNAPSHOT_DATE
    for col in ("approval_date", "effective_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    con.execute("CREATE TABLE fact_section_1115_waivers AS SELECT * FROM df")

    by_type = con.execute("SELECT authority_type, COUNT(*) n FROM fact_section_1115_waivers GROUP BY 1 ORDER BY n DESC").fetchall()
    print("\n  By authority type:")
    for t, n in by_type:
        print(f"    {t}: {n:,}")

    by_status = con.execute("SELECT status, COUNT(*) n FROM fact_section_1115_waivers GROUP BY 1 ORDER BY n DESC").fetchall()
    print("\n  By status:")
    for s, n in by_status:
        print(f"    {s}: {n:,}")

    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_section_1115_waivers WHERE state_code IS NOT NULL").fetchone()[0]
    print(f"\n  States covered: {states}")

    out_path = FACT_DIR / "section_1115_waivers" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_section_1115_waivers", out_path)

    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID, "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_section_1115_waivers.py",
        "source": "https://www.medicaid.gov/medicaid/section-1115-demo/demonstration-and-waiver-list",
        "tables": {"fact_section_1115_waivers": {"rows": row_count, "path": f"fact/section_1115_waivers/snapshot={SNAPSHOT_DATE}/data.parquet"}},
        "completed_at": datetime.now().isoformat() + "Z",
    }
    (META_DIR / f"manifest_section_1115_waivers_{SNAPSHOT_DATE}.json").write_text(json.dumps(manifest, indent=2))
    con.close()

    print("\n" + "=" * 60)
    print("SECTION 1115 WAIVERS INGESTION COMPLETE")
    print(f"  fact_section_1115_waivers: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
