#!/usr/bin/env python3
"""
build_lake_eligibility_enrollment_snapshot.py — Ingest Medicaid/CHIP eligibility
and enrollment snapshot from CMS Medicaid.gov API JSON.

Source: data.medicaid.gov API (Performance Indicator dataset)
  API: https://data.medicaid.gov/api/1/datastore/query/{dataset_id}/0
  Paginates through all records using $offset parameter (5,000 per page).

This adds value beyond the existing medicaid_applications_v2 table by including:
  - Call center metrics (volume, wait time, abandonment rate)
  - Determination processing speed buckets (< 24h, 24h-8d, 8d-31d, 31d-90d, >90d)
  - Child enrollment breakdown
  - Separate Medicaid vs CHIP enrollment totals
  - Adult Medicaid enrollment

Table built:
  fact_eligibility_enrollment_snapshot — Enriched monthly eligibility/enrollment data

Usage:
  python3 scripts/build_lake_eligibility_enrollment_snapshot.py
"""

import json
import re
import time
import uuid
import urllib.request
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

# CMS Medicaid.gov API — Eligibility & Enrollment Performance Indicator dataset
DATASET_ID = "4876993c-bf50-5005-a8e5-020b47339d33"
API_BASE = "https://data.medicaid.gov/api/1/datastore/query"
PAGE_SIZE = 5000

# Column renaming: shorten CMS's verbose truncated column names to clean snake_case
COLUMN_MAP = {
    "state_abbreviation": "state_code",
    "state_name": "state_name",
    "reporting_period": "reporting_period",
    "state_expanded_medicaid": "expansion_state",
    "preliminary_or_updated": "data_status",
    "final_report": "final_report",
    "new_applications_submitted_to_medicaid_and_chip_agencies": "new_applications",
    "applications_for_financial_assistance_submitted_to_the_stat_104d": "marketplace_applications",
    "total_applications_for_financial_assistance_submitted_at_st_d6fa": "total_applications",
    "individuals_determined_eligible_for_medicaid_at_application": "medicaid_eligible_at_application",
    "individuals_determined_eligible_for_chip_at_application": "chip_eligible_at_application",
    "total_medicaid_and_chip_determinations": "total_determinations",
    "medicaid_and_chip_child_enrollment": "child_enrollment",
    "total_medicaid_and_chip_enrollment": "total_medicaid_chip_enrollment",
    "total_medicaid_enrollment": "total_medicaid_enrollment",
    "total_chip_enrollment": "total_chip_enrollment",
    "total_adult_medicaid_enrollment": "adult_medicaid_enrollment",
    "total_medicaid_and_chip_determinations_processed_in_less_th_1e84": "determinations_lt_24h",
    "total_medicaid_and_chip_determinations_processed_between_24_756e": "determinations_24h_to_8d",
    "total_medicaid_and_chip_determinations_processed_between_8__a7a5": "determinations_8d_to_31d",
    "total_medicaid_and_chip_determinations_processed_between_31_a42c": "determinations_31d_to_90d",
    "total_medicaid_and_chip_determinations_processed_in_more_th_a7ec": "determinations_gt_90d",
    "total_call_center_volume_number_of_calls": "call_center_volume",
    "average_call_center_wait_time_minutes": "avg_call_center_wait_minutes",
    "average_call_center_abandonment_rate": "call_center_abandonment_rate",
}

# Footnote columns to drop (contain text annotations, not data)
FOOTNOTE_SUFFIXES = [
    "_85d7", "_c640", "_9919", "_4f96", "_e28a",
    "_46c2", "_cf22", "_e6e5", "_49fa", "_bfce",
]


def clean_numeric(val):
    """Convert API values to numeric, handling nulls and text."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if s in ("", "N/A", "NR", "*", "**", "+"):
        return None
    # Remove dollar signs, commas
    s = re.sub(r'[\$,\s]', '', s)
    try:
        return float(s) if '.' in s else int(s)
    except (ValueError, TypeError):
        return None


def fetch_all_pages():
    """Download all records from data.medicaid.gov API with pagination."""
    all_results = []
    offset = 0
    total_available = None

    while True:
        url = f"{API_BASE}/{DATASET_ID}/0?limit={PAGE_SIZE}&offset={offset}"
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "Aradune/1.0",
        })

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                results = data.get("results", [])
                if total_available is None:
                    total_available = data.get("count", 0)
                    print(f"  Total records available: {total_available:,}")
                break
            except Exception as e:
                print(f"  Retry {attempt + 1}/3 at offset {offset}: {e}", flush=True)
                time.sleep(3 * (attempt + 1))
                results = []
        else:
            print(f"  FAILED at offset {offset} after 3 retries", flush=True)
            break

        if not results:
            break

        all_results.extend(results)
        print(f"  Fetched {len(all_results):,} / {total_available:,} records...", flush=True)

        if len(results) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return all_results


def main():
    print("=" * 60)
    print("Eligibility & Enrollment Snapshot Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    print("  Downloading from data.medicaid.gov API (paginated)...")
    results = fetch_all_pages()

    if not results:
        print("  ERROR: No records fetched from API")
        return

    print(f"  Total records fetched: {len(results):,}")

    # Transform records
    rows = []
    for rec in results:  # each rec is a dict from the API
        row = {}
        for old_key, new_key in COLUMN_MAP.items():
            row[new_key] = rec.get(old_key)

        # Skip footnote columns entirely
        # Clean text fields
        for text_col in ("state_code", "state_name", "expansion_state", "data_status", "final_report"):
            if row.get(text_col) is not None:
                row[text_col] = str(row[text_col]).strip()

        # Clean numeric fields
        numeric_cols = [k for k in row.keys() if k not in ("state_code", "state_name",
                        "expansion_state", "data_status", "final_report", "reporting_period")]
        for col in numeric_cols:
            row[col] = clean_numeric(row[col])

        # Reporting period: "202303" -> keep as-is for compatibility with other tables
        if row.get("reporting_period"):
            row["reporting_period"] = str(row["reporting_period"]).strip()

        row["source"] = "CMS Medicaid.gov Eligibility & Enrollment API"
        row["snapshot_date"] = SNAPSHOT_DATE

        rows.append(row)

    print(f"  Transformed {len(rows)} records")

    df = pd.DataFrame(rows)
    con = duckdb.connect()
    con.execute("CREATE TABLE fact_eligibility_enrollment_snapshot AS SELECT * FROM df")

    # Stats
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM fact_eligibility_enrollment_snapshot").fetchone()[0]
    periods = con.execute("SELECT COUNT(DISTINCT reporting_period) FROM fact_eligibility_enrollment_snapshot").fetchone()[0]
    with_call_center = con.execute("""
        SELECT COUNT(*) FROM fact_eligibility_enrollment_snapshot
        WHERE call_center_volume IS NOT NULL
    """).fetchone()[0]
    with_processing = con.execute("""
        SELECT COUNT(*) FROM fact_eligibility_enrollment_snapshot
        WHERE determinations_lt_24h IS NOT NULL
    """).fetchone()[0]

    print(f"\n  States: {states}")
    print(f"  Reporting periods: {periods}")
    print(f"  Records with call center data: {with_call_center}")
    print(f"  Records with processing speed data: {with_processing}")

    # Sample
    sample = con.execute("""
        SELECT state_code, reporting_period, total_medicaid_chip_enrollment,
               avg_call_center_wait_minutes, call_center_abandonment_rate
        FROM fact_eligibility_enrollment_snapshot
        WHERE avg_call_center_wait_minutes IS NOT NULL
        ORDER BY avg_call_center_wait_minutes DESC
        LIMIT 5
    """).fetchall()
    print("\n  Longest call center waits:")
    for s, p, enr, wait, aband in sample:
        enr_str = f"{enr:,.0f}" if enr else "N/A"
        print(f"    {s} ({p}): {wait:.0f} min wait, {aband:.0%} abandon, {enr_str} enrolled")

    # Column report
    cols = [c[0] for c in con.execute("SELECT * FROM fact_eligibility_enrollment_snapshot LIMIT 0").description]
    print(f"\n  Columns ({len(cols)}):")
    for c in cols:
        print(f"    {c}")

    # Write parquet
    out_path = FACT_DIR / "eligibility_enrollment_snapshot" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY fact_eligibility_enrollment_snapshot TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    size_mb = out_path.stat().st_size / 1_048_576
    print(f"\n  -> {out_path.relative_to(PROJECT_ROOT)} ({len(rows)} rows, {size_mb:.3f} MB)")

    con.close()
    print("\n" + "=" * 60)
    print("ELIGIBILITY & ENROLLMENT SNAPSHOT INGESTION COMPLETE")
    print(f"  fact_eligibility_enrollment_snapshot: {len(rows)} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
