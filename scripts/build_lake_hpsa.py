#!/usr/bin/env python3
"""
build_lake_hpsa.py — Ingest HRSA HPSA designation data into the lake.

Reads from: data/raw/hpsa_primary_care.csv
             data/raw/hpsa_mental_health.csv
             data/raw/hpsa_dental.csv
Writes to:  data/lake/fact/hpsa/snapshot=YYYY-MM-DD/data.parquet

Tables built:
  fact_hpsa — Health Professional Shortage Area designations (all 3 disciplines)

Usage:
  python3 scripts/build_lake_hpsa.py
  python3 scripts/build_lake_hpsa.py --dry-run
"""

import argparse
import csv
import json
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
    if val is None or val == '' or val == 'NA' or val == 'N/A':
        return None
    try:
        return float(val.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def _try_int(val):
    f = _try_float(val)
    return int(f) if f is not None else None


def _parse_hpsa_csv(csv_path: Path, discipline: str) -> list[dict]:
    """Parse a single HPSA CSV file, return normalized records."""
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            state = r.get('Primary State Abbreviation', '').strip()
            status = r.get('HPSA Status', '').strip()
            if not state or len(state) != 2 or status == 'Withdrawn':
                continue
            rows.append({
                'hpsa_name': r.get('HPSA Name', '').strip(),
                'hpsa_id': r.get('HPSA ID', '').strip(),
                'discipline': discipline,
                'designation_type': r.get('Designation Type', '').strip(),
                'hpsa_score': _try_int(r.get('HPSA Score')),
                'state_code': state,
                'hpsa_status': status,
                'metro_indicator': r.get('Metropolitan Indicator', '').strip(),
                'degree_of_shortage': r.get('HPSA Degree of Shortage', '').strip(),
                'hpsa_fte': _try_float(r.get('HPSA FTE')),
                'designation_population': _try_float(r.get('HPSA Designation Population')),
                'pct_poverty': _try_float(r.get('% of Population Below 100% Poverty')),
                'formal_ratio': r.get('HPSA Formal Ratio', '').strip(),
                'population_type': r.get('HPSA Population Type', '').strip(),
                'rural_status': r.get('Rural Status', '').strip(),
                'longitude': _try_float(r.get('Longitude')),
                'latitude': _try_float(r.get('Latitude')),
                'county_name': r.get('Common County Name', '').strip(),
                'state_fips': r.get('Common State FIPS Code', '').strip(),
                'provider_type': r.get('Provider Type', '').strip(),
                'us_mexico_border': r.get('U.S. - Mexico Border 100 Kilometer Indicator', '').strip(),
                'estimated_served_pop': _try_float(r.get('HPSA Estimated Served Population')),
                'estimated_underserved_pop': _try_float(r.get('HPSA Estimated Underserved Population')),
                'resident_civilian_pop': _try_float(r.get('HPSA Resident Civilian Population')),
                'shortage': _try_float(r.get('HPSA Shortage')),
                'provider_ratio_goal': r.get('HPSA Provider Ratio Goal', '').strip(),
                'source': 'data.hrsa.gov',
                'snapshot_date': SNAPSHOT_DATE,
            })
    return rows


def build_fact_hpsa(con, dry_run: bool) -> int:
    """Build unified HPSA fact table from 3 discipline files."""
    print("Building fact_hpsa...")

    files = {
        "Primary Care": RAW_DIR / "hpsa_primary_care.csv",
        "Mental Health": RAW_DIR / "hpsa_mental_health.csv",
        "Dental Health": RAW_DIR / "hpsa_dental.csv",
    }

    all_rows = []
    for discipline, csv_path in files.items():
        if not csv_path.exists():
            print(f"  SKIPPED — {csv_path.name} not found")
            continue
        rows = _parse_hpsa_csv(csv_path, discipline)
        print(f"  {discipline}: {len(rows):,} rows (non-withdrawn)")
        all_rows.extend(rows)

    if not all_rows:
        print("  SKIPPED — no HPSA data found")
        return 0

    import tempfile
    tmp = Path(tempfile.mktemp(suffix='.csv'))
    fieldnames = list(all_rows[0].keys())
    with open(tmp, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_hpsa AS
        SELECT
            hpsa_name, hpsa_id, discipline, designation_type,
            TRY_CAST(hpsa_score AS INTEGER) AS hpsa_score,
            state_code, hpsa_status, metro_indicator, degree_of_shortage,
            TRY_CAST(hpsa_fte AS DOUBLE) AS hpsa_fte,
            TRY_CAST(designation_population AS DOUBLE) AS designation_population,
            TRY_CAST(pct_poverty AS DOUBLE) AS pct_poverty,
            formal_ratio, population_type, rural_status,
            TRY_CAST(longitude AS DOUBLE) AS longitude,
            TRY_CAST(latitude AS DOUBLE) AS latitude,
            county_name, state_fips, provider_type, us_mexico_border,
            TRY_CAST(estimated_served_pop AS DOUBLE) AS estimated_served_pop,
            TRY_CAST(estimated_underserved_pop AS DOUBLE) AS estimated_underserved_pop,
            TRY_CAST(resident_civilian_pop AS DOUBLE) AS resident_civilian_pop,
            TRY_CAST(shortage AS DOUBLE) AS shortage,
            provider_ratio_goal, source,
            TRY_CAST(snapshot_date AS DATE) AS snapshot_date
        FROM read_csv_auto('{tmp}', header=true, all_varchar=true)
    """)
    tmp.unlink()

    count = write_parquet(con, "_fact_hpsa", _snapshot_path("hpsa"), dry_run)

    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_hpsa").fetchone()[0]
    by_disc = con.execute("""
        SELECT discipline, COUNT(*) AS n
        FROM _fact_hpsa GROUP BY discipline ORDER BY discipline
    """).fetchall()
    disc_str = ", ".join(f"{d}: {n:,}" for d, n in by_disc)
    print(f"  Total: {count:,} rows, {states} states — {disc_str}")

    con.execute("DROP TABLE IF EXISTS _fact_hpsa")
    return count


def main():
    parser = argparse.ArgumentParser(description="Ingest HRSA HPSA data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print()

    con = duckdb.connect()
    count = build_fact_hpsa(con, args.dry_run)
    con.close()

    print()
    print("=" * 60)
    print("HPSA DATA LAKE INGESTION COMPLETE")
    print("=" * 60)
    status = "written" if not args.dry_run else "dry-run"
    print(f"  {'fact_hpsa':35s} {count:>12,} rows  [{status}]")

    if not args.dry_run and count > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {"fact_hpsa": {"rows": count}},
            "total_rows": count,
        }
        manifest_file = META_DIR / f"manifest_hpsa_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
