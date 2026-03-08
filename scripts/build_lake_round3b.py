#!/usr/bin/env python3
"""
build_lake_round3b.py — Additional T-MSIS + managed care datasets.

Tables built:
  fact_bh_services          — BH services by condition, service type, state, month (31K rows)
  fact_integrated_care      — Beneficiaries who could benefit from integrated care (796 rows)
  fact_1915c_participants   — 1915(c) waiver participants 2020-2022 (451 rows)
  fact_mc_share             — Share of Medicaid enrollees in managed care (513 rows)
  fact_mc_monthly           — Monthly managed care enrollment by state (31K rows)

Usage:
  python3 scripts/build_lake_round3b.py
  python3 scripts/build_lake_round3b.py --dry-run
"""

import argparse
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


def build_bh_services(con, dry_run: bool) -> int:
    """Comprehensive BH services by condition, service type, state, and month."""
    print("Building fact_bh_services...")
    csv_path = RAW_DIR / "medicaid_bh_services.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_bh_svc AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Month AS month,
            Condition AS condition,
            BehavioralHealthService AS service_type,
            TRY_CAST(REPLACE(REPLACE(ServiceCount, ',', ''), ' ', '') AS INTEGER) AS service_count,
            TRY_CAST(RatePer1000Beneficiaries AS DOUBLE) AS rate_per_1000,
            DataQuality AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_bh_svc", _snapshot_path("bh_services"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_bh_svc").fetchone()[0]
    conditions = con.execute("SELECT COUNT(DISTINCT condition) FROM _fact_bh_svc").fetchone()[0]
    services = con.execute("SELECT COUNT(DISTINCT service_type) FROM _fact_bh_svc").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {conditions} conditions, {services} service types")
    con.execute("DROP TABLE IF EXISTS _fact_bh_svc")
    return count


def build_integrated_care(con, dry_run: bool) -> int:
    """Beneficiaries who could benefit from integrated MH/SUD + physical health care."""
    print("Building fact_integrated_care...")
    csv_path = RAW_DIR / "medicaid_integrated_care.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_integ AS
        SELECT
            State AS state_name,
            TRY_CAST(Year AS INTEGER) AS year,
            Population AS population,
            TRY_CAST(REPLACE(REPLACE("Number of Beneficiaries", ',', ''), ' ', '') AS INTEGER) AS beneficiaries,
            "Data Quality" AS data_quality,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE State IS NOT NULL AND LENGTH(State) > 1
    """)

    count = write_parquet(con, "_fact_integ", _snapshot_path("integrated_care"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_integ").fetchone()[0]
    pops = con.execute("SELECT DISTINCT population FROM _fact_integ ORDER BY population").fetchall()
    print(f"  {count:,} rows, {states} states, populations: {[p[0] for p in pops]}")
    con.execute("DROP TABLE IF EXISTS _fact_integ")
    return count


def build_1915c_participants(con, dry_run: bool) -> int:
    """1915(c) waiver participants 2020-2022 from T-MSIS."""
    print("Building fact_1915c_participants...")
    csv_path = RAW_DIR / "medicaid_1915c_waiver_participants.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_1915c AS
        SELECT
            TRY_CAST(Year AS INTEGER) AS year,
            Geography AS geography,
            "Subpopulation topic" AS subpop_topic,
            Subpopulation AS subpopulation,
            Category AS category,
            TRY_CAST(REPLACE("Count of enrollees", ',', '') AS INTEGER) AS enrollee_count,
            TRY_CAST(REPLACE("Denominator count of enrollees", ',', '') AS INTEGER) AS denominator,
            TRY_CAST("Percentage of enrollees" AS DOUBLE) AS pct_enrollees,
            "Data version" AS data_version,
            'data_medicaid_gov_tmsis' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE Year IS NOT NULL
    """)

    count = write_parquet(con, "_fact_1915c", _snapshot_path("1915c_participants"), dry_run)
    years = con.execute("SELECT DISTINCT year FROM _fact_1915c ORDER BY year").fetchall()
    states = con.execute("SELECT COUNT(DISTINCT geography) FROM _fact_1915c WHERE geography != 'National'").fetchone()[0]
    print(f"  {count} rows, {states} states, years: {[y[0] for y in years]}")
    con.execute("DROP TABLE IF EXISTS _fact_1915c")
    return count


def build_mc_share(con, dry_run: bool) -> int:
    """Share of Medicaid enrollees in managed care by state."""
    import csv as csvmod
    print("Building fact_mc_share...")
    csv_path = RAW_DIR / "medicaid_mc_share.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    # DuckDB can't auto-detect this CSV's quoting — use Python csv module
    con.execute("""
        CREATE OR REPLACE TABLE _fact_mc_share (
            state_name VARCHAR, year INTEGER,
            total_enrollees INTEGER, any_mc_enrolled INTEGER,
            pct_any_mc DOUBLE, comprehensive_mc_enrolled INTEGER,
            pct_comprehensive_mc DOUBLE,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        for row in reader:
            state = row.get('State', '').strip()
            if not state or len(state) < 2 or state == 'TOTALS':
                continue
            def parse_int(v):
                v = v.replace(',', '').replace(' ', '').strip()
                return int(v) if v and v.isdigit() else None
            def parse_pct(v):
                v = v.replace('%', '').strip()
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return None
            year = int(row.get('Year', 0)) if row.get('Year', '').strip().isdigit() else None
            if year is None:
                continue
            con.execute("INSERT INTO _fact_mc_share VALUES (?,?,?,?,?,?,?,?,?)", [
                state, year,
                parse_int(row.get('Total Medicaid Enrollees', '')),
                parse_int(row.get('Individuals Enrolled (Any)', '')),
                parse_pct(row.get('Percent of all Medicaid enrollees (Any)', '')),
                parse_int(row.get('Individuals Enrolled (Comprehensive)', '')),
                parse_pct(row.get('Percent of all Medicaid enrollees (Comprehensive)', '')),
                'data_medicaid_gov', SNAPSHOT_DATE,
            ])

    count = write_parquet(con, "_fact_mc_share", _snapshot_path("mc_share"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_mc_share").fetchone()[0]
    years = con.execute("SELECT DISTINCT year FROM _fact_mc_share ORDER BY year").fetchall()
    avg_pct = con.execute("SELECT ROUND(AVG(pct_any_mc), 1) FROM _fact_mc_share WHERE year = (SELECT MAX(year) FROM _fact_mc_share)").fetchone()[0]
    print(f"  {count} rows, {states} states, years: {[y[0] for y in years]}, latest avg MC penetration: {avg_pct}%")
    con.execute("DROP TABLE IF EXISTS _fact_mc_share")
    return count


def build_mc_monthly(con, dry_run: bool) -> int:
    """Monthly managed care enrollment by state."""
    import csv as csvmod
    print("Building fact_mc_monthly...")
    csv_path = RAW_DIR / "medicaid_mc_monthly.csv"
    if not csv_path.exists():
        print("  SKIPPED — file not found")
        return 0

    con.execute("""
        CREATE OR REPLACE TABLE _fact_mc_mo (
            state_name VARCHAR, reporting_month VARCHAR,
            mc_participation VARCHAR, enrolled_count INTEGER,
            data_quality VARCHAR,
            source VARCHAR, snapshot_date DATE
        )
    """)
    with open(csv_path, newline='') as f:
        reader = csvmod.DictReader(f)
        batch = []
        for row in reader:
            state = row.get('State', '').strip()
            if not state or len(state) < 2:
                continue
            enrolled = row.get('CountEnrolled', '').replace(',', '').replace(' ', '').strip()
            enrolled_int = int(enrolled) if enrolled.isdigit() else None
            batch.append((
                state,
                row.get('Month', '').strip(),
                row.get('managedcare participation', '').strip(),
                enrolled_int,
                row.get('dunusable', '').strip(),
                'data_medicaid_gov_tmsis', SNAPSHOT_DATE,
            ))
            if len(batch) >= 1000:
                con.executemany("INSERT INTO _fact_mc_mo VALUES (?,?,?,?,?,?,?)", batch)
                batch = []
        if batch:
            con.executemany("INSERT INTO _fact_mc_mo VALUES (?,?,?,?,?,?,?)", batch)

    count = write_parquet(con, "_fact_mc_mo", _snapshot_path("mc_monthly"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_name) FROM _fact_mc_mo").fetchone()[0]
    months = con.execute("SELECT MIN(reporting_month), MAX(reporting_month) FROM _fact_mc_mo").fetchone()
    print(f"  {count:,} rows, {states} states, period: {months[0]} to {months[1]}")
    con.execute("DROP TABLE IF EXISTS _fact_mc_mo")
    return count


ALL_TABLES = {
    "bh_services": ("fact_bh_services", build_bh_services),
    "integrated": ("fact_integrated_care", build_integrated_care),
    "1915c": ("fact_1915c_participants", build_1915c_participants),
    "mc_share": ("fact_mc_share", build_mc_share),
    "mc_monthly": ("fact_mc_monthly", build_mc_monthly),
}


def main():
    parser = argparse.ArgumentParser(description="Round 3b lake ingestion")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Round 3b — Additional T-MSIS + MC Data — {SNAPSHOT_DATE}")
    print(f"{'='*60}")
    print(f"Run ID: {RUN_ID}\n")

    con = duckdb.connect()
    totals = {}

    tables_to_build = ALL_TABLES if args.table == "all" else {args.table: ALL_TABLES[args.table]}
    for key, (fact_name, builder) in tables_to_build.items():
        totals[fact_name] = builder(con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("ROUND 3b LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:40s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':40s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_round3b_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
