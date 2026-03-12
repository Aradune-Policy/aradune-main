#!/usr/bin/env python3
"""
build_lake_pfs_rvu.py — Ingest CMS Physician Fee Schedule Relative Value Files.

Downloads and processes:
  - RVU file (19K+ procedure codes with Work/PE/MP RVUs, CFs, status indicators)
  - GPCI file (113 localities with Work/PE/MP geographic adjustments)
  - Locality-county crosswalk (maps counties to Medicare payment localities)
  - Anesthesia conversion factors (per-locality anesthesia CFs)
  - OPPS cap amounts (carrier x locality payment amounts)

Source: https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files

Tables built:
  fact_pfs_rvu          — Full RVU decomposition per HCPCS/mod (the CPRA denominator)
  ref_pfs_gpci          — GPCI by Medicare locality (Work, PE, MP components)
  ref_pfs_locality      — Locality-county crosswalk
  ref_pfs_anesthesia    — Anesthesia CFs by locality
  fact_pfs_opps_cap     — OPPS cap payment amounts by carrier/locality

Usage:
  python3 scripts/build_lake_pfs_rvu.py
  python3 scripts/build_lake_pfs_rvu.py --dry-run
"""

import argparse
import csv
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "pfs_rvu" / "rvu26b"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
REF_DIR = LAKE_DIR / "reference"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# CY 2026 conversion factors
CF_NON_QPP = 33.4009
CF_QPP = 33.5675
PFS_YEAR = 2026
PFS_QUARTER = "B"  # April release

SOURCE_URL = "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu26b"


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


def _fact_path(name: str) -> Path:
    return FACT_DIR / name / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"


def _ref_path(name: str) -> Path:
    return REF_DIR / f"{name}.parquet"


# ── Parse RVU CSV (skip multi-line header, handle duplicate column names) ────

def _parse_rvu_csv(csv_path: Path) -> list[dict]:
    """Parse the CMS RVU CSV which has 9 header rows before the data header."""
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header_found = False
        for row in reader:
            if not header_found:
                if len(row) > 0 and row[0].strip() == "HCPCS":
                    header_found = True
                continue
            # Skip empty rows or trailing notes
            if not row or not row[0].strip():
                continue
            hcpcs = row[0].strip()
            if not hcpcs or len(hcpcs) > 7:
                continue
            try:
                rows.append({
                    "hcpcs_code": hcpcs,
                    "modifier": row[1].strip() if len(row) > 1 else None,
                    "description": row[2].strip() if len(row) > 2 else None,
                    "status_code": row[3].strip() if len(row) > 3 else None,
                    "not_used_for_payment": row[4].strip() if len(row) > 4 else None,
                    "work_rvu": _float(row[5]) if len(row) > 5 else None,
                    "pe_rvu_nonfac": _float(row[6]) if len(row) > 6 else None,
                    "pe_nonfac_na_indicator": row[7].strip() if len(row) > 7 else None,
                    "pe_rvu_facility": _float(row[8]) if len(row) > 8 else None,
                    "pe_fac_na_indicator": row[9].strip() if len(row) > 9 else None,
                    "mp_rvu": _float(row[10]) if len(row) > 10 else None,
                    "total_nonfac": _float(row[11]) if len(row) > 11 else None,
                    "total_fac": _float(row[12]) if len(row) > 12 else None,
                    "pctc_indicator": row[13].strip() if len(row) > 13 else None,
                    "global_days": row[14].strip() if len(row) > 14 else None,
                    "pre_op": _float(row[15]) if len(row) > 15 else None,
                    "intra_op": _float(row[16]) if len(row) > 16 else None,
                    "post_op": _float(row[17]) if len(row) > 17 else None,
                    "mult_proc": row[18].strip() if len(row) > 18 else None,
                    "bilat_surg": row[19].strip() if len(row) > 19 else None,
                    "asst_surg": row[20].strip() if len(row) > 20 else None,
                    "co_surg": row[21].strip() if len(row) > 21 else None,
                    "team_surg": row[22].strip() if len(row) > 22 else None,
                    "pricing_indicator": row[23].strip() if len(row) > 23 else None,
                    "endo_base": row[24].strip() if len(row) > 24 else None,
                    "conversion_factor": _float(row[25]) if len(row) > 25 else None,
                    "diagnostic_procedures": row[26].strip() if len(row) > 26 else None,
                    "calculation_flag": row[27].strip() if len(row) > 27 else None,
                    "supervision_indicator": row[28].strip() if len(row) > 28 else None,
                    "opps_nonfac_amount": _float(row[29]) if len(row) > 29 else None,
                    "opps_fac_amount": _float(row[30]) if len(row) > 30 else None,
                    "opps_mp_amount": _float(row[31]) if len(row) > 31 else None,
                })
            except (IndexError, ValueError):
                continue
    return rows


def _float(val: str) -> float | None:
    v = val.strip() if val else ""
    if not v or v.upper() in ("NA", "N/A", ""):
        return None
    try:
        return float(v)
    except ValueError:
        return None


# ── Parse GPCI CSV ───────────────────────────────────────────────────────────

def _parse_gpci_csv(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header_found = False
        for row in reader:
            if not header_found:
                if len(row) >= 4 and "State" in row[1]:
                    header_found = True
                continue
            if not row or not row[1].strip():
                continue
            state = row[1].strip()
            if len(state) != 2 or not state.isalpha():
                continue
            try:
                rows.append({
                    "mac": row[0].strip(),
                    "state_code": state,
                    "locality_number": row[2].strip(),
                    "locality_name": row[3].strip().rstrip("*"),
                    "gpci_work_no_floor": _float(row[4]),
                    "gpci_work": _float(row[5]),
                    "gpci_pe": _float(row[6]),
                    "gpci_mp": _float(row[7]),
                })
            except (IndexError, ValueError):
                continue
    return rows


# ── Parse locality-county crosswalk ──────────────────────────────────────────

def _parse_locality_csv(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header_found = False
        for row in reader:
            if not header_found:
                if len(row) >= 3 and "Locality" in str(row[1]):
                    header_found = True
                continue
            if not row or not row[0].strip():
                continue
            mac = row[0].strip()
            if not mac or not mac[0].isdigit():
                continue
            try:
                rows.append({
                    "mac": mac,
                    "locality_number": row[1].strip(),
                    "state_name": row[2].strip(),
                    "fee_schedule_area": row[3].strip() if len(row) > 3 else None,
                    "counties": row[4].strip() if len(row) > 4 else None,
                })
            except (IndexError, ValueError):
                continue
    return rows


# ── Parse anesthesia CF CSV ──────────────────────────────────────────────────

def _parse_anes_csv(csv_path: Path) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header_found = False
        for row in reader:
            if not header_found:
                if len(row) >= 3 and "Contractor" in row[0]:
                    header_found = True
                continue
            if not row or not row[0].strip():
                continue
            contractor = row[0].strip()
            if not contractor[0].isdigit():
                continue
            try:
                rows.append({
                    "contractor": contractor,
                    "locality": row[1].strip(),
                    "locality_name": row[2].strip().rstrip("*"),
                    "anes_cf_qpp": _float(row[3]),
                    "anes_cf_non_qpp": _float(row[4]),
                })
            except (IndexError, ValueError):
                continue
    return rows


# ── Build functions ──────────────────────────────────────────────────────────

def build_pfs_rvu(con, dry_run: bool) -> int:
    """Build fact_pfs_rvu from the non-QPP RVU file (standard rates)."""
    print("Building fact_pfs_rvu...")
    csv_path = RAW_DIR / "PPRRVU2026_Apr_nonQPP.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    rows = _parse_rvu_csv(csv_path)
    print(f"  Parsed {len(rows):,} procedure codes from RVU file")

    con.execute("DROP TABLE IF EXISTS _rvu_raw")
    con.execute("""
        CREATE TABLE _rvu_raw (
            hcpcs_code VARCHAR,
            modifier VARCHAR,
            description VARCHAR,
            status_code VARCHAR,
            not_used_for_payment VARCHAR,
            work_rvu DOUBLE,
            pe_rvu_nonfac DOUBLE,
            pe_nonfac_na_indicator VARCHAR,
            pe_rvu_facility DOUBLE,
            pe_fac_na_indicator VARCHAR,
            mp_rvu DOUBLE,
            total_nonfac DOUBLE,
            total_fac DOUBLE,
            pctc_indicator VARCHAR,
            global_days VARCHAR,
            pre_op DOUBLE,
            intra_op DOUBLE,
            post_op DOUBLE,
            mult_proc VARCHAR,
            bilat_surg VARCHAR,
            asst_surg VARCHAR,
            co_surg VARCHAR,
            team_surg VARCHAR,
            pricing_indicator VARCHAR,
            endo_base VARCHAR,
            conversion_factor DOUBLE,
            diagnostic_procedures VARCHAR,
            calculation_flag VARCHAR,
            supervision_indicator VARCHAR,
            opps_nonfac_amount DOUBLE,
            opps_fac_amount DOUBLE,
            opps_mp_amount DOUBLE
        )
    """)

    # Insert in batches
    for i in range(0, len(rows), 1000):
        batch = rows[i:i+1000]
        values = []
        for r in batch:
            vals = [
                r["hcpcs_code"], r["modifier"], r["description"],
                r["status_code"], r["not_used_for_payment"],
                r["work_rvu"], r["pe_rvu_nonfac"], r["pe_nonfac_na_indicator"],
                r["pe_rvu_facility"], r["pe_fac_na_indicator"], r["mp_rvu"],
                r["total_nonfac"], r["total_fac"], r["pctc_indicator"],
                r["global_days"], r["pre_op"], r["intra_op"], r["post_op"],
                r["mult_proc"], r["bilat_surg"], r["asst_surg"],
                r["co_surg"], r["team_surg"], r["pricing_indicator"],
                r["endo_base"], r["conversion_factor"],
                r["diagnostic_procedures"], r["calculation_flag"],
                r["supervision_indicator"], r["opps_nonfac_amount"],
                r["opps_fac_amount"], r["opps_mp_amount"],
            ]
            values.append(vals)
        con.executemany("INSERT INTO _rvu_raw VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values)

    # Add metadata columns
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_rvu AS
        SELECT
            *,
            {PFS_YEAR} AS pfs_year,
            '{PFS_QUARTER}' AS pfs_quarter,
            {CF_NON_QPP} AS cf_non_qpp,
            {CF_QPP} AS cf_qpp,
            '{SOURCE_URL}' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _rvu_raw
    """)

    count = write_parquet(con, "_fact_rvu", _fact_path("pfs_rvu"), dry_run)

    # Summary stats
    active = con.execute("""
        SELECT COUNT(*) FROM _fact_rvu
        WHERE status_code IN ('A', 'R', 'T')
          AND work_rvu > 0
    """).fetchone()[0]
    em_count = con.execute("""
        SELECT COUNT(*) FROM _fact_rvu
        WHERE hcpcs_code BETWEEN '99201' AND '99499'
          AND status_code = 'A'
    """).fetchone()[0]
    cf_val = con.execute("SELECT DISTINCT conversion_factor FROM _fact_rvu WHERE conversion_factor IS NOT NULL LIMIT 1").fetchone()
    print(f"  {count:,} total codes, {active:,} active with Work RVU > 0, {em_count} E/M codes")
    print(f"  Conversion factor: ${cf_val[0]}" if cf_val else "  No CF found")

    con.execute("DROP TABLE IF EXISTS _rvu_raw")
    con.execute("DROP TABLE IF EXISTS _fact_rvu")
    return count


def build_gpci(con, dry_run: bool) -> int:
    """Build ref_pfs_gpci from GPCI file."""
    print("Building ref_pfs_gpci...")
    csv_path = RAW_DIR / "GPCI2026.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    rows = _parse_gpci_csv(csv_path)
    print(f"  Parsed {len(rows)} localities")

    con.execute("DROP TABLE IF EXISTS _gpci")
    con.execute("""
        CREATE TABLE _gpci (
            mac VARCHAR,
            state_code VARCHAR,
            locality_number VARCHAR,
            locality_name VARCHAR,
            gpci_work_no_floor DOUBLE,
            gpci_work DOUBLE,
            gpci_pe DOUBLE,
            gpci_mp DOUBLE
        )
    """)
    for r in rows:
        con.execute("INSERT INTO _gpci VALUES (?,?,?,?,?,?,?,?)", [
            r["mac"], r["state_code"], r["locality_number"], r["locality_name"],
            r["gpci_work_no_floor"], r["gpci_work"], r["gpci_pe"], r["gpci_mp"],
        ])

    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_gpci AS
        SELECT
            *,
            {PFS_YEAR} AS pfs_year,
            '{SOURCE_URL}' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _gpci
    """)

    count = write_parquet(con, "_ref_gpci", _ref_path("ref_pfs_gpci"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _ref_gpci").fetchone()[0]
    print(f"  {count} localities across {states} states/territories")

    con.execute("DROP TABLE IF EXISTS _gpci")
    con.execute("DROP TABLE IF EXISTS _ref_gpci")
    return count


def build_locality_crosswalk(con, dry_run: bool) -> int:
    """Build ref_pfs_locality from locality-county crosswalk."""
    print("Building ref_pfs_locality...")
    csv_path = RAW_DIR / "26LOCCO.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    rows = _parse_locality_csv(csv_path)
    print(f"  Parsed {len(rows)} locality records")

    con.execute("DROP TABLE IF EXISTS _loc")
    con.execute("""
        CREATE TABLE _loc (
            mac VARCHAR,
            locality_number VARCHAR,
            state_name VARCHAR,
            fee_schedule_area VARCHAR,
            counties VARCHAR
        )
    """)
    for r in rows:
        con.execute("INSERT INTO _loc VALUES (?,?,?,?,?)", [
            r["mac"], r["locality_number"], r["state_name"],
            r["fee_schedule_area"], r["counties"],
        ])

    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_loc AS
        SELECT
            *,
            {PFS_YEAR} AS pfs_year,
            '{SOURCE_URL}' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _loc
    """)

    count = write_parquet(con, "_ref_loc", _ref_path("ref_pfs_locality"), dry_run)
    print(f"  {count} locality-county mappings")

    con.execute("DROP TABLE IF EXISTS _loc")
    con.execute("DROP TABLE IF EXISTS _ref_loc")
    return count


def build_anesthesia(con, dry_run: bool) -> int:
    """Build ref_pfs_anesthesia from anesthesia CF file."""
    print("Building ref_pfs_anesthesia...")
    csv_path = RAW_DIR / "ANES2026.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    rows = _parse_anes_csv(csv_path)
    print(f"  Parsed {len(rows)} anesthesia locality records")

    con.execute("DROP TABLE IF EXISTS _anes")
    con.execute("""
        CREATE TABLE _anes (
            contractor VARCHAR,
            locality VARCHAR,
            locality_name VARCHAR,
            anes_cf_qpp DOUBLE,
            anes_cf_non_qpp DOUBLE
        )
    """)
    for r in rows:
        con.execute("INSERT INTO _anes VALUES (?,?,?,?,?)", [
            r["contractor"], r["locality"], r["locality_name"],
            r["anes_cf_qpp"], r["anes_cf_non_qpp"],
        ])

    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_anes AS
        SELECT
            *,
            {PFS_YEAR} AS pfs_year,
            '{SOURCE_URL}' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _anes
    """)

    count = write_parquet(con, "_ref_anes", _ref_path("ref_pfs_anesthesia"), dry_run)
    print(f"  {count} localities with anesthesia CFs")

    con.execute("DROP TABLE IF EXISTS _anes")
    con.execute("DROP TABLE IF EXISTS _ref_anes")
    return count


def build_opps_cap(con, dry_run: bool) -> int:
    """Build fact_pfs_opps_cap from OPPS cap file (carrier x locality payment amounts)."""
    print("Building fact_pfs_opps_cap...")
    csv_path = RAW_DIR / "OPPSCAP_Apr.csv"
    if not csv_path.exists():
        print(f"  SKIPPED — {csv_path.name} not found")
        return 0

    # OPPS cap file is a clean CSV with header on first line
    con.execute(f"""
        CREATE OR REPLACE TABLE _opps AS
        SELECT
            HCPCS AS hcpcs_code,
            MOD AS modifier,
            PROCSTAT AS status_code,
            CARRIER AS carrier,
            LOCALITY AS locality,
            TRY_CAST("FACILITY PRICE" AS DOUBLE) AS facility_price,
            TRY_CAST("NON-FACILTY PRICE" AS DOUBLE) AS nonfacility_price,
            {PFS_YEAR} AS pfs_year,
            '{PFS_QUARTER}' AS pfs_quarter,
            '{SOURCE_URL}' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE HCPCS IS NOT NULL AND TRIM(HCPCS) != ''
    """)

    count = write_parquet(con, "_opps", _fact_path("pfs_opps_cap"), dry_run)
    codes = con.execute("SELECT COUNT(DISTINCT hcpcs_code) FROM _opps").fetchone()[0]
    localities = con.execute("SELECT COUNT(DISTINCT locality) FROM _opps").fetchone()[0]
    print(f"  {count:,} rows, {codes} unique codes, {localities} localities")

    con.execute("DROP TABLE IF EXISTS _opps")
    return count


# ── Main ─────────────────────────────────────────────────────────────────────

ALL_TABLES = {
    "fact_pfs_rvu": build_pfs_rvu,
    "ref_pfs_gpci": build_gpci,
    "ref_pfs_locality": build_locality_crosswalk,
    "ref_pfs_anesthesia": build_anesthesia,
    "fact_pfs_opps_cap": build_opps_cap,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest CMS PFS RVU files into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"CMS Physician Fee Schedule RVU Ingestion")
    print(f"  PFS Year: CY {PFS_YEAR} (Quarter {PFS_QUARTER})")
    print(f"  CF (non-QPP): ${CF_NON_QPP}")
    print(f"  CF (QPP):     ${CF_QPP}")
    print(f"  Snapshot:     {SNAPSHOT_DATE}")
    print(f"  Run ID:       {RUN_ID}")
    print()

    con = duckdb.connect()
    totals = {}
    for name, builder in ALL_TABLES.items():
        totals[name] = builder(con, args.dry_run)
        print()

    con.close()

    print("=" * 60)
    print("PFS RVU LAKE INGESTION COMPLETE")
    print("=" * 60)
    total_rows = sum(totals.values())
    for name, count in totals.items():
        status = "written" if not args.dry_run else "dry-run"
        print(f"  {name:35s} {count:>12,} rows  [{status}]")
    print(f"  {'TOTAL':35s} {total_rows:>12,} rows")

    if not args.dry_run and total_rows > 0:
        META_DIR.mkdir(parents=True, exist_ok=True)
        manifest = {
            "snapshot_date": SNAPSHOT_DATE,
            "pipeline_run_id": RUN_ID,
            "created_at": datetime.now().isoformat() + "Z",
            "source": SOURCE_URL,
            "pfs_year": PFS_YEAR,
            "pfs_quarter": PFS_QUARTER,
            "conversion_factor_non_qpp": CF_NON_QPP,
            "conversion_factor_qpp": CF_QPP,
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_pfs_rvu_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
