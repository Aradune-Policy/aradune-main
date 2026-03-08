#!/usr/bin/env python3
"""
build_lake_behavioral_health.py — Ingest behavioral health data into the lake.

Sources:
  1. SAMHSA NSDUH 2022-2023 state prevalence estimates (41 tables)
     - Substance use, mental illness, treatment gaps by state
  2. SAMHSA N-SUMHSS 2024 facility-level data (27,957 facilities)
     - MH/SUD treatment facilities, bed capacity, services offered
  3. CMS Inpatient Psychiatric Facility quality measures (state + facility)
  4. CDC BRFSS state-level behavioral health indicators

Tables built:
  fact_nsduh_prevalence     — State-level MH/SUD prevalence from NSDUH
  fact_mh_facility          — Mental health & SUD treatment facilities (N-SUMHSS)
  fact_ipf_quality_state    — Psychiatric facility quality measures by state
  fact_ipf_quality_facility — Psychiatric facility quality measures by facility
  fact_brfss_behavioral     — BRFSS behavioral health indicators by state

Usage:
  python3 scripts/build_lake_behavioral_health.py
  python3 scripts/build_lake_behavioral_health.py --dry-run
  python3 scripts/build_lake_behavioral_health.py --table nsduh
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


# Key NSDUH tables and their measures
NSDUH_TABLES = {
    1: ("illicit_drug_use_past_month", "Illicit Drug Use in Past Month"),
    3: ("marijuana_use_past_month", "Marijuana Use in Past Month"),
    9: ("heroin_use_past_year", "Heroin Use in Past Year"),
    12: ("meth_use_past_year", "Methamphetamine Use in Past Year"),
    13: ("rx_pain_reliever_misuse_past_year", "Prescription Pain Reliever Misuse in Past Year"),
    14: ("opioid_misuse_past_year", "Opioid Misuse in Past Year"),
    15: ("alcohol_use_past_month", "Alcohol Use in Past Month"),
    16: ("binge_alcohol_past_month", "Binge Alcohol Use in Past Month"),
    19: ("tobacco_use_past_month", "Tobacco Product Use in Past Month"),
    24: ("sud_past_year", "Substance Use Disorder in Past Year"),
    25: ("aud_past_year", "Alcohol Use Disorder in Past Year"),
    27: ("dud_past_year", "Drug Use Disorder in Past Year"),
    29: ("oud_past_year", "Opioid Use Disorder in Past Year"),
    30: ("su_treatment_past_year", "Received Substance Use Treatment in Past Year"),
    31: ("needing_su_treatment", "Classified as Needing SU Treatment in Past Year"),
    32: ("su_treatment_gap", "Did Not Receive SU Treatment Among Those Needing It"),
    33: ("any_mental_illness", "Any Mental Illness in Past Year (18+)"),
    34: ("serious_mental_illness", "Serious Mental Illness in Past Year (18+)"),
    35: ("co_occurring_sud_ami", "Co-occurring SUD and Any Mental Illness (18+)"),
    36: ("co_occurring_sud_smi", "Co-occurring SUD and Serious Mental Illness (18+)"),
    37: ("mh_treatment_past_year", "Received Mental Health Treatment in Past Year"),
    38: ("major_depressive_episode", "Major Depressive Episode in Past Year"),
    39: ("suicidal_thoughts", "Serious Thoughts of Suicide in Past Year"),
    40: ("suicide_plans", "Made Any Suicide Plans in Past Year"),
    41: ("suicide_attempt", "Attempted Suicide in Past Year"),
}

STATE_NAME_TO_CODE = {
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
}


def _parse_pct(val: str) -> float | None:
    """Parse percentage string like '16.67%' to float 16.67."""
    if not val or val == "--":
        return None
    return float(val.replace("%", "").replace(",", "").strip())


def build_nsduh_prevalence(con, dry_run: bool) -> int:
    """Parse NSDUH state prevalence CSVs into a single fact table."""
    print("Building fact_nsduh_prevalence...")
    nsduh_dir = RAW_DIR / "nsduh_2023"
    if not nsduh_dir.exists():
        print("  SKIPPED — nsduh_2023/ not found")
        return 0

    all_rows = []
    for tab_num, (measure_id, measure_name) in NSDUH_TABLES.items():
        csv_path = nsduh_dir / f"NSDUHsaeExcelTab{tab_num:02d}-2023.csv"
        if not csv_path.exists():
            continue

        lines = csv_path.read_text(encoding="utf-8", errors="replace").splitlines()

        # Find the header row (starts with "Order,State,")
        header_idx = None
        for i, line in enumerate(lines):
            if line.startswith("Order,State,"):
                header_idx = i
                break
        if header_idx is None:
            continue

        headers = lines[header_idx].split(",")
        # Data rows follow header, skip regions (Total U.S., Northeast, etc.)
        for line in lines[header_idx + 1:]:
            if not line.strip():
                break
            parts = line.split(",")
            if len(parts) < len(headers):
                continue
            state_name = parts[1].strip().strip('"')
            if state_name not in STATE_NAME_TO_CODE:
                continue
            state_code = STATE_NAME_TO_CODE[state_name]

            # Parse age group columns: 12+, 12-17, 18-25, 26+, 18+
            # Columns: Order, State, 12+ Est, 12+ CI Low, 12+ CI High, 12-17 Est, ...
            age_groups = [
                ("12+", 2, 3, 4),
                ("12-17", 5, 6, 7),
                ("18-25", 8, 9, 10),
                ("26+", 11, 12, 13),
                ("18+", 14, 15, 16),
            ]
            for age_label, est_idx, ci_lo_idx, ci_hi_idx in age_groups:
                if est_idx >= len(parts):
                    continue
                estimate = _parse_pct(parts[est_idx].strip().strip('"'))
                ci_low = _parse_pct(parts[ci_lo_idx].strip().strip('"')) if ci_lo_idx < len(parts) else None
                ci_high = _parse_pct(parts[ci_hi_idx].strip().strip('"')) if ci_hi_idx < len(parts) else None
                if estimate is not None:
                    all_rows.append({
                        "state_code": state_code,
                        "measure_id": measure_id,
                        "measure_name": measure_name,
                        "age_group": age_label,
                        "estimate_pct": estimate,
                        "ci_lower_pct": ci_low,
                        "ci_upper_pct": ci_high,
                        "survey_years": "2022-2023",
                        "source": "samhsa_nsduh_2023",
                    })

    if not all_rows:
        print("  No NSDUH data parsed")
        return 0

    # Load into DuckDB
    con.execute("CREATE OR REPLACE TABLE _fact_nsduh AS SELECT * FROM (SELECT NULL WHERE FALSE) LIMIT 0")
    con.execute("""
        CREATE OR REPLACE TABLE _fact_nsduh (
            state_code VARCHAR,
            measure_id VARCHAR,
            measure_name VARCHAR,
            age_group VARCHAR,
            estimate_pct DOUBLE,
            ci_lower_pct DOUBLE,
            ci_upper_pct DOUBLE,
            survey_years VARCHAR,
            source VARCHAR,
            snapshot_date DATE
        )
    """)

    for row in all_rows:
        con.execute("""
            INSERT INTO _fact_nsduh VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            row["state_code"], row["measure_id"], row["measure_name"],
            row["age_group"], row["estimate_pct"], row["ci_lower_pct"],
            row["ci_upper_pct"], row["survey_years"], row["source"],
            SNAPSHOT_DATE,
        ])

    count = write_parquet(con, "_fact_nsduh", _snapshot_path("nsduh_prevalence"), dry_run)
    measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _fact_nsduh").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_nsduh").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {measures} measures")

    # Spot checks
    for measure in ["any_mental_illness", "sud_past_year", "suicidal_thoughts"]:
        spot = con.execute(f"""
            SELECT state_code, estimate_pct FROM _fact_nsduh
            WHERE measure_id = '{measure}' AND age_group = '18+'
            ORDER BY estimate_pct DESC LIMIT 3
        """).fetchall()
        if spot:
            top3 = ", ".join(f"{s[0]} {s[1]:.1f}%" for s in spot)
            print(f"  {measure} top 3: {top3}")

    con.execute("DROP TABLE IF EXISTS _fact_nsduh")
    return count


def build_mh_facility(con, dry_run: bool) -> int:
    """Build mental health facility table from N-SUMHSS 2024."""
    print("Building fact_mh_facility...")
    csv_path = RAW_DIR / "nsumhss_2024" / "NSUMHSS_2024_PUF_CSV.csv"
    if not csv_path.exists():
        print("  SKIPPED — NSUMHSS_2024_PUF_CSV.csv not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mh AS
        SELECT
            MPRID AS facility_id,
            LOCATIONSTATE AS state_code,
            CASE FACILITYTYPE
                WHEN '1' THEN 'psychiatric_hospital'
                WHEN '2' THEN 'separate_inpatient_unit'
                WHEN '3' THEN 'residential_treatment_center'
                WHEN '4' THEN 'outpatient'
                WHEN '5' THEN 'partial_hospitalization'
                WHEN '6' THEN 'community_mh_center'
                WHEN '7' THEN 'certified_community_bhc'
                WHEN '8' THEN 'other'
                ELSE 'unknown'
            END AS facility_type,
            CASE WHEN INMH = '1' THEN TRUE ELSE FALSE END AS offers_mh,
            CASE WHEN INSU = '1' THEN TRUE ELSE FALSE END AS offers_su,
            CASE WHEN HOSPITAL = '1' THEN TRUE ELSE FALSE END AS is_hospital,
            TRY_CAST(NULLIF(HOSPBED, '-9') AS INTEGER) AS hospital_beds,
            TRY_CAST(NULLIF(RESBED, '-9') AS INTEGER) AS residential_beds,
            TRY_CAST(NULLIF(IPBEDS, '-9') AS INTEGER) AS inpatient_psych_beds,
            TRY_CAST(NULLIF(RCBEDS, '-9') AS INTEGER) AS crisis_beds,
            CASE WHEN DETOX = '1' THEN TRUE ELSE FALSE END AS offers_detox,
            CASE WHEN TREATMT_SU = '1' THEN TRUE ELSE FALSE END AS offers_su_treatment,
            CASE WHEN MHTXSA = '1' THEN TRUE ELSE FALSE END AS offers_mh_treatment,
            CASE WHEN ANTIPSYCH = '1' THEN TRUE ELSE FALSE END AS prescribes_antipsychotics,
            CASE WHEN TREATPSYCHOTHRPY = '1' THEN TRUE ELSE FALSE END AS offers_psychotherapy,
            'samhsa_nsumhss_2024' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_csv_auto('{csv_path}', header=true, all_varchar=true)
        WHERE LOCATIONSTATE IS NOT NULL
          AND LENGTH(LOCATIONSTATE) = 2
    """)

    count = write_parquet(con, "_fact_mh", _snapshot_path("mh_facility"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_mh").fetchone()[0]

    # Stats
    stats = con.execute("""
        SELECT
            COUNT(*) FILTER (WHERE offers_mh) AS mh_facilities,
            COUNT(*) FILTER (WHERE offers_su) AS su_facilities,
            COALESCE(SUM(hospital_beds), 0) AS total_hosp_beds,
            COALESCE(SUM(inpatient_psych_beds), 0) AS total_psych_beds,
            COALESCE(SUM(residential_beds), 0) AS total_res_beds,
            COALESCE(SUM(crisis_beds), 0) AS total_crisis_beds
        FROM _fact_mh
    """).fetchone()
    print(f"  {count:,} facilities across {states} states")
    print(f"  MH: {stats[0]:,} | SUD: {stats[1]:,}")
    print(f"  Beds — hospital: {stats[2]:,} | psych IP: {stats[3]:,} | residential: {stats[4]:,} | crisis: {stats[5]:,}")

    con.execute("DROP TABLE IF EXISTS _fact_mh")
    return count


def build_ipf_quality(con, dry_run: bool) -> int:
    """Build inpatient psychiatric facility quality measures (state + facility)."""
    total = 0

    # State-level
    print("Building fact_ipf_quality_state...")
    state_path = RAW_DIR / "ipf_quality_state.json"
    if state_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_ipf_state AS
            SELECT * FROM read_json_auto('{state_path}')
        """)
        n = write_parquet(con, "_fact_ipf_state", _snapshot_path("ipf_quality_state"), dry_run)
        print(f"  {n} state-level IPF quality rows")
        total += n
        con.execute("DROP TABLE IF EXISTS _fact_ipf_state")
    else:
        print("  SKIPPED — ipf_quality_state.json not found")

    # Facility-level
    print("Building fact_ipf_quality_facility...")
    fac_path = RAW_DIR / "ipf_quality_facility.json"
    if fac_path.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE _fact_ipf_fac AS
            SELECT * FROM read_json_auto('{fac_path}')
        """)
        n = write_parquet(con, "_fact_ipf_fac", _snapshot_path("ipf_quality_facility"), dry_run)
        states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_ipf_fac").fetchone()[0]
        print(f"  {n:,} facility-level IPF quality rows across {states} states")
        total += n
        con.execute("DROP TABLE IF EXISTS _fact_ipf_fac")
    else:
        print("  SKIPPED — ipf_quality_facility.json not found")

    return total


def build_mds_quality(con, dry_run: bool) -> int:
    """Build MDS nursing home quality measures."""
    print("Building fact_mds_quality...")
    json_path = RAW_DIR / "mds_quality_measures.json"
    if not json_path.exists():
        print("  SKIPPED — mds_quality_measures.json not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_mds AS
        SELECT * FROM read_json_auto('{json_path}', maximum_object_size=300000000)
    """)

    count = write_parquet(con, "_fact_mds", _snapshot_path("mds_quality"), dry_run)
    print(f"  {count:,} MDS quality measure rows")

    con.execute("DROP TABLE IF EXISTS _fact_mds")
    return count


def build_nh_provider(con, dry_run: bool) -> int:
    """Build nursing home provider info (ratings, staffing, deficiencies)."""
    print("Building fact_nh_provider_info...")
    json_path = RAW_DIR / "mds_nh_provider_info.json"
    if not json_path.exists():
        print("  SKIPPED — mds_nh_provider_info.json not found")
        return 0

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_nhpi AS
        SELECT * FROM read_json_auto('{json_path}', maximum_object_size=100000000)
    """)

    count = write_parquet(con, "_fact_nhpi", _snapshot_path("nh_provider_info"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state) FROM _fact_nhpi").fetchone()[0]
    print(f"  {count:,} nursing homes across {states} states")

    # Stats
    stats = con.execute("""
        SELECT
            ROUND(AVG(TRY_CAST(overall_rating AS INTEGER)), 1) AS avg_overall,
            ROUND(AVG(TRY_CAST(qm_rating AS INTEGER)), 1) AS avg_qm,
            ROUND(AVG(TRY_CAST(staffing_rating AS INTEGER)), 1) AS avg_staffing,
            SUM(TRY_CAST(number_of_certified_beds AS INTEGER)) AS total_beds
        FROM _fact_nhpi
    """).fetchone()
    print(f"  Avg ratings — overall: {stats[0]}, QM: {stats[1]}, staffing: {stats[2]}")
    print(f"  Total certified beds: {stats[3]:,}")

    con.execute("DROP TABLE IF EXISTS _fact_nhpi")
    return count


def build_brfss_behavioral(con, dry_run: bool) -> int:
    """Build BRFSS behavioral health indicators from CDC BRFSS data."""
    print("Building fact_brfss_behavioral...")
    json_path = RAW_DIR / "cdc_brfss_state.json"
    if not json_path.exists():
        print("  SKIPPED — cdc_brfss_state.json not found")
        return 0

    # Pre-filter the 1.3GB JSON with streaming Python to avoid DuckDB memory issues
    import ijson
    filtered_path = RAW_DIR / "brfss_behavioral_filtered.json"
    bh_classes = {
        "Alcohol Consumption", "Tobacco Use", "Mental Health",
        "Disability", "Overall Health",
    }

    if not filtered_path.exists():
        print("  Pre-filtering BRFSS JSON (1.3GB → behavioral health subset)...")
        filtered_rows = []
        with open(json_path, "rb") as f:
            for item in ijson.items(f, "item"):
                cls = item.get("class", "")
                loc = item.get("locationabbr", "")
                dv = item.get("data_value")
                if cls in bh_classes and loc and len(loc) == 2 and dv:
                    filtered_rows.append({
                        "year": item.get("year"),
                        "state_code": loc,
                        "class": cls,
                        "topic": item.get("topic"),
                        "question": item.get("question"),
                        "response": item.get("response"),
                        "break_out": item.get("break_out"),
                        "break_out_category": item.get("break_out_category"),
                        "sample_size": item.get("sample_size"),
                        "data_value": dv,
                        "ci_low": item.get("confidence_limit_low"),
                        "ci_high": item.get("confidence_limit_high"),
                        "data_value_type": item.get("data_value_type"),
                    })
                    if len(filtered_rows) % 100000 == 0:
                        print(f"    ... {len(filtered_rows):,} rows kept so far")

        with open(filtered_path, "w") as f:
            json.dump(filtered_rows, f)
        print(f"  Filtered to {len(filtered_rows):,} behavioral health rows")
    else:
        print(f"  Using cached filtered file: {filtered_path.name}")

    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_brfss AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            state_code,
            class,
            topic,
            question,
            response,
            break_out,
            break_out_category,
            TRY_CAST(sample_size AS INTEGER) AS sample_size,
            TRY_CAST(data_value AS DOUBLE) AS data_value_pct,
            TRY_CAST(ci_low AS DOUBLE) AS ci_lower_pct,
            TRY_CAST(ci_high AS DOUBLE) AS ci_upper_pct,
            data_value_type,
            'cdc_brfss' AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM read_json_auto('{filtered_path}')
    """)

    count = write_parquet(con, "_fact_brfss", _snapshot_path("brfss_behavioral"), dry_run)
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_brfss").fetchone()[0]
    topics = con.execute("SELECT COUNT(DISTINCT topic) FROM _fact_brfss").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM _fact_brfss").fetchone()
    print(f"  {count:,} rows, {states} states, {topics} topics, {years[0]}-{years[1]}")

    con.execute("DROP TABLE IF EXISTS _fact_brfss")
    return count


ALL_TABLES = {
    "nsduh": ("fact_nsduh_prevalence", build_nsduh_prevalence),
    "mh_facility": ("fact_mh_facility", build_mh_facility),
    "ipf_quality": ("fact_ipf_quality", build_ipf_quality),
    "mds_quality": ("fact_mds_quality", build_mds_quality),
    "nh_provider": ("fact_nh_provider_info", build_nh_provider),
    "brfss": ("fact_brfss_behavioral", build_brfss_behavioral),
}


def main():
    parser = argparse.ArgumentParser(description="Ingest behavioral health data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", choices=list(ALL_TABLES.keys()) + ["all"], default="all")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Behavioral Health Data Ingestion — {SNAPSHOT_DATE}")
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
    print("BEHAVIORAL HEALTH LAKE INGESTION COMPLETE")
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
            "tables": {name: {"rows": count} for name, count in totals.items()},
            "total_rows": total_rows,
        }
        manifest_file = META_DIR / f"manifest_behavioral_health_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
