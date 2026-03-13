#!/usr/bin/env python3
"""
build_lake_data_gaps.py - Comprehensive data gap ingestion (Session 20)

Downloads and ingests datasets identified in systematic gap analysis:

NEW TABLES:
  1. fact_sahie_county        - Census SAHIE county-level uninsured/insured estimates (3,143 counties)
  2. fact_sahie_state         - Census SAHIE state-level insurance estimates (52 states/territories)
  3. fact_sahie_county_138fpl - Census SAHIE at 138% FPL (Medicaid expansion threshold) by county
  4. fact_acs_disability      - ACS disability status by state (2023 1-year)
  5. fact_acs_language        - ACS language spoken at home by state (2023 1-year)
  6. fact_places_county_2025  - CDC PLACES 2025 release, GIS-friendly county data (3,143 counties, 40 measures)
  7. fact_provisional_overdose - CDC VSRR provisional drug overdose deaths by state (2015-2025)
  8. fact_mc_enrollment_by_plan - Managed care enrollment by plan/program (2016-2024, 7,806 rows)
  9. fact_teds_admissions_2023 - SAMHSA TEDS admissions 2023 (1.4M+ treatment episodes)
  10. fact_nsumhss_facility    - SAMHSA N-SUMHSS 2024 facility-level survey (21K+ facilities)

Usage:
  python3 scripts/build_lake_data_gaps.py
  python3 scripts/build_lake_data_gaps.py --dry-run
  python3 scripts/build_lake_data_gaps.py --table sahie_county
"""

import argparse
import csv
import json
import re
from datetime import date
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parent.parent
LAKE = PROJECT / "data" / "lake"
RAW = PROJECT / "data" / "raw"
SNAP = str(date.today())

# State FIPS to code mapping
FIPS_TO_STATE = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR", "78": "VI", "66": "GU", "69": "MP",
    "60": "AS",
}

STATE_FIPS_LOOKUP = {
    "Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05",
    "California": "06", "Colorado": "08", "Connecticut": "09",
    "Delaware": "10", "District of Columbia": "11", "Florida": "12",
    "Georgia": "13", "Hawaii": "15", "Idaho": "16", "Illinois": "17",
    "Indiana": "18", "Iowa": "19", "Kansas": "20", "Kentucky": "21",
    "Louisiana": "22", "Maine": "23", "Maryland": "24",
    "Massachusetts": "25", "Michigan": "26", "Minnesota": "27",
    "Mississippi": "28", "Missouri": "29", "Montana": "30",
    "Nebraska": "31", "Nevada": "32", "New Hampshire": "33",
    "New Jersey": "34", "New Mexico": "35", "New York": "36",
    "North Carolina": "37", "North Dakota": "38", "Ohio": "39",
    "Oklahoma": "40", "Oregon": "41", "Pennsylvania": "42",
    "Rhode Island": "44", "South Carolina": "45", "South Dakota": "46",
    "Tennessee": "47", "Texas": "48", "Utah": "49", "Vermont": "50",
    "Virginia": "51", "Washington": "53", "West Virginia": "54",
    "Wisconsin": "55", "Wyoming": "56", "Puerto Rico": "72",
}


def _num(val):
    """Parse numeric value, returning None for empty/invalid."""
    if val is None or val == "" or val == "." or val == "-":
        return None
    if isinstance(val, (int, float)):
        return val
    val = str(val).strip().replace(",", "").replace("$", "")
    try:
        return float(val)
    except ValueError:
        return None


def _write_parquet(con, table_name, fact_name, dry_run=False):
    """Write DuckDB table to Hive-partitioned parquet with ZSTD."""
    cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    out_dir = LAKE / "fact" / fact_name / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    if not dry_run and cnt > 0:
        con.execute(
            f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  {fact_name}: {cnt:,} rows ({size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {fact_name}: {cnt:,} rows")
    return cnt


# ── 1. SAHIE County ────────────────────────────────────────────────────

def build_sahie_county(con, dry_run=False):
    """Census SAHIE county-level health insurance estimates (2023)."""
    print("Building fact_sahie_county...")
    path = RAW / "sahie_county_2023.json"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    data = json.loads(path.read_text())
    headers = data[0]
    rows = []
    for row in data[1:]:
        d = dict(zip(headers, row))
        state_fips = d.get("state", "")
        county_fips = d.get("county", "")
        if not state_fips or not county_fips:
            continue
        state_code = FIPS_TO_STATE.get(state_fips)
        if not state_code:
            continue
        fips = state_fips + county_fips
        rows.append({
            "state_code": state_code,
            "state_fips": state_fips,
            "county_fips": fips,
            "county_name": d.get("NAME", "").replace(f", {d.get('NAME', '').split(', ')[-1]}", "")
                          if ", " in d.get("NAME", "") else d.get("NAME", ""),
            "state_name": d.get("NAME", "").split(", ")[-1] if ", " in d.get("NAME", "") else "",
            "year": int(d.get("YEAR", 2023)),
            "number_insured": _num(d.get("NIC_PT")),
            "number_insured_moe": _num(d.get("NIC_MOE")),
            "number_uninsured": _num(d.get("NUI_PT")),
            "number_uninsured_moe": _num(d.get("NUI_MOE")),
            "pct_uninsured": _num(d.get("PCTUI_PT")),
            "pct_uninsured_moe": _num(d.get("PCTUI_MOE")),
            "pct_insured": _num(d.get("PCTIC_PT")),
            "pct_insured_moe": _num(d.get("PCTIC_MOE")),
            "total_population": _num(d.get("NIPR_PT")),
            "snapshot": SNAP,
        })

    if not rows:
        print("  No rows parsed")
        return 0

    con.execute("DROP TABLE IF EXISTS _sahie_county")
    con.execute("CREATE TABLE _sahie_county AS SELECT * FROM (VALUES " +
                ", ".join(["(" + ", ".join([f"'{v}'" if isinstance(v, str) else
                                           "NULL" if v is None else str(v)
                                           for v in r.values()]) + ")"
                          for r in rows[:5]]) + ") t(" +
                ", ".join(rows[0].keys()) + ")")
    # Use DuckDB's direct JSON read instead
    con.execute("DROP TABLE IF EXISTS _sahie_county")

    # Write to temp CSV for DuckDB
    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    writer = csv.DictWriter(tmp, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    tmp.close()

    con.execute(f"""
        CREATE TABLE _sahie_county AS
        SELECT * FROM read_csv('{tmp.name}', header=true, auto_detect=true)
    """)
    import os
    os.unlink(tmp.name)

    return _write_parquet(con, "_sahie_county", "sahie_county", dry_run)


# ── 2. SAHIE State ────────────────────────────────────────────────────

def build_sahie_state(con, dry_run=False):
    """Census SAHIE state-level health insurance estimates (2023)."""
    print("Building fact_sahie_state...")
    path = RAW / "sahie_state_2023.json"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    data = json.loads(path.read_text())
    headers = data[0]
    rows = []
    for row in data[1:]:
        d = dict(zip(headers, row))
        state_fips = d.get("state", "")
        state_code = FIPS_TO_STATE.get(state_fips)
        if not state_code:
            continue
        rows.append({
            "state_code": state_code,
            "state_fips": state_fips,
            "state_name": d.get("NAME", ""),
            "year": int(d.get("YEAR", 2023)),
            "number_insured": _num(d.get("NIC_PT")),
            "number_insured_moe": _num(d.get("NIC_MOE")),
            "number_uninsured": _num(d.get("NUI_PT")),
            "number_uninsured_moe": _num(d.get("NUI_MOE")),
            "pct_uninsured": _num(d.get("PCTUI_PT")),
            "pct_uninsured_moe": _num(d.get("PCTUI_MOE")),
            "pct_insured": _num(d.get("PCTIC_PT")),
            "pct_insured_moe": _num(d.get("PCTIC_MOE")),
            "total_population": _num(d.get("NIPR_PT")),
            "snapshot": SNAP,
        })

    if not rows:
        print("  No rows parsed")
        return 0

    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    writer = csv.DictWriter(tmp, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    tmp.close()

    con.execute("DROP TABLE IF EXISTS _sahie_state")
    con.execute(f"""
        CREATE TABLE _sahie_state AS
        SELECT * FROM read_csv('{tmp.name}', header=true, auto_detect=true)
    """)
    import os
    os.unlink(tmp.name)

    return _write_parquet(con, "_sahie_state", "sahie_state", dry_run)


# ── 3. SAHIE County 138% FPL ──────────────────────────────────────────

def build_sahie_county_138fpl(con, dry_run=False):
    """Census SAHIE at 138% FPL (Medicaid expansion income threshold) by county."""
    print("Building fact_sahie_county_138fpl...")
    path = RAW / "sahie_county_138fpl_2023.json"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    data = json.loads(path.read_text())
    headers = data[0]
    rows = []
    for row in data[1:]:
        d = dict(zip(headers, row))
        state_fips = d.get("state", "")
        county_fips = d.get("county", "")
        if not state_fips or not county_fips:
            continue
        state_code = FIPS_TO_STATE.get(state_fips)
        if not state_code:
            continue
        fips = state_fips + county_fips
        rows.append({
            "state_code": state_code,
            "county_fips": fips,
            "county_name": d.get("NAME", "").split(",")[0].strip() if ", " in d.get("NAME", "") else d.get("NAME", ""),
            "year": int(d.get("YEAR", 2023)),
            "income_threshold": "138_pct_fpl",
            "number_insured": _num(d.get("NIC_PT")),
            "number_insured_moe": _num(d.get("NIC_MOE")),
            "number_uninsured": _num(d.get("NUI_PT")),
            "number_uninsured_moe": _num(d.get("NUI_MOE")),
            "pct_uninsured": _num(d.get("PCTUI_PT")),
            "pct_uninsured_moe": _num(d.get("PCTUI_MOE")),
            "pct_insured": _num(d.get("PCTIC_PT")),
            "pct_insured_moe": _num(d.get("PCTIC_MOE")),
            "total_population": _num(d.get("NIPR_PT")),
            "snapshot": SNAP,
        })

    if not rows:
        print("  No rows parsed")
        return 0

    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    writer = csv.DictWriter(tmp, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    tmp.close()

    con.execute("DROP TABLE IF EXISTS _sahie_138fpl")
    con.execute(f"""
        CREATE TABLE _sahie_138fpl AS
        SELECT * FROM read_csv('{tmp.name}', header=true, auto_detect=true)
    """)
    import os
    os.unlink(tmp.name)

    return _write_parquet(con, "_sahie_138fpl", "sahie_county_138fpl", dry_run)


# ── 4. ACS Disability ─────────────────────────────────────────────────

def build_acs_disability(con, dry_run=False):
    """ACS disability status by state (2023 1-year estimates)."""
    print("Building fact_acs_disability...")
    path = RAW / "acs_disability_state_2023.json"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    data = json.loads(path.read_text())
    headers = data[0]
    # B18101 = Sex by age by disability status
    # _001E = Total, _004E = Male <5 w/disability, etc.
    # We'll compute total with disability vs without
    rows = []
    for row in data[1:]:
        d = dict(zip(headers, row))
        state_fips = d.get("state", "")
        state_code = FIPS_TO_STATE.get(state_fips)
        if not state_code:
            continue
        total_pop = _num(d.get("B18101_001E"))
        # Male with disability: 004+007+010+013+016+019
        male_disabled = sum(filter(None, [
            _num(d.get("B18101_004E")), _num(d.get("B18101_007E")),
            _num(d.get("B18101_010E")), _num(d.get("B18101_013E")),
            _num(d.get("B18101_016E")), _num(d.get("B18101_019E")),
        ]))
        # Female with disability: 023+026+029+032+035+038
        female_disabled = sum(filter(None, [
            _num(d.get("B18101_023E")), _num(d.get("B18101_026E")),
            _num(d.get("B18101_029E")), _num(d.get("B18101_032E")),
            _num(d.get("B18101_035E")), _num(d.get("B18101_038E")),
        ]))
        total_disabled = male_disabled + female_disabled
        pct_disabled = round(total_disabled / total_pop * 100, 1) if total_pop else None

        rows.append({
            "state_code": state_code,
            "state_name": d.get("NAME", ""),
            "year": 2023,
            "total_population": total_pop,
            "total_with_disability": total_disabled,
            "male_with_disability": male_disabled,
            "female_with_disability": female_disabled,
            "pct_with_disability": pct_disabled,
            "source": "ACS 1-Year 2023 (B18101)",
            "snapshot": SNAP,
        })

    if not rows:
        print("  No rows parsed")
        return 0

    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    writer = csv.DictWriter(tmp, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    tmp.close()

    con.execute("DROP TABLE IF EXISTS _acs_disability")
    con.execute(f"""
        CREATE TABLE _acs_disability AS
        SELECT * FROM read_csv('{tmp.name}', header=true, auto_detect=true)
    """)
    import os
    os.unlink(tmp.name)

    return _write_parquet(con, "_acs_disability", "acs_disability", dry_run)


# ── 5. ACS Language ───────────────────────────────────────────────────

def build_acs_language(con, dry_run=False):
    """ACS language spoken at home by state (2023 1-year estimates)."""
    print("Building fact_acs_language...")
    path = RAW / "acs_language_state_2023.json"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    data = json.loads(path.read_text())
    headers = data[0]
    # C16001: Language spoken at home for the population 5+ years
    # _001E = Total, _002E = English only, _003E = Spanish, _006E = French/Haitian/Cajun
    # _009E = German/W.Germanic, _012E = Russian/Polish/Slavic, _015E = Other Indo-European
    # _018E = Korean, _021E = Chinese, _024E = Vietnamese, _027E = Tagalog
    # _030E = Other Asian/Pacific, _033E = Arabic, _036E = Other
    lang_map = {
        "C16001_002E": "english_only",
        "C16001_003E": "spanish",
        "C16001_006E": "french_haitian_cajun",
        "C16001_009E": "german_west_germanic",
        "C16001_012E": "russian_polish_slavic",
        "C16001_015E": "other_indo_european",
        "C16001_018E": "korean",
        "C16001_021E": "chinese",
        "C16001_024E": "vietnamese",
        "C16001_027E": "tagalog",
        "C16001_030E": "other_asian_pacific",
        "C16001_033E": "arabic",
        "C16001_036E": "other_language",
    }
    rows = []
    for row in data[1:]:
        d = dict(zip(headers, row))
        state_fips = d.get("state", "")
        state_code = FIPS_TO_STATE.get(state_fips)
        if not state_code:
            continue
        total_pop_5plus = _num(d.get("C16001_001E"))
        english_only = _num(d.get("C16001_002E"))
        non_english = (total_pop_5plus - english_only) if total_pop_5plus and english_only else None
        pct_non_english = round(non_english / total_pop_5plus * 100, 1) if total_pop_5plus and non_english else None

        rec = {
            "state_code": state_code,
            "state_name": d.get("NAME", ""),
            "year": 2023,
            "population_5_plus": total_pop_5plus,
            "english_only": english_only,
            "non_english_total": non_english,
            "pct_non_english": pct_non_english,
        }
        for var, col_name in lang_map.items():
            rec[col_name] = _num(d.get(var))
        rec["source"] = "ACS 1-Year 2023 (C16001)"
        rec["snapshot"] = SNAP
        rows.append(rec)

    if not rows:
        print("  No rows parsed")
        return 0

    import tempfile
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    writer = csv.DictWriter(tmp, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    tmp.close()

    con.execute("DROP TABLE IF EXISTS _acs_language")
    con.execute(f"""
        CREATE TABLE _acs_language AS
        SELECT * FROM read_csv('{tmp.name}', header=true, auto_detect=true)
    """)
    import os
    os.unlink(tmp.name)

    return _write_parquet(con, "_acs_language", "acs_language", dry_run)


# ── 6. CDC PLACES County 2025 (GIS) ──────────────────────────────────

def build_places_county_2025(con, dry_run=False):
    """CDC PLACES 2025 release - GIS-friendly county data (40 health measures)."""
    print("Building fact_places_county_2025...")
    path = RAW / "places_county_gis_2025.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _places_gis")
    con.execute(f"""
        CREATE TABLE _places_gis AS
        SELECT * FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true)
    """)

    # Get column names to understand schema
    cols = [c[0] for c in con.execute("SELECT * FROM _places_gis LIMIT 0").description]

    # Identify measure columns (pairs: MEASURE_CrudePrev, MEASURE_Crude95CI, etc.)
    measure_cols = set()
    for c in cols:
        if "_CrudePrev" in c:
            measure_cols.add(c.replace("_CrudePrev", ""))

    # Melt from wide to long format for consistent querying
    unions = []
    for m in sorted(measure_cols):
        crude_prev = f"{m}_CrudePrev"
        crude_ci = f"{m}_Crude95CI"
        adj_prev = f"{m}_AdjPrev"
        adj_ci = f"{m}_Adj95CI"
        # Only include columns that exist
        cp = f'"{crude_prev}"' if crude_prev in cols else "NULL"
        cc = f'"{crude_ci}"' if crude_ci in cols else "NULL"
        ap = f'"{adj_prev}"' if adj_prev in cols else "NULL"
        ac = f'"{adj_ci}"' if adj_ci in cols else "NULL"
        unions.append(f"""
            SELECT
                "StateAbbr" AS state_code,
                "StateDesc" AS state_name,
                "CountyName" AS county_name,
                "CountyFIPS" AS county_fips,
                CAST("TotalPopulation" AS BIGINT) AS total_population,
                CAST("TotalPop18plus" AS BIGINT) AS adult_population,
                '{m}' AS measure_id,
                CAST({cp} AS DOUBLE) AS crude_prevalence,
                CAST({cc} AS VARCHAR) AS crude_95ci,
                CAST({ap} AS DOUBLE) AS age_adjusted_prevalence,
                CAST({ac} AS VARCHAR) AS age_adjusted_95ci
            FROM _places_gis
            WHERE {cp} IS NOT NULL
        """)

    if not unions:
        print("  No measure columns found")
        return 0

    con.execute("DROP TABLE IF EXISTS _places_2025")
    con.execute(f"""
        CREATE TABLE _places_2025 AS
        SELECT *, '2025' AS release_year, '{SNAP}' AS snapshot
        FROM ({' UNION ALL '.join(unions)})
    """)

    return _write_parquet(con, "_places_2025", "places_county_2025", dry_run)


# ── 7. CDC Provisional Drug Overdose Deaths ───────────────────────────

def build_provisional_overdose(con, dry_run=False):
    """CDC VSRR provisional drug overdose deaths by state, 2015-2025."""
    print("Building fact_provisional_overdose...")
    path = RAW / "cdc_provisional_drug_overdose_2025.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _prov_od")
    con.execute(f"""
        CREATE TABLE _prov_od AS
        SELECT
            "State" AS state_code,
            "State Name" AS state_name,
            CAST("Year" AS INTEGER) AS year,
            "Month" AS month,
            "Period" AS period,
            "Indicator" AS indicator,
            TRY_CAST("Data Value" AS DOUBLE) AS data_value,
            TRY_CAST("Percent Complete" AS DOUBLE) AS pct_complete,
            TRY_CAST("Percent Pending Investigation" AS DOUBLE) AS pct_pending_investigation,
            TRY_CAST("Predicted Value" AS DOUBLE) AS predicted_value,
            "Footnote" AS footnote,
            '{SNAP}' AS snapshot
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true)
        WHERE "State" IS NOT NULL AND LENGTH("State") = 2
    """)

    return _write_parquet(con, "_prov_od", "provisional_overdose", dry_run)


# ── 8. Managed Care Enrollment by Plan ────────────────────────────────

def build_mc_enrollment_by_plan(con, dry_run=False):
    """Medicaid managed care enrollment by program and plan (2016-2024)."""
    print("Building fact_mc_enrollment_by_plan...")
    path = RAW / "mc_enrollment_by_plan_type_2024.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _mc_plan")
    con.execute(f"""
        CREATE TABLE _mc_plan AS
        SELECT
            "State" AS state_name,
            "Program Name" AS program_name,
            "Plan Name" AS plan_name,
            "Geographic Region" AS geographic_region,
            TRY_CAST(REPLACE("Medicaid-Only Enrollment", ',', '') AS BIGINT) AS medicaid_only_enrollment,
            TRY_CAST(REPLACE("Dual Enrollment", ',', '') AS BIGINT) AS dual_enrollment,
            TRY_CAST(REPLACE("Total Enrollment", ',', '') AS BIGINT) AS total_enrollment,
            CAST("Year" AS INTEGER) AS year,
            "Parent Organization" AS parent_organization,
            "Notes" AS notes,
            '{SNAP}' AS snapshot
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true)
        WHERE "State" IS NOT NULL
    """)

    # Add state_code
    con.execute("""
        ALTER TABLE _mc_plan ADD COLUMN state_code VARCHAR;
    """)
    state_updates = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
        "California": "CA", "Colorado": "CO", "Connecticut": "CT",
        "Delaware": "DE", "District of Columbia": "DC", "Florida": "FL",
        "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
        "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
        "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
        "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
        "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
        "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
        "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
        "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
        "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
        "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
        "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
        "Wisconsin": "WI", "Wyoming": "WY", "Puerto Rico": "PR",
    }
    for name, code in state_updates.items():
        con.execute(f"UPDATE _mc_plan SET state_code = '{code}' WHERE state_name = '{name}'")

    return _write_parquet(con, "_mc_plan", "mc_enrollment_by_plan", dry_run)


# ── 9. TEDS Admissions 2023 ───────────────────────────────────────────

def build_teds_admissions_2023(con, dry_run=False):
    """SAMHSA TEDS-A 2023 treatment episode admissions (~1.4M episodes)."""
    print("Building fact_teds_admissions_2023...")
    path = RAW / "teds_2023" / "tedsa_puf_2023.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _teds_2023")
    # Read the full CSV - select key clinical/demographic columns
    con.execute(f"""
        CREATE TABLE _teds_2023 AS
        SELECT
            CAST("ADMYR" AS INTEGER) AS admission_year,
            CAST("CASEID" AS BIGINT) AS case_id,
            CAST("STFIPS" AS VARCHAR) AS state_fips,
            CAST("AGE" AS INTEGER) AS age_group,
            CAST("SEX" AS INTEGER) AS sex,
            CAST("RACE" AS INTEGER) AS race,
            CAST("ETHNIC" AS INTEGER) AS ethnicity,
            CAST("EDUC" AS INTEGER) AS education,
            CAST("EMPLOY" AS INTEGER) AS employment_status,
            CAST("MARSTAT" AS INTEGER) AS marital_status,
            CAST("SERVICES" AS INTEGER) AS service_setting,
            CAST("DETCRIM" AS INTEGER) AS criminal_justice_referral,
            CAST("NOPRIOR" AS INTEGER) AS prior_treatment_episodes,
            CAST("PSOURCE" AS INTEGER) AS referral_source,
            CAST("SUB1" AS INTEGER) AS primary_substance,
            CAST("SUB2" AS INTEGER) AS secondary_substance,
            CAST("SUB3" AS INTEGER) AS tertiary_substance,
            CAST("ROUTE1" AS INTEGER) AS primary_route,
            CAST("FREQ1" AS INTEGER) AS primary_frequency,
            CAST("FRSTUSE1" AS INTEGER) AS primary_first_use_age,
            CAST("HLTHINS" AS INTEGER) AS health_insurance,
            CAST("PRIMPAY" AS INTEGER) AS primary_payment,
            CAST("PSYPROB" AS INTEGER) AS psychiatric_problem,
            CAST("PREG" AS INTEGER) AS pregnant,
            CAST("VET" AS INTEGER) AS veteran,
            CAST("METHUSE" AS INTEGER) AS medication_assisted_therapy,
            CAST("DAYWAIT" AS INTEGER) AS days_waiting,
            CAST("DSMCRIT" AS INTEGER) AS dsm_criteria,
            CAST("LIVARAG" AS INTEGER) AS living_arrangement,
            CAST("ARRESTS" AS INTEGER) AS arrests_past_30_days,
            CAST("FREQ_ATND_SELF_HELP" AS INTEGER) AS self_help_attendance,
            '{SNAP}' AS snapshot
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true,
                      sample_size=100000)
    """)

    # Map state FIPS to state codes
    con.execute("ALTER TABLE _teds_2023 ADD COLUMN state_code VARCHAR")
    for fips, code in FIPS_TO_STATE.items():
        con.execute(f"UPDATE _teds_2023 SET state_code = '{code}' WHERE state_fips = '{fips}'")
    # Also handle numeric FIPS (without leading zero)
    for fips, code in FIPS_TO_STATE.items():
        if fips.startswith("0"):
            con.execute(f"UPDATE _teds_2023 SET state_code = '{code}' WHERE state_fips = '{fips[1:]}'")

    return _write_parquet(con, "_teds_2023", "teds_admissions_2023", dry_run)


# ── 10. N-SUMHSS Facility Survey 2024 ────────────────────────────────

def build_nsumhss_facility(con, dry_run=False):
    """SAMHSA N-SUMHSS 2024 facility-level survey (21K+ facilities)."""
    print("Building fact_nsumhss_facility...")
    path = RAW / "nsumhss_2024" / "NSUMHSS_2024_PUF_CSV.csv"
    if not path.exists():
        print("  SKIPPED - file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _nsumhss")
    # Read with auto-detect, selecting key columns
    con.execute(f"""
        CREATE TABLE _nsumhss_raw AS
        SELECT *
        FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true,
                      sample_size=50000)
    """)

    # Get column names
    cols = [c[0] for c in con.execute("SELECT * FROM _nsumhss_raw LIMIT 0").description]

    # Select the most relevant columns for Medicaid intelligence
    # Key columns that are likely present
    select_cols = []
    col_map = {
        "MPRID": "facility_id",
        "LOCATIONSTATE": "state_fips",
        "FOCUS": "facility_focus",           # SU, MH, or both
        "CTYPE4": "accepts_medicaid",
        "CTYPEHI1": "accepts_medicare",
        "CTYPEHI2": "accepts_private_insurance",
        "CTYPE7": "accepts_self_pay",
        "ADMIN": "administrative_organization",
        "INSU": "treats_substance_use",
        "INMH": "treats_mental_health",
        "SUTRTMNTALSO": "also_treats_substance_use",
        "JAIL": "serves_criminal_justice",
        "DETOX": "provides_detox",
        "TREATMT_SU": "su_treatment_programs",
        "MHTXSA": "mh_treatment_for_su",
        "MHTXNONSA": "mh_treatment_non_su",
        "NOMHTX": "no_mh_treatment",
        "DETXALC": "detox_alcohol",
        "DETXBEN": "detox_benzodiazepines",
        "DETXCOC": "detox_cocaine",
        "DETXMET": "detox_methamphetamine",
        "DETXOP": "detox_opioids",
        "DETXOTH": "detox_other",
        "DETXMED": "detox_medication_assisted",
    }

    available = []
    for orig, renamed in col_map.items():
        if orig in cols:
            available.append(f'CAST("{orig}" AS VARCHAR) AS {renamed}')

    if not available:
        print("  No recognized columns found")
        return 0

    con.execute("DROP TABLE IF EXISTS _nsumhss")
    con.execute(f"""
        CREATE TABLE _nsumhss AS
        SELECT
            {', '.join(available)},
            '{SNAP}' AS snapshot
        FROM _nsumhss_raw
    """)

    # Map state FIPS to state codes
    con.execute("ALTER TABLE _nsumhss ADD COLUMN state_code VARCHAR")
    for fips, code in FIPS_TO_STATE.items():
        con.execute(f"UPDATE _nsumhss SET state_code = '{code}' WHERE state_fips = '{fips}'")
        if fips.startswith("0"):
            con.execute(f"UPDATE _nsumhss SET state_code = '{code}' WHERE state_fips = '{fips[1:]}'")

    con.execute("DROP TABLE IF EXISTS _nsumhss_raw")
    return _write_parquet(con, "_nsumhss", "nsumhss_facility", dry_run)


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest data gap datasets")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", help="Only build specific table")
    args = parser.parse_args()

    con = duckdb.connect()

    builders = {
        "sahie_county": build_sahie_county,
        "sahie_state": build_sahie_state,
        "sahie_county_138fpl": build_sahie_county_138fpl,
        "acs_disability": build_acs_disability,
        "acs_language": build_acs_language,
        "places_county_2025": build_places_county_2025,
        "provisional_overdose": build_provisional_overdose,
        "mc_enrollment_by_plan": build_mc_enrollment_by_plan,
        "teds_admissions_2023": build_teds_admissions_2023,
        "nsumhss_facility": build_nsumhss_facility,
    }

    total_rows = 0
    total_tables = 0

    if args.table:
        if args.table in builders:
            rows = builders[args.table](con, args.dry_run)
            if rows > 0:
                total_tables += 1
                total_rows += rows
        else:
            print(f"Unknown table: {args.table}")
            print(f"Available: {', '.join(builders.keys())}")
            return
    else:
        for name, builder in builders.items():
            try:
                rows = builder(con, args.dry_run)
                if rows > 0:
                    total_tables += 1
                    total_rows += rows
            except Exception as e:
                print(f"  ERROR building {name}: {e}")
                import traceback
                traceback.print_exc()

    print(f"\nDone: {total_tables} tables, {total_rows:,} total rows")
    con.close()


if __name__ == "__main__":
    main()
