#!/usr/bin/env python3
"""
build_lake_sdoh.py — Ingest social determinants of health (SDOH) and
community context data into the Aradune data lake.

Reads from: data/raw/sdoh/
Writes to:  data/lake/

Tables built:
  fact_county_health_rankings   — 90 health outcome/factor measures, 3,200+ counties (CHR 2025)
  ref_ruca_codes_2020           — Rural-Urban Commuting Area codes, 85K+ census tracts (USDA 2020)

Blocked / skipped:
  fact_svi_county               — CDC SVI: WAF-blocked (403)
  fact_child_opportunity_index  — diversitydatakids.org: 403
  fact_ahrq_sdoh                — AHRQ: WAF-blocked (202)
  fact_food_environment         — Already exists (957K rows, snapshot 2026-03-09)

Usage:
  python3 scripts/build_lake_sdoh.py
  python3 scripts/build_lake_sdoh.py --dry-run
  python3 scripts/build_lake_sdoh.py --only fact_county_health_rankings
"""

import argparse
import csv
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "sdoh"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
REF_DIR = LAKE_DIR / "reference"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# State FIPS to state code mapping
STATE_FIPS_TO_CODE = {
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
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

# CHR measure definitions: var_code -> (measure_name, domain, subdomain)
CHR_MEASURES = {
    "v001": ("Premature Death", "Health Outcomes", "Length of Life"),
    "v002": ("Poor or Fair Health", "Health Outcomes", "Quality of Life"),
    "v003": ("Uninsured Adults", "Clinical Care", "Access to Care"),
    "v004": ("Primary Care Physicians", "Clinical Care", "Access to Care"),
    "v005": ("Preventable Hospital Stays", "Clinical Care", "Quality of Care"),
    "v009": ("Adult Smoking", "Health Behaviors", "Tobacco Use"),
    "v011": ("Adult Obesity", "Health Behaviors", "Diet & Exercise"),
    "v014": ("Teen Births", "Health Behaviors", "Sexual Activity"),
    "v015": ("Homicides", "Social & Economic Factors", "Community Safety"),
    "v021": ("High School Graduation", "Social & Economic Factors", "Education"),
    "v023": ("Unemployment", "Social & Economic Factors", "Employment"),
    "v024": ("Children in Poverty", "Social & Economic Factors", "Income"),
    "v036": ("Poor Physical Health Days", "Health Outcomes", "Quality of Life"),
    "v037": ("Low Birth Weight", "Health Outcomes", "Quality of Life"),
    "v039": ("Motor Vehicle Crash Deaths", "Health Behaviors", "Alcohol & Drug Use"),
    "v042": ("Poor Mental Health Days", "Health Outcomes", "Quality of Life"),
    "v044": ("Income Inequality", "Social & Economic Factors", "Income"),
    "v045": ("Sexually Transmitted Infections", "Health Behaviors", "Sexual Activity"),
    "v049": ("Excessive Drinking", "Health Behaviors", "Alcohol & Drug Use"),
    "v050": ("Mammography Screening", "Clinical Care", "Quality of Care"),
    "v051": ("Population", "Demographics", "Demographics"),
    "v052": ("Pct Below 18", "Demographics", "Demographics"),
    "v053": ("Pct 65 and Older", "Demographics", "Demographics"),
    "v054": ("Pct Non-Hispanic Black", "Demographics", "Demographics"),
    "v055": ("Pct American Indian or Alaska Native", "Demographics", "Demographics"),
    "v056": ("Pct Hispanic", "Demographics", "Demographics"),
    "v057": ("Pct Female", "Demographics", "Demographics"),
    "v058": ("Pct Rural", "Demographics", "Demographics"),
    "v059": ("Pct Not Proficient in English", "Social & Economic Factors", "Community"),
    "v060": ("Diabetes Prevalence", "Health Behaviors", "Diet & Exercise"),
    "v061": ("HIV Prevalence", "Health Behaviors", "Sexual Activity"),
    "v062": ("Mental Health Providers", "Clinical Care", "Access to Care"),
    "v063": ("Median Household Income", "Social & Economic Factors", "Income"),
    "v065": ("Children Eligible Free Reduced Lunch", "Social & Economic Factors", "Income"),
    "v067": ("Driving Alone to Work", "Physical Environment", "Housing & Transit"),
    "v069": ("Some College", "Social & Economic Factors", "Education"),
    "v070": ("Physical Inactivity", "Health Behaviors", "Diet & Exercise"),
    "v080": ("Pct Native Hawaiian or Pacific Islander", "Demographics", "Demographics"),
    "v081": ("Pct Asian", "Demographics", "Demographics"),
    "v082": ("Children in Single-Parent Households", "Social & Economic Factors", "Family & Social"),
    "v083": ("Limited Access to Healthy Foods", "Health Behaviors", "Diet & Exercise"),
    "v085": ("Uninsured", "Clinical Care", "Access to Care"),
    "v088": ("Dentists", "Clinical Care", "Access to Care"),
    "v122": ("Uninsured Children", "Clinical Care", "Access to Care"),
    "v124": ("Drinking Water Violations", "Physical Environment", "Environmental Quality"),
    "v125": ("Air Pollution Particulate Matter", "Physical Environment", "Environmental Quality"),
    "v126": ("Pct Non-Hispanic White", "Demographics", "Demographics"),
    "v127": ("Premature Age-Adjusted Mortality", "Health Outcomes", "Length of Life"),
    "v128": ("Child Mortality", "Health Outcomes", "Length of Life"),
    "v129": ("Infant Mortality", "Health Outcomes", "Length of Life"),
    "v131": ("Other Primary Care Providers", "Clinical Care", "Access to Care"),
    "v132": ("Access to Exercise Opportunities", "Health Behaviors", "Diet & Exercise"),
    "v133": ("Food Environment Index", "Health Behaviors", "Diet & Exercise"),
    "v134": ("Alcohol-Impaired Driving Deaths", "Health Behaviors", "Alcohol & Drug Use"),
    "v135": ("Injury Deaths", "Health Outcomes", "Length of Life"),
    "v136": ("Severe Housing Problems", "Physical Environment", "Housing & Transit"),
    "v137": ("Long Commute Driving Alone", "Physical Environment", "Housing & Transit"),
    "v138": ("Drug Overdose Deaths", "Health Behaviors", "Alcohol & Drug Use"),
    "v139": ("Food Insecurity", "Health Behaviors", "Diet & Exercise"),
    "v140": ("Social Associations", "Social & Economic Factors", "Family & Social"),
    "v141": ("Residential Segregation Black White", "Social & Economic Factors", "Community"),
    "v143": ("Insufficient Sleep", "Health Behaviors", "Diet & Exercise"),
    "v144": ("Frequent Physical Distress", "Health Outcomes", "Quality of Life"),
    "v145": ("Frequent Mental Distress", "Health Outcomes", "Quality of Life"),
    "v147": ("Life Expectancy", "Health Outcomes", "Length of Life"),
    "v148": ("Firearm Fatalities", "Social & Economic Factors", "Community Safety"),
    "v149": ("Disconnected Youth", "Social & Economic Factors", "Education"),
    "v151": ("Gender Pay Gap", "Social & Economic Factors", "Income"),
    "v153": ("Homeownership", "Physical Environment", "Housing & Transit"),
    "v154": ("Severe Housing Cost Burden", "Physical Environment", "Housing & Transit"),
    "v155": ("Flu Vaccinations", "Clinical Care", "Quality of Care"),
    "v156": ("Traffic Volume", "Physical Environment", "Housing & Transit"),
    "v159": ("Reading Scores", "Social & Economic Factors", "Education"),
    "v160": ("Math Scores", "Social & Economic Factors", "Education"),
    "v161": ("Suicides", "Health Outcomes", "Length of Life"),
    "v166": ("Broadband Access", "Social & Economic Factors", "Community"),
    "v167": ("School Segregation", "Social & Economic Factors", "Education"),
    "v168": ("High School Completion", "Social & Economic Factors", "Education"),
    "v169": ("School Funding Adequacy", "Social & Economic Factors", "Education"),
    "v170": ("Living Wage", "Social & Economic Factors", "Income"),
    "v171": ("Child Care Cost Burden", "Social & Economic Factors", "Family & Social"),
    "v172": ("Child Care Centers", "Social & Economic Factors", "Family & Social"),
    "v177": ("Voter Turnout", "Social & Economic Factors", "Community"),
    "v178": ("Census Participation", "Social & Economic Factors", "Community"),
    "v179": ("Access to Parks", "Physical Environment", "Environmental Quality"),
    "v180": ("Pct Disability Functional Limitations", "Demographics", "Demographics"),
    "v181": ("Library Access", "Social & Economic Factors", "Community"),
    "v182": ("Adverse Climate Events", "Physical Environment", "Environmental Quality"),
    "v183": ("Feelings of Loneliness", "Health Outcomes", "Quality of Life"),
    "v184": ("Lack of Social and Emotional Support", "Social & Economic Factors", "Family & Social"),
}


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


def _ref_path(ref_name: str) -> Path:
    return REF_DIR / f"{ref_name}.parquet"


# ---------------------------------------------------------------------------
# County Health Rankings (2025 release, 90 measures, 3,200+ counties)
# ---------------------------------------------------------------------------

def build_fact_county_health_rankings(con, dry_run: bool) -> int:
    """
    Transforms County Health Rankings wide CSV (796 cols) into a long-format
    fact table: one row per county per measure. This makes it queryable for
    any combination of measures, counties, and states.

    Output columns:
      fips, state_fips, state_code, county_name, year,
      measure_code, measure_name, domain, subdomain,
      raw_value, ci_low, ci_high, numerator, denominator, flag,
      source, snapshot_date
    """
    print("Building fact_county_health_rankings...")
    csv_path = RAW_DIR / "county_health_rankings_2025.csv"
    if not csv_path.exists():
        print(f"  SKIPPED - {csv_path.name} not found")
        return 0

    # Parse with Python csv module (CHR has 2-row header: descriptive + codes)
    rows = []
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        desc_header = next(reader)  # Row 1: descriptive names
        code_header = next(reader)  # Row 2: variable codes

        # Build column index: code -> position
        col_idx = {code: i for i, code in enumerate(code_header)}

        for data_row in reader:
            fips = data_row[col_idx["fipscode"]]
            state_abbr = data_row[col_idx["state"]]
            county = data_row[col_idx["county"]]
            year = data_row[col_idx["year"]]
            state_fips = data_row[col_idx["statecode"]]

            # Skip national row and state summary rows (county FIPS = 000)
            county_fips = data_row[col_idx["countycode"]]
            if county_fips == "000":
                continue

            # Derive state_code from FIPS if state_abbr not standard
            state_code = state_abbr if len(state_abbr) == 2 else STATE_FIPS_TO_CODE.get(state_fips, "")

            for var_code, (measure_name, domain, subdomain) in CHR_MEASURES.items():
                raw_col = f"{var_code}_rawvalue"
                ci_low_col = f"{var_code}_cilow"
                ci_high_col = f"{var_code}_cihigh"
                num_col = f"{var_code}_numerator"
                den_col = f"{var_code}_denominator"
                flag_col = f"{var_code}_flag"

                def _get(col_name):
                    idx = col_idx.get(col_name)
                    if idx is None:
                        return None
                    val = data_row[idx].strip()
                    if val == "" or val == ".":
                        return None
                    return val

                raw_val = _get(raw_col)
                if raw_val is None:
                    continue  # Skip measures with no value for this county

                rows.append({
                    "fips": fips,
                    "state_fips": state_fips,
                    "state_code": state_code,
                    "county_name": county,
                    "year": int(year) if year else None,
                    "measure_code": var_code,
                    "measure_name": measure_name,
                    "domain": domain,
                    "subdomain": subdomain,
                    "raw_value": float(raw_val) if raw_val else None,
                    "ci_low": float(_get(ci_low_col)) if _get(ci_low_col) else None,
                    "ci_high": float(_get(ci_high_col)) if _get(ci_high_col) else None,
                    "numerator": float(_get(num_col)) if _get(num_col) else None,
                    "denominator": float(_get(den_col)) if _get(den_col) else None,
                    "flag": int(float(_get(flag_col))) if _get(flag_col) else None,
                    "source": "countyhealthrankings.org",
                    "snapshot_date": SNAPSHOT_DATE,
                })

    if not rows:
        print("  SKIPPED - no data rows parsed")
        return 0

    print(f"  Parsed {len(rows):,} measure-county rows from CSV")

    # Load into DuckDB
    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _chr AS SELECT * FROM df")

    # Cast types
    con.execute(f"""
        CREATE OR REPLACE TABLE _fact_chr AS
        SELECT
            CAST(fips AS VARCHAR) AS fips,
            CAST(state_fips AS VARCHAR) AS state_fips,
            CAST(state_code AS VARCHAR) AS state_code,
            CAST(county_name AS VARCHAR) AS county_name,
            CAST(year AS INTEGER) AS year,
            CAST(measure_code AS VARCHAR) AS measure_code,
            CAST(measure_name AS VARCHAR) AS measure_name,
            CAST(domain AS VARCHAR) AS domain,
            CAST(subdomain AS VARCHAR) AS subdomain,
            CAST(raw_value AS DOUBLE) AS raw_value,
            CAST(ci_low AS DOUBLE) AS ci_low,
            CAST(ci_high AS DOUBLE) AS ci_high,
            CAST(numerator AS DOUBLE) AS numerator,
            CAST(denominator AS DOUBLE) AS denominator,
            CAST(flag AS INTEGER) AS flag,
            CAST(source AS VARCHAR) AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _chr
    """)

    count = write_parquet(con, "_fact_chr", _snapshot_path("county_health_rankings"), dry_run)

    # Print summary
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _fact_chr").fetchone()[0]
    counties = con.execute("SELECT COUNT(DISTINCT fips) FROM _fact_chr").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_code) FROM _fact_chr").fetchone()[0]
    domains = con.execute("SELECT COUNT(DISTINCT domain) FROM _fact_chr").fetchone()[0]
    print(f"  {count:,} rows, {states} states, {counties:,} counties, {measures} measures, {domains} domains")

    con.execute("DROP TABLE IF EXISTS _chr")
    con.execute("DROP TABLE IF EXISTS _fact_chr")
    return count


# ---------------------------------------------------------------------------
# RUCA 2020 Codes (census tract level, 85K+ tracts)
# ---------------------------------------------------------------------------

def build_ref_ruca_codes_2020(con, dry_run: bool) -> int:
    """
    Processes USDA 2020 RUCA codes (tract-level) into a clean reference table.
    Includes primary and secondary RUCA codes, descriptions, population, and
    a simplified urban/rural classification.

    RUCA primary codes:
      1 = Metropolitan core
      2 = Metropolitan high commuting
      3 = Metropolitan low commuting
      4 = Micropolitan core
      5 = Micropolitan high commuting
      6 = Micropolitan low commuting
      7 = Small town core
      8 = Small town high commuting
      9 = Small town low commuting
      10 = Rural
    """
    print("Building ref_ruca_codes_2020...")
    csv_path = RAW_DIR / "ruca_2020_tract.csv"
    if not csv_path.exists():
        print(f"  SKIPPED - {csv_path.name} not found")
        return 0

    # Read with latin1 encoding (Spanish place names in PR)
    rows = []
    with open(csv_path, "r", encoding="latin1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            primary_ruca = row.get("PrimaryRUCA", "").strip()
            if not primary_ruca:
                continue

            try:
                primary_ruca_int = int(float(primary_ruca))
            except (ValueError, TypeError):
                continue

            # Simplified urban/rural classification
            if primary_ruca_int <= 3:
                urban_rural = "Metropolitan"
            elif primary_ruca_int <= 6:
                urban_rural = "Micropolitan"
            elif primary_ruca_int <= 9:
                urban_rural = "Small Town"
            else:
                urban_rural = "Rural"

            tract_fips = row.get("TractFIPS20", "").strip()
            state_fips = row.get("StateFIPS20", "").strip()
            state_code = STATE_FIPS_TO_CODE.get(state_fips, "")

            def _try_float(val):
                try:
                    v = val.strip() if val else ""
                    return float(v) if v else None
                except (ValueError, TypeError):
                    return None

            def _try_int(val):
                try:
                    v = val.strip() if val else ""
                    return int(float(v)) if v else None
                except (ValueError, TypeError):
                    return None

            rows.append({
                "tract_fips": tract_fips,
                "county_fips": row.get("CountyFIPS20", "").strip(),
                "county_name": row.get("CountyName20", "").strip(),
                "state_fips": state_fips,
                "state_name": row.get("StateName20", "").strip(),
                "state_code": state_code,
                "primary_ruca": _try_float(primary_ruca),
                "primary_ruca_description": row.get("PrimaryRUCADescription", "").strip(),
                "secondary_ruca": _try_float(row.get("SecondaryRUCA", "")),
                "secondary_ruca_description": row.get("SecondaryRUCADescription", "").strip(),
                "urban_area_code": row.get("UrbanAreaCode20", "").strip(),
                "urban_area_name": row.get("UrbanAreaName20", "").strip(),
                "urban_rural_class": urban_rural,
                "population": _try_int(row.get("Population", "")),
                "land_area_sq_mi": _try_float(row.get("LandArea", "")),
                "pop_density": _try_float(row.get("PopDensity", "")),
                "source": "ers.usda.gov",
                "snapshot_date": SNAPSHOT_DATE,
            })

    if not rows:
        print("  SKIPPED - no data rows parsed")
        return 0

    print(f"  Parsed {len(rows):,} tract rows from CSV")

    import pandas as pd
    df = pd.DataFrame(rows)
    con.execute("CREATE OR REPLACE TABLE _ruca AS SELECT * FROM df")

    con.execute(f"""
        CREATE OR REPLACE TABLE _ref_ruca AS
        SELECT
            CAST(tract_fips AS VARCHAR) AS tract_fips,
            CAST(county_fips AS VARCHAR) AS county_fips,
            CAST(county_name AS VARCHAR) AS county_name,
            CAST(state_fips AS VARCHAR) AS state_fips,
            CAST(state_name AS VARCHAR) AS state_name,
            CAST(state_code AS VARCHAR) AS state_code,
            CAST(primary_ruca AS DOUBLE) AS primary_ruca,
            CAST(primary_ruca_description AS VARCHAR) AS primary_ruca_description,
            CAST(secondary_ruca AS DOUBLE) AS secondary_ruca,
            CAST(secondary_ruca_description AS VARCHAR) AS secondary_ruca_description,
            CAST(urban_area_code AS VARCHAR) AS urban_area_code,
            CAST(urban_area_name AS VARCHAR) AS urban_area_name,
            CAST(urban_rural_class AS VARCHAR) AS urban_rural_class,
            CAST(population AS INTEGER) AS population,
            CAST(land_area_sq_mi AS DOUBLE) AS land_area_sq_mi,
            CAST(pop_density AS DOUBLE) AS pop_density,
            CAST(source AS VARCHAR) AS source,
            DATE '{SNAPSHOT_DATE}' AS snapshot_date
        FROM _ruca
    """)

    out_path = _ref_path("ruca_codes_2020")
    count = con.execute("SELECT COUNT(*) FROM _ref_ruca").fetchone()[0]
    if not dry_run and count > 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY _ref_ruca TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_mb = out_path.stat().st_size / (1024 * 1024)
        print(f"  -> {out_path.relative_to(LAKE_DIR)} ({count:,} rows, {size_mb:.1f} MB)")
    elif dry_run:
        print(f"  [dry-run] {out_path.relative_to(LAKE_DIR)} ({count:,} rows)")

    # Print summary
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _ref_ruca WHERE state_code != ''").fetchone()[0]
    metro = con.execute("SELECT COUNT(*) FROM _ref_ruca WHERE urban_rural_class = 'Metropolitan'").fetchone()[0]
    micro = con.execute("SELECT COUNT(*) FROM _ref_ruca WHERE urban_rural_class = 'Micropolitan'").fetchone()[0]
    small = con.execute("SELECT COUNT(*) FROM _ref_ruca WHERE urban_rural_class = 'Small Town'").fetchone()[0]
    rural = con.execute("SELECT COUNT(*) FROM _ref_ruca WHERE urban_rural_class = 'Rural'").fetchone()[0]
    print(f"  {count:,} tracts, {states} states")
    print(f"  Classification: Metropolitan={metro:,}, Micropolitan={micro:,}, Small Town={small:,}, Rural={rural:,}")

    con.execute("DROP TABLE IF EXISTS _ruca")
    con.execute("DROP TABLE IF EXISTS _ref_ruca")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_TABLES = {
    "fact_county_health_rankings": build_fact_county_health_rankings,
    "ref_ruca_codes_2020": build_ref_ruca_codes_2020,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest SDOH/community context data into Aradune lake")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", type=str, default=None)
    args = parser.parse_args()

    tables = list(ALL_TABLES.keys())
    if args.only:
        tables = [t.strip() for t in args.only.split(",")]

    print(f"Snapshot: {SNAPSHOT_DATE}")
    print(f"Run ID:   {RUN_ID}")
    print(f"Building: {', '.join(tables)}")
    print()

    # Report on blocked datasets
    print("=" * 60)
    print("BLOCKED / SKIPPED DATASETS")
    print("=" * 60)
    print("  fact_svi_county              — CDC/ATSDR WAF-blocked (403)")
    print("  fact_child_opportunity_index  — diversitydatakids.org (403)")
    print("  fact_ahrq_sdoh               — AHRQ WAF-blocked (202)")
    print("  fact_food_environment         — Already in lake (957K rows)")
    print()

    con = duckdb.connect()
    totals = {}
    for name in tables:
        if name in ALL_TABLES:
            totals[name] = ALL_TABLES[name](con, args.dry_run)
        else:
            print(f"  UNKNOWN table: {name}")
            totals[name] = 0
        print()

    con.close()

    print("=" * 60)
    print("SDOH DATA LAKE INGESTION COMPLETE")
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
            "blocked": [
                "fact_svi_county (CDC WAF 403)",
                "fact_child_opportunity_index (403)",
                "fact_ahrq_sdoh (AHRQ WAF 202)",
            ],
            "skipped": [
                "fact_food_environment (already exists, 957K rows)",
            ],
        }
        manifest_file = META_DIR / f"manifest_sdoh_{SNAPSHOT_DATE}.json"
        with open(manifest_file, "w") as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest: {manifest_file}")

    print("\nDone.")


if __name__ == "__main__":
    main()
