"""
Round 12 data ingestion: SAIPE poverty estimates, CDC PLACES county health,
HRSA health center sites, marketplace OEP enrollment.

Produces:
  - fact_saipe_poverty — Census SAIPE 2023 poverty & income by state and county
  - fact_places_county — CDC PLACES county health estimates (2025 release)
  - fact_health_center_sites — HRSA FQHC/look-alike site directory
  - fact_marketplace_oep — ACA Marketplace open enrollment by state (2025 OEP)
"""

import csv
import duckdb
import json
import xlrd
import re
import zipfile
from pathlib import Path
from datetime import date

LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
RAW  = Path(__file__).resolve().parent.parent / "data" / "raw"
SNAP = str(date.today())


def _num(val):
    """Parse numeric value, returning None for blanks/errors."""
    if val is None or val == '' or val == '.':
        return None
    if isinstance(val, (int, float)):
        return val if val != '' else None
    val = str(val).strip().replace(',', '')
    try:
        return float(val)
    except ValueError:
        return None


# ── 1. SAIPE Poverty & Income ───────────────────────────────────────────

def ingest_saipe(con):
    """Ingest Census SAIPE 2023 state & county poverty + median income."""
    path = RAW / "saipe_2023.xls"
    if not path.exists():
        print("  SAIPE: file not found")
        return 0

    wb = xlrd.open_workbook(str(path))
    sheet = wb.sheets()[0]

    rows = []
    for r in range(4, sheet.nrows):  # Data starts at row 4
        state_fips = str(sheet.cell(r, 0).value).strip()
        county_fips = str(sheet.cell(r, 1).value).strip()
        state_code = str(sheet.cell(r, 2).value).strip()
        name = str(sheet.cell(r, 3).value).strip()

        if not state_code or state_code == '':
            continue

        is_state = (county_fips == '000')

        rows.append({
            'state_code': state_code,
            'state_fips': state_fips,
            'county_fips': county_fips,
            'name': name,
            'geo_level': 'state' if is_state else 'county',
            'year': 2023,
            'poverty_estimate_all': _num(sheet.cell(r, 4).value),
            'poverty_pct_all': _num(sheet.cell(r, 7).value),
            'poverty_estimate_0_17': _num(sheet.cell(r, 10).value),
            'poverty_pct_0_17': _num(sheet.cell(r, 13).value),
            'poverty_estimate_5_17_families': _num(sheet.cell(r, 16).value),
            'poverty_pct_5_17_families': _num(sheet.cell(r, 19).value),
            'median_household_income': _num(sheet.cell(r, 22).value),
            'poverty_estimate_0_4': _num(sheet.cell(r, 25).value) if sheet.ncols > 25 else None,
            'poverty_pct_0_4': _num(sheet.cell(r, 28).value) if sheet.ncols > 28 else None,
        })

    if not rows:
        return 0

    con.execute("DROP TABLE IF EXISTS _saipe")
    con.execute("""
        CREATE TABLE _saipe (
            state_code VARCHAR, state_fips VARCHAR, county_fips VARCHAR,
            name VARCHAR, geo_level VARCHAR, year INTEGER,
            poverty_estimate_all BIGINT, poverty_pct_all DOUBLE,
            poverty_estimate_0_17 BIGINT, poverty_pct_0_17 DOUBLE,
            poverty_estimate_5_17_families BIGINT, poverty_pct_5_17_families DOUBLE,
            median_household_income BIGINT,
            poverty_estimate_0_4 BIGINT, poverty_pct_0_4 DOUBLE
        )
    """)
    con.executemany(
        "INSERT INTO _saipe VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r['state_code'], r['state_fips'], r['county_fips'], r['name'],
          r['geo_level'], r['year'],
          int(r['poverty_estimate_all']) if r['poverty_estimate_all'] else None,
          r['poverty_pct_all'],
          int(r['poverty_estimate_0_17']) if r['poverty_estimate_0_17'] else None,
          r['poverty_pct_0_17'],
          int(r['poverty_estimate_5_17_families']) if r['poverty_estimate_5_17_families'] else None,
          r['poverty_pct_5_17_families'],
          int(r['median_household_income']) if r['median_household_income'] else None,
          int(r['poverty_estimate_0_4']) if r['poverty_estimate_0_4'] else None,
          r['poverty_pct_0_4']) for r in rows]
    )

    out_dir = LAKE / "fact" / "saipe_poverty" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"COPY _saipe TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")

    states = [r for r in rows if r['geo_level'] == 'state']
    counties = [r for r in rows if r['geo_level'] == 'county']
    print(f"  saipe_poverty: {len(rows):,} rows ({len(states)} states, {len(counties):,} counties)")
    us_row = [r for r in rows if r['state_code'] == 'US']
    if us_row:
        print(f"    US 2023: {us_row[0]['poverty_pct_all']}% poverty, ${us_row[0]['median_household_income']:,.0f} median income")
    return len(rows)


# ── 2. CDC PLACES County Health ─────────────────────────────────────────

def ingest_places(con):
    """Ingest CDC PLACES county-level health estimates (2025 release)."""
    path = RAW / "places_county_2025.csv"
    if not path.exists():
        print("  CDC PLACES: file not found (still downloading?)")
        return 0

    # Read with DuckDB directly — much faster for large CSVs
    con.execute("DROP TABLE IF EXISTS _places")
    con.execute(f"""
        CREATE TABLE _places AS
        SELECT
            Year::INTEGER AS year,
            StateAbbr AS state_code,
            LocationName AS county_name,
            Category AS category,
            MeasureId AS measure_id,
            Short_Question_Text AS measure_name,
            Data_Value::DOUBLE AS data_value,
            Data_Value_Unit AS value_unit,
            Data_Value_Type AS value_type,
            Low_Confidence_Limit::DOUBLE AS ci_lower,
            High_Confidence_Limit::DOUBLE AS ci_upper,
            TotalPopulation::BIGINT AS total_population,
            LocationID AS county_fips
        FROM read_csv('{path}', header=true, auto_detect=true)
        WHERE StateAbbr IS NOT NULL
          AND Data_Value IS NOT NULL
    """)

    cnt = con.execute("SELECT COUNT(*) FROM _places").fetchone()[0]
    measures = con.execute("SELECT COUNT(DISTINCT measure_id) FROM _places").fetchone()[0]
    states = con.execute("SELECT COUNT(DISTINCT state_code) FROM _places").fetchone()[0]
    counties = con.execute("SELECT COUNT(DISTINCT county_fips) FROM _places").fetchone()[0]

    out_dir = LAKE / "fact" / "places_county" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"COPY _places TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")

    print(f"  places_county: {cnt:,} rows ({states} states, {counties:,} counties, {measures} measures)")

    # Show sample measures
    sample = con.execute("SELECT DISTINCT measure_id, measure_name FROM _places ORDER BY measure_id LIMIT 10").fetchall()
    for m_id, m_name in sample:
        print(f"    {m_id}: {m_name}")

    return cnt


# ── 3. HRSA Health Center Sites ─────────────────────────────────────────

def ingest_health_centers(con):
    """Ingest HRSA FQHC/look-alike health center site directory."""
    path = RAW / "hrsa_health_centers.csv"
    if not path.exists():
        print("  HRSA health centers: file not found")
        return 0

    con.execute("DROP TABLE IF EXISTS _hc")
    try:
        con.execute(f"""
            CREATE TABLE _hc AS
            SELECT *
            FROM read_csv('{path}', header=true, auto_detect=true, ignore_errors=true)
        """)
    except Exception as e:
        print(f"  HRSA health centers: error reading CSV: {e}")
        return 0

    cnt = con.execute("SELECT COUNT(*) FROM _hc").fetchone()[0]
    cols = [r[0] for r in con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _hc)").fetchall()]

    out_dir = LAKE / "fact" / "health_center_sites" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"COPY _hc TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")

    print(f"  health_center_sites: {cnt:,} rows")
    print(f"    Columns: {', '.join(cols[:10])}...")
    return cnt


# ── 4. Marketplace OEP Enrollment ───────────────────────────────────────

def ingest_marketplace_oep(con):
    """Ingest 2025 Marketplace Open Enrollment state-level data."""
    zip_path = RAW / "marketplace_oep_2025_state.zip"
    if not zip_path.exists():
        print("  Marketplace OEP: ZIP not found")
        return 0

    # Extract CSV from ZIP
    extract_dir = RAW / "marketplace_oep_2025"
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)
        csv_files = list(extract_dir.glob("*.csv"))
        xlsx_files = list(extract_dir.glob("*.xlsx"))

    data_file = None
    if csv_files:
        data_file = csv_files[0]
    elif xlsx_files:
        data_file = xlsx_files[0]

    if not data_file:
        print(f"  Marketplace OEP: no data file found in ZIP. Contents: {[f.name for f in extract_dir.iterdir()]}")
        return 0

    print(f"  Reading: {data_file.name}")

    con.execute("DROP TABLE IF EXISTS _mkt")
    try:
        if data_file.suffix == '.csv':
            con.execute(f"""
                CREATE TABLE _mkt AS
                SELECT * FROM read_csv('{data_file}', header=true, auto_detect=true, ignore_errors=true)
            """)
        else:
            # Excel
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute(f"""
                CREATE TABLE _mkt AS
                SELECT * FROM st_read('{data_file}')
            """)
    except Exception as e:
        print(f"  Marketplace OEP: error reading: {e}")
        return 0

    cnt = con.execute("SELECT COUNT(*) FROM _mkt").fetchone()[0]
    cols = [r[0] for r in con.execute("SELECT column_name FROM (DESCRIBE SELECT * FROM _mkt)").fetchall()]

    out_dir = LAKE / "fact" / "marketplace_oep" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"COPY _mkt TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)")

    print(f"  marketplace_oep: {cnt:,} rows")
    print(f"    Columns: {', '.join(cols[:10])}...")
    return cnt


# ── Main ────────────────────────────────────────────────────────────────

def main():
    con = duckdb.connect()
    total = 0

    print("── Round 12 Data Ingestion ──\n")

    n = ingest_saipe(con)
    total += n

    n = ingest_places(con)
    total += n

    n = ingest_health_centers(con)
    total += n

    n = ingest_marketplace_oep(con)
    total += n

    con.close()

    print(f"\n── Total: {total:,} rows across round 12 tables ──")

    # Manifest
    manifest = {
        "pipeline_run": "round12",
        "snapshot_date": SNAP,
        "total_rows": total,
        "tables": [
            "fact_saipe_poverty",
            "fact_places_county",
            "fact_health_center_sites",
            "fact_marketplace_oep",
        ],
    }
    manifest_path = LAKE / "metadata" / f"manifest_round12_{SNAP}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
