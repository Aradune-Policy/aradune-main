"""
Ingest SAMHSA NSDUH 2021-2022 state-level estimates (37 tables x 2 estimate types).

Sources:
  - data/raw/nsduh_2022_state_percent/  (37 CSVs — percentages)
  - data/raw/nsduh_2022_state_totals/   (37 CSVs — numbers in thousands)

Produces: data/lake/fact/nsduh_2022_state/data.parquet
  Unified long-format table with columns:
    state_code, state_name, table_number, measure, estimate_type, age_group,
    value, ci_lower, ci_upper, survey_years, source, snapshot_date
"""

import csv
import duckdb
import json
from pathlib import Path
from datetime import date

LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
RAW_PCT = Path(__file__).resolve().parent.parent / "data" / "raw" / "nsduh_2022_state_percent"
RAW_TOT = Path(__file__).resolve().parent.parent / "data" / "raw" / "nsduh_2022_state_totals"
SNAP = str(date.today())

STATE_NAME_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI',
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME',
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN',
    'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI',
    'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX',
    'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'Total U.S.': 'US',
}

# Regions to skip (keep only states + Total U.S.)
REGIONS = {'Northeast', 'Midwest', 'South', 'West'}

# Table 18 has a special multi-measure layout for ages 12-20
TABLE_18_MEASURES = [
    "Alcohol Use in Past Month",
    "Binge Alcohol Use in Past Month",
    "Perceptions of Great Risk from Binge Drinking",
]


def _parse_value(val, is_pct=True):
    """Parse a value string. Handles percentages, thousands, 'DU', blanks."""
    if not val or val.strip() == '':
        return None
    val = val.strip()
    # Handle suppressed values
    if val.upper() in ('DU', 'DU*', '--', 'N/A', '*'):
        return None
    val = val.replace('%', '').replace(',', '')
    try:
        return float(val)
    except ValueError:
        return None


def _extract_measure_from_title(title, table_num):
    """Extract the short measure name from the table title."""
    # Title format: "Table N. Measure Name: Among People ..."
    # Strip "Table N. " prefix
    cleaned = title
    for prefix_pattern in [f"Table {table_num}. ", f"Table {table_num}."]:
        if cleaned.startswith(prefix_pattern):
            cleaned = cleaned[len(prefix_pattern):]
            break
    # Take everything before the colon
    if ':' in cleaned:
        cleaned = cleaned.split(':')[0]
    return cleaned.strip()


def _find_header_row(lines):
    """Find the header row index. The header contains 'State' and 'Estimate'."""
    for i, line in enumerate(lines):
        cells = [c.strip() for c in line]
        if 'State' in cells:
            # Check if any cell contains 'Estimate'
            if any('Estimate' in c for c in cells):
                return i
    return None


def _infer_default_age_group(title):
    """Infer the age group from the title when headers have no age prefix."""
    # Tables like 24 target specific age ranges stated in the title
    if '12 to 20' in title or '12-20' in title:
        return '12-20'
    if '18 or Older' in title:
        return '18+'
    if '12 or Older' in title:
        return '12+'
    return 'all'


def parse_standard_table(lines, header_idx, table_num, estimate_type, is_pct):
    """Parse a standard NSDUH table (most tables: 5 age groups x 3 columns)."""
    title = lines[0][0].strip() if lines and lines[0] else ""
    measure = _extract_measure_from_title(title, table_num)
    headers = [h.strip() for h in lines[header_idx]]

    # Determine survey years from title
    if '2021 and 2022' in title:
        survey_years = '2021-2022'
    elif ', 2022' in title:
        survey_years = '2022'
    else:
        survey_years = '2021-2022'

    # Determine default age group from title (for tables with no age prefix in headers)
    default_age_group = _infer_default_age_group(title)

    rows = []
    for line in lines[header_idx + 1:]:
        if len(line) < 3:
            continue
        state_name = line[1].strip().strip('"') if len(line) > 1 else ''
        if not state_name or state_name in REGIONS:
            continue
        state_code = STATE_NAME_TO_CODE.get(state_name)
        if not state_code:
            continue

        # Parse groups of 3 columns: Estimate, CI Lower, CI Upper
        col = 2
        while col + 2 < len(line):
            if col >= len(headers):
                break
            # Extract age group from header
            h = headers[col]
            # Age group is the prefix before "Estimate" or "95% CI"
            age_group = h.replace('Estimate', '').replace('95% CI (Lower)', '').replace('95% CI (Upper)', '').strip()
            # If no age group in header (e.g. Table 24: just "Estimate"), use title-inferred default
            if not age_group:
                age_group = default_age_group

            estimate = _parse_value(line[col], is_pct)
            ci_lower = _parse_value(line[col + 1], is_pct) if col + 1 < len(line) else None
            ci_upper = _parse_value(line[col + 2], is_pct) if col + 2 < len(line) else None

            if estimate is not None:
                rows.append({
                    'state_code': state_code,
                    'state_name': state_name,
                    'table_number': table_num,
                    'measure': measure,
                    'estimate_type': estimate_type,
                    'age_group': age_group,
                    'value': estimate,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper,
                    'survey_years': survey_years,
                })
            col += 3

    return rows, measure


def parse_table_18(lines, header_idx, estimate_type, is_pct):
    """Parse Table 18 which has 3 measures for age group 12-20."""
    title = lines[0][0].strip() if lines and lines[0] else ""
    headers = [h.strip() for h in lines[header_idx]]

    survey_years = '2021-2022'
    if ', 2022' in title and '2021' not in title:
        survey_years = '2022'

    rows = []
    for line in lines[header_idx + 1:]:
        if len(line) < 3:
            continue
        state_name = line[1].strip().strip('"') if len(line) > 1 else ''
        if not state_name or state_name in REGIONS:
            continue
        state_code = STATE_NAME_TO_CODE.get(state_name)
        if not state_code:
            continue

        # Table 18 has 3 measures x 3 columns (Estimate, CI Low, CI High) = 9 data cols
        # Headers vary slightly but follow groups of 3
        col = 2
        measure_idx = 0
        while col + 2 < len(line) and measure_idx < len(TABLE_18_MEASURES):
            estimate = _parse_value(line[col], is_pct)
            ci_lower = _parse_value(line[col + 1], is_pct) if col + 1 < len(line) else None
            ci_upper = _parse_value(line[col + 2], is_pct) if col + 2 < len(line) else None

            if estimate is not None:
                rows.append({
                    'state_code': state_code,
                    'state_name': state_name,
                    'table_number': 18,
                    'measure': TABLE_18_MEASURES[measure_idx],
                    'estimate_type': estimate_type,
                    'age_group': '12-20',
                    'value': estimate,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper,
                    'survey_years': survey_years,
                })
            col += 3
            measure_idx += 1

    combined_measure = "Alcohol Use, Binge Drinking, Risk Perception (Ages 12-20)"
    return rows, combined_measure


def parse_csv_file(filepath, table_num, estimate_type, is_pct):
    """Parse a single NSDUH CSV file."""
    # Some files use Windows-1252 encoding (smart quotes, etc.)
    for enc in ('utf-8-sig', 'utf-8', 'cp1252', 'latin-1'):
        try:
            with open(filepath, 'r', encoding=enc) as f:
                lines = list(csv.reader(f))
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    else:
        return [], f"Table {table_num}"

    header_idx = _find_header_row(lines)
    if header_idx is None:
        return [], f"Table {table_num}"

    if table_num == 18:
        return parse_table_18(lines, header_idx, estimate_type, is_pct)
    else:
        return parse_standard_table(lines, header_idx, table_num, estimate_type, is_pct)


def main():
    con = duckdb.connect()
    all_rows = []

    print("== NSDUH 2021-2022 State Estimates ==\n")

    # Process percent files
    print("-- Percentages --")
    for table_num in range(1, 38):
        fname = f"NSDUHsaeExcelTab{table_num:02d}-2022.csv"
        fpath = RAW_PCT / fname
        if not fpath.exists():
            print(f"  Table {table_num:2d}: MISSING ({fname})")
            continue
        rows, measure = parse_csv_file(fpath, table_num, 'percent', is_pct=True)
        if rows:
            print(f"  Table {table_num:2d}: {len(rows):5d} rows -- {measure[:72]}")
            all_rows.extend(rows)
        else:
            print(f"  Table {table_num:2d}: EMPTY")

    # Process totals files
    print("\n-- Totals (in Thousands) --")
    for table_num in range(1, 38):
        fname = f"NSDUHsaeTotalsTab{table_num:02d}-2022.csv"
        fpath = RAW_TOT / fname
        if not fpath.exists():
            print(f"  Table {table_num:2d}: MISSING ({fname})")
            continue
        rows, measure = parse_csv_file(fpath, table_num, 'total', is_pct=False)
        if rows:
            print(f"  Table {table_num:2d}: {len(rows):5d} rows -- {measure[:72]}")
            all_rows.extend(rows)
        else:
            print(f"  Table {table_num:2d}: EMPTY")

    if not all_rows:
        print("\n  No data found. Exiting.")
        return

    print(f"\n  Total rows: {len(all_rows):,}")

    # Load into DuckDB and write to Parquet
    con.execute("DROP TABLE IF EXISTS _nsduh_2022")
    con.execute("""
        CREATE TABLE _nsduh_2022 (
            state_code      VARCHAR,
            state_name      VARCHAR,
            table_number    INTEGER,
            measure         VARCHAR,
            estimate_type   VARCHAR,
            age_group       VARCHAR,
            value           DOUBLE,
            ci_lower        DOUBLE,
            ci_upper        DOUBLE,
            survey_years    VARCHAR,
            source          VARCHAR,
            snapshot_date   VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO _nsduh_2022 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(
            r['state_code'], r['state_name'], r['table_number'], r['measure'],
            r['estimate_type'], r['age_group'], r['value'], r['ci_lower'],
            r['ci_upper'], r['survey_years'],
            'SAMHSA NSDUH 2022 State Estimates', SNAP
        ) for r in all_rows]
    )

    out_dir = LAKE / "fact" / "nsduh_2022_state"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"""
        COPY _nsduh_2022 TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    cnt = len(all_rows)
    print(f"\n  Written: nsduh_2022_state -- {cnt:,} rows")
    print(f"  Path:    {out_path}")

    # Summary stats
    stats = con.execute("""
        SELECT estimate_type, COUNT(*) as n, COUNT(DISTINCT state_code) as states,
               COUNT(DISTINCT measure) as measures, COUNT(DISTINCT age_group) as age_groups
        FROM _nsduh_2022
        GROUP BY estimate_type
    """).fetchall()
    print("\n  Summary:")
    for row in stats:
        print(f"    {row[0]:>8s}: {row[1]:,} rows, {row[2]} states/regions, {row[3]} measures, {row[4]} age groups")

    # Sample
    print("\n  Sample rows:")
    sample = con.execute("""
        SELECT state_code, measure, estimate_type, age_group, value, ci_lower, ci_upper
        FROM _nsduh_2022
        WHERE state_code = 'FL'
        ORDER BY table_number, estimate_type, age_group
        LIMIT 10
    """).fetchall()
    for row in sample:
        print(f"    {row[0]} | {row[1][:45]:45s} | {row[2]:>7s} | {row[3]:>5s} | {row[4]:>10.2f} | {row[5]:>10.2f} | {row[6]:>10.2f}")

    con.close()

    # Write manifest
    manifest = {
        "pipeline_run": "nsduh_2022_state",
        "snapshot_date": SNAP,
        "total_rows": cnt,
        "tables": ["fact_nsduh_2022_state"],
        "source_dirs": [
            "data/raw/nsduh_2022_state_percent/",
            "data/raw/nsduh_2022_state_totals/"
        ],
        "columns": [
            "state_code", "state_name", "table_number", "measure",
            "estimate_type", "age_group", "value", "ci_lower", "ci_upper",
            "survey_years", "source", "snapshot_date"
        ],
    }
    manifest_path = LAKE / "metadata" / f"manifest_nsduh_2022_{SNAP}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
