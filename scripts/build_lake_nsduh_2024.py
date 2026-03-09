"""
Ingest NSDUH 2023-2024 state prevalence estimates (41 tables, percentages).

Produces: fact_nsduh_prevalence_2024 — replaces/supplements existing fact_nsduh_prevalence
"""

import csv
import duckdb
import json
from pathlib import Path
from datetime import date

LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
RAW  = Path(__file__).resolve().parent.parent / "data" / "raw" / "nsduh_2024_prevalence"
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

# Key tables to ingest (most Medicaid-relevant)
KEY_TABLES = {
    1: "illicit_drug_use_past_month",
    3: "marijuana_use_past_month",
    13: "rx_opioid_misuse_past_year",
    14: "opioid_misuse_past_year",
    15: "alcohol_use_past_month",
    16: "binge_alcohol_past_month",
    24: "substance_use_disorder_past_year",
    25: "alcohol_use_disorder_past_year",
    27: "drug_use_disorder_past_year",
    29: "opioid_use_disorder_past_year",
    30: "received_sud_treatment_past_year",
    31: "needing_sud_treatment_past_year",
    33: "any_mental_illness_past_year",
    34: "serious_mental_illness_past_year",
    35: "co_occurring_sud_ami_past_year",
    37: "received_mh_treatment_past_year",
    38: "major_depressive_episode_past_year",
    39: "suicidal_thoughts_past_year",
}


def _parse_pct(val):
    """Parse a percentage string like '16.77%' to float."""
    if not val or val.strip() == '':
        return None
    val = val.strip().replace('%', '')
    try:
        return float(val)
    except ValueError:
        return None


def parse_table(table_num):
    """Parse a single NSDUH table CSV into structured rows."""
    fname = f"2024-nsduh-sae-excel-tab{table_num:02d}.csv"
    path = RAW / fname
    if not path.exists():
        return [], ""

    with open(path, 'r', encoding='utf-8') as f:
        lines = list(csv.reader(f))

    # First line contains the table title
    title = lines[0][0] if lines else ""
    # Extract measure name from title (before the colon)
    measure_name = title.split(":")[0].replace(f"Table {table_num}. ", "") if title else ""

    # Find header row — the row where a cell is exactly "State" and another contains "Estimate"
    header_idx = None
    for i, line in enumerate(lines):
        cells = [c.strip() for c in line]
        if 'State' in cells and any('Estimate' in c and len(c) < 30 for c in cells):
            header_idx = i
            break
    if header_idx is None:
        return [], measure_name

    headers = [h.strip() for h in lines[header_idx]]

    rows = []
    for line in lines[header_idx + 1:]:
        if len(line) < 3:
            continue
        state_name = line[1].strip() if len(line) > 1 else None
        if not state_name:
            continue
        # Skip region rows
        if state_name in ('Northeast', 'Midwest', 'South', 'West'):
            continue
        state_code = STATE_NAME_TO_CODE.get(state_name)
        if not state_code:
            continue

        # Parse age group estimates
        # Standard layout: Order, State, 12+ Est, 12+ CI Low, 12+ CI High, 12-17 Est, ...
        age_groups = []
        col = 2
        while col + 2 < len(line):
            # Find the age group from header
            h = headers[col] if col < len(headers) else ''
            age = h.replace(' Estimate', '').replace(' 95% CI (Lower)', '').replace(' 95% CI (Upper)', '').strip()
            if not age:
                col += 3
                continue

            estimate = _parse_pct(line[col])
            ci_lower = _parse_pct(line[col + 1]) if col + 1 < len(line) else None
            ci_upper = _parse_pct(line[col + 2]) if col + 2 < len(line) else None

            if estimate is not None:
                rows.append({
                    'state_code': state_code,
                    'measure_id': KEY_TABLES.get(table_num, f"table_{table_num}"),
                    'measure_name': measure_name,
                    'age_group': age,
                    'estimate_pct': estimate,
                    'ci_lower_pct': ci_lower,
                    'ci_upper_pct': ci_upper,
                    'survey_years': '2023-2024',
                })
            col += 3

    return rows, measure_name


def main():
    con = duckdb.connect()
    all_rows = []

    print("── NSDUH 2023-2024 State Prevalence ──")
    for table_num, measure_id in KEY_TABLES.items():
        rows, title = parse_table(table_num)
        if rows:
            print(f"  Table {table_num:2d}: {len(rows):4d} rows — {title[:80]}")
            all_rows.extend(rows)
        else:
            print(f"  Table {table_num:2d}: EMPTY")

    if not all_rows:
        print("  No data found")
        return

    con.execute("DROP TABLE IF EXISTS _nsduh")
    con.execute("""
        CREATE TABLE _nsduh (
            state_code VARCHAR, measure_id VARCHAR, measure_name VARCHAR,
            age_group VARCHAR, estimate_pct DOUBLE,
            ci_lower_pct DOUBLE, ci_upper_pct DOUBLE,
            survey_years VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO _nsduh VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(r['state_code'], r['measure_id'], r['measure_name'],
          r['age_group'], r['estimate_pct'], r['ci_lower_pct'],
          r['ci_upper_pct'], r['survey_years']) for r in all_rows]
    )

    out_dir = LAKE / "fact" / "nsduh_prevalence_2024" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"""
        COPY _nsduh TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)
    cnt = len(all_rows)
    print(f"\n  ✓ nsduh_prevalence_2024: {cnt:,} rows → {out_path}")

    con.close()

    # Write manifest
    manifest = {
        "pipeline_run": "nsduh_2024",
        "snapshot_date": SNAP,
        "total_rows": cnt,
        "tables": ["fact_nsduh_prevalence_2024"],
        "measures": list(KEY_TABLES.values()),
    }
    manifest_path = LAKE / "metadata" / f"manifest_nsduh_2024_{SNAP}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
