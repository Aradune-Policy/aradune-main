"""
Ingest CMS Managed Care Enrollment Summary (2016-2024) from data.medicaid.gov.

Produces: fact_mc_enrollment_summary — state-level MC enrollment by year
"""

import csv
import duckdb
import json
import re
from pathlib import Path
from datetime import date

LAKE = Path(__file__).resolve().parent.parent / "data" / "lake"
RAW  = Path(__file__).resolve().parent.parent / "data" / "raw" / "mc_enrollment_summary_2024.csv"
SNAP = str(date.today())

STATE_NAME_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Guam': 'GU',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN',
    'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA',
    'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI',
    'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
    'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND',
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Puerto Rico': 'PR', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virgin Islands': 'VI', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'American Samoa': 'AS', 'Northern Mariana Islands': 'MP',
    'TOTALS': 'US',
}


def _parse_int(val):
    """Parse integer from string, handling commas, n/a, --, etc."""
    if not val or val.strip() in ('', 'n/a', '--', 'N/A'):
        return None
    val = val.strip().replace(',', '')
    try:
        return int(val)
    except ValueError:
        return None


def _clean_state_name(raw_name):
    """Strip footnote numbers from state names like 'Arkansas6' → 'Arkansas'."""
    return re.sub(r'\d+$', '', raw_name).strip().strip('"')


def main():
    if not RAW.exists():
        print(f"  File not found: {RAW}")
        return

    con = duckdb.connect()
    rows = []

    with open(RAW, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_state = row.get('State', '').strip().strip('"')
            state_name = _clean_state_name(raw_state)
            state_code = STATE_NAME_TO_CODE.get(state_name)
            if not state_code:
                continue

            year = _parse_int(row.get('Year', ''))
            if not year:
                continue

            total_enrollees = _parse_int(row.get('Total Medicaid Enrollees', ''))
            total_mc = _parse_int(row.get('Total Medicaid Enrollment in Any Type of Managed Care', ''))
            comprehensive_mc = _parse_int(row.get('Medicaid Enrollment in Comprehensive Managed Care', ''))
            new_adults_mc = _parse_int(row.get('Medicaid Newly Eligible Adults Enrolled in Comprehensive MCOs', ''))

            # Compute MC penetration rate
            mc_pct = round(total_mc / total_enrollees * 100, 1) if total_mc and total_enrollees else None

            rows.append({
                'state_code': state_code,
                'year': year,
                'total_enrollees': total_enrollees,
                'total_mc_enrollment': total_mc,
                'comprehensive_mc_enrollment': comprehensive_mc,
                'new_adults_mc_enrollment': new_adults_mc,
                'mc_penetration_pct': mc_pct,
            })

    if not rows:
        print("  No data parsed")
        return

    con.execute("DROP TABLE IF EXISTS _mc_summary")
    con.execute("""
        CREATE TABLE _mc_summary (
            state_code VARCHAR, year INTEGER,
            total_enrollees BIGINT, total_mc_enrollment BIGINT,
            comprehensive_mc_enrollment BIGINT, new_adults_mc_enrollment BIGINT,
            mc_penetration_pct DOUBLE
        )
    """)
    con.executemany(
        "INSERT INTO _mc_summary VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(r['state_code'], r['year'], r['total_enrollees'], r['total_mc_enrollment'],
          r['comprehensive_mc_enrollment'], r['new_adults_mc_enrollment'],
          r['mc_penetration_pct']) for r in rows]
    )

    out_dir = LAKE / "fact" / "mc_enrollment_summary" / f"snapshot={SNAP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    con.execute(f"""
        COPY _mc_summary TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
    """)

    cnt = len(rows)
    print(f"── MC Enrollment Summary ──")
    print(f"  {cnt:,} rows ({len(set(r['state_code'] for r in rows))} states, "
          f"{min(r['year'] for r in rows)}-{max(r['year'] for r in rows)})")

    # Quick stats
    us_2024 = [r for r in rows if r['state_code'] == 'US' and r['year'] == 2024]
    if us_2024:
        u = us_2024[0]
        print(f"  US 2024: {u['total_enrollees']:,} enrollees, "
              f"{u['total_mc_enrollment']:,} in MC ({u['mc_penetration_pct']}%)")

    print(f"  ✓ mc_enrollment_summary: {cnt:,} rows → {out_path}")
    con.close()

    # Manifest
    manifest = {
        "pipeline_run": "mc_enrollment_summary",
        "snapshot_date": SNAP,
        "total_rows": cnt,
        "tables": ["fact_mc_enrollment_summary"],
        "source": "data.medicaid.gov Managed Care Enrollment Summary 2024",
    }
    manifest_path = LAKE / "metadata" / f"manifest_mc_summary_{SNAP}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
