"""
Bootstrap script: Enrich the DuckDB file with NPPES-derived columns.

One-time script that:
1. Opens data/medicaid-provider-spending.duckdb in write mode
2. Reads the NPPES CSV to extract NPI, state, zip3, taxonomy, provider_name
3. Materializes a `spending` table with enriched columns + computed category
4. Creates indexes for fast querying
5. Drops intermediate views and vacuums

Usage:
    python3 server/bootstrap_db.py
"""

import sys
import os
import glob
import time
import duckdb

# Resolve paths relative to project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "medicaid-provider-spending.duckdb")
NPPES_PATTERN = os.path.join(PROJECT_ROOT, "data", "npidata_pfile_*.csv")

# ── HCPCS Category Logic ──────────────────────────────────────────────
# Replicates data/tmsis_pipeline_duckdb.R lines 253-292
CATEGORY_SQL = """
CASE
  -- 5-digit numeric codes
  WHEN HCPCS_CODE ~ '^[0-9]{5}$' THEN
    CASE
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 99201 AND 99499 THEN 'E&M'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 90832 AND 90853 THEN 'Behavioral'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 96130 AND 96171 THEN 'Behavioral'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 97151 AND 97158 THEN 'Behavioral'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 59000 AND 59899 THEN 'Maternity'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 70010 AND 79999 THEN 'Imaging'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 10004 AND 69990 THEN 'Surgery'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 80047 AND 89398 THEN 'Lab/Path'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 90281 AND 90399 THEN 'Immunization'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 90460 AND 90474 THEN 'Immunization'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 90785 AND 90899 THEN 'Behavioral'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 91010 AND 91299 THEN 'Diagnostic'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 92002 AND 92499 THEN 'Vision'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 92502 AND 92700 THEN 'Audiology'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 96360 AND 96549 THEN 'Infusion'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 97010 AND 97799 THEN 'Rehab/Therapy'
      WHEN CAST(HCPCS_CODE AS INTEGER) BETWEEN 99500 AND 99607 THEN 'Home Services'
      ELSE 'Procedure'
    END
  -- Alpha-prefix codes
  WHEN HCPCS_CODE LIKE 'D%' THEN 'Dental'
  WHEN HCPCS_CODE LIKE 'J%' THEN 'Drugs'
  WHEN HCPCS_CODE LIKE 'T%' THEN 'HCBS/Waiver'
  WHEN HCPCS_CODE LIKE 'S%' THEN 'HCBS/Waiver'
  WHEN HCPCS_CODE LIKE 'H%' THEN 'Behavioral'
  WHEN HCPCS_CODE LIKE 'G%' THEN 'Temporary/CMS'
  WHEN HCPCS_CODE LIKE 'A%' THEN 'DME/Supply'
  WHEN HCPCS_CODE LIKE 'E%' THEN 'DME/Supply'
  WHEN HCPCS_CODE LIKE 'L%' THEN 'Orthotics'
  WHEN HCPCS_CODE LIKE 'V%' THEN 'Vision'
  WHEN HCPCS_CODE LIKE 'Q%' THEN 'Temporary/CMS'
  WHEN HCPCS_CODE LIKE 'K%' THEN 'DME/Supply'
  WHEN HCPCS_CODE LIKE 'C%' THEN 'Outpatient APC'
  WHEN HCPCS_CODE LIKE 'R%' THEN 'Diagnostic'
  ELSE 'Other'
END
"""

# ── State Normalization Lookup ────────────────────────────────────────
# Replicates data/tmsis_pipeline_duckdb.R lines 586-646
STATE_LOOKUP_SQL = """
CREATE TEMP TABLE state_lookup AS
SELECT * FROM (VALUES
  ('AL','AL'),('Alabama','AL'),('ALABAMA','AL'),
  ('AK','AK'),('Alaska','AK'),('ALASKA','AK'),
  ('AZ','AZ'),('Arizona','AZ'),('ARIZONA','AZ'),
  ('AR','AR'),('Arkansas','AR'),('ARKANSAS','AR'),
  ('CA','CA'),('California','CA'),('CALIFORNIA','CA'),('CA - CALIFORNIA','CA'),
  ('CO','CO'),('Colorado','CO'),('COLORADO','CO'),('CO- COLORADO','CO'),('C0','CO'),
  ('CT','CT'),('Connecticut','CT'),('CONNECTICUT','CT'),
  ('DE','DE'),('Delaware','DE'),('DELAWARE','DE'),
  ('DC','DC'),('D.C.','DC'),('District of Columbia','DC'),('DISTRICT OF COLUMBIA','DC'),
  ('FL','FL'),('Florida','FL'),('FLORIDA','FL'),
  ('GA','GA'),('Georgia','GA'),('GEORGIA','GA'),
  ('HI','HI'),('Hawaii','HI'),('HAWAII','HI'),
  ('ID','ID'),('Idaho','ID'),('IDAHO','ID'),
  ('IL','IL'),('Illinois','IL'),('ILLINOIS','IL'),
  ('IN','IN'),('Indiana','IN'),('INDIANA','IN'),
  ('IA','IA'),('Iowa','IA'),('IOWA','IA'),
  ('KS','KS'),('Kansas','KS'),('KANSAS','KS'),
  ('KY','KY'),('Kentucky','KY'),('KENTUCKY','KY'),
  ('LA','LA'),('Louisiana','LA'),('LOUISIANA','LA'),
  ('ME','ME'),('Maine','ME'),('MAINE','ME'),
  ('MD','MD'),('Maryland','MD'),('MARYLAND','MD'),('MD-MARYLAND','MD'),
  ('MA','MA'),('Massachusetts','MA'),('MASSACHUSETTS','MA'),
  ('MI','MI'),('Michigan','MI'),('MICHIGAN','MI'),
  ('MN','MN'),('Minnesota','MN'),('MINNESOTA','MN'),
  ('MS','MS'),('Mississippi','MS'),('MISSISSIPPI','MS'),
  ('MO','MO'),('Missouri','MO'),('MISSOURI','MO'),
  ('MT','MT'),('Montana','MT'),('MONTANA','MT'),
  ('NE','NE'),('Nebraska','NE'),('NEBRASKA','NE'),
  ('NV','NV'),('Nevada','NV'),('NEVADA','NV'),
  ('NH','NH'),('New Hampshire','NH'),('NEW HAMPSHIRE','NH'),
  ('NJ','NJ'),('New Jersey','NJ'),('NEW JERSEY','NJ'),
  ('NM','NM'),('New Mexico','NM'),('NEW MEXICO','NM'),
  ('NY','NY'),('New York','NY'),('NEW YORK','NY'),
  ('NC','NC'),('North Carolina','NC'),('NORTH CAROLINA','NC'),('N. Carolina','NC'),('N Carolina','NC'),
  ('ND','ND'),('North Dakota','ND'),('NORTH DAKOTA','ND'),('N. Dakota','ND'),('N Dakota','ND'),
  ('OH','OH'),('Ohio','OH'),('OHIO','OH'),
  ('OK','OK'),('Oklahoma','OK'),('OKLAHOMA','OK'),
  ('OR','OR'),('Oregon','OR'),('OREGON','OR'),
  ('PA','PA'),('Pennsylvania','PA'),('PENNSYLVANIA','PA'),
  ('RI','RI'),('Rhode Island','RI'),('RHODE ISLAND','RI'),
  ('SC','SC'),('South Carolina','SC'),('SOUTH CAROLINA','SC'),('S. Carolina','SC'),('S Carolina','SC'),
  ('SD','SD'),('South Dakota','SD'),('SOUTH DAKOTA','SD'),('S. Dakota','SD'),('S Dakota','SD'),
  ('TN','TN'),('Tennessee','TN'),('TENNESSEE','TN'),
  ('TX','TX'),('Texas','TX'),('TEXAS','TX'),
  ('UT','UT'),('Utah','UT'),('UTAH','UT'),
  ('VT','VT'),('Vermont','VT'),('VERMONT','VT'),
  ('VA','VA'),('Virginia','VA'),('VIRGINIA','VA'),
  ('WA','WA'),('Washington','WA'),('WASHINGTON','WA'),
  ('WV','WV'),('West Virginia','WV'),('WEST VIRGINIA','WV'),('W. Virginia','WV'),('W Virginia','WV'),
  ('WI','WI'),('Wisconsin','WI'),('WISCONSIN','WI'),
  ('WY','WY'),('Wyoming','WY'),('WYOMING','WY'),
  ('PR','PR'),('Puerto Rico','PR'),('PUERTO RICO','PR'),('PUESRTO RICO','PR'),('P.R.','PR'),
  ('GU','GU'),('Guam','GU'),('GUAM','GU'),
  ('VI','VI'),('Virgin Islands','VI'),('VIRGIN ISLANDS','VI'),
  ('AS','AS'),('American Samoa','AS'),('AMERICAN SAMOA','AS'),
  ('MP','MP'),('Northern Mariana Islands','MP'),('NORTHERN MARIANA ISLANDS','MP')
) AS t(raw_state, std_state)
"""

# NPPES column candidates (matches R pipeline lines 559-568)
NPI_COLS = ["NPI", "npi"]
STATE_COLS = [
    "Provider Business Practice Location Address State Name",
    "provider_business_practice_location_address_state_name",
    "STATE", "state", "practice_state",
]
ZIP_COLS = [
    "Provider Business Practice Location Address Postal Code",
    "provider_business_practice_location_address_postal_code",
    "ZIP", "zip", "practice_zip",
]
TAXONOMY_COLS = [
    "Healthcare Provider Taxonomy Code_1",
    "healthcare_provider_taxonomy_code_1",
    "HEALTHCARE_PROVIDER_TAXONOMY_CODE_1",
]
ORG_NAME_COLS = [
    "Provider Organization Name (Legal Business Name)",
    "provider_organization_name_legal_business_name",
    "PROVIDER_ORGANIZATION_NAME_LEGAL_BUSINESS_NAME",
]
FIRST_NAME_COLS = [
    "Provider First Name",
    "provider_first_name",
    "PROVIDER_FIRST_NAME",
]
LAST_NAME_COLS = [
    "Provider Last Name (Legal Name)",
    "provider_last_name_legal_name",
    "PROVIDER_LAST_NAME_LEGAL_NAME",
    "Provider Last Name",
]


def detect_col(candidates: list[str], available: list[str]) -> str | None:
    """Return the first candidate that exists in the available columns."""
    for c in candidates:
        if c in available:
            return c
    return None


def main():
    # Find NPPES file
    nppes_files = glob.glob(NPPES_PATTERN)
    if not nppes_files:
        print(f"ERROR: No NPPES file found matching {NPPES_PATTERN}")
        sys.exit(1)
    nppes_path = nppes_files[0]

    if not os.path.exists(DB_PATH):
        print(f"ERROR: DuckDB file not found at {DB_PATH}")
        sys.exit(1)

    print(f"Opening DuckDB: {DB_PATH}")
    print(f"NPPES file: {nppes_path}")

    con = duckdb.connect(DB_PATH)

    # Check if spending table already exists
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "spending" in tables:
        count = con.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
        print(f"WARNING: 'spending' table already exists with {count:,} rows.")
        resp = input("Drop and recreate? [y/N]: ").strip().lower()
        if resp != "y":
            print("Aborting.")
            con.close()
            return
        con.execute("DROP TABLE spending")
        print("Dropped existing spending table.")

    t0 = time.time()

    # Step 1: State normalization lookup
    print("\n[1/5] Creating state normalization lookup...")
    con.execute("DROP TABLE IF EXISTS state_lookup")
    con.execute(STATE_LOOKUP_SQL)
    n_lookup = con.execute("SELECT COUNT(*) FROM state_lookup").fetchone()[0]
    print(f"  State lookup: {n_lookup} variant mappings")

    # Step 2: Load NPPES as a view and detect columns
    print("\n[2/5] Loading NPPES CSV...")
    con.execute(f"""
        CREATE TEMP VIEW nppes_raw AS
        SELECT * FROM read_csv_auto('{nppes_path}', sample_size=10000)
    """)

    nppes_cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'nppes_raw'"
    ).fetchall()]
    print(f"  NPPES columns detected: {len(nppes_cols)}")

    np_npi = detect_col(NPI_COLS, nppes_cols)
    np_state = detect_col(STATE_COLS, nppes_cols)
    np_zip = detect_col(ZIP_COLS, nppes_cols)
    np_tax = detect_col(TAXONOMY_COLS, nppes_cols)
    np_org = detect_col(ORG_NAME_COLS, nppes_cols)
    np_first = detect_col(FIRST_NAME_COLS, nppes_cols)
    np_last = detect_col(LAST_NAME_COLS, nppes_cols)

    if not np_npi or not np_state:
        print(f"ERROR: Could not detect NPI column ({np_npi}) or state column ({np_state})")
        con.close()
        sys.exit(1)

    print(f"  NPI col: {np_npi}")
    print(f"  State col: {np_state}")
    print(f"  ZIP col: {np_zip}")
    print(f"  Taxonomy col: {np_tax}")
    print(f"  Org name col: {np_org}")
    print(f"  First name col: {np_first}")
    print(f"  Last name col: {np_last}")

    # Build npi_geo view
    select_parts = [
        f'CAST("{np_npi}" AS VARCHAR) AS npi',
        "sl.std_state AS state",
    ]
    if np_zip:
        select_parts.append(f'LEFT(CAST("{np_zip}" AS VARCHAR), 3) AS zip3')
    if np_tax:
        select_parts.append(f'"{np_tax}" AS taxonomy')

    # Provider name: prefer org_name, fall back to last/first
    name_expr_parts = []
    if np_org:
        name_expr_parts.append(f'"{np_org}"')
    if np_last and np_first:
        if np_org:
            name_expr_parts = [
                f'COALESCE(NULLIF(TRIM("{np_org}"), \'\'), TRIM("{np_last}") || \', \' || TRIM("{np_first}"))'
            ]
        else:
            name_expr_parts = [f'TRIM("{np_last}") || \', \' || TRIM("{np_first}")']
    elif np_org:
        name_expr_parts = [f'"{np_org}"']

    if name_expr_parts:
        select_parts.append(f"{name_expr_parts[0]} AS provider_name")

    select_clause = ",\n          ".join(select_parts)

    # Step 3: Materialize NPI geo as a TABLE (not view) for fast join
    print("\n[3/6] Materializing NPI geo table from NPPES (this reads the 10GB CSV once)...")
    con.execute("DROP TABLE IF EXISTS npi_geo")
    con.execute(f"""
        CREATE TABLE npi_geo AS
        SELECT DISTINCT
          {select_clause}
        FROM nppes_raw n
        INNER JOIN state_lookup sl ON TRIM(n."{np_state}") = sl.raw_state
        WHERE n."{np_state}" IS NOT NULL
    """)

    npi_count = con.execute("SELECT COUNT(*) FROM npi_geo").fetchone()[0]
    print(f"  NPI geo table: {npi_count:,} NPIs with valid US state")

    # Index the NPI geo table for fast join
    print("  Creating index on npi_geo.npi...")
    con.execute("CREATE INDEX idx_npi_geo_npi ON npi_geo(npi)")

    # Step 4: Identify source table columns
    source_table = "data"  # The raw table in the DuckDB file
    if source_table not in tables:
        print(f"ERROR: Source table '{source_table}' not found. Available: {tables}")
        con.close()
        sys.exit(1)

    source_cols = [r[0] for r in con.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}'"
    ).fetchall()]
    print(f"  Source table '{source_table}' columns: {source_cols}")

    # Step 5: Materialize enriched spending table
    print("\n[5/6] Materializing spending table (this may take several minutes)...")

    geo_cols = ["g.state"]
    if np_zip:
        geo_cols.append("g.zip3")
    if np_tax:
        geo_cols.append("g.taxonomy")
    if name_expr_parts:
        geo_cols.append("g.provider_name")

    geo_select = ", ".join(geo_cols)

    con.execute(f"""
        CREATE TABLE spending AS
        SELECT
          d.*,
          {geo_select},
          {CATEGORY_SQL} AS category
        FROM "{source_table}" d
        JOIN npi_geo g ON CAST(d.BILLING_PROVIDER_NPI_NUM AS VARCHAR) = g.npi
        WHERE CAST(d.TOTAL_PAID AS DOUBLE) > 0
    """)

    row_count = con.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
    print(f"  Spending table created: {row_count:,} rows")

    # Spot check
    sample = con.execute("""
        SELECT state, category, COUNT(*) as n, SUM(TOTAL_PAID) as total
        FROM spending
        WHERE state = 'FL'
        GROUP BY state, category
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()
    print("  FL spot check:")
    for row in sample:
        print(f"    {row[0]} / {row[1]}: {row[2]:,} rows, ${row[3]:,.0f}")

    # Step 6: Create indexes
    print("\n[6/6] Creating indexes...")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spending_state ON spending(state)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spending_hcpcs ON spending(HCPCS_CODE)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spending_month ON spending(CLAIM_FROM_MONTH)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_spending_npi ON spending(BILLING_PROVIDER_NPI_NUM)")
    print("  Indexes created.")

    # Cleanup
    print("\nCleaning up intermediate objects...")
    con.execute("DROP TABLE IF EXISTS npi_geo")
    con.execute("DROP VIEW IF EXISTS nppes_raw")
    con.execute("DROP TABLE IF EXISTS state_lookup")

    print("Vacuuming...")
    con.execute("VACUUM")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s. Spending table: {row_count:,} rows.")

    # Final schema check
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'spending'"
    ).fetchall()]
    print(f"Spending columns: {cols}")

    con.close()


if __name__ == "__main__":
    main()
