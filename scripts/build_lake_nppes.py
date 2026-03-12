#!/usr/bin/env python3
"""
build_lake_nppes.py — Ingest full NPPES NPI data file into the Aradune data lake.

Source: https://download.cms.gov/nppes/NPI_Files.html
File:   npidata_pfile_20050523-20260208.csv (~11 GB, 330 columns, ~8M+ NPIs)

Tables built:
  fact_nppes_provider        — Core provider registry (27 columns, ~8M rows)
  fact_nppes_taxonomy_detail — All taxonomy codes per NPI unpivoted to long format
                               (up to 15 per provider, ~12M+ rows)

Strategy: Read the 11 GB CSV once into a staging table with ~70 needed columns
(NPI + name/address + 15x4 taxonomy slots + org/auth fields), then derive both
output tables from the staging table in memory. This avoids re-reading the CSV.

Usage:
  python3 scripts/build_lake_nppes.py
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_FILE = PROJECT_ROOT / "data" / "raw" / "npidata_pfile_20050523-20260208.csv"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

SOURCE_URL = "https://download.cms.gov/nppes/NPI_Files.html"


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    """Write a DuckDB table to ZSTD-compressed Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if count == 0:
        print(f"  [SKIP] {table} has 0 rows")
        return 0
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def _select_columns_for_staging() -> str:
    """Build SELECT clause to pick only the ~86 columns we need from the 330-column CSV."""
    cols = [
        '"NPI"',
        '"Entity Type Code"',
        '"Provider Organization Name (Legal Business Name)"',
        '"Provider Last Name (Legal Name)"',
        '"Provider First Name"',
        '"Provider Credential Text"',
        '"Provider Sex Code"',
        '"Provider First Line Business Practice Location Address"',
        '"Provider Business Practice Location Address City Name"',
        '"Provider Business Practice Location Address State Name"',
        '"Provider Business Practice Location Address Postal Code"',
        '"Provider Business Practice Location Address Country Code (If outside U.S.)"',
        '"Provider Business Practice Location Address Telephone Number"',
        '"Provider Enumeration Date"',
        '"Last Update Date"',
        '"NPI Deactivation Reason Code"',
        '"NPI Deactivation Date"',
        '"NPI Reactivation Date"',
        '"Employer Identification Number (EIN)"',
        '"Is Sole Proprietor"',
        '"Is Organization Subpart"',
        '"Parent Organization LBN"',
        '"Parent Organization TIN"',
        '"Authorized Official Last Name"',
        '"Authorized Official First Name"',
        '"Authorized Official Telephone Number"',
    ]
    # Add all 15 taxonomy slots (4 columns each = 60 columns)
    for i in range(1, 16):
        cols.append(f'"Healthcare Provider Taxonomy Code_{i}"')
        cols.append(f'"Healthcare Provider Primary Taxonomy Switch_{i}"')
        cols.append(f'"Provider License Number_{i}"')
        cols.append(f'"Provider License Number State Code_{i}"')

    return ",\n            ".join(cols)


def main():
    if not RAW_FILE.exists():
        print(f"ERROR: NPPES file not found: {RAW_FILE}")
        print("Download from: https://download.cms.gov/nppes/NPI_Files.html")
        return

    file_size_gb = RAW_FILE.stat().st_size / (1024**3)
    print("=" * 70)
    print("NPPES NPI Data Ingestion")
    print(f"  Source:   {RAW_FILE.name} ({file_size_gb:.1f} GB)")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print("=" * 70)

    # Use file-backed DB so DuckDB can spill to disk (staging is ~9M rows x 86 cols)
    db_path = PROJECT_ROOT / "data" / "raw" / "_nppes_staging.duckdb"
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute("SET memory_limit = '8GB'")
    con.execute("SET threads = 4")

    # -----------------------------------------------------------------------
    # STEP 1: Read CSV once into staging table with only the ~86 columns we need
    # -----------------------------------------------------------------------
    print("\nStep 1: Reading NPPES CSV (single pass, ~86 of 330 columns)...")
    select_cols = _select_columns_for_staging()

    con.execute(f"""
        CREATE TABLE staging AS
        SELECT
            {select_cols}
        FROM read_csv_auto(
            '{RAW_FILE}',
            all_varchar=true,
            sample_size=10000,
            ignore_errors=true
        )
        WHERE "NPI" IS NOT NULL
          AND TRIM("NPI") != ''
    """)

    staging_count = con.execute("SELECT COUNT(*) FROM staging").fetchone()[0]
    print(f"  {staging_count:,} rows in staging table")

    # -----------------------------------------------------------------------
    # STEP 2: Build fact_nppes_provider from staging
    # -----------------------------------------------------------------------
    print("\nStep 2: Building fact_nppes_provider...")

    con.execute("""
        CREATE TABLE fact_nppes_provider AS
        SELECT
            "NPI"                                                                     AS npi,
            "Entity Type Code"                                                        AS entity_type,
            "Provider Organization Name (Legal Business Name)"                        AS org_name,
            "Provider Last Name (Legal Name)"                                         AS last_name,
            "Provider First Name"                                                     AS first_name,
            "Provider Credential Text"                                                AS credentials,
            "Provider Sex Code"                                                       AS gender,

            "Healthcare Provider Taxonomy Code_1"                                     AS primary_taxonomy,
            "Healthcare Provider Primary Taxonomy Switch_1"                           AS primary_taxonomy_switch,

            "Provider First Line Business Practice Location Address"                  AS address_line1,
            "Provider Business Practice Location Address City Name"                   AS city,
            "Provider Business Practice Location Address State Name"                  AS state_code,
            CASE
                WHEN LENGTH("Provider Business Practice Location Address Postal Code") >= 5
                THEN LEFT("Provider Business Practice Location Address Postal Code", 5)
                ELSE "Provider Business Practice Location Address Postal Code"
            END                                                                       AS zip_code,
            "Provider Business Practice Location Address Country Code (If outside U.S.)" AS country,
            "Provider Business Practice Location Address Telephone Number"            AS phone,

            "Provider Enumeration Date"                                               AS enumeration_date,
            "Last Update Date"                                                        AS last_update_date,
            "NPI Deactivation Reason Code"                                            AS deactivation_reason,
            "NPI Deactivation Date"                                                   AS deactivation_date,
            "NPI Reactivation Date"                                                   AS reactivation_date,

            "Employer Identification Number (EIN)"                                    AS ein,
            "Is Sole Proprietor"                                                      AS is_sole_proprietor,
            "Is Organization Subpart"                                                 AS is_org_subpart,
            "Parent Organization LBN"                                                 AS parent_org_name,
            "Parent Organization TIN"                                                 AS parent_org_tin,

            "Authorized Official Last Name"                                           AS auth_official_last,
            "Authorized Official First Name"                                          AS auth_official_first,
            "Authorized Official Telephone Number"                                    AS auth_official_phone
        FROM staging
    """)

    provider_count = con.execute("SELECT COUNT(*) FROM fact_nppes_provider").fetchone()[0]
    individuals = con.execute(
        "SELECT COUNT(*) FROM fact_nppes_provider WHERE entity_type = '1'"
    ).fetchone()[0]
    orgs = con.execute(
        "SELECT COUNT(*) FROM fact_nppes_provider WHERE entity_type = '2'"
    ).fetchone()[0]
    deactivated = con.execute(
        "SELECT COUNT(*) FROM fact_nppes_provider WHERE deactivation_date IS NOT NULL AND TRIM(deactivation_date) != ''"
    ).fetchone()[0]
    states = con.execute(
        "SELECT COUNT(DISTINCT state_code) FROM fact_nppes_provider WHERE state_code IS NOT NULL"
    ).fetchone()[0]
    with_taxonomy = con.execute(
        "SELECT COUNT(*) FROM fact_nppes_provider WHERE primary_taxonomy IS NOT NULL AND TRIM(primary_taxonomy) != ''"
    ).fetchone()[0]

    print(f"  {provider_count:,} providers")
    print(f"  {individuals:,} individuals (type 1), {orgs:,} organizations (type 2)")
    print(f"  {deactivated:,} deactivated, {states} distinct states/territories")
    print(f"  {with_taxonomy:,} with primary taxonomy code")

    # Top 10 states
    print("\n  Top 10 states by provider count:")
    top_states = con.execute("""
        SELECT state_code, COUNT(*) as n
        FROM fact_nppes_provider
        WHERE state_code IS NOT NULL AND TRIM(state_code) != ''
        GROUP BY state_code ORDER BY n DESC LIMIT 10
    """).fetchall()
    for st, n in top_states:
        print(f"    {st}: {n:,}")

    # Write provider parquet
    provider_path = FACT_DIR / "nppes_provider" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    provider_rows = write_parquet(con, "fact_nppes_provider", provider_path)

    # Free provider table memory
    con.execute("DROP TABLE fact_nppes_provider")

    # -----------------------------------------------------------------------
    # STEP 3: Build fact_nppes_taxonomy_detail from staging (UNION ALL 15 slots)
    # -----------------------------------------------------------------------
    print("\nStep 3: Building fact_nppes_taxonomy_detail (unpivoting 15 taxonomy slots)...")

    unions = []
    for i in range(1, 16):
        unions.append(f"""
        SELECT
            "NPI" AS npi,
            {i} AS seq,
            "Healthcare Provider Taxonomy Code_{i}" AS taxonomy_code,
            "Healthcare Provider Primary Taxonomy Switch_{i}" AS taxonomy_switch,
            "Provider License Number_{i}" AS license_num,
            "Provider License Number State Code_{i}" AS license_state
        FROM staging
        WHERE "Healthcare Provider Taxonomy Code_{i}" IS NOT NULL
          AND TRIM("Healthcare Provider Taxonomy Code_{i}") != ''
        """)

    full_query = " UNION ALL ".join(unions)
    con.execute(f"CREATE TABLE fact_nppes_taxonomy_detail AS {full_query}")

    taxonomy_count = con.execute("SELECT COUNT(*) FROM fact_nppes_taxonomy_detail").fetchone()[0]
    distinct_npi = con.execute(
        "SELECT COUNT(DISTINCT npi) FROM fact_nppes_taxonomy_detail"
    ).fetchone()[0]
    distinct_tax = con.execute(
        "SELECT COUNT(DISTINCT taxonomy_code) FROM fact_nppes_taxonomy_detail"
    ).fetchone()[0]
    primary_count = con.execute(
        "SELECT COUNT(*) FROM fact_nppes_taxonomy_detail WHERE taxonomy_switch = 'Y'"
    ).fetchone()[0]

    print(f"  {taxonomy_count:,} taxonomy rows (from 15 slots)")
    print(f"  {distinct_npi:,} distinct NPIs, {distinct_tax:,} distinct taxonomy codes")
    print(f"  {primary_count:,} marked as primary (switch=Y)")

    # Distribution of taxonomy slots used per provider
    print("\n  Taxonomy codes per provider:")
    dist = con.execute("""
        SELECT n_codes, COUNT(*) as n_providers
        FROM (
            SELECT npi, COUNT(*) as n_codes
            FROM fact_nppes_taxonomy_detail GROUP BY npi
        )
        GROUP BY n_codes ORDER BY n_codes
    """).fetchall()
    for n_codes, n_prov in dist[:10]:
        print(f"    {n_codes} code(s): {n_prov:,} providers")
    if len(dist) > 10:
        remaining = sum(n for _, n in dist[10:])
        print(f"    ... {len(dist) - 10} more groups ({remaining:,} providers)")

    # Write taxonomy parquet
    taxonomy_path = FACT_DIR / "nppes_taxonomy_detail" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    taxonomy_rows = write_parquet(con, "fact_nppes_taxonomy_detail", taxonomy_path)

    # -----------------------------------------------------------------------
    # STEP 4: Manifest
    # -----------------------------------------------------------------------
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_nppes.py",
        "source": SOURCE_URL,
        "source_file": str(RAW_FILE.name),
        "tables": {
            "fact_nppes_provider": {
                "rows": provider_rows,
                "path": f"fact/nppes_provider/snapshot={SNAPSHOT_DATE}/data.parquet",
                "description": "Core NPPES provider registry (27 columns from 330)",
            },
            "fact_nppes_taxonomy_detail": {
                "rows": taxonomy_rows,
                "path": f"fact/nppes_taxonomy_detail/snapshot={SNAPSHOT_DATE}/data.parquet",
                "description": "All taxonomy codes per NPI unpivoted to long format (up to 15 per provider)",
            },
        },
        "completed_at": datetime.utcnow().isoformat() + "Z",
    }
    manifest_path = META_DIR / f"manifest_nppes_{SNAPSHOT_DATE}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\n  Manifest: {manifest_path}")

    # -----------------------------------------------------------------------
    # Done — clean up temp DB
    # -----------------------------------------------------------------------
    con.close()
    db_path = PROJECT_ROOT / "data" / "raw" / "_nppes_staging.duckdb"
    for f in db_path.parent.glob("_nppes_staging.duckdb*"):
        try:
            f.unlink()
        except OSError:
            pass

    print("\n" + "=" * 70)
    print("NPPES INGESTION COMPLETE")
    print(f"  fact_nppes_provider:         {provider_rows:,} rows")
    print(f"  fact_nppes_taxonomy_detail:   {taxonomy_rows:,} rows")
    print(f"  Total:                        {provider_rows + taxonomy_rows:,} rows")
    print("=" * 70)


if __name__ == "__main__":
    main()
