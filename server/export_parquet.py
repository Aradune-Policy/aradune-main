"""
Export pre-aggregated Parquet files from the enriched DuckDB for browser-side querying.

Generates 4 Parquet files + meta.json in public/data/:
  - claims.parquet          (~10 MB)  State × HCPCS code × year
  - claims_monthly.parquet  (~30 MB)  State × HCPCS code × month (full granularity)
  - categories.parquet      (~144 KB) State × category × year
  - providers.parquet        (~15 MB)  Provider (NPI) × state summary

Usage:
    python3 server/export_parquet.py
"""

import os
import sys
import json
import time
import duckdb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "medicaid-provider-spending.duckdb")
OUT_DIR = os.path.join(PROJECT_ROOT, "public", "data")


def export():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: DuckDB file not found at {DB_PATH}")
        print("Run bootstrap_db.py first to create the enriched database.")
        sys.exit(1)

    os.makedirs(OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_PATH, read_only=True)

    # Verify spending table exists
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "spending" not in tables:
        print("ERROR: 'spending' table not found. Run bootstrap_db.py first.")
        sys.exit(1)

    total_rows = con.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
    print(f"Source: {total_rows:,} rows in spending table")

    # ── 1. claims.parquet ─────────────────────────────────────────────────
    print("\n[1/4] Exporting claims.parquet ...")
    t0 = time.time()
    claims_path = os.path.join(OUT_DIR, "claims.parquet")
    con.execute(f"""
        COPY (
            SELECT
                state,
                HCPCS_CODE AS hcpcs_code,
                category,
                CAST(LEFT(CLAIM_FROM_MONTH, 4) AS INTEGER) AS year,
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
                COUNT(DISTINCT BILLING_PROVIDER_NPI_NUM) AS provider_count
            FROM spending
            GROUP BY state, HCPCS_CODE, category,
                     CAST(LEFT(CLAIM_FROM_MONTH, 4) AS INTEGER)
        ) TO '{claims_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    claims_rows = con.execute(f"SELECT COUNT(*) FROM '{claims_path}'").fetchone()[0]
    claims_size = os.path.getsize(claims_path)
    print(f"  → {claims_rows:,} rows, {claims_size / 1_048_576:.1f} MB ({time.time()-t0:.1f}s)")

    # ── 2. claims_monthly.parquet ─────────────────────────────────────────
    print("\n[2/4] Exporting claims_monthly.parquet ...")
    t0 = time.time()
    monthly_path = os.path.join(OUT_DIR, "claims_monthly.parquet")
    con.execute(f"""
        COPY (
            SELECT
                state,
                HCPCS_CODE AS hcpcs_code,
                category,
                CLAIM_FROM_MONTH AS claim_month,
                CAST(LEFT(CLAIM_FROM_MONTH, 4) AS INTEGER) AS year,
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
                COUNT(DISTINCT BILLING_PROVIDER_NPI_NUM) AS provider_count
            FROM spending
            GROUP BY state, HCPCS_CODE, category, CLAIM_FROM_MONTH,
                     CAST(LEFT(CLAIM_FROM_MONTH, 4) AS INTEGER)
        ) TO '{monthly_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    monthly_rows = con.execute(f"SELECT COUNT(*) FROM '{monthly_path}'").fetchone()[0]
    monthly_size = os.path.getsize(monthly_path)
    print(f"  → {monthly_rows:,} rows, {monthly_size / 1_048_576:.1f} MB ({time.time()-t0:.1f}s)")

    # ── 3. categories.parquet ─────────────────────────────────────────────
    print("\n[3/4] Exporting categories.parquet ...")
    t0 = time.time()
    cats_path = os.path.join(OUT_DIR, "categories.parquet")
    con.execute(f"""
        COPY (
            SELECT
                state,
                category,
                year,
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims,
                SUM(total_beneficiaries) AS total_beneficiaries,
                COUNT(DISTINCT hcpcs_code) AS code_count
            FROM '{claims_path}'
            GROUP BY state, category, year
        ) TO '{cats_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    cats_rows = con.execute(f"SELECT COUNT(*) FROM '{cats_path}'").fetchone()[0]
    cats_size = os.path.getsize(cats_path)
    print(f"  → {cats_rows:,} rows, {cats_size / 1_048_576:.2f} MB ({time.time()-t0:.1f}s)")

    # ── 4. providers.parquet ──────────────────────────────────────────────
    print("\n[4/4] Exporting providers.parquet ...")
    t0 = time.time()
    provs_path = os.path.join(OUT_DIR, "providers.parquet")
    con.execute(f"""
        COPY (
            SELECT
                CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR) AS npi,
                provider_name,
                state,
                zip3,
                taxonomy,
                SUM(TOTAL_PAID) AS total_paid,
                SUM(TOTAL_CLAIMS) AS total_claims,
                SUM(TOTAL_UNIQUE_BENEFICIARIES) AS total_beneficiaries,
                COUNT(DISTINCT HCPCS_CODE) AS code_count
            FROM spending
            GROUP BY BILLING_PROVIDER_NPI_NUM, provider_name,
                     state, zip3, taxonomy
        ) TO '{provs_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    provs_rows = con.execute(f"SELECT COUNT(*) FROM '{provs_path}'").fetchone()[0]
    provs_size = os.path.getsize(provs_path)
    print(f"  → {provs_rows:,} rows, {provs_size / 1_048_576:.1f} MB ({time.time()-t0:.1f}s)")

    # ── 4. meta.json ──────────────────────────────────────────────────────
    print("\n[meta] Generating meta.json ...")
    states = [r[0] for r in con.execute("SELECT DISTINCT state FROM spending ORDER BY state").fetchall()]
    categories = [r[0] for r in con.execute("SELECT DISTINCT category FROM spending WHERE category IS NOT NULL ORDER BY category").fetchall()]
    date_range = con.execute("SELECT MIN(CLAIM_FROM_MONTH), MAX(CLAIM_FROM_MONTH) FROM spending").fetchone()

    meta = {
        "live": True,
        "source": "duckdb-wasm",
        "states": states,
        "categories": categories,
        "date_min": date_range[0],
        "date_max": date_range[1],
        "total_rows": total_rows,
        "parquet": {
            "claims": {"rows": claims_rows, "bytes": claims_size},
            "claims_monthly": {"rows": monthly_rows, "bytes": monthly_size},
            "categories": {"rows": cats_rows, "bytes": cats_size},
            "providers": {"rows": provs_rows, "bytes": provs_size},
        },
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    meta_path = os.path.join(OUT_DIR, "meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  → {meta_path}")

    con.close()

    # ── Summary ───────────────────────────────────────────────────────────
    total_size = claims_size + monthly_size + cats_size + provs_size
    print(f"\nDone! Total Parquet: {total_size / 1_048_576:.1f} MB")
    print(f"  claims.parquet:         {claims_size / 1_048_576:.1f} MB  ({claims_rows:,} rows)")
    print(f"  claims_monthly.parquet: {monthly_size / 1_048_576:.1f} MB  ({monthly_rows:,} rows)")
    print(f"  categories.parquet:     {cats_size / 1_048_576:.2f} MB ({cats_rows:,} rows)")
    print(f"  providers.parquet:      {provs_size / 1_048_576:.1f} MB  ({provs_rows:,} rows)")


if __name__ == "__main__":
    export()
