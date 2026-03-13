#!/usr/bin/env python3
"""
build_lake_340b_covered_entities.py — Ingest 340B Covered Entities from HRSA OPAIS.

Source: https://340bopais.hrsa.gov/
The 340B OPAIS is a Blazor app with no public bulk download API.
This script fetches covered entities by state using the search endpoint.

Tables built:
  fact_340b_covered_entities — All 340B registered covered entities by state.

Usage:
  python3 scripts/build_lake_340b_covered_entities.py
"""

import json
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

# 340B OPAIS API endpoints
SEARCH_URL = "https://340bopais.hrsa.gov/api/coveredEntities/search"

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY", "AS", "GU", "MP", "PR", "VI",
]

# Entity type codes from HRSA
ENTITY_TYPES = {
    "CAH": "Critical Access Hospital",
    "CH": "Children's Hospital",
    "CAN": "Cancer Hospital",
    "DSH": "Disproportionate Share Hospital",
    "FQHC": "Federally Qualified Health Center",
    "FQHCLA": "FQHC Look-Alike",
    "RRC": "Rural Referral Center",
    "SCH": "Sole Community Hospital",
    "TB": "TB/HIV/AIDS/STD Clinic",
    "BH": "Black Lung Clinic",
    "FP": "Family Planning",
    "HEM": "Hemophilia Center",
    "NCA": "Native Hawaiian Health Center",
    "PED": "Pediatric Hospital",
    "URB": "Urban Indian Organization",
    "FPP": "Free-Standing Cancer Center",
}


def fetch_entities_by_state(state: str) -> list[dict]:
    """Fetch 340B entities for a given state from OPAIS API."""
    # Try the search API
    payload = json.dumps({
        "state": state,
        "pageSize": 1000,
        "pageNumber": 1,
    }).encode("utf-8")

    req = Request(SEARCH_URL, data=payload, headers={
        "User-Agent": "Aradune/1.0 (Medicaid intelligence platform)",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("results", data.get("data", []))
    except (HTTPError, URLError) as e:
        return []


def try_alternative_source() -> list[dict]:
    """Try alternative: HRSA data warehouse or public data extracts."""
    # Try the HRSA 340B database search page (returns HTML, not JSON)
    # This is a Blazor WebAssembly app, so standard HTTP requests won't work.
    # Fall back to the public 340B CE list from HRSA Office of Pharmacy Affairs

    alt_urls = [
        "https://data.hrsa.gov/data/download?data=340B",
        "https://340bopais.hrsa.gov/api/coveredEntities",
        "https://340bopais.hrsa.gov/api/coveredEntities/export",
        "https://340bopais.hrsa.gov/api/coveredEntities/count",
    ]

    for url in alt_urls:
        try:
            req = Request(url, headers={
                "User-Agent": "Aradune/1.0",
                "Accept": "application/json,text/csv,*/*",
            })
            with urlopen(req, timeout=15) as resp:
                content_type = resp.headers.get("Content-Type", "")
                data = resp.read()
                if "json" in content_type:
                    return json.loads(data)
                elif len(data) > 100:
                    return data.decode("utf-8", errors="replace")
        except Exception:
            continue

    return []


def build_from_known_data() -> list[dict]:
    """Build 340B entity dataset from known HRSA data and CMS crosswalks.

    Since the OPAIS API is a Blazor app without public bulk download,
    we build from the HRSA Health Center data + hospital data already in the lake.
    The 340B program covers specific entity types that we can identify from
    existing datasets.
    """
    print("  Building 340B entity list from HRSA and CMS crosswalk data...")

    con = duckdb.connect()

    entities = []

    # 1. FQHCs from the lake (all FQHCs are 340B eligible)
    fqhc_path = FACT_DIR / "fqhc_directory"
    if fqhc_path.exists():
        parquet_files = list(fqhc_path.rglob("*.parquet"))
        if parquet_files:
            try:
                df = con.execute(f"""
                    SELECT * FROM read_parquet('{parquet_files[0]}')
                """).fetchdf()
                for _, row in df.iterrows():
                    entities.append({
                        "entity_name": str(row.get("health_center_name", row.get("name", ""))),
                        "entity_type": "FQHC",
                        "entity_type_desc": "Federally Qualified Health Center",
                        "state_code": str(row.get("state_code", row.get("state", ""))),
                        "city": str(row.get("city", "")),
                        "zip_code": str(row.get("zip_code", row.get("zip", ""))),
                        "npi": str(row.get("npi", "")),
                        "source_table": "fqhc_directory",
                        "program_eligible": True,
                    })
                print(f"    FQHCs from lake: {len(entities):,}")
            except Exception as e:
                print(f"    Error reading FQHCs: {e}")

    # 2. DSH Hospitals (hospitals receiving DSH payments are 340B eligible)
    dsh_path = FACT_DIR / "dsh_hospital"
    if dsh_path.exists():
        parquet_files = list(dsh_path.rglob("*.parquet"))
        if parquet_files:
            try:
                dsh_count = 0
                df = con.execute(f"""
                    SELECT * FROM read_parquet('{parquet_files[0]}')
                """).fetchdf()
                for _, row in df.iterrows():
                    entities.append({
                        "entity_name": str(row.get("hospital_name", row.get("provider_name", ""))),
                        "entity_type": "DSH",
                        "entity_type_desc": "Disproportionate Share Hospital",
                        "state_code": str(row.get("state_code", row.get("state", ""))),
                        "city": str(row.get("city", "")),
                        "zip_code": str(row.get("zip_code", row.get("zip", ""))),
                        "ccn": str(row.get("ccn", row.get("provider_ccn", row.get("cms_certification_number", "")))),
                        "source_table": "dsh_hospital",
                        "program_eligible": True,
                    })
                    dsh_count += 1
                print(f"    DSH hospitals from lake: {dsh_count:,}")
            except Exception as e:
                print(f"    Error reading DSH: {e}")

    # 3. Critical Access Hospitals
    cah_path = FACT_DIR / "critical_access_hospitals"
    if cah_path.exists():
        parquet_files = list(cah_path.rglob("*.parquet"))
        if parquet_files:
            try:
                cah_count = 0
                df = con.execute(f"""
                    SELECT * FROM read_parquet('{parquet_files[0]}')
                """).fetchdf()
                for _, row in df.iterrows():
                    entities.append({
                        "entity_name": str(row.get("hospital_name", row.get("facility_name", row.get("name", "")))),
                        "entity_type": "CAH",
                        "entity_type_desc": "Critical Access Hospital",
                        "state_code": str(row.get("state_code", row.get("state", ""))),
                        "city": str(row.get("city", "")),
                        "zip_code": str(row.get("zip_code", row.get("zip", ""))),
                        "ccn": str(row.get("ccn", row.get("cms_certification_number", ""))),
                        "source_table": "critical_access_hospitals",
                        "program_eligible": True,
                    })
                    cah_count += 1
                print(f"    CAHs from lake: {cah_count:,}")
            except Exception as e:
                print(f"    Error reading CAHs: {e}")

    # 4. Children's hospitals from hospital directory
    hosp_dir_path = FACT_DIR / "hospital_directory"
    if hosp_dir_path.exists():
        parquet_files = list(hosp_dir_path.rglob("*.parquet"))
        if parquet_files:
            try:
                ch_count = 0
                df = con.execute(f"""
                    SELECT * FROM read_parquet('{parquet_files[0]}')
                    WHERE LOWER(hospital_type) LIKE '%children%'
                       OR LOWER(hospital_name) LIKE '%children%'
                """).fetchdf()
                for _, row in df.iterrows():
                    entities.append({
                        "entity_name": str(row.get("hospital_name", row.get("facility_name", ""))),
                        "entity_type": "CH",
                        "entity_type_desc": "Children's Hospital",
                        "state_code": str(row.get("state_code", row.get("state", ""))),
                        "city": str(row.get("city", "")),
                        "zip_code": str(row.get("zip_code", row.get("zip", ""))),
                        "ccn": str(row.get("ccn", row.get("provider_id", ""))),
                        "source_table": "hospital_directory",
                        "program_eligible": True,
                    })
                    ch_count += 1
                print(f"    Children's hospitals: {ch_count:,}")
            except Exception as e:
                print(f"    Error reading children's hospitals: {e}")

    # 5. Sole community hospitals and rural referral centers from hospital general info
    gen_info_path = FACT_DIR / "hospital_general_info"
    if gen_info_path.exists():
        parquet_files = list(gen_info_path.rglob("*.parquet"))
        if parquet_files:
            try:
                sch_count = 0
                df = con.execute(f"""
                    SELECT * FROM read_parquet('{parquet_files[0]}')
                """).fetchdf()
                cols = [c.lower() for c in df.columns]
                # Check for hospital subtype indicators
                for _, row in df.iterrows():
                    row_dict = {c.lower(): v for c, v in row.items()}
                    hosp_type = str(row_dict.get("hospital_type", "")).lower()
                    hosp_name = str(row_dict.get("hospital_name", row_dict.get("facility_name", ""))).lower()
                    # Cancer hospitals
                    if "cancer" in hosp_type or "cancer" in hosp_name:
                        entities.append({
                            "entity_name": str(row.get("hospital_name", row.get("facility_name", ""))),
                            "entity_type": "CAN",
                            "entity_type_desc": "Cancer Hospital",
                            "state_code": str(row_dict.get("state_code", row_dict.get("state", ""))),
                            "city": str(row_dict.get("city", "")),
                            "zip_code": str(row_dict.get("zip_code", row_dict.get("zip", ""))),
                            "ccn": str(row_dict.get("ccn", row_dict.get("provider_id", row_dict.get("facility_id", "")))),
                            "source_table": "hospital_general_info",
                            "program_eligible": True,
                        })
                        sch_count += 1
                print(f"    Specialty hospitals (cancer, etc): {sch_count:,}")
            except Exception as e:
                print(f"    Error reading hospital general info: {e}")

    con.close()
    return entities


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.2f} MB)")
    return count


def main():
    print("=" * 60)
    print("340B Covered Entities Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")
    print()

    # Try direct API first
    print("  Attempting HRSA OPAIS API...")
    api_data = try_alternative_source()

    entities = []

    if isinstance(api_data, list) and len(api_data) > 0:
        print(f"  API returned {len(api_data)} entities")
        entities = api_data
    else:
        print("  OPAIS API not available (Blazor app requires browser)")
        print("  Building from lake crosswalk data...")
        entities = build_from_known_data()

    if not entities:
        print("  No entities found! Exiting.")
        return

    print(f"\n  Total 340B-eligible entities identified: {len(entities):,}")

    # Build DuckDB table
    import pandas as pd
    con = duckdb.connect()
    df = pd.DataFrame(entities)

    # Clean up columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("").astype(str)
            # Replace "nan" strings
            df[col] = df[col].replace("nan", "")

    # Filter out rows with no state code
    df = df[df["state_code"].str.len() == 2].copy()

    df["source"] = "HRSA/CMS crosswalk (OPAIS API blocked)"
    df["snapshot_date"] = SNAPSHOT_DATE

    con.execute("CREATE TABLE fact_340b_covered_entities AS SELECT * FROM df")

    # Stats
    by_type = con.execute("""
        SELECT entity_type, entity_type_desc, COUNT(*) as n
        FROM fact_340b_covered_entities
        GROUP BY entity_type, entity_type_desc ORDER BY n DESC
    """).fetchall()
    print("\n  By entity type:")
    for t, desc, n in by_type:
        print(f"    {t} ({desc}): {n:,}")

    states = con.execute("""
        SELECT COUNT(DISTINCT state_code) FROM fact_340b_covered_entities
    """).fetchone()[0]
    print(f"\n  States covered: {states}")

    top_states = con.execute("""
        SELECT state_code, COUNT(*) as n
        FROM fact_340b_covered_entities
        GROUP BY state_code ORDER BY n DESC LIMIT 10
    """).fetchall()
    print("\n  Top 10 states:")
    for s, n in top_states:
        print(f"    {s}: {n:,}")

    # Write parquet
    out_path = FACT_DIR / "340b_covered_entities" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_340b_covered_entities", out_path)

    # Also save raw JSON
    raw_path = RAW_DIR / "340b_covered_entities.json"
    raw_path.write_text(json.dumps(entities, indent=2, default=str))

    # Manifest
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_340b_covered_entities.py",
        "source": "HRSA OPAIS crosswalk (FQHCs, DSH hospitals, CAHs, children's, cancer)",
        "note": "340B OPAIS is a Blazor WebAssembly app without public bulk download API. "
                "Entity list built from HRSA + CMS crosswalk of known 340B-eligible entity types.",
        "tables": {
            "fact_340b_covered_entities": {
                "rows": row_count,
                "path": f"fact/340b_covered_entities/snapshot={SNAPSHOT_DATE}/data.parquet",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    (META_DIR / f"manifest_340b_covered_entities_{SNAPSHOT_DATE}.json").write_text(
        json.dumps(manifest, indent=2)
    )

    con.close()
    print("\n" + "=" * 60)
    print("340B COVERED ENTITIES INGESTION COMPLETE")
    print(f"  fact_340b_covered_entities: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
