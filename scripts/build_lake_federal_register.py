#!/usr/bin/env python3
"""
build_lake_federal_register.py — Ingest Federal Register Medicaid rulemaking.

Source: https://www.federalregister.gov/api/v1/
Searches for CMS documents mentioning "medicaid" — final rules, proposed rules,
and notices. Captures metadata, abstracts, and full-text URLs.

Tables built:
  fact_federal_register — Federal Register documents related to Medicaid.

Usage:
  python3 scripts/build_lake_federal_register.py
"""

import json
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlopen, Request

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
FACT_DIR = LAKE_DIR / "fact"
META_DIR = LAKE_DIR / "metadata"

SNAPSHOT_DATE = date.today().isoformat()
RUN_ID = str(uuid.uuid4())

BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"
FIELDS = [
    "document_number", "title", "type", "abstract", "publication_date",
    "agencies", "action", "dates", "citation", "start_page", "end_page",
    "html_url", "pdf_url", "raw_text_url", "docket_ids",
    "regulation_id_numbers", "cfr_references", "topics",
    "significant", "executive_order_number",
]


def fetch_page(page: int, doc_type: str = None) -> dict:
    """Fetch one page of results from Federal Register API."""
    params = (
        f"conditions[agencies][]=centers-for-medicare-medicaid-services"
        f"&conditions[term]=medicaid"
        f"&per_page=100&page={page}&order=newest"
        f"&fields[]={'&fields[]='.join(FIELDS)}"
    )
    if doc_type:
        params += f"&conditions[type][]={doc_type}"

    url = f"{BASE_URL}?{params}"
    req = Request(url, headers={"User-Agent": "Aradune/1.0 (Medicaid intelligence platform)"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_all_documents() -> list[dict]:
    """Fetch all CMS Medicaid documents from Federal Register."""
    all_docs = []

    for doc_type in ["RULE", "PRORULE", "NOTICE"]:
        type_label = {"RULE": "Final Rules", "PRORULE": "Proposed Rules", "NOTICE": "Notices"}[doc_type]
        page = 1
        type_count = 0

        while True:
            try:
                data = fetch_page(page, doc_type)
            except Exception as e:
                print(f"    Error on page {page}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            total = data.get("count", 0)
            if page == 1:
                print(f"  {type_label}: {total:,} documents")

            for doc in results:
                agencies = [a.get("name", "") for a in doc.get("agencies", [])]
                cfr_refs = [
                    f"{r.get('title', '')} CFR {r.get('part', '')}"
                    for r in doc.get("cfr_references", [])
                ]
                row = {
                    "document_number": doc.get("document_number"),
                    "title": doc.get("title"),
                    "doc_type": doc.get("type"),
                    "abstract": doc.get("abstract"),
                    "publication_date": doc.get("publication_date"),
                    "agencies": "; ".join(agencies),
                    "action": doc.get("action"),
                    "citation": doc.get("citation"),
                    "start_page": doc.get("start_page"),
                    "end_page": doc.get("end_page"),
                    "html_url": doc.get("html_url"),
                    "pdf_url": doc.get("pdf_url"),
                    "raw_text_url": doc.get("raw_text_url"),
                    "docket_ids": "; ".join(doc.get("docket_ids") or []),
                    "regulation_ids": "; ".join(doc.get("regulation_id_numbers") or []),
                    "cfr_references": "; ".join(cfr_refs),
                    "topics": "; ".join(doc.get("topics") or []),
                    "significant": doc.get("significant"),
                }
                all_docs.append(row)
                type_count += 1

            # Next page
            next_url = data.get("next_page_url")
            if not next_url or page * 100 >= total:
                break
            page += 1
            time.sleep(0.3)  # Be polite

        print(f"    Fetched {type_count:,} {type_label.lower()}")

    return all_docs


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 500000)"
    )
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    size_mb = path.stat().st_size / 1_048_576
    print(f"  -> {path.relative_to(PROJECT_ROOT)} ({count:,} rows, {size_mb:.1f} MB)")
    return count


def main():
    print("=" * 60)
    print("Federal Register Medicaid Rulemaking Ingestion")
    print(f"  Snapshot: {SNAPSHOT_DATE}")
    print(f"  Run ID:   {RUN_ID}")

    docs = fetch_all_documents()
    print(f"\n  Total documents fetched: {len(docs):,}")

    if not docs:
        print("  No documents found!")
        return

    con = duckdb.connect()
    import pandas as pd
    df = pd.DataFrame(docs)
    df["source"] = "https://www.federalregister.gov"
    df["snapshot_date"] = SNAPSHOT_DATE

    con.execute("CREATE TABLE fact_federal_register AS SELECT * FROM df")

    # Stats
    types = con.execute("""
        SELECT doc_type, COUNT(*) as n
        FROM fact_federal_register GROUP BY doc_type ORDER BY n DESC
    """).fetchall()
    print("\n  By type:")
    for t, n in types:
        print(f"    {t}: {n:,}")

    years = con.execute("""
        SELECT EXTRACT(YEAR FROM CAST(publication_date AS DATE)) as yr, COUNT(*) as n
        FROM fact_federal_register
        WHERE publication_date IS NOT NULL
        GROUP BY yr ORDER BY yr DESC LIMIT 10
    """).fetchall()
    print("\n  Recent years:")
    for yr, n in years:
        print(f"    {int(yr)}: {n:,}")

    out_path = FACT_DIR / "federal_register" / f"snapshot={SNAPSHOT_DATE}" / "data.parquet"
    row_count = write_parquet(con, "fact_federal_register", out_path)

    # Manifest
    META_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": RUN_ID,
        "snapshot_date": SNAPSHOT_DATE,
        "script": "build_lake_federal_register.py",
        "source": "https://www.federalregister.gov/api/v1/",
        "tables": {
            "fact_federal_register": {
                "rows": row_count,
                "path": f"fact/federal_register/snapshot={SNAPSHOT_DATE}/data.parquet",
            }
        },
        "completed_at": datetime.now().isoformat() + "Z",
    }
    (META_DIR / f"manifest_federal_register_{SNAPSHOT_DATE}.json").write_text(
        json.dumps(manifest, indent=2)
    )

    con.close()
    print("\n" + "=" * 60)
    print("FEDERAL REGISTER INGESTION COMPLETE")
    print(f"  fact_federal_register: {row_count:,} rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
