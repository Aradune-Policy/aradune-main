"""Ingest IPF quality + LTCH data from CMS Provider Data API into the data lake."""
import json, os, re
import pyarrow as pa
import pyarrow.parquet as pq

def snake(s):
    s = re.sub(r'[/\\()\-]', '_', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '', s)
    s = re.sub(r'__+', '_', s).strip('_')
    return s.lower()

def ingest(raw_path, table_name, desc):
    with open(raw_path) as f:
        rows = json.load(f)
    if not rows:
        print(f"  {table_name}: empty file, skipping")
        return 0
    
    # Clean column names
    cleaned = []
    for row in rows:
        cleaned.append({snake(k): v for k, v in row.items()})
    
    # Convert to Arrow table
    table = pa.Table.from_pylist(cleaned)
    
    # Write parquet
    out_dir = f"data/lake/fact/{table_name}"
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(table, f"{out_dir}/data.parquet", compression="zstd")
    print(f"  {table_name}: {len(rows):,} rows, {len(table.schema)} cols -> {out_dir}/data.parquet")
    return len(rows)

total = 0
total += ingest("data/raw/ipf_quality.json", "ipf_quality", "Inpatient Psychiatric Facility quality measures by facility")
total += ingest("data/raw/ipf_quality_state.json", "ipf_quality_state", "IPF quality measures by state")
total += ingest("data/raw/ltch_general.json", "ltch_general", "Long-Term Care Hospital general information")
total += ingest("data/raw/ltch_provider.json", "ltch_provider_data", "LTCH provider-level quality data")
print(f"\nTotal: {total:,} rows ingested across 4 tables")
