"""
Introspect the Aradune data lake: DESCRIBE + COUNT every registered table.
Output: ontology/raw_inventory.json

Usage: python3 scripts/introspect_lake.py
"""
import duckdb
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
LAKE = ROOT / "data" / "lake"
OUTPUT = ROOT / "ontology" / "raw_inventory.json"


def latest_snapshot(fact_dir: Path, fact_name: str) -> Path | None:
    fact_path = fact_dir / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


def introspect():
    con = duckdb.connect()
    dim_dir = LAKE / "dimension"
    fact_dir = LAKE / "fact"
    ref_dir = LAKE / "reference"

    # Register all dimension tables
    dims = {}
    if dim_dir.exists():
        for pf in dim_dir.glob("*.parquet"):
            view_name = pf.stem
            con.execute(f"CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM '{pf}'")
            dims[view_name] = pf

    # Register all fact tables (latest snapshot)
    facts = {}
    if fact_dir.exists():
        for subdir in sorted(fact_dir.iterdir()):
            if not subdir.is_dir():
                continue
            snap = latest_snapshot(fact_dir, subdir.name)
            if snap:
                view_name = f"fact_{subdir.name}"
                con.execute(f"CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM '{snap}'")
                facts[view_name] = snap

    # Register reference tables
    refs = {}
    if ref_dir.exists():
        for pf in ref_dir.glob("*.parquet"):
            view_name = pf.stem
            con.execute(f"CREATE VIEW IF NOT EXISTS {view_name} AS SELECT * FROM '{pf}'")
            refs[view_name] = pf
        for subdir in ref_dir.iterdir():
            if subdir.is_dir():
                snap = latest_snapshot(ref_dir, subdir.name)
                if snap:
                    con.execute(f"CREATE VIEW IF NOT EXISTS {subdir.name} AS SELECT * FROM '{snap}'")
                    refs[subdir.name] = snap

    all_tables = {}
    all_tables.update({k: ("dimension", v) for k, v in dims.items()})
    all_tables.update({k: ("fact", v) for k, v in facts.items()})
    all_tables.update({k: ("reference", v) for k, v in refs.items()})

    inventory = {}
    total = len(all_tables)
    for i, (view_name, (layer, parquet_path)) in enumerate(sorted(all_tables.items()), 1):
        try:
            schema = con.execute(f"DESCRIBE {view_name}").fetchall()
            row_count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            # Sample 3 rows
            sample_df = con.execute(f"SELECT * FROM {view_name} LIMIT 3").fetchdf()
            sample = []
            for _, row in sample_df.iterrows():
                r = {}
                for col in sample_df.columns:
                    v = row[col]
                    if hasattr(v, "isoformat"):
                        r[col] = v.isoformat()
                    elif v is not None and not isinstance(v, (str, int, float, bool)):
                        r[col] = str(v)
                    else:
                        r[col] = v
                sample.append(r)

            columns = []
            for col in schema:
                columns.append({
                    "name": col[0],
                    "type": col[1],
                    "nullable": col[2] == "YES" if len(col) > 2 else True,
                })

            inventory[view_name] = {
                "layer": layer,
                "columns": columns,
                "row_count": row_count,
                "sample": sample,
            }
            if i % 50 == 0 or i == total:
                print(f"  [{i}/{total}] {view_name}: {row_count:,} rows, {len(columns)} cols")
        except Exception as e:
            inventory[view_name] = {"layer": layer, "error": str(e)}
            print(f"  [{i}/{total}] {view_name}: ERROR - {e}")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(inventory, f, indent=2, default=str)

    # Summary
    dim_count = sum(1 for v in inventory.values() if v.get("layer") == "dimension")
    fact_count = sum(1 for v in inventory.values() if v.get("layer") == "fact")
    ref_count = sum(1 for v in inventory.values() if v.get("layer") == "reference")
    err_count = sum(1 for v in inventory.values() if "error" in v)
    total_rows = sum(v.get("row_count", 0) for v in inventory.values() if "row_count" in v)

    print(f"\nIntrospection complete:")
    print(f"  {dim_count} dimension, {fact_count} fact, {ref_count} reference tables")
    print(f"  {err_count} errors")
    print(f"  {total_rows:,} total rows")
    print(f"  Output: {OUTPUT}")
    return inventory


if __name__ == "__main__":
    introspect()
