"""
Shared fixtures for Aradune test suite.

Provides a DuckDB connection with all lake views registered,
matching the server's db.py view registration pattern.
"""

import json
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = ROOT / "data" / "lake"
INVENTORY = ROOT / "ontology" / "raw_inventory.json"


def _latest_snapshot(fact_dir: Path, fact_name: str) -> Path | None:
    fact_path = fact_dir / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


@pytest.fixture(scope="session")
def lake_db():
    """Session-scoped DuckDB connection with all lake views registered."""
    conn = duckdb.connect(":memory:")
    dim_dir = LAKE_DIR / "dimension"
    fact_dir = LAKE_DIR / "fact"
    ref_dir = LAKE_DIR / "reference"

    # Register dimension tables (flat parquet files, not subdirectories)
    if dim_dir.exists():
        for pq in dim_dir.glob("*.parquet"):
            name = pq.stem  # e.g. dim_state.parquet -> dim_state
            conn.execute(f"CREATE VIEW {name} AS SELECT * FROM '{pq}'")

    # Register fact tables (latest snapshot)
    if fact_dir.exists():
        for fact_path in sorted(fact_dir.iterdir()):
            if not fact_path.is_dir():
                continue
            snap = _latest_snapshot(fact_dir, fact_path.name)
            if snap:
                conn.execute(
                    f"CREATE VIEW fact_{fact_path.name} AS SELECT * FROM '{snap}'"
                )

    # Register reference tables (mixed: flat parquet or subdirectory/data.parquet)
    if ref_dir.exists():
        for pq in ref_dir.glob("*.parquet"):
            name = pq.stem
            conn.execute(f"CREATE VIEW {name} AS SELECT * FROM '{pq}'")
        for pq in ref_dir.glob("*/data.parquet"):
            name = pq.parent.name
            if name not in conn.execute("SELECT name FROM duckdb_views()").df()["name"].values:
                conn.execute(f"CREATE VIEW {name} AS SELECT * FROM '{pq}'")

    # Compatibility views
    try:
        conn.execute(
            "CREATE VIEW hcbs_payment_method AS SELECT * FROM ref_hcbs_payment_method"
        )
    except Exception:
        pass

    yield conn
    conn.close()


@pytest.fixture(scope="session")
def inventory():
    """Parsed raw_inventory.json."""
    if INVENTORY.exists():
        return json.load(open(INVENTORY))
    return {}
