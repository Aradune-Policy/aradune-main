"""Pipeline health and status routes."""

import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter
from server.config import settings
from server.utils.error_handler import safe_route

router = APIRouter()

LAKE_DIR = Path(settings.lake_dir)
META_DIR = LAKE_DIR / "metadata"


@router.get("/api/pipeline/status")
@safe_route(default_response={})
async def pipeline_status():
    """Get pipeline status: last run, snapshot dates, data freshness."""
    status = {
        "lake_dir": str(LAKE_DIR),
        "lake_exists": LAKE_DIR.exists(),
        "dimensions": {},
        "facts": {},
        "last_manifest": None,
    }

    # Dimension files
    dim_dir = LAKE_DIR / "dimension"
    if dim_dir.exists():
        for f in dim_dir.glob("*.parquet"):
            mtime = datetime.fromtimestamp(f.stat().st_mtime).isoformat()
            status["dimensions"][f.stem] = {
                "size_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "modified": mtime,
            }

    # Fact snapshots
    fact_dir = LAKE_DIR / "fact"
    if fact_dir.exists():
        for fact in sorted(fact_dir.iterdir()):
            if not fact.is_dir():
                continue
            snapshots = sorted(fact.glob("snapshot=*/data.parquet"), reverse=True)
            if snapshots:
                latest = snapshots[0]
                snapshot_date = latest.parent.name.replace("snapshot=", "")
                status["facts"][fact.name] = {
                    "latest_snapshot": snapshot_date,
                    "snapshot_count": len(snapshots),
                    "size_mb": round(latest.stat().st_size / (1024 * 1024), 2),
                    "modified": datetime.fromtimestamp(latest.stat().st_mtime).isoformat(),
                }

    # Latest manifest
    if META_DIR.exists():
        manifests = sorted(META_DIR.glob("manifest_*.json"), reverse=True)
        if manifests:
            with open(manifests[0]) as f:
                status["last_manifest"] = json.load(f)

    return status
