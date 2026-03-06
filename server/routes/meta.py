from fastapi import APIRouter, HTTPException
from server.models import QueryMeta
from server.db import get_cursor
from server.presets import PRESETS

router = APIRouter()


@router.get("/api/meta", response_model=QueryMeta)
async def meta():
    try:
        with get_cursor() as cur:
            states = [r[0] for r in cur.execute(
                "SELECT DISTINCT state FROM spending WHERE state IS NOT NULL ORDER BY state"
            ).fetchall()]

            categories = [r[0] for r in cur.execute(
                "SELECT DISTINCT category FROM spending WHERE category IS NOT NULL ORDER BY category"
            ).fetchall()]

            date_range = cur.execute(
                "SELECT MIN(CLAIM_FROM_MONTH), MAX(CLAIM_FROM_MONTH) FROM spending"
            ).fetchone()

            columns = [r[0] for r in cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'spending' ORDER BY ordinal_position"
            ).fetchall()]

            total_rows = cur.execute("SELECT COUNT(*) FROM spending").fetchone()[0]

        return QueryMeta(
            states=states,
            categories=categories,
            date_min=str(date_range[0]) if date_range and date_range[0] else None,
            date_max=str(date_range[1]) if date_range and date_range[1] else None,
            columns=columns,
            total_rows=total_rows,
            presets=list(PRESETS.keys()),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Meta query error: {e}")
