"""Rate Explorer -- per-code Medicaid rate lookup across all jurisdictions."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/rate-explorer")
async def rate_explorer(code: str = Query(...), modifier: str = Query(default="")):
    """Return Medicaid rates for a procedure code across all jurisdictions from fact_rate_comparison_v2."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, medicaid_rate, medicare_rate, pct_of_medicare, rate_source
                FROM fact_rate_comparison_v2
                WHERE procedure_code = $1
                  AND modifier = $2
                ORDER BY pct_of_medicare DESC
            """, [code, modifier]).fetchall()
            columns = ["state_code", "medicaid_rate", "medicare_rate", "pct_of_medicare", "rate_source"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-explorer failed: {exc}")


@router.get("/api/rate-explorer/search")
async def rate_explorer_search(q: str = Query(...)):
    """Search dim_procedure for codes matching by code or description."""
    try:
        pattern = f"%{q}%"
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT procedure_code, description, category, is_em_code, medicare_rate_nonfac
                FROM dim_procedure
                WHERE procedure_code ILIKE $1 OR description ILIKE $1
                ORDER BY procedure_code
                LIMIT 20
            """, [pattern]).fetchall()
            columns = ["procedure_code", "description", "category", "is_em_code", "medicare_rate_nonfac"]
            return {"results": [dict(zip(columns, r)) for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate-explorer search failed: {exc}")
