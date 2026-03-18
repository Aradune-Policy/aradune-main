"""Rate Explorer -- per-code Medicaid rate lookup across all jurisdictions."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/rate-explorer")
@safe_route(default_response={"rows": [], "count": 0})
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
@safe_route(default_response={"results": []})
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


@router.get("/api/rates/state-summary")
@safe_route(default_response=[])
async def rate_state_summary():
    """Per-state aggregate rate adequacy metrics from fact_rate_comparison_v2."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT
                    rc.state_code,
                    ds.state_name,
                    COUNT(*) AS total_codes,
                    ROUND(MEDIAN(rc.pct_of_medicare) * 100, 1) AS median_pct_medicare,
                    ROUND(AVG(rc.pct_of_medicare) * 100, 1) AS avg_pct_medicare,
                    COUNT(*) FILTER (WHERE rc.pct_of_medicare < 0.6) AS codes_below_60,
                    COUNT(*) FILTER (WHERE rc.pct_of_medicare < 0.8) AS codes_below_80,
                    COUNT(*) FILTER (WHERE rc.pct_of_medicare >= 1.0) AS codes_at_parity,
                    MODE(rc.rate_source) AS primary_rate_source
                FROM fact_rate_comparison_v2 rc
                LEFT JOIN dim_state ds ON rc.state_code = ds.state_code
                WHERE rc.pct_of_medicare > 0 AND rc.pct_of_medicare < 10
                GROUP BY rc.state_code, ds.state_name
                ORDER BY median_pct_medicare DESC
            """).fetchall()
            columns = [
                "state_code", "state_name", "total_codes",
                "median_pct_medicare", "avg_pct_medicare",
                "codes_below_60", "codes_below_80", "codes_at_parity",
                "primary_rate_source"
            ]
            return [dict(zip(columns, r)) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate state-summary failed: {exc}")


@router.get("/api/rates/compare-states")
@safe_route(default_response={"states": [], "codes": [], "summary": {}})
async def rate_compare_states(
    states: str = Query(..., description="Comma-separated state codes, e.g. FL,GA,TX"),
    category: Optional[str] = Query(default=None, description="Optional procedure category filter")
):
    """Side-by-side rate comparison for 2-3 selected states."""
    state_list = [s.strip().upper() for s in states.split(",") if s.strip()]
    if len(state_list) < 2 or len(state_list) > 3:
        raise HTTPException(status_code=400, detail="Provide 2 or 3 comma-separated state codes.")

    try:
        with get_cursor() as cur:
            # Build parameterized query for requested states
            placeholders = ", ".join(f"${i+1}" for i in range(len(state_list)))
            params = list(state_list)

            category_filter = ""
            if category:
                params.append(category)
                category_filter = f"AND rc.category = ${len(params)}"

            rows = cur.execute(f"""
                SELECT
                    rc.procedure_code,
                    dp.description,
                    dp.category,
                    rc.medicare_rate,
                    rc.state_code,
                    rc.medicaid_rate,
                    rc.pct_of_medicare,
                    rc.rate_source
                FROM fact_rate_comparison_v2 rc
                LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
                WHERE rc.state_code IN ({placeholders})
                  AND rc.pct_of_medicare > 0
                  AND rc.pct_of_medicare < 10
                  {category_filter}
                ORDER BY rc.procedure_code, rc.state_code
            """, params).fetchall()

            # Pivot rows into per-code structure with nested state rates
            codes_map = {}
            state_totals = {s: {"sum_pct": 0.0, "count": 0} for s in state_list}

            for row in rows:
                proc_code, description, cat, medicare_rate, st, medicaid_rate, pct, source = row
                key = proc_code

                if key not in codes_map:
                    codes_map[key] = {
                        "procedure_code": proc_code,
                        "description": description,
                        "category": cat,
                        "medicare_rate": float(medicare_rate) if medicare_rate is not None else None,
                        "rates": {}
                    }

                codes_map[key]["rates"][st] = {
                    "rate": float(medicaid_rate) if medicaid_rate is not None else None,
                    "pct_medicare": round(float(pct) * 100, 1) if pct is not None else None,
                    "source": source
                }

                if pct is not None:
                    state_totals[st]["sum_pct"] += float(pct) * 100
                    state_totals[st]["count"] += 1

            codes_list = list(codes_map.values())

            summary = {}
            for st in state_list:
                t = state_totals[st]
                summary[st] = {
                    "avg_pct": round(t["sum_pct"] / t["count"], 1) if t["count"] > 0 else None,
                    "total_codes": t["count"]
                }

            return {
                "states": state_list,
                "codes": codes_list,
                "summary": summary
            }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rate compare-states failed: {exc}")
