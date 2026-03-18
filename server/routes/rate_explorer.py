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


@router.get("/api/rates/context/{state_code}")
@safe_route(default_response={})
async def rate_context(state_code: str):
    """Cross-dataset context for rate analysis: workforce, access, quality, fiscal, claims."""
    state_code = state_code.upper()
    result = {}

    with get_cursor() as cur:
        # 1. FMAP (federal match rate)
        try:
            row = cur.execute("""
                SELECT fmap, methodology, conversion_factor
                FROM dim_state WHERE state_code = $1
            """, [state_code]).fetchone()
            if row:
                result["fmap"] = {"rate": row[0], "methodology": row[1], "conversion_factor": row[2]}
        except:
            pass

        # 2. Enrollment (latest)
        try:
            row = cur.execute("""
                SELECT total_enrollment, mc_enrollment, ffs_enrollment, year, month
                FROM fact_enrollment
                WHERE state_code = $1
                ORDER BY year DESC, month DESC LIMIT 1
            """, [state_code]).fetchone()
            if row:
                mc_pct = round(row[1] / row[0] * 100, 1) if row[0] and row[1] else None
                result["enrollment"] = {
                    "total": row[0], "managed_care": row[1], "ffs": row[2],
                    "mc_pct": mc_pct, "year": row[3], "month": row[4]
                }
        except:
            pass

        # 3. HPSA counts (primary care, dental, mental health)
        try:
            rows = cur.execute("""
                SELECT
                    COUNT(*) AS total_hpsas,
                    COUNT(*) FILTER (WHERE LOWER(hpsa_discipline_class) LIKE '%primary%'
                        OR LOWER(discipline_type) LIKE '%primary%') AS primary_care,
                    COUNT(*) FILTER (WHERE LOWER(hpsa_discipline_class) LIKE '%dental%'
                        OR LOWER(discipline_type) LIKE '%dental%') AS dental,
                    COUNT(*) FILTER (WHERE LOWER(hpsa_discipline_class) LIKE '%mental%'
                        OR LOWER(discipline_type) LIKE '%mental%') AS mental_health
                FROM fact_hpsa WHERE state_code = $1
            """, [state_code]).fetchone()
            if rows:
                result["hpsa"] = {
                    "total": rows[0], "primary_care": rows[1],
                    "dental": rows[2], "mental_health": rows[3]
                }
        except:
            pass

        # 4. Workforce (CNA/HHA median wage)
        try:
            row = cur.execute("""
                SELECT occupation_title, hourly_mean, hourly_median, annual_mean
                FROM fact_bls_wage
                WHERE state_code = $1
                AND (LOWER(occupation_title) LIKE '%nursing assist%'
                     OR LOWER(occupation_title) LIKE '%home health aide%'
                     OR soc_code IN ('31-1131', '31-1121'))
                LIMIT 1
            """, [state_code]).fetchone()
            if row:
                result["workforce"] = {
                    "occupation": row[0], "hourly_mean": row[1],
                    "hourly_median": row[2], "annual_mean": row[3]
                }
        except:
            pass

        # 5. Quality (core set summary - count of measures below median)
        try:
            rows = cur.execute("""
                SELECT COUNT(*) AS total_measures,
                    COUNT(*) FILTER (WHERE state_rate < median_rate) AS below_median
                FROM fact_quality_core_set_2024
                WHERE state_code = $1 AND state_rate IS NOT NULL AND median_rate IS NOT NULL
            """, [state_code]).fetchone()
            if rows:
                result["quality"] = {
                    "total_measures": rows[0], "below_median": rows[1],
                    "pct_below_median": round(rows[1] / max(rows[0], 1) * 100, 1)
                }
        except:
            pass

        # 6. CMS-64 expenditure (latest fiscal year)
        try:
            row = cur.execute("""
                SELECT fiscal_year, SUM(total_computable) AS total_spending,
                    SUM(federal_share) AS federal_spending
                FROM fact_cms64_multiyear
                WHERE state_code = $1 AND LOWER(category) = 'total'
                GROUP BY fiscal_year
                ORDER BY fiscal_year DESC LIMIT 1
            """, [state_code]).fetchone()
            if row:
                result["expenditure"] = {
                    "fiscal_year": row[0], "total_computable": row[1],
                    "federal_share": row[2]
                }
        except:
            pass

        # 7. T-MSIS claims-based effective rates (labeled clearly)
        try:
            row = cur.execute("""
                SELECT COUNT(*) AS total_codes,
                    ROUND(MEDIAN(pct_of_medicare) * 100, 1) AS median_pct_medicare,
                    ROUND(AVG(effective_paid_rate), 2) AS avg_paid_rate
                FROM fact_tmsis_effective_rates
                WHERE state_code = $1 AND pct_of_medicare > 0 AND pct_of_medicare < 10
            """, [state_code]).fetchone()
            if row and row[0] > 0:
                result["tmsis_claims"] = {
                    "total_codes": row[0], "median_pct_medicare": row[1],
                    "avg_paid_rate": row[2],
                    "caveat": "Claims-based: reflects actual paid amounts across all modifiers and settings, not fee schedule maximums. Expect lower than published rates."
                }
        except:
            pass

        # 8. Supplemental payments (DSH + SDP)
        try:
            dsh = cur.execute("""
                SELECT SUM(dsh_allotment) AS dsh_total
                FROM fact_dsh_hospital WHERE state_code = $1
            """, [state_code]).fetchone()
            if dsh and dsh[0]:
                result["supplemental"] = {"dsh_total": dsh[0]}
        except:
            pass

        try:
            sdp = cur.execute("""
                SELECT COUNT(*) AS sdp_count, SUM(total_expenditure) AS sdp_total
                FROM fact_sdp_preprint WHERE state_code = $1
            """, [state_code]).fetchone()
            if sdp and sdp[0] > 0:
                if "supplemental" not in result:
                    result["supplemental"] = {}
                result["supplemental"]["sdp_count"] = sdp[0]
                result["supplemental"]["sdp_total"] = sdp[1]
        except:
            pass

    return result