"""Nursing Home Ownership & Quality — Five-Star ratings, staffing, and deficiencies by ownership type and chain affiliation."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/nursing-ownership/quality-by-type")
async def nursing_quality_by_type():
    """Five-Star ratings aggregated by ownership type (For-Profit, Non-Profit, Government)."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT
                    ownership_type,
                    COUNT(*) AS facility_count,
                    ROUND(AVG(overall_rating), 2) AS avg_overall_rating,
                    ROUND(AVG(health_inspection_rating), 2) AS avg_inspection_rating,
                    ROUND(AVG(qm_rating), 2) AS avg_qm_rating,
                    ROUND(AVG(staffing_rating), 2) AS avg_staffing_rating,
                    ROUND(AVG(hprd_total), 2) AS avg_total_hprd,
                    ROUND(AVG(hprd_rn), 2) AS avg_rn_hprd,
                    ROUND(AVG(deficiency_count), 1) AS avg_deficiencies,
                    ROUND(AVG(fine_total_dollars), 0) AS avg_fine_dollars
                FROM fact_five_star
                WHERE ownership_type IS NOT NULL
                GROUP BY ownership_type
                ORDER BY avg_overall_rating DESC
            """).fetchall()
            columns = [
                "ownership_type", "facility_count", "avg_overall_rating",
                "avg_inspection_rating", "avg_qm_rating", "avg_staffing_rating",
                "avg_total_hprd", "avg_rn_hprd", "avg_deficiencies", "avg_fine_dollars",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Quality by ownership type query failed", "detail": str(e)})


@router.get("/api/research/nursing-ownership/chain-vs-independent")
async def nursing_chain_vs_independent():
    """Compare chain-affiliated vs independent facilities by ownership type."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT
                    CASE WHEN chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
                         THEN 'Chain-Affiliated'
                         ELSE 'Independent'
                    END AS affiliation,
                    ownership_type,
                    COUNT(*) AS facility_count,
                    ROUND(AVG(overall_rating), 2) AS avg_overall,
                    ROUND(AVG(staffing_rating), 2) AS avg_staffing,
                    ROUND(AVG(hprd_total), 2) AS avg_hprd,
                    ROUND(AVG(deficiency_count), 1) AS avg_deficiencies,
                    ROUND(AVG(fine_total_dollars), 0) AS avg_fines,
                    ROUND(AVG(turnover_rn_pct), 1) AS avg_rn_turnover
                FROM fact_five_star
                WHERE ownership_type IS NOT NULL
                GROUP BY affiliation, ownership_type
                ORDER BY affiliation, ownership_type
            """).fetchall()
            columns = [
                "affiliation", "ownership_type", "facility_count", "avg_overall",
                "avg_staffing", "avg_hprd", "avg_deficiencies", "avg_fines",
                "avg_rn_turnover",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Chain vs independent query failed", "detail": str(e)})


@router.get("/api/research/nursing-ownership/deficiency-patterns")
async def nursing_deficiency_patterns():
    """Top deficiency tags by ownership type and chain affiliation."""
    try:
        with get_cursor() as cur:
            # fact_nh_deficiency uses: provider_ccn, tag_number, description, scope_severity_code, severity_level
            # fact_five_star uses: provider_ccn (mapped from federal_provider_number), ownership_type, chain_name
            rows = cur.execute("""
                WITH ownership AS (
                    SELECT provider_ccn, ownership_type,
                           CASE WHEN chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
                                THEN 'Chain' ELSE 'Independent' END AS affiliation
                    FROM fact_five_star
                )
                SELECT o.ownership_type, o.affiliation,
                       d.tag_number,
                       d.description AS deficiency_description,
                       COUNT(*) AS citation_count,
                       ROUND(AVG(COALESCE(d.severity_level, 1)), 1) AS avg_severity
                FROM fact_nh_deficiency d
                INNER JOIN ownership o ON d.provider_ccn = o.provider_ccn
                WHERE o.ownership_type IS NOT NULL
                GROUP BY o.ownership_type, o.affiliation, d.tag_number, d.description
                ORDER BY citation_count DESC
                LIMIT 100
            """).fetchall()
            columns = [
                "ownership_type", "affiliation", "tag_number",
                "deficiency_description", "citation_count", "avg_severity",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Deficiency patterns query failed", "detail": str(e)})


@router.get("/api/research/nursing-ownership/chain-scoreboard")
async def nursing_chain_scoreboard(limit: int = Query(default=50, ge=1, le=500)):
    """Top chains ranked by quality (ascending -- worst first). Requires >= 5 facilities."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT chain_name,
                       COUNT(*) AS facility_count,
                       ROUND(AVG(overall_rating), 2) AS avg_overall_rating,
                       ROUND(AVG(staffing_rating), 2) AS avg_staffing_rating,
                       ROUND(AVG(qm_rating), 2) AS avg_qm_rating,
                       ROUND(AVG(hprd_total), 2) AS avg_hprd,
                       ROUND(AVG(deficiency_count), 1) AS avg_deficiencies,
                       ROUND(SUM(fine_total_dollars), 0) AS total_fines,
                       ROUND(AVG(turnover_rn_pct), 1) AS avg_rn_turnover
                FROM fact_five_star
                WHERE chain_name IS NOT NULL AND chain_name != '' AND chain_name != 'N/A'
                GROUP BY chain_name
                HAVING COUNT(*) >= 5
                ORDER BY avg_overall_rating ASC
                LIMIT $1
            """, [limit]).fetchall()
            columns = [
                "chain_name", "facility_count", "avg_overall_rating",
                "avg_staffing_rating", "avg_qm_rating", "avg_hprd",
                "avg_deficiencies", "total_fines", "avg_rn_turnover",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Chain scoreboard query failed", "detail": str(e)})


@router.get("/api/research/nursing-ownership/state-breakdown")
async def nursing_state_breakdown():
    """Facility count, rating, staffing, and deficiencies by state and ownership type."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, ownership_type,
                       COUNT(*) AS facility_count,
                       ROUND(AVG(overall_rating), 2) AS avg_rating,
                       ROUND(AVG(hprd_total), 2) AS avg_hprd,
                       ROUND(AVG(deficiency_count), 1) AS avg_deficiencies
                FROM fact_five_star
                WHERE ownership_type IS NOT NULL
                GROUP BY state_code, ownership_type
                ORDER BY state_code, ownership_type
            """).fetchall()
            columns = [
                "state_code", "ownership_type", "facility_count",
                "avg_rating", "avg_hprd", "avg_deficiencies",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "State breakdown query failed", "detail": str(e)})
