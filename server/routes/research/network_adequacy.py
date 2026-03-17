"""Network Adequacy — comprehensive access designation scoring by state."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/network-adequacy/composite")
async def network_adequacy_composite():
    """State-level network adequacy composite: primary care + dental + MH HPSAs, MUA/MUP, FQHCs, enrollment."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH pc_hpsa AS (
                    SELECT state_code,
                           COUNT(DISTINCT hpsa_id) AS pc_hpsa_count,
                           ROUND(AVG(hpsa_score), 1) AS pc_avg_score
                    FROM fact_hpsa
                    WHERE discipline = 'Primary Care'
                    GROUP BY state_code
                ),
                dental AS (
                    SELECT "Primary State Abbreviation" AS state_code,
                           COUNT(DISTINCT "HPSA ID") AS dental_hpsa_count,
                           ROUND(AVG("HPSA Score"), 1) AS dental_avg_score
                    FROM fact_dental_hpsa
                    WHERE "HPSA Status" = 'Designated'
                    GROUP BY "Primary State Abbreviation"
                ),
                mh AS (
                    SELECT "Primary State Abbreviation" AS state_code,
                           COUNT(DISTINCT "HPSA ID") AS mh_hpsa_count,
                           ROUND(AVG("HPSA Score"), 1) AS mh_avg_score
                    FROM fact_mental_health_hpsa
                    WHERE "HPSA Status" = 'Designated'
                    GROUP BY "Primary State Abbreviation"
                ),
                mua AS (
                    SELECT "State Abbreviation" AS state_code,
                           COUNT(DISTINCT "MUA/P ID") AS mua_count
                    FROM fact_mua_mup
                    WHERE "MUA/P Status Description" = 'Designated'
                    GROUP BY "State Abbreviation"
                ),
                enrollment AS (
                    SELECT state_code, MAX(total_enrollment) AS enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       COALESCE(pc.pc_hpsa_count, 0) AS pc_hpsa_count,
                       COALESCE(pc.pc_avg_score, 0) AS pc_avg_score,
                       COALESCE(dn.dental_hpsa_count, 0) AS dental_hpsa_count,
                       COALESCE(dn.dental_avg_score, 0) AS dental_avg_score,
                       COALESCE(mh.mh_hpsa_count, 0) AS mh_hpsa_count,
                       COALESCE(mh.mh_avg_score, 0) AS mh_avg_score,
                       COALESCE(mu.mua_count, 0) AS mua_count,
                       COALESCE(e.enrollment, 0) AS enrollment,
                       CASE WHEN COALESCE(e.enrollment, 0) > 0
                            THEN ROUND((COALESCE(pc.pc_hpsa_count,0) + COALESCE(dn.dental_hpsa_count,0) + COALESCE(mh.mh_hpsa_count,0)) * 100000.0
                                       / NULLIF(e.enrollment, 0), 1)
                            ELSE 0 END AS shortage_per_100k_enrollees
                FROM dim_state d
                LEFT JOIN pc_hpsa pc ON d.state_code = pc.state_code
                LEFT JOIN dental dn ON d.state_code = dn.state_code
                LEFT JOIN mh ON d.state_code = mh.state_code
                LEFT JOIN mua mu ON d.state_code = mu.state_code
                LEFT JOIN enrollment e ON d.state_code = e.state_code
                ORDER BY shortage_per_100k_enrollees DESC
            """).fetchall()
            columns = [
                "state_code", "pc_hpsa_count", "pc_avg_score",
                "dental_hpsa_count", "dental_avg_score",
                "mh_hpsa_count", "mh_avg_score",
                "mua_count", "enrollment", "shortage_per_100k_enrollees",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"network adequacy composite failed: {exc}")


@router.get("/api/research/network-adequacy/fqhc-coverage")
async def fqhc_coverage():
    """FQHC site coverage relative to shortage areas by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH fqhc AS (
                    SELECT "Site State Abbreviation" AS state_code,
                           COUNT(*) AS fqhc_sites
                    FROM fact_fqhc_sites_v2
                    GROUP BY "Site State Abbreviation"
                ),
                hpsa_total AS (
                    SELECT state_code,
                           COUNT(DISTINCT hpsa_id) AS total_hpsas
                    FROM fact_hpsa
                    GROUP BY state_code
                ),
                enrollment AS (
                    SELECT state_code, MAX(total_enrollment) AS enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       COALESCE(f.fqhc_sites, 0) AS fqhc_sites,
                       COALESCE(h.total_hpsas, 0) AS total_hpsas,
                       COALESCE(e.enrollment, 0) AS enrollment,
                       CASE WHEN COALESCE(e.enrollment, 0) > 0
                            THEN ROUND(COALESCE(f.fqhc_sites, 0) * 100000.0 / NULLIF(e.enrollment, 0), 1)
                            ELSE 0 END AS fqhc_per_100k,
                       CASE WHEN COALESCE(h.total_hpsas, 0) > 0
                            THEN ROUND(COALESCE(f.fqhc_sites, 0) * 100.0 / NULLIF(h.total_hpsas, 0), 1)
                            ELSE 0 END AS fqhc_to_hpsa_ratio
                FROM dim_state d
                LEFT JOIN fqhc f ON d.state_code = f.state_code
                LEFT JOIN hpsa_total h ON d.state_code = h.state_code
                LEFT JOIN enrollment e ON d.state_code = e.state_code
                WHERE COALESCE(f.fqhc_sites, 0) > 0 OR COALESCE(h.total_hpsas, 0) > 0
                ORDER BY fqhc_per_100k DESC
                LIMIT 100
            """).fetchall()
            columns = ["state_code", "fqhc_sites", "total_hpsas", "enrollment", "fqhc_per_100k", "fqhc_to_hpsa_ratio"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"fqhc coverage failed: {exc}")
