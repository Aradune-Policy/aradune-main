"""
Universal state context endpoint.
Returns cross-dataset signals for any state, used by all frontend modules.
Cached in-process for 1 hour per state.
"""

import time
from fastapi import APIRouter
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter(tags=["state-context"])

_cache: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 3600  # 1 hour


@router.get("/api/state-context/{state_code}")
@safe_route(default_response={})
async def state_context(state_code: str):
    """Cross-dataset context for a state. 10 independent queries, cached 1 hour."""
    sc = state_code.upper().strip()

    # Check cache
    now = time.time()
    if sc in _cache and now - _cache[sc][0] < _CACHE_TTL:
        return _cache[sc][1]

    result = {"state_code": sc, "state_name": None}

    with get_cursor() as cur:
        # 0. State name
        try:
            row = cur.execute("SELECT state_name FROM dim_state WHERE state_code = $1", [sc]).fetchone()
            if row:
                result["state_name"] = row[0]
        except: pass

        # 1. Fiscal: FMAP + methodology + conversion factor
        try:
            row = cur.execute("""
                SELECT fmap, methodology, conversion_factor
                FROM dim_state WHERE state_code = $1
            """, [sc]).fetchone()
            if row:
                result["fiscal"] = {
                    "fmap": float(row[0]) if row[0] else None,
                    "methodology": row[1],
                    "conversion_factor": str(row[2]) if row[2] else None,
                }
        except: pass

        # 1b. CMS-64 expenditure (latest FY)
        try:
            row = cur.execute("""
                SELECT fiscal_year, SUM(total_computable) AS total, SUM(federal_share) AS federal
                FROM fact_cms64_multiyear
                WHERE state_code = $1 AND LOWER(COALESCE(category, '')) IN ('total', 'medical assistance payments', '')
                GROUP BY fiscal_year
                ORDER BY fiscal_year DESC LIMIT 1
            """, [sc]).fetchone()
            if row and row[1]:
                if "fiscal" not in result:
                    result["fiscal"] = {}
                result["fiscal"]["cms64_total"] = float(row[1])
                result["fiscal"]["cms64_federal"] = float(row[2]) if row[2] else None
                result["fiscal"]["cms64_fy"] = row[0]
        except: pass

        # 2. Enrollment (latest month)
        try:
            row = cur.execute("""
                SELECT total_enrollment, mc_enrollment, ffs_enrollment, year, month
                FROM fact_enrollment
                WHERE state_code = $1
                ORDER BY year DESC, month DESC LIMIT 1
            """, [sc]).fetchone()
            if row and row[0]:
                mc_pct = round(float(row[1]) / float(row[0]) * 100, 1) if row[0] and row[1] else None
                result["enrollment"] = {
                    "total": int(row[0]), "managed_care": int(row[1]) if row[1] else None,
                    "ffs": int(row[2]) if row[2] else None,
                    "mc_pct": mc_pct, "year": row[3], "month": row[4],
                }
        except: pass

        # 3. Access: HPSA counts
        try:
            row = cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE LOWER(COALESCE(hpsa_discipline_class, discipline_type, '')) LIKE '%primary%') AS pc,
                    COUNT(*) FILTER (WHERE LOWER(COALESCE(hpsa_discipline_class, discipline_type, '')) LIKE '%dental%') AS dental,
                    COUNT(*) FILTER (WHERE LOWER(COALESCE(hpsa_discipline_class, discipline_type, '')) LIKE '%mental%') AS mh
                FROM fact_hpsa WHERE state_code = $1
            """, [sc]).fetchone()
            if row and row[0]:
                result["access"] = {
                    "hpsa_total": int(row[0]), "hpsa_primary_care": int(row[1]),
                    "hpsa_dental": int(row[2]), "hpsa_mental_health": int(row[3]),
                }
        except: pass

        # 4. Quality: Core Set measures below median
        try:
            row = cur.execute("""
                SELECT COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE state_rate < median_rate) AS below
                FROM fact_quality_core_set_2024
                WHERE state_code = $1 AND state_rate IS NOT NULL AND median_rate IS NOT NULL
            """, [sc]).fetchone()
            if row and row[0]:
                result["quality"] = {
                    "total_measures": int(row[0]), "below_median": int(row[1]),
                    "pct_below": round(int(row[1]) / max(int(row[0]), 1) * 100, 1),
                }
        except: pass

        # 5. Demographics
        try:
            row = cur.execute("""
                SELECT total_population, pct_poverty, pct_uninsured
                FROM fact_acs_state WHERE state_code = $1
                ORDER BY year DESC LIMIT 1
            """, [sc]).fetchone()
            if row:
                result["demographics"] = {
                    "population": int(row[0]) if row[0] else None,
                    "pct_poverty": float(row[1]) if row[1] else None,
                    "pct_uninsured": float(row[2]) if row[2] else None,
                }
        except: pass

        # 6. Rate adequacy (from fact_rate_comparison_v2)
        try:
            row = cur.execute("""
                SELECT
                    COUNT(*) AS total_codes,
                    ROUND(MEDIAN(pct_of_medicare), 1) AS median_pct,
                    COUNT(*) FILTER (WHERE pct_of_medicare < 60) AS below_60,
                    COUNT(*) FILTER (WHERE pct_of_medicare < 80) AS below_80,
                    MODE(rate_source) AS primary_source
                FROM fact_rate_comparison_v2
                WHERE state_code = $1 AND pct_of_medicare > 0 AND pct_of_medicare < 1000
            """, [sc]).fetchone()
            if row and row[0]:
                result["rate_adequacy"] = {
                    "median_pct_medicare": float(row[1]) if row[1] else None,
                    "codes_below_60": int(row[2]), "codes_below_80": int(row[3]),
                    "code_count": int(row[0]), "primary_rate_source": row[4],
                }
        except: pass

        # 7. Workforce: CNA, HHA, RN wages
        try:
            rows = cur.execute("""
                SELECT soc_code, hourly_median
                FROM fact_bls_wage
                WHERE state_code = $1 AND soc_code IN ('31-1131', '31-1121', '29-1141')
            """, [sc]).fetchall()
            if rows:
                wages = {r[0]: float(r[1]) if r[1] else None for r in rows}
                result["workforce"] = {
                    "cna_median_wage": wages.get("31-1131"),
                    "hha_median_wage": wages.get("31-1121"),
                    "rn_median_wage": wages.get("29-1141"),
                }
        except: pass

        # 8. HCBS waitlist
        try:
            row = cur.execute("""
                SELECT total_waiting, idd_waiting
                FROM fact_hcbs_waitlist WHERE state_code = $1
            """, [sc]).fetchone()
            if row and row[0]:
                result["hcbs_waitlist"] = {
                    "total_waiting": int(row[0]) if row[0] else None,
                    "idd_waiting": int(row[1]) if row[1] else None,
                }
        except: pass

        # 9. LTSS rebalancing
        try:
            row = cur.execute("""
                SELECT hcbs_pct, institutional_pct
                FROM fact_ltss_expenditure
                WHERE state_code = $1
                ORDER BY year DESC LIMIT 1
            """, [sc]).fetchone()
            if row:
                result["ltss"] = {
                    "hcbs_pct": float(row[0]) if row[0] else None,
                    "institutional_pct": float(row[1]) if row[1] else None,
                }
        except: pass

        # 10. T-MSIS claims-based effective rates
        try:
            row = cur.execute("""
                SELECT COUNT(*) AS codes,
                    ROUND(MEDIAN(pct_of_medicare), 1) AS median_pct,
                    ROUND(AVG(effective_paid_rate), 2) AS avg_rate
                FROM fact_tmsis_effective_rates
                WHERE state_code = $1 AND pct_of_medicare > 0 AND pct_of_medicare < 1000
            """, [sc]).fetchone()
            if row and row[0] and int(row[0]) > 0:
                result["tmsis_claims"] = {
                    "total_codes": int(row[0]),
                    "median_pct_medicare": float(row[1]) if row[1] else None,
                    "avg_paid_rate": float(row[2]) if row[2] else None,
                    "caveat": "Claims-based: reflects actual paid amounts, not fee schedule maximums.",
                }
        except: pass

        # 11. Supplemental payments (DSH + SDP)
        try:
            row = cur.execute("""
                SELECT SUM(dsh_allotment) FROM fact_dsh_hospital WHERE state_code = $1
            """, [sc]).fetchone()
            if row and row[0]:
                result["supplemental"] = {"dsh_total": float(row[0])}
        except: pass

        try:
            row = cur.execute("""
                SELECT COUNT(*), SUM(total_expenditure)
                FROM fact_sdp_preprint WHERE state_code = $1
            """, [sc]).fetchone()
            if row and row[0] and int(row[0]) > 0:
                if "supplemental" not in result:
                    result["supplemental"] = {}
                result["supplemental"]["sdp_count"] = int(row[0])
                result["supplemental"]["sdp_total"] = float(row[1]) if row[1] else None
        except: pass

    # Cache result
    _cache[sc] = (now, result)
    return result
