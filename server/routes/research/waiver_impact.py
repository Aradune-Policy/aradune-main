"""Section 1115 Waiver Impact — waiver catalog, enrollment/spending/quality time series, and waiver vs non-waiver comparison."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/waiver-impact/catalog")
async def waiver_catalog(
    state: str = Query(None),
    status: str = Query(None),
    search: str = Query(None),
):
    """List Section 1115 waivers with optional state, status, and keyword filters."""
    try:
        with get_cursor() as cur:
            # Try ref_1115_waivers first, fall back to fact_kff_1115_waivers
            for table_name in ["ref_1115_waivers", "fact_kff_1115_waivers", "fact_section_1115_waivers"]:
                try:
                    cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                    break
                except Exception:
                    continue
            else:
                raise HTTPException(status_code=500, detail={"error": "No 1115 waiver table found"})

            # Build dynamic WHERE clauses
            conditions = []
            params = []
            param_idx = 1

            if state:
                conditions.append(f"state_code = ${param_idx}")
                params.append(state.upper())
                param_idx += 1

            if status:
                conditions.append(f"waiver_status ILIKE ${param_idx}")
                params.append(f"%{status}%")
                param_idx += 1

            if search:
                conditions.append(f"waiver_title ILIKE ${param_idx}")
                params.append(f"%{search}%")
                param_idx += 1

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Defensive column selection -- handle schema variations
            rows = cur.execute(f"""
                SELECT
                    COALESCE(state_code, '') AS state_code,
                    COALESCE(waiver_title, title, '') AS waiver_title,
                    COALESCE(waiver_type, type, '') AS waiver_type,
                    COALESCE(TRY_CAST(approval_date AS VARCHAR), '') AS approval_date,
                    COALESCE(TRY_CAST(effective_date AS VARCHAR), '') AS effective_date,
                    COALESCE(TRY_CAST(expiration_date AS VARCHAR), '') AS expiration_date,
                    COALESCE(waiver_status, status, '') AS waiver_status,
                    COALESCE(key_provisions, provisions, description, '') AS key_provisions
                FROM {table_name}
                {where_clause}
                ORDER BY approval_date DESC
            """, params).fetchall()
            columns = [
                "state_code", "waiver_title", "waiver_type",
                "approval_date", "effective_date", "expiration_date",
                "waiver_status", "key_provisions",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Waiver catalog query failed", "detail": str(e)})


@router.get("/api/research/waiver-impact/enrollment/{state_code}")
async def waiver_enrollment(state_code: str):
    """Monthly enrollment time series for a specific state."""
    try:
        sc = state_code.upper()
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT year, month,
                       total_enrollment,
                       COALESCE(chip_enrollment, 0) AS chip_enrollment,
                       COALESCE(ffs_enrollment, 0) AS ffs_enrollment,
                       COALESCE(mc_enrollment, 0) AS mc_enrollment
                FROM fact_enrollment
                WHERE state_code = $1
                ORDER BY year, month
            """, [sc]).fetchall()
            columns = [
                "year", "month", "total_enrollment",
                "chip_enrollment", "ffs_enrollment", "mc_enrollment",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Waiver enrollment query failed", "detail": str(e)})


@router.get("/api/research/waiver-impact/spending/{state_code}")
async def waiver_spending(state_code: str):
    """CMS-64 spending time series for a state by fiscal year."""
    try:
        sc = state_code.upper()
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT fiscal_year,
                       SUM(total_computable) AS total_spending,
                       SUM(federal_share) AS federal_share,
                       SUM(total_computable) - SUM(federal_share) AS state_share
                FROM fact_cms64_multiyear
                WHERE state_code = $1
                GROUP BY fiscal_year
                ORDER BY fiscal_year
            """, [sc]).fetchall()
            columns = ["fiscal_year", "total_spending", "federal_share", "state_share"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Waiver spending query failed", "detail": str(e)})


@router.get("/api/research/waiver-impact/quality/{state_code}")
async def waiver_quality(state_code: str):
    """Quality measures over time for a state from the combined Core Set (2017-2024)."""
    try:
        sc = state_code.upper()
        with get_cursor() as cur:
            # fact_quality_core_set_combined uses core_set_year and state_rate
            # Individual year tables use measure_rate. Try combined first.
            try:
                rows = cur.execute("""
                    SELECT core_set_year AS data_year, measure_id,
                           measure_name,
                           state_rate AS measure_rate
                    FROM fact_quality_core_set_combined
                    WHERE state_code = $1
                      AND state_rate IS NOT NULL
                    ORDER BY core_set_year, measure_id
                """, [sc]).fetchall()
            except Exception:
                # Fall back to 2024 single-year table
                rows = cur.execute("""
                    SELECT 2024 AS data_year, measure_id,
                           measure_name,
                           COALESCE(measure_rate, state_rate) AS measure_rate
                    FROM fact_quality_core_set_2024
                    WHERE state_code = $1
                      AND COALESCE(measure_rate, state_rate) IS NOT NULL
                    ORDER BY measure_id
                """, [sc]).fetchall()
            columns = ["data_year", "measure_id", "measure_name", "measure_rate"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Waiver quality query failed", "detail": str(e)})


@router.get("/api/research/waiver-impact/compare")
async def waiver_compare(waiver_type: str = Query(default="expansion")):
    """Compare waiver states vs non-waiver states for a given waiver type on enrollment and spending."""
    try:
        search_term = f"%{waiver_type}%"
        with get_cursor() as cur:
            # Determine which waiver table is available
            waiver_table = None
            for table_name in ["ref_1115_waivers", "fact_kff_1115_waivers", "fact_section_1115_waivers"]:
                try:
                    cur.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                    waiver_table = table_name
                    break
                except Exception:
                    continue

            if not waiver_table:
                raise HTTPException(status_code=500, detail={"error": "No 1115 waiver table found"})

            # Defensive: check for waiver_type and key_provisions columns
            rows = cur.execute(f"""
                WITH waiver_states AS (
                    SELECT DISTINCT state_code
                    FROM {waiver_table}
                    WHERE COALESCE(waiver_type, type, '') ILIKE $1
                       OR COALESCE(key_provisions, provisions, description, '') ILIKE $1
                ),
                enrollment_latest AS (
                    SELECT state_code,
                           MAX(total_enrollment) AS total_enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    GROUP BY state_code
                ),
                spending AS (
                    SELECT state_code,
                           SUM(total_computable) AS total_spending
                    FROM fact_cms64_multiyear
                    WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear)
                      AND state_code != 'US'
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       CASE WHEN w.state_code IS NOT NULL THEN 'Waiver' ELSE 'No Waiver' END AS waiver_group,
                       e.total_enrollment,
                       s.total_spending,
                       CASE WHEN e.total_enrollment > 0
                            THEN ROUND(s.total_spending / e.total_enrollment, 2)
                            ELSE NULL END AS spending_per_enrollee
                FROM dim_state d
                LEFT JOIN waiver_states w ON d.state_code = w.state_code
                LEFT JOIN enrollment_latest e ON d.state_code = e.state_code
                LEFT JOIN spending s ON d.state_code = s.state_code
                WHERE e.total_enrollment IS NOT NULL
                ORDER BY waiver_group, d.state_code
            """, [search_term]).fetchall()
            columns = [
                "state_code", "waiver_group", "total_enrollment",
                "total_spending", "spending_per_enrollee",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Waiver comparison query failed", "detail": str(e)})
