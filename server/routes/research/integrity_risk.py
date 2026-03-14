"""Program Integrity Risk Index — composite risk scoring, open payments, enforcement, and improper payment rates."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/research/integrity-risk/composite")
async def integrity_risk_composite(state: str = Query(None)):
    """Composite integrity risk: open payments per enrollee, exclusions per 100K, by state."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE d.state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                WITH open_pay AS (
                    SELECT state_code,
                           SUM(total_amount) AS total_open_payments,
                           SUM(payment_count) AS payment_count
                    FROM fact_open_payments
                    GROUP BY state_code
                ),
                exclusions AS (
                    SELECT state_code,
                           COUNT(*) AS exclusion_count
                    FROM fact_leie
                    WHERE reinstatement_date IS NULL
                    GROUP BY state_code
                ),
                enrollment AS (
                    SELECT state_code, MAX(total_enrollment) AS total_enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    GROUP BY state_code
                )
                SELECT d.state_code,
                       COALESCE(op.total_open_payments, 0) AS total_open_payments,
                       COALESCE(op.payment_count, 0) AS payment_count,
                       COALESCE(ex.exclusion_count, 0) AS exclusion_count,
                       e.total_enrollment,
                       CASE WHEN COALESCE(e.total_enrollment, 0) > 0
                            THEN ROUND(COALESCE(op.total_open_payments, 0) / e.total_enrollment, 2)
                            ELSE 0 END AS open_payments_per_enrollee,
                       CASE WHEN COALESCE(e.total_enrollment, 0) > 0
                            THEN ROUND(COALESCE(ex.exclusion_count, 0) * 100000.0 / e.total_enrollment, 1)
                            ELSE 0 END AS exclusions_per_100k
                FROM dim_state d
                LEFT JOIN open_pay op ON d.state_code = op.state_code
                LEFT JOIN exclusions ex ON d.state_code = ex.state_code
                LEFT JOIN enrollment e ON d.state_code = e.state_code
                {state_filter}
                ORDER BY open_payments_per_enrollee DESC
            """, params).fetchall()
            columns = [
                "state_code", "total_open_payments", "payment_count",
                "exclusion_count", "total_enrollment",
                "open_payments_per_enrollee", "exclusions_per_100k",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Integrity risk composite query failed", "detail": str(e)})


@router.get("/api/research/integrity-risk/open-payments")
async def integrity_open_payments(state: str = Query(None)):
    """Open Payments summary by state: total amount, physician count, avg per physician."""
    try:
        with get_cursor() as cur:
            params = []
            state_filter = ""
            if state:
                state_filter = "WHERE state_code = $1"
                params.append(state.upper())
            rows = cur.execute(f"""
                SELECT state_code,
                       SUM(total_amount) AS total_amount,
                       SUM(payment_count) AS payment_count,
                       SUM(unique_physicians) AS unique_physicians,
                       SUM(unique_companies) AS unique_companies,
                       ROUND(SUM(total_amount) / NULLIF(SUM(unique_physicians), 0), 2) AS avg_per_physician
                FROM fact_open_payments
                {state_filter}
                GROUP BY state_code
                ORDER BY total_amount DESC
            """, params).fetchall()
            columns = [
                "state_code", "total_amount", "payment_count",
                "unique_physicians", "unique_companies", "avg_per_physician",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "Open payments query failed", "detail": str(e)})


@router.get("/api/research/integrity-risk/enforcement")
async def integrity_enforcement(state: str = Query(None), fiscal_year: int = Query(None)):
    """MFCU enforcement stats: cases opened, convictions, recoveries, ROI."""
    try:
        with get_cursor() as cur:
            params = []
            conditions = []
            if state:
                params.append(state.upper())
                conditions.append(f"state_code = ${len(params)}")
            if fiscal_year:
                params.append(fiscal_year)
                conditions.append(f"fiscal_year = ${len(params)}")
            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Try the expected schema first
            try:
                rows = cur.execute(f"""
                    SELECT state_code, fiscal_year,
                           COALESCE(cases_opened, 0) AS cases_opened,
                           COALESCE(convictions, 0) AS convictions,
                           COALESCE(civil_settlements, 0) AS civil_settlements,
                           COALESCE(recoveries_total, 0) AS recoveries_total,
                           COALESCE(program_expenditures, 0) AS program_expenditures,
                           ROUND(COALESCE(recoveries_total, 0) / NULLIF(COALESCE(program_expenditures, 0), 0), 2) AS roi
                    FROM fact_mfcu_stats
                    {where_clause}
                    ORDER BY fiscal_year DESC, state_code
                """, params).fetchall()
                columns = [
                    "state_code", "fiscal_year", "cases_opened", "convictions",
                    "civil_settlements", "recoveries_total", "program_expenditures", "roi",
                ]
            except Exception:
                # Fallback: discover schema and return whatever columns exist
                schema_rows = cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'fact_mfcu_stats'").fetchall()
                available_cols = [r[0] for r in schema_rows]
                select_cols = ", ".join(available_cols)
                rows = cur.execute(f"""
                    SELECT {select_cols}
                    FROM fact_mfcu_stats
                    {where_clause}
                    ORDER BY 1, 2
                """, params).fetchall()
                columns = available_cols

            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "MFCU enforcement query failed", "detail": str(e)})


@router.get("/api/research/integrity-risk/perm")
async def integrity_perm(fiscal_year: int = Query(None)):
    """PERM improper payment rates by fiscal year."""
    try:
        with get_cursor() as cur:
            params = []
            where_clause = ""
            if fiscal_year:
                where_clause = "WHERE fiscal_year = $1"
                params.append(fiscal_year)

            try:
                rows = cur.execute(f"""
                    SELECT fiscal_year,
                           COALESCE(improper_payment_rate_pct, 0) AS improper_payment_rate_pct,
                           COALESCE(ffs_rate_pct, 0) AS ffs_rate_pct,
                           COALESCE(managed_care_rate_pct, 0) AS managed_care_rate_pct,
                           COALESCE(eligibility_error_rate_pct, 0) AS eligibility_error_rate_pct
                    FROM fact_perm_rates
                    {where_clause}
                    ORDER BY fiscal_year DESC
                """, params).fetchall()
                columns = [
                    "fiscal_year", "improper_payment_rate_pct", "ffs_rate_pct",
                    "managed_care_rate_pct", "eligibility_error_rate_pct",
                ]
            except Exception:
                # Fallback: discover schema
                schema_rows = cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'fact_perm_rates'").fetchall()
                available_cols = [r[0] for r in schema_rows]
                select_cols = ", ".join(available_cols)
                rows = cur.execute(f"""
                    SELECT {select_cols}
                    FROM fact_perm_rates
                    {where_clause}
                    ORDER BY 1 DESC
                """, params).fetchall()
                columns = available_cols

            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "PERM rates query failed", "detail": str(e)})
