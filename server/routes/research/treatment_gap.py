"""Opioid Treatment Gap — demand vs supply, MAT utilization, prescribing rates, and funding analysis."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/research/treatment-gap/demand-supply")
@safe_route(default_response={})
async def treatment_gap_demand_supply():
    """OUD prevalence vs SUD treatment facility capacity by state."""
    try:
        with get_cursor() as cur:
            # Try opioid_use_disorder first, fall back to broader substance measures
            prevalence_sql = None
            # AUDIT FIX: opioid_use_disorder doesn't exist; actual measure is oud_past_year
            for measure in ("oud_past_year", "opioid_misuse_past_year", "sud_past_year", "illicit_drug_use_past_month"):
                check = cur.execute(
                    "SELECT COUNT(*) FROM fact_nsduh_prevalence WHERE measure_id = $1 AND age_group = '18+'",
                    [measure],
                ).fetchone()
                if check and check[0] > 0:
                    prevalence_sql = measure
                    break

            if prevalence_sql is None:
                # Broad fallback: any measure containing opioid or substance
                check = cur.execute("""
                    SELECT measure_id FROM fact_nsduh_prevalence
                    WHERE (measure_id ILIKE '%opioid%' OR measure_id ILIKE '%substance%')
                      AND age_group = '18+'
                    LIMIT 1
                """).fetchone()
                prevalence_sql = check[0] if check else "opioid_use_disorder"

            # Build facility CTE defensively -- offers_su/offers_detox may vary
            try:
                rows = cur.execute("""
                    WITH prevalence AS (
                        SELECT state_code, estimate_pct AS oud_prevalence_pct
                        FROM fact_nsduh_prevalence
                        WHERE measure_id = $1
                          AND age_group = '18+'
                    ),
                    facilities AS (
                        SELECT state_code,
                               COUNT(*) AS sud_facility_count,
                               COUNT(*) FILTER (WHERE offers_detox) AS detox_facilities,
                               COALESCE(SUM(residential_beds), 0) AS residential_beds
                        FROM fact_mh_facility
                        WHERE offers_su = true
                        GROUP BY state_code
                    ),
                    enrollment AS (
                        SELECT state_code, MAX(total_enrollment) AS total_enrollment
                        FROM fact_enrollment
                        WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                        GROUP BY state_code
                    )
                    SELECT p.state_code, p.oud_prevalence_pct,
                           COALESCE(f.sud_facility_count, 0) AS sud_facility_count,
                           COALESCE(f.detox_facilities, 0) AS detox_facilities,
                           COALESCE(f.residential_beds, 0) AS residential_beds,
                           e.total_enrollment,
                           CASE WHEN e.total_enrollment > 0
                                THEN ROUND(COALESCE(f.sud_facility_count, 0) * 100000.0 / e.total_enrollment, 1)
                                ELSE 0 END AS facilities_per_100k
                    FROM prevalence p
                    LEFT JOIN facilities f ON p.state_code = f.state_code
                    LEFT JOIN enrollment e ON p.state_code = e.state_code
                    ORDER BY p.oud_prevalence_pct DESC
                """, [prevalence_sql]).fetchall()
            except Exception:
                # Fallback: offers_su or offers_detox columns may not exist; use simpler filter
                rows = cur.execute("""
                    WITH prevalence AS (
                        SELECT state_code, estimate_pct AS oud_prevalence_pct
                        FROM fact_nsduh_prevalence
                        WHERE measure_id = $1
                          AND age_group = '18+'
                    ),
                    facilities AS (
                        SELECT state_code,
                               COUNT(*) AS sud_facility_count,
                               0 AS detox_facilities,
                               0 AS residential_beds
                        FROM fact_mh_facility
                        GROUP BY state_code
                    ),
                    enrollment AS (
                        SELECT state_code, MAX(total_enrollment) AS total_enrollment
                        FROM fact_enrollment
                        WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                        GROUP BY state_code
                    )
                    SELECT p.state_code, p.oud_prevalence_pct,
                           COALESCE(f.sud_facility_count, 0) AS sud_facility_count,
                           COALESCE(f.detox_facilities, 0) AS detox_facilities,
                           COALESCE(f.residential_beds, 0) AS residential_beds,
                           e.total_enrollment,
                           CASE WHEN e.total_enrollment > 0
                                THEN ROUND(COALESCE(f.sud_facility_count, 0) * 100000.0 / e.total_enrollment, 1)
                                ELSE 0 END AS facilities_per_100k
                    FROM prevalence p
                    LEFT JOIN facilities f ON p.state_code = f.state_code
                    LEFT JOIN enrollment e ON p.state_code = e.state_code
                    ORDER BY p.oud_prevalence_pct DESC
                """, [prevalence_sql]).fetchall()

            columns = [
                "state_code", "oud_prevalence_pct", "sud_facility_count",
                "detox_facilities", "residential_beds", "total_enrollment",
                "facilities_per_100k",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"treatment-gap demand-supply failed: {exc}")


@router.get("/api/research/treatment-gap/mat-utilization")
@safe_route(default_response={})
async def treatment_gap_mat_utilization():
    """Medication-Assisted Treatment (MAT) drug spending and prescriptions from SDUD by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code,
                       SUM(total_amount_reimbursed) AS mat_total_spending,
                       SUM(number_of_prescriptions) AS mat_prescriptions,
                       SUM(units_reimbursed) AS mat_units
                FROM fact_sdud_2025
                WHERE state_code != 'XX'
                  AND (product_name ILIKE '%buprenorph%'
                       OR product_name ILIKE '%bupren&nal%'
                       OR product_name ILIKE '%suboxone%'
                       OR product_name ILIKE '%naloxone%'
                       OR product_name ILIKE '%naltrexone%'
                       OR product_name ILIKE '%vivitrol%'
                       OR product_name ILIKE '%sublocade%'
                       OR product_name ILIKE '%zubsolv%')
                GROUP BY state_code
                ORDER BY mat_total_spending DESC
            """).fetchall()
            columns = ["state_code", "mat_total_spending", "mat_prescriptions", "mat_units"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"treatment-gap mat-utilization failed: {exc}")


@router.get("/api/research/treatment-gap/prescribing")
@safe_route(default_response={})
async def treatment_gap_prescribing():
    """Opioid prescribing rates by state and year, with FIPS-to-state resolution."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT COALESCE(d.state_code, o.geo_code) AS state_code,
                       o.year, o.opioid_prescribing_rate,
                       o.opioid_claims, o.total_claims
                FROM fact_opioid_prescribing o
                LEFT JOIN dim_state d ON d.state_name = o.geo_desc
                WHERE o.geo_level = 'State'
                ORDER BY o.year DESC, o.opioid_prescribing_rate DESC
                LIMIT 1000
            """).fetchall()
            columns = ["state_code", "year", "opioid_prescribing_rate", "opioid_claims", "total_claims"]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"treatment-gap prescribing failed: {exc}")


@router.get("/api/research/treatment-gap/funding")
@safe_route(default_response={})
async def treatment_gap_funding():
    """SUD prevalence vs block grant funding and per-enrollee grant amounts by state."""
    try:
        with get_cursor() as cur:
            rows = cur.execute("""
                WITH prevalence AS (
                    SELECT state_code, estimate_pct
                    FROM fact_nsduh_prevalence
                    WHERE measure_id IN ('any_substance_use_disorder', 'illicit_drug_use_past_month')
                      AND age_group = '18+'
                ),
                grants AS (
                    SELECT state_code,
                           SUM(allotment) AS total_block_grant
                    FROM fact_block_grant
                    GROUP BY state_code
                ),
                enrollment AS (
                    SELECT state_code, MAX(total_enrollment) AS total_enrollment
                    FROM fact_enrollment
                    WHERE year = (SELECT MAX(year) FROM fact_enrollment)
                    GROUP BY state_code
                )
                SELECT COALESCE(p.state_code, g.state_code) AS state_code,
                       p.estimate_pct AS prevalence_pct,
                       COALESCE(g.total_block_grant, 0) AS total_block_grant,
                       e.total_enrollment,
                       CASE WHEN COALESCE(e.total_enrollment, 0) > 0
                            THEN ROUND(COALESCE(g.total_block_grant, 0) / NULLIF(e.total_enrollment, 0), 2)
                            ELSE 0 END AS grant_per_enrollee
                FROM prevalence p
                LEFT JOIN grants g ON p.state_code = g.state_code
                LEFT JOIN enrollment e ON p.state_code = e.state_code
                ORDER BY p.estimate_pct DESC
            """).fetchall()
            columns = [
                "state_code", "prevalence_pct", "total_block_grant",
                "total_enrollment", "grant_per_enrollee",
            ]
            return {"rows": [dict(zip(columns, r)) for r in rows], "count": len(rows)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"treatment-gap funding failed: {exc}")
