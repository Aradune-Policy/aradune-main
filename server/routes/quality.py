"""Quality and facility data routes — Five-Star, HAC, Provider of Services."""

from fastapi import APIRouter, HTTPException, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/five-star/summary")
@safe_route(default_response=[])
async def five_star_summary():
    """State-level summary of nursing home ratings."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code,
                   COUNT(*) AS facility_count,
                   ROUND(AVG(overall_rating), 2) AS avg_overall,
                   ROUND(AVG(health_inspection_rating), 2) AS avg_health,
                   ROUND(AVG(staffing_rating), 2) AS avg_staffing,
                   ROUND(AVG(qm_rating), 2) AS avg_qm,
                   ROUND(AVG(hprd_total), 2) AS avg_hprd,
                   ROUND(AVG(hprd_rn), 2) AS avg_hprd_rn,
                   ROUND(AVG(turnover_total_pct), 1) AS avg_turnover,
                   SUM(certified_beds) AS total_beds,
                   SUM(CASE WHEN overall_rating = 1 THEN 1 ELSE 0 END) AS one_star,
                   SUM(CASE WHEN overall_rating = 5 THEN 1 ELSE 0 END) AS five_star,
                   SUM(CASE WHEN abuse_flag THEN 1 ELSE 0 END) AS abuse_count,
                   SUM(fine_total_dollars) AS total_fines
            FROM fact_five_star
            WHERE overall_rating > 0
            GROUP BY state_code
            ORDER BY state_code
        """).fetchall()
        columns = [
            "state_code", "facility_count", "avg_overall", "avg_health",
            "avg_staffing", "avg_qm", "avg_hprd", "avg_hprd_rn",
            "avg_turnover", "total_beds", "one_star", "five_star",
            "abuse_count", "total_fines",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/five-star/{state_code}")
@safe_route(default_response=[])
async def five_star_by_state(state_code: str, min_rating: int = Query(None)):
    """Get nursing home Five-Star ratings for a state."""
    state_code = state_code.upper()
    rating_filter = ""
    params = [state_code]
    if min_rating:
        rating_filter = "AND overall_rating >= $2"
        params.append(min_rating)

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT provider_ccn, facility_name, city, county,
                   certified_beds, avg_residents_per_day, ownership_type,
                   overall_rating, health_inspection_rating, qm_rating,
                   staffing_rating, hprd_total, hprd_rn, hprd_cna,
                   turnover_total_pct, turnover_rn_pct,
                   deficiency_count, fine_count, fine_total_dollars,
                   total_penalties, abuse_flag, special_focus_status,
                   chain_name, chain_size
            FROM fact_five_star
            WHERE state_code = $1 {rating_filter}
            ORDER BY overall_rating DESC, facility_name
        """, params).fetchall()
        columns = [
            "provider_ccn", "facility_name", "city", "county",
            "certified_beds", "avg_residents_per_day", "ownership_type",
            "overall_rating", "health_inspection_rating", "qm_rating",
            "staffing_rating", "hprd_total", "hprd_rn", "hprd_cna",
            "turnover_total_pct", "turnover_rn_pct",
            "deficiency_count", "fine_count", "fine_total_dollars",
            "total_penalties", "abuse_flag", "special_focus_status",
            "chain_name", "chain_size",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hac/{state_code}")
@safe_route(default_response=[])
async def hac_by_state(state_code: str):
    """Get HAC measure rates for hospitals in a state (joined via POS)."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT h.provider_id, p.facility_name,
                   h.measure_name, h.rate,
                   h.start_quarter, h.end_quarter
            FROM fact_hac_measure h
            LEFT JOIN fact_pos_hospital p ON h.provider_id = p.provider_id
            WHERE p.state_code = $1
            ORDER BY h.provider_id, h.measure_name
        """, [state_code]).fetchall()
        columns = ["provider_id", "facility_name", "measure_name", "rate",
                    "start_quarter", "end_quarter"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/pos/hospitals/{state_code}")
@safe_route(default_response=[])
async def pos_hospitals(state_code: str):
    """Get hospital characteristics from POS for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_id, facility_name, city, zip_code,
                   urban_rural, control_type, facility_type,
                   total_beds, certified_beds, psych_beds, rehab_beds,
                   operating_rooms, rn_count, lpn_count, total_employees,
                   has_icu, has_ob, has_er_psych,
                   medical_school_affiliation, teaching_status
            FROM fact_pos_hospital
            WHERE state_code = $1
            ORDER BY facility_name
        """, [state_code]).fetchall()
        columns = [
            "provider_id", "facility_name", "city", "zip_code",
            "urban_rural", "control_type", "facility_type",
            "total_beds", "certified_beds", "psych_beds", "rehab_beds",
            "operating_rooms", "rn_count", "lpn_count", "total_employees",
            "has_icu", "has_ob", "has_er_psych",
            "medical_school_affiliation", "teaching_status",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/pos/providers/{state_code}")
@safe_route(default_response=[])
async def pos_other_providers(state_code: str, provider_type: str = Query(None)):
    """Get non-hospital provider characteristics from POS/iQIES for a state."""
    state_code = state_code.upper()
    type_filter = ""
    params = [state_code]
    if provider_type:
        type_filter = "AND provider_type_name = $2"
        params.append(provider_type)

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT provider_id, facility_name, city, zip_code,
                   urban_rural, provider_type_name, control_type,
                   bed_count, employee_count
            FROM fact_pos_other
            WHERE state_code = $1 {type_filter}
            ORDER BY provider_type_name, facility_name
        """, params).fetchall()
        columns = [
            "provider_id", "facility_name", "city", "zip_code",
            "urban_rural", "provider_type_name", "control_type",
            "bed_count", "employee_count",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hospital-ratings/{state_code}")
@safe_route(default_response=[])
async def hospital_ratings(state_code: str):
    """Get Care Compare hospital overall ratings for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_id, facility_name, city, county, zip_code,
                   hospital_type, ownership_type, overall_rating,
                   has_emergency, birthing_friendly,
                   mort_better, mort_same, mort_worse,
                   safety_better, safety_same, safety_worse,
                   readm_better, readm_same, readm_worse
            FROM fact_hospital_rating
            WHERE state_code = $1
            ORDER BY overall_rating DESC, facility_name
        """, [state_code]).fetchall()
        columns = [
            "provider_id", "facility_name", "city", "county", "zip_code",
            "hospital_type", "ownership_type", "overall_rating",
            "has_emergency", "birthing_friendly",
            "mort_better", "mort_same", "mort_worse",
            "safety_better", "safety_same", "safety_worse",
            "readm_better", "readm_same", "readm_worse",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/vbp/{state_code}")
@safe_route(default_response=[])
async def vbp_scores(state_code: str):
    """Get Hospital Value-Based Purchasing scores for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_id, facility_name,
                   fiscal_year, total_performance_score,
                   clinical_outcomes_score, engagement_score,
                   safety_score, efficiency_score
            FROM fact_hospital_vbp
            WHERE state_code = $1
            ORDER BY total_performance_score DESC
        """, [state_code]).fetchall()
        columns = [
            "provider_id", "facility_name",
            "fiscal_year", "total_performance_score",
            "clinical_outcomes_score", "engagement_score",
            "safety_score", "efficiency_score",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hrrp/{state_code}")
@safe_route(default_response=[])
async def hrrp_by_state(state_code: str):
    """Get Hospital Readmissions Reduction Program data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT provider_id, facility_name, measure_name,
                   discharges, excess_readmission_ratio,
                   predicted_rate, expected_rate, readmissions
            FROM fact_hospital_hrrp
            WHERE state_code = $1
            ORDER BY provider_id, measure_name
        """, [state_code]).fetchall()
        columns = [
            "provider_id", "facility_name", "measure_name",
            "discharges", "excess_readmission_ratio",
            "predicted_rate", "expected_rate", "readmissions",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/epsdt")
@safe_route(default_response=[])
async def epsdt_all():
    """Get EPSDT (CMS-416) participation data for all states."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, fiscal_year,
                   eligible_total, total_screens_received,
                   screening_ratio, participant_ratio,
                   receiving_any_dental, dental_pct,
                   referred_corrective_treatment,
                   enrolled_managed_care, blood_lead_tests
            FROM fact_epsdt
            ORDER BY state_code
        """).fetchall()
        columns = [
            "state_code", "fiscal_year",
            "eligible_total", "total_screens_received",
            "screening_ratio", "participant_ratio",
            "receiving_any_dental", "dental_pct",
            "referred_corrective_treatment",
            "enrolled_managed_care", "blood_lead_tests",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hpsa/summary")
@safe_route(default_response=[])
async def hpsa_summary():
    """Get HPSA summary statistics by state and discipline."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, discipline,
                   COUNT(DISTINCT hpsa_id) AS total_hpsas,
                   AVG(hpsa_score) AS avg_score,
                   SUM(designation_population) AS total_pop,
                   SUM(shortage) AS total_shortage,
                   AVG(pct_poverty) AS avg_pct_poverty
            FROM fact_hpsa
            GROUP BY state_code, discipline
            ORDER BY state_code, discipline
        """).fetchall()
        columns = [
            "state_code", "discipline", "total_hpsas",
            "avg_score", "total_pop", "total_shortage", "avg_pct_poverty",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/hpsa/{state_code}")
@safe_route(default_response=[])
async def hpsa_by_state(state_code: str, discipline: str = Query(None)):
    """Get HPSA designations for a state."""
    state_code = state_code.upper()
    disc_filter = ""
    params = [state_code]
    if discipline:
        disc_filter = "AND discipline = $2"
        params.append(discipline)

    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT hpsa_name, hpsa_id, discipline, designation_type,
                       hpsa_score, hpsa_status, metro_indicator,
                       degree_of_shortage, hpsa_fte, designation_population,
                       pct_poverty, population_type, rural_status,
                       county_name, provider_type
                FROM fact_hpsa
                WHERE state_code = $1 {disc_filter}
                QUALIFY ROW_NUMBER() OVER (PARTITION BY hpsa_id ORDER BY hpsa_score DESC NULLS LAST) = 1
                ORDER BY hpsa_score DESC NULLS LAST
            """, params).fetchall()
            columns = [
                "hpsa_name", "hpsa_id", "discipline", "designation_type",
                "hpsa_score", "hpsa_status", "metro_indicator",
                "degree_of_shortage", "hpsa_fte", "designation_population",
                "pct_poverty", "population_type", "rural_status",
                "county_name", "provider_type",
            ]
            return [dict(zip(columns, r)) for r in rows]
    except Exception as e:
        raise HTTPException(500, {"error": "HPSA query failed", "detail": str(e)})


# ── Medically Underserved Areas ──────────────────────────────────────────

@router.get("/api/mua/summary")
@safe_route(default_response=[])
async def mua_summary():
    """State-level MUA/MUP designation summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT "State Abbreviation" AS state_code,
                   COUNT(*) AS designations,
                   COUNT(DISTINCT "County Equivalent Name") AS counties
            FROM fact_mua_designation
            GROUP BY "State Abbreviation"
            ORDER BY "State Abbreviation"
        """).fetchall()
        columns = ["state_code", "designations", "counties"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/mua/{state_code}")
@safe_route(default_response=[])
async def mua_by_state(state_code: str):
    """MUA/MUP designations for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT "MUA/P ID" AS mua_id,
                   "MUA/P Service Area Name" AS service_area,
                   "Designation Type" AS designation_type,
                   "MUA/P Status Description" AS status,
                   "Designation Date" AS designation_date,
                   "IMU Score" AS imu_score,
                   "Population Type" AS population_type,
                   "County Equivalent Name" AS county,
                   "Rural Status Description" AS rural_status,
                   "Percent of Population with Incomes at or Below 100 Percent of the U.S. Federal Poverty Level" AS pct_poverty,
                   "Percentage of Population Age 65 and Over" AS pct_age_65_over,
                   "Infant Mortality Rate" AS infant_mortality_rate,
                   "Designation Population in a Medically Underserved Area/Population (MUA/P)" AS designation_pop,
                   "Providers per 1000 Population" AS providers_per_1000
            FROM fact_mua_designation
            WHERE "State Abbreviation" = $1
            ORDER BY "County Equivalent Name"
        """, [state_code]).fetchall()
        columns = [
            "mua_id", "service_area", "designation_type", "status",
            "designation_date", "imu_score", "population_type", "county",
            "rural_status", "pct_poverty", "pct_age_65_over",
            "infant_mortality_rate", "designation_pop", "providers_per_1000",
        ]
        return [dict(zip(columns, r)) for r in rows]
