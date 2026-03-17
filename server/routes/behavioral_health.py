"""Behavioral health endpoints — NSDUH, MH facilities, IPF quality, BRFSS, block grants."""
from fastapi import APIRouter, Query
from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/behavioral-health/nsduh")
@safe_route(default_response={"rows": [], "count": 0})
def nsduh_prevalence(
    state: str = Query(default=None),
    measure: str = Query(default=None),
    age_group: str = Query(default="18+"),
):
    """SAMHSA NSDUH state-level MH/SUD prevalence estimates."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, measure_id, measure_name, age_group,
                   estimate_pct, ci_lower_pct, ci_upper_pct, survey_years
            FROM fact_nsduh_prevalence
            WHERE age_group = ?
        """
        params: list = [age_group]
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_id = ?"
            params.append(measure)
        sql += " ORDER BY state_code, measure_id"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "measure_id", "measure_name", "age_group",
                "estimate_pct", "ci_lower_pct", "ci_upper_pct", "survey_years"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/nsduh/measures")
@safe_route(default_response={"measures": []})
def nsduh_measures():
    """List available NSDUH measures."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT DISTINCT measure_id, measure_name
            FROM fact_nsduh_prevalence
            ORDER BY measure_id
        """).fetchall()
        return {"measures": [{"id": r[0], "name": r[1]} for r in rows]}


@router.get("/api/behavioral-health/nsduh/ranking")
@safe_route(default_response={"rows": [], "count": 0})
def nsduh_ranking(
    measure: str = Query(default="any_mental_illness"),
    age_group: str = Query(default="18+"),
):
    """Rank states by a specific NSDUH measure."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, estimate_pct, ci_lower_pct, ci_upper_pct
            FROM fact_nsduh_prevalence
            WHERE measure_id = ? AND age_group = ?
            ORDER BY estimate_pct DESC
        """, [measure, age_group]).fetchall()
        cols = ["state", "estimate_pct", "ci_lower_pct", "ci_upper_pct"]
        return {
            "measure": measure,
            "age_group": age_group,
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/facilities")
@safe_route(default_response={"rows": [], "count": 0})
def mh_facilities(
    state: str = Query(default=None),
    facility_type: str = Query(default=None),
):
    """Mental health & SUD treatment facilities from N-SUMHSS."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, facility_type, offers_mh, offers_su,
                   is_hospital, hospital_beds, residential_beds,
                   inpatient_psych_beds, crisis_beds,
                   offers_detox, offers_su_treatment
            FROM fact_mh_facility
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if facility_type:
            sql += " AND facility_type = ?"
            params.append(facility_type)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "facility_type", "offers_mh", "offers_su",
                "is_hospital", "hospital_beds", "residential_beds",
                "inpatient_psych_beds", "crisis_beds",
                "offers_detox", "offers_su_treatment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/facilities/summary")
@safe_route(default_response={"rows": [], "count": 0})
def mh_facilities_summary():
    """State-level summary of MH/SUD treatment facilities and bed capacity."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS total_facilities,
                COUNT(*) FILTER (WHERE offers_mh) AS mh_facilities,
                COUNT(*) FILTER (WHERE offers_su) AS su_facilities,
                COALESCE(SUM(hospital_beds), 0) AS hospital_beds,
                COALESCE(SUM(inpatient_psych_beds), 0) AS psych_beds,
                COALESCE(SUM(residential_beds), 0) AS residential_beds,
                COALESCE(SUM(crisis_beds), 0) AS crisis_beds,
                COUNT(*) FILTER (WHERE offers_detox) AS detox_facilities
            FROM fact_mh_facility
            GROUP BY state_code
            ORDER BY total_facilities DESC
        """).fetchall()
        cols = ["state", "total_facilities", "mh_facilities", "su_facilities",
                "hospital_beds", "psych_beds", "residential_beds",
                "crisis_beds", "detox_facilities"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/ipf")
@safe_route(default_response={"rows": [], "count": 0})
def ipf_quality(state: str = Query(default=None)):
    """Inpatient psychiatric facility quality measures by state."""
    with get_cursor() as cur:
        sql = "SELECT * FROM fact_ipf_quality_state"
        params = []
        if state:
            sql += " WHERE state = ?"
            params.append(state.upper())
        rows = cur.execute(sql, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/block-grants")
@safe_route(default_response={"rows": [], "count": 0})
def block_grants(state: str = Query(default=None)):
    """SAMHSA Mental Health Block Grant allotments by state."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, program, fiscal_year, allotment
            FROM fact_block_grant
        """
        params = []
        if state:
            sql += " WHERE state_code = ?"
            params.append(state.upper())
        sql += " ORDER BY allotment DESC"
        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "program", "fiscal_year", "allotment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/brfss")
@safe_route(default_response={"rows": [], "count": 0})
def brfss_behavioral(
    state: str = Query(default=None),
    topic: str = Query(default=None),
    year: int = Query(default=None),
):
    """BRFSS behavioral health indicators by state."""
    with get_cursor() as cur:
        sql = """
            SELECT year, state_code, topic, question, response,
                   break_out, break_out_category,
                   sample_size, data_value_pct, ci_lower_pct, ci_upper_pct
            FROM fact_brfss_behavioral
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if topic:
            sql += " AND topic = ?"
            params.append(topic)
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["year", "state", "topic", "question", "response",
                "break_out", "break_out_category",
                "sample_size", "data_value_pct", "ci_lower_pct", "ci_upper_pct"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/gme/providers")
@safe_route(default_response={"rows": [], "count": 0})
def gme_providers(
    state: str = Query(default=None),
    teaching_only: bool = Query(default=False),
):
    """CMS Provider Specific File — GME/IME/DSH operational data."""
    with get_cursor() as cur:
        sql = """
            SELECT provider_ccn, npi, state_code, bed_size, case_mix_index,
                   interns_to_beds_ratio, dgme_passthrough,
                   capital_ime_ratio, operating_dsh,
                   ssi_ratio, medicaid_ratio, uncompensated_care_amount,
                   vbp_adjustment, hrrp_adjustment
            FROM fact_provider_specific
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if teaching_only:
            sql += " AND interns_to_beds_ratio > 0"
        sql += " ORDER BY interns_to_beds_ratio DESC NULLS LAST LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["provider_ccn", "npi", "state", "bed_size", "case_mix_index",
                "interns_to_beds_ratio", "dgme_passthrough",
                "capital_ime_ratio", "operating_dsh",
                "ssi_ratio", "medicaid_ratio", "uncompensated_care_amount",
                "vbp_adjustment", "hrrp_adjustment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/gme/summary")
@safe_route(default_response={"rows": [], "count": 0})
def gme_summary():
    """State-level summary of teaching hospitals and GME/IME data."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS total_providers,
                COUNT(*) FILTER (WHERE interns_to_beds_ratio > 0) AS teaching_hospitals,
                ROUND(AVG(case_mix_index), 3) AS avg_cmi,
                ROUND(SUM(dgme_passthrough), 0) AS total_dgme,
                ROUND(SUM(uncompensated_care_amount), 0) AS total_uc,
                ROUND(AVG(medicaid_ratio), 4) AS avg_medicaid_ratio,
                ROUND(AVG(ssi_ratio), 4) AS avg_ssi_ratio
            FROM fact_provider_specific
            GROUP BY state_code
            ORDER BY teaching_hospitals DESC
        """).fetchall()
        cols = ["state", "total_providers", "teaching_hospitals", "avg_cmi",
                "total_dgme", "total_uc", "avg_medicaid_ratio", "avg_ssi_ratio"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/chip/enrollment")
@safe_route(default_response={"rows": [], "count": 0})
def chip_enrollment(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Monthly CHIP and Medicaid enrollment by state."""
    with get_cursor() as cur:
        sql = """
            SELECT state_code, reporting_period, expansion_state,
                   new_applications, medicaid_determinations, chip_determinations,
                   child_enrollment, total_enrollment,
                   medicaid_enrollment, chip_enrollment, adult_enrollment
            FROM fact_chip_enrollment
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        if year:
            sql += " AND reporting_period LIKE ?"
            params.append(f"{year}%")
        sql += " ORDER BY state_code, reporting_period"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "reporting_period", "expansion_state",
                "new_applications", "medicaid_determinations", "chip_determinations",
                "child_enrollment", "total_enrollment",
                "medicaid_enrollment", "chip_enrollment", "adult_enrollment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/chip/summary")
@safe_route(default_response={"rows": [], "count": 0})
def chip_summary():
    """Latest CHIP enrollment summary by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code,
                   MAX(reporting_period) AS latest_period,
                   LAST(chip_enrollment ORDER BY reporting_period) AS chip_enrollment,
                   LAST(medicaid_enrollment ORDER BY reporting_period) AS medicaid_enrollment,
                   LAST(total_enrollment ORDER BY reporting_period) AS total_enrollment
            FROM fact_chip_enrollment
            WHERE chip_enrollment IS NOT NULL
            GROUP BY state_code
            ORDER BY chip_enrollment DESC
        """).fetchall()
        cols = ["state", "latest_period", "chip_enrollment",
                "medicaid_enrollment", "total_enrollment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hcbs/waiver-enrollment")
@safe_route(default_response={"rows": [], "count": 0})
def hcbs_waiver_enrollment(state: str = Query(default=None)):
    """1915(c) HCBS waiver participant counts by state."""
    with get_cursor() as cur:
        sql = """
            SELECT year, state_name, category, enrollee_count,
                   denominator_count, enrollee_pct
            FROM fact_hcbs_waiver_enrollment
            WHERE category = 'Enrolled in 1915(c) waiver'
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        sql += " ORDER BY year, enrollee_count DESC"

        rows = cur.execute(sql, params).fetchall()
        cols = ["year", "state", "category", "enrollee_count",
                "denominator_count", "enrollee_pct"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/quality")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_quality(
    state: str = Query(default=None),
    measure: str = Query(default=None),
):
    """Hospice facility quality measures from CMS Care Compare."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code, county,
                   measure_code, measure_name, score, measure_date_range
            FROM fact_hospice_quality
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_code = ?"
            params.append(measure)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code", "county",
                "measure_code", "measure_name", "score", "measure_date_range"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/summary")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_summary():
    """State-level hospice facility summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT ccn) AS facilities,
                COUNT(DISTINCT measure_code) AS measures,
                ROUND(AVG(score) FILTER (WHERE measure_code = 'H_001_01_OBSERVED'), 1) AS avg_treatment_prefs,
                ROUND(AVG(score) FILTER (WHERE measure_code = 'H_003_01_OBSERVED'), 1) AS avg_pain_screening
            FROM fact_hospice_quality
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "measures", "avg_treatment_prefs", "avg_pain_screening"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/measures")
@safe_route(default_response={"measures": []})
def hospice_measures():
    """List available hospice quality measures."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT DISTINCT measure_code, measure_name
            FROM fact_hospice_quality
            WHERE measure_name != ''
            ORDER BY measure_code
        """).fetchall()
        return {"measures": [{"id": r[0], "name": r[1]} for r in rows]}


@router.get("/api/chip/eligibility")
@safe_route(default_response={"rows": [], "count": 0})
def chip_eligibility():
    """CHIP and Medicaid eligibility income thresholds by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_name, medicaid_ages_0_1, medicaid_ages_1_5, medicaid_ages_6_18,
                   separate_chip, pregnant_women_medicaid, pregnant_women_chip,
                   parent_caretaker, expansion_adults, separate_chip_ages
            FROM fact_chip_eligibility
            ORDER BY state_name
        """).fetchall()
        cols = ["state", "medicaid_0_1", "medicaid_1_5", "medicaid_6_18",
                "separate_chip", "pregnant_medicaid", "pregnant_chip",
                "parent_caretaker", "expansion_adults", "chip_ages"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/eligibility/continuous")
@safe_route(default_response={"rows": [], "count": 0})
def continuous_eligibility():
    """Continuous eligibility policies by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_name, chip_continuous, medicaid_continuous, exceptions
            FROM fact_continuous_eligibility
            ORDER BY state_name
        """).fetchall()
        cols = ["state", "chip_continuous", "medicaid_continuous", "exceptions"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hcbs/authority")
@safe_route(default_response={"rows": [], "count": 0})
def hcbs_authority():
    """HCBS measures by authority type from Medicaid Scorecard."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT measure_id, measure_name, measure_type, period_type,
                   data_range, num_states, median_value, mean_value,
                   reporting_program, pillar_id
            FROM fact_hcbs_authority
            ORDER BY measure_id
        """).fetchall()
        cols = ["measure_id", "measure_name", "measure_type", "period_type",
                "data_range", "num_states", "median_value", "mean_value",
                "reporting_program", "pillar_id"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/quality-features")
@safe_route(default_response={"rows": [], "count": 0})
def mc_quality_features():
    """Managed care quality assurance features by plan type."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT feature, comprehensive_mco, pccm, pccm_entity, mltss,
                   bho, dental, transportation, other_php, pace, year
            FROM fact_mc_quality_features
            ORDER BY feature
        """).fetchall()
        cols = ["feature", "comprehensive_mco", "pccm", "pccm_entity", "mltss",
                "bho", "dental", "transportation", "other_php", "pace", "year"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/maternal-health")
@safe_route(default_response={"rows": [], "count": 0})
def maternal_health(
    state: str = Query(default=None),
    measure: str = Query(default=None),
):
    """Hospital-level maternal health quality measures."""
    with get_cursor() as cur:
        sql = """
            SELECT facility_id, facility_name, state, city, county,
                   measure_id, measure_name, score, sample_size, start_date, end_date
            FROM fact_maternal_health
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_id = ?"
            params.append(measure)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["facility_id", "facility_name", "state", "city", "county",
                "measure_id", "measure_name", "score", "sample_size", "start_date", "end_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/maternal-health/summary")
@safe_route(default_response={"rows": [], "count": 0})
def maternal_health_summary():
    """State-level maternal health facility summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT facility_id) AS facilities,
                COUNT(*) FILTER (WHERE measure_id = 'SM_7' AND score = 'Yes') AS structural_measure_yes,
                COUNT(*) FILTER (WHERE measure_id = 'SM_7') AS structural_measure_total
            FROM fact_maternal_health
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "structural_measure_yes", "structural_measure_total"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/state")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_state_data(state: str = Query(default=None)):
    """State-level hospice quality measures."""
    with get_cursor() as cur:
        sql = """
            SELECT state, measure_code, measure_name, score, measure_date_range
            FROM fact_hospice_state
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        sql += " ORDER BY state, measure_code"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "measure_code", "measure_name", "score", "measure_date_range"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/asc/quality")
@safe_route(default_response={"rows": [], "count": 0})
def asc_quality_state():
    """Ambulatory surgical center quality measures by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT * FROM fact_asc_quality_state ORDER BY state
        """).fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/home-health/state")
@safe_route(default_response={"rows": [], "count": 0})
def home_health_state_data():
    """Home health quality ratings by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT * FROM fact_home_health_state2 ORDER BY state
        """).fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/oas-cahps/state")
@safe_route(default_response={"rows": [], "count": 0})
def oas_cahps_state(state: str = Query(default=None)):
    """Outpatient surgery patient experience (OAS CAHPS) by state."""
    with get_cursor() as cur:
        sql = """
            SELECT state, measure_id, question, answer_description,
                   answer_pct, start_date, end_date
            FROM fact_oas_cahps_state
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        sql += " ORDER BY state, measure_id"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "measure_id", "question", "answer_description",
                "answer_pct", "start_date", "end_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hcbs/cms372")
@safe_route(default_response={"rows": [], "count": 0})
def cms372_waiver(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """CMS-372 waiver program data — participants, expenditures, days of service."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, waiver_number, waiver_name, target_group,
                   total_participants, total_expenditures,
                   avg_participant_months, year
            FROM fact_cms372_waiver
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY state_name, waiver_number"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "waiver_number", "waiver_name", "target_group",
                "total_participants", "total_expenditures",
                "avg_participant_months", "year"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hcbs/cms372/summary")
@safe_route(default_response={"rows": [], "count": 0})
def cms372_summary():
    """State-level summary of 1915(c) waiver expenditures and participants."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_name,
                year,
                COUNT(*) AS waivers,
                ROUND(SUM(total_participants)) AS participants,
                ROUND(SUM(total_expenditures)) AS expenditures,
                ROUND(SUM(total_expenditures) / NULLIF(SUM(total_participants), 0)) AS per_participant
            FROM fact_cms372_waiver
            GROUP BY state_name, year
            ORDER BY expenditures DESC
        """).fetchall()
        cols = ["state", "year", "waivers", "participants", "expenditures", "per_participant"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/plans")
@safe_route(default_response={"rows": [], "count": 0})
def mc_plans(
    state: str = Query(default=None),
    year: int = Query(default=None),
    pace_only: bool = Query(default=False),
):
    """Managed care enrollment by plan (includes PACE organizations)."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, program_name, plan_name, geographic_region,
                   medicaid_only_enrollment, dual_enrollment, total_enrollment,
                   year, parent_organization, is_pace
            FROM fact_mc_enrollment_plan
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if pace_only:
            sql += " AND is_pace = TRUE"
        sql += " ORDER BY total_enrollment DESC NULLS LAST LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "program_name", "plan_name", "geographic_region",
                "medicaid_only_enrollment", "dual_enrollment", "total_enrollment",
                "year", "parent_organization", "is_pace"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/pace")
@safe_route(default_response={"rows": [], "count": 0})
def pace_enrollment():
    """PACE enrollment summary by state and year."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_name,
                year,
                COUNT(*) AS plans,
                SUM(total_enrollment) AS total_enrollment,
                SUM(dual_enrollment) AS dual_enrollment
            FROM fact_mc_enrollment_plan
            WHERE is_pace = TRUE
            GROUP BY state_name, year
            ORDER BY year DESC, total_enrollment DESC
        """).fetchall()
        cols = ["state", "year", "plans", "total_enrollment", "dual_enrollment"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


# ── Round 3 endpoints ──────────────────────────────────────────────────────


@router.get("/api/behavioral-health/conditions")
@safe_route(default_response={"rows": [], "count": 0})
def bh_by_condition(
    state: str = Query(default=None),
    year: int = Query(default=None),
    condition: str = Query(default=None),
):
    """Behavioral health conditions among Medicaid beneficiaries by state."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, condition, beneficiaries, pct_of_bh, data_quality
            FROM fact_bh_by_condition
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if condition:
            sql += " AND condition ILIKE ?"
            params.append(f"%{condition}%")
        sql += " ORDER BY state_name, year, condition"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "condition", "beneficiaries", "pct_of_bh", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/conditions/summary")
@safe_route(default_response={"rows": [], "count": 0})
def bh_conditions_summary():
    """Summary of BH conditions — most prevalent conditions by state count."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                condition,
                COUNT(DISTINCT state_name) AS states,
                SUM(beneficiaries) AS total_beneficiaries,
                ROUND(AVG(pct_of_bh), 1) AS avg_pct
            FROM fact_bh_by_condition
            WHERE beneficiaries IS NOT NULL
            GROUP BY condition
            ORDER BY total_beneficiaries DESC NULLS LAST
        """).fetchall()
        cols = ["condition", "states", "total_beneficiaries", "avg_pct"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/mh-sud-recipients")
@safe_route(default_response={"rows": [], "count": 0})
def mh_sud_recipients(
    year: int = Query(default=None),
    geography: str = Query(default=None),
):
    """MH/SUD service recipients from T-MSIS by state and subpopulation."""
    with get_cursor() as cur:
        sql = """
            SELECT year, geography, subpop_topic, subpopulation, category,
                   enrollee_count, denominator, pct_enrollees, data_version
            FROM fact_mh_sud_recipients
            WHERE 1=1
        """
        params = []
        if year:
            sql += " AND year = ?"
            params.append(year)
        if geography:
            sql += " AND geography ILIKE ?"
            params.append(f"%{geography}%")
        sql += " ORDER BY year, geography, subpop_topic"

        rows = cur.execute(sql, params).fetchall()
        cols = ["year", "geography", "subpop_topic", "subpopulation", "category",
                "enrollee_count", "denominator", "pct_enrollees", "data_version"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/maternal/morbidity")
@safe_route(default_response={"rows": [], "count": 0})
def maternal_morbidity(
    year: int = Query(default=None),
    geography: str = Query(default=None),
):
    """Preterm birth and severe maternal morbidity among Medicaid births."""
    with get_cursor() as cur:
        sql = """
            SELECT year, geography, subpop_topic, subpopulation, category,
                   delivery_count, denominator, rate, data_version
            FROM fact_maternal_morbidity
            WHERE 1=1
        """
        params = []
        if year:
            sql += " AND year = ?"
            params.append(year)
        if geography:
            sql += " AND geography ILIKE ?"
            params.append(f"%{geography}%")
        sql += " ORDER BY year, geography, category"

        rows = cur.execute(sql, params).fetchall()
        cols = ["year", "geography", "subpop_topic", "subpopulation", "category",
                "delivery_count", "denominator", "rate", "data_version"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/dental/services")
@safe_route(default_response={"rows": [], "count": 0})
def dental_services(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Dental services to Medicaid/CHIP children under 19."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, dental_service,
                   service_count, rate_per_1000, data_quality
            FROM fact_dental_services
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY state_name, year, month"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "dental_service",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/telehealth/services")
@safe_route(default_response={"rows": [], "count": 0})
def telehealth_services(
    state: str = Query(default=None),
    year: int = Query(default=None),
    telehealth_type: str = Query(default=None),
):
    """Telehealth utilization by state, type, and month."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, telehealth_type, service_type,
                   service_count, rate_per_1000, data_quality
            FROM fact_telehealth_services
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if telehealth_type:
            sql += " AND telehealth_type ILIKE ?"
            params.append(f"%{telehealth_type}%")
        sql += " ORDER BY state_name, year, month LIMIT 2000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "telehealth_type", "service_type",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/irf/quality")
@safe_route(default_response={"rows": [], "count": 0})
def irf_quality(
    state: str = Query(default=None),
    measure: str = Query(default=None),
):
    """Inpatient Rehabilitation Facility quality measures."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code, county,
                   measure_code, score, footnote, start_date, end_date
            FROM fact_irf_provider
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_code = ?"
            params.append(measure)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code", "county",
                "measure_code", "score", "footnote", "start_date", "end_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/irf/summary")
@safe_route(default_response={"rows": [], "count": 0})
def irf_summary():
    """State-level IRF summary — facility count and average scores."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT ccn) AS facilities,
                COUNT(DISTINCT measure_code) AS measures,
                ROUND(AVG(score), 1) AS avg_score
            FROM fact_irf_provider
            WHERE score IS NOT NULL
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "measures", "avg_score"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/ltch/quality")
@safe_route(default_response={"rows": [], "count": 0})
def ltch_quality(
    state: str = Query(default=None),
    measure: str = Query(default=None),
):
    """Long-Term Care Hospital quality measures."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code, county,
                   measure_code, score, footnote, start_date, end_date
            FROM fact_ltch_provider
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_code = ?"
            params.append(measure)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code", "county",
                "measure_code", "score", "footnote", "start_date", "end_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/ltch/summary")
@safe_route(default_response={"rows": [], "count": 0})
def ltch_summary():
    """State-level LTCH summary — facility count and average scores."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT ccn) AS facilities,
                COUNT(DISTINCT measure_code) AS measures,
                ROUND(AVG(score), 1) AS avg_score
            FROM fact_ltch_provider
            WHERE score IS NOT NULL
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "measures", "avg_score"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/home-health/agencies")
@safe_route(default_response={"rows": [], "count": 0})
def home_health_agencies(
    state: str = Query(default=None),
):
    """Home health agencies with services offered and quality ratings."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code,
                   ownership_type, offers_nursing, offers_pt, offers_ot,
                   offers_speech, offers_social, offers_hha,
                   quality_star_rating, certification_date
            FROM fact_home_health_agency
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        sql += " ORDER BY quality_star_rating DESC NULLS LAST LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code",
                "ownership_type", "offers_nursing", "offers_pt", "offers_ot",
                "offers_speech", "offers_social", "offers_hha",
                "quality_star_rating", "certification_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/home-health/agencies/summary")
@safe_route(default_response={"rows": [], "count": 0})
def home_health_agencies_summary():
    """State-level HHA summary — count, ownership mix, quality ratings."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(*) AS agencies,
                ROUND(AVG(quality_star_rating), 2) AS avg_quality,
                COUNT(*) FILTER (WHERE ownership_type ILIKE '%profit%' AND ownership_type NOT ILIKE '%non%') AS for_profit,
                COUNT(*) FILTER (WHERE ownership_type ILIKE '%non%profit%') AS non_profit,
                COUNT(*) FILTER (WHERE ownership_type ILIKE '%government%') AS government
            FROM fact_home_health_agency
            GROUP BY state
            ORDER BY agencies DESC
        """).fetchall()
        cols = ["state", "agencies", "avg_quality", "for_profit", "non_profit", "government"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/physical-among-mh")
@safe_route(default_response={"rows": [], "count": 0})
def physical_among_mh(
    state: str = Query(default=None),
    year: int = Query(default=None),
    condition: str = Query(default=None),
):
    """Physical health conditions among Medicaid beneficiaries with MH conditions."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, condition, beneficiaries, pct_of_mh, data_quality
            FROM fact_physical_among_mh
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if condition:
            sql += " AND condition ILIKE ?"
            params.append(f"%{condition}%")
        sql += " ORDER BY state_name, year, condition"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "condition", "beneficiaries", "pct_of_mh", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/physical-among-sud")
@safe_route(default_response={"rows": [], "count": 0})
def physical_among_sud(
    state: str = Query(default=None),
    year: int = Query(default=None),
    condition: str = Query(default=None),
):
    """Physical health conditions among Medicaid beneficiaries with SUD conditions."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, sud_category, sud_category_value,
                   condition, beneficiaries, pct_of_sud, data_quality
            FROM fact_physical_among_sud
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if condition:
            sql += " AND condition ILIKE ?"
            params.append(f"%{condition}%")
        sql += " ORDER BY state_name, year, condition"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "sud_category", "sud_category_value",
                "condition", "beneficiaries", "pct_of_sud", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


# ── Round 3b endpoints ─────────────────────────────────────────────────────


@router.get("/api/behavioral-health/services")
@safe_route(default_response={"rows": [], "count": 0})
def bh_services(
    state: str = Query(default=None),
    year: int = Query(default=None),
    condition: str = Query(default=None),
    service_type: str = Query(default=None),
):
    """Comprehensive BH services by condition, service type, state, and month."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, condition, service_type,
                   service_count, rate_per_1000, data_quality
            FROM fact_bh_services
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if condition:
            sql += " AND condition ILIKE ?"
            params.append(f"%{condition}%")
        if service_type:
            sql += " AND service_type ILIKE ?"
            params.append(f"%{service_type}%")
        sql += " ORDER BY state_name, year, month LIMIT 2000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "condition", "service_type",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/services/summary")
@safe_route(default_response={"rows": [], "count": 0})
def bh_services_summary():
    """State-level BH services summary — total service counts by condition."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_name,
                condition,
                SUM(service_count) AS total_services,
                ROUND(AVG(rate_per_1000), 1) AS avg_rate_per_1000,
                COUNT(DISTINCT service_type) AS service_types
            FROM fact_bh_services
            WHERE service_count IS NOT NULL
            GROUP BY state_name, condition
            ORDER BY total_services DESC NULLS LAST
        """).fetchall()
        cols = ["state", "condition", "total_services", "avg_rate_per_1000", "service_types"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/integrated-care")
@safe_route(default_response={"rows": [], "count": 0})
def integrated_care(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Beneficiaries who could benefit from integrated MH/SUD + physical health care."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, population, beneficiaries, data_quality
            FROM fact_integrated_care
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY state_name, year, population"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "population", "beneficiaries", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hcbs/1915c-participants")
@safe_route(default_response={"rows": [], "count": 0})
def waiver_1915c_participants(
    year: int = Query(default=None),
    geography: str = Query(default=None),
):
    """1915(c) waiver participants from T-MSIS (2020-2022)."""
    with get_cursor() as cur:
        sql = """
            SELECT year, geography, subpop_topic, subpopulation, category,
                   enrollee_count, denominator, pct_enrollees, data_version
            FROM fact_1915c_participants
            WHERE 1=1
        """
        params = []
        if year:
            sql += " AND year = ?"
            params.append(year)
        if geography:
            sql += " AND geography ILIKE ?"
            params.append(f"%{geography}%")
        sql += " ORDER BY year, geography"

        rows = cur.execute(sql, params).fetchall()
        cols = ["year", "geography", "subpop_topic", "subpopulation", "category",
                "enrollee_count", "denominator", "pct_enrollees", "data_version"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/share")
@safe_route(default_response={"rows": [], "count": 0})
def mc_share(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Share of Medicaid enrollees in managed care by state and year."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, total_enrollees,
                   any_mc_enrolled, pct_any_mc,
                   comprehensive_mc_enrolled, pct_comprehensive_mc
            FROM fact_mc_share
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY year DESC, state_name"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "total_enrollees",
                "any_mc_enrolled", "pct_any_mc",
                "comprehensive_mc_enrolled", "pct_comprehensive_mc"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/monthly")
@safe_route(default_response={"rows": [], "count": 0})
def mc_monthly(
    state: str = Query(default=None),
    mc_type: str = Query(default=None),
):
    """Monthly managed care enrollment by state (2016-2022)."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, reporting_month, mc_participation,
                   enrolled_count, data_quality
            FROM fact_mc_monthly
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if mc_type:
            sql += " AND mc_participation ILIKE ?"
            params.append(f"%{mc_type}%")
        sql += " ORDER BY state_name, reporting_month LIMIT 2000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "reporting_month", "mc_participation",
                "enrolled_count", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


# ── Round 3c endpoints ─────────────────────────────────────────────────────


@router.get("/api/dialysis/facilities")
@safe_route(default_response={"rows": [], "count": 0})
def dialysis_facilities(
    state: str = Query(default=None),
    min_stars: int = Query(default=None),
):
    """Dialysis facilities with Five Star ratings and quality measures."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code, county,
                   five_star, ownership_type, chain_owned, dialysis_stations,
                   mortality_rate, hospitalization_rate, readmission_rate,
                   fistula_rate, survival_category
            FROM fact_dialysis_facility
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if min_stars:
            sql += " AND five_star >= ?"
            params.append(min_stars)
        sql += " ORDER BY five_star DESC NULLS LAST LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code", "county",
                "five_star", "ownership_type", "chain_owned", "dialysis_stations",
                "mortality_rate", "hospitalization_rate", "readmission_rate",
                "fistula_rate", "survival_category"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/dialysis/summary")
@safe_route(default_response={"rows": [], "count": 0})
def dialysis_summary():
    """State-level dialysis facility summary — counts, stars, ownership."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(*) AS facilities,
                SUM(dialysis_stations) AS total_stations,
                ROUND(AVG(five_star), 2) AS avg_five_star,
                ROUND(AVG(mortality_rate), 2) AS avg_mortality_rate,
                COUNT(*) FILTER (WHERE ownership_type ILIKE '%profit%' AND ownership_type NOT ILIKE '%non%') AS for_profit,
                COUNT(*) FILTER (WHERE chain_owned = 'Y') AS chain_owned
            FROM fact_dialysis_facility
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "total_stations", "avg_five_star",
                "avg_mortality_rate", "for_profit", "chain_owned"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/ipf-facility")
@safe_route(default_response={"rows": [], "count": 0})
def ipf_facility(
    state: str = Query(default=None),
):
    """Inpatient Psychiatric Facility quality measures by facility."""
    with get_cursor() as cur:
        sql = """
            SELECT facility_id, facility_name, state, city, zip_code, county,
                   hbips2_rate, hbips3_rate, smd_pct, sub2_pct, tob3_pct,
                   imm2_pct, readm30_rate, readm30_category, start_date, end_date
            FROM fact_ipf_facility
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        sql += " ORDER BY facility_name LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["facility_id", "facility_name", "state", "city", "zip_code", "county",
                "hbips2_rate", "hbips3_rate", "smd_pct", "sub2_pct", "tob3_pct",
                "imm2_pct", "readm30_rate", "readm30_category", "start_date", "end_date"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/behavioral-health/ipf-facility/summary")
@safe_route(default_response={"rows": [], "count": 0})
def ipf_facility_summary():
    """State-level IPF summary — facility count and average quality metrics."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(*) AS facilities,
                ROUND(AVG(readm30_rate), 1) AS avg_readm30_rate,
                ROUND(AVG(smd_pct), 1) AS avg_smd_pct,
                ROUND(AVG(imm2_pct), 1) AS avg_imm2_pct
            FROM fact_ipf_facility
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "avg_readm30_rate", "avg_smd_pct", "avg_imm2_pct"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/providers")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_providers(
    state: str = Query(default=None),
    measure: str = Query(default=None),
):
    """Hospice provider quality measures (465K+ rows, 6,943 hospices)."""
    with get_cursor() as cur:
        sql = """
            SELECT ccn, facility_name, state, city, zip_code, county,
                   measure_code, measure_name, score, measure_date_range
            FROM fact_hospice_provider
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if measure:
            sql += " AND measure_code = ?"
            params.append(measure)
        sql += " LIMIT 1000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["ccn", "facility_name", "state", "city", "zip_code", "county",
                "measure_code", "measure_name", "score", "measure_date_range"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospice/providers/summary")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_providers_summary():
    """State-level hospice provider summary."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT ccn) AS hospices,
                COUNT(DISTINCT measure_code) AS measures,
                ROUND(AVG(score), 1) AS avg_score
            FROM fact_hospice_provider
            WHERE score IS NOT NULL
            GROUP BY state
            ORDER BY hospices DESC
        """).fetchall()
        cols = ["state", "hospices", "measures", "avg_score"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


# ── Round 4 endpoints ──────────────────────────────────────────────────────


@router.get("/api/children/screenings")
@safe_route(default_response={"rows": [], "count": 0})
def health_screenings(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Health screenings for Medicaid/CHIP children under 19."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, screening_service,
                   service_count, rate_per_1000, data_quality
            FROM fact_health_screenings
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY state_name, year, month"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "screening_service",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/acute-care/services")
@safe_route(default_response={"rows": [], "count": 0})
def acute_care_services(
    state: str = Query(default=None),
    year: int = Query(default=None),
    condition: str = Query(default=None),
):
    """Acute care services by condition for Medicaid/CHIP population."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, condition, service_type,
                   service_count, rate_per_1000, data_quality
            FROM fact_acute_care
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        if condition:
            sql += " AND condition ILIKE ?"
            params.append(f"%{condition}%")
        sql += " ORDER BY state_name, year, month LIMIT 2000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "condition", "service_type",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/acute-care/summary")
@safe_route(default_response={"rows": [], "count": 0})
def acute_care_summary():
    """State-level acute care summary — total services by condition."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_name, condition,
                SUM(service_count) AS total_services,
                ROUND(AVG(rate_per_1000), 1) AS avg_rate_per_1000,
                COUNT(DISTINCT service_type) AS service_types
            FROM fact_acute_care
            WHERE service_count IS NOT NULL
            GROUP BY state_name, condition
            ORDER BY total_services DESC NULLS LAST
        """).fetchall()
        cols = ["state", "condition", "total_services", "avg_rate_per_1000", "service_types"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/perinatal/services")
@safe_route(default_response={"rows": [], "count": 0})
def perinatal_services(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Perinatal care services for Medicaid/CHIP beneficiaries ages 15-44."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, month, care_type,
                   service_count, rate_per_1000, data_quality
            FROM fact_perinatal_care
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY state_name, year, month LIMIT 2000"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "month", "care_type",
                "service_count", "rate_per_1000", "data_quality"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/managed-care/summary")
@safe_route(default_response={"rows": [], "count": 0})
def mc_enrollment_summary(
    state: str = Query(default=None),
    year: int = Query(default=None),
):
    """Managed care enrollment summary — total, any MC, comprehensive MC, new adults."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, year, total_enrollees,
                   any_mc_enrollment, comprehensive_mc_enrollment,
                   new_adult_comprehensive_mco
            FROM fact_mc_summary
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_name ILIKE ?"
            params.append(f"%{state}%")
        if year:
            sql += " AND year = ?"
            params.append(year)
        sql += " ORDER BY year DESC, state_name"

        rows = cur.execute(sql, params).fetchall()
        cols = ["state", "year", "total_enrollees",
                "any_mc_enrollment", "comprehensive_mc_enrollment",
                "new_adult_comprehensive_mco"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/asc/facilities")
@safe_route(default_response={"rows": [], "count": 0})
def asc_facilities(
    state: str = Query(default=None),
):
    """Ambulatory Surgical Center quality measures by facility."""
    with get_cursor() as cur:
        sql = """
            SELECT facility_name, facility_id, npi, city, state, zip_code,
                   year, asc1_rate, asc2_rate, asc3_rate, asc4_rate
            FROM fact_asc_facility
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        sql += " ORDER BY facility_name LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["facility_name", "facility_id", "npi", "city", "state", "zip_code",
                "year", "asc1_rate", "asc2_rate", "asc3_rate", "asc4_rate"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/asc/facilities/summary")
@safe_route(default_response={"rows": [], "count": 0})
def asc_facilities_summary():
    """State-level ASC summary — facility count and average quality rates."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(DISTINCT facility_id) AS facilities,
                ROUND(AVG(asc1_rate), 1) AS avg_asc1,
                ROUND(AVG(asc2_rate), 1) AS avg_asc2,
                ROUND(AVG(asc3_rate), 1) AS avg_asc3,
                ROUND(AVG(asc4_rate), 1) AS avg_asc4
            FROM fact_asc_facility
            GROUP BY state
            ORDER BY facilities DESC
        """).fetchall()
        cols = ["state", "facilities", "avg_asc1", "avg_asc2", "avg_asc3", "avg_asc4"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospitals/directory")
@safe_route(default_response={"rows": [], "count": 0})
def hospital_directory(
    state: str = Query(default=None),
    hospital_type: str = Query(default=None),
    min_rating: int = Query(default=None),
):
    """CMS Hospital General Information — directory with ratings, types, ownership."""
    with get_cursor() as cur:
        sql = """
            SELECT facility_id, facility_name, state, city, zip_code, county,
                   hospital_type, ownership, emergency_services,
                   birthing_friendly, overall_rating
            FROM fact_hospital_directory
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state = ?"
            params.append(state.upper())
        if hospital_type:
            sql += " AND hospital_type ILIKE ?"
            params.append(f"%{hospital_type}%")
        if min_rating:
            sql += " AND overall_rating >= ?"
            params.append(min_rating)
        sql += " ORDER BY overall_rating DESC NULLS LAST LIMIT 500"

        rows = cur.execute(sql, params).fetchall()
        cols = ["facility_id", "facility_name", "state", "city", "zip_code", "county",
                "hospital_type", "ownership", "emergency_services",
                "birthing_friendly", "overall_rating"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


@router.get("/api/hospitals/directory/summary")
@safe_route(default_response={"rows": [], "count": 0})
def hospital_directory_summary():
    """State-level hospital directory summary — counts by type, ratings, ownership."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(*) AS total_hospitals,
                COUNT(*) FILTER (WHERE hospital_type = 'Acute Care Hospitals') AS acute_care,
                COUNT(*) FILTER (WHERE hospital_type = 'Critical Access Hospitals') AS critical_access,
                COUNT(*) FILTER (WHERE hospital_type = 'Psychiatric') AS psychiatric,
                COUNT(*) FILTER (WHERE hospital_type = 'Childrens') AS childrens,
                ROUND(AVG(overall_rating), 2) AS avg_rating,
                COUNT(*) FILTER (WHERE birthing_friendly = 'Y') AS birthing_friendly,
                COUNT(*) FILTER (WHERE emergency_services = 'Yes') AS with_emergency
            FROM fact_hospital_directory
            GROUP BY state
            ORDER BY total_hospitals DESC
        """).fetchall()
        cols = ["state", "total_hospitals", "acute_care", "critical_access",
                "psychiatric", "childrens", "avg_rating",
                "birthing_friendly", "with_emergency"]
        return {
            "rows": [dict(zip(cols, r)) for r in rows],
            "count": len(rows),
        }


# ── Managed Care Programs (2019) ──────────────────────────────────

@router.get("/api/managed-care/programs")
@safe_route(default_response={"rows": [], "count": 0})
def mc_programs(
    state: str = Query(default=None),
    program_type: str = Query(default=None),
    authority: str = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """Managed care programs by state with benefits, populations, and quality requirements."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if program_type:
            where.append("program_type ILIKE ?")
            params.append(f"%{program_type}%")
        if authority:
            where.append("federal_authority ILIKE ?")
            params.append(f"%{authority}%")
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_mc_programs
            WHERE {' AND '.join(where)}
            ORDER BY state, program_name
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/managed-care/programs/summary")
@safe_route(default_response={"rows": [], "count": 0})
def mc_programs_summary():
    """Summary of managed care programs by state and program type."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state,
                COUNT(*) AS total_programs,
                COUNT(*) FILTER (WHERE program_type ILIKE '%MCO%') AS mco_programs,
                COUNT(*) FILTER (WHERE program_type ILIKE '%PACE%') AS pace_programs,
                COUNT(*) FILTER (WHERE program_type ILIKE '%PCCM%') AS pccm_programs,
                COUNT(*) FILTER (WHERE program_type ILIKE '%Behavioral%' OR program_type ILIKE '%BHO%') AS bh_programs,
                COUNT(*) FILTER (WHERE ben_inpatient_bh = true) AS programs_with_bh_inpatient,
                COUNT(*) FILTER (WHERE ben_hcbs = true) AS programs_with_hcbs,
                COUNT(*) FILTER (WHERE qa_hedis = true) AS programs_requiring_hedis,
                COUNT(*) FILTER (WHERE qa_accreditation = true) AS programs_requiring_accreditation
            FROM fact_mc_programs
            GROUP BY state
            ORDER BY total_programs DESC
        """).fetchall()
        cols = ["state", "total_programs", "mco_programs", "pace_programs",
                "pccm_programs", "bh_programs", "programs_with_bh_inpatient",
                "programs_with_hcbs", "programs_requiring_hedis", "programs_requiring_accreditation"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── CHIP Enrollment ───────────────────────────────────────────────

@router.get("/api/chip/enrollment-unwinding")
@safe_route(default_response={"rows": [], "count": 0})
def chip_enrollment_unwinding(
    state: str = Query(default=None),
    limit: int = Query(default=1000, le=5000),
):
    """CHIP enrollment by month/state during CAA/unwinding period."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_chip_enrollment_unwinding
            WHERE {' AND '.join(where)}
            ORDER BY state_name, reporting_period
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/chip/program-monthly")
@safe_route(default_response={"rows": [], "count": 0})
def chip_program_monthly(
    state: str = Query(default=None),
    program_type: str = Query(default=None),
    limit: int = Query(default=2000, le=15000),
):
    """Medicaid vs CHIP enrollment by program type, month, and state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if program_type:
            where.append("program_type = ?")
            params.append(program_type)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_chip_program_monthly
            WHERE {' AND '.join(where)}
            ORDER BY state_name, reporting_month
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/chip/program-monthly/summary")
@safe_route(default_response={"rows": [], "count": 0})
def chip_program_monthly_summary():
    """Summary of Medicaid vs CHIP enrollment by state (latest month)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            WITH latest AS (
                SELECT state_name, program_type, enrolled_count,
                       ROW_NUMBER() OVER (PARTITION BY state_name, program_type
                                          ORDER BY reporting_month DESC) AS rn
                FROM fact_chip_program_monthly
                WHERE enrolled_count IS NOT NULL
            )
            SELECT
                state_name,
                SUM(CASE WHEN program_type = 'Medicaid' THEN enrolled_count END) AS medicaid_enrollment,
                SUM(CASE WHEN program_type = 'CHIP' THEN enrolled_count END) AS chip_enrollment
            FROM latest WHERE rn = 1
            GROUP BY state_name
            ORDER BY medicaid_enrollment DESC NULLS LAST
        """).fetchall()
        cols = ["state_name", "medicaid_enrollment", "chip_enrollment"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Applications & Renewals ───────────────────────────────────────

@router.get("/api/enrollment/applications")
@safe_route(default_response={"rows": [], "count": 0})
def medicaid_applications(
    state: str = Query(default=None),
    limit: int = Query(default=1000, le=15000),
):
    """Medicaid/CHIP applications, determinations, and enrollment by state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code = ?")
            params.append(state.upper())
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_medicaid_applications
            WHERE {' AND '.join(where)}
            ORDER BY state_code, reporting_period DESC
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/renewals")
@safe_route(default_response={"rows": [], "count": 0})
def renewal_processing(
    state: str = Query(default=None),
    limit: int = Query(default=1000, le=5000),
):
    """Eligibility renewal processing during unwinding by state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code = ?")
            params.append(state.upper())
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_renewal_processing
            WHERE {' AND '.join(where)}
            ORDER BY state_code, reporting_period DESC
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/renewals/summary")
@safe_route(default_response={"rows": [], "count": 0})
def renewal_processing_summary():
    """Summary of renewal processing outcomes by state (latest period)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            WITH latest AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY state_code
                                             ORDER BY reporting_period DESC) AS rn
                FROM fact_renewal_processing
                WHERE renewals_initiated IS NOT NULL
            )
            SELECT
                state_code, state_name,
                renewals_initiated, renewals_completed,
                determined_eligible, determined_ineligible,
                ROUND(100.0 * determined_ineligible /
                      NULLIF(renewals_completed, 0), 1) AS pct_disenrolled,
                reporting_period
            FROM latest WHERE rn = 1
            ORDER BY pct_disenrolled DESC NULLS LAST
        """).fetchall()
        cols = ["state_code", "state_name", "renewals_initiated", "renewals_completed",
                "determined_eligible", "determined_ineligible", "pct_disenrolled",
                "reporting_period"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Children's Health Services ────────────────────────────────────

@router.get("/api/children/vaccinations")
@safe_route(default_response={"rows": [], "count": 0})
def children_vaccinations(
    state: str = Query(default=None),
    vaccine_type: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=1000, le=50000),
):
    """Vaccinations provided to Medicaid/CHIP children under 19."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if vaccine_type:
            where.append("vaccine_type ILIKE ?")
            params.append(f"%{vaccine_type}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_vaccinations
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year, month
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/children/vaccinations/summary")
@safe_route(default_response={"rows": [], "count": 0})
def children_vaccinations_summary():
    """Summary of vaccination rates by state (latest year)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_name,
                MAX(year) AS latest_year,
                COUNT(DISTINCT vaccine_type) AS vaccine_types,
                SUM(service_count) AS total_vaccinations,
                ROUND(AVG(rate_per_1000), 1) AS avg_rate_per_1000
            FROM fact_vaccinations
            WHERE year = (SELECT MAX(year) FROM fact_vaccinations)
                AND vaccine_type = 'All'
            GROUP BY state_name
            ORDER BY avg_rate_per_1000 DESC NULLS LAST
        """).fetchall()
        cols = ["state_name", "latest_year", "vaccine_types",
                "total_vaccinations", "avg_rate_per_1000"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/children/lead-screening")
@safe_route(default_response={"rows": [], "count": 0})
def blood_lead_screening(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=5000),
):
    """Blood lead screening services for Medicaid/CHIP beneficiaries ages 1-2."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_blood_lead_screening
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year, month
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Dual Eligibility ─────────────────────────────────────────────

@router.get("/api/enrollment/dual-status")
@safe_route(default_response={"rows": [], "count": 0})
def dual_status_monthly(
    state: str = Query(default=None),
    dual_status: str = Query(default=None),
    limit: int = Query(default=2000, le=15000),
):
    """Dual eligibility status (full/partial/non-dual) by month and state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if dual_status:
            where.append("dual_status ILIKE ?")
            params.append(f"%{dual_status}%")
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_dual_status_monthly
            WHERE {' AND '.join(where)}
            ORDER BY state_name, reporting_month
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/dual-status/summary")
@safe_route(default_response={"rows": [], "count": 0})
def dual_status_summary():
    """Summary of dual eligibility by state (latest month)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            WITH latest AS (
                SELECT state_name, dual_status, enrolled_count,
                       ROW_NUMBER() OVER (PARTITION BY state_name, dual_status
                                          ORDER BY reporting_month DESC) AS rn
                FROM fact_dual_status_monthly
                WHERE enrolled_count IS NOT NULL
            )
            SELECT
                state_name,
                SUM(CASE WHEN dual_status = 'Full dual eligibility' THEN enrolled_count END) AS full_duals,
                SUM(CASE WHEN dual_status = 'Partial dual eligibility' THEN enrolled_count END) AS partial_duals,
                SUM(CASE WHEN dual_status = 'Not dually eligible' THEN enrolled_count END) AS non_duals,
                SUM(enrolled_count) AS total_enrollment,
                ROUND(100.0 * SUM(CASE WHEN dual_status ILIKE '%dual%' AND dual_status != 'Not dually eligible'
                      THEN enrolled_count END) / NULLIF(SUM(enrolled_count), 0), 1) AS pct_duals
            FROM latest WHERE rn = 1
            GROUP BY state_name
            ORDER BY pct_duals DESC NULLS LAST
        """).fetchall()
        cols = ["state_name", "full_duals", "partial_duals", "non_duals",
                "total_enrollment", "pct_duals"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Benefit Package & Program Annual ─────────────────────────────

@router.get("/api/enrollment/benefit-package")
@safe_route(default_response={"rows": [], "count": 0})
def benefit_package(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """Enrollment by benefit package type (full-scope, comprehensive, limited) by state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_benefit_package
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/enrollment/program-annual")
@safe_route(default_response={"rows": [], "count": 0})
def program_annual(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """Annual Medicaid vs CHIP enrollment summary by state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_program_annual
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Maternal Health (Extended) ────────────────────────────────────

@router.get("/api/maternal/nas-rates")
@safe_route(default_response={"rows": [], "count": 0})
def nas_rates(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=1000),
):
    """Neonatal Abstinence Syndrome (NAS) rates per 1,000 Medicaid births."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_nas_rates
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/maternal/nas-rates/summary")
@safe_route(default_response={"rows": [], "count": 0})
def nas_rates_summary():
    """NAS rates by state for the latest year."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_name, year, nas_births, rate_per_1000, data_quality
            FROM fact_nas_rates
            WHERE year = (SELECT MAX(year) FROM fact_nas_rates)
                AND rate_per_1000 IS NOT NULL
            ORDER BY rate_per_1000 DESC
        """).fetchall()
        cols = ["state_name", "year", "nas_births", "rate_per_1000", "data_quality"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/maternal/pregnant-postpartum")
@safe_route(default_response={"rows": [], "count": 0})
def pregnant_postpartum(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """Pregnant and postpartum Medicaid/CHIP beneficiaries."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_pregnant_postpartum
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/maternal/smm-extended")
@safe_route(default_response={"rows": [], "count": 0})
def smm_extended(
    state: str = Query(default=None),
    year: int = Query(default=None),
    limit: int = Query(default=500, le=2000),
):
    """Severe Maternal Morbidity (SMM) rates among Medicaid-covered deliveries 2017-2021."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if year:
            where.append("year = ?")
            params.append(year)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_smm_extended
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── CAA 2023 Enhanced FMAP ───────────────────────────────────────

@router.get("/api/expenditure/caa-fmap")
@safe_route(default_response={"rows": [], "count": 0})
def caa_fmap(
    state: str = Query(default=None),
    limit: int = Query(default=200, le=500),
):
    """CAA 2023 enhanced FMAP expenditures by state ($73.6B total)."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_caa_fmap
            WHERE {' AND '.join(where)}
            ORDER BY total_federal_caa DESC NULLS LAST
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Hospital Quality (Facility-Level) ────────────────────────────

def _hospital_quality_endpoint(table_name: str, has_condition: bool = False):
    """Factory for hospital quality measure endpoints."""
    def handler(
        state: str = Query(default=None),
        facility_id: str = Query(default=None),
        measure_id: str = Query(default=None),
        limit: int = Query(default=1000, le=50000),
    ):
        with get_cursor() as cur:
            where, params = ["1=1"], []
            if state:
                where.append("state = ?")
                params.append(state.upper())
            if facility_id:
                where.append("facility_id = ?")
                params.append(facility_id)
            if measure_id:
                where.append("measure_id = ?")
                params.append(measure_id)
            params.append(limit)
            rows = cur.execute(f"""
                SELECT * FROM {table_name}
                WHERE {' AND '.join(where)}
                ORDER BY state, facility_id
                LIMIT ?
            """, params).fetchall()
            cols = [d[0] for d in cur.description]
            return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}
    return handler


router.get("/api/hospitals/hai-facility")(
    _hospital_quality_endpoint("fact_hai_hospital2"))
router.get("/api/hospitals/complications-facility")(
    _hospital_quality_endpoint("fact_complications_hosp"))
router.get("/api/hospitals/timely-effective-facility")(
    _hospital_quality_endpoint("fact_timely_effective_hosp", has_condition=True))
router.get("/api/hospitals/unplanned-visits-facility")(
    _hospital_quality_endpoint("fact_unplanned_visits_hosp"))
router.get("/api/hospitals/psi90-facility")(
    _hospital_quality_endpoint("fact_psi90_hospital"))


# ── SNF / Nursing Home Quality ────────────────────────────────────

@router.get("/api/nursing-homes/snf-vbp")
@safe_route(default_response={"rows": [], "count": 0})
def snf_vbp(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    limit: int = Query(default=500, le=15000),
):
    """SNF Value-Based Purchasing performance (readmission rates, incentive multipliers)."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_snf_vbp
            WHERE {' AND '.join(where)}
            ORDER BY vbp_ranking
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/nursing-homes/claims-quality")
@safe_route(default_response={"rows": [], "count": 0})
def nh_claims_quality(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    measure_code: str = Query(default=None),
    limit: int = Query(default=1000, le=60000),
):
    """Nursing home claims-based quality measures (readmissions, ED visits)."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        if measure_code:
            where.append("measure_code = ?")
            params.append(measure_code)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_nh_claims_quality
            WHERE {' AND '.join(where)}
            ORDER BY state, ccn
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/nursing-homes/snf-quality")
@safe_route(default_response={"rows": [], "count": 0})
def snf_quality_provider(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    measure_code: str = Query(default=None),
    limit: int = Query(default=1000, le=50000),
):
    """SNF Quality Reporting Program measures by provider (57 measures, 14.7K facilities)."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        if measure_code:
            where.append("measure_code = ?")
            params.append(measure_code)
        params.append(limit)
        rows = cur.execute(f"""
            SELECT * FROM fact_snf_quality_provider
            WHERE {' AND '.join(where)}
            ORDER BY state, ccn
            LIMIT ?
        """, params).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/nursing-homes/state-averages")
@safe_route(default_response={"rows": [], "count": 0})
def nh_state_averages():
    """Nursing home state and national quality averages (ratings, staffing, deficiencies)."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT * FROM fact_nh_state_averages
            ORDER BY overall_rating DESC NULLS LAST
        """).fetchall()
        cols = [d[0] for d in cur.description]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ---------------------------------------------------------------------------
# HRSA FQHC Directory
# ---------------------------------------------------------------------------

@router.get("/api/providers/fqhc")
@safe_route(default_response={"rows": [], "count": 0})
def fqhc_directory_v1(
    state: str = Query(default=None),
    health_center_type: str = Query(default=None),
    limit: int = Query(default=1000, le=50000),
):
    """HRSA Federally Qualified Health Center (FQHC) site directory."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code = ?")
            params.append(state.upper())
        if health_center_type:
            where.append("center_type ILIKE ?")
            params.append(f"%{health_center_type}%")
        params.append(limit)
        rows = cur.execute(f"""
            SELECT state_code, site_name, address, city, zip_code,
                   telephone, npi, health_center_name, center_type,
                   location_setting, operator_type,
                   operating_hours_per_week, county_name, congressional_district,
                   latitude, longitude
            FROM fact_fqhc_directory
            WHERE {' AND '.join(where)}
            ORDER BY state_code, site_name
            LIMIT ?
        """, params).fetchall()
        cols = ["state", "site_name", "address", "city", "zip_code",
                "telephone", "npi", "health_center_name", "center_type",
                "location_setting", "operator_type",
                "operating_hours_per_week", "county_name", "congressional_district",
                "latitude", "longitude"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/providers/fqhc/summary")
@safe_route(default_response={"rows": [], "count": 0})
def fqhc_summary_v1():
    """FQHC site count by state."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, COUNT(*) AS site_count,
                   COUNT(DISTINCT health_center_name) AS health_center_count,
                   COUNT(DISTINCT county_name) AS county_count
            FROM fact_fqhc_directory
            WHERE state_code IS NOT NULL
            GROUP BY state_code
            ORDER BY site_count DESC
        """).fetchall()
        cols = ["state", "site_count", "health_center_count", "county_count"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ---------------------------------------------------------------------------
# CDC Vital Statistics (VSRR)
# ---------------------------------------------------------------------------

@router.get("/api/demographics/vital-stats")
@safe_route(default_response={"rows": [], "count": 0})
def vital_stats(
    state: str = Query(default=None),
    indicator: str = Query(default=None),
    year: int = Query(default=None),
    period: str = Query(default=None),
):
    """CDC VSRR provisional births, deaths, and infant deaths by state."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code = ?")
            params.append(state.upper())
        if indicator:
            where.append("indicator ILIKE ?")
            params.append(f"%{indicator}%")
        if year:
            where.append("year = ?")
            params.append(year)
        if period:
            where.append("period = ?")
            params.append(period)
        rows = cur.execute(f"""
            SELECT state_code, state_name, year, month, period,
                   indicator, data_value
            FROM fact_vital_stats
            WHERE {' AND '.join(where)}
            ORDER BY state_code, year, month
        """, params).fetchall()
        cols = ["state", "state_name", "year", "month", "period",
                "indicator", "data_value"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ---------------------------------------------------------------------------
# CDC Maternal Mortality (National, Provisional)
# ---------------------------------------------------------------------------

@router.get("/api/maternal/mortality-national")
@safe_route(default_response={"rows": [], "count": 0})
def maternal_mortality_national(
    group: str = Query(default=None),
    subgroup: str = Query(default=None),
    year: int = Query(default=None),
):
    """CDC provisional maternal mortality rates (national level, by demographics)."""
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if group:
            where.append("demographic_group = ?")
            params.append(group)
        if subgroup:
            where.append("subgroup = ?")
            params.append(subgroup)
        if year:
            where.append("year_of_death = ?")
            params.append(year)
        rows = cur.execute(f"""
            SELECT jurisdiction, demographic_group, subgroup,
                   year_of_death, month_of_death, time_period,
                   month_ending_date, maternal_deaths, live_births,
                   maternal_mortality_rate
            FROM fact_maternal_mortality_national
            WHERE {' AND '.join(where)}
            ORDER BY year_of_death, month_of_death
        """, params).fetchall()
        cols = ["jurisdiction", "demographic_group", "subgroup",
                "year_of_death", "month_of_death", "time_period",
                "month_ending_date", "maternal_deaths", "live_births",
                "maternal_mortality_rate"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── HHCAHPS ─────────────────────────────────────────────────────────────

@router.get("/api/home-health/hhcahps-provider")
@safe_route(default_response={"rows": [], "count": 0})
def hhcahps_provider(
    state: str | None = None,
    ccn: str | None = None,
    min_stars: int | None = None,
    limit: int = 500,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        if min_stars:
            where.append("summary_star_rating >= ?")
            params.append(min_stars)
        rows = cur.execute(f"""
            SELECT ccn, summary_star_rating,
                   star_professional_care, pct_professional_care,
                   star_communication, pct_communication,
                   star_medicines_safety, pct_medicines_safety,
                   star_overall_care, pct_high_rating,
                   pct_would_recommend, completed_surveys, response_rate
            FROM fact_hhcahps_provider
            WHERE {' AND '.join(where)}
            ORDER BY summary_star_rating DESC NULLS LAST
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["ccn", "summary_star_rating",
                "star_professional_care", "pct_professional_care",
                "star_communication", "pct_communication",
                "star_medicines_safety", "pct_medicines_safety",
                "star_overall_care", "pct_high_rating",
                "pct_would_recommend", "completed_surveys", "response_rate"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/home-health/hhcahps-state")
@safe_route(default_response={"rows": [], "count": 0})
def hhcahps_state(state: str | None = None):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code ILIKE ?")
            params.append(state)
        rows = cur.execute(f"""
            SELECT state_code, pct_professional_care, pct_communication,
                   pct_medicines_safety, pct_high_rating,
                   pct_would_recommend, completed_surveys, response_rate
            FROM fact_hhcahps_state
            WHERE {' AND '.join(where)}
            ORDER BY state_code
        """, params).fetchall()
        cols = ["state_code", "pct_professional_care", "pct_communication",
                "pct_medicines_safety", "pct_high_rating",
                "pct_would_recommend", "completed_surveys", "response_rate"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Hospice Directory + CAHPS ────────────────────────────────────────────

@router.get("/api/hospice/directory")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_directory(
    state: str | None = None,
    ownership: str | None = None,
    limit: int = 500,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code ILIKE ?")
            params.append(state)
        if ownership:
            where.append("ownership_type ILIKE ?")
            params.append(f"%{ownership}%")
        rows = cur.execute(f"""
            SELECT ccn, facility_name, address, city, state_code,
                   zip_code, county, telephone, cms_region,
                   ownership_type, certification_date
            FROM fact_hospice_directory
            WHERE {' AND '.join(where)}
            ORDER BY state_code, facility_name
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["ccn", "facility_name", "address", "city", "state_code",
                "zip_code", "county", "telephone", "cms_region",
                "ownership_type", "certification_date"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/hospice/cahps-state")
@safe_route(default_response={"rows": [], "count": 0})
def hospice_cahps_state(
    state: str | None = None,
    measure: str | None = None,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code ILIKE ?")
            params.append(state)
        if measure:
            where.append("measure_code ILIKE ?")
            params.append(f"%{measure}%")
        rows = cur.execute(f"""
            SELECT state_code, measure_code, measure_name,
                   score, footnote, measure_period
            FROM fact_hospice_cahps_state
            WHERE {' AND '.join(where)}
            ORDER BY state_code, measure_code
        """, params).fetchall()
        cols = ["state_code", "measure_code", "measure_name",
                "score", "footnote", "measure_period"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Medicare Spending by Claim ───────────────────────────────────────────

@router.get("/api/medicare/spending-by-claim")
@safe_route(default_response={"rows": [], "count": 0})
def medicare_spending_by_claim(
    state: str | None = None,
    facility_id: str | None = None,
    claim_type: str | None = None,
    limit: int = 500,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code ILIKE ?")
            params.append(state)
        if facility_id:
            where.append("facility_id = ?")
            params.append(facility_id)
        if claim_type:
            where.append("claim_type ILIKE ?")
            params.append(f"%{claim_type}%")
        rows = cur.execute(f"""
            SELECT facility_name, facility_id, state_code, period, claim_type,
                   avg_spending_hospital, avg_spending_state, avg_spending_national,
                   pct_spending_hospital, pct_spending_state, pct_spending_national,
                   start_date, end_date
            FROM fact_medicare_spending_claim
            WHERE {' AND '.join(where)}
            ORDER BY facility_id, period, claim_type
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["facility_name", "facility_id", "state_code", "period", "claim_type",
                "avg_spending_hospital", "avg_spending_state", "avg_spending_national",
                "pct_spending_hospital", "pct_spending_state", "pct_spending_national",
                "start_date", "end_date"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── VHA Providers ────────────────────────────────────────────────────────

@router.get("/api/vha/providers")
@safe_route(default_response={"rows": [], "count": 0})
def vha_providers(
    state: str | None = None,
    min_rating: int | None = None,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_code ILIKE ?")
            params.append(state)
        if min_rating:
            where.append("overall_rating >= ?")
            params.append(min_rating)
        rows = cur.execute(f"""
            SELECT facility_id, facility_name, address, city, state_code,
                   zip_code, county, telephone, hospital_type, ownership,
                   emergency_services, overall_rating
            FROM fact_vha_provider
            WHERE {' AND '.join(where)}
            ORDER BY state_code, facility_name
        """, params).fetchall()
        cols = ["facility_id", "facility_name", "address", "city", "state_code",
                "zip_code", "county", "telephone", "hospital_type", "ownership",
                "emergency_services", "overall_rating"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Pregnancy Outcomes ───────────────────────────────────────────────────

@router.get("/api/maternal/pregnancy-outcomes")
@safe_route(default_response={"rows": [], "count": 0})
def pregnancy_outcomes(
    state: str | None = None,
    outcome: str | None = None,
    year: int | None = None,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if outcome:
            where.append("pregnancy_outcome ILIKE ?")
            params.append(f"%{outcome}%")
        if year:
            where.append("year = ?")
            params.append(year)
        rows = cur.execute(f"""
            SELECT state_name, year, month, pregnancy_outcome,
                   service_count, rate_per_1000, data_quality
            FROM fact_pregnancy_outcomes
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year, month
            LIMIT 2000
        """, params).fetchall()
        cols = ["state_name", "year", "month", "pregnancy_outcome",
                "service_count", "rate_per_1000", "data_quality"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── CHIP Program Annual ─────────────────────────────────────────────────

@router.get("/api/chip/program-annual")
@safe_route(default_response={"rows": [], "count": 0})
def chip_program_annual(
    state: str | None = None,
    program_type: str | None = None,
    year: int | None = None,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if state:
            where.append("state_name ILIKE ?")
            params.append(f"%{state}%")
        if program_type:
            where.append("program_type ILIKE ?")
            params.append(f"%{program_type}%")
        if year:
            where.append("year = ?")
            params.append(year)
        rows = cur.execute(f"""
            SELECT state_name, year, program_type,
                   ever_enrolled, last_month_enrollment,
                   avg_monthly_enrollment, data_quality
            FROM fact_chip_program_annual
            WHERE {' AND '.join(where)}
            ORDER BY state_name, year
        """, params).fetchall()
        cols = ["state_name", "year", "program_type",
                "ever_enrolled", "last_month_enrollment",
                "avg_monthly_enrollment", "data_quality"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Well-Child Visits ────────────────────────────────────────────────────

@router.get("/api/children/well-child-visits")
@safe_route(default_response={"rows": [], "count": 0})
def well_child_visits(year: int | None = None):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if year:
            where.append("year = ?")
            params.append(year)
        rows = cur.execute(f"""
            SELECT year, geography, subpopulation_topic, subpopulation,
                   category, enrollee_count, denominator_count, pct_enrollees
            FROM fact_well_child_visits
            WHERE {' AND '.join(where)}
            ORDER BY year, geography
        """, params).fetchall()
        cols = ["year", "geography", "subpopulation_topic", "subpopulation",
                "category", "enrollee_count", "denominator_count", "pct_enrollees"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ── Financial Management National ────────────────────────────────────────

@router.get("/api/expenditure/financial-mgmt-national")
@safe_route(default_response={"rows": [], "count": 0})
def financial_mgmt_national(
    year: int | None = None,
    program: str | None = None,
):
    with get_cursor() as cur:
        where, params = ["1=1"], []
        if year:
            where.append("year = ?")
            params.append(year)
        if program:
            where.append("program ILIKE ?")
            params.append(f"%{program}%")
        rows = cur.execute(f"""
            SELECT year, program, service_category,
                   total_computable, federal_share,
                   federal_share_medicaid, state_share
            FROM fact_financial_mgmt_national
            WHERE {' AND '.join(where)}
            ORDER BY year, program, service_category
        """, params).fetchall()
        cols = ["year", "program", "service_category",
                "total_computable", "federal_share",
                "federal_share_medicaid", "state_share"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ══════════════════════════════════════════════════════════════════════
# Round 8: MSPB Hospital, Imaging Hospital, ESRD QIP, Physician
# ══════════════════════════════════════════════════════════════════════

@router.get("/api/medicare/mspb-hospital")
@safe_route(default_response={"rows": [], "count": 0})
def mspb_hospital_detail(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    limit: int = Query(default=500, le=5000),
):
    """Medicare Spending Per Beneficiary by hospital."""
    with get_cursor() as cur:
        where = ["1=1"]
        params: list = []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        rows = cur.execute(f"""
            SELECT ccn, facility_name, state, zip_code,
                   measure_id, score, start_date, end_date
            FROM fact_mspb_hospital_detail
            WHERE {' AND '.join(where)}
            ORDER BY state, facility_name
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["ccn", "facility_name", "state", "zip_code",
                "measure_id", "score", "start_date", "end_date"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/hospitals/imaging")
@safe_route(default_response={"rows": [], "count": 0})
def imaging_hospital_detail(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    measure: str = Query(default=None),
    limit: int = Query(default=500, le=5000),
):
    """Outpatient Imaging Efficiency — hospital-level measures."""
    with get_cursor() as cur:
        where = ["1=1"]
        params: list = []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        if measure:
            where.append("measure_id = ?")
            params.append(measure)
        rows = cur.execute(f"""
            SELECT ccn, facility_name, state, zip_code,
                   measure_id, measure_name, score, start_date, end_date
            FROM fact_imaging_hospital
            WHERE {' AND '.join(where)}
            ORDER BY state, facility_name
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["ccn", "facility_name", "state", "zip_code",
                "measure_id", "measure_name", "score", "start_date", "end_date"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/dialysis/esrd-qip")
@safe_route(default_response={"rows": [], "count": 0})
def esrd_qip_quality(
    state: str = Query(default=None),
    ccn: str = Query(default=None),
    limit: int = Query(default=500, le=5000),
):
    """ESRD Quality Incentive Program — dialysis facility quality."""
    with get_cursor() as cur:
        where = ["1=1"]
        params: list = []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if ccn:
            where.append("ccn = ?")
            params.append(ccn)
        rows = cur.execute(f"""
            SELECT ccn, facility_name, state, zip_code,
                   five_star, dialysis_stations, profit_or_nonprofit,
                   chain_organization, mortality_rate, survival_category,
                   hospitalization_rate, readmission_rate,
                   fistula_rate, catheter_rate, hypercalcemia_rate
            FROM fact_esrd_qip
            WHERE {' AND '.join(where)}
            ORDER BY state, facility_name
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["ccn", "facility_name", "state", "zip_code",
                "five_star", "dialysis_stations", "profit_or_nonprofit",
                "chain_organization", "mortality_rate", "survival_category",
                "hospitalization_rate", "readmission_rate",
                "fistula_rate", "catheter_rate", "hypercalcemia_rate"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/dialysis/esrd-qip/summary")
@safe_route(default_response={"rows": [], "count": 0})
def esrd_qip_summary():
    """ESRD QIP state-level summary statistics."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state,
                   COUNT(*) AS facilities,
                   AVG(five_star) AS avg_stars,
                   SUM(dialysis_stations) AS total_stations,
                   AVG(mortality_rate) AS avg_mortality,
                   AVG(hospitalization_rate) AS avg_hospitalization
            FROM fact_esrd_qip
            WHERE state IS NOT NULL
            GROUP BY state
            ORDER BY state
        """).fetchall()
        cols = ["state", "facilities", "avg_stars", "total_stations",
                "avg_mortality", "avg_hospitalization"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/providers/physicians")
@safe_route(default_response={"rows": [], "count": 0})
def physician_directory(
    state: str = Query(default=None),
    specialty: str = Query(default=None),
    npi: str = Query(default=None),
    limit: int = Query(default=500, le=5000),
):
    """Physician/Clinician directory (Doctors and Clinicians)."""
    with get_cursor() as cur:
        where = ["1=1"]
        params: list = []
        if state:
            where.append("state = ?")
            params.append(state.upper())
        if specialty:
            where.append("primary_specialty ILIKE ?")
            params.append(f"%{specialty}%")
        if npi:
            where.append("npi = ?")
            params.append(npi)
        rows = cur.execute(f"""
            SELECT npi, provider_last_name, provider_first_name,
                   credential, primary_specialty, facility_name,
                   state, city, zip_code, telehealth
            FROM fact_physician_compare
            WHERE {' AND '.join(where)}
            ORDER BY state, provider_last_name
            LIMIT ?
        """, params + [limit]).fetchall()
        cols = ["npi", "last_name", "first_name", "credential",
                "primary_specialty", "facility_name", "state",
                "city", "zip_code", "telehealth"]
        return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/providers/physicians/specialties")
@safe_route(default_response={"rows": [], "count": 0})
def physician_specialties():
    """Count of physicians by specialty."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT primary_specialty, COUNT(*) AS count
            FROM fact_physician_compare
            WHERE primary_specialty IS NOT NULL
            GROUP BY primary_specialty
            ORDER BY count DESC
        """).fetchall()
        return {"rows": [{"specialty": r[0], "count": r[1]} for r in rows]}
