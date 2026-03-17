"""
Cross-dataset insights for State Profiles.

Generates automated observations that connect data across tables —
enrollment + rates + hospitals + workforce + quality + economics.
Pure SQL, no AI needed.
"""

from fastapi import APIRouter
from server.db import get_cursor

router = APIRouter(tags=["insights"])


def _safe_query(sql: str, params: list | None = None) -> dict | None:
    """Run a query, return first row as dict, or None on error."""
    try:
        with get_cursor() as cur:
            result = cur.execute(sql, params or []).fetchone()
            if result is None:
                return None
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, result))
    except Exception:
        return None


def _safe_query_all(sql: str, params: list | None = None) -> list[dict]:
    """Run a query, return all rows as list of dicts."""
    try:
        with get_cursor() as cur:
            rows = cur.execute(sql, params or []).fetchall()
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return []


@router.get("/api/insights/{state_code}")
def get_state_insights(state_code: str):
    """Generate cross-dataset insights for a state."""
    sc = state_code.upper()
    insights = []

    # 1. Rate adequacy + HPSA correlation
    rate_data = _safe_query("""
        SELECT
            ROUND(AVG(pct_of_medicare), 1) AS avg_pct_medicare,
            COUNT(DISTINCT procedure_code) AS code_count
        FROM fact_rate_comparison
        WHERE state_code = $1
          AND pct_of_medicare > 0 AND pct_of_medicare < 1000
          AND em_category IS NOT NULL
    """, [sc])
    hpsa_data = _safe_query("""
        SELECT
            COUNT(DISTINCT hpsa_id) AS total_hpsas,
            ROUND(AVG(hpsa_score), 1) AS avg_score,
            SUM(CASE WHEN hpsa_score >= 18 THEN 1 ELSE 0 END) AS severe_count
        FROM fact_hpsa
        WHERE state_code = $1
          AND discipline = 'Primary Care'
    """, [sc])
    if rate_data and hpsa_data and rate_data.get("avg_pct_medicare") and hpsa_data.get("total_hpsas"):
        pct = rate_data["avg_pct_medicare"]
        hpsas = hpsa_data["total_hpsas"]
        severe = hpsa_data.get("severe_count", 0)
        level = "below" if pct < 80 else ("near" if pct < 100 else "above")
        if level == "below" and severe and severe > 5:
            insights.append({
                "type": "warning",
                "title": "Low Rates + Provider Shortages",
                "text": f"Medicaid pays {pct}% of Medicare for E/M codes while the state has {hpsas} primary care HPSAs ({severe} with severe scores). Low reimbursement may be exacerbating provider shortages.",
                "domains": ["rates", "workforce"],
            })
        elif level == "below":
            insights.append({
                "type": "warning",
                "title": "Below-Average Rate Adequacy",
                "text": f"Medicaid pays {pct}% of Medicare for E/M codes across {rate_data['code_count']} procedure codes — below the national median of ~85%.",
                "domains": ["rates"],
            })

    # 2. Enrollment trend + economic context
    enrollment = _safe_query("""
        SELECT
            MAX(total_enrollment) FILTER (WHERE year = (SELECT MAX(year) FROM fact_enrollment WHERE state_code = $1)) AS latest,
            MAX(total_enrollment) FILTER (WHERE year = (SELECT MAX(year) - 2 FROM fact_enrollment WHERE state_code = $1)) AS two_years_ago
        FROM fact_enrollment
        WHERE state_code = $1
    """, [sc])
    econ = _safe_query("""
        SELECT
            pct_poverty AS poverty_rate, pct_uninsured AS uninsured_rate
        FROM fact_acs_state
        WHERE state_code = $1
    """, [sc])
    if enrollment and econ and enrollment.get("latest") and enrollment.get("two_years_ago"):
        change_pct = round((enrollment["latest"] - enrollment["two_years_ago"]) / enrollment["two_years_ago"] * 100, 1)
        pov = econ.get("poverty_rate")
        direction = "grew" if change_pct > 0 else "declined"
        if pov and pov > 15 and change_pct > 0:
            insights.append({
                "type": "info",
                "title": "Enrollment Growth + High Poverty",
                "text": f"Medicaid enrollment {direction} {abs(change_pct)}% over 2 years in a state with {pov}% poverty rate. Demand pressure likely to continue.",
                "domains": ["enrollment", "economic"],
            })
        elif abs(change_pct) > 5:
            insights.append({
                "type": "info",
                "title": f"Enrollment {'Surge' if change_pct > 0 else 'Decline'}",
                "text": f"Medicaid enrollment {direction} {abs(change_pct)}% over the last 2 years.",
                "domains": ["enrollment"],
            })

    # 3. Hospital Medicaid dependence + safety net
    hospital = _safe_query("""
        SELECT
            COUNT(*) AS total_hospitals,
            ROUND(AVG(medicaid_day_pct), 1) AS avg_medicaid_pct,
            SUM(CASE WHEN medicaid_day_pct > 25 THEN 1 ELSE 0 END) AS high_medicaid_hospitals,
            ROUND(SUM(uncompensated_care_cost) / 1e6, 0) AS total_uncompensated_m
        FROM fact_hospital_cost
        WHERE state_code = $1
          AND report_year = (SELECT MAX(report_year) FROM fact_hospital_cost WHERE state_code = $1)
    """, [sc])
    if hospital and hospital.get("total_hospitals"):
        high_med = hospital.get("high_medicaid_hospitals", 0)
        total = hospital["total_hospitals"]
        uncomp = hospital.get("total_uncompensated_m")
        if high_med and high_med > total * 0.3:
            text = f"{high_med} of {total} hospitals ({round(high_med/total*100)}%) have Medicaid patients making up >25% of patient days"
            if uncomp and uncomp > 500:
                text += f", with ${uncomp:,.0f}M in total uncompensated care costs"
            text += "."
            insights.append({
                "type": "info",
                "title": "High Hospital Medicaid Dependence",
                "text": text,
                "domains": ["hospitals"],
            })

    # 4. HCBS waitlist + LTSS spending
    waitlist = _safe_query("""
        SELECT total_waiting, idd_waiting
        FROM fact_hcbs_waitlist
        WHERE state_code = $1
    """, [sc])
    ltss = _safe_query("""
        SELECT
            hcbs_pct, institutional_pct, year
        FROM fact_ltss_expenditure
        WHERE state_code = $1
        ORDER BY year DESC LIMIT 1
    """, [sc])
    if waitlist and waitlist.get("total_waiting") and waitlist["total_waiting"] > 1000:
        text = f"{waitlist['total_waiting']:,} people on HCBS waitlists"
        if waitlist.get("idd_waiting"):
            text += f" ({waitlist['idd_waiting']:,} I/DD)"
        if ltss and ltss.get("hcbs_pct"):
            text += f". HCBS accounts for {ltss['hcbs_pct']}% of LTSS spending"
            if ltss["hcbs_pct"] < 50:
                text += " — still below the 50% rebalancing benchmark"
        text += "."
        insights.append({
            "type": "warning",
            "title": "Significant HCBS Waitlists",
            "text": text,
            "domains": ["enrollment", "hospitals"],
        })

    # 5. Nursing home quality
    nh = _safe_query("""
        SELECT
            COUNT(*) AS total_facilities,
            ROUND(AVG(CASE WHEN overall_rating IS NOT NULL THEN overall_rating END), 1) AS avg_rating,
            SUM(CASE WHEN overall_rating <= 2 THEN 1 ELSE 0 END) AS low_rated,
            SUM(CASE WHEN abuse_flag = true THEN 1 ELSE 0 END) AS abuse_flagged
        FROM fact_five_star
        WHERE state_code = $1
    """, [sc])
    if nh and nh.get("total_facilities") and nh["total_facilities"] > 10:
        low = nh.get("low_rated", 0)
        total = nh["total_facilities"]
        avg = nh.get("avg_rating")
        if low and low > total * 0.3:
            text = f"{low} of {total} nursing homes ({round(low/total*100)}%) have 1-2 star ratings (avg: {avg} stars)"
            abuse = nh.get("abuse_flagged", 0)
            if abuse:
                text += f". {abuse} facilit{'y' if abuse == 1 else 'ies'} flagged for abuse"
            text += "."
            insights.append({
                "type": "warning",
                "title": "Nursing Home Quality Concerns",
                "text": text,
                "domains": ["quality"],
            })

    # 6. Drug spending
    drug_data = _safe_query("""
        SELECT
            ROUND(SUM(total_amount_reimbursed) / 1e6, 0) AS total_drug_spend_m,
            COUNT(DISTINCT ndc) AS unique_drugs
        FROM fact_drug_utilization
        WHERE state_code = $1
          AND year = (SELECT MAX(year) FROM fact_drug_utilization WHERE state_code = $1)
    """, [sc])
    if drug_data and drug_data.get("total_drug_spend_m") and drug_data["total_drug_spend_m"] > 100:
        insights.append({
            "type": "info",
            "title": "Prescription Drug Spending",
            "text": f"${drug_data['total_drug_spend_m']:,.0f}M in Medicaid drug reimbursement across {drug_data['unique_drugs']:,} unique NDCs in the most recent year.",
            "domains": ["pharmacy"],
        })

    # 7. Unwinding impact
    unwinding = _safe_query("""
        SELECT
            SUM(terminated_count) AS total_terminated,
            MAX(terminated_pct) AS max_pct
        FROM fact_unwinding
        WHERE state_code = $1
          AND metric ILIKE '%disenroll%'
    """, [sc])
    if unwinding and unwinding.get("total_terminated") and unwinding["total_terminated"] > 10000:
        insights.append({
            "type": "info",
            "title": "Unwinding Impact",
            "text": f"{unwinding['total_terminated']:,} Medicaid enrollees terminated during unwinding.",
            "domains": ["enrollment"],
        })

    # 8. Opioid prescribing
    opioid = _safe_query("""
        SELECT
            o.opioid_prescribing_rate,
            o.opioid_rate_1y_change,
            o.year
        FROM fact_opioid_prescribing o
        JOIN dim_state d ON d.state_name = o.geo_desc
        WHERE d.state_code = $1
          AND o.geo_level = 'State'
          AND o.plan_type = 'All'
          AND o.year = (
              SELECT MAX(o2.year)
              FROM fact_opioid_prescribing o2
              JOIN dim_state d2 ON d2.state_name = o2.geo_desc
              WHERE d2.state_code = $1 AND o2.geo_level = 'State'
          )
    """, [sc])
    national_opioid = _safe_query("""
        SELECT opioid_prescribing_rate
        FROM fact_opioid_prescribing
        WHERE geo_level = 'National'
          AND plan_type = 'All'
          AND year = (SELECT MAX(year) FROM fact_opioid_prescribing WHERE geo_level = 'National')
    """)
    if opioid and opioid.get("opioid_prescribing_rate") and national_opioid:
        state_rate = opioid["opioid_prescribing_rate"]
        nat_rate = national_opioid.get("opioid_prescribing_rate", 0)
        if state_rate and nat_rate and state_rate > nat_rate * 1.2:
            insights.append({
                "type": "warning",
                "title": "Elevated Opioid Prescribing",
                "text": f"Medicaid opioid prescribing rate is {state_rate}%, {round((state_rate/nat_rate - 1) * 100)}% above the national average of {nat_rate}%.",
                "domains": ["pharmacy"],
            })

    # 9. Nursing home deficiency patterns
    deficiency = _safe_query("""
        SELECT
            COUNT(*) AS total_deficiencies,
            SUM(CASE WHEN severity_level >= 3 THEN 1 ELSE 0 END) AS serious_count,
            COUNT(DISTINCT federal_provider_number) AS facilities_cited
        FROM fact_nh_deficiency
        WHERE state_code = $1
    """, [sc])
    if deficiency and deficiency.get("total_deficiencies") and deficiency["total_deficiencies"] > 100:
        serious = deficiency.get("serious_count", 0)
        total = deficiency["total_deficiencies"]
        facilities = deficiency.get("facilities_cited", 0)
        if serious and serious > total * 0.1:
            insights.append({
                "type": "warning",
                "title": "Nursing Home Deficiency Pattern",
                "text": f"{total:,} survey deficiencies across {facilities:,} nursing homes, with {serious:,} ({round(serious/total*100)}%) at serious harm level or above.",
                "domains": ["quality"],
            })

    # 10. Managed care penetration
    mc = _safe_query("""
        SELECT
            total_enrollment, mc_enrollment
        FROM fact_enrollment
        WHERE state_code = $1
        ORDER BY year DESC, month DESC NULLS LAST
        LIMIT 1
    """, [sc])
    if mc and mc.get("total_enrollment") and mc.get("mc_enrollment"):
        mc_pct = round(mc["mc_enrollment"] / mc["total_enrollment"] * 100, 1)
        if mc_pct > 85:
            insights.append({
                "type": "info",
                "title": "High Managed Care Penetration",
                "text": f"{mc_pct}% of Medicaid enrollees are in managed care plans. Rate adequacy analysis should account for MCO negotiated rates, which may differ from FFS schedules.",
                "domains": ["enrollment"],
            })

    # 11. Maternal health signals
    maternal = _safe_query("""
        SELECT
            measure_name, rate
        FROM fact_maternal_health
        WHERE state_code = $1
          AND measure_name ILIKE '%severe maternal morbidity%'
        ORDER BY year DESC LIMIT 1
    """, [sc])
    if maternal and maternal.get("rate"):
        insights.append({
            "type": "warning" if maternal["rate"] > 2.0 else "info",
            "title": "Maternal Health",
            "text": f"Severe maternal morbidity rate: {maternal['rate']}%. {'' if maternal['rate'] <= 2.0 else 'This exceeds the national benchmark and warrants attention to prenatal care access and hospital capacity.'}",
            "domains": ["quality"],
        })

    return {
        "state_code": sc,
        "insights": insights,
        "count": len(insights),
    }


@router.get("/api/sdoh/{state_code}")
def get_state_sdoh(state_code: str):
    """Social Determinants of Health indicators for a state."""
    sc = state_code.upper()
    result: dict = {"state_code": sc}

    # 1. Average ADI national rank
    adi = _safe_query("""
        SELECT
            ROUND(AVG(adi_national_rank), 1) AS avg_adi_rank,
            COUNT(*) AS block_group_count
        FROM fact_adi_block_group
        WHERE state_code = $1
          AND adi_national_rank IS NOT NULL
    """, [sc])
    result["adi"] = {
        "avg_national_rank": adi["avg_adi_rank"] if adi else None,
        "block_group_count": adi["block_group_count"] if adi else 0,
    }

    # 2. Food desert tracts (LILA 1 and 10 mile threshold)
    #    food_access_research_atlas uses full state names — join via dim_state
    food = _safe_query("""
        SELECT
            COUNT(*) AS total_tracts,
            SUM(CASE WHEN "LILATracts_1And10" = 1 THEN 1 ELSE 0 END) AS food_desert_tracts,
            ROUND(AVG("PovertyRate"), 1) AS avg_poverty_rate
        FROM fact_food_access_research_atlas f
        JOIN dim_state d ON d.state_name = f."State"
        WHERE d.state_code = $1
    """, [sc])
    result["food_access"] = {
        "total_tracts": food["total_tracts"] if food else 0,
        "food_desert_tracts": food["food_desert_tracts"] if food else 0,
        "avg_poverty_rate": food["avg_poverty_rate"] if food else None,
    }

    # 3. Dental HPSA count (designated only)
    dental = _safe_query("""
        SELECT
            COUNT(DISTINCT "HPSA ID") AS designated_count,
            ROUND(AVG("HPSA Score"), 1) AS avg_score
        FROM fact_dental_hpsa
        WHERE "Primary State Abbreviation" = $1
          AND "HPSA Status" = 'Designated'
    """, [sc])
    result["dental_hpsa"] = {
        "designated_count": dental["designated_count"] if dental else 0,
        "avg_score": dental["avg_score"] if dental else None,
    }

    # 4. Mental health HPSA count (designated only)
    mh = _safe_query("""
        SELECT
            COUNT(DISTINCT "HPSA ID") AS designated_count,
            ROUND(AVG("HPSA Score"), 1) AS avg_score
        FROM fact_mental_health_hpsa
        WHERE "Primary State Abbreviation" = $1
          AND "HPSA Status" = 'Designated'
    """, [sc])
    result["mental_health_hpsa"] = {
        "designated_count": mh["designated_count"] if mh else 0,
        "avg_score": mh["avg_score"] if mh else None,
    }

    # 5. MUA/MUP count (designated only)
    mua = _safe_query("""
        SELECT
            COUNT(DISTINCT "MUA/P ID") AS designated_count,
            ROUND(AVG("IMU Score"), 1) AS avg_imu_score
        FROM fact_mua_mup
        WHERE "State Abbreviation" = $1
          AND "MUA/P Status Description" = 'Designated'
    """, [sc])
    result["mua_mup"] = {
        "designated_count": mua["designated_count"] if mua else 0,
        "avg_imu_score": mua["avg_imu_score"] if mua else None,
    }

    return result
