"""Enrollment routes — eligibility, ACA expansion, unwinding, managed care plans."""

from fastapi import APIRouter, Query
from server.db import get_cursor

router = APIRouter()


@router.get("/api/enrollment/eligibility/{state_code}")
async def eligibility(state_code: str):
    """Get monthly eligibility and enrollment data for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT reporting_period, expansion_state, data_status,
                   new_applications, medicaid_eligible_at_app, chip_eligible_at_app,
                   total_determinations, total_medicaid_chip_enrollment,
                   total_medicaid_enrollment, total_chip_enrollment,
                   adult_medicaid_enrollment, child_enrollment
            FROM fact_eligibility
            WHERE state_code = $1
            ORDER BY reporting_period
        """, [state_code]).fetchall()
        columns = ["reporting_period", "expansion_state", "data_status",
                    "new_applications", "medicaid_eligible_at_app", "chip_eligible_at_app",
                    "total_determinations", "total_medicaid_chip_enrollment",
                    "total_medicaid_enrollment", "total_chip_enrollment",
                    "adult_medicaid_enrollment", "child_enrollment"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/enrollment/expansion/{state_code}")
async def expansion_enrollment(state_code: str):
    """Get ACA expansion (new adult group VIII) enrollment for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT enrollment_year, enrollment_month,
                   total_medicaid_enrollees, viii_group_enrollees,
                   viii_newly_eligible, viii_not_newly_eligible
            FROM fact_new_adult
            WHERE state_code = $1
            ORDER BY enrollment_year, enrollment_month
        """, [state_code]).fetchall()
        columns = ["enrollment_year", "enrollment_month",
                    "total_medicaid_enrollees", "viii_group_enrollees",
                    "viii_newly_eligible", "viii_not_newly_eligible"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/enrollment/unwinding/{state_code}")
async def unwinding(state_code: str):
    """Get post-PHE unwinding/redetermination outcomes for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT metric, time_period,
                   terminated_count, terminated_pct,
                   cumulative_terminated, cumulative_terminated_pct
            FROM fact_unwinding
            WHERE state_code = $1
            ORDER BY time_period, metric
        """, [state_code]).fetchall()
        columns = ["metric", "time_period",
                    "terminated_count", "terminated_pct",
                    "cumulative_terminated", "cumulative_terminated_pct"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/enrollment/managed-care-plans/{state_code}")
async def managed_care_plans(state_code: str):
    """Get managed care plan enrollment details for a state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT program_name, plan_name, parent_org, geographic_region,
                   medicaid_only_enrollment, dual_enrollment, total_enrollment, year
            FROM fact_mc_enrollment
            WHERE state_code = $1
            ORDER BY total_enrollment DESC NULLS LAST
        """, [state_code]).fetchall()
        columns = ["program_name", "plan_name", "parent_org", "geographic_region",
                    "medicaid_only_enrollment", "dual_enrollment", "total_enrollment", "year"]
        return [dict(zip(columns, r)) for r in rows]
