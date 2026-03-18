"""
System Dynamics API routes.

Stock-flow simulation endpoints for enrollment, provider capacity,
workforce, HCBS, and integrated policy simulation. Each endpoint
builds a model from the data lake, applies interventions, and returns
time-series projections.
"""

import hashlib
import json
import time
from collections import OrderedDict
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter(tags=["dynamics"])

# ---------------------------------------------------------------------------
# Cache (same pattern as intelligence.py)
# ---------------------------------------------------------------------------

_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_CACHE_TTL = 3600
_CACHE_MAX = 100


def _cache_key(data: dict) -> str:
    s = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(s.encode()).hexdigest()[:20]


def _cache_get(key: str) -> Optional[dict]:
    now = time.time()
    if key in _cache and now - _cache[key][0] < _CACHE_TTL:
        return _cache[key][1]
    return None


def _cache_put(key: str, value: dict) -> None:
    _cache[key] = (time.time(), value)
    if len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class EnrollmentRequest(BaseModel):
    state_code: str
    horizon_months: int = Field(default=36, ge=6, le=120)
    unemployment_delta: float = Field(default=0.0, ge=-5, le=10)
    policy_shock: Optional[str] = None  # "expansion", "tightening", "redetermination"


class ProviderRequest(BaseModel):
    state_code: str
    horizon_months: int = Field(default=36, ge=6, le=120)
    rate_change_pct: float = Field(default=0.0, ge=-50, le=100)


class WorkforceRequest(BaseModel):
    state_code: str
    horizon_months: int = Field(default=60, ge=6, le=120)
    wage_change_dollars: float = Field(default=0.0, ge=-5, le=10)


class HcbsRequest(BaseModel):
    state_code: str
    horizon_months: int = Field(default=60, ge=6, le=120)
    funding_increase_pct: float = Field(default=0.0, ge=0, le=50)


class InterventionItem(BaseModel):
    type: str  # rate_change, wage_increase, hcbs_funding, unemployment_shock
    value: float
    start_month: int = 0
    duration_months: Optional[int] = None
    service_type: Optional[str] = None


class PolicySimulatorRequest(BaseModel):
    state_code: str
    horizon_months: int = Field(default=60, ge=12, le=120)
    interventions: list[InterventionItem] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Helper: convert SDResult dataclass to dict
# ---------------------------------------------------------------------------


def _result_to_dict(result) -> dict:
    return {
        "model_type": result.model_type,
        "months": result.months,
        "parameters_used": result.parameters_used,
        "calibration_sources": result.calibration_sources,
        "warnings": result.warnings,
    }


# ---------------------------------------------------------------------------
# 1. Enrollment stock-flow model
# ---------------------------------------------------------------------------


@router.post("/api/dynamics/enrollment")
@safe_route(default_response={})
async def dynamics_enrollment(req: EnrollmentRequest):
    """Enrollment stock-flow model with economic and policy shocks."""
    key = _cache_key(req.model_dump())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from server.engines.system_dynamics import Intervention, build_enrollment_model

    interventions = []
    if req.unemployment_delta != 0:
        interventions.append(Intervention(
            type="unemployment_shock",
            value=req.unemployment_delta,
            start_month=0,
        ))
    if req.policy_shock:
        shock_values = {"expansion": 15, "tightening": -10, "redetermination": -5}
        interventions.append(Intervention(
            type="policy_shock",
            value=shock_values.get(req.policy_shock, 0),
            start_month=0,
        ))

    with get_cursor() as cur:
        model = build_enrollment_model(cur, req.state_code.upper())
        result = model.solve(req.horizon_months, interventions)

    response = {
        "state_code": req.state_code.upper(),
        "horizon_months": req.horizon_months,
        **_result_to_dict(result),
    }

    _cache_put(key, response)
    return response


# ---------------------------------------------------------------------------
# 2. Provider capacity model
# ---------------------------------------------------------------------------


@router.post("/api/dynamics/provider")
@safe_route(default_response={})
async def dynamics_provider(req: ProviderRequest):
    """Provider capacity stock-flow model with rate sensitivity."""
    key = _cache_key(req.model_dump())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from server.engines.system_dynamics import Intervention, build_provider_model

    interventions = []
    if req.rate_change_pct != 0:
        interventions.append(Intervention(
            type="rate_change",
            value=req.rate_change_pct,
            start_month=0,
        ))

    with get_cursor() as cur:
        model = build_provider_model(cur, req.state_code.upper())
        result = model.solve(req.horizon_months, interventions)

    response = {
        "state_code": req.state_code.upper(),
        "horizon_months": req.horizon_months,
        **_result_to_dict(result),
    }

    _cache_put(key, response)
    return response


# ---------------------------------------------------------------------------
# 3. Workforce model
# ---------------------------------------------------------------------------


@router.post("/api/dynamics/workforce")
@safe_route(default_response={})
async def dynamics_workforce(req: WorkforceRequest):
    """Workforce stock-flow model with wage sensitivity."""
    key = _cache_key(req.model_dump())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from server.engines.system_dynamics import Intervention, build_workforce_model

    interventions = []
    if req.wage_change_dollars != 0:
        interventions.append(Intervention(
            type="wage_increase",
            value=req.wage_change_dollars,
            start_month=0,
        ))

    with get_cursor() as cur:
        model = build_workforce_model(cur, req.state_code.upper())
        result = model.solve(req.horizon_months, interventions)

    response = {
        "state_code": req.state_code.upper(),
        "horizon_months": req.horizon_months,
        **_result_to_dict(result),
    }

    _cache_put(key, response)
    return response


# ---------------------------------------------------------------------------
# 4. HCBS model
# ---------------------------------------------------------------------------


@router.post("/api/dynamics/hcbs")
@safe_route(default_response={})
async def dynamics_hcbs(req: HcbsRequest):
    """HCBS stock-flow model with funding sensitivity."""
    key = _cache_key(req.model_dump())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from server.engines.system_dynamics import Intervention, build_hcbs_model

    interventions = []
    if req.funding_increase_pct != 0:
        interventions.append(Intervention(
            type="hcbs_funding",
            value=req.funding_increase_pct,
            start_month=0,
        ))

    with get_cursor() as cur:
        model = build_hcbs_model(cur, req.state_code.upper())
        result = model.solve(req.horizon_months, interventions)

    response = {
        "state_code": req.state_code.upper(),
        "horizon_months": req.horizon_months,
        **_result_to_dict(result),
    }

    _cache_put(key, response)
    return response


# ---------------------------------------------------------------------------
# 5. Integrated policy simulator
# ---------------------------------------------------------------------------


@router.post("/api/dynamics/policy-simulator")
@safe_route(default_response={})
async def dynamics_policy_simulator(req: PolicySimulatorRequest):
    """Integrated system dynamics model: baseline vs scenario with feedback loops."""
    key = _cache_key(req.model_dump())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    from server.engines.system_dynamics import Intervention, build_integrated_model

    # Convert request interventions to engine Intervention dataclass
    interventions = [
        Intervention(
            type=item.type,
            value=item.value,
            start_month=item.start_month,
            duration_months=item.duration_months,
            service_type=item.service_type,
        )
        for item in req.interventions
    ]

    with get_cursor() as cur:
        model = build_integrated_model(cur, req.state_code.upper())

        # Run baseline (no interventions)
        baseline_result = model.solve(req.horizon_months, [])

        # Run scenario (with interventions)
        scenario_result = model.solve(req.horizon_months, interventions)

    baseline = _result_to_dict(baseline_result)
    scenario = _result_to_dict(scenario_result)

    # Compute impact summary: deltas between final-month values
    impact_summary = {}
    baseline_months = baseline.get("months", [])
    scenario_months = scenario.get("months", [])

    if baseline_months and scenario_months:
        baseline_final = baseline_months[-1]
        scenario_final = scenario_months[-1]

        for metric_key in scenario_final:
            if metric_key == "month":
                continue
            b_val = baseline_final.get(metric_key)
            s_val = scenario_final.get(metric_key)
            if isinstance(b_val, (int, float)) and isinstance(s_val, (int, float)):
                abs_delta = s_val - b_val
                pct_delta = ((s_val - b_val) / b_val * 100) if b_val != 0 else None
                impact_summary[metric_key] = {
                    "baseline": b_val,
                    "scenario": s_val,
                    "absolute_delta": round(abs_delta, 2),
                    "percent_delta": round(pct_delta, 2) if pct_delta is not None else None,
                }

    # Identify active feedback loops
    feedback_loops_active = []
    if baseline_months and scenario_months and len(baseline_months) > 1:
        # A feedback loop is active if a cross-model coupling changed values
        # Check each month for divergence across coupled metrics
        coupling_pairs = [
            ("enrollment", "provider_capacity", "Enrollment -> Provider Load"),
            ("provider_capacity", "wait_time", "Provider Capacity -> Access"),
            ("wage_level", "workforce_supply", "Wage -> Workforce Supply"),
            ("workforce_supply", "hcbs_served", "Workforce -> HCBS Capacity"),
            ("enrollment", "expenditure", "Enrollment -> Expenditure"),
            ("hcbs_served", "institutional_census", "HCBS Diversion -> Institutional"),
        ]
        for source_key, target_key, loop_name in coupling_pairs:
            # Check if both metrics exist and diverged
            if len(scenario_months) > 0:
                final_b = baseline_months[-1]
                final_s = scenario_months[-1]
                source_in_b = source_key in final_b
                target_in_b = target_key in final_b
                source_in_s = source_key in final_s
                target_in_s = target_key in final_s
                if source_in_b and target_in_b and source_in_s and target_in_s:
                    source_delta = final_s.get(source_key, 0) - final_b.get(source_key, 0)
                    target_delta = final_s.get(target_key, 0) - final_b.get(target_key, 0)
                    if abs(source_delta) > 0 and abs(target_delta) > 0:
                        feedback_loops_active.append({
                            "name": loop_name,
                            "source_metric": source_key,
                            "target_metric": target_key,
                            "source_delta": round(source_delta, 2),
                            "target_delta": round(target_delta, 2),
                        })

    # Collect parameters and calibration sources from both runs
    parameters_used = {
        "baseline": baseline.get("parameters_used", {}),
        "scenario": scenario.get("parameters_used", {}),
    }
    calibration_sources = list(set(
        baseline.get("calibration_sources", [])
        + scenario.get("calibration_sources", [])
    ))
    warnings = list(set(
        baseline.get("warnings", [])
        + scenario.get("warnings", [])
    ))

    response = {
        "state_code": req.state_code.upper(),
        "horizon_months": req.horizon_months,
        "baseline": baseline,
        "scenario": scenario,
        "impact_summary": impact_summary,
        "feedback_loops_active": feedback_loops_active,
        "parameters_used": parameters_used,
        "calibration_sources": calibration_sources,
        "warnings": warnings,
    }

    _cache_put(key, response)
    return response
