"""
System Dynamics Engine - Stock-Flow Models for Medicaid Policy Modeling

Uses scipy.integrate.solve_ivp for stock-flow simulation of enrollment
pipelines, provider networks, workforce supply, HCBS transitions, and
integrated cross-domain models with policy interventions.

Follows the caseload_forecast.py pattern (dataclasses, get_cursor, fallbacks).

Usage:
    from server.engines.system_dynamics import build_enrollment_model
    from server.db import get_cursor
    with get_cursor() as cur:
        model = build_enrollment_model(cur, "FL")
        result = model.solve(36, [Intervention("unemployment_shock", 0.03, 6)])
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Callable
import numpy as np
from scipy.integrate import solve_ivp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Core abstractions
# ---------------------------------------------------------------------------

@dataclass
class Stock:
    name: str; initial: float; description: str = ""; unit: str = ""
    min_value: float = 0.0

@dataclass
class Parameter:
    name: str; value: float; source: str = "default"; description: str = ""

@dataclass
class Intervention:
    type: str        # rate_change, wage_increase, hcbs_funding, unemployment_shock, policy_shock
    value: float     # magnitude
    start_month: int = 0
    duration_months: int | None = None  # None = permanent
    service_type: str | None = None

@dataclass
class SDResult:
    months: list[dict] = field(default_factory=list)
    parameters_used: dict = field(default_factory=dict)
    calibration_sources: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    model_type: str = "unknown"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iv(intervention: Intervention, t: float) -> float:
    """Return intervention magnitude at time t, or 0 if inactive."""
    if t < intervention.start_month:
        return 0.0
    if intervention.duration_months is not None and t > intervention.start_month + intervention.duration_months:
        return 0.0
    return intervention.value

def _sum_iv(interventions: list[Intervention], t: float, itype: str) -> float:
    """Sum active intervention values of a given type at time t."""
    return sum(_iv(iv, t) for iv in interventions if iv.type == itype)

def _c(val: float, floor: float = 0.001) -> float:
    """Clamp to prevent negatives and log(0)."""
    return max(val, floor)

def _logistic(x: float, mid: float, k: float = 10.0) -> float:
    """Logistic sigmoid [0,1]."""
    z = max(min(k * (x - mid), 50.0), -50.0)
    return 1.0 / (1.0 + np.exp(-z))

def _q(cur, sql, params=None, default=None):
    """Safe query with fallback."""
    try:
        cur.execute(sql, params or [])
        return cur.fetchone() or default
    except Exception:
        return default

# ---------------------------------------------------------------------------
# SDModel
# ---------------------------------------------------------------------------

class SDModel:
    """Stock-flow system dynamics model solved via scipy solve_ivp."""

    def __init__(self, stocks: list[Stock], parameters: list[Parameter],
                 build_dydt_fn: Callable, model_type: str = "unknown",
                 calibration_sources: list[str] | None = None,
                 warnings: list[str] | None = None):
        self.stocks = stocks
        self.parameters = parameters
        self.build_dydt_fn = build_dydt_fn
        self.model_type = model_type
        self.calibration_sources = calibration_sources or []
        self.warnings = warnings or []
        self._params_dict = {p.name: p.value for p in parameters}

    def solve(self, horizon_months: int = 36,
              interventions: list[Intervention] | None = None) -> SDResult:
        """Solve ODE system. RK45 first, fallback to Radau if stiff."""
        interventions = interventions or []
        y0 = np.array([s.initial for s in self.stocks], dtype=np.float64)
        t_eval = np.arange(0, horizon_months + 1, 1, dtype=np.float64)
        mins = np.array([s.min_value for s in self.stocks], dtype=np.float64)

        def wrapper(t, y):
            return self.build_dydt_fn(t, np.maximum(y, mins + 0.001),
                                      self._params_dict, interventions)

        warns = list(self.warnings)
        sol = None
        for method in ("RK45", "Radau"):
            try:
                sol = solve_ivp(wrapper, (0.0, float(horizon_months)), y0,
                                method=method, t_eval=t_eval, max_step=1.0,
                                rtol=1e-6, atol=1e-8)
                if sol.success:
                    break
                warns.append(f"{method} failed: {sol.message}")
                sol = None
            except Exception as e:
                warns.append(f"{method} exception: {e}")
                sol = None

        if sol is None or not sol.success:
            warns.append("Solver failed. Returning flat line.")
            return self._flat(horizon_months, warns)

        months = []
        for i, tv in enumerate(sol.t):
            snap = {"t": int(round(tv))}
            for j, s in enumerate(self.stocks):
                snap[s.name] = max(float(sol.y[j, i]), s.min_value)
            months.append(snap)

        return SDResult(months=months, model_type=self.model_type,
                        parameters_used={p.name: {"value": p.value, "source": p.source}
                                         for p in self.parameters},
                        calibration_sources=list(self.calibration_sources),
                        warnings=warns)

    def _flat(self, horizon: int, warns: list[str]) -> SDResult:
        months = [{"t": t, **{s.name: s.initial for s in self.stocks}}
                  for t in range(horizon + 1)]
        return SDResult(months=months, model_type=self.model_type,
                        parameters_used={p.name: {"value": p.value, "source": p.source}
                                         for p in self.parameters},
                        calibration_sources=list(self.calibration_sources),
                        warnings=warns)

# ---------------------------------------------------------------------------
# 1. Enrollment Model
# ---------------------------------------------------------------------------

def _cal_enrollment(cur, sc):
    cal, src = {}, []
    row = _q(cur, "SELECT total_enrollment FROM fact_enrollment WHERE state_code=$1 ORDER BY year DESC, month DESC NULLS LAST LIMIT 1", [sc])
    if row and row[0]:
        cal["enrolled"] = float(row[0]); src.append("lake:fact_enrollment")
    else:
        cal["enrolled"] = 3_000_000; src.append("national_avg:enrollment_fallback")
    row = _q(cur, "SELECT AVG(disenrolled)/NULLIF(AVG(total_enrollment),0) FROM fact_unwinding WHERE state_code=$1", [sc])
    if row and row[0] and row[0] > 0:
        cal["disenroll_rate"] = min(float(row[0]), 0.10); src.append("lake:fact_unwinding")
    else:
        cal["disenroll_rate"] = 0.02; src.append("literature:CMS_2024_disenroll_default")
    row = _q(cur, "SELECT population_below_200pct_fpl FROM fact_acs_state WHERE state_code=$1 ORDER BY year DESC LIMIT 1", [sc])
    if row and row[0]:
        cal["eligible_pool"] = float(row[0]) * 0.6; src.append("lake:fact_acs_state")
    else:
        cal["eligible_pool"] = cal["enrolled"] * 1.3; src.append("national_avg:eligible_pool_fallback")
    return cal, src

def build_enrollment_model(cur, state_code: str, overrides: dict | None = None) -> SDModel:
    """4-stock enrollment pipeline: eligible_pool -> processing -> enrolled -> disenrolled."""
    sc = state_code.upper()
    cal, src = _cal_enrollment(cur, sc)
    ov = overrides or {}
    enrolled = ov.get("enrolled", cal["enrolled"])
    eligible = ov.get("eligible_pool", cal["eligible_pool"])
    dis_rate = ov.get("disenroll_rate", cal["disenroll_rate"])

    stocks = [
        Stock("eligible_pool", eligible, "Eligible but not enrolled", "persons"),
        Stock("processing_queue", eligible * 0.01, "Applications in processing", "persons"),
        Stock("enrolled", enrolled, "Currently enrolled", "persons"),
        Stock("disenrolled", 0.0, "Cumulative disenrollments", "persons"),
    ]
    params = [
        Parameter("application_rate", 0.03, "literature:CMS_2024"),
        Parameter("determination_rate", 0.60, "literature:CMS_2024"),
        Parameter("approval_prob", 0.75, "national_avg"),
        Parameter("disenroll_rate", dis_rate, "lake"),
        Parameter("population_growth", eligible * 0.001, "literature:Census_2024"),
        Parameter("unemployment_elasticity", 0.5, "literature:Sommers_2012"),
    ]

    def dydt(t, y, p, ivs):
        el, pr, en = _c(y[0]), _c(y[1]), _c(y[2])
        econ = 1.0 + p["unemployment_elasticity"] * _sum_iv(ivs, t, "unemployment_shock")
        pol = _sum_iv(ivs, t, "policy_shock")
        ar = p["application_rate"] * econ * (1.0 + pol)
        dr, ap, di = p["determination_rate"], p["approval_prob"], p["disenroll_rate"]
        return [
            -ar * el + p["population_growth"] + di * en * 0.3,
            ar * el - dr * pr,
            ap * dr * pr - di * en,
            di * en,
        ]

    return SDModel(stocks, params, dydt, "enrollment", src)

# ---------------------------------------------------------------------------
# 2. Provider Model
# ---------------------------------------------------------------------------

def _cal_provider(cur, sc):
    cal, src = {}, []
    row = _q(cur, "SELECT COUNT(*) FROM fact_nppes_provider WHERE state_code=$1 AND entity_type_code='1'", [sc])
    if row and row[0] and int(row[0]) > 0:
        cal["provider_count"] = int(row[0]); src.append("lake:fact_nppes_provider")
    else:
        row2 = _q(cur, "SELECT COUNT(DISTINCT designation_id) FROM fact_hpsa WHERE state_code=$1", [sc])
        if row2 and row2[0] and int(row2[0]) > 0:
            cal["provider_count"] = int(row2[0]) * 50; src.append("lake:fact_hpsa")
        else:
            cal["provider_count"] = 50_000; src.append("national_avg:provider_count_fallback")
    row = _q(cur, "SELECT AVG(pct_of_medicare) FROM fact_rate_comparison WHERE state_code=$1 AND pct_of_medicare>0", [sc])
    if row and row[0]:
        cal["pct_mcr"] = float(row[0]); src.append("lake:fact_rate_comparison")
    else:
        cal["pct_mcr"] = 72.0; src.append("national_avg:pct_of_medicare_fallback")
    return cal, src

def build_provider_model(cur, state_code: str, overrides: dict | None = None) -> SDModel:
    """2-stock provider model: provider_count, access_score. Entry/exit via logistic rate thresholds."""
    sc = state_code.upper()
    cal, src = _cal_provider(cur, sc)
    ov = overrides or {}
    prov = ov.get("provider_count", cal["provider_count"])
    pct = ov.get("pct_of_medicare", cal["pct_mcr"])

    stocks = [
        Stock("provider_count", prov, "Active Medicaid providers", "providers"),
        Stock("access_score", min(pct, 100.0), "Access adequacy score", "score_0_100"),
    ]
    params = [
        Parameter("base_entry_rate", 0.005, "literature:GAO_2022"),
        Parameter("base_exit_rate", 0.004, "literature:GAO_2022"),
        Parameter("pct_of_medicare", pct / 100.0, "lake"),
        Parameter("access_smoothing", 1.0 / 12.0, "literature"),
    ]

    def dydt(t, y, p, ivs):
        pv, ac = _c(y[0], 1.0), y[1]
        ratio = p["pct_of_medicare"] + _sum_iv(ivs, t, "rate_change")
        ef = _logistic(ratio, 0.70, 8.0)
        xf = 1.0 - _logistic(ratio, 0.50, 8.0)
        d_pv = (p["base_entry_rate"] * (0.5 + 1.5 * ef) - p["base_exit_rate"] * (0.5 + 1.5 * xf)) * pv
        d_ac = p["access_smoothing"] * (min(100.0, ratio * 120.0) - ac)
        return [d_pv, d_ac]

    return SDModel(stocks, params, dydt, "provider", src)

# ---------------------------------------------------------------------------
# 3. Workforce Model
# ---------------------------------------------------------------------------

def _cal_workforce(cur, sc):
    cal, src = {}, []
    row = _q(cur, "SELECT SUM(total_hrs_worked)/160.0 FROM fact_pbj_nurse_staffing WHERE state_code=$1", [sc])
    if row and row[0] and row[0] > 0:
        cal["active"] = float(row[0]); src.append("lake:fact_pbj_nurse_staffing")
    else:
        cal["active"] = 25_000; src.append("national_avg:workforce_fallback")
    row = _q(cur, "SELECT avg_hourly_wage FROM fact_bls_wage WHERE state_code=$1 AND occupation_code IN ('31-1120','31-1131','31-1132') ORDER BY year DESC LIMIT 1", [sc])
    if row and row[0]:
        cal["wage"] = float(row[0]); src.append("lake:fact_bls_wage")
    else:
        cal["wage"] = 15.0; src.append("national_avg:wage_fallback")
    row = _q(cur, "SELECT SUM(number_of_certified_beds) FROM fact_nh_provider_info WHERE state_code=$1", [sc])
    if row and row[0] and row[0] > 0:
        cal["beds"] = float(row[0]); src.append("lake:fact_nh_provider_info")
    else:
        cal["beds"] = 40_000; src.append("national_avg:beds_fallback")
    return cal, src

def build_workforce_model(cur, state_code: str, overrides: dict | None = None) -> SDModel:
    """3-stock workforce: applicant_pool, active_workers, experienced_workers. Wage-driven recruitment/turnover."""
    sc = state_code.upper()
    cal, src = _cal_workforce(cur, sc)
    ov = overrides or {}
    active = ov.get("active_workers", cal["active"])
    wage = ov.get("medicaid_wage", cal["wage"])
    retail = 16.0

    stocks = [
        Stock("applicant_pool", active * 0.15, "Workforce applicants", "persons"),
        Stock("active_workers", active, "Active direct care workers", "persons"),
        Stock("experienced_workers", active * 0.40, "Workers with 2+ yr experience", "persons"),
    ]
    params = [
        Parameter("base_recruit_rate", 0.08, "literature:PHI_2023"),
        Parameter("base_turnover_rate", 0.06, "literature:PHI_2023"),
        Parameter("experience_rate", 1.0 / 24.0, "default"),
        Parameter("medicaid_wage", wage, "lake"),
        Parameter("retail_benchmark", retail, "literature:BLS_2024"),
        Parameter("applicant_inflow", active * 0.02, "literature"),
    ]

    def dydt(t, y, p, ivs):
        ap, ac, ex = _c(y[0]), _c(y[1]), _c(y[2])
        w = p["medicaid_wage"] + _sum_iv(ivs, t, "wage_increase")
        wr = w / p["retail_benchmark"]
        rr = p["base_recruit_rate"] * min(wr, 2.0)
        tr = p["base_turnover_rate"] / _c(wr, 0.5)
        rec, sep = rr * ap, tr * ac
        esep = tr * 0.5 * ex
        return [
            p["applicant_inflow"] - rec + sep * 0.4,
            rec - sep - p["experience_rate"] * ac + esep,
            p["experience_rate"] * ac - esep,
        ]

    return SDModel(stocks, params, dydt, "workforce", src)

# ---------------------------------------------------------------------------
# 4. HCBS Model
# ---------------------------------------------------------------------------

def _cal_hcbs(cur, sc):
    cal, src = {}, []
    row = _q(cur, "SELECT SUM(people_waiting) FROM fact_hcbs_waitlist WHERE state_code=$1", [sc])
    if row and row[0] and row[0] > 0:
        cal["waitlist"] = float(row[0]); src.append("lake:fact_hcbs_waitlist")
    else:
        cal["waitlist"] = 10_000; src.append("national_avg:waitlist_fallback")
    row = _q(cur, "SELECT hcbs_spending, total_ltss_spending FROM fact_ltss_expenditure WHERE state_code=$1 ORDER BY year DESC LIMIT 1", [sc])
    if row and row[0] and row[1] and row[1] > 0:
        s = float(row[0]) / float(row[1])
        cal["hcbs_share"] = s; cal["inst"] = 50_000 * (1.0 - s); cal["comm"] = 50_000 * s
        src.append("lake:fact_ltss_expenditure")
    else:
        cal["hcbs_share"] = 0.58; cal["inst"] = 21_000; cal["comm"] = 29_000
        src.append("national_avg:ltss_split_fallback")
    return cal, src

def build_hcbs_model(cur, state_code: str, overrides: dict | None = None) -> SDModel:
    """3-stock HCBS rebalancing: institutional_pop, community_pop, waitlist."""
    sc = state_code.upper()
    cal, src = _cal_hcbs(cur, sc)
    ov = overrides or {}
    inst = ov.get("institutional_pop", cal["inst"])
    comm = ov.get("community_pop", cal["comm"])
    wait = ov.get("waitlist", cal["waitlist"])

    stocks = [
        Stock("institutional_pop", inst, "Institutional LTSS population", "persons"),
        Stock("community_pop", comm, "HCBS/community population", "persons"),
        Stock("waitlist", wait, "HCBS waitlist", "persons"),
    ]
    params = [
        Parameter("base_transition_rate", 0.01, "literature:MACPAC_2024"),
        Parameter("hcbs_funding_ratio", cal["hcbs_share"], "lake"),
        Parameter("new_need_rate", 0.005, "literature:CBO_2024"),
        Parameter("community_exit_rate", 0.008, "literature"),
        Parameter("institutional_exit_rate", 0.015, "literature"),
        Parameter("waitlist_clearance_rate", 0.02, "default"),
    ]

    def dydt(t, y, p, ivs):
        ins, cm, wt = _c(y[0]), _c(y[1]), _c(y[2])
        fr = min(p["hcbs_funding_ratio"] + _sum_iv(ivs, t, "hcbs_funding"), 1.0)
        tr = p["base_transition_rate"] * (fr / 0.50)
        cap = cm / _c(cm + wt, 1.0)
        nn = p["new_need_rate"] * (ins + cm)
        cl = p["waitlist_clearance_rate"] * cap * wt
        return [
            nn * (1.0 - fr) - tr * ins - p["institutional_exit_rate"] * ins,
            tr * ins + cl - p["community_exit_rate"] * cm,
            nn * fr * (1.0 - cap) - cl,
        ]

    return SDModel(stocks, params, dydt, "hcbs", src)

# ---------------------------------------------------------------------------
# 5. Integrated Model (~12 stocks, cross-domain coupling)
# ---------------------------------------------------------------------------

def build_integrated_model(cur, state_code: str,
                           interventions: list[Intervention] | None = None,
                           overrides: dict | None = None) -> SDModel:
    """12-stock integrated model. Cross-model coupling: rates->providers->access->enrollment,
    enrollment->spending->budget_pressure, wages->workforce->provider_capacity, HCBS funding->transitions."""
    sc = state_code.upper()
    ce, se = _cal_enrollment(cur, sc); cp, sp = _cal_provider(cur, sc)
    cw, sw = _cal_workforce(cur, sc);  ch, sh = _cal_hcbs(cur, sc)
    all_src = se + sp + sw + sh
    ov = overrides or {}

    en = ov.get("enrolled", ce["enrolled"]); el = ov.get("eligible_pool", ce["eligible_pool"])
    pv = ov.get("provider_count", cp["provider_count"]); pct = cp["pct_mcr"]
    ac = ov.get("active_workers", cw["active"]); wg = cw["wage"]

    stocks = [
        Stock("eligible_pool", el), Stock("processing_queue", el * 0.01),
        Stock("enrolled", en), Stock("disenrolled", 0.0),
        Stock("provider_count", pv), Stock("access_score", min(pct, 100.0)),
        Stock("applicant_pool", ac * 0.15), Stock("active_workers", ac),
        Stock("experienced_workers", ac * 0.40),
        Stock("institutional_pop", ov.get("institutional_pop", ch["inst"])),
        Stock("community_pop", ov.get("community_pop", ch["comm"])),
        Stock("waitlist", ov.get("waitlist", ch["waitlist"])),
    ]
    params = [
        Parameter("application_rate", 0.03, "literature:CMS_2024"),
        Parameter("determination_rate", 0.60, "literature:CMS_2024"),
        Parameter("approval_prob", 0.75, "national_avg"),
        Parameter("disenroll_rate", ce["disenroll_rate"], "lake"),
        Parameter("population_growth", el * 0.001, "literature:Census_2024"),
        Parameter("unemployment_elasticity", 0.5, "literature:Sommers_2012"),
        Parameter("base_entry_rate", 0.005, "literature:GAO_2022"),
        Parameter("base_exit_rate", 0.004, "literature:GAO_2022"),
        Parameter("pct_of_medicare", pct / 100.0, "lake"),
        Parameter("access_smoothing", 1.0 / 12.0, "literature"),
        Parameter("base_recruit_rate", 0.08, "literature:PHI_2023"),
        Parameter("base_turnover_rate", 0.06, "literature:PHI_2023"),
        Parameter("experience_rate", 1.0 / 24.0, "default"),
        Parameter("medicaid_wage", wg, "lake"),
        Parameter("retail_benchmark", 16.0, "literature:BLS_2024"),
        Parameter("applicant_inflow", ac * 0.02, "literature"),
        Parameter("base_transition_rate", 0.01, "literature:MACPAC_2024"),
        Parameter("hcbs_funding_ratio", ch["hcbs_share"], "lake"),
        Parameter("new_need_rate", 0.005, "literature:CBO_2024"),
        Parameter("community_exit_rate", 0.008, "literature"),
        Parameter("institutional_exit_rate", 0.015, "literature"),
        Parameter("waitlist_clearance_rate", 0.02, "default"),
        Parameter("spending_per_enrollee_monthly", 600.0, "national_avg"),
        Parameter("budget_target_monthly", en * 600.0, "derived"),
        Parameter("workforce_capacity_baseline", ac, "derived"),
    ]

    def dydt(t, y, p, ivs):
        # Unpack with clamp
        el, pr, en = _c(y[0]), _c(y[1]), _c(y[2])
        pv, ac_s = _c(y[4], 1.0), y[5]
        ap, aw, ew = _c(y[6]), _c(y[7]), _c(y[8])
        ins, cm, wt = _c(y[9]), _c(y[10]), _c(y[11])

        # Interventions
        ud = _sum_iv(ivs, t, "unemployment_shock")
        rd = _sum_iv(ivs, t, "rate_change")
        wd = _sum_iv(ivs, t, "wage_increase")
        hd = _sum_iv(ivs, t, "hcbs_funding")
        pd = _sum_iv(ivs, t, "policy_shock")

        # Cross-domain coupling
        access_mod = 0.8 + 0.4 * (ac_s / 100.0)
        wf_ratio = aw / _c(p["workforce_capacity_baseline"], 1.0)
        wf_mod = min(wf_ratio, 1.5)
        spending = en * p["spending_per_enrollee_monthly"]
        bp = spending / _c(p["budget_target_monthly"], 1.0)
        ratio = p["pct_of_medicare"] + rd
        cw = p["medicaid_wage"] + wd
        wr = cw / p["retail_benchmark"]
        fr = min(p["hcbs_funding_ratio"] + hd, 1.0)

        # Enrollment
        econ = 1.0 + p["unemployment_elasticity"] * ud
        ar = p["application_rate"] * econ * access_mod * (1.0 + pd)
        di = p["disenroll_rate"] * (1.0 + max(0, bp - 1.2) * 0.5)
        d0 = -ar * el + p["population_growth"] + di * en * 0.3
        d1 = ar * el - p["determination_rate"] * pr
        d2 = p["approval_prob"] * p["determination_rate"] * pr - di * en
        d3 = di * en

        # Providers
        ef = _logistic(ratio, 0.70, 8.0); xf = 1.0 - _logistic(ratio, 0.50, 8.0)
        pe = p["base_entry_rate"] * (0.5 + 1.5 * ef) * wf_mod
        px = p["base_exit_rate"] * (0.5 + 1.5 * xf)
        d4 = (pe - px) * pv
        d5 = p["access_smoothing"] * (min(100.0, ratio * 120.0) - ac_s)

        # Workforce
        rr = p["base_recruit_rate"] * min(wr, 2.0)
        tr = p["base_turnover_rate"] / _c(wr, 0.5)
        rec, sep, esep = rr * ap, tr * aw, tr * 0.5 * ew
        d6 = p["applicant_inflow"] - rec + sep * 0.4
        d7 = rec - sep - p["experience_rate"] * aw + esep
        d8 = p["experience_rate"] * aw - esep

        # HCBS
        trate = p["base_transition_rate"] * (fr / 0.50)
        cap = cm / _c(cm + wt, 1.0)
        nn = p["new_need_rate"] * (ins + cm)
        cl = p["waitlist_clearance_rate"] * cap * wt
        d9 = nn * (1.0 - fr) - trate * ins - p["institutional_exit_rate"] * ins
        d10 = trate * ins + cl - p["community_exit_rate"] * cm
        d11 = nn * fr * (1.0 - cap) - cl

        return [d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11]

    return SDModel(stocks, params, dydt, "integrated", list(set(all_src)))
