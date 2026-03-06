"""Bulk data endpoints serving pre-computed shapes matching frontend static JSON formats.

These endpoints query the DuckDB lake views and reshape data to match the exact
structure the frontend expects from its static JSON files, enabling API-first
loading with graceful fallback to static JSON when the API is unavailable.
"""

from fastapi import APIRouter

from server.db import get_cursor

router = APIRouter()

# Application-level cache — populated on first request, lives until process restart.
_cache: dict = {}


def _cached(key: str, fn):
    if key not in _cache:
        _cache[key] = fn()
    return _cache[key]


@router.get("/api/bulk/medicare-rates")
async def bulk_medicare_rates():
    """Medicare PFS rates + RVUs keyed by code. Matches medicare_rates.json shape."""
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT procedure_code, description,
                       medicare_rate_nonfac, medicare_rate_fac,
                       work_rvu, pe_rvu_nonfacility, mp_rvu,
                       total_rvu_nonfac, conversion_factor
                FROM dim_procedure
                WHERE medicare_rate_nonfac IS NOT NULL
                   OR medicare_rate_fac IS NOT NULL
            """).fetchall()
        rates = {}
        for code, desc, nf, fac, w, pe, mp, rvu, cf in rows:
            entry: dict = {}
            if nf is not None:
                entry["r"] = round(float(nf), 2)
            if fac is not None:
                entry["fr"] = round(float(fac), 2)
            if rvu is not None:
                entry["rvu"] = round(float(rvu), 4)
            if w is not None:
                entry["w"] = round(float(w), 4)
            if pe is not None:
                entry["pe"] = round(float(pe), 4)
            if mp is not None:
                entry["mp"] = round(float(mp), 4)
            if desc:
                entry["d"] = desc
            rates[code] = entry
        return {"rates": rates, "cf": 33.4009, "year": 2025}
    return _cached("medicare_rates", compute)


@router.get("/api/bulk/medicaid-rates")
async def bulk_medicaid_rates():
    """Medicaid fee schedule rates by state/code. Matches medicaid_rates.json shape.

    Returns: {state: {code: [rate, "", source]}}
    """
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, procedure_code,
                       COALESCE(rate, rate_nonfacility, rate_facility) AS eff_rate,
                       source_file
                FROM fact_medicaid_rate
                WHERE COALESCE(rate, rate_nonfacility, rate_facility) IS NOT NULL
            """).fetchall()
        result: dict = {}
        for state, code, rate, source in rows:
            if state not in result:
                result[state] = {}
            result[state][code] = [round(float(rate), 2), "", source or ""]
        return result
    return _cached("medicaid_rates", compute)


@router.get("/api/bulk/hcpcs-rates")
async def bulk_hcpcs_rates():
    """T-MSIS actual-paid rates by code/state. Matches hcpcs.json shape.

    Returns: [{code, rates: {state: rate}}]
    """
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT procedure_code, state_code, avg_paid_per_claim
                FROM fact_claims
                WHERE avg_paid_per_claim > 0 AND avg_paid_per_claim < 50000
            """).fetchall()
        codes: dict = {}
        for code, state, rate in rows:
            if code not in codes:
                codes[code] = {}
            codes[code][state] = round(float(rate), 2)
        return [{"code": c, "rates": r} for c, r in codes.items()]
    return _cached("hcpcs_rates", compute)


@router.get("/api/bulk/gpci")
async def bulk_gpci():
    """GPCI values by locality. Matches gpci.json shape."""
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT state_code, mac_locality, locality_name,
                       gpci_work, gpci_pe, gpci_mp
                FROM dim_medicare_locality
                ORDER BY state_code, mac_locality
            """).fetchall()
        return [
            {
                "state": st, "locality": loc, "locality_name": name,
                "pw_gpci": round(float(pw), 4) if pw else 1.0,
                "pw_gpci_floor": round(float(pw), 4) if pw else 1.0,
                "pe_gpci": round(float(pe), 4) if pe else 1.0,
                "mp_gpci": round(float(mp), 4) if mp else 1.0,
            }
            for st, loc, name, pw, pe, mp in rows
        ]
    return _cached("gpci", compute)


@router.get("/api/bulk/quality-measures")
async def bulk_quality_measures():
    """Quality measures with rates and metadata. Matches quality_measures.json shape.

    Returns: {measures: {id: {name, domain, ...}}, rates: {id: {state: rate}}, measure_hcpcs: {}}
    """
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT measure_id, state_code, rate, measure_name, domain
                FROM fact_quality_measure
                ORDER BY measure_id, state_code
            """).fetchall()
        measures: dict = {}
        rates: dict = {}
        for mid, state, rate, name, domain in rows:
            if mid not in measures:
                measures[mid] = {"name": name or mid, "domain": domain or "", "rate_def": ""}
            if mid not in rates:
                rates[mid] = {}
            if rate is not None:
                rates[mid][state] = round(float(rate), 2)
        # Compute median and n_states
        for mid in measures:
            vals = sorted(v for v in rates.get(mid, {}).values() if v is not None)
            measures[mid]["n_states"] = len(vals)
            measures[mid]["median"] = round(vals[len(vals) // 2], 2) if vals else 0
        return {"measures": measures, "rates": rates, "measure_hcpcs": {}}
    return _cached("quality_measures", compute)


@router.get("/api/bulk/states")
async def bulk_states():
    """State-level aggregates. Matches states.json shape."""
    def compute():
        with get_cursor() as cur:
            meta = {}
            for r in cur.execute("""
                SELECT state_code, state_name, fmap, total_enrollment,
                       ffs_enrollment, mc_enrollment, pct_managed_care,
                       methodology, conversion_factor, fee_index, update_frequency
                FROM dim_state
            """).fetchall():
                meta[r[0]] = {
                    "state": r[0], "state_name": r[1], "fmap": float(r[2]) if r[2] else None,
                    "enrollment_fy2023": int(r[3]) if r[3] else 0,
                    "ffs_enrollment": int(r[4]) if r[4] else 0,
                    "mc_enrollment": int(r[5]) if r[5] else 0,
                    "pct_managed_care": float(r[6]) if r[6] else 0,
                    "methodology": r[7] or "", "conversion_factor": float(r[8]) if r[8] else None,
                    "fee_index": r[9] or "", "update_frequency": r[10] or "",
                    "total_spend": 0, "total_claims": 0, "total_bene": 0, "n_providers": 0,
                }
            # Aggregate claims
            for r in cur.execute("""
                SELECT state_code,
                       SUM(total_paid) AS s, SUM(total_claims) AS c,
                       MAX(total_beneficiaries) AS b, MAX(provider_count) AS p
                FROM fact_claims
                GROUP BY state_code
            """).fetchall():
                if r[0] in meta:
                    meta[r[0]]["total_spend"] = round(float(r[1]), 2) if r[1] else 0
                    meta[r[0]]["total_claims"] = int(r[2]) if r[2] else 0
                    meta[r[0]]["total_bene"] = int(r[3]) if r[3] else 0
                    meta[r[0]]["n_providers"] = int(r[4]) if r[4] else 0
        return list(meta.values())
    return _cached("states", compute)


@router.get("/api/bulk/fee-schedule-rates")
async def bulk_fee_schedule_rates():
    """Rate comparison data keyed by code. Matches fee_schedule_rates.json shape.

    Returns: {code: {states: {state: rate}, desc, medicare}}
    """
    def compute():
        with get_cursor() as cur:
            rows = cur.execute("""
                SELECT rc.procedure_code, rc.state_code,
                       rc.medicaid_rate, rc.medicare_nonfac_rate,
                       dp.description
                FROM fact_rate_comparison rc
                LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
                WHERE rc.medicaid_rate IS NOT NULL
            """).fetchall()
        result: dict = {}
        for code, state, med_rate, mcr_rate, desc in rows:
            if code not in result:
                result[code] = {
                    "states": {},
                    "desc": desc or "",
                    "medicare": round(float(mcr_rate), 2) if mcr_rate else None,
                }
            result[code]["states"][state] = round(float(med_rate), 2)
        return result
    return _cached("fee_schedule_rates", compute)
