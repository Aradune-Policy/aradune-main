"""
Caseload Forecasting API routes.

Template-driven upload pattern (same as CPRA upload):
  1. Download template CSVs
  2. Fill in your data
  3. Upload → get forecast JSON / CSV back
"""

import io
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from server.db import get_cursor
from server.engines.caseload_forecast import (
    CaseloadForecaster,
    generate_caseload_template,
    generate_events_template,
)
from server.engines.expenditure_model import (
    ExpenditureModeler,
    generate_params_template,
)

router = APIRouter()


# ─── Templates ──────────────────────────────────────────────────────────


@router.get("/api/forecast/templates/caseload")
def forecast_caseload_template(
    include_regional: bool = Query(False),
    include_delivery: bool = Query(False),
):
    """Download a blank caseload CSV template."""
    content = generate_caseload_template(
        include_regional=include_regional,
        include_delivery=include_delivery,
    )
    return StreamingResponse(
        io.BytesIO(content.encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=caseload_template.csv"
        },
    )


@router.get("/api/forecast/templates/events")
def forecast_events_template():
    """Download a blank events CSV template with example structural events."""
    content = generate_events_template()
    return StreamingResponse(
        io.BytesIO(content.encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=events_template.csv"
        },
    )


# ─── Generate forecast ─────────────────────────────────────────────────


@router.post("/api/forecast/generate")
async def forecast_generate(
    state: str = Form(...),
    caseload: UploadFile = File(...),
    events: Optional[UploadFile] = File(None),
    horizon_months: int = Form(36),
    include_seasonality: bool = Form(True),
    include_economic: bool = Form(True),
):
    """
    Upload caseload CSV (+ optional events CSV) → get forecast JSON.

    The caseload CSV must have columns: month, category, enrollment
    Optional columns: county, region, delivery_system, subcategory

    Returns per-category forecasts with 80/95% confidence intervals,
    model selection metadata, intervention effects, and aggregate totals.
    """
    state = state.upper().strip()

    try:
        forecaster = CaseloadForecaster(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    # Load caseload data
    caseload_bytes = await caseload.read()
    errors = forecaster.load_caseload_bytes(caseload_bytes)
    if any(e.severity == "error" for e in errors):
        raise HTTPException(422, {
            "stage": "caseload",
            "errors": [{"field": e.field, "message": e.message} for e in errors],
        })

    # Load events (optional)
    if events is not None:
        events_bytes = await events.read()
        if events_bytes.strip():
            evt_errors = forecaster.load_events_bytes(events_bytes)
            if any(e.severity == "error" for e in evt_errors):
                raise HTTPException(422, {
                    "stage": "events",
                    "errors": [
                        {"field": e.field, "message": e.message} for e in evt_errors
                    ],
                })

    # Enrich with Aradune economic data
    if include_economic:
        try:
            with get_cursor() as cur:
                forecaster.load_economic_data(cur)
        except Exception:
            pass  # Non-fatal — economic data is optional

    # Run forecast
    try:
        result = forecaster.forecast(
            horizon_months=min(horizon_months, 60),
            include_seasonality=include_seasonality,
            include_economic=include_economic,
        )
    except Exception as e:
        raise HTTPException(500, f"Forecast engine error: {e}")

    return result.to_json()


@router.post("/api/forecast/generate/csv")
async def forecast_generate_csv(
    state: str = Form(...),
    caseload: UploadFile = File(...),
    events: Optional[UploadFile] = File(None),
    horizon_months: int = Form(36),
    include_seasonality: bool = Form(True),
    include_economic: bool = Form(True),
):
    """Upload caseload CSV → get forecast CSV download."""
    state = state.upper().strip()

    try:
        forecaster = CaseloadForecaster(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    caseload_bytes = await caseload.read()
    errors = forecaster.load_caseload_bytes(caseload_bytes)
    if any(e.severity == "error" for e in errors):
        raise HTTPException(422, {
            "stage": "caseload",
            "errors": [{"field": e.field, "message": e.message} for e in errors],
        })

    if events is not None:
        events_bytes = await events.read()
        if events_bytes.strip():
            forecaster.load_events_bytes(events_bytes)

    if include_economic:
        try:
            with get_cursor() as cur:
                forecaster.load_economic_data(cur)
        except Exception:
            pass

    try:
        result = forecaster.forecast(
            horizon_months=min(horizon_months, 60),
            include_seasonality=include_seasonality,
            include_economic=include_economic,
        )
    except Exception as e:
        raise HTTPException(500, f"Forecast engine error: {e}")

    csv_bytes = result.to_csv_bytes()
    filename = f"caseload_forecast_{state}_{result.forecast_date}.csv"

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─── Public enrollment data (no upload needed) ─────────────────────────


@router.get("/api/forecast/public-enrollment")
async def public_enrollment(
    state: Optional[str] = Query(None),
):
    """
    Get Aradune's public enrollment time series for a state.
    Useful for users who want to see what public data exists before uploading.
    """
    with get_cursor() as cur:
        sql = """
            SELECT state_code, year, month, total_enrollment,
                   ffs_enrollment, mc_enrollment
            FROM fact_enrollment
            WHERE 1=1
        """
        params = []
        if state:
            sql += " AND state_code = ?"
            params.append(state.upper())
        sql += " ORDER BY state_code, year, month"

        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


@router.get("/api/forecast/public-enrollment/by-group")
async def public_enrollment_by_group(
    state: Optional[str] = Query(None),
):
    """Get public enrollment by eligibility group (monthly)."""
    with get_cursor() as cur:
        sql = """
            SELECT state_name, month_str, eligibility_group, count_enrolled
            FROM fact_elig_group_monthly
            WHERE count_enrolled IS NOT NULL
        """
        params = []
        if state:
            sql += " AND state_name = ?"
            params.append(state)
        sql += " ORDER BY state_name, month_str, eligibility_group"

        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

    return {"rows": [dict(zip(cols, r)) for r in rows], "count": len(rows)}


# ─── Expenditure modeling ─────────────────────────────────────────────


@router.get("/api/forecast/templates/expenditure-params")
def expenditure_params_template():
    """Download a blank expenditure parameters CSV template."""
    content = generate_params_template()
    return StreamingResponse(
        io.BytesIO(content.encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=expenditure_params_template.csv"
        },
    )


@router.post("/api/forecast/expenditure")
async def forecast_expenditure(
    state: str = Form(...),
    caseload: UploadFile = File(...),
    params: UploadFile = File(...),
    events: Optional[UploadFile] = File(None),
    horizon_months: int = Form(36),
    include_seasonality: bool = Form(True),
    include_economic: bool = Form(True),
):
    """
    Full pipeline: upload caseload CSV + expenditure params → expenditure projections.

    Runs caseload forecast first, then applies expenditure parameters.
    Returns both caseload forecast and expenditure projection.
    """
    state = state.upper().strip()

    # Step 1: Run caseload forecast
    try:
        forecaster = CaseloadForecaster(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    caseload_bytes = await caseload.read()
    errors = forecaster.load_caseload_bytes(caseload_bytes)
    if any(e.severity == "error" for e in errors):
        raise HTTPException(422, {
            "stage": "caseload",
            "errors": [{"field": e.field, "message": e.message} for e in errors],
        })

    if events is not None:
        events_bytes = await events.read()
        if events_bytes.strip():
            evt_errors = forecaster.load_events_bytes(events_bytes)
            if any(e.severity == "error" for e in evt_errors):
                raise HTTPException(422, {
                    "stage": "events",
                    "errors": [
                        {"field": e.field, "message": e.message} for e in evt_errors
                    ],
                })

    if include_economic:
        try:
            with get_cursor() as cur:
                forecaster.load_economic_data(cur)
        except Exception:
            pass

    try:
        forecast_result = forecaster.forecast(
            horizon_months=min(horizon_months, 60),
            include_seasonality=include_seasonality,
            include_economic=include_economic,
        )
    except Exception as e:
        raise HTTPException(500, f"Forecast engine error: {e}")

    forecast_json = forecast_result.to_json()

    # Step 2: Run expenditure projection
    try:
        modeler = ExpenditureModeler(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    load_errors = modeler.load_caseload_from_forecast(forecast_json)
    if any(e.severity == "error" for e in load_errors):
        raise HTTPException(422, {
            "stage": "caseload_load",
            "errors": [{"field": e.field, "message": e.message} for e in load_errors],
        })

    params_bytes = await params.read()
    param_errors = modeler.load_params_bytes(params_bytes)
    if any(e.severity == "error" for e in param_errors):
        raise HTTPException(422, {
            "stage": "params",
            "errors": [{"field": e.field, "message": e.message} for e in param_errors],
        })

    try:
        exp_result = modeler.project()
    except Exception as e:
        raise HTTPException(500, f"Expenditure projection error: {e}")

    return {
        "forecast": forecast_json,
        "expenditure": exp_result.to_json(),
    }


@router.post("/api/forecast/expenditure/csv")
async def forecast_expenditure_csv(
    state: str = Form(...),
    caseload: UploadFile = File(...),
    params: UploadFile = File(...),
    events: Optional[UploadFile] = File(None),
    horizon_months: int = Form(36),
    include_seasonality: bool = Form(True),
    include_economic: bool = Form(True),
):
    """Upload caseload + params → expenditure CSV download."""
    state = state.upper().strip()

    try:
        forecaster = CaseloadForecaster(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    caseload_bytes = await caseload.read()
    errors = forecaster.load_caseload_bytes(caseload_bytes)
    if any(e.severity == "error" for e in errors):
        raise HTTPException(422, {
            "stage": "caseload",
            "errors": [{"field": e.field, "message": e.message} for e in errors],
        })

    if events is not None:
        events_bytes = await events.read()
        if events_bytes.strip():
            forecaster.load_events_bytes(events_bytes)

    if include_economic:
        try:
            with get_cursor() as cur:
                forecaster.load_economic_data(cur)
        except Exception:
            pass

    forecast_result = forecaster.forecast(
        horizon_months=min(horizon_months, 60),
        include_seasonality=include_seasonality,
        include_economic=include_economic,
    )
    forecast_json = forecast_result.to_json()

    try:
        modeler = ExpenditureModeler(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    load_errors = modeler.load_caseload_from_forecast(forecast_json)
    if any(e.severity == "error" for e in load_errors):
        raise HTTPException(422, {
            "stage": "caseload_load",
            "errors": [{"field": e.field, "message": e.message} for e in load_errors],
        })

    params_bytes = await params.read()
    param_errors = modeler.load_params_bytes(params_bytes)
    if any(e.severity == "error" for e in param_errors):
        raise HTTPException(422, {
            "stage": "params",
            "errors": [{"field": e.field, "message": e.message} for e in param_errors],
        })

    try:
        exp_result = modeler.project()
    except Exception as e:
        raise HTTPException(500, f"Expenditure projection error: {e}")
    csv_bytes = exp_result.to_csv_bytes()
    filename = f"expenditure_projection_{state}_{exp_result.projection_date}.csv"

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/api/forecast/expenditure-only")
async def expenditure_from_forecast(
    state: str = Form(...),
    forecast_csv: UploadFile = File(...),
    params: UploadFile = File(...),
):
    """
    Upload a previously-generated caseload forecast CSV + expenditure params.
    Skips re-running the forecast — just applies expenditure parameters.
    """
    state = state.upper().strip()

    try:
        modeler = ExpenditureModeler(state)
    except ValueError as e:
        raise HTTPException(400, str(e))

    forecast_bytes = await forecast_csv.read()
    load_errors = modeler.load_caseload_from_csv(forecast_bytes)
    if any(e.severity == "error" for e in load_errors):
        raise HTTPException(422, {
            "stage": "forecast_csv",
            "errors": [{"field": e.field, "message": e.message} for e in load_errors],
        })

    params_bytes = await params.read()
    param_errors = modeler.load_params_bytes(params_bytes)
    if any(e.severity == "error" for e in param_errors):
        raise HTTPException(422, {
            "stage": "params",
            "errors": [{"field": e.field, "message": e.message} for e in param_errors],
        })

    try:
        result = modeler.project()
    except Exception as e:
        raise HTTPException(500, f"Expenditure projection error: {e}")
    return result.to_json()
