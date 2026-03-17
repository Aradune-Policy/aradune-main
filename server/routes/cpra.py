"""CPRA (Comparative Payment Rate Analysis) API routes.

Two sections:
1. Pre-computed rate comparisons from the data lake (general fee-to-Medicare comparison)
2. User-upload CPRA generation (42 CFR 447.203 compliance tool)
"""

import io
import tempfile
from datetime import date
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from server.db import get_cursor
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/cpra/states")
@safe_route(default_response=[])
async def cpra_states():
    """List all states with CPRA rate comparison data."""
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT
                state_code,
                COUNT(*) AS total_codes,
                SUM(CASE WHEN em_category IS NOT NULL THEN 1 ELSE 0 END) AS em_codes,
                ROUND(MEDIAN(pct_of_medicare), 2) AS median_pct,
                ROUND(AVG(pct_of_medicare), 2) AS avg_pct
            FROM fact_rate_comparison
            WHERE pct_of_medicare > 0 AND pct_of_medicare < 1000
            GROUP BY state_code
            ORDER BY state_code
        """).fetchall()
        columns = ["state_code", "total_codes", "em_codes", "median_pct", "avg_pct"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/rates/{state_code}")
@safe_route(default_response=[])
async def cpra_rates(
    state_code: str,
    em_only: bool = Query(False, description="Filter to E/M codes only"),
):
    """Get rate comparison data for a specific state."""
    state_code = state_code.upper()
    if len(state_code) != 2:
        raise HTTPException(400, "state_code must be 2-letter abbreviation")

    em_filter = "AND rc.em_category IS NOT NULL" if em_only else ""
    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT
                rc.procedure_code,
                rc.modifier,
                rc.medicaid_rate,
                rc.medicare_nonfac_rate,
                rc.medicare_fac_rate,
                rc.pct_of_medicare,
                rc.em_category,
                dp.category AS category,
                dp.description,
                rc.rate_effective_date AS medicaid_rate_date
            FROM fact_rate_comparison rc
            LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
            WHERE rc.state_code = $1 {em_filter}
            ORDER BY rc.pct_of_medicare ASC
        """, [state_code]).fetchall()

        columns = [
            "procedure_code", "modifier", "medicaid_rate",
            "medicare_nonfac_rate", "medicare_fac_rate", "pct_of_medicare",
            "em_category", "category", "description", "medicaid_rate_date",
        ]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/dq/{state_code}")
@safe_route(default_response=[])
async def cpra_dq_flags(state_code: str):
    """Get data quality flags for a specific state."""
    state_code = state_code.upper()
    with get_cursor() as cur:
        rows = cur.execute("""
            SELECT state_code, procedure_code, flag, detail, created_at
            FROM fact_dq_flag
            WHERE state_code = $1
            ORDER BY flag, procedure_code
        """, [state_code]).fetchall()

        columns = ["state_code", "procedure_code", "flag", "detail", "created_at"]
        return [dict(zip(columns, r)) for r in rows]


@router.get("/api/cpra/compare")
@safe_route(default_response=[])
async def cpra_compare_codes(
    codes: str = Query(..., description="Comma-separated HCPCS codes"),
    states: str = Query(None, description="Comma-separated state codes (all if omitted)"),
):
    """Compare specific codes across states."""
    code_list = [c.strip() for c in codes.split(",")]
    placeholders = ", ".join(f"${i+1}" for i in range(len(code_list)))

    params = list(code_list)
    state_filter = ""
    if states:
        state_list = [s.strip().upper() for s in states.split(",")]
        state_placeholders = ", ".join(f"${len(params)+i+1}" for i in range(len(state_list)))
        state_filter = f"AND rc.state_code IN ({state_placeholders})"
        params.extend(state_list)

    with get_cursor() as cur:
        rows = cur.execute(f"""
            SELECT
                rc.state_code,
                rc.procedure_code,
                rc.medicaid_rate,
                rc.medicare_nonfac_rate,
                rc.pct_of_medicare,
                rc.em_category,
                dp.description,
                ds.state_name
            FROM fact_rate_comparison rc
            LEFT JOIN dim_procedure dp ON rc.procedure_code = dp.procedure_code
            LEFT JOIN dim_state ds ON rc.state_code = ds.state_code
            WHERE rc.procedure_code IN ({placeholders}) {state_filter}
            ORDER BY rc.procedure_code, rc.state_code
        """, params).fetchall()

        columns = [
            "state_code", "procedure_code", "medicaid_rate",
            "medicare_nonfac_rate", "pct_of_medicare", "em_category",
            "description", "state_name",
        ]
        return [dict(zip(columns, r)) for r in rows]


# ---------------------------------------------------------------------------
# CPRA Upload Tool — 42 CFR 447.203 compliance generator
# ---------------------------------------------------------------------------

from server.engines.cpra_upload import (
    CATEGORIES,
    CONVERSION_FACTOR,
    RATE_YEAR,
    STATE_NAMES as CPRA_STATE_NAMES,
    CpraGenerator,
    REF_DIR,
)


def _get_state_localities() -> dict:
    """Precompute state → locality count mapping from GPCI file."""
    import duckdb

    gpci_path = REF_DIR / "GPCI2025.csv"
    if not gpci_path.exists():
        return {}

    db = duckdb.connect()
    db.execute(f"""
        CREATE TABLE gpcis AS
        SELECT
            TRIM(column1) AS state_code,
            CAST(TRIM(column2) AS VARCHAR) AS locality_number,
            TRIM(column3) AS locality_name
        FROM read_csv(
            '{gpci_path}',
            header=false,
            skip=3,
            columns={{
                'column0': 'VARCHAR', 'column1': 'VARCHAR',
                'column2': 'VARCHAR', 'column3': 'VARCHAR',
                'column4': 'VARCHAR', 'column5': 'VARCHAR',
                'column6': 'VARCHAR'
            }}
        )
        WHERE TRIM(column1) IS NOT NULL AND LENGTH(TRIM(column1)) = 2
    """)

    rows = db.execute("""
        SELECT state_code, COUNT(*) AS n, LIST(locality_name) AS locs
        FROM gpcis GROUP BY state_code ORDER BY state_code
    """).fetchall()
    db.close()

    result = {}
    for code, n, locs in rows:
        if code in CPRA_STATE_NAMES:
            result[code] = {
                "state_code": code,
                "state_name": CPRA_STATE_NAMES[code],
                "n_localities": n,
                "localities": locs,
            }
    return result


_cpra_state_localities = _get_state_localities()


@router.get("/api/cpra/upload/states")
@safe_route(default_response={"states": [], "total": 0})
def cpra_upload_states():
    """List all 53 states/territories with Medicare locality info for the upload tool."""
    return {
        "states": list(_cpra_state_localities.values()),
        "total": len(_cpra_state_localities),
    }


@router.get("/api/cpra/upload/codes")
@safe_route(default_response={"n_codes": 0, "codes": [], "categories": [], "code_categories": []})
def cpra_upload_codes():
    """Return the 68 CMS CY 2025 E/M codes and category mapping."""
    import csv

    em_path = REF_DIR / "em_codes.csv"
    cat_path = REF_DIR / "code_categories.csv"

    codes = []
    with open(em_path) as f:
        for row in csv.DictReader(f):
            codes.append({
                "hcpcs_code": row["hcpcs_code"],
                "description": row["description"],
                "is_primary_care": row["is_primary_care"] == "TRUE",
                "is_obgyn": row["is_obgyn"] == "TRUE",
                "is_mhsud": row["is_mhsud"] == "TRUE",
            })

    categories = []
    with open(cat_path) as f:
        for row in csv.DictReader(f):
            categories.append(row)

    return {
        "n_codes": len(codes),
        "n_code_category_pairs": len(categories),
        "categories": CATEGORIES,
        "codes": codes,
        "code_categories": categories,
        "rate_year": RATE_YEAR,
        "conversion_factor": CONVERSION_FACTOR,
    }


@router.get("/api/cpra/upload/templates/fee-schedule")
@safe_route(default_response={"error": "Template generation failed"})
def cpra_fee_schedule_template():
    """Download a fee schedule CSV template with the 68 E/M codes."""
    import csv as csv_mod

    em_path = REF_DIR / "em_codes.csv"
    buf = io.StringIO()
    writer = csv_mod.writer(buf)
    writer.writerow(["hcpcs_code", "medicaid_rate", "description"])

    with open(em_path) as f:
        for row in csv_mod.DictReader(f):
            writer.writerow([row["hcpcs_code"], "", row["description"]])

    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cpra_fee_schedule_template.csv"},
    )


@router.get("/api/cpra/upload/templates/utilization")
@safe_route(default_response={"error": "Template generation failed"})
def cpra_utilization_template():
    """Download a utilization CSV template with the 171 code-category pairs."""
    import csv as csv_mod

    cat_path = REF_DIR / "code_categories.csv"
    buf = io.StringIO()
    writer = csv_mod.writer(buf)
    writer.writerow([
        "hcpcs_code", "category", "total_claims", "unique_beneficiaries",
        "total_units", "total_paid",
    ])

    with open(cat_path) as f:
        for row in csv_mod.DictReader(f):
            writer.writerow([row["hcpcs_code"], row["category"], "", "", "", ""])

    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cpra_utilization_template.csv"},
    )


@router.post("/api/cpra/upload/generate")
@safe_route(default_response={"meta": {}, "statewide": [], "category_summary": [], "category_locality_summary": [], "codes_no_rate": []})
async def cpra_upload_generate(
    state: str = Form(...),
    fee_schedule: UploadFile = File(...),
    utilization: UploadFile = File(...),
):
    """
    Generate a CPRA from uploaded fee schedule + utilization CSVs.

    Returns the full CPRA result as JSON: statewide comparison, category
    summaries, category x locality breakdowns, and codes without rates.
    """
    state = state.upper().strip()
    if state not in CPRA_STATE_NAMES:
        raise HTTPException(400, f"Unknown state code: {state}")

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_path = Path(tmpdir) / "fee_schedule.csv"
        ut_path = Path(tmpdir) / "utilization.csv"

        fs_path.write_bytes(await fee_schedule.read())
        ut_path.write_bytes(await utilization.read())

        try:
            gen = CpraGenerator(state)

            errors = gen.load_fee_schedule(fs_path)
            if any(e.severity == "error" for e in errors):
                gen.close()
                raise HTTPException(422, {
                    "stage": "fee_schedule",
                    "errors": [{"field": e.field, "message": e.message} for e in errors],
                })

            errors = gen.load_utilization(ut_path)
            if any(e.severity == "error" for e in errors):
                gen.close()
                raise HTTPException(422, {
                    "stage": "utilization",
                    "errors": [{"field": e.field, "message": e.message} for e in errors],
                })

            result = gen.generate()
            gen.close()

        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(500, f"CPRA generation failed: {exc}")

    return {
        "meta": {
            "state_code": result.state_code,
            "state_name": result.state_name,
            "rate_year": result.rate_year,
            "util_year": result.util_year,
            "conversion_factor": result.conversion_factor,
            "n_codes": result.n_codes,
            "n_with_rate": result.n_with_rate,
            "n_without_rate": result.n_without_rate,
            "n_code_category_pairs": result.n_code_category_pairs,
            "n_localities": result.n_localities,
            "utilization_source": result.utilization_source,
            "warnings": result.warnings,
        },
        "statewide": result.statewide,
        "category_summary": result.category_summary,
        "category_locality_summary": result.category_locality,
        "codes_no_rate": result.codes_no_rate,
    }


@router.post("/api/cpra/upload/generate/csv")
@safe_route(default_response={"error": "CPRA CSV generation failed"})
async def cpra_upload_generate_csv(
    state: str = Form(...),
    fee_schedule: UploadFile = File(...),
    utilization: UploadFile = File(...),
):
    """Generate a CPRA and return the statewide comparison as a CSV download."""
    result_json = await cpra_upload_generate(state, fee_schedule, utilization)
    rows = result_json["statewide"]
    if not rows:
        raise HTTPException(404, "No results generated")

    import csv as csv_mod

    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    state_lower = state.lower().strip()
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=cpra_{state_lower}_statewide.csv"
        },
    )


@router.post("/api/cpra/upload/generate/report")
@safe_route(default_response={"error": "CPRA report generation failed"})
async def cpra_upload_generate_report(
    state: str = Form(...),
    fee_schedule: UploadFile = File(...),
    utilization: UploadFile = File(...),
):
    """Generate a CPRA and return a self-contained HTML report (print-to-PDF ready)."""
    result_json = await cpra_upload_generate(state, fee_schedule, utilization)
    html = _build_cpra_html_report(result_json)

    state_lower = state.lower().strip()
    return StreamingResponse(
        io.BytesIO(html.encode()),
        media_type="text/html",
        headers={
            "Content-Disposition": f"attachment; filename=cpra_{state_lower}_report.html"
        },
    )


def _build_cpra_html_report(data: dict) -> str:
    """Build a self-contained, print-ready HTML CPRA report."""
    m = data["meta"]

    def pct_color(p):
        if p is None: return "#6b7280"
        if p >= 100: return "#166534"
        if p >= 80: return "#3f6212"
        if p >= 60: return "#854d0e"
        return "#991b1b"

    def pct_bg(p):
        if p is None: return "#f3f4f6"
        if p >= 100: return "#dcfce7"
        if p >= 80: return "#ecfccb"
        if p >= 60: return "#fef9c3"
        return "#fee2e2"

    def dollar(v):
        if v is None: return "---"
        return f"${v:,.2f}"

    cat_rows = ""
    for cat in data["category_summary"]:
        cat_rows += f"""<tr>
            <td>{cat['category']}</td>
            <td style="text-align:right;font-weight:bold;">{cat['weighted_pct_medicare']}%</td>
            <td style="text-align:right;">{cat['median_pct_medicare']}%</td>
            <td style="text-align:right;">{cat['min_pct_medicare']}%-{cat['max_pct_medicare']}%</td>
            <td style="text-align:right;">{cat['n_codes']}</td>
            <td style="text-align:right;">{(cat['total_claims'] or 0):,}</td>
        </tr>"""

    cat_tables = ""
    for cat_name in ["Primary Care", "OB-GYN", "Outpatient MH/SUD"]:
        rows = sorted(
            [r for r in data["statewide"] if r["category"] == cat_name],
            key=lambda r: r["hcpcs_code"],
        )
        table_rows = ""
        for r in rows:
            pct = r["pct_of_medicare_avg"]
            pct_str = f"{pct}%" if pct is not None else "---"
            med_str = dollar(r["medicaid_rate"]) if r["has_medicaid_rate"] else "---"
            mcr_str = dollar(r["medicare_nf_rate_avg"])
            claims = "*" if r["is_suppressed"] else f"{(r['total_claims'] or 0):,}"
            benes = "*" if r["is_suppressed"] else f"{(r['unique_beneficiaries'] or 0):,}"
            bg = pct_bg(pct)
            clr = pct_color(pct)
            table_rows += f"""<tr>
                <td>{r['hcpcs_code']}</td>
                <td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;">{r['description']}</td>
                <td style="text-align:right;font-family:monospace;">{med_str}</td>
                <td style="text-align:right;font-family:monospace;">{mcr_str}</td>
                <td style="text-align:right;font-family:monospace;font-weight:bold;background:{bg};color:{clr};border-radius:3px;">{pct_str}</td>
                <td style="text-align:right;font-family:monospace;">{claims}</td>
                <td style="text-align:right;font-family:monospace;">{benes}</td>
            </tr>"""

        cat_tables += f"""
        <h3 style="margin-top:24px;">{cat_name}</h3>
        <table>
            <thead><tr>
                <th>Code</th><th>Description</th>
                <th style="text-align:right;">Medicaid</th><th style="text-align:right;">Medicare</th>
                <th style="text-align:right;">% of MCR</th>
                <th style="text-align:right;">Claims</th><th style="text-align:right;">Bene</th>
            </tr></thead>
            <tbody>{table_rows}</tbody>
        </table>"""

    no_rate_html = ""
    if data["codes_no_rate"]:
        nr_rows = ""
        for r in data["codes_no_rate"]:
            nr_rows += f"<tr><td>{r['hcpcs_code']}</td><td>{r['description']}</td><td style='text-align:right;font-family:monospace;'>{dollar(r['medicare_nf_rate_avg'])}</td></tr>"
        no_rate_html = f"""
        <h3 style="margin-top:24px;">{len(data['codes_no_rate'])} E/M Codes Without Medicaid Rate</h3>
        <table><thead><tr><th>Code</th><th>Description</th><th style="text-align:right;">Medicare Rate (Avg)</th></tr></thead>
        <tbody>{nr_rows}</tbody></table>
        <p style="font-size:12px;color:#6b7280;margin-top:8px;">These codes do not appear on the state Medicaid fee schedule.</p>"""

    loc_rows = ""
    for r in data["category_locality_summary"]:
        loc_rows += f"""<tr>
            <td>{r['category']}</td><td>{r['locality_name']}</td>
            <td style="text-align:right;">{r['n_codes']}</td>
            <td style="text-align:right;font-weight:bold;">{r['weighted_pct_medicare']}%</td>
            <td style="text-align:right;">{r['median_pct_medicare']}%</td>
            <td style="text-align:right;">{r['min_pct_medicare']}%</td>
            <td style="text-align:right;">{r['max_pct_medicare']}%</td>
            <td style="text-align:right;">{(r['total_claims'] or 0):,}</td>
        </tr>"""

    n_loc = m['n_localities']
    loc_word = 'y' if n_loc == 1 else 'ies'
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>CPRA Report — {m['state_name']}</title>
<style>
  body {{ font-family: 'Times New Roman', Georgia, serif; max-width: 900px; margin: 0 auto; padding: 40px; color: #0A2540; line-height: 1.6; font-size: 12px; }}
  h1 {{ font-size: 22px; text-align: center; margin-bottom: 4px; }}
  h2 {{ font-size: 16px; border-bottom: 2px solid #0A2540; padding-bottom: 4px; margin-top: 28px; }}
  h3 {{ font-size: 14px; margin-top: 16px; }}
  .subtitle {{ text-align: center; font-size: 14px; color: #666; margin-bottom: 24px; }}
  .meta {{ text-align: center; font-size: 11px; color: #999; margin-bottom: 32px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 11px; margin-top: 8px; page-break-inside: auto; }}
  th {{ text-align: left; padding: 6px 8px; border-bottom: 2px solid #333; font-size: 10px; text-transform: uppercase; letter-spacing: 0.3px; }}
  td {{ padding: 4px 8px; border-bottom: 1px solid #e5e7eb; }}
  tr {{ page-break-inside: avoid; }}
  .summary-box {{ background: #f5f7f5; border: 1px solid #d1d5db; border-radius: 6px; padding: 16px; margin: 16px 0; }}
  .note {{ font-size: 11px; color: #6b7280; font-style: italic; }}
  @media print {{ body {{ font-size: 11px; padding: 20px; }} h1 {{ font-size: 18px; }} }}
  @page {{ margin: 0.75in; }}
</style>
</head><body>

<h1>Comparative Payment Rate Analysis</h1>
<div class="subtitle">{m['state_name']} Medicaid FFS vs Medicare Non-Facility Rates | CY {m['rate_year']}</div>
<div class="meta">42 CFR 447.203(b)(2)-(3) | Generated {date.today().strftime('%B %d, %Y')} | Medicare PFS CF = ${m['conversion_factor']}</div>

<h2>1. Executive Summary</h2>
<p>This Comparative Payment Rate Analysis compares {m['state_name']} Medicaid fee-for-service base payment rates to Medicare non-facility rates for {m['n_codes']} evaluation and management (E/M) codes from the official CMS CY {m['rate_year']} E/M Code List.</p>
<p>Of {m['n_codes']} codes, <strong>{m['n_with_rate']}</strong> have a published Medicaid rate; <strong>{m['n_without_rate']}</strong> do not appear on the fee schedule.</p>

<div class="summary-box">
<table>
    <thead><tr><th>Category</th><th style="text-align:right;">Wtd Avg % MCR</th><th style="text-align:right;">Median %</th><th style="text-align:right;">Range</th><th style="text-align:right;">Codes</th><th style="text-align:right;">Claims</th></tr></thead>
    <tbody>{cat_rows}</tbody>
</table>
</div>

<h2>2. Methodology</h2>
<p><strong>Medicare rate formula:</strong> Rate = [(Work RVU x Work GPCI) + (PE RVU<sub>NF</sub> x PE GPCI) + (MP RVU x MP GPCI)] x ${m['conversion_factor']}</p>
<p><strong>Medicare localities:</strong> {m['state_name']} has {n_loc} Medicare payment localit{loc_word}. Statewide averages equal-weight across localities.</p>
<p><strong>Small cell suppression:</strong> Beneficiary counts 1-10 suppressed (*). Suppressed rows excluded from weighted averages.</p>
<p><strong>Data sources:</strong> Medicare rates from CMS PFS RVU file (CY {m['rate_year']}). Medicaid rates and utilization from state-provided data.</p>

<h2>3. Results by Service Category</h2>
{cat_tables}

{no_rate_html}

<h2>4. Summary by Category and Locality</h2>
<table>
    <thead><tr><th>Category</th><th>Locality</th><th style="text-align:right;">Codes</th><th style="text-align:right;">Wtd Avg %</th><th style="text-align:right;">Median %</th><th style="text-align:right;">Min %</th><th style="text-align:right;">Max %</th><th style="text-align:right;">Claims</th></tr></thead>
    <tbody>{loc_rows}</tbody>
</table>

<hr style="margin-top:32px;">
<p class="note">Generated by Aradune CPRA Generator. E/M codes per official CMS CY {m['rate_year']} E/M Code List (42 CFR 447.203). Medicare conversion factor: ${m['conversion_factor']} (non-QPP). {m['n_code_category_pairs']} code-category pairs analyzed.</p>
</body></html>"""
