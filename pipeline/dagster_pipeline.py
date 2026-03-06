"""
Aradune Dagster Pipeline — Orchestrates the entire data lake build.

Assets:
  Dimensions: dim_state, dim_procedure, dim_medicare_locality, dim_time, dim_provider_taxonomy
  Facts:      fact_medicaid_rate, fact_medicare_rate, fact_rate_comparison, fact_dq_flag,
              fact_enrollment, fact_quality_measure, fact_expenditure,
              fact_claims, fact_claims_monthly, fact_provider
  Exports:    export_frontend (reads lake, writes public/data/)

Usage:
  # Run everything:
  dagster asset materialize --select '*' -m pipeline.dagster_pipeline

  # Run just dimensions:
  dagster asset materialize --select 'dim_*' -m pipeline.dagster_pipeline

  # Run Dagster UI:
  dagster dev -m pipeline.dagster_pipeline

  # Run specific assets:
  dagster asset materialize --select 'fact_rate_comparison' -m pipeline.dagster_pipeline
"""

import json
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Definitions,
    HookContext,
    MetadataValue,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    failure_hook,
    success_hook,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
DIM_DIR = LAKE_DIR / "dimension"
FACT_DIR = LAKE_DIR / "fact"
PUBLIC_DATA = PROJECT_ROOT / "public" / "data"


def _run_script(script_name: str, args: list[str] = None) -> subprocess.CompletedProcess:
    """Run a build script and return the result."""
    cmd = [sys.executable, str(SCRIPTS_DIR / script_name)] + (args or [])
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    return result


def _parquet_row_count(path: Path) -> int:
    """Count rows in a Parquet file using DuckDB."""
    if not path.exists():
        return 0
    con = duckdb.connect()
    count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    con.close()
    return count


def _latest_snapshot(fact_name: str) -> Path | None:
    """Find the latest snapshot Parquet for a fact table."""
    fact_path = FACT_DIR / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


# ---------------------------------------------------------------------------
# DIMENSION ASSETS
# ---------------------------------------------------------------------------

@asset(group_name="dimensions", description="Build all dimension tables from SQLite + CPRA DuckDB")
def dimensions(context: AssetExecutionContext):
    """Build dim_state, dim_procedure, dim_medicare_locality, dim_time, dim_provider_taxonomy."""
    result = _run_script("build_dimensions.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_dimensions.py failed:\n{result.stderr}")

    # Collect metadata
    row_counts = {}
    for f in DIM_DIR.glob("*.parquet"):
        name = f.stem
        rows = _parquet_row_count(f)
        row_counts[name] = rows

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


# ---------------------------------------------------------------------------
# FACT ASSETS — CPRA Pipeline
# ---------------------------------------------------------------------------

@asset(
    group_name="facts_cpra",
    deps=[AssetKey("dimensions")],
    description="Build CPRA fact tables: medicaid_rate, medicare_rate, rate_comparison, dq_flag, etc.",
)
def facts_cpra(context: AssetExecutionContext):
    """Build all CPRA-sourced fact tables."""
    result = _run_script("build_facts.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_facts.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["medicaid_rate", "medicare_rate", "medicare_rate_state",
                       "rate_comparison", "dq_flag", "enrollment",
                       "quality_measure", "expenditure"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


# ---------------------------------------------------------------------------
# FACT ASSETS — T-MSIS Pipeline
# ---------------------------------------------------------------------------

@asset(
    group_name="facts_tmsis",
    deps=[AssetKey("dimensions")],
    description="Ingest T-MSIS Parquet files into the data lake.",
)
def facts_tmsis(context: AssetExecutionContext):
    """Ingest claims, claims_monthly, categories, providers from R pipeline output."""
    result = _run_script("build_facts_tmsis.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_facts_tmsis.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["claims", "claims_monthly", "claims_categories", "provider"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


# ---------------------------------------------------------------------------
# FACT ASSETS — CMS Supplemental Data
# ---------------------------------------------------------------------------

@asset(
    group_name="facts_economic",
    deps=[AssetKey("dimensions")],
    description="Ingest BLS OEWS wage data for Medicaid-relevant occupations.",
)
def facts_bls(context: AssetExecutionContext):
    """Ingest BLS economic indicators into the data lake."""
    result = _run_script("build_lake_bls.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_bls.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["bls_wage", "bls_wage_msa", "bls_wage_national"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_pbj",
    deps=[AssetKey("dimensions")],
    description="Ingest PBJ nursing facility staffing data from CMS.",
)
def facts_pbj(context: AssetExecutionContext):
    """Ingest Payroll-Based Journal daily staffing data."""
    result = _run_script("build_lake_pbj.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_pbj.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["pbj_nurse_staffing", "pbj_nonnurse_staffing", "pbj_employee"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_enrollment",
    deps=[AssetKey("dimensions")],
    description="Ingest Medicaid enrollment, eligibility, unwinding, and managed care plan data.",
)
def facts_enrollment(context: AssetExecutionContext):
    """Ingest enrollment/eligibility/unwinding data from data.medicaid.gov."""
    result = _run_script("build_lake_enrollment.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_enrollment.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["eligibility", "new_adult", "unwinding", "mc_enrollment"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_hcris",
    deps=[AssetKey("dimensions")],
    description="Ingest HCRIS hospital and SNF cost report data.",
)
def facts_hcris(context: AssetExecutionContext):
    """Ingest hospital and nursing facility financial data from CMS cost reports."""
    result = _run_script("build_lake_hcris.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_hcris.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["hospital_cost", "snf_cost"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_quality",
    deps=[AssetKey("dimensions")],
    description="Ingest Five-Star ratings, HAC measures, and Provider of Services data.",
)
def facts_quality(context: AssetExecutionContext):
    """Ingest quality and facility characteristic data."""
    result = _run_script("build_lake_quality.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_quality.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["five_star", "hac_measure", "pos_hospital", "pos_other"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_hospital_quality",
    deps=[AssetKey("dimensions")],
    description="Ingest hospital ratings, VBP scores, HRRP data, and EPSDT participation.",
)
def facts_hospital_quality(context: AssetExecutionContext):
    """Ingest hospital quality and EPSDT data."""
    result = _run_script("build_lake_hospital_quality.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_hospital_quality.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["hospital_rating", "hospital_vbp", "hospital_hrrp", "epsdt"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_supplemental",
    deps=[AssetKey("dimensions")],
    description="Ingest Care Compare, Medicaid supplemental, economic, HPSA, scorecard, HAI, NH ownership data.",
)
def facts_supplemental(context: AssetExecutionContext):
    """Ingest all supplemental datasets."""
    scripts = [
        ("build_lake_care_compare.py", ["mspb_state", "timely_effective", "complications",
            "unplanned_visits", "dialysis_state", "home_health_state"]),
        ("build_lake_medicaid_supplemental.py", ["mltss", "financial_mgmt",
            "eligibility_levels", "aca_ful", "dq_atlas"]),
        ("build_lake_economic.py", ["cpi", "unemployment", "median_income", "mspb_hospital"]),
        ("build_lake_hpsa.py", ["hpsa"]),
        ("build_lake_scorecard.py", ["scorecard", "elig_group_monthly", "elig_group_annual",
            "cms64_new_adult", "ffcra_fmap", "mc_enroll_pop", "mc_enroll_duals", "hai_state"]),
        ("build_lake_hai_ownership.py", ["hai_hospital", "nh_ownership"]),
        ("build_lake_supplemental.py", ["fmr_supplemental", "macpac_supplemental"]),
    ]
    row_counts = {}
    for script_name, fact_names in scripts:
        result = _run_script(script_name)
        context.log.info(result.stdout)
        if result.returncode != 0:
            context.log.warning(f"{script_name} failed:\n{result.stderr}")
            continue
        for fact_name in fact_names:
            p = _latest_snapshot(fact_name)
            if p:
                row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


@asset(
    group_name="facts_cms",
    deps=[AssetKey("dimensions")],
    description="Migrate CMS supplemental data: SDUD, NADAC, managed care, DSH, FMAP, SPAs, waivers, NCCI.",
)
def facts_cms(context: AssetExecutionContext):
    """Migrate all CMS supplemental data from SQLite to the lake."""
    result = _run_script("build_lake_cms.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"build_lake_cms.py failed:\n{result.stderr}")

    row_counts = {}
    for fact_name in ["drug_utilization", "nadac", "managed_care",
                       "dsh_payment", "fmap", "spa"]:
        p = _latest_snapshot(fact_name)
        if p:
            row_counts[fact_name] = _parquet_row_count(p)

    return {
        "row_counts": row_counts,
        "total_rows": sum(row_counts.values()),
    }


# ---------------------------------------------------------------------------
# EXPORT ASSET
# ---------------------------------------------------------------------------

@asset(
    group_name="export",
    deps=[AssetKey("facts_cpra"), AssetKey("facts_tmsis"), AssetKey("facts_cms"), AssetKey("facts_bls"), AssetKey("facts_hcris"), AssetKey("facts_enrollment"), AssetKey("facts_pbj"), AssetKey("facts_quality"), AssetKey("facts_hospital_quality"), AssetKey("facts_supplemental")],
    description="Export validated data from lake to public/data/ for the frontend.",
)
def frontend_export(context: AssetExecutionContext):
    """Run quality gates and export to public/data/."""
    result = _run_script("export_frontend.py")
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"export_frontend.py failed:\n{result.stderr}")

    # Report file sizes
    files = {}
    for name in ["cpra_em.json", "cpra_summary.json", "dq_flags_em.json",
                  "dim_447_codes.json", "medicare_rates.json", "conversion_factors.json"]:
        f = PUBLIC_DATA / name
        if f.exists():
            files[name] = f.stat().st_size

    return {"files": files}


# ---------------------------------------------------------------------------
# ASSET CHECKS
# ---------------------------------------------------------------------------

@asset_check(asset=AssetKey("facts_cpra"), description="Validate CPRA rate comparison data")
def check_rate_comparison(context):
    """Verify rate_comparison has enough states and plausible values."""
    p = _latest_snapshot("rate_comparison")
    if not p:
        return AssetCheckResult(passed=False, metadata={"error": "No snapshot found"})

    con = duckdb.connect()
    states = con.execute(f"SELECT COUNT(DISTINCT state_code) FROM '{p}'").fetchone()[0]
    median = con.execute(f"""
        SELECT MEDIAN(pct_of_medicare)
        FROM '{p}'
        WHERE pct_of_medicare > 0 AND pct_of_medicare < 1000
    """).fetchone()[0]
    em_count = con.execute(f"SELECT COUNT(*) FROM '{p}' WHERE em_category IS NOT NULL").fetchone()[0]
    con.close()

    passed = states >= 30 and 50 < median < 200 and em_count >= 1000
    return AssetCheckResult(
        passed=passed,
        metadata={
            "states": MetadataValue.int(states),
            "median_pct_of_medicare": MetadataValue.float(float(median)),
            "em_rows": MetadataValue.int(em_count),
        },
    )


@asset_check(asset=AssetKey("facts_cpra"), description="Verify Medicare CF is $33.4009")
def check_conversion_factor(context):
    """Ensure the correct conversion factor is used."""
    dp = DIM_DIR / "dim_procedure.parquet"
    if not dp.exists():
        return AssetCheckResult(passed=False, metadata={"error": "dim_procedure.parquet not found"})

    con = duckdb.connect()
    cfs = con.execute(f"""
        SELECT DISTINCT conversion_factor
        FROM '{dp}'
        WHERE conversion_factor IS NOT NULL
    """).fetchall()
    con.close()

    cf_values = [float(c[0]) for c in cfs]
    passed = len(cf_values) == 1 and abs(cf_values[0] - 33.4009) < 0.01
    return AssetCheckResult(
        passed=passed,
        metadata={"conversion_factors": MetadataValue.text(str(cf_values))},
    )


@asset_check(asset=AssetKey("facts_tmsis"), description="Validate T-MSIS claims data")
def check_claims(context):
    """Verify claims data has expected volume."""
    p = _latest_snapshot("claims")
    if not p:
        return AssetCheckResult(passed=False, metadata={"error": "No snapshot found"})

    con = duckdb.connect()
    rows = con.execute(f"SELECT COUNT(*) FROM '{p}'").fetchone()[0]
    states = con.execute(f"SELECT COUNT(DISTINCT state_code) FROM '{p}'").fetchone()[0]
    con.close()

    passed = rows >= 500000 and states >= 50
    return AssetCheckResult(
        passed=passed,
        metadata={
            "rows": MetadataValue.int(rows),
            "states": MetadataValue.int(states),
        },
    )


# ---------------------------------------------------------------------------
# JOBS & SCHEDULES
# ---------------------------------------------------------------------------

# Full pipeline: dimensions -> facts -> export
full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection="*",
    description="Run the complete Aradune data pipeline: dimensions, facts, and frontend export.",
)

# CPRA-only: dimensions -> cpra facts -> export
cpra_refresh_job = define_asset_job(
    name="cpra_refresh",
    selection=["dimensions", "facts_cpra", "frontend_export"],
    description="Refresh CPRA data only (fee schedules, rate comparisons).",
)

# Weekly full rebuild
weekly_full_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 2 * * 0",  # Sunday at 2am
    name="weekly_full_rebuild",
    description="Full pipeline rebuild every Sunday at 2am.",
)

# Daily CPRA refresh (fee schedules change more often)
daily_cpra_schedule = ScheduleDefinition(
    job=cpra_refresh_job,
    cron_schedule="0 6 * * *",  # Daily at 6am
    name="daily_cpra_refresh",
    description="Refresh CPRA rate data daily at 6am.",
)


# ---------------------------------------------------------------------------
# S3 SYNC ASSET
# ---------------------------------------------------------------------------

@asset(
    group_name="deploy",
    deps=[AssetKey("frontend_export")],
    description="Upload lake data to S3 for production API.",
)
def s3_sync(context: AssetExecutionContext):
    """Upload the data lake to S3."""
    bucket = os.environ.get("ARADUNE_S3_BUCKET")
    if not bucket:
        context.log.warning("ARADUNE_S3_BUCKET not set — skipping S3 sync")
        return {"skipped": True}

    result = _run_script("sync_lake.py", ["upload"])
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"sync_lake.py upload failed:\n{result.stderr}")
    return {"uploaded": True}


# ---------------------------------------------------------------------------
# ALERTING HOOKS
# ---------------------------------------------------------------------------

def _send_alert(title: str, message: str):
    """Send alert via configured channel (webhook URL from env)."""
    webhook_url = os.environ.get("ARADUNE_ALERT_WEBHOOK")
    if not webhook_url:
        return

    import urllib.request
    payload = json.dumps({"text": f"*{title}*\n{message}"}).encode()
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # Don't fail the pipeline over alerting


@failure_hook
def alert_on_failure(context: HookContext):
    """Send alert when any asset fails."""
    _send_alert(
        "Aradune Pipeline Failure",
        f"Asset `{context.op.name}` failed in job `{context.job_name}`.\n"
        f"Run ID: `{context.run_id}`",
    )


@success_hook
def alert_on_success(context: HookContext):
    """Send alert when full pipeline completes."""
    if context.op.name == "s3_sync":
        _send_alert(
            "Aradune Pipeline Complete",
            f"Full pipeline run `{context.run_id}` completed successfully. "
            f"Lake synced to S3.",
        )


# ---------------------------------------------------------------------------
# JOBS & SCHEDULES (updated with hooks + s3_sync)
# ---------------------------------------------------------------------------

full_pipeline_with_deploy = define_asset_job(
    name="full_pipeline_deploy",
    selection="*",
    description="Full pipeline with S3 deploy.",
    hooks={alert_on_failure, alert_on_success},
)


# ---------------------------------------------------------------------------
# DEFINITIONS
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[dimensions, facts_cpra, facts_tmsis, facts_cms, facts_bls, facts_hcris, facts_enrollment, facts_pbj, facts_quality, facts_hospital_quality, facts_supplemental, frontend_export, s3_sync],
    asset_checks=[check_rate_comparison, check_conversion_factor, check_claims],
    jobs=[full_pipeline_job, cpra_refresh_job, full_pipeline_with_deploy],
    schedules=[weekly_full_schedule, daily_cpra_schedule],
)
