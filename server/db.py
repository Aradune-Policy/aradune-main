"""
DuckDB connection backed by the Aradune data lake (Parquet files).

On startup, creates an in-memory DuckDB and registers lake Parquet files
as views. All queries hit these views — no persistent DuckDB file needed.
"""

import duckdb
from contextlib import contextmanager
from pathlib import Path

from server.config import settings

_conn: duckdb.DuckDBPyConnection | None = None


def _latest_snapshot(fact_dir: Path, fact_name: str) -> Path | None:
    """Find the most recent snapshot Parquet for a fact table."""
    fact_path = fact_dir / fact_name
    if not fact_path.exists():
        return None
    snapshots = sorted(fact_path.glob("snapshot=*/data.parquet"), reverse=True)
    return snapshots[0] if snapshots else None


def init_db() -> None:
    """Create in-memory DuckDB and register lake Parquet files as views."""
    global _conn
    _conn = duckdb.connect()

    lake = Path(settings.lake_dir)
    dim_dir = lake / "dimension"
    fact_dir = lake / "fact"

    # Register dimension tables
    for parquet_file in dim_dir.glob("*.parquet"):
        view_name = parquet_file.stem  # dim_state, dim_procedure, etc.
        _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")

    # Register fact tables (latest snapshot)
    fact_names = [
        "medicaid_rate", "medicare_rate", "medicare_rate_state",
        "rate_comparison", "dq_flag",
        "enrollment", "quality_measure", "expenditure",
        "claims", "claims_monthly", "claims_categories", "provider",
        "drug_utilization", "nadac", "managed_care",
        "dsh_payment", "fmap", "spa",
        "bls_wage", "bls_wage_msa", "bls_wage_national",
        "hospital_cost", "snf_cost",
        "eligibility", "new_adult", "unwinding", "mc_enrollment",
        "pbj_nurse_staffing", "pbj_nonnurse_staffing", "pbj_employee",
        "five_star", "hac_measure", "pos_hospital", "pos_other",
        "hospital_rating", "hospital_vbp", "hospital_hrrp", "epsdt",
        "mspb_state", "timely_effective", "complications",
        "unplanned_visits", "dialysis_state", "home_health_state",
        "mltss", "financial_mgmt", "eligibility_levels",
        "aca_ful", "dq_atlas",
        "cpi", "unemployment", "median_income", "mspb_hospital",
        "hpsa",
        "scorecard", "elig_group_monthly", "elig_group_annual",
        "cms64_new_adult", "ffcra_fmap",
        "mc_enroll_pop", "mc_enroll_duals", "hai_state",
        "hai_hospital", "nh_ownership",
        "acs_state", "drug_overdose", "mortality_trend",
        "state_gdp", "state_population", "nh_penalties",
        "nh_deficiencies", "brfss", "hcahps_state", "imaging_hospital",
        "fmr_supplemental", "macpac_supplemental",
    ]
    for fact_name in fact_names:
        p = _latest_snapshot(fact_dir, fact_name)
        if p:
            view_name = f"fact_{fact_name}"
            _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{p}'")

    # Register reference tables
    ref_dir = lake / "reference"
    if ref_dir.exists():
        for parquet_file in ref_dir.glob("*.parquet"):
            view_name = parquet_file.stem  # ref_drug_rebate, ref_ncci_edits, etc.
            _conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")

    # Create a unified 'spending' view for backward compatibility with query_builder
    # Maps the old column names to the new lake schema
    claims_path = _latest_snapshot(fact_dir, "claims")
    provider_path = _latest_snapshot(fact_dir, "provider")
    if claims_path:
        _conn.execute(f"""
            CREATE VIEW spending AS
            SELECT
                state_code            AS state,
                procedure_code        AS HCPCS_CODE,
                category,
                year,
                month,
                total_paid            AS TOTAL_PAID,
                total_claims          AS TOTAL_CLAIMS,
                total_beneficiaries   AS TOTAL_UNIQUE_BENEFICIARIES,
                provider_count,
                avg_paid_per_claim,
                claim_type,
                LPAD(CAST(year AS VARCHAR), 4, '0') || '-' ||
                    LPAD(COALESCE(CAST(month AS VARCHAR), '01'), 2, '0')
                                      AS CLAIM_FROM_MONTH,
                snapshot_date
            FROM '{claims_path}'
        """)

    if provider_path:
        _conn.execute(f"""
            CREATE VIEW spending_providers AS
            SELECT
                npi                   AS BILLING_PROVIDER_NPI_NUM,
                state_code            AS state,
                provider_name,
                zip3,
                taxonomy_code         AS taxonomy,
                total_paid            AS TOTAL_PAID,
                total_claims          AS TOTAL_CLAIMS,
                total_beneficiaries   AS TOTAL_UNIQUE_BENEFICIARIES,
                code_count
            FROM '{provider_path}'
        """)


def close_db() -> None:
    global _conn
    if _conn:
        _conn.close()
        _conn = None


@contextmanager
def get_cursor():
    """Yield a thread-safe DuckDB cursor from the shared connection."""
    if _conn is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    cursor = _conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
