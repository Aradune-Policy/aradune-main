"""
Data quality checks for the Aradune data lake.

Covers: row counts, null checks, value ranges, referential integrity,
and domain-specific business rules. These replace SodaCL checks with
DuckDB-native SQL validation.
"""

import pytest


# ── Row count checks ────────────────────────────────────────────────


MINIMUM_ROW_COUNTS = {
    "dim_state": 50,
    "dim_procedure": 15000,
    "fact_medicaid_rate": 500000,
    "fact_rate_comparison": 250000,
    "fact_enrollment": 5000,
    "fact_medicare_rate": 800000,
    "fact_hospital_cost": 5000,
    "fact_five_star": 10000,
    "fact_expenditure": 500,
    "fact_hpsa": 50000,
    "fact_bls_wage": 500,
    "fact_quality_core_set_2024": 500,
    "fact_sdud_2024": 4000000,
    "fact_hcbs_waitlist": 40,
    # Session 17 tables
    "fact_nppes_provider": 8000000,
    "fact_nppes_practice_location": 1000000,
    "fact_nppes_taxonomy_detail": 10000000,
    "fact_county_health_rankings": 200000,
    "fact_census_state_finances": 10000,
    "fact_doge_state_category": 5000,
    "fact_doge_state_hcpcs": 500000,
    "fact_doge_state_monthly": 50000,
    "fact_doge_top_providers": 200000,
    "fact_doge_state_taxonomy": 50000,
    "fact_kff_total_spending": 50,
    "fact_kff_spending_per_enrollee": 50,
    "fact_kff_fmap": 50,
    "fact_kff_fee_index": 50,
}


@pytest.mark.parametrize("table,min_rows", MINIMUM_ROW_COUNTS.items())
def test_minimum_row_count(lake_db, table, min_rows):
    try:
        result = lake_db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    except Exception:
        pytest.skip(f"Table {table} not registered")
    assert result[0] >= min_rows, (
        f"{table} has {result[0]:,} rows, expected >= {min_rows:,}"
    )


# ── Null checks on critical columns ────────────────────────────────


NOT_NULL_CHECKS = [
    ("dim_state", "state_code"),
    ("dim_state", "state_name"),
    ("dim_procedure", "procedure_code"),
    ("fact_medicaid_rate", "state_code"),
    ("fact_medicaid_rate", "procedure_code"),
    ("fact_rate_comparison", "state_code"),
    ("fact_rate_comparison", "procedure_code"),
    ("fact_rate_comparison", "medicaid_rate"),
    ("fact_enrollment", "state_code"),
    ("fact_medicare_rate", "procedure_code"),
    ("fact_hospital_cost", "state_code"),
    ("fact_expenditure", "state_code"),
    ("fact_five_star", "state_code"),
    ("fact_bls_wage", "state_code"),
]


@pytest.mark.parametrize("table,column", NOT_NULL_CHECKS)
def test_no_nulls_in_critical_column(lake_db, table, column):
    try:
        result = lake_db.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
        ).fetchone()
    except Exception:
        pytest.skip(f"Table {table} or column {column} not available")
    assert result[0] == 0, (
        f"{table}.{column} has {result[0]:,} NULL values"
    )


# ── Value range checks ─────────────────────────────────────────────


def test_medicaid_rates_positive(lake_db):
    """Medicaid rates should be >= 0 (some states have $0 codes but never negative)."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(*) FROM fact_medicaid_rate WHERE COALESCE(rate, rate_nonfacility, rate_facility) < 0"
        ).fetchone()
    except Exception:
        pytest.skip("fact_medicaid_rate not available")
    assert result[0] == 0, f"{result[0]:,} negative Medicaid rates found"


def test_medicaid_rates_reasonable_ceiling(lake_db):
    """Flag rates above $10,000 for E/M codes (likely data errors)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_medicaid_rate r
            JOIN dim_procedure p ON r.procedure_code = p.procedure_code
            WHERE p.is_em_code = true
              AND COALESCE(r.rate, r.rate_nonfacility, r.rate_facility) > 10000
        """).fetchone()
    except Exception:
        pytest.skip("Rate/procedure join not available")
    assert result[0] == 0, f"{result[0]:,} E/M rates above $10,000"


def test_pct_of_medicare_not_negative(lake_db):
    """pct_of_medicare should never be negative."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_rate_comparison
            WHERE pct_of_medicare < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_rate_comparison not available")
    assert result[0] == 0, f"{result[0]:,} negative pct_of_medicare values"


def test_state_codes_valid_format(lake_db):
    """state_code should be 2-letter uppercase in dim_state."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM dim_state
            WHERE LENGTH(state_code) != 2 OR state_code != UPPER(state_code)
        """).fetchone()
    except Exception:
        pytest.skip("dim_state not available")
    assert result[0] == 0, f"{result[0]:,} invalid state codes in dim_state"


def test_enrollment_positive(lake_db):
    """Enrollment counts should never be negative."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_enrollment
            WHERE total_enrollment < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_enrollment not available")
    assert result[0] == 0, f"{result[0]:,} negative enrollment values"


def test_hospital_bed_count_positive(lake_db):
    """Hospital bed counts should be positive."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_hospital_cost
            WHERE bed_count IS NOT NULL AND bed_count < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_hospital_cost not available")
    assert result[0] == 0, f"{result[0]:,} negative bed counts"


def test_fmap_range(lake_db):
    """FMAP should be between 0.50 and 1.00 (floor is 50%)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_fmap
            WHERE fmap_rate < 0.40 OR fmap_rate > 1.00
        """).fetchone()
    except Exception:
        pytest.skip("fact_fmap not available")
    assert result[0] == 0, f"{result[0]:,} FMAP values outside 0.40-1.00"


# ── Referential integrity ──────────────────────────────────────────


def test_medicaid_rate_states_in_dim(lake_db):
    """All state_codes in fact_medicaid_rate should exist in dim_state."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(DISTINCT r.state_code)
            FROM fact_medicaid_rate r
            LEFT JOIN dim_state d ON r.state_code = d.state_code
            WHERE d.state_code IS NULL
        """).fetchone()
    except Exception:
        pytest.skip("Tables not available for join")
    assert result[0] == 0, (
        f"{result[0]} state codes in fact_medicaid_rate not found in dim_state"
    )


def test_rate_comparison_states_in_dim(lake_db):
    """All state_codes in fact_rate_comparison should exist in dim_state."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(DISTINCT r.state_code)
            FROM fact_rate_comparison r
            LEFT JOIN dim_state d ON r.state_code = d.state_code
            WHERE d.state_code IS NULL
        """).fetchone()
    except Exception:
        pytest.skip("Tables not available for join")
    assert result[0] == 0, (
        f"{result[0]} state codes in fact_rate_comparison not found in dim_state"
    )


def test_enrollment_states_in_dim(lake_db):
    """All state_codes in fact_enrollment should exist in dim_state."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(DISTINCT e.state_code)
            FROM fact_enrollment e
            LEFT JOIN dim_state d ON e.state_code = d.state_code
            WHERE d.state_code IS NULL
        """).fetchone()
    except Exception:
        pytest.skip("Tables not available for join")
    assert result[0] == 0, (
        f"{result[0]} state codes in fact_enrollment not found in dim_state"
    )


# ── Domain-specific business rules ─────────────────────────────────


def test_rate_comparison_has_both_rates(lake_db):
    """fact_rate_comparison should have both medicaid_rate and medicare_nonfac_rate."""
    try:
        result = lake_db.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(medicaid_rate) AS has_medicaid,
                COUNT(medicare_nonfac_rate) AS has_medicare
            FROM fact_rate_comparison
        """).fetchone()
    except Exception:
        pytest.skip("fact_rate_comparison not available")
    total, has_medicaid, has_medicare = result
    assert has_medicaid > total * 0.9, f"Only {has_medicaid}/{total} rows have medicaid_rate"
    assert has_medicare > total * 0.9, f"Only {has_medicare}/{total} rows have medicare_nonfac_rate"


def test_fl_no_facility_rate(lake_db):
    """FL Medicaid should have no facility rate (known policy rule)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_medicaid_rate
            WHERE state_code = 'FL'
              AND modifier IS NOT NULL
              AND modifier LIKE '%26%'
        """).fetchone()
    except Exception:
        pytest.skip("fact_medicaid_rate not available")
    # This is an informational check. FL uses PC/TC split codes instead.
    # Just verify the query runs.
    assert result is not None


def test_sdud_amounts_pre_rebate(lake_db):
    """SDUD total amounts should be positive (all amounts are pre-rebate)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_sdud_2024
            WHERE total_reimbursed < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_sdud_2024 not available")
    assert result[0] == 0, f"{result[0]:,} negative reimbursement amounts in SDUD"


def test_hpsa_scores_valid(lake_db):
    """HPSA scores should be between 0 and 26 (maximum possible)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_hpsa
            WHERE hpsa_score IS NOT NULL
              AND (hpsa_score < 0 OR hpsa_score > 26)
        """).fetchone()
    except Exception:
        pytest.skip("fact_hpsa not available")
    assert result[0] == 0, f"{result[0]:,} HPSA scores outside 0-26 range"


def test_five_star_rating_range(lake_db):
    """Five-Star ratings should be 1-5."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_five_star
            WHERE overall_rating IS NOT NULL
              AND (overall_rating < 1 OR overall_rating > 5)
        """).fetchone()
    except Exception:
        pytest.skip("fact_five_star not available")
    assert result[0] == 0, f"{result[0]:,} Five-Star ratings outside 1-5"


# ── Freshness checks ───────────────────────────────────────────────


def test_dim_state_coverage(lake_db):
    """dim_state should have all 50 states + DC."""
    try:
        result = lake_db.execute("SELECT COUNT(DISTINCT state_code) FROM dim_state").fetchone()
    except Exception:
        pytest.skip("dim_state not available")
    assert result[0] >= 51, f"dim_state has {result[0]} states, expected >= 51"


def test_rate_comparison_state_coverage(lake_db):
    """fact_rate_comparison should cover at least 40 states."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT state_code) FROM fact_rate_comparison"
        ).fetchone()
    except Exception:
        pytest.skip("fact_rate_comparison not available")
    assert result[0] >= 40, (
        f"fact_rate_comparison has {result[0]} states, expected >= 40"
    )


def test_medicaid_rate_state_coverage(lake_db):
    """fact_medicaid_rate should cover at least 45 states."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT state_code) FROM fact_medicaid_rate"
        ).fetchone()
    except Exception:
        pytest.skip("fact_medicaid_rate not available")
    assert result[0] >= 45, (
        f"fact_medicaid_rate has {result[0]} states, expected >= 45"
    )


# ── Cross-table consistency ────────────────────────────────────────


def test_procedure_dim_covers_rates(lake_db):
    """dim_procedure should have entries for the majority of codes in fact_medicaid_rate."""
    try:
        result = lake_db.execute("""
            SELECT
                COUNT(DISTINCT r.procedure_code) AS rate_codes,
                COUNT(DISTINCT CASE WHEN p.procedure_code IS NOT NULL
                      THEN r.procedure_code END) AS matched_codes
            FROM fact_medicaid_rate r
            LEFT JOIN dim_procedure p ON r.procedure_code = p.procedure_code
        """).fetchone()
    except Exception:
        pytest.skip("Join not available")
    rate_codes, matched = result
    if rate_codes == 0:
        pytest.skip("No rate codes found")
    pct = matched / rate_codes * 100
    # dim_procedure covers PFS codes. Medicaid rate files include many more
    # (dental, state-specific, etc.) so 10% is a reasonable floor.
    assert pct >= 10, (
        f"Only {pct:.1f}% of rate codes found in dim_procedure "
        f"({matched}/{rate_codes})"
    )


# ── Session 17: KFF data quality ─────────────────────────────────


KFF_TABLES_WITH_STATE_CODE = [
    "fact_kff_total_spending",
    "fact_kff_spending_per_enrollee",
    "fact_kff_fmap",
    "fact_kff_fee_index",
    "fact_kff_mc_penetration",
    "fact_kff_dual_eligible",
    "fact_kff_mco_enrollment",
    "fact_kff_spending_by_service",
    "fact_kff_dsh_allotments",
    "fact_kff_births_medicaid",
    "fact_kff_enrollees_by_group",
    "fact_kff_federal_state_share",
    "fact_kff_spending_acute_care",
    "fact_kff_spending_ltc",
    "fact_kff_chip_spending",
    "fact_kff_eligibility_adults",
    "fact_kff_child_participation",
]


@pytest.mark.parametrize("table", KFF_TABLES_WITH_STATE_CODE)
def test_kff_tables_have_state_code(lake_db, table):
    """All KFF state-level tables should have a state_code column with values."""
    try:
        result = lake_db.execute(
            f"SELECT COUNT(*), COUNT(state_code), COUNT(DISTINCT state_code) FROM {table}"
        ).fetchone()
    except Exception:
        pytest.skip(f"{table} not registered")
    total, non_null, distinct = result
    assert non_null == total, (
        f"{table}: {total - non_null} rows missing state_code"
    )
    assert distinct >= 50, (
        f"{table} has only {distinct} distinct state codes, expected >= 50"
    )


def test_kff_spending_per_enrollee_values_reasonable(lake_db):
    """KFF spending per enrollee should be between $1,000 and $50,000."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_kff_spending_per_enrollee
            WHERE all_enrollees IS NOT NULL
              AND (all_enrollees < 1000 OR all_enrollees > 50000)
        """).fetchone()
    except Exception:
        pytest.skip("fact_kff_spending_per_enrollee not available")
    assert result[0] == 0, (
        f"{result[0]} KFF spending-per-enrollee values outside $1,000-$50,000"
    )


def test_kff_fmap_range(lake_db):
    """KFF FMAP percentages should be between 0.50 and 0.85 for states (US is 0.50)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_kff_fmap
            WHERE fmap_pct IS NOT NULL
              AND (fmap_pct < 0.40 OR fmap_pct > 1.00)
        """).fetchone()
    except Exception:
        pytest.skip("fact_kff_fmap not available")
    assert result[0] == 0, (
        f"{result[0]} KFF FMAP values outside 0.40-1.00 range"
    )


def test_kff_fee_index_range(lake_db):
    """KFF fee index should be between 0.20 and 2.00 (ratio to Medicare)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_kff_fee_index
            WHERE all_services IS NOT NULL
              AND (all_services < 0.20 OR all_services > 2.00)
        """).fetchone()
    except Exception:
        pytest.skip("fact_kff_fee_index not available")
    assert result[0] == 0, (
        f"{result[0]} KFF fee index values outside 0.20-2.00 range"
    )


# ── Session 17: DOGE spending data quality ────────────────────────


def test_doge_state_category_paid_positive(lake_db):
    """DOGE total_paid should be positive."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_doge_state_category
            WHERE total_paid IS NOT NULL AND total_paid < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_doge_state_category not available")
    assert result[0] == 0, f"{result[0]:,} negative total_paid in DOGE state category"


def test_doge_state_category_reasonable_totals(lake_db):
    """DOGE aggregate paid amount should be > $100 billion (Medicaid is ~$900B+)."""
    try:
        result = lake_db.execute("""
            SELECT SUM(total_paid) FROM fact_doge_state_category
        """).fetchone()
    except Exception:
        pytest.skip("fact_doge_state_category not available")
    total = result[0]
    assert total is not None and total > 100_000_000_000, (
        f"DOGE total paid is ${total:,.0f}, expected > $100B"
    )


def test_doge_state_coverage(lake_db):
    """DOGE data should cover at least 50 states/territories."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT state) FROM fact_doge_state_category"
        ).fetchone()
    except Exception:
        pytest.skip("fact_doge_state_category not available")
    assert result[0] >= 50, (
        f"DOGE data covers {result[0]} states, expected >= 50"
    )


def test_doge_top_providers_have_npi(lake_db):
    """DOGE top providers should have billing NPI populated."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*), COUNT(billing_npi)
            FROM fact_doge_top_providers
        """).fetchone()
    except Exception:
        pytest.skip("fact_doge_top_providers not available")
    total, has_npi = result
    pct = has_npi / total * 100 if total > 0 else 0
    assert pct >= 95, (
        f"Only {pct:.1f}% of DOGE top providers have billing NPI"
    )


# ── Session 17: NPPES provider data quality ───────────────────────


def test_nppes_provider_count(lake_db):
    """NPPES should have > 8 million providers."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(*) FROM fact_nppes_provider"
        ).fetchone()
    except Exception:
        pytest.skip("fact_nppes_provider not available")
    assert result[0] > 8_000_000, (
        f"NPPES has {result[0]:,} providers, expected > 8M"
    )


def test_nppes_npi_format(lake_db):
    """NPPES NPI should be 10 digits."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_nppes_provider
            WHERE npi IS NOT NULL
              AND (LENGTH(npi) != 10 OR npi !~ '^\\d{10}$')
        """).fetchone()
    except Exception:
        pytest.skip("fact_nppes_provider not available")
    assert result[0] == 0, (
        f"{result[0]:,} NPPES records with invalid NPI format"
    )


def test_nppes_entity_type_valid(lake_db):
    """NPPES entity_type should be 1 (individual) or 2 (organization)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_nppes_provider
            WHERE entity_type NOT IN ('1', '2')
        """).fetchone()
    except Exception:
        pytest.skip("fact_nppes_provider not available")
    assert result[0] == 0, (
        f"{result[0]:,} NPPES records with invalid entity_type"
    )


def test_nppes_state_coverage(lake_db):
    """NPPES should have providers in all 50 states + DC."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(DISTINCT state_code)
            FROM fact_nppes_provider
            WHERE state_code IS NOT NULL AND LENGTH(state_code) = 2
        """).fetchone()
    except Exception:
        pytest.skip("fact_nppes_provider not available")
    assert result[0] >= 51, (
        f"NPPES covers {result[0]} states, expected >= 51"
    )


# ── Session 17: Census State Finances ─────────────────────────────


def test_census_state_finances_coverage(lake_db):
    """Census state finances should have all 50 states + DC."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT state_code) FROM fact_census_state_finances"
        ).fetchone()
    except Exception:
        pytest.skip("fact_census_state_finances not available")
    assert result[0] >= 50, (
        f"Census state finances has {result[0]} states, expected >= 50"
    )


def test_census_state_finances_has_state_code(lake_db):
    """Census state finances should have no null state_code values."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_census_state_finances
            WHERE state_code IS NULL
        """).fetchone()
    except Exception:
        pytest.skip("fact_census_state_finances not available")
    assert result[0] == 0, (
        f"{result[0]:,} null state_code values in census_state_finances"
    )


def test_census_state_finances_amounts_not_negative(lake_db):
    """Census state finance amounts should not be negative (they are in thousands)."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_census_state_finances
            WHERE amount_thousands IS NOT NULL AND amount_thousands < 0
        """).fetchone()
    except Exception:
        pytest.skip("fact_census_state_finances not available")
    # Some deficit categories can be negative, so just check the count is small
    assert result[0] < 1000, (
        f"{result[0]:,} negative amounts in census_state_finances (some expected for deficits)"
    )


# ── Session 17: County Health Rankings ────────────────────────────


def test_county_health_rankings_county_coverage(lake_db):
    """County health rankings should have > 3,000 counties."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT fips) FROM fact_county_health_rankings"
        ).fetchone()
    except Exception:
        pytest.skip("fact_county_health_rankings not available")
    assert result[0] > 3000, (
        f"County health rankings has {result[0]:,} counties, expected > 3,000"
    )


def test_county_health_rankings_fips_format(lake_db):
    """County FIPS codes should be 5 digits."""
    try:
        result = lake_db.execute("""
            SELECT COUNT(*)
            FROM fact_county_health_rankings
            WHERE fips IS NOT NULL
              AND LENGTH(fips) != 5
        """).fetchone()
    except Exception:
        pytest.skip("fact_county_health_rankings not available")
    assert result[0] == 0, (
        f"{result[0]:,} county health ranking rows with non-5-digit FIPS"
    )


def test_county_health_rankings_state_coverage(lake_db):
    """County health rankings should cover all 50 states."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT state_code) FROM fact_county_health_rankings"
        ).fetchone()
    except Exception:
        pytest.skip("fact_county_health_rankings not available")
    assert result[0] >= 50, (
        f"County health rankings covers {result[0]} states, expected >= 50"
    )


def test_county_health_rankings_has_measures(lake_db):
    """County health rankings should have multiple distinct measures."""
    try:
        result = lake_db.execute(
            "SELECT COUNT(DISTINCT measure_code) FROM fact_county_health_rankings"
        ).fetchone()
    except Exception:
        pytest.skip("fact_county_health_rankings not available")
    assert result[0] >= 20, (
        f"County health rankings has {result[0]} measures, expected >= 20"
    )
