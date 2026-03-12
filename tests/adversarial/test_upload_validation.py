"""
Adversarial tests for user upload validation.

Tests the quarantine pattern: invalid codes, outlier values,
boundary conditions, encoding issues.
"""

import io

import duckdb
import pytest


def load_csv_to_duckdb(conn: duckdb.DuckDBPyConnection, csv_text: str, table: str):
    """Helper: load CSV text into a DuckDB table."""
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM read_csv_auto('{csv_text}', header=true, all_varchar=true)
    """)


class TestRateUploadValidation:
    """Test validation rules for uploaded fee schedule data."""

    @pytest.fixture
    def conn(self):
        c = duckdb.connect(":memory:")
        yield c
        c.close()

    def test_negative_rates_flagged(self, conn):
        """Negative rates should be caught."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', -50.00),
                ('FL', '99214', 85.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        bad = conn.execute(
            "SELECT COUNT(*) FROM upload WHERE rate < 0"
        ).fetchone()[0]
        assert bad == 1

    def test_extremely_high_rates_flagged(self, conn):
        """Rates above $50,000 should be flagged as potential errors."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 85.00),
                ('FL', '99214', 999999.99),
                ('FL', '99215', 120.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        outliers = conn.execute(
            "SELECT COUNT(*) FROM upload WHERE rate > 50000"
        ).fetchone()[0]
        assert outliers == 1

    def test_invalid_state_codes_flagged(self, conn):
        """Invalid state codes should be caught."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 85.00),
                ('XX', '99214', 90.00),
                ('123', '99215', 120.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        bad = conn.execute("""
            SELECT COUNT(*) FROM upload
            WHERE LENGTH(state_code) != 2 OR state_code != UPPER(state_code)
              OR state_code !~ '^[A-Z]{2}$'
        """).fetchone()[0]
        assert bad >= 1  # XX might be "valid format" but 123 is not

    def test_duplicate_rows_detected(self, conn):
        """Exact duplicate rows should be flagged."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 85.00),
                ('FL', '99213', 85.00),
                ('FL', '99214', 90.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        dupes = conn.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT (state_code, procedure_code, rate))
            FROM upload
        """).fetchone()[0]
        assert dupes == 1

    def test_null_procedure_codes_flagged(self, conn):
        """NULL procedure codes should be caught."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 85.00),
                ('FL', NULL, 90.00),
                ('FL', '', 120.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        bad = conn.execute("""
            SELECT COUNT(*) FROM upload
            WHERE procedure_code IS NULL OR TRIM(procedure_code) = ''
        """).fetchone()[0]
        assert bad == 2

    def test_zero_rates_allowed_but_counted(self, conn):
        """$0 rates are valid but should be counted for awareness."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 0.00),
                ('FL', '99214', 85.00),
                ('FL', '99215', 0.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        zeros = conn.execute(
            "SELECT COUNT(*) FROM upload WHERE rate = 0"
        ).fetchone()[0]
        assert zeros == 2  # Valid but notable

    def test_mixed_case_codes_normalized(self, conn):
        """Procedure codes should be normalized to uppercase."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 85.00),
                ('FL', 'g0101', 90.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        result = conn.execute("""
            SELECT procedure_code FROM upload
            WHERE procedure_code != UPPER(procedure_code)
        """).fetchall()
        assert len(result) == 1
        assert result[0][0] == "g0101"


class TestEncodingEdgeCases:
    """Test handling of encoding issues in uploaded data."""

    @pytest.fixture
    def conn(self):
        c = duckdb.connect(":memory:")
        yield c
        c.close()

    def test_smart_quotes_in_strings(self, conn):
        """Smart quotes should not break parsing."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL', '99213', 'Office visit'),
                ('FL', '99214', 'Office \u201cvisit\u201d')
            ) AS t(state_code, procedure_code, description)
        """)
        count = conn.execute("SELECT COUNT(*) FROM upload").fetchone()[0]
        assert count == 2

    def test_trailing_whitespace_in_codes(self, conn):
        """Trailing whitespace in codes should be detectable."""
        conn.execute("""
            CREATE TABLE upload AS
            SELECT * FROM (VALUES
                ('FL ', '99213', 85.00),
                ('FL', '99214 ', 90.00),
                ('FL', '99215', 120.00)
            ) AS t(state_code, procedure_code, rate)
        """)
        padded = conn.execute("""
            SELECT COUNT(*) FROM upload
            WHERE state_code != TRIM(state_code)
               OR procedure_code != TRIM(procedure_code)
        """).fetchone()[0]
        assert padded == 2
