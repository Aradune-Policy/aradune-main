"""
CPRA Generator — User-Upload Comparative Payment Rate Analysis

Accepts a state code + two CSVs (fee schedule + utilization), computes the
full CPRA required by 42 CFR § 447.203, and outputs CSV + JSON results.

Architecture: DuckDB in-memory (matches Aradune server pattern).
Reference data: CMS CY 2025 E/M Code List (68 codes), PPRRVU25, GPCI2025.

Usage:
    from cpra_generator import CpraGenerator
    gen = CpraGenerator(state_code="FL")
    gen.load_fee_schedule("path/to/fee_schedule.csv")
    gen.load_utilization("path/to/utilization.csv")
    result = gen.generate()
    result.to_csv("output/")
    result.to_json("output/cpra.json")
"""

import csv
import io
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import duckdb

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONVERSION_FACTOR = 32.3465  # CY 2025 Medicare PFS (non-QPP for CPRA)
RATE_YEAR = 2025
UTIL_YEAR = 2024
SMALL_CELL_THRESHOLD = 10

CATEGORIES = ["Primary Care", "OB-GYN", "Outpatient MH/SUD"]

# Paths — reference data shipped with the tool
REF_DIR = Path(__file__).parent.parent.parent / "data" / "reference" / "cpra"

# ---------------------------------------------------------------------------
# Validation schemas
# ---------------------------------------------------------------------------

FEE_SCHEDULE_REQUIRED_COLS = {"hcpcs_code", "medicaid_rate"}
FEE_SCHEDULE_OPTIONAL_COLS = {"description", "medicaid_rate_facility", "modifier"}

UTILIZATION_REQUIRED_COLS = {"hcpcs_code", "category", "total_claims", "unique_beneficiaries"}
UTILIZATION_OPTIONAL_COLS = {"total_units", "total_paid", "data_source", "util_year"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationError:
    field: str
    message: str
    severity: str = "error"  # "error" or "warning"


@dataclass
class CpraResult:
    """Container for all CPRA outputs."""
    state_code: str
    state_name: str
    rate_year: int
    util_year: int
    conversion_factor: float

    # Core data (list of dicts)
    merged: list = field(default_factory=list)           # Per code × category × locality
    statewide: list = field(default_factory=list)        # Per code × category (avg across localities)
    category_summary: list = field(default_factory=list)  # Per category
    category_locality: list = field(default_factory=list)  # Per category × locality
    codes_no_rate: list = field(default_factory=list)     # Codes without Medicaid rate

    # Metadata
    n_codes: int = 0
    n_with_rate: int = 0
    n_without_rate: int = 0
    n_categories: int = 3
    n_code_category_pairs: int = 0
    n_localities: int = 0
    utilization_source: str = ""
    warnings: list = field(default_factory=list)

    def to_csv(self, output_dir: str | Path) -> list[Path]:
        """Write all outputs as CSV files. Returns list of paths written."""
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        written = []

        for name, data in [
            ("statewide_comparison", self.statewide),
            ("merged_analysis", self.merged),
            ("category_summary", self.category_summary),
            ("category_locality_summary", self.category_locality),
            ("codes_no_rate", self.codes_no_rate),
        ]:
            if data:
                path = out / f"{name}.csv"
                _write_csv(data, path)
                written.append(path)

        return written

    def to_json(self, output_path: str | Path) -> Path:
        """Write complete CPRA result as JSON (Aradune-compatible shape)."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "meta": {
                "state_code": self.state_code,
                "state_name": self.state_name,
                "rate_year": self.rate_year,
                "util_year": self.util_year,
                "conversion_factor": self.conversion_factor,
                "n_codes": self.n_codes,
                "n_with_rate": self.n_with_rate,
                "n_without_rate": self.n_without_rate,
                "n_code_category_pairs": self.n_code_category_pairs,
                "n_localities": self.n_localities,
                "utilization_source": self.utilization_source,
                "warnings": self.warnings,
            },
            "statewide": self.statewide,
            "category_summary": self.category_summary,
            "category_locality_summary": self.category_locality,
            "codes_no_rate": self.codes_no_rate,
        }

        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

        return path


def _write_csv(rows: list[dict], path: Path):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# State lookup
# ---------------------------------------------------------------------------

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "AS": "American Samoa", "GU": "Guam", "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico", "VI": "Virgin Islands",
}


# ---------------------------------------------------------------------------
# Main generator class
# ---------------------------------------------------------------------------

class CpraGenerator:
    """
    Generates a CPRA for any US state.

    1. Load reference data (68 E/M codes, RVUs, GPCIs) — automatic
    2. User provides fee_schedule CSV and utilization CSV
    3. Compute Medicare rates for the state's localities
    4. Merge, calculate ratios, apply small cell suppression
    5. Output CpraResult with CSVs and JSON
    """

    def __init__(self, state_code: str, ref_dir: Optional[Path] = None):
        self.state_code = state_code.upper()
        if self.state_code not in STATE_NAMES:
            raise ValueError(f"Unknown state code: {self.state_code}")
        self.state_name = STATE_NAMES[self.state_code]
        self.ref_dir = Path(ref_dir) if ref_dir else REF_DIR

        self.db = duckdb.connect()
        self._fee_schedule_loaded = False
        self._utilization_loaded = False
        self._validation_warnings: list[str] = []

        self._load_reference_data()

    def close(self):
        if self.db:
            self.db.close()

    # --- Public API ---

    def load_fee_schedule(self, path: str | Path) -> list[ValidationError]:
        """Load and validate a state Medicaid fee schedule CSV."""
        path = Path(path)
        errors = self._validate_csv(path, FEE_SCHEDULE_REQUIRED_COLS, "fee_schedule")
        if any(e.severity == "error" for e in errors):
            return errors

        self.db.execute("DROP TABLE IF EXISTS user_fee_schedule")

        # Detect available columns (medicaid_rate_facility is optional)
        header_cols = self.db.execute(
            f"SELECT * FROM read_csv_auto('{path}', header=true, all_varchar=true) LIMIT 0"
        ).description
        col_names = {desc[0].lower() for desc in header_cols}
        has_fac_col = "medicaid_rate_facility" in col_names

        fac_expr = (
            "CAST(COALESCE(TRY_CAST(medicaid_rate_facility AS DOUBLE), NULL) AS DOUBLE)"
            if has_fac_col else "NULL::DOUBLE"
        )

        self.db.execute(f"""
            CREATE TABLE user_fee_schedule AS
            SELECT
                CAST(hcpcs_code AS VARCHAR) AS hcpcs_code,
                CAST(medicaid_rate AS DOUBLE) AS medicaid_rate,
                {fac_expr} AS medicaid_rate_facility
            FROM read_csv_auto('{path}', header=true, all_varchar=true)
            WHERE TRY_CAST(medicaid_rate AS DOUBLE) IS NOT NULL
        """)

        # Validate against E/M code list
        matched = self.db.execute("""
            SELECT COUNT(DISTINCT f.hcpcs_code)
            FROM user_fee_schedule f
            JOIN em_codes e ON f.hcpcs_code = e.hcpcs_code
        """).fetchone()[0]

        total_uploaded = self.db.execute(
            "SELECT COUNT(DISTINCT hcpcs_code) FROM user_fee_schedule"
        ).fetchone()[0]

        if matched == 0:
            errors.append(ValidationError(
                "hcpcs_code", "No uploaded codes match the 68 CMS E/M codes", "error"
            ))
            return errors

        if matched < 30:
            self._validation_warnings.append(
                f"Only {matched} of 68 E/M codes found in fee schedule "
                f"({total_uploaded} total codes uploaded, {total_uploaded - matched} non-E/M)"
            )

        # Create the fee schedule view with has_fl_rate flag
        self.db.execute("DROP VIEW IF EXISTS fee_schedule")
        self.db.execute("""
            CREATE VIEW fee_schedule AS
            SELECT
                e.hcpcs_code,
                COALESCE(f.medicaid_rate, 0) AS medicaid_rate,
                f.medicaid_rate_facility,
                CASE WHEN f.hcpcs_code IS NOT NULL THEN TRUE ELSE FALSE END AS has_rate
            FROM em_codes e
            LEFT JOIN user_fee_schedule f ON e.hcpcs_code = f.hcpcs_code
        """)

        n_with = self.db.execute(
            "SELECT COUNT(*) FROM fee_schedule WHERE has_rate"
        ).fetchone()[0]
        n_without = self.db.execute(
            "SELECT COUNT(*) FROM fee_schedule WHERE NOT has_rate"
        ).fetchone()[0]

        self._fee_schedule_loaded = True
        self._validation_warnings.append(
            f"Fee schedule: {n_with} of 68 E/M codes have rates, {n_without} do not"
        )
        return errors

    def load_utilization(self, path: str | Path) -> list[ValidationError]:
        """Load and validate a utilization CSV (one row per code × category)."""
        path = Path(path)
        errors = self._validate_csv(path, UTILIZATION_REQUIRED_COLS, "utilization")
        if any(e.severity == "error" for e in errors):
            return errors

        self.db.execute("DROP TABLE IF EXISTS user_utilization")
        self.db.execute(f"""
            CREATE TABLE user_utilization AS
            SELECT
                CAST(hcpcs_code AS VARCHAR) AS hcpcs_code,
                CAST(category AS VARCHAR) AS category,
                COALESCE(TRY_CAST(total_claims AS INTEGER), 0) AS total_claims,
                COALESCE(TRY_CAST(unique_beneficiaries AS INTEGER), 0) AS unique_beneficiaries,
                COALESCE(TRY_CAST(total_units AS INTEGER), 0) AS total_units,
                COALESCE(TRY_CAST(total_paid AS DOUBLE), 0) AS total_paid
            FROM read_csv_auto('{path}', header=true, all_varchar=true)
        """)

        # Validate categories
        bad_cats = self.db.execute("""
            SELECT DISTINCT category FROM user_utilization
            WHERE category NOT IN ('Primary Care', 'OB-GYN', 'Outpatient MH/SUD')
        """).fetchall()
        if bad_cats:
            bad_names = [r[0] for r in bad_cats]
            errors.append(ValidationError(
                "category",
                f"Unknown categories: {bad_names}. Expected: {CATEGORIES}",
                "error"
            ))
            return errors

        # Check coverage
        matched = self.db.execute("""
            SELECT COUNT(DISTINCT u.hcpcs_code)
            FROM user_utilization u
            JOIN em_codes e ON u.hcpcs_code = e.hcpcs_code
        """).fetchone()[0]

        if matched == 0:
            errors.append(ValidationError(
                "hcpcs_code", "No utilization codes match the 68 CMS E/M codes", "error"
            ))
            return errors

        self._utilization_loaded = True
        n_rows = self.db.execute("SELECT COUNT(*) FROM user_utilization").fetchone()[0]
        self._validation_warnings.append(
            f"Utilization: {n_rows} rows, {matched} of 68 E/M codes matched"
        )
        return errors

    def generate(self) -> CpraResult:
        """Run the full CPRA computation. Returns CpraResult."""
        if not self._fee_schedule_loaded:
            raise RuntimeError("Fee schedule not loaded. Call load_fee_schedule() first.")
        if not self._utilization_loaded:
            raise RuntimeError("Utilization not loaded. Call load_utilization() first.")

        # Step 1: Compute Medicare rates for this state's localities
        self._compute_medicare_rates()

        # Step 2: Merge fee schedule + utilization + Medicare rates
        self._merge_and_compare()

        # Step 3: Apply small cell suppression
        self._apply_suppression()

        # Step 4: Compute statewide averages (across localities)
        self._compute_statewide()

        # Step 5: Compute category and category × locality summaries
        self._compute_summaries()

        # Step 6: Collect results
        return self._collect_results()

    # --- Private: Reference data loading ---

    def _load_reference_data(self):
        """Load the 68 E/M codes, code-category map, RVUs, and GPCIs."""
        em_path = self.ref_dir / "em_codes.csv"
        cat_path = self.ref_dir / "code_categories.csv"
        gpci_path = self.ref_dir / "GPCI2025.csv"

        for p in [em_path, cat_path, gpci_path]:
            if not p.exists():
                raise FileNotFoundError(f"Reference file not found: {p}")

        # E/M codes (68 rows)
        self.db.execute(f"""
            CREATE TABLE em_codes AS
            SELECT
                CAST(hcpcs_code AS VARCHAR) AS hcpcs_code,
                description,
                CAST(is_primary_care AS BOOLEAN) AS is_primary_care,
                CAST(is_obgyn AS BOOLEAN) AS is_obgyn,
                CAST(is_mhsud AS BOOLEAN) AS is_mhsud,
                CAST(work_rvu AS DOUBLE) AS work_rvu,
                CAST(pe_rvu_nf AS DOUBLE) AS pe_rvu_nf,
                CAST(mp_rvu AS DOUBLE) AS mp_rvu
            FROM read_csv_auto('{em_path}', header=true)
        """)

        # Code-category mapping (171 rows)
        self.db.execute(f"""
            CREATE TABLE code_categories AS
            SELECT
                CAST(hcpcs_code AS VARCHAR) AS hcpcs_code,
                description,
                category
            FROM read_csv_auto('{cat_path}', header=true)
        """)

        # GPCIs — skip the 2-row header, find this state's localities
        self.db.execute(f"""
            CREATE TABLE all_gpcis AS
            SELECT
                TRIM(column1) AS state_code,
                CAST(TRIM(column2) AS VARCHAR) AS locality_number,
                TRIM(column3) AS locality_name,
                CAST(column4 AS DOUBLE) AS work_gpci,
                CAST(column5 AS DOUBLE) AS pe_gpci,
                CAST(column6 AS DOUBLE) AS mp_gpci
            FROM read_csv(
                '{gpci_path}',
                header=false,
                skip=3,
                columns={{
                    'column0': 'VARCHAR',
                    'column1': 'VARCHAR',
                    'column2': 'VARCHAR',
                    'column3': 'VARCHAR',
                    'column4': 'VARCHAR',
                    'column5': 'VARCHAR',
                    'column6': 'VARCHAR'
                }}
            )
            WHERE TRIM(column1) IS NOT NULL
              AND LENGTH(TRIM(column1)) = 2
        """)

        # Filter to this state
        self.db.execute("""
            CREATE TABLE state_gpcis AS
            SELECT * FROM all_gpcis
            WHERE state_code = $1
        """, [self.state_code])

        n_localities = self.db.execute(
            "SELECT COUNT(*) FROM state_gpcis"
        ).fetchone()[0]

        if n_localities == 0:
            raise ValueError(
                f"No Medicare localities found for {self.state_code}. "
                "Check that GPCI2025.csv is present and correctly formatted."
            )

        self._n_localities = n_localities

    # --- Private: Medicare rate computation ---

    def _compute_medicare_rates(self):
        """Compute Medicare non-facility rates for each E/M code × locality."""
        self.db.execute(f"""
            CREATE TABLE medicare_rates AS
            SELECT
                e.hcpcs_code,
                e.description,
                g.locality_number,
                g.locality_name,
                g.work_gpci,
                g.pe_gpci,
                g.mp_gpci,
                e.work_rvu,
                e.pe_rvu_nf,
                e.mp_rvu,
                ROUND(
                    (e.work_rvu * g.work_gpci
                     + e.pe_rvu_nf * g.pe_gpci
                     + e.mp_rvu * g.mp_gpci)
                    * {CONVERSION_FACTOR},
                    2
                ) AS medicare_nf_rate
            FROM em_codes e
            CROSS JOIN state_gpcis g
        """)

    # --- Private: Merge and compare ---

    def _merge_and_compare(self):
        """Cross-join code-categories with Medicare rates, attach fee schedule + utilization."""
        self.db.execute("""
            CREATE TABLE merged AS
            SELECT
                cc.hcpcs_code,
                cc.description,
                cc.category,
                mr.locality_number,
                mr.locality_name,
                mr.medicare_nf_rate,
                fs.medicaid_rate,
                fs.has_rate AS has_medicaid_rate,
                COALESCE(u.total_claims, 0) AS total_claims,
                COALESCE(u.unique_beneficiaries, 0) AS unique_beneficiaries,
                COALESCE(u.total_units, 0) AS total_units,
                COALESCE(u.total_paid, 0.0) AS total_paid,
                CASE
                    WHEN fs.has_rate AND mr.medicare_nf_rate > 0
                    THEN ROUND(fs.medicaid_rate / mr.medicare_nf_rate * 100, 1)
                    ELSE NULL
                END AS pct_of_medicare,
                CASE
                    WHEN fs.has_rate
                    THEN ROUND(fs.medicaid_rate - mr.medicare_nf_rate, 2)
                    ELSE NULL
                END AS rate_difference,
                FALSE AS is_suppressed
            FROM code_categories cc
            JOIN medicare_rates mr ON cc.hcpcs_code = mr.hcpcs_code
            LEFT JOIN fee_schedule fs ON cc.hcpcs_code = fs.hcpcs_code
            LEFT JOIN user_utilization u
                ON cc.hcpcs_code = u.hcpcs_code
                AND cc.category = u.category
        """)

    # --- Private: Small cell suppression ---

    def _apply_suppression(self):
        """Suppress rows where beneficiary count is 1-SMALL_CELL_THRESHOLD."""
        self.db.execute(f"""
            UPDATE merged
            SET is_suppressed = TRUE
            WHERE unique_beneficiaries BETWEEN 1 AND {SMALL_CELL_THRESHOLD}
        """)

    # --- Private: Statewide averages ---

    def _compute_statewide(self):
        """Average across localities to get one row per code × category."""
        self.db.execute("""
            CREATE TABLE statewide AS
            SELECT
                hcpcs_code,
                description,
                category,
                medicaid_rate,
                has_medicaid_rate,
                total_claims,
                unique_beneficiaries,
                total_units,
                total_paid,
                is_suppressed,
                ROUND(AVG(medicare_nf_rate), 2) AS medicare_nf_rate_avg,
                ROUND(MIN(medicare_nf_rate), 2) AS medicare_nf_rate_min,
                ROUND(MAX(medicare_nf_rate), 2) AS medicare_nf_rate_max,
                CASE
                    WHEN has_medicaid_rate AND AVG(medicare_nf_rate) > 0
                    THEN ROUND(medicaid_rate / AVG(medicare_nf_rate) * 100, 1)
                    ELSE NULL
                END AS pct_of_medicare_avg,
                CASE
                    WHEN has_medicaid_rate
                    THEN ROUND(medicaid_rate - AVG(medicare_nf_rate), 2)
                    ELSE NULL
                END AS rate_difference
            FROM merged
            GROUP BY
                hcpcs_code, description, category, medicaid_rate,
                has_medicaid_rate, total_claims, unique_beneficiaries,
                total_units, total_paid, is_suppressed
        """)

    # --- Private: Summaries ---

    def _compute_summaries(self):
        """Compute category-level and category × locality summaries."""

        # Category summary (weighted by claims)
        self.db.execute("""
            CREATE TABLE category_summary AS
            SELECT
                category,
                COUNT(*) AS n_codes,
                ROUND(
                    SUM(CASE WHEN NOT is_suppressed AND has_medicaid_rate AND pct_of_medicare_avg IS NOT NULL
                        THEN pct_of_medicare_avg * total_claims ELSE 0 END)
                    /
                    NULLIF(SUM(CASE WHEN NOT is_suppressed AND has_medicaid_rate AND pct_of_medicare_avg IS NOT NULL
                        THEN total_claims ELSE 0 END), 0),
                    1
                ) AS weighted_pct_medicare,
                ROUND(MEDIAN(pct_of_medicare_avg) FILTER (
                    WHERE has_medicaid_rate AND NOT is_suppressed
                ), 1) AS median_pct_medicare,
                MIN(pct_of_medicare_avg) FILTER (
                    WHERE has_medicaid_rate AND NOT is_suppressed
                ) AS min_pct_medicare,
                MAX(pct_of_medicare_avg) FILTER (
                    WHERE has_medicaid_rate AND NOT is_suppressed
                ) AS max_pct_medicare,
                SUM(CASE WHEN NOT is_suppressed THEN total_claims ELSE 0 END) AS total_claims,
                SUM(CASE WHEN NOT is_suppressed THEN unique_beneficiaries ELSE 0 END) AS total_beneficiaries
            FROM statewide
            GROUP BY category
            ORDER BY category
        """)

        # Category × locality summary
        self.db.execute("""
            CREATE TABLE category_locality AS
            SELECT
                m.category,
                m.locality_number,
                m.locality_name,
                COUNT(*) AS n_codes,
                ROUND(
                    SUM(CASE WHEN NOT m.is_suppressed AND m.has_medicaid_rate AND m.pct_of_medicare IS NOT NULL
                        THEN m.pct_of_medicare * m.total_claims ELSE 0 END)
                    /
                    NULLIF(SUM(CASE WHEN NOT m.is_suppressed AND m.has_medicaid_rate AND m.pct_of_medicare IS NOT NULL
                        THEN m.total_claims ELSE 0 END), 0),
                    1
                ) AS weighted_pct_medicare,
                ROUND(MEDIAN(m.pct_of_medicare) FILTER (
                    WHERE m.has_medicaid_rate AND NOT m.is_suppressed
                ), 1) AS median_pct_medicare,
                MIN(m.pct_of_medicare) FILTER (
                    WHERE m.has_medicaid_rate AND NOT m.is_suppressed
                ) AS min_pct_medicare,
                MAX(m.pct_of_medicare) FILTER (
                    WHERE m.has_medicaid_rate AND NOT m.is_suppressed
                ) AS max_pct_medicare,
                SUM(CASE WHEN NOT m.is_suppressed THEN m.total_claims ELSE 0 END) AS total_claims
            FROM merged m
            GROUP BY m.category, m.locality_number, m.locality_name
            ORDER BY m.category, m.locality_number
        """)

    # --- Private: Collect results ---

    def _collect_results(self) -> CpraResult:
        """Fetch all computed tables into a CpraResult."""

        merged = self._fetch_all("SELECT * FROM merged ORDER BY category, hcpcs_code, locality_number")
        statewide = self._fetch_all("SELECT * FROM statewide ORDER BY category, hcpcs_code")
        cat_summary = self._fetch_all("SELECT * FROM category_summary ORDER BY category")
        cat_locality = self._fetch_all("SELECT * FROM category_locality ORDER BY category, locality_number")

        codes_no_rate = self._fetch_all("""
            SELECT DISTINCT hcpcs_code, description, medicare_nf_rate_avg
            FROM statewide
            WHERE NOT has_medicaid_rate
            ORDER BY hcpcs_code
        """)

        n_codes = self.db.execute("SELECT COUNT(*) FROM em_codes").fetchone()[0]
        n_with = self.db.execute(
            "SELECT COUNT(DISTINCT hcpcs_code) FROM statewide WHERE has_medicaid_rate"
        ).fetchone()[0]

        # Format display columns for statewide
        for row in statewide:
            if row["is_suppressed"]:
                row["bene_display"] = "*"
                row["claims_display"] = "*"
                row["paid_display"] = "*"
            else:
                row["bene_display"] = f"{row['unique_beneficiaries']:,}"
                row["claims_display"] = f"{row['total_claims']:,}"
                row["paid_display"] = f"${row['total_paid']:,.2f}"

        # Check utilization source
        try:
            src = self.db.execute("""
                SELECT DISTINCT data_source FROM user_utilization
                WHERE data_source IS NOT NULL LIMIT 1
            """).fetchone()
            util_source = src[0] if src else "User Upload"
        except Exception:
            util_source = "User Upload"

        return CpraResult(
            state_code=self.state_code,
            state_name=self.state_name,
            rate_year=RATE_YEAR,
            util_year=UTIL_YEAR,
            conversion_factor=CONVERSION_FACTOR,
            merged=merged,
            statewide=statewide,
            category_summary=cat_summary,
            category_locality=cat_locality,
            codes_no_rate=codes_no_rate,
            n_codes=n_codes,
            n_with_rate=n_with,
            n_without_rate=n_codes - n_with,
            n_code_category_pairs=len(statewide),
            n_localities=self._n_localities,
            utilization_source=util_source,
            warnings=self._validation_warnings,
        )

    # --- Private: Helpers ---

    def _fetch_all(self, sql: str) -> list[dict]:
        """Execute SQL and return list of dicts."""
        result = self.db.execute(sql)
        cols = [desc[0] for desc in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    def _validate_csv(self, path: Path, required: set, name: str) -> list[ValidationError]:
        """Validate a CSV file has required columns."""
        errors = []

        if not path.exists():
            errors.append(ValidationError("file", f"File not found: {path}", "error"))
            return errors

        if path.suffix.lower() != ".csv":
            errors.append(ValidationError("file", f"Expected CSV file, got: {path.suffix}", "error"))
            return errors

        with open(path, "r") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                errors.append(ValidationError("file", f"{name} CSV is empty", "error"))
                return errors

        header_lower = {col.strip().lower() for col in header}
        missing = required - header_lower
        if missing:
            errors.append(ValidationError(
                "columns",
                f"{name} missing required columns: {missing}. Found: {sorted(header_lower)}",
                "error"
            ))

        return errors


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate a CPRA report from uploaded fee schedule + utilization CSVs"
    )
    parser.add_argument("--state", required=True, help="Two-letter state code (e.g., FL)")
    parser.add_argument("--fee-schedule", required=True, help="Path to Medicaid fee schedule CSV")
    parser.add_argument("--utilization", required=True, help="Path to utilization CSV")
    parser.add_argument("--output-dir", default="output/cpra", help="Output directory")
    parser.add_argument("--ref-dir", default=None, help="Reference data directory (default: data/raw/)")

    args = parser.parse_args()

    print(f"\nCPRA Generator — {args.state}")
    print("=" * 50)

    gen = CpraGenerator(args.state, ref_dir=args.ref_dir)

    print(f"State: {gen.state_name} ({gen.state_code})")
    print(f"Medicare localities: {gen._n_localities}")

    # Load fee schedule
    print(f"\nLoading fee schedule: {args.fee_schedule}")
    errors = gen.load_fee_schedule(args.fee_schedule)
    for e in errors:
        print(f"  [{e.severity.upper()}] {e.field}: {e.message}")
    if any(e.severity == "error" for e in errors):
        print("\nFATAL: Cannot proceed with fee schedule errors.")
        return 1

    # Load utilization
    print(f"\nLoading utilization: {args.utilization}")
    errors = gen.load_utilization(args.utilization)
    for e in errors:
        print(f"  [{e.severity.upper()}] {e.field}: {e.message}")
    if any(e.severity == "error" for e in errors):
        print("\nFATAL: Cannot proceed with utilization errors.")
        return 1

    # Generate
    print("\nGenerating CPRA...")
    result = gen.generate()

    # Print summary
    print(f"\n{'=' * 50}")
    print(f"CPRA Results — {result.state_name}")
    print(f"{'=' * 50}")
    print(f"E/M codes:        {result.n_codes}")
    print(f"  With rate:      {result.n_with_rate}")
    print(f"  Without rate:   {result.n_without_rate}")
    print(f"Code × category:  {result.n_code_category_pairs}")
    print(f"Medicare localities: {result.n_localities}")

    print("\nCategory Summary:")
    for cat in result.category_summary:
        print(f"  {cat['category']:25s} "
              f"Wtd avg: {cat['weighted_pct_medicare']:5.1f}% MCR  "
              f"Range: {cat['min_pct_medicare']}-{cat['max_pct_medicare']}%  "
              f"Claims: {cat['total_claims']:,}")

    if result.warnings:
        print("\nWarnings:")
        for w in result.warnings:
            print(f"  - {w}")

    # Write outputs
    csv_paths = result.to_csv(args.output_dir)
    print(f"\nCSV outputs ({len(csv_paths)} files):")
    for p in csv_paths:
        print(f"  {p}")

    json_path = result.to_json(Path(args.output_dir) / "cpra_result.json")
    print(f"JSON output: {json_path}")

    gen.close()
    print("\nDone.")
    return 0


if __name__ == "__main__":
    exit(main())
