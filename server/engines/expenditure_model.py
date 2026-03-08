"""
Expenditure Modeling Engine — Build on Caseload Forecasts

Takes approved caseload forecasts (by category/month) and applies expenditure
parameters to produce expenditure projections:

  - Managed Care: blended cap rates × enrollment by MC subcategory
  - Fee for Service: cost per eligible × FFS category enrollment
  - Optional: trend factors, inflation adjustments, policy adjustments

Architecture: Same template-driven upload pattern as CPRA and Caseload Forecast.
User uploads:
  1. Caseload forecast CSV (or uses just-generated caseload forecast)
  2. Expenditure parameters CSV (cap rates, cost-per-eligible, trends)

Usage:
    from expenditure_model import ExpenditureModeler
    em = ExpenditureModeler(state_code="FL")
    em.load_caseload_forecast(caseload_json)    # from CaseloadForecaster output
    em.load_expenditure_params(params_bytes)     # CSV with rates/costs
    result = em.project()
    result.to_json()
"""

import csv
import io
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PARAMS_REQUIRED_COLS = {"category", "payment_type"}
# payment_type: "capitation" or "ffs"

PARAMS_CAPITATION_COLS = {"cap_rate_pmpm"}  # per-member-per-month
PARAMS_FFS_COLS = {"cost_per_eligible"}  # monthly cost per eligible

PARAMS_OPTIONAL_COLS = {
    "annual_trend_pct",      # annual rate of increase (e.g., 5.0 = 5%)
    "admin_load_pct",        # administrative load as % (e.g., 8.0 = 8%)
    "risk_margin_pct",       # risk/profit margin (e.g., 2.0 = 2%)
    "policy_adjustment_pct", # one-time policy adjustment (e.g., -3.0 for 3% cut)
    "policy_start_month",    # when policy adjustment takes effect (YYYY-MM)
    "notes",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationError:
    field: str
    message: str
    severity: str = "error"


@dataclass
class CategoryExpenditure:
    """Expenditure projection for a single category."""
    category: str
    payment_type: str  # "capitation" or "ffs"

    # Parameters used
    base_rate: float  # cap_rate_pmpm or cost_per_eligible
    annual_trend_pct: float
    admin_load_pct: float
    risk_margin_pct: float

    # Historical actuals (if caseload had actuals with known expenditure)
    actuals: list = field(default_factory=list)
    # Projections: {month, enrollment, rate, expenditure, lower_80, upper_80, lower_95, upper_95}
    projections: list = field(default_factory=list)

    # Summary
    total_projected: float = 0.0
    avg_monthly: float = 0.0
    peak_month: str = ""
    peak_expenditure: float = 0.0


@dataclass
class ExpenditureResult:
    """Container for expenditure projection outputs."""
    state_code: str
    state_name: str
    projection_date: str
    horizon_months: int

    categories: list[CategoryExpenditure] = field(default_factory=list)

    # Aggregate
    aggregate_projections: list = field(default_factory=list)

    # Summary
    total_projected: float = 0.0
    total_mc_projected: float = 0.0
    total_ffs_projected: float = 0.0
    n_categories: int = 0
    warnings: list = field(default_factory=list)

    def to_json(self, output_path: str | Path = None) -> dict:
        payload = {
            "meta": {
                "state_code": self.state_code,
                "state_name": self.state_name,
                "projection_date": self.projection_date,
                "horizon_months": self.horizon_months,
                "n_categories": self.n_categories,
                "total_projected": round(self.total_projected, 2),
                "total_mc_projected": round(self.total_mc_projected, 2),
                "total_ffs_projected": round(self.total_ffs_projected, 2),
                "warnings": self.warnings,
            },
            "categories": [],
            "aggregate": {
                "projections": self.aggregate_projections,
            },
        }

        for ce in self.categories:
            payload["categories"].append({
                "category": ce.category,
                "payment_type": ce.payment_type,
                "base_rate": round(ce.base_rate, 2),
                "annual_trend_pct": ce.annual_trend_pct,
                "admin_load_pct": ce.admin_load_pct,
                "risk_margin_pct": ce.risk_margin_pct,
                "total_projected": round(ce.total_projected, 2),
                "avg_monthly": round(ce.avg_monthly, 2),
                "peak_month": ce.peak_month,
                "peak_expenditure": round(ce.peak_expenditure, 2),
                "projections": ce.projections,
            })

        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(payload, f, indent=2, default=str)

        return payload

    def to_csv_bytes(self) -> bytes:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "category", "payment_type", "month", "enrollment",
            "rate_pmpm", "expenditure", "lower_80", "upper_80",
            "lower_95", "upper_95",
        ])

        for ce in self.categories:
            for p in ce.projections:
                writer.writerow([
                    ce.category, ce.payment_type, p["month"], p["enrollment"],
                    p.get("rate", ""), p["expenditure"],
                    p.get("lower_80", ""), p.get("upper_80", ""),
                    p.get("lower_95", ""), p.get("upper_95", ""),
                ])

        # Aggregate
        for p in self.aggregate_projections:
            writer.writerow([
                "TOTAL", "all", p["month"], p.get("enrollment", ""),
                "", p["expenditure"],
                p.get("lower_80", ""), p.get("upper_80", ""),
                p.get("lower_95", ""), p.get("upper_95", ""),
            ])

        buf.seek(0)
        return buf.getvalue().encode()


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
}


# ---------------------------------------------------------------------------
# Template generators
# ---------------------------------------------------------------------------

def generate_params_template(categories: list[str] = None) -> str:
    """Generate a blank expenditure parameters CSV template."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "category", "payment_type", "cap_rate_pmpm", "cost_per_eligible",
        "annual_trend_pct", "admin_load_pct", "risk_margin_pct",
        "policy_adjustment_pct", "policy_start_month", "notes",
    ])

    if categories is None:
        categories = [
            "MMA Managed Care", "LTC Managed Care",
            "SSI Aged", "SSI Disabled",
            "TANF Children", "TANF Adults",
            "Medically Needy", "SOBRA Pregnant Women",
        ]

    for cat in categories:
        # Default: MC categories get capitation, FFS categories get ffs
        is_mc = "managed care" in cat.lower() or "mma" in cat.lower() or "mc" in cat.lower()
        writer.writerow([
            cat,
            "capitation" if is_mc else "ffs",
            "" if not is_mc else "",  # cap_rate_pmpm
            "" if is_mc else "",      # cost_per_eligible
            "5.0",    # annual_trend_pct
            "8.0" if is_mc else "",   # admin_load_pct
            "2.0" if is_mc else "",   # risk_margin_pct
            "",       # policy_adjustment_pct
            "",       # policy_start_month
            "",       # notes
        ])

    buf.seek(0)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class ExpenditureModeler:
    """Expenditure projection engine using caseload forecasts + parameters."""

    def __init__(self, state_code: str):
        state_code = state_code.upper().strip()
        if state_code not in STATE_NAMES:
            raise ValueError(f"Unknown state code: {state_code}")
        self.state_code = state_code
        self.state_name = STATE_NAMES[state_code]

        # Caseload data (from forecast engine output)
        self.caseload_categories: list[dict] = []  # per-category forecast data
        self.caseload_aggregate: dict = {}
        self.horizon_months: int = 36

        # Expenditure parameters
        self.params: dict[str, dict] = {}  # category -> params dict

        self._warnings: list[str] = []

    def load_caseload_from_forecast(self, forecast_json: dict) -> list[ValidationError]:
        """Load caseload data from CaseloadForecaster output JSON."""
        errors: list[ValidationError] = []

        if "categories" not in forecast_json:
            errors.append(ValidationError("categories", "Missing 'categories' in forecast data"))
            return errors

        self.caseload_categories = forecast_json["categories"]
        self.caseload_aggregate = forecast_json.get("aggregate", {})

        meta = forecast_json.get("meta", {})
        self.horizon_months = meta.get("horizon_months", 36)

        if not self.caseload_categories:
            errors.append(ValidationError("categories", "No category data found"))

        # Check that each category has forecasts
        for cat in self.caseload_categories:
            if not cat.get("forecasts"):
                errors.append(ValidationError(
                    cat.get("category", "unknown"),
                    f"No forecast data for category '{cat.get('category', 'unknown')}'",
                    severity="warning",
                ))

        self._warnings.append(
            f"Loaded {len(self.caseload_categories)} categories from forecast"
        )

        return errors

    def load_caseload_from_csv(self, content: bytes) -> list[ValidationError]:
        """Load caseload forecast from CSV (alternative to JSON)."""
        errors: list[ValidationError] = []

        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        if not rows:
            errors.append(ValidationError("csv", "Empty CSV file"))
            return errors

        # Expected columns: category, month, type, enrollment
        required = {"category", "month", "enrollment"}
        headers = set(r.strip().lower() for r in (rows[0].keys() if rows else []))
        missing = required - headers
        if missing:
            errors.append(ValidationError("columns", f"Missing required columns: {missing}"))
            return errors

        # Build category data from CSV rows
        cat_data: dict[str, dict] = {}
        for row in rows:
            cat = row.get("category", "").strip()
            month = row.get("month", "").strip()
            row_type = row.get("type", "forecast").strip().lower()
            enrollment_str = row.get("enrollment", "").strip()

            if not cat or not month or not enrollment_str:
                continue

            try:
                enrollment = float(enrollment_str)
            except ValueError:
                continue

            if cat not in cat_data:
                cat_data[cat] = {"category": cat, "actuals": [], "forecasts": []}

            if row_type == "actual":
                cat_data[cat]["actuals"].append({"month": month, "enrollment": enrollment})
            else:
                point_data = {"month": month, "point": enrollment}
                # Try to get CI columns
                for col in ["lower_80", "upper_80", "lower_95", "upper_95"]:
                    val = row.get(col, "").strip()
                    if val:
                        try:
                            point_data[col] = float(val)
                        except ValueError:
                            pass
                # Default CIs if not provided
                if "lower_80" not in point_data:
                    point_data["lower_80"] = enrollment * 0.9
                    point_data["upper_80"] = enrollment * 1.1
                    point_data["lower_95"] = enrollment * 0.85
                    point_data["upper_95"] = enrollment * 1.15
                cat_data[cat]["forecasts"].append(point_data)

        self.caseload_categories = list(cat_data.values())

        if not self.caseload_categories:
            errors.append(ValidationError("csv", "No valid category data found in CSV"))

        # Determine horizon from first category's forecasts
        if self.caseload_categories:
            self.horizon_months = len(self.caseload_categories[0].get("forecasts", []))

        self._warnings.append(
            f"Loaded {len(self.caseload_categories)} categories from CSV"
        )

        return errors

    def load_params_bytes(self, content: bytes) -> list[ValidationError]:
        """Load expenditure parameters from CSV bytes."""
        errors: list[ValidationError] = []

        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = content.decode("latin-1")

        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)

        if not rows:
            errors.append(ValidationError("csv", "Empty parameters CSV"))
            return errors

        # Normalize headers
        headers = set()
        for row in rows:
            headers.update(k.strip().lower() for k in row.keys() if k is not None)

        missing = PARAMS_REQUIRED_COLS - headers
        if missing:
            errors.append(ValidationError("columns", f"Missing required columns: {missing}"))
            return errors

        for i, row in enumerate(rows):
            # Normalize keys
            norm = {k.strip().lower(): (v.strip() if v else "") for k, v in row.items() if k is not None}

            category = norm.get("category", "")
            payment_type = norm.get("payment_type", "").lower()

            if not category:
                continue
            if payment_type not in ("capitation", "ffs"):
                errors.append(ValidationError(
                    f"row {i+2}",
                    f"Invalid payment_type '{payment_type}' for '{category}'. Must be 'capitation' or 'ffs'.",
                ))
                continue

            params = {"payment_type": payment_type}

            # Parse numeric fields
            if payment_type == "capitation":
                cap_rate = norm.get("cap_rate_pmpm", "")
                if not cap_rate:
                    errors.append(ValidationError(
                        f"row {i+2}",
                        f"cap_rate_pmpm required for capitation category '{category}'",
                    ))
                    continue
                try:
                    params["cap_rate_pmpm"] = float(cap_rate.replace(",", "").replace("$", ""))
                except ValueError:
                    errors.append(ValidationError(f"row {i+2}", f"Invalid cap_rate_pmpm: '{cap_rate}'"))
                    continue
            else:
                cpe = norm.get("cost_per_eligible", "")
                if not cpe:
                    errors.append(ValidationError(
                        f"row {i+2}",
                        f"cost_per_eligible required for FFS category '{category}'",
                    ))
                    continue
                try:
                    params["cost_per_eligible"] = float(cpe.replace(",", "").replace("$", ""))
                except ValueError:
                    errors.append(ValidationError(f"row {i+2}", f"Invalid cost_per_eligible: '{cpe}'"))
                    continue

            # Optional params
            for key in ["annual_trend_pct", "admin_load_pct", "risk_margin_pct", "policy_adjustment_pct"]:
                val = norm.get(key, "")
                if val:
                    try:
                        params[key] = float(val.replace("%", ""))
                    except ValueError:
                        errors.append(ValidationError(f"row {i+2}", f"Invalid {key}: '{val}'", severity="warning"))

            params["policy_start_month"] = norm.get("policy_start_month", "")
            params["notes"] = norm.get("notes", "")

            self.params[category] = params

        if not self.params:
            errors.append(ValidationError("params", "No valid expenditure parameters found"))
        else:
            self._warnings.append(
                f"Loaded parameters for {len(self.params)} categories: "
                f"{sum(1 for p in self.params.values() if p['payment_type'] == 'capitation')} capitation, "
                f"{sum(1 for p in self.params.values() if p['payment_type'] == 'ffs')} FFS"
            )

        return errors

    def project(self) -> ExpenditureResult:
        """Run expenditure projection using caseload forecasts + parameters."""
        result = ExpenditureResult(
            state_code=self.state_code,
            state_name=self.state_name,
            projection_date=str(date.today()),
            horizon_months=self.horizon_months,
            warnings=list(self._warnings),
        )

        aggregate_by_month: dict[str, dict] = {}  # month -> totals

        for cat_data in self.caseload_categories:
            cat_name = cat_data.get("category", "")
            forecasts = cat_data.get("forecasts", [])

            if cat_name not in self.params:
                result.warnings.append(
                    f"No expenditure parameters for '{cat_name}' — skipping"
                )
                continue

            params = self.params[cat_name]
            payment_type = params["payment_type"]

            base_rate = (
                params.get("cap_rate_pmpm", 0)
                if payment_type == "capitation"
                else params.get("cost_per_eligible", 0)
            )

            annual_trend = params.get("annual_trend_pct", 0) / 100
            admin_load = params.get("admin_load_pct", 0) / 100
            risk_margin = params.get("risk_margin_pct", 0) / 100
            policy_adj = params.get("policy_adjustment_pct", 0) / 100
            policy_start = params.get("policy_start_month", "")

            ce = CategoryExpenditure(
                category=cat_name,
                payment_type=payment_type,
                base_rate=base_rate,
                annual_trend_pct=params.get("annual_trend_pct", 0),
                admin_load_pct=params.get("admin_load_pct", 0),
                risk_margin_pct=params.get("risk_margin_pct", 0),
            )

            # Determine base month for trending
            if forecasts:
                base_month = forecasts[0]["month"]
            else:
                continue

            total = 0.0
            peak_exp = 0.0
            peak_mo = ""

            for i, f in enumerate(forecasts):
                month = f["month"]
                enrollment = f.get("point", f.get("enrollment", 0))
                lower_80 = f.get("lower_80", enrollment * 0.9)
                upper_80 = f.get("upper_80", enrollment * 1.1)
                lower_95 = f.get("lower_95", enrollment * 0.85)
                upper_95 = f.get("upper_95", enrollment * 1.15)

                # Apply annual trend (compound monthly)
                months_from_base = i
                trend_factor = (1 + annual_trend / 12) ** months_from_base

                # Adjusted rate
                rate = base_rate * trend_factor

                # Apply admin load + risk margin for capitation
                if payment_type == "capitation":
                    rate *= (1 + admin_load) * (1 + risk_margin)

                # Apply policy adjustment if after start month
                if policy_adj and policy_start and month >= policy_start:
                    rate *= (1 + policy_adj)

                # Calculate expenditure
                expenditure = enrollment * rate
                exp_lower_80 = lower_80 * rate
                exp_upper_80 = upper_80 * rate
                exp_lower_95 = lower_95 * rate
                exp_upper_95 = upper_95 * rate

                ce.projections.append({
                    "month": month,
                    "enrollment": round(enrollment),
                    "rate": round(rate, 2),
                    "expenditure": round(expenditure, 2),
                    "lower_80": round(exp_lower_80, 2),
                    "upper_80": round(exp_upper_80, 2),
                    "lower_95": round(exp_lower_95, 2),
                    "upper_95": round(exp_upper_95, 2),
                })

                total += expenditure
                if expenditure > peak_exp:
                    peak_exp = expenditure
                    peak_mo = month

                # Aggregate
                if month not in aggregate_by_month:
                    aggregate_by_month[month] = {
                        "month": month,
                        "expenditure": 0,
                        "enrollment": 0,
                        "lower_80": 0, "upper_80": 0,
                        "lower_95": 0, "upper_95": 0,
                    }
                aggregate_by_month[month]["expenditure"] += expenditure
                aggregate_by_month[month]["enrollment"] += enrollment
                aggregate_by_month[month]["lower_80"] += exp_lower_80
                aggregate_by_month[month]["upper_80"] += exp_upper_80
                aggregate_by_month[month]["lower_95"] += exp_lower_95
                aggregate_by_month[month]["upper_95"] += exp_upper_95

            ce.total_projected = total
            ce.avg_monthly = total / max(len(forecasts), 1)
            ce.peak_month = peak_mo
            ce.peak_expenditure = peak_exp

            result.categories.append(ce)

            if payment_type == "capitation":
                result.total_mc_projected += total
            else:
                result.total_ffs_projected += total

        # Build aggregate
        result.aggregate_projections = [
            {
                "month": v["month"],
                "expenditure": round(v["expenditure"], 2),
                "enrollment": round(v["enrollment"]),
                "lower_80": round(v["lower_80"], 2),
                "upper_80": round(v["upper_80"], 2),
                "lower_95": round(v["lower_95"], 2),
                "upper_95": round(v["upper_95"], 2),
            }
            for v in sorted(aggregate_by_month.values(), key=lambda x: x["month"])
        ]

        result.total_projected = result.total_mc_projected + result.total_ffs_projected
        result.n_categories = len(result.categories)

        return result
