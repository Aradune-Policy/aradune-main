"""
Caseload Forecasting Engine — User-Upload Time Series Analysis

Accepts monthly caseload data (enrollment by category), optional structural
events, and produces forecasts with confidence intervals using SARIMAX + ETS
model competition. Enriches with Aradune's public economic/enrollment data.

Architecture: Follows the CPRA upload pattern — DuckDB in-memory, stateless,
template-driven CSV upload, immediate results.

Usage:
    from caseload_forecast import CaseloadForecaster
    fc = CaseloadForecaster(state_code="FL")
    fc.load_caseload("caseload.csv")
    fc.load_events("events.csv")  # optional
    result = fc.forecast(horizon_months=36)
    result.to_json("forecast.json")
    result.to_csv("forecast.csv")
"""

import csv
import io
import json
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Suppress convergence warnings during model fitting
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Known structural events that apply to all states
KNOWN_EVENTS = [
    {"date": "2020-03", "event_type": "phe_start",
     "description": "COVID-19 PHE declared — continuous coverage begins",
     "affected_categories": "ALL"},
    {"date": "2023-04", "event_type": "unwinding_start",
     "description": "PHE unwinding begins — redeterminations resume",
     "affected_categories": "ALL"},
    {"date": "2023-06", "event_type": "unwinding_peak",
     "description": "Unwinding disenrollments peak (national)",
     "affected_categories": "ALL"},
]

CASELOAD_REQUIRED_COLS = {"month", "category", "enrollment"}
CASELOAD_OPTIONAL_COLS = {"county", "region", "delivery_system", "subcategory"}

EVENTS_REQUIRED_COLS = {"date", "event_type", "description"}
EVENTS_OPTIONAL_COLS = {"affected_categories", "magnitude", "direction"}

# Minimum months of history to attempt forecasting
MIN_HISTORY_MONTHS = 24
# Maximum forecast horizon
MAX_HORIZON_MONTHS = 60


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationError:
    field: str
    message: str
    severity: str = "error"  # "error" or "warning"


@dataclass
class CategoryForecast:
    """Forecast result for a single category."""
    category: str
    model_used: str  # "sarimax", "ets", "naive"
    model_order: str  # e.g. "(1,1,1)(1,0,1,12)"
    fit_aic: float
    fit_mape: Optional[float]  # holdout MAPE if enough data
    history_months: int
    forecast_months: int

    # Actuals (list of {month, enrollment})
    actuals: list = field(default_factory=list)
    # Forecasts (list of {month, point, lower_80, upper_80, lower_95, upper_95})
    forecasts: list = field(default_factory=list)
    # Detected events/interventions
    events: list = field(default_factory=list)
    # Model coefficients for intervention variables
    intervention_effects: dict = field(default_factory=dict)

    warnings: list = field(default_factory=list)


@dataclass
class ForecastResult:
    """Container for all forecasting outputs."""
    state_code: str
    state_name: str
    forecast_date: str
    horizon_months: int

    # Per-category forecasts
    categories: list[CategoryForecast] = field(default_factory=list)

    # Aggregate forecast (sum of all categories)
    aggregate_actuals: list = field(default_factory=list)
    aggregate_forecasts: list = field(default_factory=list)

    # Metadata
    n_categories: int = 0
    total_history_months: int = 0
    has_regional_data: bool = False
    has_delivery_system: bool = False
    economic_covariates_used: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    def to_json(self, output_path: str | Path = None) -> dict:
        """Serialize to JSON-compatible dict (or write to file)."""
        payload = {
            "meta": {
                "state_code": self.state_code,
                "state_name": self.state_name,
                "forecast_date": self.forecast_date,
                "horizon_months": self.horizon_months,
                "n_categories": self.n_categories,
                "total_history_months": self.total_history_months,
                "has_regional_data": self.has_regional_data,
                "has_delivery_system": self.has_delivery_system,
                "economic_covariates_used": self.economic_covariates_used,
                "warnings": self.warnings,
            },
            "categories": [],
            "aggregate": {
                "actuals": self.aggregate_actuals,
                "forecasts": self.aggregate_forecasts,
            },
        }

        for cf in self.categories:
            payload["categories"].append({
                "category": cf.category,
                "model_used": cf.model_used,
                "model_order": cf.model_order,
                "fit_aic": cf.fit_aic,
                "fit_mape": cf.fit_mape,
                "history_months": cf.history_months,
                "forecast_months": cf.forecast_months,
                "actuals": cf.actuals,
                "forecasts": cf.forecasts,
                "events": cf.events,
                "intervention_effects": cf.intervention_effects,
                "warnings": cf.warnings,
            })

        if output_path:
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(payload, f, indent=2, default=str)

        return payload

    def to_csv_bytes(self) -> bytes:
        """Export forecast as CSV bytes for download."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "category", "month", "type", "enrollment",
            "lower_80", "upper_80", "lower_95", "upper_95",
            "model", "notes",
        ])

        for cf in self.categories:
            for a in cf.actuals:
                writer.writerow([
                    cf.category, a["month"], "actual", a["enrollment"],
                    "", "", "", "", "", "",
                ])
            for f in cf.forecasts:
                writer.writerow([
                    cf.category, f["month"], "forecast", f["point"],
                    f["lower_80"], f["upper_80"], f["lower_95"], f["upper_95"],
                    cf.model_used, "",
                ])

        # Aggregate
        for a in self.aggregate_actuals:
            writer.writerow([
                "TOTAL", a["month"], "actual", a["enrollment"],
                "", "", "", "", "", "",
            ])
        for f in self.aggregate_forecasts:
            writer.writerow([
                "TOTAL", f["month"], "forecast", f["point"],
                f["lower_80"], f["upper_80"], f["lower_95"], f["upper_95"],
                "aggregate", "",
            ])

        buf.seek(0)
        return buf.getvalue().encode()


# ---------------------------------------------------------------------------
# State lookup (shared with CPRA)
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
# Main forecasting class
# ---------------------------------------------------------------------------

class CaseloadForecaster:
    """
    Generates caseload forecasts from user-uploaded monthly enrollment data.

    1. User uploads caseload.csv (month, category, enrollment)
    2. Optionally uploads events.csv (structural breaks)
    3. Engine enriches with Aradune public data (unemployment, population)
    4. Runs SARIMAX + ETS per category, picks best model
    5. Returns ForecastResult with per-category and aggregate forecasts
    """

    def __init__(self, state_code: str):
        self.state_code = state_code.upper()
        if self.state_code not in STATE_NAMES:
            raise ValueError(f"Unknown state code: {self.state_code}")
        self.state_name = STATE_NAMES[self.state_code]

        self._caseload_df: Optional[pd.DataFrame] = None
        self._events_df: Optional[pd.DataFrame] = None
        self._economic_df: Optional[pd.DataFrame] = None
        self._validation_warnings: list[str] = []

    # --- Public API ---

    def load_caseload(self, path: str | Path) -> list[ValidationError]:
        """Load and validate caseload CSV."""
        path = Path(path)
        errors = self._validate_csv(path, CASELOAD_REQUIRED_COLS, "caseload")
        if any(e.severity == "error" for e in errors):
            return errors

        df = pd.read_csv(path, dtype=str)
        df.columns = df.columns.str.strip().str.lower()

        # Parse month column — accept YYYY-MM, YYYYMM, MM/YYYY, etc.
        df["month_parsed"] = pd.to_datetime(
            df["month"].str.strip(), format="mixed", dayfirst=False
        )
        bad_dates = df["month_parsed"].isna().sum()
        if bad_dates > 0:
            errors.append(ValidationError(
                "month", f"{bad_dates} rows have unparseable month values", "warning"
            ))
            df = df.dropna(subset=["month_parsed"])

        # Parse enrollment
        df["enrollment"] = (
            df["enrollment"].str.replace(",", "").str.strip()
        )
        df["enrollment_num"] = pd.to_numeric(df["enrollment"], errors="coerce")
        bad_enroll = df["enrollment_num"].isna().sum()
        if bad_enroll > 0:
            errors.append(ValidationError(
                "enrollment", f"{bad_enroll} rows have non-numeric enrollment", "warning"
            ))
            df = df.dropna(subset=["enrollment_num"])

        df["enrollment_num"] = df["enrollment_num"].astype(int)
        df["category"] = df["category"].str.strip()

        # Check minimum history
        n_months = df["month_parsed"].nunique()
        if n_months < MIN_HISTORY_MONTHS:
            errors.append(ValidationError(
                "month",
                f"Only {n_months} months of data. Need at least {MIN_HISTORY_MONTHS} "
                f"for reliable forecasting.",
                "error"
            ))
            return errors

        categories = df["category"].unique()
        self._validation_warnings.append(
            f"Loaded {len(df):,} rows: {len(categories)} categories, "
            f"{n_months} months ({df['month_parsed'].min():%Y-%m} to "
            f"{df['month_parsed'].max():%Y-%m})"
        )

        # Check for optional columns
        has_county = "county" in df.columns
        has_region = "region" in df.columns
        has_delivery = "delivery_system" in df.columns
        if has_county or has_region:
            self._validation_warnings.append(
                f"Geographic detail detected: "
                f"{'county' if has_county else ''}"
                f"{' + ' if has_county and has_region else ''}"
                f"{'region' if has_region else ''}"
            )
        if has_delivery:
            delivery_types = df["delivery_system"].dropna().unique()
            self._validation_warnings.append(
                f"Delivery system detail: {list(delivery_types)}"
            )

        self._caseload_df = df
        return errors

    def load_caseload_bytes(self, content: bytes) -> list[ValidationError]:
        """Load caseload from in-memory bytes (for API uploads)."""
        import tempfile
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            f.write(content)
            tmp_path = f.name
        try:
            return self.load_caseload(tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def load_events(self, path: str | Path) -> list[ValidationError]:
        """Load optional structural events CSV."""
        path = Path(path)
        errors = self._validate_csv(path, EVENTS_REQUIRED_COLS, "events")
        if any(e.severity == "error" for e in errors):
            return errors

        df = pd.read_csv(path, dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        df["date_parsed"] = pd.to_datetime(df["date"].str.strip(), format="mixed")

        bad_dates = df["date_parsed"].isna().sum()
        if bad_dates > 0:
            df = df.dropna(subset=["date_parsed"])

        if "affected_categories" not in df.columns:
            df["affected_categories"] = "ALL"

        self._events_df = df
        self._validation_warnings.append(f"Loaded {len(df)} structural events")
        return errors

    def load_events_bytes(self, content: bytes) -> list[ValidationError]:
        """Load events from in-memory bytes."""
        import tempfile
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
            f.write(content)
            tmp_path = f.name
        try:
            return self.load_events(tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def load_economic_data(self, db_cursor=None):
        """Enrich with Aradune public economic data if available."""
        if db_cursor is None:
            return

        try:
            # Pull unemployment for this state
            db_cursor.execute("""
                SELECT year, month, unemployment_rate
                FROM fact_unemployment
                WHERE state_code = ?
                ORDER BY year, month
            """, [self.state_code])
            cols = [d[0] for d in db_cursor.description]
            rows = db_cursor.fetchall()
            if rows:
                unemp_df = pd.DataFrame(rows, columns=cols)
                unemp_df["month_parsed"] = pd.to_datetime(
                    unemp_df["year"].astype(str) + "-" +
                    unemp_df["month"].astype(str).str.zfill(2) + "-01"
                )
                self._economic_df = unemp_df[["month_parsed", "unemployment_rate"]]
                self._validation_warnings.append(
                    f"Economic covariates: unemployment ({len(rows)} months)"
                )
        except Exception:
            pass  # Economic data is optional enrichment

    def forecast(
        self,
        horizon_months: int = 36,
        include_seasonality: bool = True,
        include_economic: bool = True,
        holdout_months: int = 6,
    ) -> ForecastResult:
        """
        Run forecasting on all categories.

        Args:
            horizon_months: How many months to forecast
            include_seasonality: Include seasonal component in models
            include_economic: Use economic covariates if available
            holdout_months: Months to hold out for MAPE evaluation (0=none)
        """
        if self._caseload_df is None:
            raise RuntimeError("No caseload data loaded. Call load_caseload() first.")

        horizon_months = min(horizon_months, MAX_HORIZON_MONTHS)

        # Aggregate to category × month (collapse county/region if present)
        agg_df = (
            self._caseload_df
            .groupby(["month_parsed", "category"])["enrollment_num"]
            .sum()
            .reset_index()
        )

        categories = sorted(agg_df["category"].unique())
        result = ForecastResult(
            state_code=self.state_code,
            state_name=self.state_name,
            forecast_date=str(date.today()),
            horizon_months=horizon_months,
            n_categories=len(categories),
            has_regional_data="region" in self._caseload_df.columns
                or "county" in self._caseload_df.columns,
            has_delivery_system="delivery_system" in self._caseload_df.columns,
            warnings=list(self._validation_warnings),
        )

        # Build events list (user events + known events)
        all_events = self._build_events_list()

        # Forecast each category
        category_forecasts = []
        for cat in categories:
            cat_data = agg_df[agg_df["category"] == cat].copy()
            cat_data = cat_data.sort_values("month_parsed")
            cat_data = cat_data.set_index("month_parsed")

            # Ensure continuous monthly frequency
            cat_series = cat_data["enrollment_num"].resample("MS").sum()
            cat_series = cat_series[cat_series > 0]  # drop zero-fill gaps

            if len(cat_series) < MIN_HISTORY_MONTHS:
                result.warnings.append(
                    f"Category '{cat}' has only {len(cat_series)} months — skipping"
                )
                continue

            # Get relevant events for this category
            cat_events = [
                e for e in all_events
                if e.get("affected_categories") == "ALL"
                or cat in (e.get("affected_categories") or "").split(";")
            ]

            cf = self._forecast_category(
                cat, cat_series, cat_events,
                horizon_months=horizon_months,
                include_seasonality=include_seasonality,
                include_economic=include_economic,
                holdout_months=holdout_months,
            )
            category_forecasts.append(cf)

        result.categories = category_forecasts
        result.total_history_months = int(
            agg_df["month_parsed"].nunique()
        ) if not agg_df.empty else 0

        # Build aggregate forecast
        self._build_aggregate(result)

        return result

    # --- Internal methods ---

    def _build_events_list(self) -> list[dict]:
        """Combine known events with user events, deduplicating by date+type."""
        events = []
        seen = set()  # (date, event_type) to deduplicate

        # Add user events first (they take priority)
        if self._events_df is not None:
            for _, row in self._events_df.iterrows():
                dt = row["date_parsed"].strftime("%Y-%m")
                etype = row.get("event_type", "user_event")
                key = (dt, etype)
                if key not in seen:
                    seen.add(key)
                    events.append({
                        "date": dt,
                        "event_type": etype,
                        "description": row.get("description", ""),
                        "affected_categories": row.get("affected_categories", "ALL"),
                        "source": "user",
                    })

        # Add known events only if not already covered by user events
        for ke in KNOWN_EVENTS:
            key = (ke["date"], ke["event_type"])
            if key not in seen:
                seen.add(key)
                events.append({
                    "date": ke["date"],
                    "event_type": ke["event_type"],
                    "description": ke["description"],
                    "affected_categories": ke["affected_categories"],
                    "source": "system",
                })

        return events

    def _build_intervention_matrix(
        self, index: pd.DatetimeIndex, events: list[dict]
    ) -> Optional[pd.DataFrame]:
        """Build exogenous intervention variables for SARIMAX."""
        if not events:
            return None

        exog = pd.DataFrame(index=index)
        for evt in events:
            try:
                evt_date = pd.Timestamp(evt["date"] + "-01")
            except Exception:
                continue

            col_name = evt["event_type"]
            # Avoid duplicate column names
            if col_name in exog.columns:
                col_name = f"{col_name}_{evt['date']}"

            # Step function: 0 before event, 1 after
            exog[col_name] = (index >= evt_date).astype(int)

        # Drop columns that are all 0 or all 1 (no information)
        for col in exog.columns:
            if exog[col].nunique() <= 1:
                exog = exog.drop(columns=[col])

        if exog.empty:
            return None

        return exog

    def _forecast_category(
        self,
        category: str,
        series: pd.Series,
        events: list[dict],
        horizon_months: int,
        include_seasonality: bool,
        include_economic: bool,
        holdout_months: int,
    ) -> CategoryForecast:
        """Forecast a single category using model competition."""
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        from statsmodels.tsa.exponential_smoothing.ets import ETSModel

        # Build intervention matrix
        exog = self._build_intervention_matrix(series.index, events)

        # Add economic covariates if available
        econ_used = []
        if include_economic and self._economic_df is not None:
            econ = self._economic_df.set_index("month_parsed")["unemployment_rate"]
            # Align with series index
            aligned_econ = econ.reindex(series.index)
            if aligned_econ.notna().sum() > len(series) * 0.5:
                aligned_econ = aligned_econ.ffill().bfill()
                if exog is None:
                    exog = pd.DataFrame(index=series.index)
                exog["unemployment"] = aligned_econ.values
                econ_used.append("unemployment")

        # Split for holdout validation
        if holdout_months > 0 and len(series) > holdout_months + MIN_HISTORY_MONTHS:
            train = series.iloc[:-holdout_months]
            test = series.iloc[-holdout_months:]
            exog_train = exog.iloc[:-holdout_months] if exog is not None else None
            exog_test = exog.iloc[-holdout_months:] if exog is not None else None
        else:
            train = series
            test = None
            exog_train = exog
            exog_test = None
            holdout_months = 0

        # --- Model competition ---
        best_model = None
        best_aic = float("inf")
        best_result = None
        best_label = "naive"
        best_order = ""

        seasonal_period = 12 if include_seasonality and len(train) >= 24 else 0

        # 1. Try SARIMAX models
        sarimax_orders = [
            ((1, 1, 1), (1, 0, 1, 12) if seasonal_period else (0, 0, 0, 0)),
            ((1, 1, 0), (1, 0, 0, 12) if seasonal_period else (0, 0, 0, 0)),
            ((0, 1, 1), (0, 0, 1, 12) if seasonal_period else (0, 0, 0, 0)),
            ((1, 1, 1), (0, 0, 0, 0)),  # non-seasonal ARIMA
            ((0, 1, 1), (0, 0, 0, 0)),  # simple IMA(1,1)
        ]

        for order, seasonal_order in sarimax_orders:
            try:
                model = SARIMAX(
                    train,
                    exog=exog_train,
                    order=order,
                    seasonal_order=seasonal_order,
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    res = model.fit(disp=False, maxiter=100)

                if res.aic < best_aic and np.isfinite(res.aic):
                    best_aic = res.aic
                    best_result = res
                    best_label = "sarimax"
                    best_order = f"{order}{seasonal_order}"
            except Exception:
                continue

        # 2. Try ETS
        try:
            ets_model = ETSModel(
                train,
                error="add",
                trend="add",
                damped_trend=True,
                seasonal="add" if seasonal_period else None,
                seasonal_periods=12 if seasonal_period else None,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                ets_res = ets_model.fit(disp=False, maxiter=100)

            if ets_res.aic < best_aic and np.isfinite(ets_res.aic):
                best_aic = ets_res.aic
                best_result = ets_res
                best_label = "ets"
                best_order = "AAdA" if seasonal_period else "AAdN"
        except Exception:
            pass

        # 3. Try auto_arima if available and nothing worked well
        if best_result is None:
            try:
                import pmdarima as pm
                auto = pm.auto_arima(
                    train,
                    exogenous=exog_train,
                    seasonal=bool(seasonal_period),
                    m=12 if seasonal_period else 1,
                    stepwise=True,
                    suppress_warnings=True,
                    max_p=3, max_q=3, max_P=2, max_Q=2,
                    max_order=6,
                )
                best_result = auto.arima_res_
                best_aic = auto.aic()
                best_label = "auto_arima"
                best_order = str(auto.order) + str(auto.seasonal_order)
            except Exception:
                pass

        # --- Holdout MAPE ---
        holdout_mape = None
        if test is not None and best_result is not None and best_label != "ets":
            try:
                pred = best_result.get_forecast(
                    steps=holdout_months,
                    exog=exog_test
                )
                pred_vals = pred.predicted_mean.values
                actual_vals = test.values
                mask = actual_vals > 0
                if mask.sum() > 0:
                    holdout_mape = float(np.mean(
                        np.abs(pred_vals[mask] - actual_vals[mask]) / actual_vals[mask]
                    ) * 100)
            except Exception:
                pass

        # --- Refit on full data and generate forecast ---
        if best_result is not None and best_label == "sarimax" and holdout_months > 0:
            try:
                spec = best_result.specification
                full_model = SARIMAX(
                    series,
                    exog=exog,
                    order=spec.get("order", (1, 1, 1)),
                    seasonal_order=spec.get("seasonal_order", (0, 0, 0, 0)),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    best_result = full_model.fit(disp=False, maxiter=100)
            except Exception:
                pass  # Fall back to train-only fit

        # Build future exog for forecast period
        future_dates = pd.date_range(
            start=series.index[-1] + pd.DateOffset(months=1),
            periods=horizon_months,
            freq="MS",
        )
        future_exog = None
        if exog is not None and best_label in ("sarimax", "auto_arima"):
            # Build future exog with same columns as training exog
            future_exog = pd.DataFrame(index=future_dates)
            for col in exog.columns:
                if col == "unemployment":
                    # Project unemployment forward with last known value
                    future_exog[col] = exog[col].iloc[-1]
                else:
                    # Step functions: extend based on whether event date is before future
                    # All past events remain active (step = 1)
                    future_exog[col] = exog[col].iloc[-1]
            future_exog = future_exog[exog.columns]

        # Generate forecasts
        forecast_points = []
        if best_result is not None:
            try:
                if best_label == "ets":
                    pred = best_result.get_prediction(
                        start=len(series),
                        end=len(series) + horizon_months - 1,
                    )
                    sf = pred.summary_frame(alpha=0.05)
                    sf80 = pred.summary_frame(alpha=0.20)
                    point_vals = sf["mean"].values
                    lower_95 = sf["pi_lower"].values
                    upper_95 = sf["pi_upper"].values
                    lower_80 = sf80["pi_lower"].values
                    upper_80 = sf80["pi_upper"].values
                else:
                    pred = best_result.get_forecast(
                        steps=horizon_months,
                        exog=future_exog,
                    )
                    point_vals = pred.predicted_mean.values
                    ci95 = pred.conf_int(alpha=0.05)
                    ci80 = pred.conf_int(alpha=0.20)
                    lower_95 = ci95.iloc[:, 0].values
                    upper_95 = ci95.iloc[:, 1].values
                    lower_80 = ci80.iloc[:, 0].values
                    upper_80 = ci80.iloc[:, 1].values

                for i, dt in enumerate(future_dates):
                    forecast_points.append({
                        "month": dt.strftime("%Y-%m"),
                        "point": int(max(0, round(point_vals[i]))),
                        "lower_80": int(max(0, round(lower_80[i]))),
                        "upper_80": int(max(0, round(upper_80[i]))),
                        "lower_95": int(max(0, round(lower_95[i]))),
                        "upper_95": int(max(0, round(upper_95[i]))),
                    })
            except Exception as e:
                self._validation_warnings.append(
                    f"Forecast generation failed for {category}: {e}"
                )

        # Fall back to naive forecast if models failed
        if not forecast_points:
            # Use last 12 months average
            recent = series.iloc[-min(12, len(series)):]
            avg = int(recent.mean())
            for dt in future_dates:
                forecast_points.append({
                    "month": dt.strftime("%Y-%m"),
                    "point": avg,
                    "lower_80": int(avg * 0.9),
                    "upper_80": int(avg * 1.1),
                    "lower_95": int(avg * 0.8),
                    "upper_95": int(avg * 1.2),
                })
            best_label = "naive"
            best_order = "12mo_avg"
            best_aic = float("nan")

        # Extract intervention effects
        intervention_effects = {}
        if best_result is not None and best_label in ("sarimax", "auto_arima"):
            try:
                for name in best_result.param_names:
                    if name in ("sigma2",) or name.startswith("ar.") or name.startswith("ma."):
                        continue
                    intervention_effects[name] = float(best_result.params[name])
            except Exception:
                pass

        # Build actuals list
        actuals = [
            {"month": dt.strftime("%Y-%m"), "enrollment": int(val)}
            for dt, val in series.items()
        ]

        # Build events output
        events_output = [
            {"date": e["date"], "type": e["event_type"],
             "description": e["description"], "source": e.get("source", "")}
            for e in events
        ]

        cf = CategoryForecast(
            category=category,
            model_used=best_label,
            model_order=best_order,
            fit_aic=round(best_aic, 1) if np.isfinite(best_aic) else None,
            fit_mape=round(holdout_mape, 2) if holdout_mape is not None else None,
            history_months=len(series),
            forecast_months=horizon_months,
            actuals=actuals,
            forecasts=forecast_points,
            events=events_output,
            intervention_effects=intervention_effects,
            warnings=[],
        )

        if econ_used:
            cf.warnings.append(f"Economic covariates: {econ_used}")

        return cf

    def _build_aggregate(self, result: ForecastResult):
        """Build aggregate (total) forecast from per-category forecasts."""
        if not result.categories:
            return

        # Aggregate actuals
        actual_dict: dict[str, int] = {}
        for cf in result.categories:
            for a in cf.actuals:
                actual_dict[a["month"]] = actual_dict.get(a["month"], 0) + a["enrollment"]

        result.aggregate_actuals = [
            {"month": m, "enrollment": v}
            for m, v in sorted(actual_dict.items())
        ]

        # Aggregate forecasts
        fc_dict: dict[str, dict] = {}
        for cf in result.categories:
            for f in cf.forecasts:
                m = f["month"]
                if m not in fc_dict:
                    fc_dict[m] = {
                        "month": m, "point": 0,
                        "lower_80": 0, "upper_80": 0,
                        "lower_95": 0, "upper_95": 0,
                    }
                fc_dict[m]["point"] += f["point"]
                fc_dict[m]["lower_80"] += f["lower_80"]
                fc_dict[m]["upper_80"] += f["upper_80"]
                fc_dict[m]["lower_95"] += f["lower_95"]
                fc_dict[m]["upper_95"] += f["upper_95"]

        result.aggregate_forecasts = [
            fc_dict[m] for m in sorted(fc_dict.keys())
        ]

        # Covariates used
        if self._economic_df is not None:
            result.economic_covariates_used.append("unemployment_rate")

    def _validate_csv(
        self, path: Path, required_cols: set, label: str
    ) -> list[ValidationError]:
        """Validate a CSV has the required columns."""
        errors = []

        if not path.exists():
            errors.append(ValidationError("file", f"{label} file not found", "error"))
            return errors

        try:
            with open(path) as f:
                reader = csv.reader(f)
                header = next(reader, None)
        except Exception as e:
            errors.append(ValidationError("file", f"Cannot read {label}: {e}", "error"))
            return errors

        if not header:
            errors.append(ValidationError("file", f"{label} file is empty", "error"))
            return errors

        header_lower = {h.strip().lower() for h in header}
        missing = required_cols - header_lower
        if missing:
            errors.append(ValidationError(
                "columns",
                f"{label} missing required columns: {missing}. "
                f"Found: {sorted(header_lower)}",
                "error"
            ))

        return errors


# ---------------------------------------------------------------------------
# Template generation
# ---------------------------------------------------------------------------

def generate_caseload_template(
    include_regional: bool = False,
    include_delivery: bool = False,
) -> str:
    """Generate a blank caseload CSV template."""
    buf = io.StringIO()
    writer = csv.writer(buf)

    cols = ["month", "category", "enrollment"]
    if include_regional:
        cols.extend(["county", "region"])
    if include_delivery:
        cols.append("delivery_system")

    writer.writerow(cols)

    # Example rows
    example_categories = [
        "SSI Aged", "SSI Disabled", "TANF Children", "TANF Adults",
        "Medically Needy", "SOBRA Pregnant Women", "Refugee",
        "MMA Managed Care", "LTC Managed Care",
    ]
    example_months = ["2020-01", "2020-02", "2020-03"]

    for month in example_months:
        for cat in example_categories:
            row = [month, cat, ""]
            if include_regional:
                row.extend(["", ""])
            if include_delivery:
                row.append("")
            writer.writerow(row)

    buf.seek(0)
    return buf.getvalue()


def generate_events_template() -> str:
    """Generate a blank events CSV template with example events."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "date", "event_type", "description", "affected_categories", "magnitude",
    ])

    # Pre-fill with known events
    writer.writerow([
        "2020-03", "phe_start", "COVID-19 PHE declared", "ALL", "",
    ])
    writer.writerow([
        "2023-04", "unwinding_start", "PHE unwinding — redeterminations resume",
        "ALL", "",
    ])
    writer.writerow([
        "", "mc_launch", "Example: new managed care program",
        "MMA Managed Care", "moderate",
    ])
    writer.writerow([
        "", "eligibility_change", "Example: income threshold change",
        "TANF Children;TANF Adults", "moderate",
    ])

    buf.seek(0)
    return buf.getvalue()
