"""
Fiscal Impact Engine — Phase 4 Forecasting

Calculates the budget impact of Medicaid rate/benefit policy changes:

  Rate increase % → federal match at FMAP → UPL headroom check
  → biennial budget impact with state/federal cost split

Architecture: Reads from the Aradune data lake (CMS-64, FMAP, enrollment,
rates) via DuckDB cursor. No file upload needed — all data is in the lake.

Usage:
    from fiscal_impact import FiscalImpactEngine
    engine = FiscalImpactEngine("FL", cursor)
    engine.set_rate_adjustment("E&M", 10.0)
    engine.set_biennium(2027, 2028)
    result = engine.calculate()
    result.to_json()
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RateAdjustment:
    """A proposed rate change for a service category."""
    service_type: str       # "E&M", "HCBS", "All FFS", etc.
    increase_pct: float     # e.g. 10.0 = 10% increase
    description: str = ""


@dataclass
class FederalMatchResult:
    """FMAP-based cost split for a fiscal year."""
    fiscal_year: int
    fmap_rate: float
    total_expenditure: float
    incremental_cost: float
    state_share: float
    federal_share: float


@dataclass
class UPLAnalysis:
    """Upper Payment Limit compliance check."""
    service_type: str
    current_medicaid_rate: float
    medicare_benchmark: float
    proposed_rate: float
    headroom_pct: float
    compliant: bool
    warning: Optional[str] = None


@dataclass
class MonthlyProjection:
    """Single month in the biennial projection."""
    month: str  # YYYY-MM
    fiscal_year: int
    baseline_expenditure: float
    adjusted_expenditure: float
    incremental: float
    enrollment: int
    fmap_rate: float
    state_share: float
    federal_share: float


@dataclass
class FiscalImpactResult:
    """Complete fiscal impact analysis."""
    state_code: str
    analysis_date: str
    fy_start: int
    fy_end: int

    rate_adjustments: list = field(default_factory=list)
    monthly_projections: list = field(default_factory=list)
    fy_summaries: dict = field(default_factory=dict)
    biennial_total: dict = field(default_factory=dict)
    upl_checks: list = field(default_factory=list)
    fmap_detail: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    data_sources: list = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "state_code": self.state_code,
            "analysis_date": self.analysis_date,
            "fy_start": self.fy_start,
            "fy_end": self.fy_end,
            "rate_adjustments": [
                {"service_type": a.service_type, "increase_pct": a.increase_pct,
                 "description": a.description}
                for a in self.rate_adjustments
            ],
            "monthly_projections": [
                {"month": m.month, "fiscal_year": m.fiscal_year,
                 "baseline_expenditure": m.baseline_expenditure,
                 "adjusted_expenditure": m.adjusted_expenditure,
                 "incremental": m.incremental,
                 "enrollment": m.enrollment,
                 "fmap_rate": m.fmap_rate,
                 "state_share": m.state_share,
                 "federal_share": m.federal_share}
                for m in self.monthly_projections
            ],
            "fy_summaries": self.fy_summaries,
            "biennial_total": self.biennial_total,
            "upl_checks": [
                {"service_type": u.service_type,
                 "current_medicaid_rate": u.current_medicaid_rate,
                 "medicare_benchmark": u.medicare_benchmark,
                 "proposed_rate": u.proposed_rate,
                 "headroom_pct": round(u.headroom_pct, 2),
                 "compliant": u.compliant,
                 "warning": u.warning}
                for u in self.upl_checks
            ],
            "fmap_detail": [
                {"fiscal_year": f.fiscal_year, "fmap_rate": f.fmap_rate,
                 "total_expenditure": f.total_expenditure,
                 "incremental_cost": f.incremental_cost,
                 "state_share": f.state_share,
                 "federal_share": f.federal_share}
                for f in self.fmap_detail
            ],
            "warnings": self.warnings,
            "data_sources": self.data_sources,
        }

    def to_csv_bytes(self) -> bytes:
        """Export monthly projections as CSV."""
        import csv
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "month", "fiscal_year", "baseline_expenditure",
            "adjusted_expenditure", "incremental", "enrollment",
            "fmap_rate", "state_share", "federal_share",
        ])
        for m in self.monthly_projections:
            writer.writerow([
                m.month, m.fiscal_year, round(m.baseline_expenditure, 2),
                round(m.adjusted_expenditure, 2), round(m.incremental, 2),
                m.enrollment, round(m.fmap_rate, 4),
                round(m.state_share, 2), round(m.federal_share, 2),
            ])
        return buf.getvalue().encode()


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class FiscalImpactEngine:
    """Calculate state budget impact of Medicaid rate/benefit policy changes."""

    def __init__(self, state_code: str, cursor):
        self.state_code = state_code.upper()
        self.cur = cursor
        self.adjustments: list[RateAdjustment] = []
        self.fy_start = date.today().year + 1
        self.fy_end = self.fy_start + 1

        # Data loaded from lake
        self._fmap: dict[int, float] = {}          # fy -> fmap rate
        self._cms64: dict[int, float] = {}          # fy -> total computable
        self._enrollment: dict[str, int] = {}       # YYYY-MM -> enrollment
        self._avg_em_rate: float = 0.0              # avg Medicaid E&M rate
        self._medicare_em_rate: float = 0.0         # avg Medicare E&M rate
        self._per_enrollee: dict[int, float] = {}   # fy -> per-enrollee spending

    def set_biennium(self, fy_start: int, fy_end: int):
        self.fy_start = fy_start
        self.fy_end = fy_end

    def add_rate_adjustment(self, service_type: str, increase_pct: float, description: str = ""):
        self.adjustments.append(RateAdjustment(service_type, increase_pct, description))

    def load_data(self):
        """Load all required data from the lake."""
        self._load_fmap()
        self._load_cms64()
        self._load_enrollment()
        self._load_rates()

    def _load_fmap(self):
        """Load FMAP rates by fiscal year."""
        try:
            rows = self.cur.execute("""
                SELECT fiscal_year, fmap_rate
                FROM fact_fmap_historical
                WHERE state_code = ?
                ORDER BY fiscal_year
            """, [self.state_code]).fetchall()
            self._fmap = {int(r[0]): float(r[1]) for r in rows if r[1] is not None}
        except Exception:
            # Fallback to dim_state
            try:
                rows = self.cur.execute("""
                    SELECT fmap FROM dim_state WHERE state_code = ?
                """, [self.state_code]).fetchall()
                if rows and rows[0][0]:
                    fmap = float(rows[0][0])
                    for fy in range(self.fy_start, self.fy_end + 1):
                        self._fmap[fy] = fmap
            except Exception:
                pass

    def _load_cms64(self):
        """Load CMS-64 expenditure by fiscal year."""
        try:
            rows = self.cur.execute("""
                SELECT fiscal_year,
                       SUM(CASE WHEN item_name ILIKE '%total computable%'
                           THEN amount ELSE 0 END) AS total_computable
                FROM fact_cms64_multiyear
                WHERE state_code = ?
                GROUP BY fiscal_year
                ORDER BY fiscal_year
            """, [self.state_code]).fetchall()
            self._cms64 = {int(r[0]): float(r[1]) for r in rows if r[1]}
        except Exception:
            pass

    def _load_enrollment(self):
        """Load monthly enrollment for projection period."""
        try:
            rows = self.cur.execute("""
                SELECT year, month, total_enrollment
                FROM fact_enrollment
                WHERE state_code = ?
                ORDER BY year, month
            """, [self.state_code]).fetchall()
            for r in rows:
                if r[2] is not None:
                    key = f"{int(r[0]):04d}-{int(r[1]):02d}"
                    self._enrollment[key] = int(r[2])
        except Exception:
            pass

    def _load_rates(self):
        """Load average Medicaid and Medicare E&M rates for UPL check."""
        try:
            rows = self.cur.execute("""
                SELECT AVG(medicaid_rate), AVG(medicare_nonfac_rate)
                FROM fact_rate_comparison
                WHERE state_code = ? AND category_447 IS NOT NULL
                  AND medicaid_rate > 0 AND medicare_nonfac_rate > 0
            """, [self.state_code]).fetchall()
            if rows and rows[0][0]:
                self._avg_em_rate = float(rows[0][0])
                self._medicare_em_rate = float(rows[0][1]) if rows[0][1] else 0.0
        except Exception:
            pass

    def _get_fmap(self, fy: int) -> float:
        """Get FMAP for a fiscal year, with fallback."""
        if fy in self._fmap:
            return self._fmap[fy]
        # Use most recent available
        available = sorted(self._fmap.keys())
        if available:
            return self._fmap[available[-1]]
        return 0.50  # national average fallback

    def _get_baseline_monthly_expenditure(self, fy: int) -> float:
        """Estimate monthly baseline expenditure from CMS-64."""
        if fy in self._cms64 and self._cms64[fy] > 0:
            return self._cms64[fy] / 12.0

        # Extrapolate from most recent year with trend
        available = sorted(k for k in self._cms64.keys() if self._cms64[k] > 0)
        if len(available) >= 2:
            last_fy = available[-1]
            prev_fy = available[-2]
            growth = self._cms64[last_fy] / self._cms64[prev_fy]
            years_out = fy - last_fy
            return (self._cms64[last_fy] * (growth ** years_out)) / 12.0
        elif available:
            # Single year, assume 4% growth
            last_fy = available[-1]
            years_out = fy - last_fy
            return (self._cms64[last_fy] * (1.04 ** years_out)) / 12.0
        return 0.0

    def _get_enrollment(self, year: int, month: int) -> int:
        """Get enrollment for a month, with extrapolation."""
        key = f"{year:04d}-{month:02d}"
        if key in self._enrollment:
            return self._enrollment[key]
        # Use most recent month
        available = sorted(self._enrollment.keys())
        if available:
            return self._enrollment[available[-1]]
        return 0

    def _weighted_increase_pct(self) -> float:
        """Compute blended rate increase across all adjustments."""
        if not self.adjustments:
            return 0.0
        # Simple average for MVP; could weight by expenditure share
        return sum(a.increase_pct for a in self.adjustments) / len(self.adjustments)

    def calculate(self) -> FiscalImpactResult:
        """Run the full fiscal impact analysis."""
        self.load_data()

        result = FiscalImpactResult(
            state_code=self.state_code,
            analysis_date=date.today().isoformat(),
            fy_start=self.fy_start,
            fy_end=self.fy_end,
            rate_adjustments=list(self.adjustments),
        )

        blended_pct = self._weighted_increase_pct()
        if blended_pct == 0:
            result.warnings.append("No rate adjustments specified. Showing baseline only.")

        # Track data sources
        if self._fmap:
            result.data_sources.append(f"FMAP: fact_fmap_historical ({len(self._fmap)} years)")
        if self._cms64:
            result.data_sources.append(f"CMS-64: fact_cms64_multiyear ({len(self._cms64)} years)")
        if self._enrollment:
            result.data_sources.append(f"Enrollment: fact_enrollment ({len(self._enrollment)} months)")

        # Monthly projections across biennium
        fy_totals: dict[int, dict] = {}
        for fy in range(self.fy_start, self.fy_end + 1):
            fy_totals[fy] = {
                "baseline": 0.0, "adjusted": 0.0, "incremental": 0.0,
                "state_share": 0.0, "federal_share": 0.0,
            }
            # Federal fiscal year: Oct (fy-1) through Sep (fy)
            for m_offset in range(12):
                cal_month = ((9 + m_offset) % 12) + 1  # Oct=10, Nov=11, ..., Sep=9
                cal_year = (fy - 1) if cal_month >= 10 else fy
                month_str = f"{cal_year:04d}-{cal_month:02d}"

                fmap = self._get_fmap(fy)
                baseline = self._get_baseline_monthly_expenditure(fy)
                enrollment = self._get_enrollment(cal_year, cal_month)
                adjusted = baseline * (1 + blended_pct / 100.0)
                incremental = adjusted - baseline
                state_share = incremental * (1 - fmap)
                federal_share = incremental * fmap

                mp = MonthlyProjection(
                    month=month_str,
                    fiscal_year=fy,
                    baseline_expenditure=round(baseline, 2),
                    adjusted_expenditure=round(adjusted, 2),
                    incremental=round(incremental, 2),
                    enrollment=enrollment,
                    fmap_rate=round(fmap, 4),
                    state_share=round(state_share, 2),
                    federal_share=round(federal_share, 2),
                )
                result.monthly_projections.append(mp)

                fy_totals[fy]["baseline"] += baseline
                fy_totals[fy]["adjusted"] += adjusted
                fy_totals[fy]["incremental"] += incremental
                fy_totals[fy]["state_share"] += state_share
                fy_totals[fy]["federal_share"] += federal_share

        # FY summaries
        total_baseline = 0.0
        total_adjusted = 0.0
        total_incremental = 0.0
        total_state = 0.0
        total_federal = 0.0

        for fy in range(self.fy_start, self.fy_end + 1):
            t = fy_totals[fy]
            fmap = self._get_fmap(fy)
            result.fy_summaries[str(fy)] = {
                "baseline": round(t["baseline"], 2),
                "adjusted": round(t["adjusted"], 2),
                "incremental": round(t["incremental"], 2),
                "state_share": round(t["state_share"], 2),
                "federal_share": round(t["federal_share"], 2),
                "fmap_rate": round(fmap, 4),
            }
            result.fmap_detail.append(FederalMatchResult(
                fiscal_year=fy,
                fmap_rate=fmap,
                total_expenditure=round(t["adjusted"], 2),
                incremental_cost=round(t["incremental"], 2),
                state_share=round(t["state_share"], 2),
                federal_share=round(t["federal_share"], 2),
            ))

            total_baseline += t["baseline"]
            total_adjusted += t["adjusted"]
            total_incremental += t["incremental"]
            total_state += t["state_share"]
            total_federal += t["federal_share"]

        result.biennial_total = {
            "baseline": round(total_baseline, 2),
            "adjusted": round(total_adjusted, 2),
            "incremental": round(total_incremental, 2),
            "state_share": round(total_state, 2),
            "federal_share": round(total_federal, 2),
            "years": self.fy_end - self.fy_start + 1,
        }

        # UPL headroom check
        if self._avg_em_rate > 0 and self._medicare_em_rate > 0:
            for adj in self.adjustments:
                proposed = self._avg_em_rate * (1 + adj.increase_pct / 100.0)
                headroom = ((self._medicare_em_rate - proposed) / self._medicare_em_rate) * 100
                compliant = proposed <= self._medicare_em_rate
                warning = None
                if not compliant:
                    warning = (
                        f"Proposed rate (${proposed:.2f}) exceeds Medicare benchmark "
                        f"(${self._medicare_em_rate:.2f}). UPL waiver may be required."
                    )
                elif headroom < 5:
                    warning = f"Only {headroom:.1f}% UPL headroom remaining."

                result.upl_checks.append(UPLAnalysis(
                    service_type=adj.service_type,
                    current_medicaid_rate=round(self._avg_em_rate, 2),
                    medicare_benchmark=round(self._medicare_em_rate, 2),
                    proposed_rate=round(proposed, 2),
                    headroom_pct=headroom,
                    compliant=compliant,
                    warning=warning,
                ))

        # Warnings
        if not self._cms64:
            result.warnings.append(
                "No CMS-64 data found. Expenditure baseline estimated from enrollment."
            )
        if not self._fmap:
            result.warnings.append(
                "No FMAP history found. Using national average (50%)."
            )
        if self._enrollment:
            latest = sorted(self._enrollment.keys())[-1]
            if latest < f"{self.fy_start - 1:04d}-10":
                result.warnings.append(
                    f"Most recent enrollment data is {latest}. "
                    f"Projections for FY{self.fy_start}-{self.fy_end} use last known enrollment."
                )

        # Check for FMAP changes across biennium
        fmaps = [self._get_fmap(fy) for fy in range(self.fy_start, self.fy_end + 1)]
        if len(set(fmaps)) > 1:
            result.warnings.append(
                f"FMAP changes across biennium: "
                + ", ".join(f"FY{fy}: {self._get_fmap(fy):.2%}"
                           for fy in range(self.fy_start, self.fy_end + 1))
            )

        return result
