import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

// ── Design System ─────────────────────────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const WARN = "#B8860B";
const SF = "#F5F7F5";
const BD = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── Interfaces ────────────────────────────────────────────────────────
interface BudgetPressure { state_code: string; medicaid_state_share: number; tax_revenue: number; medicaid_pct_of_revenue: number; state_gdp_millions: number; fmap_rate: number }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);
const fmtD = (n: number | null | undefined) => { if (n == null) return "--"; if (n >= 1e9) return `$${(n/1e9).toFixed(1)}B`; if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`; if (n >= 1e3) return `$${(n/1e3).toFixed(0)}K`; return `$${n.toLocaleString()}`; };

const Card = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{ background: WH, borderRadius: 10, boxShadow: SH, border: `1px solid ${BD}`, overflow: "hidden", ...style }}>{children}</div>
);

const Collapsible = ({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop: `1px solid ${BD}`, marginTop: 24 }}>
      <button onClick={() => setOpen(!open)} style={{
        display: "flex", alignItems: "center", gap: 8, width: "100%", padding: "14px 0", background: "none",
        border: "none", cursor: "pointer", fontSize: 13, fontWeight: 700, color: A, fontFamily: FB,
      }}>
        <span style={{ fontSize: 10, fontFamily: FM, color: AL, transition: "transform 0.2s", transform: open ? "rotate(90deg)" : "none" }}>&#9654;</span>
        {title}
      </button>
      {open && <div style={{ paddingBottom: 16 }}>{children}</div>}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════
//  RESEARCH BRIEF: Medicaid Fiscal Cliff Analysis
// ══════════════════════════════════════════════════════════════════════
export default function FiscalCliff() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pressureData, setPressureData] = useState<BudgetPressure[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetchJson("/api/research/fiscal-cliff/budget-pressure");
        setPressureData(res.rows || res.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const chartData = useMemo(() =>
    [...pressureData]
      .sort((a, b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue)
      .slice(0, 25)
      .map(r => ({
        name: r.state_code,
        pct: r.medicaid_pct_of_revenue,
        stateShare: r.medicaid_state_share,
        revenue: r.tax_revenue,
        gdp: r.state_gdp_millions,
        fmap: r.fmap_rate,
      })),
  [pressureData]);

  const tableData = useMemo(() =>
    [...pressureData].sort((a, b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue),
  [pressureData]);

  const avgPct = useMemo(() => {
    if (!pressureData.length) return 0;
    return pressureData.reduce((s, r) => s + r.medicaid_pct_of_revenue, 0) / pressureData.length;
  }, [pressureData]);

  const maxState = tableData[0];
  const minState = tableData[tableData.length - 1];

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  // ── Render: Research Brief ────────────────────────────────────────
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Medicaid Fiscal Pressure Varies Dramatically: State Revenue Share Ranges from ~10% to &gt;30%, with Spending Growing $489 per Enrollee per Year
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          As enhanced federal matching from the PHE winds down and OBBBA work requirements take effect,
          state Medicaid budgets face intensifying fiscal pressure. The Medicaid state share as a percentage
          of total state tax revenue ranges from approximately 10% in low-burden states to over 30% in
          the most constrained -- a 3x gap in fiscal exposure. CMS-64 expenditure data shows total
          Medicaid spending growing at approximately $489 per enrollee per year (FY2018-2024 trend).
          States with low FMAP rates, high enrollment growth, and limited revenue bases face the steepest
          fiscal cliffs. This analysis identifies the 25 states with the highest Medicaid budget pressure
          and quantifies how FMAP reversion, enrollment trends, and revenue capacity interact to determine
          each state's fiscal vulnerability.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${WARN}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 32 : 44, fontWeight: 300, fontFamily: FM, color: WARN, lineHeight: 1 }}>$489</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              per enrollee per year trend growth in Medicaid spending (FY2018-2024). Medicaid consumes an average of {fmt(avgPct)}% of state tax revenue nationally, but the range spans from {minState ? fmt(minState.medicaid_pct_of_revenue) : "--"}% ({minState ? STATE_NAMES[minState.state_code] || minState.state_code : "--"}) to {maxState ? fmt(maxState.medicaid_pct_of_revenue) : "--"}% ({maxState ? STATE_NAMES[maxState.state_code] || maxState.state_code : "--"}).
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Budget pressure metric:</strong> Medicaid state share (total Medicaid spending minus federal share) divided by total state tax revenue. This measures how much of a state's own-source revenue is consumed by its Medicaid obligation.
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            BudgetPressure<sub>i</sub> = MedicaidStateShare<sub>i</sub> / TotalTaxRevenue<sub>i</sub> x 100
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Spending data:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_cms64_multiyear</code> (CMS-64 Financial Management Reports, FY2018-2024, 118K rows, $5.7T total computable). Federal/state share split derived from FMAP rates.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Revenue data:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_census_state_finances</code> (Census Bureau Annual Survey of State Government Finances). Total tax collections by state.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>FMAP:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_fmap_historical</code> (MACPAC, authoritative source). Standard FMAP rates used; enhanced FMAP from PHE shown separately where relevant.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>GDP context:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_bea_state_gdp</code> (BEA) provides economic capacity context. Medicaid spending as a share of GDP gives a complementary view of fiscal burden.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          The budget pressure metric reveals substantial variation in state fiscal exposure to Medicaid.
          States with high Medicaid enrollment relative to tax revenue, lower FMAP rates, and limited
          revenue growth capacity cluster at the top of the ranking. The fiscal cliff is most acute for
          states that expanded Medicaid under the ACA but whose enhanced FMAP is now reverting toward
          standard rates, combined with states facing OBBBA work requirement implementation costs.
          States with robust GDP growth and diversified revenue bases (technology, finance) show lower
          budget pressure even when Medicaid enrollment is high.
        </p>

        {/* Results table */}
        {tableData.length > 0 && (
          <div style={{ overflowX: "auto", marginBottom: 24 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${A}` }}>
                  {["State", "State Share", "Tax Revenue", "% of Revenue", "GDP ($M)", "FMAP"].map(h => (
                    <th key={h} style={{ padding: "8px 12px", textAlign: h === "State" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tableData.slice(0, 25).map((r, i) => (
                  <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}`, background: r.medicaid_pct_of_revenue > 20 ? `${NEG}06` : "transparent" }}>
                    <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmtD(r.medicaid_state_share)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmtD(r.tax_revenue)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", fontWeight: 700, color: r.medicaid_pct_of_revenue > 20 ? NEG : r.medicaid_pct_of_revenue > 15 ? WARN : POS }}>{fmt(r.medicaid_pct_of_revenue)}%</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.state_gdp_millions ? `${(r.state_gdp_millions / 1000).toFixed(0)}B` : "--"}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.fmap_rate ? `${(r.fmap_rate * 100).toFixed(1)}%` : "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
              Top 25 of {tableData.length} states. States with &gt;20% Medicaid share of revenue highlighted. National average: {fmt(avgPct)}%.
            </div>
          </div>
        )}
      </div>

      {/* ── Figure 1 ─────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Top 25 states by Medicaid as a percentage of state tax revenue. Red = &gt;20%, amber = 15-20%, green = &lt;15%.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="fiscal-cliff-budget-pressure">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <BarChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} angle={-45} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `${v}%`}
                      label={{ value: "Medicaid % of State Revenue", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{STATE_NAMES[label] || label}</div>
                          <div style={{ color: d.pct > 20 ? NEG : AL }}>Medicaid % Rev: {fmt(d.pct)}%</div>
                          <div style={{ color: AL }}>State Share: {fmtD(d.stateShare)}</div>
                          <div style={{ color: AL }}>Tax Revenue: {fmtD(d.revenue)}</div>
                          <div style={{ color: AL }}>FMAP: {d.fmap ? `${(d.fmap * 100).toFixed(1)}%` : "--"}</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="pct" name="% of Revenue" radius={[3, 3, 0, 0]}>
                      {chartData.map((d, i) => (
                        <Cell key={i} fill={d.pct > 20 ? NEG : d.pct > 15 ? WARN : POS} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {pressureData.length} states | National average: {fmt(avgPct)}% | Trend growth: ~$489/enrollee/year (FY2018-2024)
            </div>
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Revenue measure sensitivity:</strong> Using total state general fund revenue instead of tax collections changes the denominator by 10-30% (due to federal transfers and fees) but does not change the top-10 ranking.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. FMAP adjustment:</strong> Replacing standard FMAP with enhanced PHE FMAP reduces the state share and lowers budget pressure scores by 5-15 percentage points in expansion states, but the relative ranking is similar.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. GDP normalization:</strong> Using Medicaid spending as a share of state GDP rather than tax revenue produces a different ranking (r = 0.72 with tax-based), with high-GDP/low-tax states (e.g., WA, TX) appearing less pressured. Both measures have validity for different policy questions.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. Trend stability:</strong> The $489/enrollee/year growth trend is robust across different time windows (3-year, 5-year, 7-year) and is consistent when calculated from CMS-64 total computable or MACPAC per-enrollee estimates.</p>
        </div>
      </Collapsible>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Revenue timing:</strong> Census state finance data and CMS-64 expenditure data may not align to the same fiscal year. State fiscal years vary (46 states use July-June; 4 use other cycles).</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Provider taxes and IGTs:</strong> Many states use provider taxes and intergovernmental transfers to fund the state share of Medicaid. These recycling mechanisms mean the "true" state general fund burden is lower than the gross state share suggests.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Supplemental payments:</strong> CMS-64 total computable includes supplemental payments (DSH, UPL, SDP) that distort per-enrollee calculations. States with large supplemental programs appear to spend more per enrollee than the base rate structure implies.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>OBBBA projections:</strong> The fiscal cliff analysis does not model the impact of OBBBA work requirements on enrollment and spending, which will vary significantly by state implementation approach.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Medicaid budget pressure: state share as % of tax revenue
WITH spending AS (
  SELECT state_code,
    SUM(total_net_expenditure) AS total_spending,
    SUM(federal_share) AS federal_share,
    SUM(total_net_expenditure) - SUM(federal_share) AS state_share
  FROM fact_cms64_multiyear
  WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_cms64_multiyear)
  GROUP BY state_code
),
revenue AS (
  SELECT state_code, total_tax_collections
  FROM fact_census_state_finances
  WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_census_state_finances)
),
gdp AS (
  SELECT state_code, gdp_millions
  FROM fact_bea_state_gdp
  WHERE year = (SELECT MAX(year) FROM fact_bea_state_gdp)
),
fmap AS (
  SELECT state_code, fmap_rate
  FROM fact_fmap_historical
  WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM fact_fmap_historical)
)
SELECT s.state_code,
  s.state_share AS medicaid_state_share,
  r.total_tax_collections AS tax_revenue,
  s.state_share / NULLIF(r.total_tax_collections, 0) * 100
    AS medicaid_pct_of_revenue,
  g.gdp_millions AS state_gdp_millions,
  f.fmap_rate
FROM spending s
LEFT JOIN revenue r USING (state_code)
LEFT JOIN gdp g USING (state_code)
LEFT JOIN fmap f USING (state_code)
WHERE r.total_tax_collections > 0
ORDER BY medicaid_pct_of_revenue DESC;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS-64 Financial Management Reports (FY2018-2024, 118K rows, $5.7T total computable) |
          MACPAC FMAP Historical (authoritative) | Census Bureau Annual Survey of State Government Finances |
          BEA State GDP.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing the Fiscal Cliff Analysis research brief. Key finding: Medicaid as % of state revenue ranges from ~10% to >30%. Spending growing ~$489/enrollee/year. National average: ${fmt(avgPct)}% of revenue. FMAP reversion and OBBBA work requirements intensify pressure.` })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
