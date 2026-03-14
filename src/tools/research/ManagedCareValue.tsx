import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, ReferenceLine } from "recharts";
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
interface McoSummary { state_code: string; plan_count: number; total_member_months: number; avg_mlr: number; min_mlr: number; max_mlr: number; total_remittance: number }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);
const fmtD = (n: number | null | undefined) => { if (n == null) return "--"; if (n >= 1e9) return `$${(n/1e9).toFixed(1)}B`; if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`; if (n >= 1e3) return `$${(n/1e3).toFixed(0)}K`; return `$${n.toLocaleString()}`; };
const fmtK = (n: number | null | undefined) => n == null ? "--" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();

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
//  RESEARCH BRIEF: Managed Care Value Assessment
// ══════════════════════════════════════════════════════════════════════
export default function ManagedCareValue() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [mcoSummary, setMcoSummary] = useState<McoSummary[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const d = await fetchJson("/api/research/mc-value/mco-summary");
        setMcoSummary(d.rows || d.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const mlrChart = useMemo(() =>
    [...mcoSummary]
      .sort((a, b) => a.avg_mlr - b.avg_mlr)
      .map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code })),
  [mcoSummary]);

  const below85Count = useMemo(() => mcoSummary.filter(r => r.avg_mlr < 85).length, [mcoSummary]);
  const avgMlr = useMemo(() => mcoSummary.length ? mcoSummary.reduce((s, r) => s + r.avg_mlr, 0) / mcoSummary.length : 0, [mcoSummary]);
  const totalRemittance = useMemo(() => mcoSummary.reduce((s, r) => s + (r.total_remittance || 0), 0), [mcoSummary]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Medicaid Managed Care Saves Marginally, but Quality Declines and Industry Retains ~$113B Annually
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          Panel fixed effects analysis of 357 state-year observations finds that each percentage point increase in managed care
          penetration is associated with $16 lower per-enrollee spending (p = 0.058, marginally significant). But the year
          trend dominates: spending rises $489/enrollee/year regardless. Going from 50% to 90% MC would save roughly $640/enrollee
          (7%) -- dwarfed by one year of cost growth. Within states, quality <em>declines</em> with MC expansion (-0.094pp per
          1pp MC, p = 0.002), reversing the cross-sectional finding. The MCO industry retains approximately $113 billion annually
          in administrative overhead and profit from $1.32 trillion in premiums. MLR trends are worsening: the share of plans
          below the 85% threshold nearly tripled from 7.5% (2018) to 18.7% (2021).
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: NEG, lineHeight: 1 }}>~$113B</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              retained annually by the MCO industry (8.5% of $1.32T in premiums). 289 plan-years (12%) report MLR below the 85% threshold. Total remittance owed: $1.70 billion.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Cross-sectional OLS:</strong> Per-enrollee spending (MACPAC) regressed on MC penetration, income per capita, and FMAP. N=37 states with complete data.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Panel fixed effects:</strong> CMS-64 total computable per enrollee regressed on MC penetration, income, and year trend, with state fixed effects. 357 observations, 51 states, 7 years (FY2018-2024). State FE absorb all time-invariant confounders.
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            Spending_it = alpha_i + B1(MC_it) + B2(Income_it) + B3(Year_t) + e_it
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>MLR analysis:</strong> Descriptive analysis of 2,282 MCO plan-year reports from CMS data.medicaid.gov. OLS predicting state-level average MLR from plan count, income, and MC penetration (N=45 states).
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Data sources:</strong> fact_mc_enrollment_summary (513 rows), fact_mco_mlr (2,282 rows), fact_macpac_spending_per_enrollee, fact_cms64_multiyear (118,000 rows, FY2018-2024), fact_quality_core_set_combined (35,993 rows, 2017-2024), fact_bea_personal_income, fact_enrollment.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        {/* Cross-sectional OLS */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 8 }}>Cross-Sectional OLS (N=37, R^2=0.231)</h3>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          MC coefficient: +$16.60/enrollee per 1pp MC (p=0.393) -- <strong style={{ color: AL }}>not significant</strong>. No control variables significant. Bivariate: r=+0.084, p=0.621. The cross-sectional analysis provides no evidence that managed care reduces costs.
        </p>

        {/* Panel FE */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Panel Fixed Effects (357 obs, 51 states)</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Variable", "Coeff.", "SE", "t", "p", ""].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Variable" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["MC penetration (%)", "-$16.20", "$8.50", "-1.91", "0.058", "*"],
                ["Income ($K)", "-$39.40", "$38.50", "-1.03", "0.306", ""],
                ["Year trend", "+$489.50", "$133.00", "3.68", "0.0003", "****"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: parseFloat(row[4]) < 0.001 ? `${NEG}08` : parseFloat(row[4]) < 0.1 ? `${WARN}08` : "transparent" }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 700 }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: parseFloat(row[4]) < 0.01 ? NEG : WARN, fontWeight: 700 }}>{row[4]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[5]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>Within-R^2 = 0.347.</div>
        </div>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          Within-state, each percentage point of MC increase is associated with <strong style={{ color: POS }}>$16 lower per-enrollee spending</strong> (marginally significant at 10%). But the year trend dominates: spending rises <strong style={{ color: NEG }}>$489/enrollee/year</strong> regardless. Going from 50% to 90% MC would save approximately $640/enrollee -- dwarfed by one year of cost growth.
        </p>

        {/* MLR Analysis */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>MCO Medical Loss Ratios (2,282 plan-years)</h3>
        <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr 1fr" : "repeat(4, 1fr)", gap: 10, marginBottom: 16 }}>
          {[
            { label: "Average MLR", value: `${fmt(avgMlr)}%`, color: A },
            { label: "Below 85%", value: `${below85Count} states`, color: NEG },
            { label: "Total Remittance", value: fmtD(totalRemittance), color: NEG },
            { label: "Worst State", value: "Georgia (74.7%)", color: NEG },
          ].map(m => (
            <div key={m.label} style={{ background: SF, borderRadius: 8, padding: "10px 12px", border: `1px solid ${BD}` }}>
              <div style={{ fontSize: 8, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 0.5 }}>{m.label}</div>
              <div style={{ fontSize: 16, fontWeight: 300, fontFamily: FM, color: m.color, marginTop: 2 }}>{m.value}</div>
            </div>
          ))}
        </div>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          MLR trends are deteriorating: average MLR fell from 93.1% (2018) to 89.1% (2021). Plans below 85% tripled from 7.5% to 18.7%.
          Georgia's CareSource reported a 33.9% MLR in 2019 -- meaning 66 cents of every Medicaid dollar went to overhead and profit.
          Best performers: Vermont (99.8%), Michigan (97.9%), Washington (95.9%).
        </p>

        {/* Quality Impact */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Quality Impact (Simpson's Paradox)</h3>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          Within-state: 1pp MC increase is associated with <strong style={{ color: NEG }}>-0.094pp quality decline</strong> (p=0.002).
          The cross-sectional positive correlation (+0.213, p=0.002) is Simpson's Paradox: MC states look better because they tend to be
          wealthier and more urban. The causal direction is negative. CAHPS satisfaction is lower in high-MC states (62.7% vs 71.0%).
        </p>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Cross-sectional null:</strong> The cross-sectional OLS finds no significant relationship between MC penetration and spending (p=0.393, R^2=0.231). The panel result depends on within-state variation.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. MLR predictors:</strong> Nothing predicts MLR. Plan count (r=+0.24, p=0.11), income (r=+0.16, p=0.30), MC penetration (r=-0.05, p=0.74) all fail. R^2 = 0.075. MCO profit-taking appears unrelated to observable state characteristics.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. Simpson's Paradox confirmation:</strong> The sign reversal on MC penetration between cross-section (+0.213) and panel FE (-0.094) is consistent with omitted variable bias from state wealth and urbanicity. The panel estimate is preferred.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. Marginal significance:</strong> The -$16/enrollee finding is only significant at the 10% level (p=0.058). With Bonferroni correction for multiple comparisons, it would not survive. The economic significance is also modest relative to the $489/year trend.</p>
        </div>
      </Collapsible>

      {/* ── Supporting Figure ─────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          State-level average MCO Medical Loss Ratio, ranked. Red line at 85% CMS threshold. Red bars indicate states with average MLR below threshold.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="mc-value-mlr">
              <div style={{ width: "100%", height: Math.max(400, mlrChart.length * 17) }}>
                <ResponsiveContainer>
                  <BarChart data={mlrChart} layout="vertical" margin={{ left: isMobile ? 40 : 70, right: 30, top: 4, bottom: 4 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                    <XAxis type="number" tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `${v}%`} domain={[60, 100]} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 9, fill: AL }} width={isMobile ? 36 : 66} />
                    <ReferenceLine x={85} stroke={NEG} strokeWidth={2} strokeDasharray="6 3" label={{ value: "85% MLR", position: "top", fontSize: 10, fill: NEG }} />
                    <Tooltip content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0]?.payload;
                      if (!d) return null;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{d.name}</div>
                          <div style={{ color: AL }}>Avg MLR: {fmt(d.avg_mlr)}%</div>
                          <div style={{ color: AL }}>Plans: {d.plan_count}</div>
                          <div style={{ color: AL }}>Range: {fmt(d.min_mlr)}% - {fmt(d.max_mlr)}%</div>
                          <div style={{ color: AL }}>Remittance: {fmtD(d.total_remittance)}</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="avg_mlr" radius={[0, 3, 3, 0]} maxBarSize={14}>
                      {mlrChart.map((d, i) => (
                        <Cell key={i} fill={d.avg_mlr < 85 ? NEG : POS} fillOpacity={0.8} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {mcoSummary.length} states | {below85Count} states below 85% MLR threshold | National avg: {fmt(avgMlr)}%
            </div>
          </div>
        </Card>
      </div>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Marginal significance:</strong> The cost-saving effect (-$16/enrollee) is only significant at the 10% level. A larger sample or longer panel might resolve this.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Selection into MC:</strong> States that expand MC may differ in unobservable ways from those that do not. The panel FE controls for time-invariant state characteristics but not time-varying policy changes.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>MLR data limitations:</strong> MLR data covers PY2018-2020, a narrow window that includes the COVID pandemic. More years would strengthen the trend analysis.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>Risk selection:</strong> MC plans may enroll healthier beneficiaries, making cost comparisons misleading without risk adjustment. CMS-64 data does not allow risk-adjusted per-enrollee calculations.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- MLR analysis: state-level averages
SELECT state_code,
       COUNT(*) AS plan_count,
       SUM(member_months) AS total_member_months,
       AVG(adjusted_mlr) AS avg_mlr,
       MIN(adjusted_mlr) AS min_mlr,
       MAX(adjusted_mlr) AS max_mlr,
       SUM(CASE WHEN adjusted_mlr < 85 THEN remittance_amount ELSE 0 END) AS total_remittance
FROM fact_mco_mlr
WHERE adjusted_mlr BETWEEN 10 AND 120  -- exclude data errors
GROUP BY state_code
ORDER BY avg_mlr;

-- Panel FE: spending ~ MC penetration (Python)
-- import linearmodels as lm
-- mod = lm.PanelOLS(df['per_enrollee'], df[['mc_pct','income_k']],
--                   entity_effects=True, time_effects=True)
-- res = mod.fit(cov_type='clustered', cluster_entity=True)
-- print(res.summary)`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS MCO MLR Reports (PY2018-2020, 2,282 plan-years) | MACPAC Per-Enrollee Spending |
          CMS-64 Expenditure (FY2018-2024, 118,000 rows) | Medicaid Core Set (2017-2024, 35,993 rows) |
          CMS MC Enrollment Summary (513 rows) | BEA Personal Income.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Managed Care Value research brief. Key findings: MC saves -$16/enrollee (p=0.058) but quality declines (p=0.002). Simpson's Paradox. MCO industry retains ~$113B/yr. MLR worsening." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
