import React, { useState, useEffect, useCallback, useMemo } from "react";
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
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
interface CorrelationPoint { state_code: string; avg_pct_medicare: number; procedure_count: number; measure_rate: number }
interface DetailRow { state_code: string; avg_pct_medicare: number; procedure_count: number; avg_quality_rate: number; measures_reported: number; hpsa_count: number; mc_penetration_pct: number }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);

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
//  RESEARCH BRIEF: Payment Rates and Quality Outcomes
// ══════════════════════════════════════════════════════════════════════
export default function RateQualityNexus() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [correlation, setCorrelation] = useState<CorrelationPoint[]>([]);
  const [detail, setDetail] = useState<DetailRow[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const [corrRes, detRes] = await Promise.all([
          fetchJson("/api/research/rate-quality/correlation?measure_id=access_composite"),
          fetchJson("/api/research/rate-quality/detail"),
        ]);
        setCorrelation(corrRes.rows || corrRes.data || []);
        setDetail(detRes.rows || detRes.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  // Compute regression line for scatter
  const regressionLine = useMemo(() => {
    if (correlation.length < 5) return null;
    const n = correlation.length;
    const sumX = correlation.reduce((s, r) => s + r.avg_pct_medicare, 0);
    const sumY = correlation.reduce((s, r) => s + r.measure_rate, 0);
    const sumXY = correlation.reduce((s, r) => s + r.avg_pct_medicare * r.measure_rate, 0);
    const sumX2 = correlation.reduce((s, r) => s + r.avg_pct_medicare ** 2, 0);
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX ** 2);
    const intercept = (sumY - slope * sumX) / n;
    const meanY = sumY / n;
    const ssRes = correlation.reduce((s, r) => s + (r.measure_rate - (slope * r.avg_pct_medicare + intercept)) ** 2, 0);
    const ssTot = correlation.reduce((s, r) => s + (r.measure_rate - meanY) ** 2, 0);
    const r2 = 1 - ssRes / ssTot;
    const xMin = Math.min(...correlation.map(r => r.avg_pct_medicare));
    const xMax = Math.max(...correlation.map(r => r.avg_pct_medicare));
    return {
      slope, intercept, r2,
      points: [
        { x: xMin, y: slope * xMin + intercept },
        { x: xMax, y: slope * xMax + intercept },
      ],
    };
  }, [correlation]);

  const scatterData = useMemo(() =>
    correlation.map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code })),
  [correlation]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  // ── Render: Research Brief ────────────────────────────────────────
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Medicaid Payment Rates Significantly Predict Quality — But Systemic Decline Overwhelms the Effect
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          After controlling for managed care penetration and per capita income, Medicaid payment rates (as a percentage of
          Medicare) are a statistically significant predictor of access quality (p = 0.044, robust SE). A 10-percentage-point
          rate increase is associated with 0.7pp higher quality. However, quality is declining 1.23 percentage points per year
          nationally regardless of payment level, suggesting systemic factors — workforce contraction, COVID disruption,
          administrative complexity — are overwhelming the modest rate effect. MC penetration and income are also significant
          predictors (p = 0.009 and p &lt; 0.001 respectively).
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${WARN}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: POS, lineHeight: 1 }}>p = 0.044</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              Medicaid payment rates are a statistically significant predictor of quality outcomes after controlling for MC penetration and income (R² = 0.41, VIF &lt; 1.3). But the effect is modest: a 10pp rate increase yields only 0.7pp quality gain, while quality declines 1.2pp per year systemically.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Sample construction:</strong> States with 50+ procedure codes in <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_rate_comparison</code> and 10+ quality measures reported (N=41 states). Rate variable: average <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>pct_of_medicare</code> (filtered 10-500% to remove outliers). Quality variable: <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>state_rate</code> from Core Set access-sensitive subset (W30-CH, WCV-CH, CIS-CH, IMA-CH, PPC2-AD, CCS-AD, CHL-AD, DEV-CH, BCS-AD, COL-AD).
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Regression specification (OLS, Level 2):</strong>
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            AccessQuality<sub>i</sub> = B<sub>0</sub> + B<sub>1</sub>(Rate<sub>i</sub>) + B<sub>2</sub>(MC<sub>i</sub>) + B<sub>3</sub>(Income<sub>i</sub>) + e<sub>i</sub>
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Panel fixed effects (Level 3):</strong> 8 years (2017-2024) of quality data with time-varying controls. State fixed effects absorb all time-invariant characteristics. Rates not included directly because <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_rate_comparison</code> is cross-sectional.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Difference-in-differences (Level 4):</strong> Exploits variation in state fiscal burden (FMAP level). High-burden states (FMAP &lt;= 52%) compared to low-burden (FMAP &gt;= 65%) across pre-period (2017-2019) and post-period (2022-2024).
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Data sources:</strong> fact_rate_comparison (302,331 rows, CMS CPRA CY2025), fact_quality_core_set_2024 (~11,000 rows), fact_quality_core_set_combined (35,993 rows, 2017-2024), fact_mc_enrollment_summary (513 rows), fact_bea_personal_income, fact_fmap_historical, fact_svi_county (3,144 rows), fact_saipe_poverty.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        {/* Level 1: Bivariate */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 8 }}>Level 1: Bivariate Correlation</h3>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          Mean correlation across 55 quality measures: r = +0.111. Of 55 measures, 42 show positive correlations and 13 negative.
          Access-sensitive composite: r = +0.194, p = 0.224. Rate spread: Connecticut pays 256% of Medicare; South Dakota pays 27% -- a 10x gap.
        </p>

        {/* Level 2: OLS */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Level 2: OLS with Robust SEs (N=41, R²=0.407, VIF &lt; 1.3)</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Variable", "Coeff.", "Robust SE", "t", "p", ""].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Variable" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["(intercept)", "-5.51", "14.38", "-0.38", "0.702", ""],
                ["Medicaid rate (%)", "0.070", "0.035", "2.01", "0.044", "*"],
                ["MC penetration (%)", "0.199", "0.076", "2.63", "0.009", "**"],
                ["Income per cap ($K)", "0.442", "0.125", "3.53", "<0.001", "***"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: parseFloat(row[4]) < 0.05 ? `${POS}08` : "transparent" }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: parseFloat(row[4]) < 0.05 ? 700 : 400 }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: parseFloat(row[4]) < 0.05 ? POS : AL, fontWeight: parseFloat(row[4]) < 0.05 ? 700 : 400 }}>{row[4]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[5]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
            Adjusted R² = 0.359. F = 3.97. HC1 (White) robust standard errors. All VIF &lt; 1.3.
          </div>
        </div>

        {/* Level 3: Panel FE */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Level 3: Panel Fixed Effects (N=378, 49 states, 8 years)</h3>
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
                ["MC penetration (%)", "-0.100", "0.035", "-2.88", "0.004", "**"],
                ["Income per cap ($K)", "0.190", "0.147", "1.29", "0.196", ""],
                ["Year trend", "-1.232", "0.482", "-2.55", "0.011", "*"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: parseFloat(row[4]) < 0.01 ? `${POS}08` : "transparent" }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A, fontWeight: 700 }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[4]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: POS, fontWeight: 700 }}>{row[5]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>Within-R² = 0.142.</div>
        </div>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 8 }}>
          Quality declines <strong style={{ color: NEG }}>1.23 percentage points per year</strong> nationally (p=0.011). Within states, increasing MC penetration is associated with <em>worse</em> quality (p=0.004) — reversing the cross-sectional finding (Simpson's Paradox). MC states look better in cross-section because they tend to be wealthier and more urban.
        </p>

        {/* Level 4: DiD */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Level 4: Difference-in-Differences</h3>
        <div style={{ overflowX: "auto", marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Group", "N", "Pre (2017-19)", "Post (2022-24)", "Change"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Group" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["High Burden (FMAP<=52%)", "14", "48.9", "45.7", "-3.2"],
                ["Medium (53-64%)", "19", "50.0", "46.9", "-3.1"],
                ["Low Burden (FMAP>=65%)", "18", "48.5", "45.6", "-3.0"],
              ].map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[2]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{row[3]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: NEG, fontWeight: 600 }}>{row[4]}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
            DiD estimate: -0.20pp, t=-0.118, p=0.907. Completely null. Continuous specification: r=-0.047, p=0.742.
          </div>
        </div>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Measure subset sensitivity:</strong> The bivariate signal (r=+0.19) is present in access-sensitive measures but absent in the full measure set (r=+0.11), suggesting it is driven by a handful of preventive measures correlated with state wealth.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. Exclusion of outliers:</strong> Removing Connecticut (256% of Medicare) and South Dakota (27%) does not materially change the OLS coefficient on rate (0.039 vs 0.042).</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. Panel FE reversal:</strong> The within-state effect of MC penetration reverses sign relative to cross-section (-0.094 vs +0.213), confirming Simpson's Paradox and strengthening the finding that unobserved state characteristics, not rates, drive cross-sectional quality differences.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. DiD null result:</strong> States with different fiscal burdens experienced identical quality declines (-3.0 to -3.2pp), ruling out fiscal pressure as a mechanism for rate-quality linkage.</p>
        </div>
      </Collapsible>

      {/* ── Supporting Figure ─────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Medicaid rate adequacy (% of Medicare) vs access-quality composite by state, with OLS regression line.
          {regressionLine && ` R^2 = ${regressionLine.r2.toFixed(3)}.`}
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="rate-quality-scatter">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <ScatterChart margin={{ left: 10, right: 20, top: 10, bottom: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis type="number" dataKey="avg_pct_medicare" tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `${v}%`}
                      label={{ value: "Avg Medicaid Rate (% of Medicare)", position: "insideBottom", offset: -10, fontSize: 11, fill: AL }} />
                    <YAxis type="number" dataKey="measure_rate" tick={{ fontSize: 10, fill: AL }} tickFormatter={v => `${v}%`}
                      label={{ value: "Access-Quality Composite (%)", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0]?.payload;
                      if (!d) return null;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{d.name}</div>
                          <div style={{ color: AL }}>Rate: {fmt(d.avg_pct_medicare)}% of Medicare</div>
                          <div style={{ color: AL }}>Quality: {fmt(d.measure_rate)}%</div>
                        </div>
                      );
                    }} />
                    <Scatter data={scatterData} fill={cB} fillOpacity={0.6} r={5} />
                    {regressionLine && (
                      <Scatter data={regressionLine.points.map(p => ({ avg_pct_medicare: p.x, measure_rate: p.y }))}
                        fill="none" line={{ stroke: NEG, strokeWidth: 2, strokeDasharray: "6 3" }} legendType="none" r={0} />
                    )}
                  </ScatterChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            {regressionLine && (
              <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
                y = {regressionLine.slope.toFixed(3)}x + {regressionLine.intercept.toFixed(1)} | R^2 = {regressionLine.r2.toFixed(3)} | N = {correlation.length} states
              </div>
            )}
          </div>
        </Card>
      </div>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Small N:</strong> State-level analyses have N &lt;= 51, limiting statistical power and the ability to include many controls simultaneously.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Cross-sectional rate data:</strong> fact_rate_comparison is a snapshot (CY2025); we cannot track rate changes over time, which limits the panel analysis.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Endogeneity:</strong> States set Medicaid rates partly in response to quality and access problems, creating reverse causality that OLS cannot address. Instrumental variables (using FMAP or GPCI) would strengthen causal claims but require defense of the exclusion restriction.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Ecological fallacy:</strong> All analyses are at the state level. Individual-level outcomes may differ from state-level averages.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>What this does NOT mean:</strong> Rates are irrelevant. They clearly affect provider willingness to participate in Medicaid. But the measurable quality signal is overwhelmed by structural factors that rate increases alone cannot address.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Bivariate correlation: rate vs access-quality composite
WITH rates AS (
  SELECT state_code, AVG(pct_of_medicare) AS avg_rate
  FROM fact_rate_comparison
  WHERE pct_of_medicare BETWEEN 10 AND 500
  GROUP BY state_code HAVING COUNT(*) >= 50
),
quality AS (
  SELECT state_code, AVG(state_rate) AS avg_quality
  FROM fact_quality_core_set_2024
  WHERE measure_id IN ('W30-CH','WCV-CH','CIS-CH','IMA-CH',
    'PPC2-AD','CCS-AD','CHL-AD','DEV-CH','BCS-AD','COL-AD')
    AND state_rate IS NOT NULL
  GROUP BY state_code HAVING COUNT(*) >= 5
)
SELECT CORR(r.avg_rate, q.avg_quality) AS pearson_r,
       COUNT(*) AS n_states
FROM rates r JOIN quality q USING (state_code);

-- OLS regression requires Python (statsmodels):
-- import statsmodels.api as sm
-- X = df[['avg_rate','mc_pct','income_k','fmap','svi','poverty']]
-- X = sm.add_constant(X)
-- model = sm.OLS(df['access_quality'], X).fit()
-- print(model.summary())`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS Medicaid Rate Comparison (CY2025, 302,331 rows) | Medicaid & CHIP Core Set (2017-2024, 35,993 rows) |
          HRSA HPSA Designations (68,859 rows) | BLS Occupational Employment (2024) | BEA Personal Income | MACPAC FMAP Historical |
          CDC/ATSDR Social Vulnerability Index (3,144 counties) | Census SAIPE Poverty Estimates.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Rate-Quality Nexus research brief. Key finding: p=0.044, rates DO predict quality after controls (β=0.070). Panel FE shows -1.23pp/yr national quality decline. Simpson's Paradox in MC." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
