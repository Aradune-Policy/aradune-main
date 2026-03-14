import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ScatterChart, Scatter, ZAxis, LineChart, Line } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

// ── Design System (matches Aradune v14) ─────────────────────────────────
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
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── Interfaces ─────────────────────────────────────────────────────────
interface PenetrationSpending { state_code: string; mc_penetration_pct: number; per_enrollee_spending: number }
interface McoRow { state_code: string; plan_name: string; program_name: string; member_months: number; adjusted_mlr: number; mlr_numerator: number; mlr_denominator: number; remittance_amount: number }
interface McoSummary { state_code: string; plan_count: number; total_member_months: number; avg_mlr: number; min_mlr: number; max_mlr: number; total_remittance: number }
interface QualityTier { mc_tier: string; measure_id: string; measure_name: string; avg_measure_rate: number; state_count: number }
interface TrendRow { year: number; state_code: string; mc_penetration_pct: number; total_spending: number }

// ── Shared Components ─────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",borderTop:accent?`3px solid ${accent}`:"none",border:`1px solid ${BD}` }}>{children}</div>
);
const CH = ({ t, b, r }: { t: string; b?: string; r?: string }) => (
  <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"10px 14px 2px" }}>
    <div><span style={{ fontSize:11,fontWeight:700,color:A }}>{t}</span>{b&&<span style={{ fontSize:9,color:AL,marginLeft:6 }}>{b}</span>}</div>
    {r&&<span style={{ fontSize:9,color:AL,fontFamily:FM }}>{r}</span>}
  </div>
);
const Met = ({ l, v, cl, sub }: { l: string; v: React.ReactNode; cl?: string; sub?: string }) => (
  <div style={{ padding:"6px 10px" }}>
    <div style={{ fontSize:8,color:AL,textTransform:"uppercase",letterSpacing:0.5,fontFamily:FM }}>{l}</div>
    <div style={{ fontSize:16,fontWeight:300,color:cl||A,fontFamily:FM }}>{v}</div>
    {sub && <div style={{ fontSize:8,color:AL,marginTop:1 }}>{sub}</div>}
  </div>
);
const Pill = ({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) => (
  <button onClick={onClick} style={{
    padding:"4px 12px",borderRadius:6,fontSize:10,fontWeight:600,fontFamily:FM,border:`1px solid ${active?cB:BD}`,
    background:active?cB:WH,color:active?WH:AL,cursor:"pointer",whiteSpace:"nowrap",
  }}>{label}</button>
);

const fmt = (n: number | null | undefined, d = 1) => n == null ? "\u2014" : n.toFixed(d);
const fmtK = (n: number | null | undefined) => n == null ? "\u2014" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();
const fmtD = (n: number | null | undefined) => n == null ? "\u2014" : `$${n >= 1e9 ? (n/1e9).toFixed(1) + "B" : n >= 1e6 ? (n/1e6).toFixed(1) + "M" : n >= 1e3 ? (n/1e3).toFixed(0) + "K" : n.toLocaleString()}`;

const SafeTip = ({ active, payload, label, formatter }: { active?: boolean; payload?: Array<{ value: number; dataKey: string }>; label?: string; formatter?: (v: number, key: string) => string }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
      <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color:AL }}>
          {p.dataKey}: {formatter ? formatter(p.value, p.dataKey) : p.value}
        </div>
      ))}
    </div>
  );
};

// ── Tabs ──────────────────────────────────────────────────────────────
const TABS = ["Penetration vs Spending", "MCO Financials", "Quality by MC Tier", "Trend Analysis"] as const;
type Tab = typeof TABS[number];

// ── Color palette for tiers ──────────────────────────────────────────
const TIER_COLORS: Record<string, string> = { "High": POS, "Medium": WARN, "Low": NEG };

// ═══════════════════════════════════════════════════════════════════════
//  MANAGED CARE VALUE ASSESSMENT MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function ManagedCareValue() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Penetration vs Spending");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Penetration vs Spending state ──
  const [penSpending, setPenSpending] = useState<PenetrationSpending[]>([]);

  // ── MCO Financials state ──
  const [mcoSummary, setMcoSummary] = useState<McoSummary[]>([]);

  // ── Quality by Tier state ──
  const [qualityTiers, setQualityTiers] = useState<QualityTier[]>([]);
  const [selectedQualMeasure, setSelectedQualMeasure] = useState<string>("");

  // ── Trend state ──
  const [trendData, setTrendData] = useState<TrendRow[]>([]);

  // ── Fetch helpers ─────────────────────────────────────────────────
  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load tab data ──
  useEffect(() => {
    setLoading(true);
    setError(null);
    const load = async () => {
      try {
        if (tab === "Penetration vs Spending") {
          const d = await fetchJson("/api/research/mc-value/penetration-spending");
          setPenSpending(d.rows || d.data || []);
        } else if (tab === "MCO Financials") {
          const d = await fetchJson("/api/research/mc-value/mco-summary");
          setMcoSummary(d.rows || d.data || []);
        } else if (tab === "Quality by MC Tier") {
          const d = await fetchJson("/api/research/mc-value/quality-by-tier");
          const rows = d.rows || d.data || [];
          setQualityTiers(rows);
          if (rows.length && !selectedQualMeasure) {
            setSelectedQualMeasure(rows[0].measure_id);
          }
        } else if (tab === "Trend Analysis") {
          const d = await fetchJson("/api/research/mc-value/trend");
          setTrendData(d.rows || d.data || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, fetchJson]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Computed: Penetration vs Spending ─────────────────────────────
  const penChart = useMemo(() =>
    penSpending.map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
      tier: r.mc_penetration_pct >= 70 ? "High" : r.mc_penetration_pct >= 40 ? "Medium" : "Low",
    })),
  [penSpending]);

  const penAvgSpending = useMemo(() => {
    if (!penSpending.length) return 0;
    return penSpending.reduce((s, r) => s + r.per_enrollee_spending, 0) / penSpending.length;
  }, [penSpending]);

  const penAvgPenetration = useMemo(() => {
    if (!penSpending.length) return 0;
    return penSpending.reduce((s, r) => s + r.mc_penetration_pct, 0) / penSpending.length;
  }, [penSpending]);

  // Tier averages
  const tierAvgs = useMemo(() => {
    const tiers = { High: { sum: 0, count: 0 }, Medium: { sum: 0, count: 0 }, Low: { sum: 0, count: 0 } };
    penChart.forEach(r => {
      const t = tiers[r.tier as keyof typeof tiers];
      if (t) { t.sum += r.per_enrollee_spending; t.count++; }
    });
    return Object.entries(tiers).map(([tier, { sum, count }]) => ({
      tier,
      avg: count ? sum / count : 0,
      count,
    }));
  }, [penChart]);

  // ── Computed: MCO Financials ──────────────────────────────────────
  const mcoChart = useMemo(() =>
    [...mcoSummary]
      .sort((a, b) => a.avg_mlr - b.avg_mlr)
      .map(r => ({
        ...r,
        name: STATE_NAMES[r.state_code] || r.state_code,
      })),
  [mcoSummary]);

  const mcoTotalRemittance = useMemo(() => mcoSummary.reduce((s, r) => s + (r.total_remittance || 0), 0), [mcoSummary]);
  const mcoBelow85 = useMemo(() => mcoSummary.filter(r => r.avg_mlr < 85).length, [mcoSummary]);
  const mcoAvgMlr = useMemo(() => {
    if (!mcoSummary.length) return 0;
    return mcoSummary.reduce((s, r) => s + r.avg_mlr, 0) / mcoSummary.length;
  }, [mcoSummary]);
  const mcoTotalPlans = useMemo(() => mcoSummary.reduce((s, r) => s + r.plan_count, 0), [mcoSummary]);

  // ── Computed: Quality by Tier ─────────────────────────────────────
  const qualMeasures = useMemo(() => {
    const seen = new Set<string>();
    return qualityTiers.filter(t => {
      if (seen.has(t.measure_id)) return false;
      seen.add(t.measure_id);
      return true;
    }).map(t => ({ id: t.measure_id, name: t.measure_name }));
  }, [qualityTiers]);

  const qualChartData = useMemo(() => {
    const measureId = selectedQualMeasure || (qualMeasures.length ? qualMeasures[0].id : "");
    return qualityTiers
      .filter(t => t.measure_id === measureId)
      .sort((a, b) => {
        const order: Record<string, number> = { "High": 0, "Medium": 1, "Low": 2 };
        return (order[a.mc_tier] ?? 3) - (order[b.mc_tier] ?? 3);
      });
  }, [qualityTiers, selectedQualMeasure, qualMeasures]);

  const qualAllMeasuresByTier = useMemo(() => {
    const map: Record<string, QualityTier[]> = {};
    qualityTiers.forEach(t => {
      if (!map[t.mc_tier]) map[t.mc_tier] = [];
      map[t.mc_tier].push(t);
    });
    return map;
  }, [qualityTiers]);

  // ── Computed: Trend ───────────────────────────────────────────────
  const trendYears = useMemo(() => {
    const years = [...new Set(trendData.map(r => r.year))].sort();
    return years;
  }, [trendData]);

  const trendAggregated = useMemo(() =>
    trendYears.map(year => {
      const yearRows = trendData.filter(r => r.year === year);
      const avgPen = yearRows.length ? yearRows.reduce((s, r) => s + r.mc_penetration_pct, 0) / yearRows.length : 0;
      const totalSpending = yearRows.reduce((s, r) => s + (r.total_spending || 0), 0);
      return { year, avgPenetration: avgPen, totalSpending, states: yearRows.length };
    }),
  [trendData, trendYears]);

  const selectedQualName = qualMeasures.find(m => m.id === selectedQualMeasure)?.name || selectedQualMeasure;

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Managed Care Value Assessment</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>MCO MLR + CMS-64 + Core Set + Enrollment</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Evaluating whether Medicaid managed care delivers on its promise of lower costs and better outcomes. Cross-state analysis of MCO finances, spending efficiency, and quality performance.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: PENETRATION VS SPENDING                                */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Penetration vs Spending" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={penSpending.length} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg MC Penetration" v={`${fmt(penAvgPenetration)}%`} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Per-Enrollee" v={fmtD(penAvgSpending)} sub="Annual spending" />
            </Card>
            <Card accent={WARN}>
              <Met l="Highest Spending" v={penSpending.length ? fmtD(Math.max(...penSpending.map(r => r.per_enrollee_spending))) : "\u2014"} />
            </Card>
          </div>

          {/* Tier breakdown */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:10 }}>
            {tierAvgs.map(t => (
              <Card key={t.tier} accent={TIER_COLORS[t.tier] || cB}>
                <CH t={`${t.tier} MC Penetration`} b={`${t.count} states`} />
                <Met l="Avg Per-Enrollee Spending" v={fmtD(t.avg)} />
              </Card>
            ))}
          </div>

          {/* Scatter chart */}
          {penChart.length > 0 ? (
            <Card>
              <CH t="MC Penetration vs Per-Enrollee Spending" b="Each dot = one state" r={`${penChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="mc-value-penetration">
                  <div style={{ width:"100%",height:400 }}>
                    <ResponsiveContainer>
                      <ScatterChart margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis type="number" dataKey="mc_penetration_pct" name="MC Penetration" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"Managed Care Penetration %",position:"insideBottom",offset:-5,fontSize:9,fill:AL }} />
                        <YAxis type="number" dataKey="per_enrollee_spending" name="Per-Enrollee" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} label={{ value:"Per-Enrollee Spending",angle:-90,position:"insideLeft",offset:10,fontSize:9,fill:AL }} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>MC Penetration: {fmt(d.mc_penetration_pct)}%</div>
                              <div style={{ color:AL }}>Per-Enrollee: {fmtD(d.per_enrollee_spending)}</div>
                              <div style={{ color:AL }}>Tier: {d.tier}</div>
                            </div>
                          );
                        }} />
                        <Scatter data={penChart} fillOpacity={0.7}>
                          {penChart.map((d, i) => (
                            <Cell key={i} fill={TIER_COLORS[d.tier] || cB} fillOpacity={0.7} />
                          ))}
                        </Scatter>
                      </ScatterChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No penetration/spending data available.</div></Card>
          )}

          {/* Table */}
          {penSpending.length > 0 && (
            <Card>
              <CH t="State-Level Comparison" r={`${penSpending.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","MC Penetration","Per-Enrollee Spending","Tier","Spending vs Avg"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...penSpending].sort((a, b) => b.mc_penetration_pct - a.mc_penetration_pct).map((r, i) => {
                      const tier = r.mc_penetration_pct >= 70 ? "High" : r.mc_penetration_pct >= 40 ? "Medium" : "Low";
                      const diff = r.per_enrollee_spending - penAvgSpending;
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.mc_penetration_pct)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.per_enrollee_spending)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",fontWeight:600,color:TIER_COLORS[tier] }}>{tier}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:diff > 0 ? NEG : POS,fontWeight:500 }}>
                            {diff > 0 ? "+" : ""}{fmtD(diff)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: MCO FINANCIALS                                         */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "MCO Financials" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total MCO Plans" v={fmtK(mcoTotalPlans)} sub={`Across ${mcoSummary.length} states`} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg MLR" v={`${fmt(mcoAvgMlr)}%`} sub="Medical loss ratio" />
            </Card>
            <Card accent={NEG}>
              <Met l="States Below 85%" v={mcoBelow85} sub="Below CMS threshold" />
            </Card>
            <Card accent={WARN}>
              <Met l="Total Remittance" v={fmtD(mcoTotalRemittance)} sub="Owed by MCOs" />
            </Card>
          </div>

          {/* MLR bar chart */}
          {mcoChart.length > 0 ? (
            <Card>
              <CH t="Average MLR by State" b="MCO medical loss ratios ranked" r={`${mcoChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="mc-value-mlr">
                  <div style={{ width:"100%",height:Math.max(360, mcoChart.length * 18) }}>
                    <ResponsiveContainer>
                      <BarChart data={mcoChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} domain={[0, "dataMax + 5"]} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Avg MLR: {fmt(d.avg_mlr)}%</div>
                              <div style={{ color:AL }}>Plans: {d.plan_count}</div>
                              <div style={{ color:AL }}>Min MLR: {fmt(d.min_mlr)}%</div>
                              <div style={{ color:AL }}>Max MLR: {fmt(d.max_mlr)}%</div>
                              <div style={{ color:AL }}>Remittance: {fmtD(d.total_remittance)}</div>
                            </div>
                          );
                        }} />
                        {/* 85% reference line */}
                        <Bar dataKey="avg_mlr" radius={[0,3,3,0]} maxBarSize={14}>
                          {mcoChart.map((d, i) => (
                            <Cell key={i} fill={d.avg_mlr < 85 ? NEG : POS} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
                <div style={{ fontSize:9,color:AL,fontFamily:FM,marginTop:4,textAlign:"center" }}>
                  Red bars indicate states with average MLR below 85% CMS threshold
                </div>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No MCO financial data available.</div></Card>
          )}

          {/* MCO table */}
          {mcoSummary.length > 0 && (
            <Card>
              <CH t="MCO Financial Summary by State" r={`${mcoSummary.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["State","Plans","Member Months","Avg MLR","Min MLR","Max MLR","Remittance"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {mcoChart.map(r => (
                      <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.name}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.plan_count}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_member_months)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_mlr < 85 ? NEG : A,fontWeight:r.avg_mlr < 85 ? 600 : 400 }}>{fmt(r.avg_mlr)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.min_mlr < 85 ? NEG : AL }}>{fmt(r.min_mlr)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.max_mlr)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.total_remittance > 0 ? NEG : AL }}>{fmtD(r.total_remittance)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: QUALITY BY MC TIER                                     */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Quality by MC Tier" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Measure selector */}
          <Card>
            <CH t="Quality Measure" b="Select a Core Set measure to compare across MC tiers" />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:6,flexWrap:"wrap" }}>
              {qualMeasures.length > 0 ? (
                <select value={selectedQualMeasure} onChange={e => setSelectedQualMeasure(e.target.value)}
                  style={{ fontSize:10,fontFamily:FM,padding:"4px 8px",borderRadius:6,border:`1px solid ${BD}`,color:AL,background:WH,maxWidth:isMobile?"100%":400 }}>
                  {qualMeasures.map(m => <option key={m.id} value={m.id}>{m.name}</option>)}
                </select>
              ) : (
                <span style={{ fontSize:10,color:AL,fontFamily:FM }}>No quality measures available</span>
              )}
            </div>
          </Card>

          {/* Tier comparison metrics */}
          {qualChartData.length > 0 && (
            <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:10 }}>
              {qualChartData.map(t => (
                <Card key={t.mc_tier} accent={TIER_COLORS[t.mc_tier] || cB}>
                  <CH t={`${t.mc_tier} MC Penetration`} b={`${t.state_count} states`} />
                  <Met l="Avg Measure Rate" v={`${fmt(t.avg_measure_rate)}%`} sub={selectedQualName} />
                </Card>
              ))}
            </div>
          )}

          {/* Grouped bar chart */}
          {qualChartData.length > 0 ? (
            <Card>
              <CH t="Quality Performance by MC Tier" b={selectedQualName} r={`${qualChartData.length} tiers`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename={`mc-value-quality-${selectedQualMeasure}`}>
                  <div style={{ width:"100%",height:280 }}>
                    <ResponsiveContainer>
                      <BarChart data={qualChartData} margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis dataKey="mc_tier" tick={{ fontSize:10,fill:AL }} />
                        <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                        <Tooltip content={<SafeTip formatter={(v, k) => k === "avg_measure_rate" ? `${v.toFixed(1)}%` : `${v}`} />} />
                        <Bar dataKey="avg_measure_rate" radius={[4,4,0,0]} maxBarSize={60}>
                          {qualChartData.map((d, i) => (
                            <Cell key={i} fill={TIER_COLORS[d.mc_tier] || cB} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No quality tier data available.</div></Card>
          )}

          {/* All measures by tier table */}
          {Object.keys(qualAllMeasuresByTier).length > 0 && (
            <Card>
              <CH t="All Measures by MC Tier" r={`${qualMeasures.length} measures`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["Measure","High MC","Medium MC","Low MC"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="Measure"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {qualMeasures.map(m => {
                      const high = qualityTiers.find(t => t.measure_id === m.id && t.mc_tier === "High");
                      const med = qualityTiers.find(t => t.measure_id === m.id && t.mc_tier === "Medium");
                      const low = qualityTiers.find(t => t.measure_id === m.id && t.mc_tier === "Low");
                      return (
                        <tr key={m.id} style={{ borderBottom:`1px solid ${BD}20`,cursor:"pointer",background:m.id === selectedQualMeasure ? `${SF}` : "transparent" }}
                          onClick={() => setSelectedQualMeasure(m.id)}>
                          <td style={{ padding:"5px 10px",fontWeight:m.id === selectedQualMeasure ? 700 : 600,color:A,maxWidth:300 }}>{m.name}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:POS }}>{high ? `${fmt(high.avg_measure_rate)}%` : "\u2014"}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:WARN }}>{med ? `${fmt(med.avg_measure_rate)}%` : "\u2014"}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:NEG }}>{low ? `${fmt(low.avg_measure_rate)}%` : "\u2014"}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 4: TREND ANALYSIS                                         */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Trend Analysis" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Years Available" v={trendYears.length} sub={trendYears.length >= 2 ? `${trendYears[0]}\u2013${trendYears[trendYears.length-1]}` : ""} />
            </Card>
            <Card accent={cB}>
              <Met l="States" v={trendData.length ? [...new Set(trendData.map(r => r.state_code))].length : 0} />
            </Card>
            <Card accent={trendAggregated.length >= 2 && trendAggregated[trendAggregated.length-1].avgPenetration > trendAggregated[0].avgPenetration ? POS : WARN}>
              <Met l="Penetration Trend"
                v={trendAggregated.length >= 2 ? `${fmt(trendAggregated[trendAggregated.length-1].avgPenetration - trendAggregated[0].avgPenetration, 1)}pp` : "\u2014"}
                sub="Change over period" />
            </Card>
            <Card accent={cB}>
              <Met l="Latest Spending" v={trendAggregated.length ? fmtD(trendAggregated[trendAggregated.length-1].totalSpending) : "\u2014"} sub="Total across states" />
            </Card>
          </div>

          {/* Dual line chart */}
          {trendAggregated.length > 0 ? (
            <Card>
              <CH t="MC Penetration & Spending Over Time" b="National averages" r={`${trendYears.length} years`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="mc-value-trend">
                  <div style={{ width:"100%",height:360 }}>
                    <ResponsiveContainer>
                      <LineChart data={trendAggregated} margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis dataKey="year" tick={{ fontSize:9,fill:AL }} />
                        <YAxis yAxisId="left" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"Avg MC Penetration %",angle:-90,position:"insideLeft",offset:10,fontSize:9,fill:AL }} />
                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} label={{ value:"Total Spending",angle:90,position:"insideRight",offset:10,fontSize:9,fill:AL }} />
                        <Tooltip content={({ active, payload, label: tipLabel }) => {
                          if (!active || !payload?.length) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{tipLabel}</div>
                              {payload.map((p: any, i: number) => (
                                <div key={i} style={{ color:AL }}>
                                  {p.dataKey === "avgPenetration" ? `Avg Penetration: ${fmt(p.value)}%` : `Total Spending: ${fmtD(p.value)}`}
                                </div>
                              ))}
                            </div>
                          );
                        }} />
                        <Line yAxisId="left" type="monotone" dataKey="avgPenetration" stroke={cB} strokeWidth={2} dot={{ fill:cB,r:4 }} name="Avg MC Penetration" />
                        <Line yAxisId="right" type="monotone" dataKey="totalSpending" stroke="#6366F1" strokeWidth={2} dot={{ fill:"#6366F1",r:4 }} name="Total Spending" />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
                <div style={{ display:"flex",gap:16,justifyContent:"center",marginTop:6 }}>
                  <span style={{ fontSize:9,fontFamily:FM,color:AL }}><span style={{ display:"inline-block",width:12,height:2,background:cB,marginRight:4,verticalAlign:"middle" }}></span>MC Penetration</span>
                  <span style={{ fontSize:9,fontFamily:FM,color:AL }}><span style={{ display:"inline-block",width:12,height:2,background:"#6366F1",marginRight:4,verticalAlign:"middle" }}></span>Total Spending</span>
                </div>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No trend data available.</div></Card>
          )}

          {/* Trend table */}
          {trendAggregated.length > 0 && (
            <Card>
              <CH t="Annual Aggregated Data" r={`${trendAggregated.length} years`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["Year","States","Avg MC Penetration","Total Spending","Spending Change"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="Year"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {trendAggregated.map((r, i) => {
                      const prevSpending = i > 0 ? trendAggregated[i-1].totalSpending : null;
                      const change = prevSpending ? r.totalSpending - prevSpending : null;
                      return (
                        <tr key={r.year} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.year}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.states}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.avgPenetration)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.totalSpending)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:change != null ? (change > 0 ? NEG : POS) : AL,fontWeight:500 }}>
                            {change != null ? `${change > 0 ? "+" : ""}${fmtD(change)}` : "\u2014"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Managed Care Value Assessment ${tab} data. ${penSpending.length} states in penetration analysis. ${mcoSummary.length} states with MCO data. ${qualMeasures.length} quality measures tracked.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS MCO MLR Reports (PY2018-2020) &middot; MACPAC Per-Enrollee Spending &middot; CMS-64 Expenditure (FY2018-2024) &middot; Medicaid Core Set (2017-2024)
      </div>
    </div>
  );
}
