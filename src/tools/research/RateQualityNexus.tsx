import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ScatterChart, Scatter, ZAxis } from "recharts";
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
interface CorrelationPoint { state_code: string; avg_pct_medicare: number; procedure_count: number; measure_rate: number }
interface QualityMeasure { id: string; name: string }
interface AccessRow { state_code: string; avg_pct_medicare: number; hpsa_count: number }
interface WorkforceRow { state_code: string; avg_pct_medicare: number; avg_healthcare_wage: number }
interface DetailRow { state_code: string; avg_pct_medicare: number; procedure_count: number; avg_quality_rate: number; measures_reported: number; hpsa_count: number; mc_penetration_pct: number }

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
const TABS = ["Rate-Quality Correlation", "Access Impact", "Workforce Connection", "State Detail"] as const;
type Tab = typeof TABS[number];

// ── Sort helpers ──────────────────────────────────────────────────────
type SortKey = "state_code" | "avg_pct_medicare" | "procedure_count" | "avg_quality_rate" | "measures_reported" | "hpsa_count" | "mc_penetration_pct";
type SortDir = "asc" | "desc";

// ═══════════════════════════════════════════════════════════════════════
//  RATE-QUALITY NEXUS MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function RateQualityNexus() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Rate-Quality Correlation");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Correlation state ──
  const [measures, setMeasures] = useState<QualityMeasure[]>([]);
  const [selectedMeasure, setSelectedMeasure] = useState<string>("");
  const [correlation, setCorrelation] = useState<CorrelationPoint[]>([]);

  // ── Access state ──
  const [accessData, setAccessData] = useState<AccessRow[]>([]);

  // ── Workforce state ──
  const [workforceData, setWorkforceData] = useState<WorkforceRow[]>([]);

  // ── Detail state ──
  const [detailData, setDetailData] = useState<DetailRow[]>([]);
  const [sortKey, setSortKey] = useState<SortKey>("avg_pct_medicare");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // ── Fetch helpers ─────────────────────────────────────────────────
  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load measures on mount ──
  useEffect(() => {
    fetchJson("/api/research/rate-quality/measures")
      .then(d => {
        const ms = d.measures || d.rows || [];
        if (ms.length) {
          setMeasures(ms);
          if (!selectedMeasure) setSelectedMeasure(ms[0].id);
        }
      })
      .catch(() => {});
  }, [fetchJson]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Load tab data ──
  useEffect(() => {
    setLoading(true);
    setError(null);
    const load = async () => {
      try {
        if (tab === "Rate-Quality Correlation") {
          if (selectedMeasure) {
            const d = await fetchJson(`/api/research/rate-quality/correlation?measure_id=${encodeURIComponent(selectedMeasure)}`);
            setCorrelation(d.rows || d.data || []);
          }
        } else if (tab === "Access Impact") {
          const d = await fetchJson("/api/research/rate-quality/access");
          setAccessData(d.rows || d.data || []);
        } else if (tab === "Workforce Connection") {
          const d = await fetchJson("/api/research/rate-quality/workforce");
          setWorkforceData(d.rows || d.data || []);
        } else if (tab === "State Detail") {
          const d = await fetchJson("/api/research/rate-quality/detail");
          setDetailData(d.rows || d.data || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, selectedMeasure, fetchJson]);

  // ── Computed: Correlation ────────────────────────────────────────────
  const correlationChart = useMemo(() =>
    correlation.map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [correlation]);

  const corrAvgRate = useMemo(() => {
    if (!correlation.length) return 0;
    return correlation.reduce((s, r) => s + r.avg_pct_medicare, 0) / correlation.length;
  }, [correlation]);

  const corrAvgMeasure = useMemo(() => {
    if (!correlation.length) return 0;
    return correlation.reduce((s, r) => s + r.measure_rate, 0) / correlation.length;
  }, [correlation]);

  // Simple correlation direction
  const corrDirection = useMemo(() => {
    if (correlation.length < 5) return "insufficient data";
    const aboveAvgRate = correlation.filter(r => r.avg_pct_medicare > corrAvgRate);
    const belowAvgRate = correlation.filter(r => r.avg_pct_medicare <= corrAvgRate);
    const avgMeasureAbove = aboveAvgRate.length ? aboveAvgRate.reduce((s, r) => s + r.measure_rate, 0) / aboveAvgRate.length : 0;
    const avgMeasureBelow = belowAvgRate.length ? belowAvgRate.reduce((s, r) => s + r.measure_rate, 0) / belowAvgRate.length : 0;
    const diff = avgMeasureAbove - avgMeasureBelow;
    if (Math.abs(diff) < 1) return "no clear correlation";
    return diff > 0 ? "positive" : "negative";
  }, [correlation, corrAvgRate]);

  // ── Computed: Access ────────────────────────────────────────────────
  const accessChart = useMemo(() =>
    [...accessData]
      .sort((a, b) => b.hpsa_count - a.hpsa_count)
      .slice(0, 30)
      .map(r => ({
        ...r,
        name: STATE_NAMES[r.state_code] || r.state_code,
      })),
  [accessData]);

  const accessAvgRate = useMemo(() => {
    if (!accessData.length) return 0;
    return accessData.reduce((s, r) => s + r.avg_pct_medicare, 0) / accessData.length;
  }, [accessData]);

  // ── Computed: Workforce ─────────────────────────────────────────────
  const workforceChart = useMemo(() =>
    workforceData.map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [workforceData]);

  const wfAvgWage = useMemo(() => {
    if (!workforceData.length) return 0;
    return workforceData.reduce((s, r) => s + r.avg_healthcare_wage, 0) / workforceData.length;
  }, [workforceData]);

  const wfAvgRate = useMemo(() => {
    if (!workforceData.length) return 0;
    return workforceData.reduce((s, r) => s + r.avg_pct_medicare, 0) / workforceData.length;
  }, [workforceData]);

  // ── Computed: Detail (sorted) ───────────────────────────────────────
  const sortedDetail = useMemo(() => {
    const sorted = [...detailData].sort((a, b) => {
      const av = a[sortKey] ?? 0;
      const bv = b[sortKey] ?? 0;
      if (typeof av === "string" && typeof bv === "string") return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      return sortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });
    return sorted;
  }, [detailData, sortKey, sortDir]);

  const detailAvgRate = useMemo(() => {
    if (!detailData.length) return 0;
    return detailData.reduce((s, r) => s + r.avg_pct_medicare, 0) / detailData.length;
  }, [detailData]);

  const detailAvgQuality = useMemo(() => {
    if (!detailData.length) return 0;
    return detailData.reduce((s, r) => s + (r.avg_quality_rate || 0), 0) / detailData.length;
  }, [detailData]);

  const detailAvgHpsa = useMemo(() => {
    if (!detailData.length) return 0;
    return Math.round(detailData.reduce((s, r) => s + (r.hpsa_count || 0), 0) / detailData.length);
  }, [detailData]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const sortArrow = (key: SortKey) => sortKey === key ? (sortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  const measureName = measures.find(m => m.id === selectedMeasure)?.name || selectedMeasure;

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Rate-Quality Nexus</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>CPRA + Core Set + HPSA + BLS</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Does paying providers more improve Medicaid outcomes? Cross-domain analysis of rate adequacy, quality measures, workforce supply, and provider access across all states.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: RATE-QUALITY CORRELATION                               */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Rate-Quality Correlation" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Measure selector */}
          <Card>
            <CH t="Quality Measure" b="Select a Core Set measure to correlate with rate adequacy" />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:6,flexWrap:"wrap" }}>
              {measures.length > 0 && (
                <select value={selectedMeasure} onChange={e => setSelectedMeasure(e.target.value)}
                  style={{ fontSize:10,fontFamily:FM,padding:"4px 8px",borderRadius:6,border:`1px solid ${BD}`,color:AL,background:WH,maxWidth:isMobile?"100%":400 }}>
                  {measures.map(m => <option key={m.id} value={m.id}>{m.name}</option>)}
                </select>
              )}
              {measures.length === 0 && !error && (
                <span style={{ fontSize:10,color:AL,fontFamily:FM }}>Loading measures...</span>
              )}
            </div>
          </Card>

          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={correlation.length} sub="with both rate and quality data" />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Rate Adequacy" v={`${fmt(corrAvgRate)}%`} sub="of Medicare" />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Measure Rate" v={`${fmt(corrAvgMeasure)}%`} sub={measureName} />
            </Card>
            <Card accent={corrDirection === "positive" ? POS : corrDirection === "negative" ? NEG : WARN}>
              <Met l="Correlation" v={corrDirection} cl={corrDirection === "positive" ? POS : corrDirection === "negative" ? NEG : AL} />
            </Card>
          </div>

          {/* Scatter chart */}
          {correlationChart.length > 0 ? (
            <Card>
              <CH t="Rate Adequacy vs Quality Performance" b={measureName} r={`${correlationChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename={`rate-quality-${selectedMeasure}`}>
                  <div style={{ width:"100%",height:400 }}>
                    <ResponsiveContainer>
                      <ScatterChart margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis type="number" dataKey="avg_pct_medicare" name="Rate Adequacy" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"Avg % of Medicare",position:"insideBottom",offset:-5,fontSize:9,fill:AL }} />
                        <YAxis type="number" dataKey="measure_rate" name="Quality Rate" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"Quality Measure Rate",angle:-90,position:"insideLeft",offset:10,fontSize:9,fill:AL }} />
                        <ZAxis type="number" dataKey="procedure_count" range={[40, 300]} name="Procedures" />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Rate: {fmt(d.avg_pct_medicare)}% of Medicare</div>
                              <div style={{ color:AL }}>Quality: {fmt(d.measure_rate)}%</div>
                              <div style={{ color:AL }}>Procedures: {fmtK(d.procedure_count)}</div>
                            </div>
                          );
                        }} />
                        <Scatter data={correlationChart} fill={cB} fillOpacity={0.7}>
                          {correlationChart.map((d, i) => (
                            <Cell key={i} fill={d.measure_rate > corrAvgMeasure ? POS : NEG} fillOpacity={0.7} />
                          ))}
                        </Scatter>
                      </ScatterChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No correlation data available for the selected measure.</div></Card>
          )}

          {/* Correlation table */}
          {correlationChart.length > 0 && (
            <Card>
              <CH t="State-Level Data" r={`${correlation.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Avg % Medicare","Measure Rate","Procedures","vs Avg Rate","vs Avg Quality"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...correlation].sort((a, b) => b.avg_pct_medicare - a.avg_pct_medicare).map((r, i) => {
                      const rateDiff = r.avg_pct_medicare - corrAvgRate;
                      const qualDiff = r.measure_rate - corrAvgMeasure;
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.avg_pct_medicare)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.measure_rate)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.procedure_count)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:rateDiff > 0 ? POS : NEG,fontWeight:500 }}>
                            {rateDiff > 0 ? "+" : ""}{fmt(rateDiff, 1)}pp
                          </td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:qualDiff > 0 ? POS : NEG,fontWeight:500 }}>
                            {qualDiff > 0 ? "+" : ""}{fmt(qualDiff, 1)}pp
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
      {/* TAB 2: ACCESS IMPACT                                          */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Access Impact" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={accessData.length} />
            </Card>
            <Card accent={NEG}>
              <Met l="Highest HPSA Count" v={accessChart.length ? fmtK(accessChart[0]?.hpsa_count) : "\u2014"} sub={accessChart.length ? STATE_NAMES[accessChart[0]?.state_code] || "" : ""} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Rate Adequacy" v={`${fmt(accessAvgRate)}%`} sub="of Medicare" />
            </Card>
            <Card accent={WARN}>
              <Met l="Total HPSAs" v={fmtK(accessData.reduce((s, r) => s + (r.hpsa_count || 0), 0))} sub="Across all states" />
            </Card>
          </div>

          {/* Bar chart: HPSA counts colored by rate adequacy */}
          {accessChart.length > 0 ? (
            <Card>
              <CH t="Provider Shortage Areas by State" b="Colored by rate adequacy (green = higher rates)" r={`Top 30 of ${accessData.length}`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="rate-quality-access">
                  <div style={{ width:"100%",height:Math.max(360, accessChart.length * 18) }}>
                    <ResponsiveContainer>
                      <BarChart data={accessChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} label={{ value:"HPSA Designations",position:"insideBottom",offset:-5,fontSize:9,fill:AL }} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>HPSAs: {fmtK(d.hpsa_count)}</div>
                              <div style={{ color:AL }}>Rate: {fmt(d.avg_pct_medicare)}% of Medicare</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="hpsa_count" radius={[0,3,3,0]} maxBarSize={14}>
                          {accessChart.map((d, i) => (
                            <Cell key={i} fill={d.avg_pct_medicare >= accessAvgRate ? POS : NEG} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No access data available.</div></Card>
          )}

          {/* Access table */}
          {accessData.length > 0 && (
            <Card>
              <CH t="Rate Adequacy vs Provider Shortages" r={`${accessData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Avg % Medicare","HPSA Count","Rate vs Avg","Access Risk"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...accessData].sort((a, b) => b.hpsa_count - a.hpsa_count).map((r, i) => {
                      const rateDiff = r.avg_pct_medicare - accessAvgRate;
                      const highShortage = r.hpsa_count > (accessData.reduce((s, d) => s + d.hpsa_count, 0) / accessData.length);
                      const lowRate = r.avg_pct_medicare < accessAvgRate;
                      const risk = highShortage && lowRate ? "High" : highShortage || lowRate ? "Medium" : "Low";
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.avg_pct_medicare)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtK(r.hpsa_count)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:rateDiff > 0 ? POS : NEG,fontWeight:500 }}>
                            {rateDiff > 0 ? "+" : ""}{fmt(rateDiff, 1)}pp
                          </td>
                          <td style={{ padding:"5px 10px",textAlign:"right",fontWeight:600,color:risk === "High" ? NEG : risk === "Medium" ? WARN : POS }}>{risk}</td>
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
      {/* TAB 3: WORKFORCE CONNECTION                                   */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Workforce Connection" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={workforceData.length} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Healthcare Wage" v={fmtD(wfAvgWage)} sub="Annual mean" />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Rate Adequacy" v={`${fmt(wfAvgRate)}%`} sub="of Medicare" />
            </Card>
            <Card accent={POS}>
              <Met l="Highest Wage" v={workforceData.length ? fmtD(Math.max(...workforceData.map(r => r.avg_healthcare_wage))) : "\u2014"} />
            </Card>
          </div>

          {/* Scatter chart */}
          {workforceChart.length > 0 ? (
            <Card>
              <CH t="Healthcare Wages vs Medicaid Rate Adequacy" b="State-level comparison" r={`${workforceChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="rate-quality-workforce">
                  <div style={{ width:"100%",height:400 }}>
                    <ResponsiveContainer>
                      <ScatterChart margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis type="number" dataKey="avg_healthcare_wage" name="Avg Wage" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} label={{ value:"Avg Healthcare Wage",position:"insideBottom",offset:-5,fontSize:9,fill:AL }} />
                        <YAxis type="number" dataKey="avg_pct_medicare" name="Rate Adequacy" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"Avg % of Medicare",angle:-90,position:"insideLeft",offset:10,fontSize:9,fill:AL }} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Wage: {fmtD(d.avg_healthcare_wage)}</div>
                              <div style={{ color:AL }}>Rate: {fmt(d.avg_pct_medicare)}% of Medicare</div>
                            </div>
                          );
                        }} />
                        <Scatter data={workforceChart} fill={cB} fillOpacity={0.7}>
                          {workforceChart.map((d, i) => (
                            <Cell key={i} fill={d.avg_pct_medicare > wfAvgRate ? POS : NEG} fillOpacity={0.7} />
                          ))}
                        </Scatter>
                      </ScatterChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No workforce data available.</div></Card>
          )}

          {/* Workforce table */}
          {workforceData.length > 0 && (
            <Card>
              <CH t="Workforce & Rate Detail" r={`${workforceData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Avg Healthcare Wage","Avg % Medicare","Wage vs Avg","Rate vs Avg"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...workforceData].sort((a, b) => b.avg_healthcare_wage - a.avg_healthcare_wage).map((r, i) => {
                      const wageDiff = r.avg_healthcare_wage - wfAvgWage;
                      const rateDiff = r.avg_pct_medicare - wfAvgRate;
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.avg_healthcare_wage)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.avg_pct_medicare)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:wageDiff > 0 ? POS : NEG,fontWeight:500 }}>
                            {wageDiff > 0 ? "+" : ""}{fmtD(wageDiff)}
                          </td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:rateDiff > 0 ? POS : NEG,fontWeight:500 }}>
                            {rateDiff > 0 ? "+" : ""}{fmt(rateDiff, 1)}pp
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
      {/* TAB 4: STATE DETAIL                                           */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "State Detail" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={detailData.length} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Rate Adequacy" v={`${fmt(detailAvgRate)}%`} sub="of Medicare" />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Quality Rate" v={`${fmt(detailAvgQuality)}%`} />
            </Card>
            <Card accent={WARN}>
              <Met l="Avg HPSAs" v={detailAvgHpsa} sub="per state" />
            </Card>
          </div>

          {/* Full sortable table */}
          {sortedDetail.length > 0 ? (
            <Card>
              <CH t="Comprehensive State Comparison" b="Click column headers to sort" r={`${detailData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {[
                        { key: "state_code" as SortKey, label: "State", align: "left" },
                        { key: "avg_pct_medicare" as SortKey, label: "Avg % Medicare", align: "right" },
                        { key: "procedure_count" as SortKey, label: "Procedures", align: "right" },
                        { key: "avg_quality_rate" as SortKey, label: "Avg Quality", align: "right" },
                        { key: "measures_reported" as SortKey, label: "Measures", align: "right" },
                        { key: "hpsa_count" as SortKey, label: "HPSAs", align: "right" },
                        { key: "mc_penetration_pct" as SortKey, label: "MC Penetration", align: "right" },
                      ].map(col => (
                        <th key={col.key}
                          onClick={() => toggleSort(col.key)}
                          style={{ padding:"6px 10px",textAlign:col.align as "left"|"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap",cursor:"pointer",userSelect:"none" }}>
                          {col.label}{sortArrow(col.key)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedDetail.map(r => (
                      <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_pct_medicare >= detailAvgRate ? POS : NEG }}>{fmt(r.avg_pct_medicare)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.procedure_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_quality_rate >= detailAvgQuality ? POS : NEG }}>{fmt(r.avg_quality_rate)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.measures_reported}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.hpsa_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.mc_penetration_pct)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No state detail data available.</div></Card>
          )}
        </div>
      )}

      {/* Ask Aradune */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Rate-Quality Nexus ${tab} data. ${correlation.length} states in correlation. ${accessData.length} states in access. ${detailData.length} states in detail. Selected measure: ${measureName}.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Aradune about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS Medicaid Rate Comparison (CY2025) &middot; Medicaid & CHIP Core Set (2024) &middot; HRSA HPSA Designations &middot; BLS Occupational Employment (2024)
      </div>
    </div>
  );
}
