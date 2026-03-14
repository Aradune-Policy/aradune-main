import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ScatterChart, Scatter, ZAxis, LineChart, Line } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

// -- Design System (matches Aradune v14) --
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

const STATE_NAMES: Record<string, string> = {
  AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"North Carolina",ND:"North Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"South Carolina",SD:"South Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"West Virginia",WI:"Wisconsin",WY:"Wyoming",DC:"District of Columbia",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"
};
const fmt = (n: number | null | undefined, d = 1) => n == null ? "\u2014" : n.toFixed(d);
const fmtK = (n: number | null | undefined) => n == null ? "\u2014" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();
const fmtD = (n: number | null | undefined) => { if (n == null) return "\u2014"; if (n >= 1e9) return `$${(n/1e9).toFixed(1)}B`; if (n >= 1e6) return `$${(n/1e6).toFixed(1)}M`; if (n >= 1e3) return `$${(n/1e3).toFixed(0)}K`; return `$${n.toLocaleString()}`; };
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
const SafeTip = ({ active, payload, label, formatter }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
      <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{label}</div>
      {payload.map((p: any, i: number) => (
        <div key={i} style={{ color:AL }}>{p.dataKey}: {formatter ? formatter(p.value, p.dataKey) : p.value}</div>
      ))}
    </div>
  );
};

// -- Data Interfaces --
interface SpendingRevenue { state_code: string; fiscal_year: number; total_spending: number; federal_share: number; state_share: number; total_tax_collections: number; medicaid_pct_of_revenue: number }
interface FmapRow { state_code: string; fiscal_year: number; fmap_rate: number; enhanced_fmap: number }
interface BudgetPressure { state_code: string; medicaid_state_share: number; tax_revenue: number; medicaid_pct_of_revenue: number; state_gdp_millions: number; fmap_rate: number }
interface VulnerabilityRow { state_code: string; budget_share_pct: number; spending_growth_pct: number; state_burden_pct: number }

type Tab = "spending" | "fmap" | "pressure" | "vulnerability";

const FMAP_DEFAULT_STATES = ["FL", "CA", "TX", "NY", "OH", "MS"];

export default function FiscalCliff() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();

  const [tab, setTab] = useState<Tab>("spending");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [spendingData, setSpendingData] = useState<SpendingRevenue[]>([]);
  const [fmapData, setFmapData] = useState<FmapRow[]>([]);
  const [pressureData, setPressureData] = useState<BudgetPressure[]>([]);
  const [vulnData, setVulnData] = useState<VulnerabilityRow[]>([]);

  const [selectedFmapStates, setSelectedFmapStates] = useState<Set<string>>(new Set(FMAP_DEFAULT_STATES));

  useEffect(() => {
    setLoading(true);
    setLoadError(null);
    const endpoints: Record<Tab, string> = {
      spending: "/api/research/fiscal-cliff/spending-vs-revenue",
      fmap: "/api/research/fiscal-cliff/fmap-impact",
      pressure: "/api/research/fiscal-cliff/budget-pressure",
      vulnerability: "/api/research/fiscal-cliff/vulnerability",
    };
    fetch(`${API_BASE}${endpoints[tab]}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        const rows = Array.isArray(d) ? d : d.rows || d.data || [];
        if (tab === "spending") setSpendingData(rows);
        else if (tab === "fmap") setFmapData(rows);
        else if (tab === "pressure") setPressureData(rows);
        else setVulnData(rows);
      })
      .catch(e => setLoadError(e.message))
      .finally(() => setLoading(false));
  }, [tab]);

  // -- Spending vs Revenue --
  const latestFY = useMemo(() => {
    if (!spendingData.length) return 0;
    return Math.max(...spendingData.map(r => r.fiscal_year));
  }, [spendingData]);

  const spendingLatest = useMemo(() =>
    spendingData.filter(r => r.fiscal_year === latestFY), [spendingData, latestFY]);

  const spendingChart = useMemo(() =>
    [...spendingLatest].sort((a, b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue)
      .slice(0, 30).map(r => ({
        name: r.state_code, pct: r.medicaid_pct_of_revenue, spending: r.total_spending, revenue: r.total_tax_collections,
      })), [spendingLatest]);

  // -- FMAP over time --
  const fmapStates = useMemo(() => {
    const s = new Set<string>();
    fmapData.forEach(r => s.add(r.state_code));
    return Array.from(s).sort();
  }, [fmapData]);

  const fmapLineData = useMemo(() => {
    const byYear = new Map<number, Record<string, number>>();
    fmapData.filter(r => selectedFmapStates.has(r.state_code)).forEach(r => {
      if (!byYear.has(r.fiscal_year)) byYear.set(r.fiscal_year, {});
      byYear.get(r.fiscal_year)![r.state_code] = r.fmap_rate;
    });
    return Array.from(byYear.entries()).sort(([a],[b]) => a - b).map(([fy, vals]) => ({
      name: `FY${fy}`, ...vals,
    }));
  }, [fmapData, selectedFmapStates]);

  const fmapStats = useMemo(() => {
    if (!fmapData.length) return { avgFmap: 0, minFmap: 0, maxFmap: 0, minState: "", maxState: "" };
    const latest = Math.max(...fmapData.map(r => r.fiscal_year));
    const latestRows = fmapData.filter(r => r.fiscal_year === latest);
    if (!latestRows.length) return { avgFmap: 0, minFmap: 0, maxFmap: 0, minState: "", maxState: "" };
    const avg = latestRows.reduce((s, r) => s + r.fmap_rate, 0) / latestRows.length;
    const sorted = [...latestRows].sort((a, b) => a.fmap_rate - b.fmap_rate);
    return { avgFmap: avg, minFmap: sorted[0].fmap_rate, maxFmap: sorted[sorted.length-1].fmap_rate, minState: sorted[0].state_code, maxState: sorted[sorted.length-1].state_code };
  }, [fmapData]);

  const toggleFmapState = useCallback((st: string) => {
    setSelectedFmapStates(prev => {
      const next = new Set(prev);
      if (next.has(st)) next.delete(st);
      else next.add(st);
      return next;
    });
  }, []);

  // -- Budget Pressure --
  const pressureChart = useMemo(() =>
    [...pressureData].sort((a, b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue)
      .slice(0, 30).map(r => ({
        name: r.state_code, pct: r.medicaid_pct_of_revenue, stateShare: r.medicaid_state_share, gdp: r.state_gdp_millions,
      })), [pressureData]);

  const pressureScatter = useMemo(() =>
    pressureData.filter(r => r.state_gdp_millions != null && r.medicaid_state_share != null).map(r => ({
      name: r.state_code, x: r.state_gdp_millions, y: r.medicaid_state_share, z: r.medicaid_pct_of_revenue,
    })), [pressureData]);

  // -- Vulnerability --
  const vulnChart = useMemo(() =>
    [...vulnData].sort((a, b) => b.budget_share_pct - a.budget_share_pct)
      .slice(0, 35).map(r => ({
        name: r.state_code, budgetShare: r.budget_share_pct, growth: r.spending_growth_pct, burden: r.state_burden_pct,
      })), [vulnData]);

  const vulnStats = useMemo(() => {
    if (!vulnData.length) return { most: "", least: "", avg: 0 };
    const sorted = [...vulnData].sort((a, b) => b.budget_share_pct - a.budget_share_pct);
    const avg = vulnData.reduce((s, r) => s + r.budget_share_pct, 0) / vulnData.length;
    return { most: sorted[0]?.state_code || "", least: sorted[sorted.length - 1]?.state_code || "", avg };
  }, [vulnData]);

  const LINE_COLORS = ["#2E6B4A", "#A4262C", "#B8860B", "#0A2540", "#6366F1", "#0891B2", "#D946EF", "#EA580C"];

  const tabs: { key: Tab; label: string }[] = [
    { key: "spending", label: "Spending vs Revenue" },
    { key: "fmap", label: "FMAP Impact" },
    { key: "pressure", label: "Budget Pressure" },
    { key: "vulnerability", label: "Vulnerability Ranking" },
  ];

  if (loading) return <div style={{ maxWidth:960,margin:"0 auto",padding:"20px 16px" }}><LoadingBar /></div>;

  if (loadError) return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"20px 16px" }}>
      <Card><div style={{ padding:"20px",textAlign:"center" }}>
        <div style={{ fontSize:16,fontWeight:600,marginBottom:8,color:NEG }}>Error Loading Data</div>
        <div style={{ fontSize:12,color:AL,lineHeight:1.7 }}>{loadError}</div>
      </div></Card>
    </div>
  );

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:12 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(184,134,11,0.1)",color:WARN,fontWeight:600 }}>RESEARCH</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>CMS-64 + FMAP + Census + BEA</span>
          </div>
          <button onClick={() => openIntelligence({ summary: `User is viewing Fiscal Cliff Analysis -- ${tab} tab. ${spendingData.length} spending records, ${fmapData.length} FMAP records.` })} style={{
            padding:"5px 12px",borderRadius:6,border:"none",background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
          }}>Ask Aradune</button>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(184,134,11,0.03)",borderLeft:`3px solid ${WARN}` }}>
        <span style={{ fontWeight:700,color:A }}>Fiscal Cliff Analysis.</span> Comparative state fiscal pressure analysis as enhanced federal matching expires and Medicaid spending grows against state revenue capacity.
      </div></Card>

      {/* Tab pills */}
      <div style={{ display:"flex",gap:6,marginTop:10,marginBottom:12,flexWrap:"wrap" }}>
        {tabs.map(t => <Pill key={t.key} active={tab===t.key} label={t.label} onClick={() => setTab(t.key)} />)}
      </div>

      {/* Spending vs Revenue */}
      {tab === "spending" && <>
        {latestFY > 0 && <div style={{ fontSize:10,color:AL,fontFamily:FM,marginBottom:8 }}>Showing FY{latestFY} data ({spendingLatest.length} states)</div>}

        {spendingChart.length > 0 ? <Card>
          <CH t="Medicaid as % of State Revenue" b={`FY${latestFY} -- Red = >20%`} r={`${spendingLatest.length} states`} />
          <ChartActions filename="spending-vs-revenue">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={spendingChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:d.pct>20?NEG:AL }}>Medicaid % of Revenue: {fmt(d.pct)}%</div>
                    <div style={{ color:AL }}>Spending: {fmtD(d.spending)}</div>
                    <div style={{ color:AL }}>Tax Revenue: {fmtD(d.revenue)}</div>
                  </div>;
                }} />
                <Bar dataKey="pct" name="% of Revenue" radius={[3,3,0,0]}>
                  {spendingChart.map((d, i) => <Cell key={i} fill={d.pct > 20 ? NEG : d.pct > 15 ? WARN : POS} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No spending vs revenue data available</div></Card>}

        {spendingLatest.length > 0 && <Card>
          <CH t="Spending & Revenue Detail" b={`FY${latestFY}`} r={`${spendingLatest.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Total Spending","Federal Share","State Share","Tax Revenue","Medicaid % Rev"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...spendingLatest].sort((a,b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.total_spending)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:POS }}>{fmtD(r.federal_share)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:NEG }}>{fmtD(r.state_share)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.total_tax_collections)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:r.medicaid_pct_of_revenue>20?NEG:r.medicaid_pct_of_revenue>15?WARN:POS }}>{fmt(r.medicaid_pct_of_revenue)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* FMAP Impact */}
      {tab === "fmap" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={POS}><Met l="Avg FMAP" v={`${fmt(fmapStats.avgFmap * 100)}%`} sub="Latest fiscal year" /></Card>
          <Card accent={NEG}><Met l="Lowest FMAP" v={`${fmt(fmapStats.minFmap * 100)}%`} sub={STATE_NAMES[fmapStats.minState] || fmapStats.minState} /></Card>
          <Card accent={POS}><Met l="Highest FMAP" v={`${fmt(fmapStats.maxFmap * 100)}%`} sub={STATE_NAMES[fmapStats.maxState] || fmapStats.maxState} /></Card>
        </div>

        {/* State selector */}
        <Card>
          <CH t="Select States" b="Click to toggle" r={`${selectedFmapStates.size} selected`} />
          <div style={{ padding:"4px 14px 10px",display:"flex",gap:4,flexWrap:"wrap" }}>
            {fmapStates.map(st => (
              <button key={st} onClick={() => toggleFmapState(st)} style={{
                padding:"2px 8px",borderRadius:4,fontSize:9,fontFamily:FM,border:`1px solid ${selectedFmapStates.has(st)?cB:BD}`,
                background:selectedFmapStates.has(st)?cB:WH,color:selectedFmapStates.has(st)?WH:AL,cursor:"pointer",
              }}>{st}</button>
            ))}
          </div>
        </Card>

        {fmapLineData.length > 0 ? <Card>
          <CH t="FMAP Rates Over Time" b="Selected states" r={`${fmapLineData.length} fiscal years`} />
          <ChartActions filename="fmap-trends">
            <ResponsiveContainer width="100%" height={320}>
              <LineChart data={fmapLineData} margin={{ top:10,right:20,bottom:4,left:10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${(v*100).toFixed(0)}%`} domain={["auto","auto"]} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{label}</div>
                    {payload.map((p: any, i: number) => (
                      <div key={i} style={{ color:p.color }}>{p.dataKey}: {p.value != null ? `${(p.value*100).toFixed(1)}%` : "\u2014"}</div>
                    ))}
                  </div>;
                }} />
                {Array.from(selectedFmapStates).map((st, i) => (
                  <Line key={st} type="monotone" dataKey={st} stroke={LINE_COLORS[i % LINE_COLORS.length]} strokeWidth={1.5} dot={{ r:2 }} connectNulls />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ display:"flex",gap:12,padding:"4px 14px 8px",fontSize:9,fontFamily:FM,flexWrap:"wrap" }}>
            {Array.from(selectedFmapStates).map((st, i) => (
              <span key={st}><span style={{ display:"inline-block",width:12,height:2,background:LINE_COLORS[i % LINE_COLORS.length],verticalAlign:"middle",marginRight:4 }}/>{st}</span>
            ))}
          </div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No FMAP data available. Select states above.</div></Card>}
      </>}

      {/* Budget Pressure */}
      {tab === "pressure" && <>
        {pressureChart.length > 0 ? <Card>
          <CH t="Medicaid as % of State Revenue" b="Budget pressure ranking" r={`${pressureData.length} states`} />
          <ChartActions filename="budget-pressure-bar">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={pressureChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:d.pct>20?NEG:AL }}>Medicaid % Rev: {fmt(d.pct)}%</div>
                    <div style={{ color:AL }}>State Share: {fmtD(d.stateShare)}</div>
                    <div style={{ color:AL }}>GDP: {fmtD(d.gdp * 1e6)}</div>
                  </div>;
                }} />
                <Bar dataKey="pct" name="% of Revenue" radius={[3,3,0,0]}>
                  {pressureChart.map((d, i) => <Cell key={i} fill={d.pct > 20 ? NEG : d.pct > 15 ? WARN : POS} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No budget pressure data available</div></Card>}

        {pressureScatter.length > 0 && <Card>
          <CH t="GDP vs Medicaid State Share" b="Bubble size = % of revenue" />
          <ChartActions filename="budget-scatter">
            <ResponsiveContainer width="100%" height={300}>
              <ScatterChart margin={{ top:10,right:20,bottom:10,left:10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="x" name="GDP ($M)" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v*1e6)} label={{ value:"State GDP",position:"bottom",fontSize:9,fill:AL }} />
                <YAxis dataKey="y" name="State Share" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} label={{ value:"Medicaid State Share",angle:-90,position:"insideLeft",fontSize:9,fill:AL }} />
                <ZAxis dataKey="z" range={[30,300]} />
                <Tooltip content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[d.name] || d.name}</div>
                    <div style={{ color:AL }}>GDP: {fmtD(d.x * 1e6)}</div>
                    <div style={{ color:AL }}>State Share: {fmtD(d.y)}</div>
                    <div style={{ color:AL }}>% of Revenue: {fmt(d.z)}%</div>
                  </div>;
                }} />
                <Scatter data={pressureScatter} fill={cB}>
                  {pressureScatter.map((d, i) => <Cell key={i} fill={d.z > 20 ? NEG : d.z > 15 ? WARN : cB} />)}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card>}

        {pressureData.length > 0 && <Card>
          <CH t="Budget Pressure Detail" b={`${pressureData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","State Share","Tax Revenue","% Revenue","GDP ($M)","FMAP"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...pressureData].sort((a,b) => b.medicaid_pct_of_revenue - a.medicaid_pct_of_revenue).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.medicaid_state_share)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.tax_revenue)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:r.medicaid_pct_of_revenue>20?NEG:r.medicaid_pct_of_revenue>15?WARN:POS }}>{fmt(r.medicaid_pct_of_revenue)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.state_gdp_millions)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{r.fmap_rate ? `${(r.fmap_rate * 100).toFixed(1)}%` : "\u2014"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Vulnerability Ranking */}
      {tab === "vulnerability" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Most Vulnerable" v={STATE_NAMES[vulnStats.most] || vulnStats.most || "\u2014"} cl={NEG} /></Card>
          <Card accent={POS}><Met l="Least Vulnerable" v={STATE_NAMES[vulnStats.least] || vulnStats.least || "\u2014"} cl={POS} /></Card>
          <Card accent={WARN}><Met l="National Avg Budget Share" v={`${fmt(vulnStats.avg)}%`} /></Card>
        </div>

        {vulnChart.length > 0 ? <Card>
          <CH t="Fiscal Vulnerability Ranking" b="By budget share %" r={`${vulnData.length} states`} />
          <ChartActions filename="vulnerability-ranking">
            <ResponsiveContainer width="100%" height={360}>
              <BarChart data={vulnChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:AL }}>Budget Share: {fmt(d.budgetShare)}%</div>
                    <div style={{ color:AL }}>Spending Growth: {fmt(d.growth)}%</div>
                    <div style={{ color:AL }}>State Burden: {fmt(d.burden)}%</div>
                  </div>;
                }} />
                <Bar dataKey="budgetShare" name="Budget Share %" radius={[3,3,0,0]}>
                  {vulnChart.map((d, i) => {
                    const maxPct = vulnChart[0]?.budgetShare || 1;
                    const pct = d.budgetShare / maxPct;
                    return <Cell key={i} fill={pct > 0.7 ? NEG : pct > 0.4 ? WARN : POS} />;
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No vulnerability data available</div></Card>}

        {vulnData.length > 0 && <Card>
          <CH t="Vulnerability Detail" b={`${vulnData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Budget Share %","Spending Growth %","State Burden %"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...vulnData].sort((a,b) => b.budget_share_pct - a.budget_share_pct).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:r.budget_share_pct>20?NEG:r.budget_share_pct>15?WARN:POS }}>{fmt(r.budget_share_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.spending_growth_pct>10?NEG:AL }}>{fmt(r.spending_growth_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.state_burden_pct)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing Fiscal Cliff Analysis -- ${tab} tab. ${spendingData.length} spending, ${fmapData.length} FMAP, ${pressureData.length} pressure, ${vulnData.length} vulnerability records.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Sources */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS-64 Expenditure (FY2018-2024) . MACPAC FMAP Historical . Census State Finances . BEA State GDP . Tax Foundation Collections
      </div>
    </div>
  );
}
