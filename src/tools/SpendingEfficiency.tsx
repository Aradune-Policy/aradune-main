import React, { useState, useMemo, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ScatterChart, Scatter, ZAxis } from "recharts";
import type { SafeTipProps } from "../types";
import { API_BASE } from "../lib/api";
import { LoadingBar } from "../components/LoadingBar";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";
import { useIsMobile } from "../design";
import StateContextBar from "../components/StateContextBar";

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

// Reverse lookup: state name -> abbreviation
const STATE_ABBREVS: Record<string, string> = {};
for (const [abbr, name] of Object.entries(STATE_NAMES)) {
  STATE_ABBREVS[name] = abbr;
  STATE_ABBREVS[name.toLowerCase()] = abbr;
}
// Handle common alternate names
STATE_ABBREVS["District of Columbia"] = "DC";
STATE_ABBREVS["district of columbia"] = "DC";
STATE_ABBREVS["North Carolina"] = "NC";
STATE_ABBREVS["north carolina"] = "NC";
STATE_ABBREVS["South Carolina"] = "SC";
STATE_ABBREVS["south carolina"] = "SC";
STATE_ABBREVS["North Dakota"] = "ND";
STATE_ABBREVS["north dakota"] = "ND";
STATE_ABBREVS["South Dakota"] = "SD";
STATE_ABBREVS["south dakota"] = "SD";
STATE_ABBREVS["West Virginia"] = "WV";
STATE_ABBREVS["west virginia"] = "WV";
STATE_ABBREVS["New Hampshire"] = "NH";
STATE_ABBREVS["new hampshire"] = "NH";
STATE_ABBREVS["New Jersey"] = "NJ";
STATE_ABBREVS["new jersey"] = "NJ";
STATE_ABBREVS["New Mexico"] = "NM";
STATE_ABBREVS["new mexico"] = "NM";
STATE_ABBREVS["New York"] = "NY";
STATE_ABBREVS["new york"] = "NY";
STATE_ABBREVS["Rhode Island"] = "RI";
STATE_ABBREVS["rhode island"] = "RI";

// ── Data Shape Interfaces ─────────────────────────────────────────────
interface StateInfo {
  state_code: string;
  state_name: string;
  fmap: number | null;
  total_enrollment: number | null;
  pct_managed_care: number | null;
  mc_penetration_pct: number | null;
  region: string | null;
}

interface ExpenditureRow {
  state_code: string;
  fiscal_year: number;
  total_computable: number;
  federal_share: number;
  state_share: number;
}

interface PerEnrolleeRow {
  state_name: string;
  fiscal_year: number;
  total_all: number | null;
  total_full_benefit: number | null;
  child_all: number | null;
  new_adult_all: number | null;
  disabled_all: number | null;
  aged_all: number | null;
}

interface McShareRow {
  state: string;
  year: number;
  total_enrollees: number | null;
  any_mc_enrolled: number | null;
  pct_any_mc: number | null;
  comprehensive_mc_enrolled: number | null;
  pct_comprehensive_mc: number | null;
}

// ── Shared Components ─────────────────────────────────────────────────
const Card = ({ children, accent, x }: { children: React.ReactNode; accent?: string; x?: boolean }) => (
  <div style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",borderTop:accent?`3px solid ${accent}`:"none",border:x?"none":`1px solid ${BD}`,marginBottom:10 }}>{children}</div>
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
    {sub&&<div style={{ fontSize:9,color:AL }}>{sub}</div>}
  </div>
);
const Pill = ({ on, onClick, children }: { on: boolean; onClick: () => void; children: React.ReactNode }) => (
  <button aria-pressed={on} onClick={onClick} style={{ padding:"3px 9px",fontSize:10,fontWeight:on?700:400,color:on?WH:AL,background:on?cB:"transparent",border:`1px solid ${on?cB:BD}`,borderRadius:5,cursor:"pointer" }}>{children}</button>
);

const f$ = (v: number): string => {
  if (v==null||isNaN(v)||!isFinite(v)) return "$0";
  const abs=Math.abs(v),sign=v<0?"-":"";
  if(abs>=1e9)return `${sign}$${(abs/1e9).toFixed(1)}B`;
  if(abs>=1e6)return `${sign}$${(abs/1e6).toFixed(1)}M`;
  if(abs>=1e3)return `${sign}$${abs.toLocaleString(undefined,{maximumFractionDigits:0})}`;
  if(abs<10)return `${sign}$${abs.toFixed(2)}`;
  return `${sign}$${abs.toFixed(0)}`;
};
const fN = (v: number): string => {
  if(v==null||isNaN(v)||!isFinite(v)) return "0";
  if(v>=1e6)return `${(v/1e6).toFixed(1)}M`;
  if(v>=1e3)return `${(v/1e3).toFixed(0)}K`;
  return `${v}`;
};
const safe = (v: number | null | undefined, fb: number = 0): number => (v==null||isNaN(v))?fb:v;

const SafeTip = ({ active, payload, render }: SafeTipProps) => {
  if (!active||!payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;
  return <div style={{ background:"rgba(10,37,64,0.95)",color:WH,padding:"8px 12px",borderRadius:6,fontSize:11,lineHeight:1.6,maxWidth:280,boxShadow:"0 4px 16px rgba(0,0,0,.2)" }}>{render(d)}</div>;
};

function downloadCSV(name: string, headers: string[], rows: (string | number | null | undefined)[][]) {
  const esc = (v: string | number | null | undefined) => `"${String(v??"").replace(/"/g,'""')}"`;
  const csv = [headers.map(esc).join(","), ...rows.map((r: (string | number | null | undefined)[]) => r.map(esc).join(","))].join("\n");
  const a = document.createElement("a");
  a.href = "data:text/csv;charset=utf-8," + encodeURIComponent(csv);
  a.download = name; a.click();
}
const ExportBtn = ({ onClick, label }: { onClick: () => void; label?: string }) => (
  <button onClick={onClick} style={{ fontSize:9,color:AL,background:SF,border:`1px solid ${BD}`,borderRadius:5,padding:"3px 8px",cursor:"pointer",fontFamily:FM }}>{label||"Export CSV"}</button>
);

// ── Merged State Data ────────────────────────────────────────────────
interface MergedState {
  st: string;
  name: string;
  perEnrollee: number | null;
  perEnrolleeFullBenefit: number | null;
  childPerEnrollee: number | null;
  newAdultPerEnrollee: number | null;
  disabledPerEnrollee: number | null;
  agedPerEnrollee: number | null;
  totalComputable: number | null;
  federalShare: number | null;
  stateShare: number | null;
  fmap: number | null;
  enrollment: number | null;
  mcPct: number | null;
  region: string | null;
  fiscalYear: number | null;
}

// ── Main Component ──────────────────────────────────────────────────────
export default function SpendingEfficiency() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<"per-enrollee"|"total"|"efficiency">("per-enrollee");
  const [sortBy, setSortBy] = useState<"perEnrollee"|"totalComputable"|"name">("perEnrollee");
  const [sortDir, setSortDir] = useState<"asc"|"desc">("desc");
  const [highlightState, setHighlightState] = useState("FL");

  // Data state
  const [statesData, setStatesData] = useState<StateInfo[]>([]);
  const [expenditureData, setExpenditureData] = useState<ExpenditureRow[]>([]);
  const [perEnrolleeData, setPerEnrolleeData] = useState<PerEnrolleeRow[]>([]);
  const [mcShareData, setMcShareData] = useState<McShareRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  // Fetch all data
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [statesRes, expRes, peRes, mcRes] = await Promise.all([
          fetch(`${API_BASE}/api/states`).then(r => r.ok ? r.json() : []),
          fetch(`${API_BASE}/api/spending/by-state`).then(r => r.ok ? r.json() : { rows: [] }),
          fetch(`${API_BASE}/api/spending/per-enrollee`).then(r => r.ok ? r.json() : { rows: [] }),
          fetch(`${API_BASE}/api/managed-care/share`).then(r => r.ok ? r.json() : { rows: [] }),
        ]);
        if (cancelled) return;
        setStatesData(Array.isArray(statesRes) ? statesRes : []);
        setExpenditureData(expRes.rows || []);
        setPerEnrolleeData(peRes.rows || []);
        setMcShareData(mcRes.rows || []);
      } catch (e) {
        console.error(e);
        if (!cancelled) setLoadError("Failed to load spending data. Please refresh or try again later.");
      }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // Merge all datasets by state
  const merged = useMemo((): MergedState[] => {
    const map: Record<string, MergedState> = {};

    // Seed from dim_state
    for (const s of statesData) {
      const code = s.state_code;
      if (!code || !STATE_NAMES[code]) continue;
      map[code] = {
        st: code,
        name: STATE_NAMES[code] || s.state_name || code,
        perEnrollee: null,
        perEnrolleeFullBenefit: null,
        childPerEnrollee: null,
        newAdultPerEnrollee: null,
        disabledPerEnrollee: null,
        agedPerEnrollee: null,
        totalComputable: null,
        federalShare: null,
        stateShare: null,
        fmap: s.fmap != null ? Number(s.fmap) : null,
        enrollment: s.total_enrollment != null ? Number(s.total_enrollment) : null,
        mcPct: s.pct_managed_care != null ? Number(s.pct_managed_care) : (s.mc_penetration_pct != null ? Number(s.mc_penetration_pct) / 100 : null),
        region: s.region,
        fiscalYear: null,
      };
    }

    // Overlay expenditure
    for (const e of expenditureData) {
      const code = e.state_code;
      if (!code || !map[code]) continue;
      map[code].totalComputable = safe(e.total_computable);
      map[code].federalShare = safe(e.federal_share);
      map[code].stateShare = safe(e.state_share);
      map[code].fiscalYear = e.fiscal_year;
    }

    // Overlay per-enrollee from MACPAC
    for (const pe of perEnrolleeData) {
      const abbr = STATE_ABBREVS[pe.state_name] || STATE_ABBREVS[pe.state_name?.toLowerCase?.() ?? ""];
      if (!abbr || !map[abbr]) continue;
      map[abbr].perEnrollee = pe.total_all != null ? Number(pe.total_all) : null;
      map[abbr].perEnrolleeFullBenefit = pe.total_full_benefit != null ? Number(pe.total_full_benefit) : null;
      map[abbr].childPerEnrollee = pe.child_all != null ? Number(pe.child_all) : null;
      map[abbr].newAdultPerEnrollee = pe.new_adult_all != null ? Number(pe.new_adult_all) : null;
      map[abbr].disabledPerEnrollee = pe.disabled_all != null ? Number(pe.disabled_all) : null;
      map[abbr].agedPerEnrollee = pe.aged_all != null ? Number(pe.aged_all) : null;
      if (!map[abbr].fiscalYear) map[abbr].fiscalYear = pe.fiscal_year;
    }

    // Overlay MC penetration from fact_mc_share (latest year)
    const mcYears = mcShareData.map(m => m.year || 0);
    const latestMcYear = mcYears.length ? Math.max(...mcYears) : 0;
    for (const mc of mcShareData) {
      if (mc.year !== latestMcYear) continue;
      const abbr = STATE_ABBREVS[mc.state] || STATE_ABBREVS[mc.state?.toLowerCase?.() ?? ""];
      if (!abbr || !map[abbr]) continue;
      if (mc.pct_any_mc != null) {
        map[abbr].mcPct = Number(mc.pct_any_mc) / 100;
      }
    }

    return Object.values(map).filter(s => s.st !== "US");
  }, [statesData, expenditureData, perEnrolleeData, mcShareData]);

  // Compute national averages
  const national = useMemo(() => {
    const withPE = merged.filter(s => s.perEnrollee != null && s.perEnrollee > 0);
    const withTC = merged.filter(s => s.totalComputable != null && s.totalComputable > 0);
    const avgPE = withPE.length > 0 ? withPE.reduce((s, d) => s + safe(d.perEnrollee), 0) / withPE.length : 0;
    const medianPE = withPE.length > 0 ? withPE.map(d => safe(d.perEnrollee)).sort((a, b) => a - b)[Math.floor(withPE.length / 2)] : 0;
    const totalTC = withTC.reduce((s, d) => s + safe(d.totalComputable), 0);
    const totalFed = withTC.reduce((s, d) => s + safe(d.federalShare), 0);
    const totalEnroll = merged.reduce((s, d) => s + safe(d.enrollment), 0);
    return { avgPE, medianPE, totalTC, totalFed, totalEnroll, statesWithPE: withPE.length, statesWithTC: withTC.length };
  }, [merged]);

  // Sorted data for current tab
  const sorted = useMemo(() => {
    const arr = [...merged];
    arr.sort((a, b) => {
      let av: number, bv: number;
      if (sortBy === "name") return sortDir === "asc" ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name);
      if (sortBy === "perEnrollee") { av = safe(a.perEnrollee); bv = safe(b.perEnrollee); }
      else { av = safe(a.totalComputable); bv = safe(b.totalComputable); }
      return sortDir === "asc" ? av - bv : bv - av;
    });
    return arr;
  }, [merged, sortBy, sortDir]);

  // Chart data for per-enrollee tab
  const peChartData = useMemo(() => {
    return merged
      .filter(s => s.perEnrollee != null && s.perEnrollee > 0)
      .sort((a, b) => safe(b.perEnrollee) - safe(a.perEnrollee));
  }, [merged]);

  // Chart data for total expenditure tab
  const tcChartData = useMemo(() => {
    return merged
      .filter(s => s.totalComputable != null && s.totalComputable > 0)
      .sort((a, b) => safe(b.totalComputable) - safe(a.totalComputable));
  }, [merged]);

  // Scatter data for efficiency tab (per-enrollee vs MC penetration)
  const scatterData = useMemo(() => {
    return merged
      .filter(s => s.perEnrollee != null && s.perEnrollee > 0 && s.mcPct != null && s.mcPct > 0)
      .map(s => ({
        ...s,
        mcPctDisplay: safe(s.mcPct) * 100,
        enrollSize: safe(s.enrollment),
      }));
  }, [merged]);

  // Fiscal year label
  const fyLabel = useMemo(() => {
    const fy = merged.find(s => s.fiscalYear)?.fiscalYear;
    return fy ? `FY ${fy}` : "";
  }, [merged]);

  if (loading) return <LoadingBar text="Loading spending data" detail="CMS-64, MACPAC per-enrollee, managed care penetration" />;

  if (loadError) return (
    <div style={{ maxWidth:640,margin:"0 auto",padding:"40px 16px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>
      <Card><div style={{ padding:24,textAlign:"center" }}>
        <div style={{ fontSize:16,fontWeight:600,marginBottom:8,color:NEG }}>Error Loading Data</div>
        <div style={{ fontSize:12,color:AL,lineHeight:1.7 }}>{loadError}</div>
      </div></Card>
    </div>
  );

  if (!merged.length) return (
    <div style={{ maxWidth:640,margin:"0 auto",padding:"40px 16px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>
      <Card><div style={{ padding:24,textAlign:"center" }}>
        <div style={{ fontSize:16,fontWeight:600,marginBottom:8 }}>No spending data available</div>
        <div style={{ fontSize:12,color:AL,lineHeight:1.7 }}>
          Ensure the data lake has <code style={{ fontFamily:FM,background:SF,padding:"2px 6px",borderRadius:3 }}>fact_cms64_multiyear</code> and <code style={{ fontFamily:FM,background:SF,padding:"2px 6px",borderRadius:3 }}>fact_macpac_spending_per_enrollee</code> loaded.
        </div>
      </div></Card>
    </div>
  );

  const toggleSort = (col: "perEnrollee"|"totalComputable"|"name") => {
    if (sortBy === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortBy(col); setSortDir(col === "name" ? "asc" : "desc"); }
  };

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:12 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(14,98,69,0.1)",color:POS,fontWeight:600 }}>{fyLabel || "CMS-64 + MACPAC"}</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{merged.length} states</span>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <button onClick={() => openIntelligence({ summary: `User is viewing Spending Efficiency analysis — ${tab} tab` })} style={{
              padding: "5px 12px", borderRadius: 6, border: "none",
              background: cB, color: "#fff", fontSize: 11, cursor: "pointer", fontWeight: 600,
            }}>Ask Aradune</button>
            <ExportBtn label="Export Data" onClick={() => {
              downloadCSV("spending_efficiency.csv",
                ["State","Per Enrollee ($)","Total Computable ($M)","Federal Share ($M)","State Share ($M)","FMAP","Enrollment","MC Penetration %","Region"],
                sorted.map(s => [s.name, s.perEnrollee?.toFixed(0), s.totalComputable ? (s.totalComputable / 1e6).toFixed(1) : "", s.federalShare ? (s.federalShare / 1e6).toFixed(1) : "", s.stateShare ? (s.stateShare / 1e6).toFixed(1) : "", s.fmap?.toFixed(4), s.enrollment, s.mcPct ? (s.mcPct * 100).toFixed(1) : "", s.region])
              );
            }}/>
          </div>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Spending Efficiency.</span> Compares Medicaid spending across states using CMS-64 expenditure reports (total computable and federal/state split), MACPAC per-enrollee benefit spending by eligibility group, and managed care penetration. Identifies high-cost states and examines the relationship between spending and delivery model.
      </div></Card>

      {/* Tab Controls */}
      <div style={{ display:"flex",gap:4,margin:"10px 0",flexWrap:"wrap" }}>
        <Pill on={tab==="per-enrollee"} onClick={()=>setTab("per-enrollee")}>Per-Enrollee Spending</Pill>
        <Pill on={tab==="total"} onClick={()=>setTab("total")}>Total Expenditure</Pill>
        <Pill on={tab==="efficiency"} onClick={()=>setTab("efficiency")}>Efficiency Metrics</Pill>
      </div>

      {/* Highlight state selector */}
      <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap",margin:"8px 0" }}>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Highlight State</span>
          <select value={highlightState} onChange={e=>setHighlightState(e.currentTarget.value)} style={{ background:SF,border:`1px solid ${BD}`,padding:"5px 10px",borderRadius:6,fontSize:11,color:A }}>
            {merged.sort((a,b)=>a.name.localeCompare(b.name)).map(s=><option key={s.st} value={s.st}>{s.name}</option>)}
          </select>
        </div>
      </div>

      <StateContextBar stateCode={highlightState} mode="expanded" />

      {/* ═══════════════════ TAB 1: Per-Enrollee Spending ═══════════════════ */}
      {tab === "per-enrollee" && <>
        {/* Summary metrics */}
        <Card accent={cB}>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",padding:"8px 6px" }}>
            <Met l="National Average" v={f$(national.avgPE)} sub={`${national.statesWithPE} states reporting`}/>
            <Met l="National Median" v={f$(national.medianPE)}/>
            <Met l="Highest" v={peChartData[0] ? `${peChartData[0].name}: ${f$(safe(peChartData[0].perEnrollee))}` : "---"} cl={NEG}/>
            <Met l="Lowest" v={peChartData.length > 0 ? `${peChartData[peChartData.length-1].name}: ${f$(safe(peChartData[peChartData.length-1].perEnrollee))}` : "---"} cl={POS}/>
          </div>
        </Card>

        {/* Ranked bar chart */}
        {peChartData.length > 0 && <Card x>
          <CH t="Per-Enrollee Spending by State" b={`MACPAC ${fyLabel} — all enrollees`} r={`Nat'l avg: ${f$(national.avgPE)}`}/>
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="per-enrollee-spending">
            <ResponsiveContainer width="100%" height={Math.max(240, peChartData.length * 14)}>
              <BarChart data={peChartData} layout="vertical" margin={{ left:isMobile?48:56,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number) => f$(v)}/>
                <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} axisLine={false} tickLine={false} width={isMobile?36:44}/>
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Per enrollee: <b>{f$(safe(d.perEnrollee as number | null))}</b></div>
                    <div>Full benefit: <b>{f$(safe(d.perEnrolleeFullBenefit as number | null))}</b></div>
                    {(d.childPerEnrollee as number) > 0 && <div style={{ fontSize:10 }}>Child: {f$(d.childPerEnrollee as number)}</div>}
                    {(d.disabledPerEnrollee as number) > 0 && <div style={{ fontSize:10 }}>Disabled: {f$(d.disabledPerEnrollee as number)}</div>}
                    {(d.agedPerEnrollee as number) > 0 && <div style={{ fontSize:10 }}>Aged: {f$(d.agedPerEnrollee as number)}</div>}
                    <div style={{ fontSize:10 }}>Enrollment: {fN(safe(d.enrollment as number | null))}</div>
                    <div style={{ fontSize:10 }}>FMAP: {d.fmap ? `${(safe(d.fmap as number) * 100).toFixed(1)}%` : "---"}</div>
                  </div>
                )}/>}/>
                <Bar dataKey="perEnrollee" barSize={8} radius={[0,3,3,0]}>
                  {peChartData.map((d, i) => <Cell key={i} fill={d.st===highlightState?"#C4590A":safe(d.perEnrollee)>national.avgPE?NEG:POS} opacity={d.st===highlightState?1:0.55}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:"#C4590A",verticalAlign:"middle",marginRight:3 }}/>{STATE_NAMES[highlightState]||highlightState}</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>Above national average</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>At or below average</span>
            </div>
          </div>
        </Card>}

        {/* Eligibility breakdown for highlighted state */}
        {(() => {
          const hs = merged.find(s => s.st === highlightState);
          if (!hs || !hs.perEnrollee) return null;
          const groups = [
            { label: "All Enrollees", val: hs.perEnrollee },
            { label: "Full Benefit", val: hs.perEnrolleeFullBenefit },
            { label: "Children", val: hs.childPerEnrollee },
            { label: "New Adults", val: hs.newAdultPerEnrollee },
            { label: "Disabled", val: hs.disabledPerEnrollee },
            { label: "Aged", val: hs.agedPerEnrollee },
          ].filter(g => g.val != null && g.val > 0);
          return <Card accent={cB}>
            <CH t={`${STATE_NAMES[highlightState]} Per-Enrollee Breakdown`} b="MACPAC benefit spending by eligibility group"/>
            <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"4px 6px 10px" }}>
              {groups.map(g => <Met key={g.label} l={g.label} v={f$(safe(g.val))}/>)}
            </div>
          </Card>;
        })()}

        {/* Data table */}
        <Card x>
          <CH t="Per-Enrollee Detail" b={`${peChartData.length} states with data`}/>
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {[
                  { label:"State", key:"name" as const },
                  { label:"Per Enrollee", key:"perEnrollee" as const },
                  { label:"Full Benefit", key:"perEnrollee" as const },
                  { label:"Child", key:"perEnrollee" as const },
                  { label:"Disabled", key:"perEnrollee" as const },
                  { label:"Aged", key:"perEnrollee" as const },
                  { label:"Enrollment", key:"perEnrollee" as const },
                ].map((h, i) => (
                  <th key={i} onClick={() => i <= 1 ? toggleSort(i === 0 ? "name" : "perEnrollee") : undefined}
                    style={{ textAlign:i===0?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM,cursor:i<=1?"pointer":"default" }}>
                    {h.label}{sortBy===(i===0?"name":"perEnrollee")&&i<=1?(sortDir==="asc"?" \u2191":" \u2193"):""}
                  </th>
                ))}
              </tr></thead>
              <tbody>
                {sorted.filter(s => s.perEnrollee != null && s.perEnrollee > 0).map(s => (
                  <tr key={s.st} style={{ borderBottom:`1px solid ${SF}`,background:s.st===highlightState?"rgba(46,107,74,0.04)":"transparent" }}>
                    <td style={{ padding:"4px",fontWeight:s.st===highlightState?700:400,color:s.st===highlightState?cB:A }}>{s.name}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:safe(s.perEnrollee)>national.avgPE?NEG:POS }}>{f$(safe(s.perEnrollee))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.perEnrolleeFullBenefit ? f$(s.perEnrolleeFullBenefit) : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.childPerEnrollee ? f$(s.childPerEnrollee) : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.disabledPerEnrollee ? f$(s.disabledPerEnrollee) : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.agedPerEnrollee ? f$(s.agedPerEnrollee) : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.enrollment ? fN(s.enrollment) : "---"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </>}

      {/* ═══════════════════ TAB 2: Total Expenditure ═══════════════════ */}
      {tab === "total" && <>
        {/* Summary metrics */}
        <Card accent={cB}>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",padding:"8px 6px" }}>
            <Met l="Total Computable" v={f$(national.totalTC)} sub={`${national.statesWithTC} states · ${fyLabel}`}/>
            <Met l="Federal Share" v={f$(national.totalFed)} sub={`${national.totalTC > 0 ? ((national.totalFed / national.totalTC) * 100).toFixed(1) : 0}% of total`}/>
            <Met l="State Share" v={f$(national.totalTC - national.totalFed)}/>
            <Met l="Total Enrollment" v={fN(national.totalEnroll)}/>
          </div>
        </Card>

        {/* Stacked bar chart */}
        {tcChartData.length > 0 && <Card x>
          <CH t="CMS-64 Total Computable by State" b={`Federal + State Share · ${fyLabel}`} r={`${tcChartData.length} states`}/>
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="total-expenditure">
            <ResponsiveContainer width="100%" height={Math.max(280, tcChartData.length * 14)}>
              <BarChart data={tcChartData} layout="vertical" margin={{ left:isMobile?48:56,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number) => f$(v)}/>
                <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} axisLine={false} tickLine={false} width={isMobile?36:44}/>
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Total computable: <b>{f$(safe(d.totalComputable as number | null))}</b></div>
                    <div>Federal share: <b>{f$(safe(d.federalShare as number | null))}</b></div>
                    <div>State share: <b>{f$(safe(d.stateShare as number | null))}</b></div>
                    <div style={{ fontSize:10 }}>FMAP: {d.fmap ? `${(safe(d.fmap as number) * 100).toFixed(1)}%` : "---"}</div>
                    <div style={{ fontSize:10 }}>Enrollment: {fN(safe(d.enrollment as number | null))}</div>
                  </div>
                )}/>}/>
                <Bar dataKey="federalShare" stackId="a" barSize={8} fill={cB} name="Federal Share" radius={[0,0,0,0]}>
                  {tcChartData.map((d, i) => <Cell key={i} fill={d.st===highlightState?"#1a5c3a":cB} opacity={d.st===highlightState?1:0.5}/>)}
                </Bar>
                <Bar dataKey="stateShare" stackId="a" barSize={8} fill={AL} name="State Share" radius={[0,3,3,0]}>
                  {tcChartData.map((d, i) => <Cell key={i} fill={d.st===highlightState?"#C4590A":AL} opacity={d.st===highlightState?1:0.35}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:cB,verticalAlign:"middle",marginRight:3 }}/>Federal share</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:AL,verticalAlign:"middle",marginRight:3 }}/>State share</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:"#C4590A",verticalAlign:"middle",marginRight:3 }}/>{STATE_NAMES[highlightState]||highlightState}</span>
            </div>
          </div>
        </Card>}

        {/* FMAP impact card for highlighted state */}
        {(() => {
          const hs = merged.find(s => s.st === highlightState);
          if (!hs || !hs.totalComputable) return null;
          return <Card accent={cB}>
            <CH t={`${STATE_NAMES[highlightState]} Federal-State Split`} b={fyLabel}/>
            <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",padding:"4px 6px 10px" }}>
              <Met l="Total Computable" v={f$(safe(hs.totalComputable))}/>
              <Met l="Federal Share" v={f$(safe(hs.federalShare))} cl={POS}/>
              <Met l="State Share" v={f$(safe(hs.stateShare))} cl={NEG}/>
              <Met l="FMAP" v={hs.fmap ? `${(hs.fmap * 100).toFixed(2)}%` : "---"}/>
              <Met l="Enrollment" v={fN(safe(hs.enrollment))}/>
              <Met l="Implied Per Enrollee" v={hs.enrollment && hs.enrollment > 0 ? f$(safe(hs.totalComputable) / hs.enrollment) : "---"} sub="Total computable / enrollment"/>
            </div>
          </Card>;
        })()}

        {/* Data table */}
        <Card x>
          <CH t="Expenditure Detail" b={`${tcChartData.length} states with CMS-64 data`}/>
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Total Computable","Federal Share","State Share","FMAP","Enrollment","Per Enrollee"].map((h, i) => (
                  <th key={h} onClick={() => i === 0 ? toggleSort("name") : i === 1 ? toggleSort("totalComputable") : undefined}
                    style={{ textAlign:i===0?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM,cursor:i<=1?"pointer":"default" }}>
                    {h}{sortBy===(i===0?"name":"totalComputable")&&i<=1?(sortDir==="asc"?" \u2191":" \u2193"):""}
                  </th>
                ))}
              </tr></thead>
              <tbody>
                {sorted.filter(s => s.totalComputable != null && s.totalComputable > 0).map(s => (
                  <tr key={s.st} style={{ borderBottom:`1px solid ${SF}`,background:s.st===highlightState?"rgba(46,107,74,0.04)":"transparent" }}>
                    <td style={{ padding:"4px",fontWeight:s.st===highlightState?700:400,color:s.st===highlightState?cB:A }}>{s.name}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600 }}>{f$(safe(s.totalComputable))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:POS }}>{f$(safe(s.federalShare))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:NEG }}>{f$(safe(s.stateShare))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.fmap ? `${(s.fmap * 100).toFixed(2)}%` : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.enrollment ? fN(s.enrollment) : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.enrollment && s.enrollment > 0 && s.totalComputable ? f$(s.totalComputable / s.enrollment) : "---"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </>}

      {/* ═══════════════════ TAB 3: Efficiency Metrics ═══════════════════ */}
      {tab === "efficiency" && <>
        {/* Summary */}
        <Card accent={cB}>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",padding:"8px 6px" }}>
            <Met l="States Plotted" v={`${scatterData.length}`} sub="With both per-enrollee + MC data"/>
            <Met l="Avg MC Penetration" v={`${scatterData.length > 0 ? (scatterData.reduce((s, d) => s + d.mcPctDisplay, 0) / scatterData.length).toFixed(1) : 0}%`}/>
            <Met l="Avg Per Enrollee" v={f$(scatterData.length > 0 ? scatterData.reduce((s, d) => s + safe(d.perEnrollee), 0) / scatterData.length : 0)}/>
            <Met l="Correlation" v={(() => {
              if (scatterData.length < 3) return "---";
              const xm = scatterData.reduce((s, d) => s + d.mcPctDisplay, 0) / scatterData.length;
              const ym = scatterData.reduce((s, d) => s + safe(d.perEnrollee), 0) / scatterData.length;
              let num = 0, dx2 = 0, dy2 = 0;
              for (const d of scatterData) {
                const dx = d.mcPctDisplay - xm;
                const dy = safe(d.perEnrollee) - ym;
                num += dx * dy; dx2 += dx * dx; dy2 += dy * dy;
              }
              const r = dx2 > 0 && dy2 > 0 ? num / Math.sqrt(dx2 * dy2) : 0;
              return r.toFixed(3);
            })()} sub="MC penetration vs spending"/>
          </div>
        </Card>

        {/* Scatter chart */}
        {scatterData.length > 0 && <Card x>
          <CH t="Per-Enrollee Spending vs. Managed Care Penetration" b={`${scatterData.length} states · ${fyLabel}`} r="Bubble size = enrollment"/>
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="spending-vs-mc">
            <ResponsiveContainer width="100%" height={isMobile ? 300 : 420}>
              <ScatterChart margin={{ top:10,right:20,bottom:30,left:isMobile?10:20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD}/>
                <XAxis type="number" dataKey="mcPctDisplay" name="MC Penetration"
                  tick={{ fill:AL,fontSize:9,fontFamily:FM }} axisLine={{ stroke:BD }}
                  tickLine={false} label={{ value:"Managed Care Penetration (%)",position:"bottom",offset:14,fill:AL,fontSize:10 }}/>
                <YAxis type="number" dataKey="perEnrollee" name="Per Enrollee Spending"
                  tick={{ fill:AL,fontSize:9,fontFamily:FM }} axisLine={{ stroke:BD }}
                  tickLine={false} tickFormatter={(v: number) => f$(v)}
                  label={{ value:"Per Enrollee ($)",angle:-90,position:"insideLeft",offset:isMobile?-5:0,fill:AL,fontSize:10 }}/>
                <ZAxis type="number" dataKey="enrollSize" range={[40, 800]} name="Enrollment"/>
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Per enrollee: <b>{f$(safe(d.perEnrollee as number | null))}</b></div>
                    <div>MC penetration: <b>{safe(d.mcPctDisplay as number | null).toFixed(1)}%</b></div>
                    <div style={{ fontSize:10 }}>Enrollment: {fN(safe(d.enrollment as number | null))}</div>
                    <div style={{ fontSize:10 }}>FMAP: {d.fmap ? `${(safe(d.fmap as number) * 100).toFixed(1)}%` : "---"}</div>
                    <div style={{ fontSize:10 }}>Region: {String(d.region ?? "---")}</div>
                  </div>
                )}/>}/>
                <Scatter data={scatterData} fill={cB}>
                  {scatterData.map((d, i) => (
                    <Cell key={i}
                      fill={d.st===highlightState?"#C4590A":safe(d.perEnrollee)>national.avgPE?NEG:POS}
                      opacity={d.st===highlightState?1:0.6}
                      stroke={d.st===highlightState?"#C4590A":"none"}
                      strokeWidth={d.st===highlightState?2:0}
                    />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:"#C4590A",verticalAlign:"middle",marginRight:3 }}/>{STATE_NAMES[highlightState]||highlightState}</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>Above avg spending</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>Below avg spending</span>
              <span>Bubble size = enrollment</span>
            </div>
          </div>
        </Card>}

        {/* Quadrant analysis */}
        {scatterData.length > 0 && <Card x>
          <CH t="Quadrant Analysis" b="Spending vs. managed care penetration" r={`Avg MC: ${(scatterData.reduce((s,d)=>s+d.mcPctDisplay,0)/scatterData.length).toFixed(0)}% · Avg PE: ${f$(national.avgPE)}`}/>
          <div style={{ padding:"4px 14px 10px" }}>
            {(() => {
              const avgMC = scatterData.reduce((s, d) => s + d.mcPctDisplay, 0) / scatterData.length;
              const quadrants = [
                { label: "High MC, Low Spend", desc: "Efficient managed care", color: POS, states: scatterData.filter(d => d.mcPctDisplay >= avgMC && safe(d.perEnrollee) <= national.avgPE) },
                { label: "High MC, High Spend", desc: "High cost despite MC", color: WARN, states: scatterData.filter(d => d.mcPctDisplay >= avgMC && safe(d.perEnrollee) > national.avgPE) },
                { label: "Low MC, Low Spend", desc: "Low cost, FFS-heavy", color: AL, states: scatterData.filter(d => d.mcPctDisplay < avgMC && safe(d.perEnrollee) <= national.avgPE) },
                { label: "Low MC, High Spend", desc: "High cost, FFS-heavy", color: NEG, states: scatterData.filter(d => d.mcPctDisplay < avgMC && safe(d.perEnrollee) > national.avgPE) },
              ];
              return <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"1fr 1fr",gap:8 }}>
                {quadrants.map(q => (
                  <div key={q.label} style={{ padding:"8px 10px",background:SF,borderRadius:6,borderLeft:`3px solid ${q.color}` }}>
                    <div style={{ fontSize:10,fontWeight:700,color:q.color }}>{q.label}</div>
                    <div style={{ fontSize:9,color:AL,marginBottom:4 }}>{q.desc} ({q.states.length} states)</div>
                    <div style={{ fontSize:9,color:A,lineHeight:1.6 }}>
                      {q.states.map(s => s.st).join(", ") || "None"}
                    </div>
                  </div>
                ))}
              </div>;
            })()}
          </div>
        </Card>}

        {/* Efficiency table */}
        <Card x>
          <CH t="Efficiency Detail" b={`${scatterData.length} states`}/>
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Per Enrollee","MC Penetration","FMAP","Enrollment","Total Computable","Region"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {scatterData.sort((a,b) => safe(a.perEnrollee) - safe(b.perEnrollee)).map(s => (
                  <tr key={s.st} style={{ borderBottom:`1px solid ${SF}`,background:s.st===highlightState?"rgba(46,107,74,0.04)":"transparent" }}>
                    <td style={{ padding:"4px",fontWeight:s.st===highlightState?700:400,color:s.st===highlightState?cB:A }}>{s.name}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:safe(s.perEnrollee)>national.avgPE?NEG:POS }}>{f$(safe(s.perEnrollee))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{s.mcPctDisplay.toFixed(1)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.fmap ? `${(s.fmap * 100).toFixed(1)}%` : "---"}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{fN(safe(s.enrollment))}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{s.totalComputable ? f$(s.totalComputable) : "---"}</td>
                    <td style={{ padding:"4px",color:AL }}>{s.region || "---"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </>}

      {/* About */}
      <Card><CH t="Data Sources & Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Per-Enrollee Spending:</b> MACPAC Exhibit 22 -- Medicaid benefit spending per full-year-equivalent enrollee by state and eligibility group (FY 2023). Includes all Medicaid benefit spending (federal + state share). Eligibility groups: children, new adults (expansion), disabled, aged.<br/>
        <b>Total Expenditure:</b> CMS-64 quarterly expenditure reports aggregated to fiscal year. Total computable = federal share + state share. Includes all Medicaid spending categories (MAP, ADM, etc.).<br/>
        <b>Federal Match:</b> Federal Medical Assistance Percentage (FMAP) determines the federal share of Medicaid costs. Standard FMAP ranges from 50% to ~77%. Expansion populations receive enhanced FMAP (90%).<br/>
        <b>Managed Care Penetration:</b> KFF/MACPAC share of Medicaid enrollees in any managed care arrangement by state. Higher MC penetration does not guarantee lower per-enrollee spending -- acuity mix, benefit generosity, and supplemental payments all affect total cost.<br/>
        <b>Limitations:</b> Per-enrollee spending uses full-year equivalents, not raw headcounts -- a person enrolled 6 months counts as 0.5 FYE. CMS-64 and MACPAC data may reflect different fiscal years. Supplemental payments (DSH, UPL, directed payments) are included in CMS-64 but not always in per-enrollee calculations. Cross-state comparisons should account for differences in benefit packages, acuity, and cost of living.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Spending Efficiency v1.0 -- CMS-64 + MACPAC + KFF</div>
    </div>
  );
}
