import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine, Cell, ScatterChart, Scatter, ZAxis } from "recharts";
import type { SafeTipProps, TooltipEntry, DecayHcpcs } from "../types";
import { API_BASE } from "../lib/api";
import { LoadingBar } from "../components/LoadingBar";
import ChartActions from "../components/ChartActions";

// ── Design System ───────────────────────────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const WARN = "#B8860B";
const SF = "#F5F7F5";
const BD = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const cO = "#C4590A";
const FM = "'SF Mono',Menlo,monospace";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming"};

// Common E&M and high-volume codes for comparison
const BENCHMARK_CODES = [
  { code:"99213", desc:"Office Visit (Est, Low)", category:"E&M" },
  { code:"99214", desc:"Office Visit (Est, Mod)", category:"E&M" },
  { code:"99215", desc:"Office Visit (Est, High)", category:"E&M" },
  { code:"99203", desc:"Office Visit (New, Low)", category:"E&M" },
  { code:"99204", desc:"Office Visit (New, Mod)", category:"E&M" },
  { code:"99385", desc:"Preventive Visit 18-39", category:"Preventive" },
  { code:"99386", desc:"Preventive Visit 40-64", category:"Preventive" },
  { code:"99393", desc:"Preventive Estab 12-17", category:"Preventive" },
  { code:"99394", desc:"Preventive Estab 18-39", category:"Preventive" },
  { code:"90834", desc:"Psychotherapy 45min", category:"Behavioral" },
  { code:"90837", desc:"Psychotherapy 60min", category:"Behavioral" },
  { code:"90832", desc:"Psychotherapy 30min", category:"Behavioral" },
  { code:"97110", desc:"Therapeutic Exercise", category:"Therapy" },
  { code:"97530", desc:"Therapeutic Activities", category:"Therapy" },
  { code:"97140", desc:"Manual Therapy", category:"Therapy" },
  { code:"92507", desc:"Speech Therapy", category:"Therapy" },
  { code:"90460", desc:"Immunization Admin", category:"Preventive" },
  { code:"36415", desc:"Venipuncture", category:"Lab" },
  { code:"99381", desc:"Preventive New <1yr", category:"Preventive" },
  { code:"99391", desc:"Preventive Estab <1yr", category:"Preventive" },
  { code:"99395", desc:"Preventive Estab 40-64", category:"Preventive" },
  { code:"90837", desc:"Psychotherapy 60min", category:"Behavioral" },
  { code:"11102", desc:"Tangential Biopsy", category:"Surgery" },
  { code:"17000", desc:"Destruct Premalignant", category:"Surgery" },
  { code:"10060", desc:"I&D Abscess Simple", category:"Surgery" },
];

const CATEGORIES = ["All", "E&M", "Preventive", "Behavioral", "Therapy", "Surgery", "Lab"];

// ── Shared Components ─────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",borderTop:accent?`3px solid ${accent}`:"none",border:`1px solid ${BD}`,marginBottom:10 }}>{children}</div>
);
const CH = ({ t, b, r }: { t: string; b?: string; r?: string }) => (
  <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"10px 14px 2px",flexWrap:"wrap",gap:4 }}>
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
const SafeTip = ({ active, payload, render }: SafeTipProps) => {
  if (!active||!payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;
  return <div style={{ background:"rgba(10,37,64,0.95)",color:WH,padding:"8px 12px",borderRadius:6,fontSize:11,lineHeight:1.6,maxWidth:300,boxShadow:"0 4px 16px rgba(0,0,0,.2)" }}>{render(d)}</div>;
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

// ── Chart data shapes (for SafeTip render callbacks) ────────────────────
interface DecayChartRow {
  code: string;
  desc: string;
  category: string;
  medicare: number | null;
  effectiveRate: number | null;
  pctMedicare: number | null;
  gap: number | null;
  rateSource: string | null;
  tmsis: number | null;
  tmsisPctMedicare: number | null;
  feeSchedule: number | null;
}

interface MultiStateRow {
  st: string;
  name: string;
  tmsis: number;
  medicare: number;
  pct: number;
  isHighlight: boolean;
}

// ── Main Component ──────────────────────────────────────────────────────
export default function RateDecay() {
  const [s1, setS1] = useState("FL");
  const [catFilter, setCat] = useState("All");
  const [hcpcsData, setHCPCS] = useState<DecayHcpcs[] | null>(null);
  const [medicareData, setMedicare] = useState<{ rates: Record<string, { r?: number; fr?: number; rvu?: number; w?: number; d?: string }> } | null>(null);
  const [medicaidRates, setMedicaidRates] = useState<Record<string, Record<string, [number, string, string]>> | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const tryApi = async (apiPath: string, fallback: string) => {
          if (API_BASE) { try { const r = await fetch(`${API_BASE}${apiPath}`); if (r.ok) return r.json(); } catch {} }
          return fetch(fallback).then(r=>r.ok?r.json():null).catch(()=>null);
        };
        const [hcpcs, medicare, mcdRates] = await Promise.all([
          tryApi("/api/bulk/hcpcs-rates", "/data/hcpcs.json"),
          tryApi("/api/bulk/medicare-rates", "/data/medicare_rates.json"),
          tryApi("/api/bulk/medicaid-rates", "/data/medicaid_rates.json"),
        ]);
        if (cancelled) return;
        if (hcpcs) setHCPCS(hcpcs);
        if (medicare) setMedicare(medicare);
        if (mcdRates) setMedicaidRates(mcdRates);
      } catch(e) { console.error(e); }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // Available states from T-MSIS data
  const stateList = useMemo(() => {
    return Object.keys(STATE_NAMES).sort((a,b) => STATE_NAMES[a].localeCompare(STATE_NAMES[b]));
  }, []);

  // Get Medicare rate for a code
  const getMedicareRate = useCallback((code: string): number | null => {
    if (!medicareData?.rates) return null;
    const r = medicareData.rates[code];
    if (!r) return null;
    return r.r || null;
  }, [medicareData]);

  // Get T-MSIS actual-paid rate for a code + state
  const getTmsisRate = useCallback((state: string, code: string): number | null => {
    if (!hcpcsData || !Array.isArray(hcpcsData)) return null;
    const h = hcpcsData.find((r: DecayHcpcs) => (r.code||r.c) === code);
    if (!h) return null;
    const ratesObj = h.rates || h.r;
    if (ratesObj && typeof ratesObj === 'object') return ratesObj[state] || null;
    return null;
  }, [hcpcsData]);

  // Get fee schedule rate for a code + state
  const getFeeScheduleRate = useCallback((state: string, code: string): number | null => {
    if (!medicaidRates) return null;
    const entry = medicaidRates[state]?.[code];
    return entry ? entry[0] : null;
  }, [medicaidRates]);

  // Build analysis data
  const analysis = useMemo(() => {
    const codes = catFilter === "All" ? BENCHMARK_CODES : BENCHMARK_CODES.filter(c => c.category === catFilter);
    // Deduplicate
    const seen = new Set();
    const unique = codes.filter(c => { if (seen.has(c.code)) return false; seen.add(c.code); return true; });

    return unique.map(bc => {
      const mcr = getMedicareRate(bc.code);
      const tmsis = getTmsisRate(s1, bc.code);
      const fs = getFeeScheduleRate(s1, bc.code);
      // Prefer fee schedule rate, fall back to T-MSIS
      const effectiveRate = (fs && fs > 0) ? fs : tmsis;

      const pctMedicare = (effectiveRate && mcr && mcr > 0) ? (effectiveRate / mcr * 100) : null;
      const tmsisPct = (tmsis && mcr && mcr > 0) ? (tmsis / mcr * 100) : null;
      const gap = (effectiveRate && mcr) ? effectiveRate - mcr : null;

      return {
        ...bc,
        medicare: mcr,
        feeSchedule: fs,
        tmsis,
        effectiveRate,
        pctMedicare,
        tmsisPctMedicare: tmsisPct,
        gap,
        rateSource: (fs && fs > 0) ? "Fee Schedule" : tmsis ? "T-MSIS Actual" : null,
      };
    }).filter(d => d.effectiveRate != null || d.medicare != null);
  }, [catFilter, s1, getMedicareRate, getTmsisRate, getFeeScheduleRate]);

  // Only rows with both rates
  const withBoth = useMemo(() => analysis.filter(d => d.pctMedicare != null), [analysis]);

  // Summary stats
  const stats = useMemo(() => {
    if (withBoth.length === 0) return null;
    const pcts = withBoth.map(d => d.pctMedicare!).sort((a,b) => a-b);
    const median = pcts[Math.floor(pcts.length / 2)];
    const mean = pcts.reduce((a,b)=>a+b,0) / pcts.length;
    const below50 = pcts.filter(p => p < 50).length;
    const below75 = pcts.filter(p => p < 75).length;
    const above100 = pcts.filter(p => p >= 100).length;
    const lowest = withBoth.reduce((a,b) => (a.pctMedicare! < b.pctMedicare! ? a : b));
    const highest = withBoth.reduce((a,b) => (a.pctMedicare! > b.pctMedicare! ? a : b));
    return { median, mean, below50, below75, above100, lowest, highest, n: withBoth.length };
  }, [withBoth]);

  // Multi-state comparison for a single code
  const [compareCode, setCompareCode] = useState("99213");
  const multiState = useMemo(() => {
    const mcr = getMedicareRate(compareCode);
    if (!mcr) return [];
    return stateList.map(st => {
      const tmsis = getTmsisRate(st, compareCode);
      if (!tmsis) return null;
      return {
        st,
        name: STATE_NAMES[st],
        tmsis,
        medicare: mcr,
        pct: (tmsis / mcr * 100),
        isHighlight: st === s1,
      };
    }).filter((d): d is NonNullable<typeof d> => d != null).sort((a,b) => a.pct - b.pct);
  }, [compareCode, stateList, getMedicareRate, getTmsisRate, s1]);

  if (loading) return <LoadingBar text="Loading rate data" detail="Medicare and Medicaid fee schedules" />;

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:10 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(14,98,69,0.1)",color:POS,fontWeight:600 }}>CY2026 Medicare PFS</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{analysis.length} benchmark codes</span>
          </div>
          <ExportBtn label="Export Analysis" onClick={() => {
            downloadCSV(`rate_decay_${s1}.csv`,
              ["Code","Description","Category","Medicare Rate","State Rate","Source","% of Medicare","Gap"],
              analysis.map(d=>[d.code,d.desc,d.category,d.medicare?.toFixed(2),d.effectiveRate?.toFixed(2),d.rateSource,d.pctMedicare?.toFixed(1),d.gap?.toFixed(2)])
            );
          }}/>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Rate Decay Tracker.</span> Measures how far Medicaid rates have drifted from the Medicare Physician Fee Schedule. Medicare updates rates annually using RBRVS methodology with practice cost adjustments. When Medicaid rates fall behind Medicare, it signals erosion in provider payment adequacy: the "decay" that accumulates when fee schedules aren't regularly updated.
      </div></Card>

      {/* Controls */}
      <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap",margin:"10px 0" }}>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>State</span>
          <select value={s1} onChange={e=>setS1(e.target.value)} style={{ background:SF,border:`1px solid ${BD}`,padding:"5px 10px",borderRadius:6,fontSize:11,color:A }}>
            {stateList.map(k=><option key={k} value={k}>{STATE_NAMES[k]}</option>)}
          </select>
        </div>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Category</span>
          <div style={{ display:"flex",gap:3,flexWrap:"wrap" }}>
            {CATEGORIES.map(c=><Pill key={c} on={catFilter===c} onClick={()=>setCat(c)}>{c}</Pill>)}
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      {stats && <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:10,marginBottom:10 }}>
        <Card accent={stats.median<75?NEG:stats.median<90?WARN:POS}>
          <Met l={`Median % of Medicare`} v={`${stats.median.toFixed(0)}%`} cl={stats.median<75?NEG:stats.median<90?WARN:POS} sub={`${stats.n} codes analyzed`}/>
        </Card>
        <Card>
          <Met l="Below 50% of Medicare" v={`${stats.below50}`} cl={stats.below50>0?NEG:POS} sub={`of ${stats.n} codes`}/>
        </Card>
        <Card>
          <Met l="Below 75% of Medicare" v={`${stats.below75}`} cl={stats.below75>stats.n/2?NEG:WARN} sub={`of ${stats.n} codes`}/>
        </Card>
        <Card>
          <Met l="Most Decayed" v={`${stats.lowest.code}`} cl={NEG} sub={`${stats.lowest.pctMedicare?.toFixed(0)}% of Medicare`}/>
        </Card>
        <Card>
          <Met l="Highest Relative" v={`${stats.highest.code}`} cl={POS} sub={`${stats.highest.pctMedicare?.toFixed(0)}% of Medicare`}/>
        </Card>
      </div>}

      {/* Decay Chart */}
      {withBoth.length > 0 && <Card>
        <CH t={`${STATE_NAMES[s1]} Rates as % of Medicare`} b={`${withBoth.length} codes · CY2026 Medicare PFS`} r="100% = Medicare parity"/>
        <div style={{ padding:"0 14px 8px" }}>
          <ChartActions filename="rate-decay">
          <ResponsiveContainer width="100%" height={Math.max(200, withBoth.length * 18)}>
            <BarChart data={[...withBoth].sort((a,b)=>(a.pctMedicare??0)-(b.pctMedicare??0))} layout="vertical" margin={{ left:90,right:16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
              <XAxis type="number" domain={[0, Math.max(120, ...withBoth.map(d=>(d.pctMedicare??0)+5))]} tick={{ fill:AL,fontSize:8,fontFamily:FM }} tickFormatter={(v: number)=>`${v}%`}/>
              <YAxis type="category" dataKey="code" tick={{ fill:A,fontSize:8,fontFamily:FM }} width={85} tickFormatter={(v: string) => {
                const d = withBoth.find(x=>x.code===v);
                return d ? `${v} ${d.desc?.substring(0,15)||""}` : v;
              }}/>
              <ReferenceLine x={100} stroke={A} strokeWidth={2} label={{ value:"Medicare",position:"top",style:{fontSize:8,fill:A,fontFamily:FM} }}/>
              <ReferenceLine x={75} stroke={WARN} strokeDasharray="4 4" label={{ value:"75%",position:"top",style:{fontSize:7,fill:WARN} }}/>
              <ReferenceLine x={50} stroke={NEG} strokeDasharray="4 4" label={{ value:"50%",position:"top",style:{fontSize:7,fill:NEG} }}/>
              <Tooltip content={<SafeTip render={(raw)=>{
                const d = raw as unknown as DecayChartRow;
                return (
                <div>
                  <div style={{ fontWeight:600 }}>{d.code} — {d.desc}</div>
                  <div>Medicare: <b>${d.medicare?.toFixed(2)}</b></div>
                  <div>{d.rateSource}: <b>${d.effectiveRate?.toFixed(2)}</b></div>
                  {d.feeSchedule && d.tmsis && d.feeSchedule !== d.tmsis && <div style={{ fontSize:9,opacity:0.8 }}>T-MSIS avg: ${d.tmsis?.toFixed(2)}</div>}
                  <div style={{ color:(d.pctMedicare??0)<75?"#ff9999":(d.pctMedicare??0)<100?"#ffcc99":"#99ff99" }}>
                    <b>{d.pctMedicare?.toFixed(1)}%</b> of Medicare ({(d.gap??0)>=0?"+":""}${d.gap?.toFixed(2)})
                  </div>
                </div>
                );
              }}/>}/>
              <Bar dataKey="pctMedicare" barSize={10} radius={[0,3,3,0]}>
                {[...withBoth].sort((a,b)=>(a.pctMedicare??0)-(b.pctMedicare??0)).map((d,i)=>(
                  <Cell key={i} fill={(d.pctMedicare??0)<50?NEG:(d.pctMedicare??0)<75?WARN:(d.pctMedicare??0)<100?cO:POS} opacity={0.7}/>
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          </ChartActions>
          <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>&lt;50%</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:WARN,verticalAlign:"middle",marginRight:3 }}/>50-75%</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:cO,verticalAlign:"middle",marginRight:3 }}/>75-100%</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>≥100% (at or above Medicare)</span>
          </div>
        </div>
      </Card>}

      {/* Code Detail Table */}
      {analysis.length > 0 && <Card>
        <CH t="Code-Level Detail" b={`${analysis.length} codes`}/>
        <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
          <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
            <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
              {["Code","Description","Category","Medicare","Medicaid Rate","Source","% of MCR","Gap"].map(h=>(
                <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {[...analysis].sort((a,b) => (a.pctMedicare||999) - (b.pctMedicare||999)).map(d => (
                <tr key={d.code} style={{ borderBottom:`1px solid ${SF}`,cursor:"pointer" }} onClick={()=>setCompareCode(d.code)}>
                  <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:cB }}>{d.code}</td>
                  <td style={{ padding:"4px",color:AL,maxWidth:140,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{d.desc}</td>
                  <td style={{ fontSize:9,color:AL }}>{d.category}</td>
                  <td style={{ fontFamily:FM }}>{d.medicare?`$${d.medicare.toFixed(2)}`:"—"}</td>
                  <td style={{ fontFamily:FM,fontWeight:600 }}>{d.effectiveRate?`$${d.effectiveRate.toFixed(2)}`:"—"}</td>
                  <td style={{ fontSize:8 }}>
                    {d.rateSource === "Fee Schedule"
                      ? <span style={{ padding:"1px 5px",borderRadius:8,background:"rgba(46,107,74,0.1)",color:POS,fontWeight:600 }}>FS</span>
                      : d.rateSource
                      ? <span style={{ padding:"1px 5px",borderRadius:8,background:SF,color:AL,fontWeight:600 }}>T-MSIS</span>
                      : "—"}
                  </td>
                  <td style={{ fontFamily:FM,fontWeight:700,color:d.pctMedicare==null?AL:d.pctMedicare<50?NEG:d.pctMedicare<75?WARN:d.pctMedicare<100?cO:POS }}>{d.pctMedicare!=null?`${d.pctMedicare.toFixed(1)}%`:"—"}</td>
                  <td style={{ fontFamily:FM,color:d.gap!=null?(d.gap<0?NEG:POS):AL }}>{d.gap!=null?`${d.gap>=0?"+":""}$${d.gap.toFixed(2)}`:"—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>}

      {/* Multi-State Comparison */}
      {multiState.length > 0 && <Card>
        <CH t={`${compareCode} Across States`} b={`${multiState.length} states with T-MSIS data`} r={`Medicare: $${getMedicareRate(compareCode)?.toFixed(2)||"?"}`}/>
        <div style={{ padding:"2px 14px 4px" }}>
          <span style={{ fontSize:9,color:AL }}>Click any code above to compare across states</span>
        </div>
        <div style={{ padding:"0 14px 8px" }}>
          <ChartActions filename="rate-decay-comparison">
          <ResponsiveContainer width="100%" height={Math.max(200, multiState.length * 12)}>
            <BarChart data={multiState} layout="vertical" margin={{ left:52,right:16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
              <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} tickFormatter={v=>`${v.toFixed(0)}%`}/>
              <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} width={28}/>
              <ReferenceLine x={100} stroke={A} strokeWidth={1.5}/>
              <Tooltip content={<SafeTip render={(raw)=>{
                const d = raw as unknown as MultiStateRow;
                return (
                <div>
                  <div style={{ fontWeight:600 }}>{d.name}</div>
                  <div>T-MSIS actual paid: <b>${d.tmsis.toFixed(2)}</b></div>
                  <div>Medicare: <b>${d.medicare.toFixed(2)}</b></div>
                  <div style={{ color:d.pct<75?"#ff9999":"#99ff99" }}><b>{d.pct.toFixed(1)}%</b> of Medicare</div>
                </div>
                );
              }}/>}/>
              <Bar dataKey="pct" barSize={7} radius={[0,3,3,0]}>
                {multiState.map((d,i)=><Cell key={i} fill={d.isHighlight?cO:d.pct<75?NEG:d.pct<100?WARN:POS} opacity={d.isHighlight?1:0.45}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          </ChartActions>
        </div>
      </Card>}

      {/* No data fallback */}
      {withBoth.length === 0 && <Card>
        <div style={{ padding:24,textAlign:"center" }}>
          <div style={{ fontSize:14,fontWeight:500,marginBottom:8,color:A }}>Limited Rate Data Available</div>
          <div style={{ fontSize:11,color:AL,lineHeight:1.7 }}>
            The Rate Decay Tracker compares state Medicaid rates against Medicare. Currently using T-MSIS actual-paid rates. As the all-state fee schedule database is built out, this tool will show fee schedule rates directly, providing a cleaner comparison than blended T-MSIS averages.
          </div>
        </div>
      </Card>}

      {/* About */}
      <Card><CH t="Data Sources & Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Medicare rates:</b> CY2026 Medicare Physician Fee Schedule, non-facility rates. These represent CMS's RBRVS-derived rates including work, practice expense, and malpractice RVUs adjusted by the conversion factor.<br/>
        <b>Medicaid rates:</b> State fee schedule rates where available, otherwise T-MSIS actual-paid averages. Fee schedule rates are preferred as they represent the state's intentional rate-setting; T-MSIS reflects what was actually paid including managed care encounters, which may differ.<br/>
        <b>% of Medicare:</b> (Medicaid rate ÷ Medicare rate) × 100. This is the standard benchmark used by MACPAC, KFF, and state rate-setting analyses. Values below 100% indicate Medicaid pays less than Medicare for the same service.<br/>
        <b>Benchmark codes:</b> High-volume E&M, preventive, behavioral health, and therapy codes selected to represent the services most commonly delivered in Medicaid. Not exhaustive. The full fee schedule comparison will be available as state data is integrated.<br/>
        <b>Rate decay:</b> When a state's rates fall behind Medicare over time, it creates compounding access problems. Medicare adjusts annually; if Medicaid doesn't match, the gap widens each year. A code at 60% of Medicare that stays frozen while Medicare increases 3% annually drops to ~52% in 5 years.
      </div></Card>

      {/* Cross-link to Compliance Report */}
      <Card>
        <div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6 }}>
          This tool shows how far Medicaid has fallen behind Medicare. For regulatory compliance analysis under 42 CFR §447.203, see the{" "}
          <a href="#/compliance" style={{ color:cB,fontWeight:600,textDecoration:"none" }}>Compliance Report</a>.
        </div>
      </Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Rate Decay Tracker v1.0 · CY2026 Medicare PFS + T-MSIS</div>
    </div>
  );
}
