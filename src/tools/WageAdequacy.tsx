import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ReferenceLine, ScatterChart, Scatter, ZAxis } from "recharts";
import type { SafeTipProps, TooltipEntry, WageCategory } from "../types";

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
const cO = "#C4590A";
const FM = "'SF Mono',Menlo,monospace";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// State minimum wages (approximate as of 2024; may need periodic updates)
const MIN_WAGE: Record<string, number> = {AL:7.25,AK:11.73,AZ:14.35,AR:11.00,CA:16.00,CO:14.42,CT:15.69,DE:13.25,DC:17.50,FL:13.00,GA:7.25,HI:14.00,ID:7.25,IL:14.00,IN:7.25,IA:7.25,KS:7.25,KY:7.25,LA:7.25,ME:14.15,MD:15.00,MA:15.00,MI:10.33,MN:10.85,MS:7.25,MO:12.30,MT:10.30,NE:12.00,NV:12.00,NH:7.25,NJ:15.13,NM:12.00,NY:15.00,NC:7.25,ND:7.25,OH:10.45,OK:7.25,OR:14.70,PA:7.25,RI:14.00,SC:7.25,SD:11.20,TN:7.25,TX:7.25,UT:7.25,VT:13.67,VA:12.00,WA:16.28,WV:8.75,WI:7.25,WY:7.25,PR:10.50};
const FED_MIN = 7.25;

// ── Data Shape Interfaces ─────────────────────────────────────────────
interface WageDetail {
  h_median?: number;
  h_p10?: number;
  h_p25?: number;
  h_p75?: number;
  h_p90?: number;
  emp?: number;
  title?: string;
  [key: string]: unknown;
}

interface BlsData {
  states: Record<string, Record<string, WageDetail>>;
  national?: Record<string, WageDetail>;
}

interface CrosswalkCode {
  hcpcs: string;
  desc?: string;
  unit?: string;
  units_per_hour?: number;
  [key: string]: unknown;
}

interface CrosswalkCategory {
  id: string;
  name: string;
  soc: string;
  codes: CrosswalkCode[];
  overhead_range?: [number, number];
  overhead_note?: string;
  [key: string]: unknown;
}

interface CrosswalkData {
  categories: CrosswalkCategory[];
}

interface HcpcsEntry {
  code?: string;
  c?: string;
  rates?: Record<string, number>;
  r?: Record<string, number>;
  [key: string]: unknown;
}

interface QualMeasureMeta {
  name: string;
  domain: string;
  median?: number;
  type?: string;
  [key: string]: unknown;
}

interface QualDataShape {
  rates?: Record<string, Record<string, number>>;
  measures?: Record<string, QualMeasureMeta>;
}

interface CodeAnalysisEntry {
  hcpcs: string;
  desc?: string;
  unit?: string;
  units_per_hour?: number;
  tmsisRate: number | null;
  impliedHourly: number | null;
  gapVsBls: number | null;
  gapVsMin: number | null;
  [key: string]: unknown;
}

interface AllStateEntry {
  st: string;
  name: string;
  blsMedian: number | null;
  impliedHourly: number | null;
  tmsisRate: number | null;
  gap: number | null;
  gapPct: number | null;
  minWage: number;
  belowMin: boolean;
  emp: number;
}

interface QualityLinkEntry {
  id: string;
  name: string;
  domain: string;
  stateRate: number | undefined;
  median: number | undefined;
  gapVsMedian: number | null;
  direction: string | undefined;
}

// ── Shared Components ─────────────────────────────────────────────────
const Card = ({ children, accent, x }: { children: React.ReactNode; accent?: string; x?: boolean }) => (
  <div style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",borderTop:accent?`3px solid ${accent}`:"none",border:x?"none":`1px solid ${BD}` }}>{children}</div>
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

// ── Main Component ──────────────────────────────────────────────────────
export default function WageAdequacy() {
  const [s1, setS1] = useState("FL");
  const [cat, setCat] = useState("hcbs");
  const [overhead, setOverhead] = useState(35);
  const [blsData, setBLS] = useState<BlsData | null>(null);
  const [crosswalk, setCW] = useState<CrosswalkData | null>(null);
  const [hcpcsData, setHCPCS] = useState<HcpcsEntry[] | null>(null);
  const [statesData, setStates] = useState<Record<string, unknown> | null>(null);
  const [qualData, setQual] = useState<QualDataShape | null>(null);
  const [loading, setLoading] = useState(true);

  // Load data
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [bls, cw, hcpcs, states, qual] = await Promise.all([
          fetch("/data/bls_wages.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/soc_hcpcs_crosswalk.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/hcpcs.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/states.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/quality_measures.json").then(r=>r.ok?r.json():null).catch(()=>null),
        ]);
        if (cancelled) return;
        if (bls) setBLS(bls as BlsData);
        if (cw) setCW(cw as CrosswalkData);
        if (hcpcs) setHCPCS(hcpcs as HcpcsEntry[]);
        if (states) setStates(states as Record<string, unknown>);
        if (qual) setQual(qual as QualDataShape);
      } catch(e) { console.error(e); }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  const SL = useMemo(() => {
    if (!blsData?.states) return [];
    return Object.keys(blsData.states).filter((k: string)=>k!=="US").sort((a: string, b: string)=>(STATE_NAMES[a]||a).localeCompare(STATE_NAMES[b]||b));
  }, [blsData]);

  const curCat = useMemo((): CrosswalkCategory | null => {
    if (!crosswalk?.categories) return null;
    return crosswalk.categories.find((c: CrosswalkCategory) => c.id === cat) || crosswalk.categories[0] || null;
  }, [crosswalk, cat]);

  // Get T-MSIS rate for a code in a state
  const getTmsisRate = useCallback((state: string, code: string): number | null => {
    if (!hcpcsData || !Array.isArray(hcpcsData)) return null;
    const h = hcpcsData.find((r: HcpcsEntry) => (r.code||r.c) === code);
    if (!h) return null;
    const ratesObj = h.rates || h.r;
    if (ratesObj && typeof ratesObj === 'object') return (ratesObj as Record<string, number>)[state] || null;
    return null;
  }, [hcpcsData]);

  // Get T-MSIS rate (alias for getTmsisRate)
  const getTmsisRateAlt = getTmsisRate;

  // Compute analysis for current category + state
  const analysis = useMemo(() => {
    if (!blsData || !curCat) return null;

    const stateWages = blsData.states[s1];
    const natlWages = blsData.national;
    if (!stateWages) return null;

    const socCode = curCat.soc;
    const wage = stateWages[socCode];
    const natlWage = natlWages?.[socCode];
    if (!wage) return null;

    const minWage = MIN_WAGE[s1] || FED_MIN;

    // Get T-MSIS rates for each code in this category
    const codeAnalysis: CodeAnalysisEntry[] = curCat.codes.map((code: CrosswalkCode) => {
      const tmsisRate = getTmsisRateAlt(s1, code.hcpcs);
      let impliedHourly: number | null = null;
      if (tmsisRate && code.units_per_hour) {
        const hourlyRevenue = tmsisRate * code.units_per_hour;
        impliedHourly = hourlyRevenue * (1 - overhead / 100);
      }
      return {
        ...code,
        tmsisRate,
        impliedHourly,
        gapVsBls: impliedHourly && wage.h_median ? impliedHourly - wage.h_median : null,
        gapVsMin: impliedHourly ? impliedHourly - minWage : null,
      };
    }).filter((c: CodeAnalysisEntry) => c.tmsisRate != null && c.tmsisRate > 0);

    // Primary code (first with per-hour conversion)
    const primary = codeAnalysis.find((c: CodeAnalysisEntry) => c.impliedHourly != null) || codeAnalysis[0] || null;

    return {
      state: s1,
      stateName: STATE_NAMES[s1] || s1,
      category: curCat,
      wage,
      natlWage,
      minWage,
      codes: codeAnalysis,
      primary,
    };
  }, [blsData, curCat, s1, overhead, getTmsisRateAlt]);

  // All-state comparison for the primary code
  const allStates = useMemo((): AllStateEntry[] => {
    if (!blsData || !curCat || !curCat.codes[0]) return [];
    const socCode = curCat.soc;
    const primaryCode = curCat.codes.find((c: CrosswalkCode) => c.units_per_hour) || curCat.codes[0];

    return SL.map((st: string) => {
      const wage = blsData.states[st]?.[socCode];
      const tmsisRate = getTmsisRateAlt(st, primaryCode.hcpcs);
      let impliedHourly: number | null = null;
      if (tmsisRate && primaryCode.units_per_hour) {
        impliedHourly = tmsisRate * primaryCode.units_per_hour * (1 - overhead / 100);
      }
      const blsMedian = wage?.h_median || null;
      const minW = MIN_WAGE[st] || FED_MIN;
      return {
        st,
        name: STATE_NAMES[st] || st,
        blsMedian,
        impliedHourly,
        tmsisRate,
        gap: (impliedHourly && blsMedian) ? impliedHourly - blsMedian : null,
        gapPct: (impliedHourly && blsMedian) ? ((impliedHourly / blsMedian - 1) * 100) : null,
        minWage: minW,
        belowMin: impliedHourly != null && impliedHourly < minW,
        emp: wage?.emp || 0,
      };
    }).filter((s: AllStateEntry) => s.impliedHourly != null && s.blsMedian != null)
      .sort((a: AllStateEntry, b: AllStateEntry) => safe(a.gap) - safe(b.gap));
  }, [blsData, curCat, SL, overhead, getTmsisRateAlt]);

  // Quality measure linkage
  const qualityLink = useMemo((): QualityLinkEntry[] | null => {
    if (!qualData?.rates || !curCat) return null;
    // Find quality measures that relate to this service category
    const catMeasures: Record<string, string[]> = {
      hcbs: [],
      behavioral: ['FUH-AD', 'FUM-AD', 'IET-AD', 'CDF-AD'],
      dental: ['SFM-CH', 'OEV-CH', 'TFL-CH'],
      nursing: [],
      aba: ['ADD-CH', 'APM-CH', 'APP-CH'],
      therapy: [],
      respite: [],
    };
    const measureIds = catMeasures[cat] || [];
    if (measureIds.length === 0) return null;

    const mapped = measureIds.map((mId: string): QualityLinkEntry | null => {
      const meta = qualData.measures?.[mId];
      const rates = qualData.rates?.[mId];
      if (!meta || !rates) return null;
      const stateRate = rates[s1];
      return {
        id: mId,
        name: meta.name,
        domain: meta.domain,
        stateRate,
        median: meta.median,
        gapVsMedian: (stateRate != null && meta.median != null) ? stateRate - meta.median : null,
        direction: meta.type,
      };
    });
    const results: QualityLinkEntry[] = mapped.filter((m): m is QualityLinkEntry => m != null);

    return results.length > 0 ? results : null;
  }, [qualData, curCat, cat, s1]);

  if (loading) return (
    <div style={{ display:"flex",justifyContent:"center",alignItems:"center",minHeight:400,fontFamily:"Helvetica Neue,Arial,sans-serif" }}>
      <div style={{ textAlign:"center" }}><div style={{ fontSize:16,fontWeight:600,color:A }}>Loading Wage Data...</div><div style={{ fontSize:11,color:AL,marginTop:4 }}>BLS + T-MSIS</div></div>
    </div>
  );

  if (!blsData) return (
    <div style={{ maxWidth:640,margin:"0 auto",padding:"40px 16px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>
      <Card><div style={{ padding:24,textAlign:"center" }}>
        <div style={{ fontSize:16,fontWeight:600,marginBottom:8 }}>Wage data not loaded</div>
        <div style={{ fontSize:12,color:AL,lineHeight:1.7 }}>
          Place <code style={{ fontFamily:FM,background:SF,padding:"2px 6px",borderRadius:3 }}>bls_wages.json</code> and <code style={{ fontFamily:FM,background:SF,padding:"2px 6px",borderRadius:3 }}>soc_hcpcs_crosswalk.json</code> in the <code style={{ fontFamily:FM }}>/data</code> directory to enable this tool.
        </div>
      </div></Card>
    </div>
  );

  const categories = crosswalk?.categories || [];

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:12 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(14,98,69,0.1)",color:POS,fontWeight:600 }}>BLS May 2024</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{SL.length} states · {categories.length} categories</span>
          </div>
          <ExportBtn label="Export Analysis" onClick={() => {
            if (!allStates.length) return;
            const pc = curCat?.codes.find((c: CrosswalkCode)=>c.units_per_hour) || curCat?.codes[0];
            downloadCSV(`wage_adequacy_${cat}_${overhead}pct.csv`,
              ["State","BLS Median $/hr","Medicaid Rate","Implied Wage $/hr","Gap $/hr","Gap %","Min Wage","Below Min Wage","Employment"],
              allStates.map((s: AllStateEntry)=>[s.name,s.blsMedian?.toFixed(2),s.tmsisRate?.toFixed(2),s.impliedHourly?.toFixed(2),s.gap?.toFixed(2),s.gapPct?.toFixed(1),s.minWage,s.belowMin?"YES":"",s.emp])
            );
          }}/>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Rate & Wage Comparison</span> — Compares Medicaid reimbursement rates to BLS market wages for the equivalent occupation. For each service category, the tool converts the Medicaid rate into an implied hourly wage (after agency overhead), then shows how it compares to what workers in that field actually earn.
      </div></Card>

      {/* Controls */}
      <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap",margin:"10px 0" }}>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>State</span>
          <select value={s1} onChange={e=>setS1(e.currentTarget.value)} style={{ background:SF,border:`1px solid ${BD}`,padding:"5px 10px",borderRadius:6,fontSize:11,color:A }}>
            {SL.map((k: string)=><option key={k} value={k}>{STATE_NAMES[k]||k}</option>)}
          </select>
        </div>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Service Category</span>
          <div style={{ display:"flex",gap:3,flexWrap:"wrap" }}>
            {categories.map((c: CrosswalkCategory)=><Pill key={c.id} on={cat===c.id} onClick={()=>setCat(c.id)}>{c.name}</Pill>)}
          </div>
        </div>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Agency Overhead: {overhead}%</span>
          <div style={{ display:"flex",alignItems:"center",gap:6 }}>
            <input type="range" min={curCat?.overhead_range?.[0]||20} max={curCat?.overhead_range?.[1]||50} value={overhead} onChange={e=>setOverhead(+e.currentTarget.value)} style={{ width:120 }}/>
            <span style={{ fontFamily:FM,fontSize:11,fontWeight:600,color:A }}>{overhead}%</span>
          </div>
        </div>
      </div>

      {/* Primary Analysis Card */}
      {analysis && analysis.primary && <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:10 }}>
        <Card accent={analysis.primary.gapVsBls!=null&&analysis.primary.gapVsBls<0?NEG:POS}>
          <div style={{ padding:"14px 16px 10px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>{analysis.stateName}</div>
            <div style={{ fontSize:10,color:AL }}>{curCat?.name} · {analysis.wage.title}</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",padding:"0 6px 12px" }}>
            <Met l="BLS Median Wage" v={`$${safe(analysis.wage.h_median).toFixed(2)}/hr`} sub={`${fN(analysis.wage.emp ?? 0)} employed`}/>
            <Met l="Medicaid Implied Wage" v={analysis.primary.impliedHourly!=null?`$${analysis.primary.impliedHourly.toFixed(2)}/hr`:"—"} sub={`${analysis.primary.hcpcs} @ $${safe(analysis.primary.tmsisRate).toFixed(2)}/${analysis.primary.unit}`} cl={analysis.primary.gapVsBls!=null&&analysis.primary.gapVsBls<0?NEG:POS}/>
            <Met l="Wage Gap" v={analysis.primary.gapVsBls!=null?`${analysis.primary.gapVsBls>=0?"+":""}$${analysis.primary.gapVsBls.toFixed(2)}/hr`:"—"} sub={analysis.primary.gapVsBls!=null&&analysis.primary.impliedHourly!=null&&analysis.wage.h_median?`${((analysis.primary.impliedHourly/analysis.wage.h_median-1)*100).toFixed(0)}% vs market`:"No rate data"} cl={analysis.primary.gapVsBls!=null&&analysis.primary.gapVsBls<0?NEG:POS}/>
          </div>
          {analysis.primary.gapVsMin!=null && analysis.primary.gapVsMin<0 && (
            <div style={{ padding:"6px 16px 10px",borderTop:`1px solid ${BD}`,background:"rgba(164,38,44,0.04)" }}>
              <span style={{ fontSize:10,fontWeight:700,color:NEG }}>⚠ Below minimum wage.</span>
              <span style={{ fontSize:10,color:AL,marginLeft:4 }}>{analysis.stateName} min wage is ${analysis.minWage.toFixed(2)}/hr. Medicaid rate implies ${analysis.primary.impliedHourly?.toFixed(2)}/hr after {overhead}% overhead.</span>
            </div>
          )}
          <div style={{ padding:"4px 16px 10px",fontSize:9,color:AL,lineHeight:1.6 }}>
            <b>How this works:</b> {analysis.primary.hcpcs} ({analysis.primary.desc}) pays ${safe(analysis.primary.tmsisRate).toFixed(2)} per {analysis.primary.unit} in T-MSIS actual-paid data. At {analysis.primary.units_per_hour} units/hour = ${(safe(analysis.primary.tmsisRate)*safe(analysis.primary.units_per_hour)).toFixed(2)}/hr gross. After {overhead}% agency overhead → ${analysis.primary.impliedHourly?.toFixed(2)}/hr worker wage. BLS reports the median {analysis.wage.title?.toLowerCase()} in {analysis.stateName} earns ${analysis.wage.h_median?.toFixed(2)}/hr.
          </div>
        </Card>

        {/* Wage Distribution */}
        <Card x>
          <CH t="Wage Distribution" b={`${analysis.wage.title} in ${analysis.stateName}`}/>
          <div style={{ padding:"8px 14px 12px" }}>
            {(() => {
              const w = analysis.wage;
              const implied = analysis.primary?.impliedHourly ?? null;
              const maxVal = Math.max(safe(w.h_p90), safe(implied,0)) * 1.1;
              const markers = [
                { label:"10th pct", val:w.h_p10, color:AL },
                { label:"25th pct", val:w.h_p25, color:AL },
                { label:"Median", val:w.h_median, color:A },
                { label:"75th pct", val:w.h_p75, color:AL },
                { label:"90th pct", val:w.h_p90, color:AL },
              ];
              return <div style={{ position:"relative",height:120,marginTop:8 }}>
                {/* Background bar */}
                <div style={{ position:"absolute",top:40,left:0,right:0,height:24,background:SF,borderRadius:6,overflow:"hidden" }}>
                  {/* Range bar (25th to 75th) */}
                  <div style={{ position:"absolute",left:`${(safe(w.h_p25)/maxVal)*100}%`,width:`${((safe(w.h_p75)-safe(w.h_p25))/maxVal)*100}%`,height:"100%",background:"rgba(46,107,74,0.15)",borderRadius:4 }}/>
                </div>
                {/* Min wage line */}
                <div style={{ position:"absolute",top:32,left:`${(analysis.minWage/maxVal)*100}%`,width:2,height:40,background:WARN,opacity:0.6 }}/>
                <div style={{ position:"absolute",top:22,left:`${(analysis.minWage/maxVal)*100}%`,fontSize:7,color:WARN,fontFamily:FM,transform:"translateX(-50%)",whiteSpace:"nowrap" }}>Min ${analysis.minWage}</div>
                {/* Median marker */}
                <div style={{ position:"absolute",top:36,left:`${(safe(w.h_median)/maxVal)*100}%`,width:3,height:32,background:cB,borderRadius:2 }}/>
                <div style={{ position:"absolute",top:72,left:`${(safe(w.h_median)/maxVal)*100}%`,fontSize:8,color:cB,fontFamily:FM,fontWeight:700,transform:"translateX(-50%)",whiteSpace:"nowrap" }}>Mkt ${w.h_median?.toFixed(2)}</div>
                {/* Implied wage marker */}
                {implied!=null && <><div style={{ position:"absolute",top:36,left:`${(implied/maxVal)*100}%`,width:3,height:32,background:implied<safe(w.h_median)?NEG:POS,borderRadius:2 }}/>
                <div style={{ position:"absolute",top:84,left:`${(implied/maxVal)*100}%`,fontSize:8,color:implied<safe(w.h_median)?NEG:POS,fontFamily:FM,fontWeight:700,transform:"translateX(-50%)",whiteSpace:"nowrap" }}>Mcaid ${implied.toFixed(2)}</div></>}
                {/* Percentile labels */}
                {markers.map((m: { label: string; val: number | undefined; color: string })=>(
                  <div key={m.label} style={{ position:"absolute",top:10,left:`${(safe(m.val)/maxVal)*100}%`,fontSize:6,color:m.color,fontFamily:FM,transform:"translateX(-50%)",whiteSpace:"nowrap",opacity:m.label==="Median"?1:0.5 }}>${safe(m.val).toFixed(0)}</div>
                ))}
              </div>;
            })()}
          </div>
        </Card>
      </div>}

      {/* All Codes in Category */}
      {analysis && analysis.codes.length > 0 && <Card x>
        <CH t={`${curCat?.name} Code Breakdown`} b={`${analysis.codes.length} codes with T-MSIS data`} r={`${overhead}% overhead`}/>
        <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
          <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
            <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
              {["Code","Description","Rate/Unit","Gross $/hr","After Overhead","BLS Median","Gap","vs Min Wage"].map((h: string)=>(
                <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {analysis.codes.map((c: CodeAnalysisEntry) => (
                <tr key={c.hcpcs} style={{ borderBottom:`1px solid ${SF}` }}>
                  <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:cB }}>{c.hcpcs}</td>
                  <td style={{ padding:"4px",color:AL,maxWidth:140,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{c.desc}</td>
                  <td style={{ fontFamily:FM }}>${safe(c.tmsisRate).toFixed(2)}/{c.unit}</td>
                  <td style={{ fontFamily:FM }}>{c.units_per_hour?`$${(safe(c.tmsisRate)*c.units_per_hour).toFixed(2)}`:"—"}</td>
                  <td style={{ fontFamily:FM,fontWeight:600,color:c.gapVsBls!=null&&c.gapVsBls<0?NEG:POS }}>{c.impliedHourly!=null?`$${c.impliedHourly.toFixed(2)}`:"—"}</td>
                  <td style={{ fontFamily:FM,color:AL }}>${safe(analysis.wage.h_median).toFixed(2)}</td>
                  <td style={{ fontFamily:FM,fontWeight:700,color:c.gapVsBls!=null&&c.gapVsBls<0?NEG:c.gapVsBls!=null&&c.gapVsBls>0?POS:AL }}>{c.gapVsBls!=null?`${c.gapVsBls>=0?"+":""}$${c.gapVsBls.toFixed(2)}`:"—"}</td>
                  <td style={{ fontFamily:FM,color:c.gapVsMin!=null&&c.gapVsMin<0?NEG:POS }}>{c.gapVsMin!=null?`${c.gapVsMin>=0?"+":""}$${c.gapVsMin.toFixed(2)}`:"—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ fontSize:9,color:AL,marginTop:6,lineHeight:1.6 }}>
            T-MSIS rates = avg paid per claim (actual Medicaid payments, not fee schedule). {curCat?.overhead_note}
          </div>
        </div>
      </Card>}

      {/* National Comparison */}
      {allStates.length > 0 && <Card x>
        <CH t="Rate-Wage Gap by State" b={`${curCat?.codes.find((c: CrosswalkCode)=>c.units_per_hour)?.hcpcs||curCat?.codes[0]?.hcpcs} · ${allStates.length} states`} r={`Negative = Medicaid can't match market`}/>
        <div style={{ padding:"0 14px 8px" }}>
          <ResponsiveContainer width="100%" height={Math.max(240,allStates.length*13)}>
            <BarChart data={allStates} layout="vertical" margin={{ left:52,right:16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
              <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number)=>`${v>=0?"+":""}$${v.toFixed(0)}`}/>
              <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} axisLine={false} tickLine={false} width={28}/>
              <ReferenceLine x={0} stroke={A} strokeWidth={1.5}/>
              <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                <div>
                  <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                  <div>BLS median: <b>${safe(d.blsMedian as number | null).toFixed(2)}/hr</b></div>
                  <div style={{ color:((d.gap as number) ?? 0)<0?"#ff9999":"#99ff99" }}>Medicaid implied: <b>${safe(d.impliedHourly as number | null).toFixed(2)}/hr</b></div>
                  <div>Gap: {((d.gap as number) ?? 0)>=0?"+":""}${safe(d.gap as number | null).toFixed(2)}/hr ({safe(d.gapPct as number | null).toFixed(0)}%)</div>
                  <div style={{ fontSize:9 }}>Min wage: ${String(d.minWage ?? "")} {d.belowMin?"⚠ BELOW":""}</div>
                  <div style={{ fontSize:9 }}>{fN(d.emp as number ?? 0)} workers in state</div>
                </div>
              )}/>}/>
              <Bar dataKey="gap" barSize={8} radius={[0,3,3,0]}>
                {allStates.map((d: AllStateEntry,i: number)=><Cell key={i} fill={d.st===s1?cO:(d.gap ?? 0)<0?NEG:POS} opacity={d.st===s1?1:d.belowMin?0.9:0.5}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:cO,verticalAlign:"middle",marginRight:3 }}/>{STATE_NAMES[s1]||s1}</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>Below market (Medicaid can't match)</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>At or above market</span>
            <span>{allStates.filter((s: AllStateEntry)=>(s.gap ?? 0)<0).length} of {allStates.length} states below market · {allStates.filter((s: AllStateEntry)=>s.belowMin).length} below min wage</span>
          </div>
        </div>
      </Card>}

      {/* Quality Measure Linkage */}
      {qualityLink && qualityLink.length > 0 && <Card x>
        <CH t="Quality Outcome Linkage" b={`${curCat?.name} quality measures for ${STATE_NAMES[s1]||s1}`}/>
        <div style={{ padding:"6px 14px 12px" }}>
          <div style={{ fontSize:10,color:AL,marginBottom:8,lineHeight:1.6 }}>
            These Medicaid quality measures are linked to the services in this category. Low rates and poor quality outcomes together suggest a rate adequacy problem — providers can't afford to deliver the services that drive these measures.
          </div>
          {qualityLink.map((m: QualityLinkEntry) => {
            const isGood = m.direction?.includes("Higher") ? (m.stateRate ?? 0) >= safe(m.median) : (m.stateRate ?? 0) <= safe(m.median);
            return <div key={m.id} style={{ display:"flex",alignItems:"center",gap:8,padding:"5px 0",borderBottom:`1px solid ${SF}` }}>
              <span style={{ fontFamily:FM,fontSize:9,fontWeight:600,width:60,color:cB }}>{m.id}</span>
              <div style={{ flex:1,minWidth:0 }}>
                <div style={{ fontSize:10,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{m.name}</div>
                <div style={{ fontSize:9,color:AL }}>{m.domain}</div>
              </div>
              <div style={{ textAlign:"right",flexShrink:0 }}>
                <div style={{ fontFamily:FM,fontWeight:600,fontSize:12,color:isGood?POS:NEG }}>{m.stateRate!=null?`${m.stateRate}%`:"—"}</div>
                <div style={{ fontSize:8,color:AL }}>median {m.median}%</div>
              </div>
            </div>;
          })}
        </div>
      </Card>}

      {/* Summary Stats */}
      {allStates.length > 0 && <Card>
        <CH t="National Summary" b={`${curCat?.name} at ${overhead}% overhead`}/>
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 10px" }}>
          <Met l="States Below Market" v={`${allStates.filter((s: AllStateEntry)=>(s.gap ?? 0)<0).length} / ${allStates.length}`} cl={allStates.filter((s: AllStateEntry)=>(s.gap ?? 0)<0).length > allStates.length/2?NEG:POS}/>
          <Met l="States Below Min Wage" v={`${allStates.filter((s: AllStateEntry)=>s.belowMin).length}`} cl={allStates.filter((s: AllStateEntry)=>s.belowMin).length>0?NEG:POS}/>
          <Met l="Median Gap" v={`$${(allStates.map((s: AllStateEntry)=>s.gap ?? 0).sort((a: number, b: number)=>a-b)[Math.floor(allStates.length/2)]||0).toFixed(2)}/hr`}/>
          <Met l="Worst Gap" v={allStates[0]?`${allStates[0].name}: $${allStates[0].gap?.toFixed(2)}`:"—"} cl={NEG}/>
          <Met l="Best Gap" v={allStates[allStates.length-1]?`${allStates[allStates.length-1].name}: +$${allStates[allStates.length-1].gap?.toFixed(2)}`:"—"} cl={POS}/>
        </div>
      </Card>}

      {/* About */}
      <Card><CH t="Data Sources & Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Wages:</b> Bureau of Labor Statistics, Occupational Employment and Wage Statistics (OEWS), May 2024. State-level employment and wage estimates for Standard Occupational Classification (SOC) codes mapped to Medicaid service categories.<br/>
        <b>Medicaid rates:</b> T-MSIS actual-paid rates from HHS Medicaid Provider Spending data. These are blended rates across all modifiers, places of service, and managed care encounters — not fee schedule rates.<br/>
        <b>Implied wage calculation:</b> (Medicaid rate × units per hour) × (1 − overhead %). For 15-minute codes, units per hour = 4. Overhead covers employer payroll taxes, workers' comp, admin, benefits, and agency margin.<br/>
        <b>Minimum wages:</b> State minimum wages as of 2024. Federal minimum ($7.25) used where state has no higher minimum.<br/>
        <b>Quality measures:</b> CMS Medicaid & CHIP Core Set, 2024 reporting cycle (services primarily CY2023). State-level performance rates for measures linked to specific service categories.<br/>
        <b>Limitations:</b> T-MSIS rates are averages across all claim types and may not reflect rates for specific programs (e.g., waiver rates vs. state plan rates). The overhead model is a simplification — actual overhead varies by agency size, geography, and program requirements. BLS wage data covers all employers, not just Medicaid-funded positions. This tool provides directional analysis for rate adequacy discussions, not definitive cost modeling.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Rate & Wage Comparison v1.0 · BLS OEWS May 2024 + T-MSIS + CMS Core Set 2024</div>
    </div>
  );
}
