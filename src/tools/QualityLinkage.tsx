import React, { useState, useMemo, useEffect, useCallback } from "react";
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine, Cell, BarChart, Bar, Legend } from "recharts";
import type { QualData, LinkedMeasure, MeasureHcpcsInfo, MeasureMeta, SafeTipProps, TooltipEntry, QualHcpcsRecord } from "../types";

// ── Design System (matches Aradune) ─────────────────────────────────────
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

const DOMAIN_COLORS: Record<string, string> = {
  "Behavioral Health Care": "#6B46C1",
  "Dental and Oral Health Services": "#2E6B4A",
  "Primary Care Access and Preventive Care": "#2563EB",
  "Maternal and Perinatal Health": "#DB2777",
  "Care of Acute and Chronic Conditions": "#C4590A",
  "Long-Term Services and Supports": "#B8860B",
  "Experience of Care": "#64748B",
};

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
  <button onClick={onClick} style={{ padding:"3px 9px",fontSize:10,fontWeight:on?700:400,color:on?WH:AL,background:on?cB:"transparent",border:`1px solid ${on?cB:BD}`,borderRadius:5,cursor:"pointer",whiteSpace:"nowrap" }}>{children}</button>
);
const SafeTip = ({ active, payload, render }: SafeTipProps) => {
  if (!active||!payload?.length) return null;
  const d = payload[0]?.payload;
  if (!d) return null;
  return <div style={{ background:"rgba(10,37,64,0.95)",color:WH,padding:"8px 12px",borderRadius:6,fontSize:11,lineHeight:1.6,maxWidth:300,boxShadow:"0 4px 16px rgba(0,0,0,.2)" }}>{render(d)}</div>;
};
const f$ = (v: number): string => {
  if (v==null||isNaN(v)) return "$0";
  return v < 10 ? `$${v.toFixed(2)}` : `$${v.toFixed(0)}`;
};

function downloadCSV(name: string, headers: string[], rows: (string | number | null | undefined)[][]) {
  const esc = (v: string | number | null | undefined) => `"${String(v??"").replace(/"/g,'""')}"`;
  const csv = [headers.map(esc).join(","), ...rows.map(r => r.map(esc).join(","))].join("\n");
  const a = document.createElement("a");
  a.href = "data:text/csv;charset=utf-8," + encodeURIComponent(csv);
  a.download = name; a.click();
}
const ExportBtn = ({ onClick, label }: { onClick: () => void; label?: string }) => (
  <button onClick={onClick} style={{ fontSize:9,color:AL,background:SF,border:`1px solid ${BD}`,borderRadius:5,padding:"3px 8px",cursor:"pointer",fontFamily:FM }}>{label||"Export CSV"}</button>
);

// ── Main Component ──────────────────────────────────────────────────────
export default function QualityLinkage() {
  const [qualData, setQual] = useState<QualData | null>(null);
  const [hcpcsData, setHCPCS] = useState<QualHcpcsRecord[] | null>(null);
  const [selectedMeasure, setMeasure] = useState<string | null>(null);
  const [highlightState, setHighlight] = useState("FL");
  const [domainFilter, setDomain] = useState("all");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [qual, hcpcs] = await Promise.all([
          fetch("/data/quality_measures.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/hcpcs.json").then(r=>r.ok?r.json():null).catch(()=>null),
        ]);
        if (cancelled) return;
        if (qual) { setQual(qual); }
        if (hcpcs) setHCPCS(hcpcs);
      } catch(e) { console.error(e); }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // Auto-select first linked measure
  useEffect(() => {
    if (qualData?.measure_hcpcs && !selectedMeasure) {
      setMeasure(Object.keys(qualData.measure_hcpcs)[0]);
    }
  }, [qualData, selectedMeasure]);

  // Get T-MSIS rate for a code in a state
  const getTmsisRate = useCallback((state: string, code: string): number | null => {
    if (!hcpcsData || !Array.isArray(hcpcsData)) return null;
    const h = hcpcsData.find((r: QualHcpcsRecord) => (r.code||r.c) === code);
    if (!h) return null;
    if (h.r && typeof h.r === 'object') return h.r[state] || null;
    if (h.rates_by_state) {
      const sr = h.rates_by_state.find((s: { state: string; avg_rate: number }) => s.state === state);
      return sr?.avg_rate || null;
    }
    return null;
  }, [hcpcsData]);

  // Build linked measures list with metadata
  const linkedMeasures: LinkedMeasure[] = useMemo(() => {
    if (!qualData?.measure_hcpcs || !qualData?.measures) return [];
    return Object.entries(qualData.measure_hcpcs).map(([id, info]: [string, MeasureHcpcsInfo]) => {
      const meta: MeasureMeta | undefined = qualData.measures[id];
      if (!meta) return null;
      return { id, ...info, ...meta } as LinkedMeasure;
    }).filter((m): m is LinkedMeasure => m !== null).sort((a, b) => (a.domain||"").localeCompare(b.domain||""));
  }, [qualData]);

  // Filter by domain
  const filteredMeasures = useMemo(() => {
    if (domainFilter === "all") return linkedMeasures;
    return linkedMeasures.filter((m: LinkedMeasure) => m.domain === domainFilter);
  }, [linkedMeasures, domainFilter]);

  const domains = useMemo(() => {
    const d = new Set(linkedMeasures.map((m: LinkedMeasure) => m.domain));
    return ["all", ...Array.from(d).sort()];
  }, [linkedMeasures]);

  // Build scatter data: for selected measure, get (rate, quality) for each state
  const scatterData = useMemo(() => {
    if (!qualData || !selectedMeasure) return [];
    const mInfo = qualData.measure_hcpcs[selectedMeasure];
    const rates = qualData.rates[selectedMeasure];
    if (!mInfo || !rates) return [];

    const codes = mInfo.codes;
    const states = Object.keys(rates);

    return states.map(st => {
      const qualRate = rates[st];
      // Get average T-MSIS rate across linked codes for this state
      const codeRates = codes.map((c: string) => getTmsisRate(st, c)).filter((r): r is number => r != null && r > 0);
      const avgRate = codeRates.length > 0 ? codeRates.reduce((a: number, b: number) => a+b, 0) / codeRates.length : null;

      return {
        st,
        name: STATE_NAMES[st] || st,
        qualRate,
        medicaidRate: avgRate,
        nCodes: codeRates.length,
        isHighlight: st === highlightState,
      };
    }).filter(d => d.qualRate != null);
  }, [qualData, selectedMeasure, hcpcsData, getTmsisRate, highlightState]);

  // Split scatter data: states with rate data vs without
  const withRates = useMemo(() => scatterData.filter(d => d.medicaidRate != null), [scatterData]);
  const withoutRates = useMemo(() => scatterData.filter(d => d.medicaidRate == null), [scatterData]);

  // Compute correlation if we have enough points
  const correlation = useMemo(() => {
    if (withRates.length < 5) return null;
    const n = withRates.length;
    const xs = withRates.map(d => d.medicaidRate!);
    const ys = withRates.map(d => d.qualRate);
    const mx = xs.reduce((a,b)=>a+b,0) / n;
    const my = ys.reduce((a,b)=>a+b,0) / n;
    let num = 0, dx2 = 0, dy2 = 0;
    for (let i = 0; i < n; i++) {
      const dx = xs[i] - mx, dy = ys[i] - my;
      num += dx * dy;
      dx2 += dx * dx;
      dy2 += dy * dy;
    }
    const denom = Math.sqrt(dx2 * dy2);
    const r = denom > 0 ? num / denom : 0;
    // Regression line
    const slope = dx2 > 0 ? num / dx2 : 0;
    const intercept = my - slope * mx;
    const minX = Math.min(...xs);
    const maxX = Math.max(...xs);
    return { r, r2: r*r, n, slope, intercept, minX, maxX };
  }, [withRates]);

  // Selected measure metadata
  const curMeasure = useMemo(() => {
    if (!qualData || !selectedMeasure) return null;
    const meta = qualData.measures[selectedMeasure];
    const hcpcs = qualData.measure_hcpcs[selectedMeasure];
    if (!meta) return null;
    return { id: selectedMeasure, ...meta, ...hcpcs };
  }, [qualData, selectedMeasure]);

  // State ranking table for selected measure
  const stateRanking = useMemo(() => {
    return [...scatterData].sort((a,b) => (b.qualRate||0) - (a.qualRate||0));
  }, [scatterData]);

  // All-measures overview for highlighted state
  const stateOverview = useMemo(() => {
    if (!qualData || !highlightState) return [];
    return linkedMeasures.map((m: LinkedMeasure) => {
      const rate = qualData.rates[m.id]?.[highlightState];
      const median = m.median;
      return {
        id: m.id,
        name: m.name,
        domain: m.domain,
        rate,
        median,
        gap: rate != null && median != null ? rate - median : null,
        aboveMedian: rate != null && median != null ? rate >= median : null,
      };
    }).filter(m => m.rate != null);
  }, [qualData, linkedMeasures, highlightState]);

  if (loading) return (
    <div style={{ display:"flex",justifyContent:"center",alignItems:"center",minHeight:400,fontFamily:"Helvetica Neue,Arial,sans-serif" }}>
      <div style={{ textAlign:"center" }}><div style={{ fontSize:16,fontWeight:600,color:A }}>Loading Quality Data...</div></div>
    </div>
  );

  if (!qualData) return (
    <div style={{ maxWidth:640,margin:"0 auto",padding:"40px 16px",fontFamily:"Helvetica Neue,Arial,sans-serif" }}>
      <Card><div style={{ padding:24,textAlign:"center" }}>
        <div style={{ fontSize:16,fontWeight:600,marginBottom:8,color:A }}>Quality data not loaded</div>
        <div style={{ fontSize:12,color:AL }}>Place <code style={{ fontFamily:FM,background:SF,padding:"2px 6px",borderRadius:3 }}>quality_measures.json</code> in <code style={{ fontFamily:FM }}>/data</code></div>
      </div></Card>
    </div>
  );

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:10 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(14,98,69,0.1)",color:POS,fontWeight:600 }}>CMS Core Set 2024</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{linkedMeasures.length} measures linked to HCPCS · {Object.keys(qualData.rates).length} total measures</span>
          </div>
          <ExportBtn label="Export Data" onClick={() => {
            if (!stateRanking.length || !curMeasure) return;
            downloadCSV(`quality_${selectedMeasure}.csv`,
              ["State","Quality Rate %","Avg Medicaid Rate","# Codes with Data","Measure","Domain"],
              stateRanking.map(s=>[s.name,s.qualRate,s.medicaidRate?.toFixed(2)||"",s.nCodes,curMeasure.name,curMeasure.domain])
            );
          }}/>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Quality ↔ Rate Linkage</span> — Connects CMS Medicaid Core Set quality outcomes to the actual reimbursement rates for the services that drive those outcomes. If a state pays poorly for well-child visits, does it show up in their well-child visit quality measure? This tool makes that question answerable with data.
      </div></Card>

      {/* Controls */}
      <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap",margin:"10px 0" }}>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Highlight State</span>
          <select value={highlightState} onChange={e=>setHighlight(e.currentTarget.value)} style={{ background:SF,border:`1px solid ${BD}`,padding:"5px 10px",borderRadius:6,fontSize:11,color:A }}>
            {Object.keys(STATE_NAMES).sort((a,b)=>(STATE_NAMES[a]).localeCompare(STATE_NAMES[b])).map(k=><option key={k} value={k}>{STATE_NAMES[k]}</option>)}
          </select>
        </div>
        <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Domain</span>
          <div style={{ display:"flex",gap:3,flexWrap:"wrap" }}>
            {domains.map(d=><Pill key={d} on={domainFilter===d} onClick={()=>setDomain(d)}>{d==="all"?"All Domains":d.replace("Care","").replace("Services","").trim()}</Pill>)}
          </div>
        </div>
      </div>

      {/* Measure selector */}
      <Card>
        <CH t="Select Quality Measure" b={`${filteredMeasures.length} measures with HCPCS linkage`}/>
        <div style={{ padding:"4px 14px 10px",maxHeight:220,overflowY:"auto" }}>
          {filteredMeasures.map((m: LinkedMeasure) => {
            const stRate = qualData.rates[m.id]?.[highlightState];
            const domColor = DOMAIN_COLORS[m.domain] || AL;
            return (
              <div key={m.id} onClick={()=>setMeasure(m.id)}
                style={{ display:"flex",alignItems:"center",gap:8,padding:"5px 6px",borderRadius:6,cursor:"pointer",
                  background:selectedMeasure===m.id?"rgba(46,107,74,0.06)":"transparent",
                  borderLeft:selectedMeasure===m.id?`3px solid ${cB}`:"3px solid transparent",
                  transition:"all 0.15s" }}>
                <span style={{ fontFamily:FM,fontSize:9,fontWeight:600,width:68,flexShrink:0,color:cB }}>{m.id}</span>
                <div style={{ flex:1,minWidth:0 }}>
                  <div style={{ fontSize:10,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{m.name}</div>
                  <div style={{ fontSize:8,color:domColor,fontWeight:500 }}>{m.domain} · {m.codes.slice(0,3).join(", ")}{m.codes.length>3?"...":""}</div>
                </div>
                <div style={{ textAlign:"right",flexShrink:0,width:70 }}>
                  <div style={{ fontFamily:FM,fontSize:11,fontWeight:600,color:stRate!=null?(stRate>=(m.median??0)?POS:NEG):AL }}>{stRate!=null?`${stRate}%`:"—"}</div>
                  <div style={{ fontSize:7,color:AL }}>med {m.median}%</div>
                </div>
              </div>
            );
          })}
        </div>
      </Card>

      {/* Analysis for selected measure */}
      {curMeasure && <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(280px,1fr))",gap:10 }}>

        {/* Measure Detail Card */}
        <Card accent={DOMAIN_COLORS[curMeasure.domain]}>
          <div style={{ padding:"14px 16px 6px" }}>
            <div style={{ fontSize:14,fontWeight:500,lineHeight:1.3 }}>{curMeasure.name}</div>
            <div style={{ fontSize:9,color:DOMAIN_COLORS[curMeasure.domain]||AL,fontWeight:500,marginTop:2 }}>{curMeasure.domain}</div>
            <div style={{ fontSize:10,color:AL,marginTop:4,lineHeight:1.5 }}>{curMeasure.rate_def}</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",padding:"4px 6px 10px" }}>
            <Met l={`${STATE_NAMES[highlightState]} Rate`} v={selectedMeasure && qualData.rates[selectedMeasure]?.[highlightState]!=null?`${qualData.rates[selectedMeasure][highlightState]}%`:"—"} cl={selectedMeasure && qualData.rates[selectedMeasure]?.[highlightState]>=(curMeasure.median??0)?POS:NEG}/>
            <Met l="National Median" v={curMeasure.median!=null?`${curMeasure.median}%`:"—"}/>
            <Met l="States Reporting" v={`${curMeasure.n_states}`} sub={`of ${Object.keys(STATE_NAMES).length}`}/>
          </div>
          <div style={{ padding:"4px 16px 10px" }}>
            <div style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5,marginBottom:4 }}>Linked HCPCS Codes</div>
            <div style={{ display:"flex",gap:4,flexWrap:"wrap" }}>
              {curMeasure.codes?.map((c: string) => (
                <span key={c} style={{ fontFamily:FM,fontSize:10,background:SF,padding:"2px 6px",borderRadius:4,border:`1px solid ${BD}` }}>{c}</span>
              ))}
            </div>
            <div style={{ fontSize:9,color:AL,marginTop:4 }}>{curMeasure.desc}</div>
          </div>
        </Card>

        {/* Scatter Plot: Rate vs Quality */}
        <Card>
          <CH t="Rate → Quality Relationship" b={withRates.length>0?`${withRates.length} states with both rate + quality data`:`Quality data only (no T-MSIS rate match)`} r={correlation?`r = ${correlation.r.toFixed(3)}`:""}/>
          {withRates.length >= 3 ? (
            <div style={{ padding:"0 8px 8px" }}>
              <ResponsiveContainer width="100%" height={260}>
                <ScatterChart margin={{ left:8,right:16,top:8,bottom:8 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BD}/>
                  <XAxis type="number" dataKey="medicaidRate" name="Medicaid Rate" tick={{ fill:AL,fontSize:8,fontFamily:FM }} tickFormatter={(v: number)=>`$${v.toFixed(0)}`} label={{ value:"Avg Medicaid Rate ($)",position:"bottom",offset:-2,style:{fontSize:9,fill:AL} }}/>
                  <YAxis type="number" dataKey="qualRate" name="Quality %" tick={{ fill:AL,fontSize:8,fontFamily:FM }} tickFormatter={(v: number)=>`${v}%`} label={{ value:"Quality Measure %",angle:-90,position:"insideLeft",style:{fontSize:9,fill:AL} }}/>
                  {curMeasure.median && <ReferenceLine y={curMeasure.median} stroke={WARN} strokeDasharray="4 4" label={{ value:`Median ${curMeasure.median}%`,position:"right",style:{fontSize:8,fill:WARN} }}/>}
                  <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                    <div>
                      <div style={{ fontWeight:600 }}>{d.name as string} ({d.st as string})</div>
                      <div>Quality: <b>{d.qualRate as number}%</b></div>
                      <div>Avg Medicaid rate: <b>${(d.medicaidRate as number)?.toFixed(2)}</b></div>
                      <div style={{ fontSize:9 }}>{d.nCodes as number} of {curMeasure.codes?.length ?? 0} linked codes found in T-MSIS</div>
                    </div>
                  )}/>}/>
                  <Scatter data={withRates} fill={cB}>
                    {withRates.map((d,i)=><Cell key={i} fill={d.isHighlight?cO:cB} r={d.isHighlight?6:4} opacity={d.isHighlight?1:0.5}/>)}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
              {correlation && <div style={{ padding:"4px 8px",fontSize:10,color:AL,lineHeight:1.6 }}>
                {Math.abs(correlation.r) < 0.2 && "Weak or no linear relationship between reimbursement rates and quality outcomes for this measure. Other factors (eligibility, outreach, provider networks) likely dominate."}
                {Math.abs(correlation.r) >= 0.2 && Math.abs(correlation.r) < 0.4 && `Modest ${correlation.r>0?"positive":"negative"} correlation (r=${correlation.r.toFixed(2)}). States paying more for these services ${correlation.r>0?"tend to":"don't necessarily"} score better on this measure, but the relationship is not strong.`}
                {Math.abs(correlation.r) >= 0.4 && `Notable ${correlation.r>0?"positive":"negative"} correlation (r=${correlation.r.toFixed(2)}). States paying ${correlation.r>0?"higher":"lower"} rates for linked services tend to perform ${correlation.r>0?"better":"worse"} on this quality measure. This doesn't prove causation, but is consistent with rate adequacy affecting outcomes.`}
              </div>}
            </div>
          ) : (
            <div style={{ padding:"16px",fontSize:11,color:AL,textAlign:"center" }}>
              {withRates.length === 0 ? "No T-MSIS rate data found for linked HCPCS codes. Rate data will appear once fee schedule database is integrated." : "Insufficient data points for scatter plot (need ≥3 states with both rate and quality data)."}
              <br/><span style={{ fontSize:10 }}>{scatterData.length} states have quality data for this measure.</span>
            </div>
          )}
        </Card>
      </div>}

      {/* State Rankings */}
      {stateRanking.length > 0 && curMeasure && <Card>
        <CH t={`State Rankings: ${curMeasure.id}`} b={`${stateRanking.length} states`} r={`Higher = better for this measure`}/>
        <div style={{ padding:"0 14px 8px" }}>
          <ResponsiveContainer width="100%" height={Math.max(200, Math.min(stateRanking.length * 12, 500))}>
            <BarChart data={stateRanking} layout="vertical" margin={{ left:52,right:16 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
              <XAxis type="number" domain={[0,'auto']} tick={{ fill:AL,fontSize:8,fontFamily:FM }} tickFormatter={(v: number)=>`${v}%`}/>
              <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} width={28}/>
              {curMeasure.median && <ReferenceLine x={curMeasure.median} stroke={WARN} strokeWidth={1.5} strokeDasharray="4 4"/>}
              <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                <div>
                  <div style={{ fontWeight:600 }}>{d.name as string}</div>
                  <div>Quality: <b>{d.qualRate as number}%</b> {(d.qualRate as number)>=(curMeasure.median??0)?"(above median)":"(below median)"}</div>
                  {d.medicaidRate != null && <div>Avg Medicaid rate: <b>${(d.medicaidRate as number).toFixed(2)}</b></div>}
                </div>
              )}/>}/>
              <Bar dataKey="qualRate" barSize={7} radius={[0,3,3,0]}>
                {stateRanking.map((d,i)=><Cell key={i} fill={d.st===highlightState?cO:d.qualRate>=(curMeasure.median??0)?POS:NEG} opacity={d.st===highlightState?1:0.45}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0" }}>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:cO,verticalAlign:"middle",marginRight:3 }}/>{STATE_NAMES[highlightState]}</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:WARN,verticalAlign:"middle",marginRight:3 }}/>National median ({curMeasure.median}%)</span>
            <span>{stateRanking.filter(s=>s.qualRate>=(curMeasure.median??0)).length} above median · {stateRanking.filter(s=>s.qualRate<(curMeasure.median??0)).length} below</span>
          </div>
        </div>
      </Card>}

      {/* State Quality Overview */}
      {stateOverview.length > 0 && <Card>
        <CH t={`${STATE_NAMES[highlightState]} Quality Scorecard`} b={`${stateOverview.length} measures with data`} r={`${stateOverview.filter(m=>m.aboveMedian).length} above median · ${stateOverview.filter(m=>!m.aboveMedian).length} below`}/>
        <div style={{ padding:"4px 14px 10px",overflowX:"auto" }}>
          <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
            <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
              {["Measure","Domain","State Rate","Median","Gap","Status"].map(h=>(
                <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {stateOverview.map(m => (
                <tr key={m.id} onClick={()=>setMeasure(m.id)} style={{ borderBottom:`1px solid ${SF}`,cursor:"pointer",background:selectedMeasure===m.id?"rgba(46,107,74,0.04)":"transparent" }}>
                  <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:cB }}>{m.id}</td>
                  <td style={{ padding:"4px",color:DOMAIN_COLORS[m.domain]||AL,fontSize:9,maxWidth:140,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{m.domain}</td>
                  <td style={{ fontFamily:FM,fontWeight:600 }}>{m.rate}%</td>
                  <td style={{ fontFamily:FM,color:AL }}>{m.median}%</td>
                  <td style={{ fontFamily:FM,fontWeight:700,color:(m.gap ?? 0)>=0?POS:NEG }}>{(m.gap ?? 0)>=0?"+":""}{m.gap?.toFixed(1)}pp</td>
                  <td>{m.aboveMedian?<span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(46,107,74,0.1)",color:POS,fontWeight:600 }}>Above</span>:<span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(164,38,44,0.1)",color:NEG,fontWeight:600 }}>Below</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>}

      {/* About */}
      <Card><CH t="Data Sources & Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Quality measures:</b> CMS Medicaid & CHIP Core Set, 2024 reporting cycle (services primarily CY2023). Includes both Adult and Child Core Sets. Only rates flagged as "used in calculating state mean and median" are included.<br/>
        <b>HCPCS linkage:</b> Each quality measure is mapped to the HCPCS/CDT codes that represent the clinical services driving performance on that measure. For example, the Well-Child Visit measure (WCV-CH) is linked to preventive E&M codes 99391-99395. These mappings are based on measure technical specifications and clinical logic.<br/>
        <b>Medicaid rates:</b> T-MSIS actual-paid rates (average paid per claim). When a measure links to multiple codes, the average rate across available codes is used. Not all linked codes may have T-MSIS data for all states.<br/>
        <b>Correlation:</b> Pearson correlation coefficient between state-level average Medicaid rate and quality measure performance. This is an ecological association — it does not establish causation. Many factors beyond reimbursement affect quality outcomes, including eligibility policies, care management programs, provider networks, and social determinants.<br/>
        <b>Limitations:</b> The HCPCS linkage is approximate — quality measures often depend on care delivery patterns that extend beyond the specific billed codes. T-MSIS rates are blended averages that may not reflect rates for specific populations or delivery systems.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Quality-Rate Linkage v1.0 · CMS Core Set 2024 + T-MSIS</div>
    </div>
  );
}
