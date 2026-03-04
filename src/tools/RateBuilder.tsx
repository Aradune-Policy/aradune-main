import React, { useState, useMemo, useEffect, useCallback } from "react";
import type { Methodology, ComputeContext, RateResult, RateBuilderHcpcs, RateBuilderMedicare } from "../types";
import { useProAccess, ProBadge, ProGateModal } from "../components/ProGate";
import { query as duckQuery } from "../lib/duckdb";

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

// Rate-setting methodologies
const METHODOLOGIES: Methodology[] = [
  {
    id: "rbrvs",
    name: "RBRVS (% of Medicare)",
    desc: "Apply a conversion factor to Medicare RVUs. Most common methodology for physician services.",
    fields: [
      { id: "pctMedicare", label: "% of Medicare", type: "range", min: 25, max: 150, default: 80, step: 1, unit: "%" },
    ],
    compute: (inputs: Record<string, number>, ctx: ComputeContext): RateResult | null => {
      if (!ctx.medicareRate) return null;
      const rate = ctx.medicareRate * (inputs.pctMedicare / 100);
      return {
        rate,
        formula: `Medicare $${ctx.medicareRate.toFixed(2)} × ${inputs.pctMedicare}% = $${rate.toFixed(2)}`,
        components: [
          { label: "Medicare PFS Rate", value: `$${ctx.medicareRate.toFixed(2)}`, note: "CY2026 non-facility" },
          { label: "Scaling Factor", value: `${inputs.pctMedicare}%`, note: "Percentage of Medicare" },
          { label: "Calculated Rate", value: `$${rate.toFixed(2)}`, note: "Your Medicaid rate", bold: true },
        ],
      };
    },
  },
  {
    id: "cf",
    name: "Custom Conversion Factor",
    desc: "Multiply total RVUs by a state-specific conversion factor. Used by FL, TX, and many RBRVS states.",
    fields: [
      { id: "cf", label: "Conversion Factor ($)", type: "number", min: 0.01, max: 100, default: 28.25, step: 0.01, unit: "$" },
    ],
    compute: (inputs: Record<string, number>, ctx: ComputeContext): RateResult | null => {
      if (!ctx.rvu) return null;
      const rate = ctx.rvu * inputs.cf;
      return {
        rate,
        formula: `RVU ${ctx.rvu.toFixed(4)} × CF $${inputs.cf.toFixed(2)} = $${rate.toFixed(2)}`,
        components: [
          { label: "Total RVU (Non-Facility)", value: ctx.rvu.toFixed(4), note: "Work + PE + MP" },
          { label: "Conversion Factor", value: `$${inputs.cf.toFixed(2)}`, note: "State CF" },
          { label: "Calculated Rate", value: `$${rate.toFixed(2)}`, note: "Your Medicaid rate", bold: true },
        ],
      };
    },
  },
  {
    id: "peer_median",
    name: "Peer State Median",
    desc: "Set rate at the median of selected comparison states. Common for new codes or benchmark studies.",
    fields: [
      { id: "adjustment", label: "Adjustment Factor", type: "range", min: 50, max: 150, default: 100, step: 5, unit: "%" },
    ],
    compute: (inputs: Record<string, number>, ctx: ComputeContext): RateResult | null => {
      if (!ctx.peerRates || ctx.peerRates.length === 0) return null;
      const sorted = [...ctx.peerRates].sort((a, b) => a.rate - b.rate);
      const median = sorted[Math.floor(sorted.length / 2)].rate;
      const rate = median * (inputs.adjustment / 100);
      return {
        rate,
        formula: `Peer median $${median.toFixed(2)} × ${inputs.adjustment}% = $${rate.toFixed(2)}`,
        components: [
          { label: "Peer States", value: `${ctx.peerRates.length} states`, note: ctx.peerRates.map((p: { st: string; rate: number }) => p.st).join(", ") },
          { label: "Median Rate", value: `$${median.toFixed(2)}`, note: `Range: $${sorted[0].rate.toFixed(2)} – $${sorted[sorted.length-1].rate.toFixed(2)}` },
          { label: "Adjustment", value: `${inputs.adjustment}%` },
          { label: "Calculated Rate", value: `$${rate.toFixed(2)}`, note: "Your Medicaid rate", bold: true },
        ],
      };
    },
  },
  {
    id: "flat",
    name: "Flat Rate / Manual",
    desc: "Set a specific dollar amount. Document your rationale.",
    fields: [
      { id: "flatRate", label: "Rate ($)", type: "number", min: 0.01, max: 10000, default: 50, step: 0.01, unit: "$" },
    ],
    compute: (inputs: Record<string, number>, ctx: ComputeContext): RateResult | null => {
      const rate = inputs.flatRate;
      const pctMcr = ctx.medicareRate ? (rate / ctx.medicareRate * 100) : null;
      return {
        rate,
        formula: `Manual rate: $${rate.toFixed(2)}`,
        components: [
          { label: "Set Rate", value: `$${rate.toFixed(2)}`, bold: true },
          ...(pctMcr && ctx.medicareRate ? [{ label: "% of Medicare", value: `${pctMcr.toFixed(1)}%`, note: `Medicare = $${ctx.medicareRate.toFixed(2)}` }] : []),
        ],
      };
    },
  },
];

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

function downloadCSV(name: string, headers: string[], rows: (string | number | null | undefined)[][]) {
  const esc = (v: string | number | null | undefined) => `"${String(v??"").replace(/"/g,'""')}"`;
  const csv = [headers.map(esc).join(","), ...rows.map((r: (string | number | null | undefined)[]) => r.map(esc).join(","))].join("\n");
  const a = document.createElement("a");
  a.href = "data:text/csv;charset=utf-8," + encodeURIComponent(csv);
  a.download = name; a.click();
}

// ── Selected Code State Shape ────────────────────────────────────────
interface StateRate {
  st: string;
  rate: number;
  name: string;
  feeScheduleRate: number | null;
  rateSource: "fee_schedule" | "tmsis";
}

interface SelectedCodeData {
  code: string;
  medicare: Record<string, unknown> | null;
  medicareRate: number | null;
  rvu: number | null;
  workRvu: number | null;
  peRvu: number | null;
  mpRvu: number | null;
  desc: string | null;
  stateRates: StateRate[];
  nStates: number;
}

// ── Main Component ──────────────────────────────────────────────────────
export default function RateBuilder() {
  const [codeInput, setCodeInput] = useState("");
  const [selectedCode, setSelectedCode] = useState<SelectedCodeData | null>(null);
  const [methodology, setMethodology] = useState("rbrvs");
  const [methodInputs, setInputs] = useState<Record<string, number>>({});
  const [hcpcsData, setHCPCS] = useState<RateBuilderHcpcs[] | null>(null);
  const [medicareData, setMedicare] = useState<RateBuilderMedicare | null>(null);
  const [loading, setLoading] = useState(true);
  const [medicaidRates, setMedicaidRates] = useState<Record<string, Record<string, [number, string, string]>> | null>(null);
  const [gpciData, setGpciData] = useState<{ state: string; pe_gpci: number; pw_gpci: number; mp_gpci: number }[]>([]);
  const [gpciAdjust, setGpciAdjust] = useState(false);
  const { isPro } = useProAccess();
  const [showGate, setShowGate] = useState(false);
  const [gateFeature, setGateFeature] = useState("");
  const [fiscalState, setFiscalState] = useState("FL");
  const [codeSpending, setCodeSpending] = useState<{ paid: number; claims: number; bene: number } | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [hcpcs, medicare, mcdRates, gpci] = await Promise.all([
          fetch("/data/hcpcs.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/medicare_rates.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/medicaid_rates.json").then(r=>r.ok?r.json():null).catch(()=>null),
          fetch("/data/gpci.json").then(r=>r.ok?r.json():[]).catch(()=>[]),
        ]);
        if (cancelled) return;
        if (hcpcs) setHCPCS(hcpcs as RateBuilderHcpcs[]);
        if (medicare) setMedicare(medicare as RateBuilderMedicare);
        if (mcdRates) setMedicaidRates(mcdRates);
        if (gpci) setGpciData(gpci);
      } catch(e) { console.error(e); }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // Initialize method inputs with defaults
  useEffect(() => {
    const meth = METHODOLOGIES.find(m => m.id === methodology);
    if (meth) {
      const defaults: Record<string, number> = {};
      meth.fields.forEach(f => { defaults[f.id] = f.default; });
      setInputs(prev => ({ ...defaults, ...prev }));
    }
  }, [methodology]);

  // Get Medicare data for a code
  const getMedicareData = useCallback((code: string): Record<string, unknown> | null => {
    if (!medicareData) return null;
    const entry = medicareData.rates[code];
    if (entry) return entry as unknown as Record<string, unknown>;
    return null;
  }, [medicareData]);

  // Get rates across states for a code (fee schedule preferred, T-MSIS fallback)
  const getStateRates = useCallback((code: string): StateRate[] => {
    const result: Record<string, StateRate> = {};

    // First, populate from T-MSIS
    if (hcpcsData && Array.isArray(hcpcsData)) {
      const h = hcpcsData.find((r: RateBuilderHcpcs) => (r.code||r.c) === code);
      if (h) {
        const ratesObj = h.rates || h.r;
        if (ratesObj && typeof ratesObj === 'object') {
          for (const [st, rate] of Object.entries(ratesObj)) {
            if ((rate as number) > 0) {
              result[st] = { st, rate: rate as number, name: STATE_NAMES[st]||st, feeScheduleRate: null, rateSource: "tmsis" };
            }
          }
        }
      }
    }

    // Override with fee schedule rates where available
    if (medicaidRates) {
      for (const st of Object.keys(medicaidRates)) {
        const entry = medicaidRates[st][code];
        if (entry && entry[0] > 0) {
          const fsRate = entry[0];
          if (result[st]) {
            result[st].feeScheduleRate = fsRate;
            result[st].rate = fsRate;
            result[st].rateSource = "fee_schedule";
          } else {
            result[st] = { st, rate: fsRate, name: STATE_NAMES[st]||st, feeScheduleRate: fsRate, rateSource: "fee_schedule" };
          }
        }
      }
    }

    return Object.values(result).filter(d => d.rate > 0);
  }, [hcpcsData, medicaidRates]);

  // Look up code
  const lookupCode = useCallback(() => {
    const code = codeInput.trim().toUpperCase();
    if (!code) return;
    const mcr = getMedicareData(code);
    const states = getStateRates(code);
    // Also check hcpcs data for description fallback
    const hRec = hcpcsData?.find((r: RateBuilderHcpcs) => (r.code||r.c) === code);
    // Medicare data fields: r (non-facility rate), fr (facility rate), rvu (total RVU), w (work RVU), d (description)
    setSelectedCode({
      code,
      medicare: mcr,
      medicareRate: (mcr?.r || null) as number | null,
      rvu: (mcr?.rvu || null) as number | null,
      workRvu: (mcr?.w || null) as number | null,
      peRvu: (mcr?.pe || null) as number | null,
      mpRvu: (mcr?.mp || null) as number | null,
      desc: (mcr?.d || hRec?.desc || hRec?.d || null) as string | null,
      stateRates: states.sort((a: StateRate, b: StateRate) => a.rate - b.rate),
      nStates: states.length,
    });
  }, [codeInput, getMedicareData, getStateRates, hcpcsData]);

  const curMethod = METHODOLOGIES.find(m => m.id === methodology);

  // GPCI factor for fiscal state
  const stateGpciFactor = useMemo(() => {
    if (!gpciData.length) return null;
    const entries = gpciData.filter(g => g.state === fiscalState);
    if (!entries.length) return null;
    // Approximate blended GPCI: 50.9% work, 44.8% PE, 4.3% MP (CMS standard)
    const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
    const pw = avg(entries.map(g => g.pw_gpci));
    const pe = avg(entries.map(g => g.pe_gpci));
    const mp = avg(entries.map(g => g.mp_gpci));
    const blended = pw * 0.509 + pe * 0.448 + mp * 0.043;
    return { pw, pe, mp, blended, localities: entries.length };
  }, [gpciData, fiscalState]);

  // Compute rate
  const result = useMemo((): RateResult | null => {
    if (!selectedCode || !curMethod) return null;
    let mcrRate = selectedCode.medicareRate ?? undefined;
    // Apply GPCI adjustment if toggled on
    if (gpciAdjust && stateGpciFactor && mcrRate) {
      mcrRate = mcrRate * stateGpciFactor.blended;
    }
    const ctx: ComputeContext = {
      medicareRate: mcrRate,
      rvu: selectedCode.rvu ?? undefined,
      peerRates: selectedCode.stateRates,
    };
    try {
      return curMethod.compute(methodInputs, ctx);
    } catch(e) {
      return null;
    }
  }, [selectedCode, curMethod, methodInputs, gpciAdjust, stateGpciFactor]);

  // Fiscal impact estimate
  const fiscalImpact = useMemo(() => {
    if (!result || !selectedCode || selectedCode.stateRates.length === 0) return null;
    const curRates = selectedCode.stateRates;
    const newRate = result.rate;
    return curRates.map((s: StateRate) => ({
      ...s,
      newRate,
      change: newRate - s.rate,
      changePct: ((newRate / s.rate) - 1) * 100,
    }));
  }, [result, selectedCode]);

  // Fiscal impact: query DuckDB for code spending in selected state
  useEffect(() => {
    if (!selectedCode) { setCodeSpending(null); return; }
    const code = selectedCode.code.replace(/'/g, "''");
    const st = fiscalState.replace(/'/g, "''");
    const sql = `SELECT SUM(total_paid) AS paid, SUM(total_claims) AS claims, SUM(total_beneficiaries) AS bene FROM 'claims.parquet' WHERE state='${st}' AND hcpcs_code='${code}' AND year=2023`;
    duckQuery(sql).then(r => {
      const row = r.rows[0];
      if (row) setCodeSpending({ paid: Number(row.paid ?? 0), claims: Number(row.claims ?? 0), bene: Number(row.bene ?? 0) });
      else setCodeSpending(null);
    }).catch(() => setCodeSpending(null));
  }, [selectedCode, fiscalState]);

  const fiscalSim = useMemo(() => {
    if (!result || !codeSpending || codeSpending.claims <= 0) return null;
    const curRate = codeSpending.paid / codeSpending.claims;
    const newSpending = result.rate * codeSpending.claims;
    const impact = newSpending - codeSpending.paid;
    return { curRate, newSpending, impact, pctChange: (impact / codeSpending.paid) * 100 };
  }, [result, codeSpending]);

  if (loading) return (
    <div style={{ display:"flex",justifyContent:"center",alignItems:"center",minHeight:400,fontFamily:"Helvetica Neue,Arial,sans-serif" }}>
      <div style={{ textAlign:"center" }}><div style={{ fontSize:16,fontWeight:600,color:A }}>Loading...</div></div>
    </div>
  );

  return (
    <div style={{ maxWidth:800,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:10 }}>
        <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(46,107,74,0.1)",color:POS,fontWeight:600 }}>Free Tool</span>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Rate Builder</span> — Enter a HCPCS code, choose a methodology, and get a calculated Medicaid rate with full documentation. See how the rate compares to Medicare and other states. Export the calculation for your records.
      </div></Card>

      {/* Code Input */}
      <Card>
        <CH t="1. Enter HCPCS Code"/>
        <div style={{ padding:"6px 14px 14px",display:"flex",gap:8,alignItems:"center" }}>
          <input
            type="text"
            value={codeInput}
            onChange={e => setCodeInput(e.currentTarget.value.toUpperCase())}
            onKeyDown={e => e.key === "Enter" && lookupCode()}
            placeholder="e.g., 99213, 90834, D1351"
            style={{ flex:1,padding:"8px 12px",border:`1px solid ${BD}`,borderRadius:6,fontSize:14,fontFamily:FM,color:A,background:SF }}
          />
          <button onClick={lookupCode} style={{ padding:"8px 20px",background:cB,color:WH,border:"none",borderRadius:6,fontSize:12,fontWeight:600,cursor:"pointer" }}>Look Up</button>
        </div>
      </Card>

      {/* Code Info */}
      {selectedCode && <Card accent={cB}>
        <div style={{ padding:"14px 16px" }}>
          <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline" }}>
            <div>
              <span style={{ fontSize:22,fontWeight:300,fontFamily:FM }}>{selectedCode.code}</span>
              {selectedCode.desc && <span style={{ fontSize:12,color:AL,marginLeft:10 }}>{selectedCode.desc}</span>}
            </div>
          </div>
        </div>
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(110px,1fr))",padding:"0 6px 12px" }}>
          <Met l="Medicare Rate" v={selectedCode.medicareRate?`$${selectedCode.medicareRate.toFixed(2)}`:"Not found"} sub="CY2026 non-facility"/>
          <Met l="Total RVU" v={selectedCode.rvu?selectedCode.rvu.toFixed(4):"—"} sub={selectedCode.workRvu?`W:${selectedCode.workRvu} PE:${selectedCode.peRvu} MP:${selectedCode.mpRvu}`:""}/>
          <Met l="States with Data" v={`${selectedCode.nStates}`} sub="T-MSIS actual-paid"/>
          {selectedCode.stateRates.length > 0 && <>
            <Met l="T-MSIS Median" v={`$${selectedCode.stateRates[Math.floor(selectedCode.stateRates.length/2)].rate.toFixed(2)}`} sub={`Range: $${selectedCode.stateRates[0].rate.toFixed(2)}–$${selectedCode.stateRates[selectedCode.stateRates.length-1].rate.toFixed(2)}`}/>
          </>}
        </div>
        {!selectedCode.medicareRate && !selectedCode.stateRates.length && (
          <div style={{ padding:"8px 16px 12px",fontSize:11,color:WARN }}>
            No Medicare or T-MSIS data found for this code. You can still use the Flat Rate methodology to set a rate manually.
          </div>
        )}
      </Card>}

      {/* Methodology Selection */}
      {selectedCode && <Card>
        <CH t="2. Choose Methodology"/>
        <div style={{ padding:"6px 14px 10px" }}>
          {METHODOLOGIES.map(m => {
            const disabled = (m.id === "rbrvs" && !selectedCode.medicareRate) ||
                           (m.id === "cf" && !selectedCode.rvu) ||
                           (m.id === "peer_median" && selectedCode.stateRates.length < 3);
            return (
              <div key={m.id} onClick={()=>!disabled && setMethodology(m.id)}
                style={{ display:"flex",alignItems:"flex-start",gap:8,padding:"8px 6px",borderRadius:6,cursor:disabled?"not-allowed":"pointer",
                  opacity:disabled?0.4:1,
                  background:methodology===m.id?"rgba(46,107,74,0.06)":"transparent",
                  borderLeft:methodology===m.id?`3px solid ${cB}`:"3px solid transparent" }}>
                <div style={{ width:16,height:16,borderRadius:"50%",border:`2px solid ${methodology===m.id?cB:BD}`,flexShrink:0,marginTop:1,display:"flex",alignItems:"center",justifyContent:"center" }}>
                  {methodology===m.id && <div style={{ width:8,height:8,borderRadius:"50%",background:cB }}/>}
                </div>
                <div>
                  <div style={{ fontSize:11,fontWeight:600,color:A }}>{m.name}</div>
                  <div style={{ fontSize:10,color:AL }}>{m.desc}</div>
                  {disabled && <div style={{ fontSize:9,color:WARN,marginTop:2 }}>{m.id==="rbrvs"?"No Medicare rate available":m.id==="cf"?"No RVU data available":"Need ≥3 states with data"}</div>}
                </div>
              </div>
            );
          })}
        </div>
      </Card>}

      {/* Method Parameters — inline compact */}
      {selectedCode && curMethod && (
        <div style={{ padding:"0 0 10px" }}>
          {curMethod.fields.map(f => (
            <div key={f.id} style={{ display:"flex",alignItems:"center",gap:10,padding:"6px 14px",background:SF,borderRadius:8,marginBottom:6 }}>
              <span style={{ fontSize:10,fontWeight:600,color:AL,whiteSpace:"nowrap" }}>{f.label}</span>
              {f.type === "range" ? (
                <input type="range" min={f.min} max={f.max} step={f.step}
                  value={methodInputs[f.id]??f.default}
                  onChange={e => { const v = Number(e.target.value); setInputs(prev => ({...prev, [f.id]: v})); }}
                  style={{ flex:1,maxWidth:180 }}/>
              ) : (
                <input type="number" min={f.min} max={f.max} step={f.step}
                  value={methodInputs[f.id]??f.default}
                  onChange={e => { const v = Number(e.target.value); setInputs(prev => ({...prev, [f.id]: v})); }}
                  style={{ width:90,padding:"4px 8px",border:`1px solid ${BD}`,borderRadius:6,fontSize:12,fontFamily:FM,color:A }}/>
              )}
              <span style={{ fontFamily:FM,fontSize:13,fontWeight:700,color:cB,whiteSpace:"nowrap" }}>{f.unit==="$"?"$":""}{(methodInputs[f.id]??f.default)}{f.unit==="%"?"%":""}</span>
            </div>
          ))}
          {/* GPCI Adjustment Toggle */}
          {stateGpciFactor && (methodology === "rbrvs" || methodology === "flat") && (
            <div style={{ display:"flex",alignItems:"center",gap:8,padding:"6px 14px",background:gpciAdjust?"rgba(46,107,74,0.06)":SF,borderRadius:8,marginBottom:6,cursor:"pointer",border:`1px solid ${gpciAdjust?cB:BD}` }}
              onClick={() => setGpciAdjust(v => !v)}>
              <div style={{ width:14,height:14,borderRadius:3,border:`2px solid ${gpciAdjust?cB:BD}`,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0 }}>
                {gpciAdjust && <span style={{ fontSize:10,color:cB,fontWeight:700 }}>{"\u2713"}</span>}
              </div>
              <div>
                <span style={{ fontSize:10,fontWeight:600,color:A }}>GPCI-Adjust Medicare Rate</span>
                <span style={{ fontSize:9,color:AL,marginLeft:6 }}>
                  {STATE_NAMES[fiscalState]} factor: {stateGpciFactor.blended.toFixed(3)} ({stateGpciFactor.localities} localit{stateGpciFactor.localities > 1 ? "ies" : "y"})
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Result */}
      {result && selectedCode && <Card accent={cB}>
        <CH t="3. Calculated Rate" r={result.formula}/>
        <div style={{ padding:"6px 14px 14px" }}>
          {result.components.map((c,i) => (
            <div key={i} style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"4px 0",borderBottom:i<result.components.length-1?`1px solid ${SF}`:"none" }}>
              <div>
                <span style={{ fontSize:c.bold?12:10,fontWeight:c.bold?700:400,color:c.bold?A:AL }}>{c.label}</span>
                {c.note && <span style={{ fontSize:9,color:AL,marginLeft:6 }}>{c.note}</span>}
              </div>
              <span style={{ fontFamily:FM,fontSize:c.bold?18:12,fontWeight:c.bold?600:400,color:c.bold?cB:A }}>{c.value}</span>
            </div>
          ))}

          {/* Context: where does this rate land? */}
          {selectedCode.medicareRate && result.rate && (
            <div style={{ marginTop:10,padding:"8px 10px",background:SF,borderRadius:6 }}>
              <div style={{ fontSize:9,fontWeight:600,color:AL,marginBottom:4 }}>Rate Context</div>
              <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",gap:4 }}>
                <div style={{ fontSize:10 }}>
                  <span style={{ color:AL }}>% of Medicare: </span>
                  <span style={{ fontFamily:FM,fontWeight:700,color:(result.rate/selectedCode.medicareRate*100)<75?NEG:cB }}>{(result.rate/selectedCode.medicareRate*100).toFixed(1)}%</span>
                </div>
                {selectedCode.stateRates.length > 0 && (() => {
                  const sorted = selectedCode.stateRates.map((s: StateRate)=>s.rate).sort((a: number, b: number)=>a-b);
                  const rank = sorted.filter((r: number) => r <= result.rate).length;
                  const pctile = (rank / sorted.length * 100).toFixed(0);
                  return <div style={{ fontSize:10 }}>
                    <span style={{ color:AL }}>Percentile among states: </span>
                    <span style={{ fontFamily:FM,fontWeight:700 }}>{pctile}th</span>
                    <span style={{ color:AL,fontSize:9 }}> ({selectedCode.stateRates.length} states)</span>
                  </div>;
                })()}
                <div style={{ fontSize:10 }}>
                  <span style={{ color:AL }}>vs Medicare: </span>
                  <span style={{ fontFamily:FM,fontWeight:700,color:result.rate>=selectedCode.medicareRate?POS:NEG }}>{result.rate>=selectedCode.medicareRate?"+":""}${(result.rate-selectedCode.medicareRate).toFixed(2)}</span>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Export */}
        <div style={{ padding:"0 14px 10px",display:"flex",gap:8,flexWrap:"wrap" }}>
          <button onClick={() => {
            const lines = [
              `RATE CALCULATION — ${selectedCode.code}`,
              `${selectedCode.desc || ""}`,
              ``,
              `Methodology: ${curMethod ? curMethod.name : ""}`,
              `Formula: ${result.formula}`,
              ``,
              ...result.components.map(c => `${c.label}: ${c.value}${c.note ? ` (${c.note})` : ""}`),
              ``,
              `Medicare CY2026: $${selectedCode.medicareRate?.toFixed(2) || "N/A"}`,
              `% of Medicare: ${selectedCode.medicareRate ? (result.rate/selectedCode.medicareRate*100).toFixed(1) + "%" : "N/A"}`,
              `States with T-MSIS data: ${selectedCode.nStates}`,
              selectedCode.stateRates.length > 0 ? `T-MSIS median: $${selectedCode.stateRates[Math.floor(selectedCode.stateRates.length/2)].rate.toFixed(2)}` : "",
              ``,
              `Generated by Aradune Rate Builder — aradune.co`,
              `Date: ${new Date().toISOString().split("T")[0]}`,
            ];
            const blob = new Blob([lines.join("\n")], {type:"text/plain"});
            const a = document.createElement("a");
            const url = URL.createObjectURL(blob);
            a.href = url;
            a.download = `rate_calc_${selectedCode.code}_${new Date().toISOString().split("T")[0]}.txt`;
            a.click();
            setTimeout(() => URL.revokeObjectURL(url), 1000);
          }} style={{ padding:"6px 16px",background:cB,color:WH,border:"none",borderRadius:6,fontSize:11,fontWeight:600,cursor:"pointer" }}>
            Export Calculation
          </button>
          {selectedCode.stateRates.length > 0 && <button onClick={() => {
            downloadCSV(`state_rates_${selectedCode.code}.csv`,
              ["State","Medicaid Rate","Source","Calculated Rate","Change","Change %"],
              selectedCode.stateRates.map((s: StateRate)=>[s.name,s.rate.toFixed(2),s.rateSource==="fee_schedule"?"Fee Schedule":"T-MSIS",result.rate.toFixed(2),(result.rate-s.rate).toFixed(2),((result.rate/s.rate-1)*100).toFixed(1)])
            );
          }} style={{ padding:"6px 16px",background:SF,color:A,border:`1px solid ${BD}`,borderRadius:6,fontSize:11,cursor:"pointer" }}>
            Export State Comparison
          </button>}
          <button onClick={() => {
            if (!isPro) { setGateFeature("PDF Report"); setShowGate(true); return; }
            import("../utils/rateBuilderPdf").then(m => m.generateRateBuilderPdf({
              code: selectedCode.code, desc: selectedCode.desc, medicareRate: selectedCode.medicareRate,
              methodology: curMethod?.name || "", formula: result.formula, components: result.components,
              rate: result.rate, stateRates: selectedCode.stateRates, nStates: selectedCode.nStates,
            }));
          }} style={{ padding:"6px 16px",background:SF,color:A,border:`1px solid ${BD}`,borderRadius:6,fontSize:11,cursor:"pointer",display:"inline-flex",alignItems:"center" }}>
            Export PDF<ProBadge />
          </button>
          <button onClick={() => {
            if (!isPro) { setGateFeature("Excel Export"); setShowGate(true); return; }
            import("../utils/rateBuilderXlsx").then(m => m.generateRateBuilderXlsx({
              code: selectedCode.code, desc: selectedCode.desc, medicareRate: selectedCode.medicareRate,
              methodology: curMethod?.name || "", formula: result.formula, components: result.components,
              rate: result.rate, stateRates: selectedCode.stateRates, nStates: selectedCode.nStates,
            }));
          }} style={{ padding:"6px 16px",background:SF,color:A,border:`1px solid ${BD}`,borderRadius:6,fontSize:11,cursor:"pointer",display:"inline-flex",alignItems:"center" }}>
            Export XLSX<ProBadge />
          </button>
        </div>
      </Card>}

      {/* State Rate Comparison */}
      {result && selectedCode && selectedCode.stateRates.length > 0 && <Card>
        <CH t={`${selectedCode.code} by State vs Your Rate`} b={`$${result.rate.toFixed(2)} calculated`}/>
        <div style={{ padding:"6px 14px 10px",maxHeight:400,overflowY:"auto" }}>
          <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
            <thead><tr style={{ borderBottom:`2px solid ${BD}`,position:"sticky",top:0,background:WH }}>
              {["State","Medicaid Rate","Src","Your Rate","Difference","% Change"].map(h=>(
                <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {selectedCode.stateRates.map((s: StateRate) => {
                const diff = result.rate - s.rate;
                const pct = ((result.rate / s.rate) - 1) * 100;
                return (
                  <tr key={s.st} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:500 }}>{s.name}</td>
                    <td style={{ fontFamily:FM }}>${s.rate.toFixed(2)}</td>
                    <td><span style={{ fontSize:7,padding:"1px 4px",borderRadius:6,background:s.rateSource==="fee_schedule"?"rgba(46,107,74,0.1)":SF,color:s.rateSource==="fee_schedule"?POS:AL,fontWeight:600 }}>{s.rateSource==="fee_schedule"?"FS":"T-MSIS"}</span></td>
                    <td style={{ fontFamily:FM,fontWeight:600,color:cB }}>${result.rate.toFixed(2)}</td>
                    <td style={{ fontFamily:FM,color:diff>=0?POS:NEG }}>{diff>=0?"+":""}${diff.toFixed(2)}</td>
                    <td style={{ fontFamily:FM,color:pct>=0?POS:NEG }}>{pct>=0?"+":""}{pct.toFixed(1)}%</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>}

      {/* Fiscal Impact Simulator */}
      {result && selectedCode && <Card accent={cO}>
        <CH t="Fiscal Impact Simulator" b="What if this rate applied?" r="T-MSIS CY2023"/>
        <div style={{ padding:"6px 14px 14px" }}>
          <div style={{ display:"flex",alignItems:"center",gap:8,marginBottom:10 }}>
            <span style={{ fontSize:10,fontWeight:600,color:AL }}>State:</span>
            <select value={fiscalState} onChange={e => setFiscalState(e.target.value)}
              style={{ padding:"4px 8px",border:`1px solid ${BD}`,borderRadius:6,fontSize:11,fontFamily:FM }}>
              {Object.entries(STATE_NAMES).map(([s,n]) => <option key={s} value={s}>{s} — {n}</option>)}
            </select>
          </div>
          {!codeSpending || codeSpending.claims <= 0 ? (
            <div style={{ fontSize:11,color:AL,padding:"8px 0" }}>No T-MSIS spending data for {selectedCode.code} in {STATE_NAMES[fiscalState] ?? fiscalState}</div>
          ) : fiscalSim ? (
            <div>
              <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",gap:4,marginBottom:10 }}>
                <Met l="Current Annual" v={`$${(codeSpending.paid/1e6).toFixed(1)}M`} sub={`${(codeSpending.claims/1e3).toFixed(0)}K claims`}/>
                <Met l="Current Rate" v={`$${fiscalSim.curRate.toFixed(2)}`} sub="effective rate"/>
                <Met l="Your Rate" v={`$${result.rate.toFixed(2)}`} cl={cB} sub={curMethod?.name}/>
                <Met l="New Spending" v={`$${(fiscalSim.newSpending/1e6).toFixed(1)}M`} cl={fiscalSim.impact>=0?NEG:POS}/>
              </div>
              <div style={{ padding:"10px 12px",background:fiscalSim.impact>=0?"#FEE2E2":"#E8F5E9",borderRadius:8,fontSize:12 }}>
                <span style={{ fontWeight:700,color:fiscalSim.impact>=0?NEG:POS }}>
                  {fiscalSim.impact>=0?"+":" "}{(fiscalSim.impact/1e6).toFixed(2)}M
                </span>
                <span style={{ color:AL }}> annual impact </span>
                <span style={{ fontFamily:FM,color:fiscalSim.impact>=0?NEG:POS }}>
                  ({fiscalSim.pctChange>=0?"+":""}{fiscalSim.pctChange.toFixed(1)}%)
                </span>
                <span style={{ color:AL }}> — {STATE_NAMES[fiscalState]} {selectedCode.code}</span>
              </div>
            </div>
          ) : null}
        </div>
      </Card>}

      {/* AI Upsell */}
      <Card>
        <div style={{ padding:"16px",background:"linear-gradient(135deg,rgba(46,107,74,0.04),rgba(196,89,10,0.04))",textAlign:"center" }}>
          <div style={{ fontSize:13,fontWeight:600,color:A,marginBottom:4 }}>Need help with complex rate-setting?</div>
          <div style={{ fontSize:11,color:AL,lineHeight:1.6,maxWidth:500,margin:"0 auto" }}>
            New code families, code splits, cross-state methodology research, fiscal impact analysis with utilization modeling — the <b>Policy Analyst</b> tier uses AI grounded in Aradune's complete dataset to handle the hard cases.
          </div>
          <div style={{ marginTop:8 }}>
            <span style={{ fontFamily:FM,fontSize:10,color:cO,fontWeight:600 }}>Coming soon</span>
          </div>
        </div>
      </Card>

      {/* About */}
      <Card><CH t="About Rate Builder"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Purpose:</b> A free, transparent rate calculation tool for Medicaid analysts, advocates, and researchers. Enter any HCPCS code, select a standard rate-setting methodology, and get a calculated rate with full documentation.<br/>
        <b>Data sources:</b> CY2026 Medicare Physician Fee Schedule (RVUs and rates), T-MSIS actual-paid rates by state. Fee schedule rates from individual states will be added as the database is built.<br/>
        <b>Methodologies:</b> RBRVS % of Medicare (most common), custom conversion factor (used by FL, TX, etc.), peer state median, or manual flat rate. Each methodology documents the formula and components for audit trail purposes.<br/>
        <b>Exports:</b> Every calculation can be exported as a text file documenting the code, methodology, formula, inputs, and result. State comparisons export as CSV.<br/>
        <b>Limitations:</b> This tool calculates rates based on your selected methodology and inputs. It does not account for state-specific modifiers, place-of-service adjustments, specialty-specific rates, or managed care negotiations. T-MSIS rates are blended averages and may not reflect fee schedule rates.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Rate Builder v1.0 · Free Tool · CY2026 Medicare PFS + T-MSIS</div>
      <ProGateModal feature={gateFeature} open={showGate} onClose={() => setShowGate(false)} />
    </div>
  );
}
