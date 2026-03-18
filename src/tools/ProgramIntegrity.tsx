import React, { useState, useMemo, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, LineChart, Line } from "recharts";
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

// ── Data Shape Interfaces ─────────────────────────────────────────────
interface LeieStateRow {
  state_code: string;
  total_exclusions: number;
  individual_count: number;
  entity_count: number;
  unique_npis: number;
}

interface LeieData {
  by_state: LeieStateRow[];
  count: number;
}

interface OpenPayStateRow {
  state_code: string;
  total_payments: number;
  total_amount: number;
  avg_payment: number;
  unique_physicians: number;
  unique_companies: number;
}

interface OpenPayTypeRow {
  payment_nature: string;
  total_payments: number;
  total_amount: number;
}

interface OpenPayData {
  by_state: OpenPayStateRow[];
  by_payment_type: OpenPayTypeRow[];
  count: number;
}

interface MfcuRow {
  state_code: string;
  state_name: string;
  fiscal_year: number;
  total_investigations: number;
  fraud_investigations: number;
  abuse_neglect_investigations: number;
  total_convictions: number;
  fraud_convictions: number;
  abuse_neglect_convictions: number;
  total_recoveries: number;
  total_criminal_recoveries: number;
  civil_recoveries_global: number;
  civil_settlements_judgments: number;
  mfcu_grant_expenditures: number;
  total_medicaid_expenditures: number;
  staff_on_board: number;
  recovery_pct_of_medicaid: number | null;
  roi_ratio: number | null;
}

interface MfcuData {
  rows: MfcuRow[];
  count: number;
}

interface PermRow {
  program: string;
  year: number;
  overall_rate_pct: number;
  ffs_rate_pct: number;
  mc_rate_pct: number;
  eligibility_rate_pct: number;
  estimated_improper_payments_billions: number;
}

interface PermData {
  rows: PermRow[];
  count: number;
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

const f$ = (v: number | null | undefined): string => {
  if (v==null||isNaN(v)||!isFinite(v)) return "$0";
  const abs=Math.abs(v),sign=v<0?"-":"";
  if(abs>=1e9)return `${sign}$${(abs/1e9).toFixed(1)}B`;
  if(abs>=1e6)return `${sign}$${(abs/1e6).toFixed(1)}M`;
  if(abs>=1e3)return `${sign}$${abs.toLocaleString(undefined,{maximumFractionDigits:0})}`;
  if(abs<10)return `${sign}$${abs.toFixed(2)}`;
  return `${sign}$${abs.toFixed(0)}`;
};
const fN = (v: number | null | undefined): string => {
  if(v==null||isNaN(v)||!isFinite(v)) return "0";
  if(v>=1e6)return `${(v/1e6).toFixed(1)}M`;
  if(v>=1e3)return `${(v/1e3).toFixed(1)}K`;
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

type TabKey = "leie" | "open-payments" | "mfcu-perm";

// ── Main Component ──────────────────────────────────────────────────────
export default function ProgramIntegrity() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<TabKey>("leie");
  const [selectedState, setSelectedState] = useState<string | null>(null);
  const [leieData, setLeieData] = useState<LeieData | null>(null);
  const [openPayData, setOpenPayData] = useState<OpenPayData | null>(null);
  const [mfcuData, setMfcuData] = useState<MfcuData | null>(null);
  const [permData, setPermData] = useState<PermData | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [leie, openPay, mfcu, perm] = await Promise.all([
          fetch(`${API_BASE}/api/integrity/leie-summary`).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(`${API_BASE}/api/integrity/open-payments-summary`).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(`${API_BASE}/api/integrity/mfcu`).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(`${API_BASE}/api/integrity/perm`).then(r => r.ok ? r.json() : null).catch(() => null),
        ]);
        if (cancelled) return;
        if (leie) setLeieData(leie);
        if (openPay) setOpenPayData(openPay);
        if (mfcu) setMfcuData(mfcu);
        if (perm) setPermData(perm);
        if (!leie && !openPay && !mfcu && !perm) {
          setLoadError("Could not load program integrity data. The API may be unavailable.");
        }
      } catch (e) {
        console.error(e);
        if (!cancelled) setLoadError("Failed to load program integrity data.");
      }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // ── LEIE derived data ──
  const leieChart = useMemo(() => {
    if (!leieData?.by_state) return [];
    return leieData.by_state
      .filter(r => r.state_code && STATE_NAMES[r.state_code])
      .sort((a, b) => b.total_exclusions - a.total_exclusions)
      .slice(0, 30)
      .map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code }));
  }, [leieData]);

  const leieNational = useMemo(() => {
    if (!leieData?.by_state) return null;
    const total = leieData.by_state.reduce((s, r) => s + r.total_exclusions, 0);
    const individuals = leieData.by_state.reduce((s, r) => s + r.individual_count, 0);
    const entities = leieData.by_state.reduce((s, r) => s + r.entity_count, 0);
    const npis = leieData.by_state.reduce((s, r) => s + r.unique_npis, 0);
    const stateCount = leieData.by_state.filter(r => STATE_NAMES[r.state_code]).length;
    return { total, individuals, entities, npis, stateCount };
  }, [leieData]);

  // ── Open Payments derived data ──
  const opChart = useMemo(() => {
    if (!openPayData?.by_state) return [];
    return openPayData.by_state
      .filter(r => r.state_code && STATE_NAMES[r.state_code])
      .sort((a, b) => b.total_amount - a.total_amount)
      .slice(0, 30)
      .map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code, total_M: r.total_amount / 1e6 }));
  }, [openPayData]);

  const opNational = useMemo(() => {
    if (!openPayData?.by_state) return null;
    const totalAmt = openPayData.by_state.reduce((s, r) => s + safe(r.total_amount), 0);
    const totalPayments = openPayData.by_state.reduce((s, r) => s + safe(r.total_payments), 0);
    const totalPhysicians = openPayData.by_state.reduce((s, r) => s + safe(r.unique_physicians), 0);
    const stateCount = openPayData.by_state.filter(r => STATE_NAMES[r.state_code]).length;
    return { totalAmt, totalPayments, totalPhysicians, stateCount };
  }, [openPayData]);

  // ── MFCU derived data ──
  const mfcuChart = useMemo(() => {
    if (!mfcuData?.rows) return [];
    return mfcuData.rows
      .filter(r => r.state_code && STATE_NAMES[r.state_code] && r.total_recoveries > 0)
      .sort((a, b) => b.total_recoveries - a.total_recoveries)
      .slice(0, 30)
      .map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code, recoveries_M: r.total_recoveries / 1e6 }));
  }, [mfcuData]);

  const mfcuNational = useMemo(() => {
    if (!mfcuData?.rows) return null;
    const totalRecoveries = mfcuData.rows.reduce((s, r) => s + safe(r.total_recoveries), 0);
    const totalInvestigations = mfcuData.rows.reduce((s, r) => s + safe(r.total_investigations), 0);
    const totalConvictions = mfcuData.rows.reduce((s, r) => s + safe(r.total_convictions), 0);
    const totalStaff = mfcuData.rows.reduce((s, r) => s + safe(r.staff_on_board), 0);
    const stateCount = mfcuData.rows.filter(r => STATE_NAMES[r.state_code]).length;
    return { totalRecoveries, totalInvestigations, totalConvictions, totalStaff, stateCount };
  }, [mfcuData]);

  // ── PERM derived data ──
  const permMedicaid = useMemo(() => {
    if (!permData?.rows) return [];
    return permData.rows.filter(r => r.program === "Medicaid").sort((a, b) => a.year - b.year);
  }, [permData]);

  const permChip = useMemo(() => {
    if (!permData?.rows) return [];
    return permData.rows.filter(r => r.program === "CHIP").sort((a, b) => a.year - b.year);
  }, [permData]);

  const permLatest = useMemo(() => {
    if (!permMedicaid.length) return null;
    return permMedicaid[permMedicaid.length - 1];
  }, [permMedicaid]);


  if (loading) return <LoadingBar text="Loading program integrity data" detail="LEIE, Open Payments, MFCU, PERM" />;

  if (loadError) return (
    <div style={{ maxWidth:640,margin:"0 auto",padding:"40px 16px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>
      <Card><div style={{ padding:24,textAlign:"center" }}>
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
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(220,38,38,0.1)",color:"#DC2626",fontWeight:600 }}>OIG / CMS</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>LEIE + Open Payments + MFCU + PERM</span>
          </div>
          <div style={{ display:"flex",gap:8,alignItems:"center" }}>
            <button onClick={() => openIntelligence({ summary: "User is viewing Program Integrity data -- LEIE exclusions, Open Payments, MFCU stats, and PERM error rates" })} style={{
              padding:"5px 12px",borderRadius:6,border:"none",
              background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
            }}>Ask Aradune</button>
          </div>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(220,38,38,0.03)",borderLeft:"3px solid #DC2626" }}>
        <span style={{ fontWeight:700,color:A }}>Program Integrity.</span> OIG List of Excluded Individuals/Entities (LEIE), CMS Open Payments (industry payments to physicians), Medicaid Fraud Control Unit (MFCU) statistics, and Payment Error Rate Measurement (PERM) improper payment rates. These datasets support fraud/waste/abuse monitoring and compliance oversight.
      </div></Card>

      {/* Tab pills + state selector */}
      <div style={{ display:"flex",gap:4,margin:"10px 0",flexWrap:"wrap",alignItems:"center" }}>
        <Pill on={tab==="leie"} onClick={()=>setTab("leie")}>Exclusions (LEIE)</Pill>
        <Pill on={tab==="open-payments"} onClick={()=>setTab("open-payments")}>Open Payments</Pill>
        <Pill on={tab==="mfcu-perm"} onClick={()=>setTab("mfcu-perm")}>MFCU & PERM</Pill>
        <select value={selectedState || ""} onChange={e => setSelectedState(e.target.value || null)}
          style={{ fontSize:12,padding:"4px 8px",borderRadius:4,border:`1px solid ${BD}`,fontFamily:FM,marginLeft:"auto" }}>
          <option value="">All States</option>
          {Object.entries(STATE_NAMES).sort((a, b) => a[1].localeCompare(b[1])).map(([code, name]) =>
            <option key={code} value={code}>{name}</option>
          )}
        </select>
        {selectedState && <Pill on={false} onClick={() => setSelectedState(null)}>Clear</Pill>}
      </div>

      {selectedState && <StateContextBar stateCode={selectedState} mode="compact" />}

      {/* ═══════════════════════════════════════════════════════════════
           TAB 1: LEIE Exclusions
         ═══════════════════════════════════════════════════════════════ */}
      {tab === "leie" && <>
        {/* National summary metrics */}
        {leieNational && <Card accent={NEG}>
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>OIG LEIE Exclusion List</div>
            <div style={{ fontSize:10,color:AL }}>Currently excluded individuals and entities across {leieNational.stateCount} states</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l="Total Exclusions" v={fN(leieNational.total)} cl={NEG} />
            <Met l="Individuals" v={fN(leieNational.individuals)} sub={leieNational.total ? `${((leieNational.individuals/leieNational.total)*100).toFixed(0)}% of total` : ""} />
            <Met l="Entities" v={fN(leieNational.entities)} sub={leieNational.total ? `${((leieNational.entities/leieNational.total)*100).toFixed(0)}% of total` : ""} />
            <Met l="With NPI" v={fN(leieNational.npis)} sub="Linked to provider NPIs" />
          </div>
        </Card>}

        {/* LEIE bar chart: top states */}
        {leieChart.length > 0 && <Card x>
          <CH t="Exclusions by State" b={`Top ${leieChart.length} states`} r="OIG LEIE" />
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="leie-by-state">
            <ResponsiveContainer width="100%" height={Math.max(280, leieChart.length * 16)}>
              <BarChart data={leieChart} layout="vertical" margin={{ left:60,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="state_code" tick={{ fill:A,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} width={28} />
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>) => (
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Total exclusions: <b>{fN(d.total_exclusions as number)}</b></div>
                    <div>Individuals: {fN(d.individual_count as number)}</div>
                    <div>Entities: {fN(d.entity_count as number)}</div>
                    <div>Unique NPIs: {fN(d.unique_npis as number)}</div>
                  </div>
                )} />} />
                <Bar dataKey="total_exclusions" barSize={10} radius={[0,3,3,0]}>
                  {leieChart.map((_, i) => <Cell key={i} fill={NEG} opacity={0.7} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
          </div>
          <div style={{ padding:"4px 14px 10px",display:"flex",justifyContent:"flex-end" }}>
            <ExportBtn label="Export LEIE CSV" onClick={() => {
              if (!leieData?.by_state) return;
              downloadCSV("leie_exclusions_by_state.csv",
                ["State","Total Exclusions","Individuals","Entities","Unique NPIs"],
                leieData.by_state.map(r => [STATE_NAMES[r.state_code]||r.state_code,r.total_exclusions,r.individual_count,r.entity_count,r.unique_npis])
              );
            }} />
          </div>
        </Card>}

        {/* LEIE table */}
        {leieData?.by_state && leieData.by_state.length > 0 && <Card x>
          <CH t="All States" b={`${leieData.by_state.length} jurisdictions`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Total","Individuals","Entities","Unique NPIs"].map(h => (
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {leieData.by_state.filter(r => STATE_NAMES[r.state_code]).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:cB,cursor:"pointer",textDecoration:"underline",textDecorationColor:`${cB}40` }} onClick={() => setSelectedState(r.state_code)}>{STATE_NAMES[r.state_code]||r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,color:NEG,fontWeight:600 }}>{r.total_exclusions.toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.individual_count.toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.entity_count.toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.unique_npis.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* ═══════════════════════════════════════════════════════════════
           TAB 2: Open Payments
         ═══════════════════════════════════════════════════════════════ */}
      {tab === "open-payments" && <>
        {/* National summary */}
        {opNational && <Card accent={WARN}>
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>CMS Open Payments</div>
            <div style={{ fontSize:10,color:AL }}>Industry payments to physicians across {opNational.stateCount} states (Program Year 2024)</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l="Total Amount" v={f$(opNational.totalAmt)} cl={WARN} />
            <Met l="Total Payments" v={fN(opNational.totalPayments)} sub="Individual payment records" />
            <Met l="Physicians" v={fN(opNational.totalPhysicians)} sub="Unique recipients" />
            <Met l="States" v={`${opNational.stateCount}`} />
          </div>
        </Card>}

        {/* Payment types breakdown */}
        {openPayData?.by_payment_type && openPayData.by_payment_type.length > 0 && <Card x>
          <CH t="Payments by Type" b="Top payment categories" r="PGYR 2024" />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["Payment Type","Total Payments","Total Amount"].map(h => (
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {openPayData.by_payment_type.map((r, i) => (
                  <tr key={i} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",color:A,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.payment_nature || "Other"}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{safe(r.total_payments).toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:WARN }}>{f$(r.total_amount)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}

        {/* Open Payments bar chart */}
        {opChart.length > 0 && <Card x>
          <CH t="Industry Payments by State" b={`Top ${opChart.length} states`} r="CMS Open Payments" />
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="open-payments-by-state">
            <ResponsiveContainer width="100%" height={Math.max(280, opChart.length * 16)}>
              <BarChart data={opChart} layout="vertical" margin={{ left:60,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number) => `$${(v).toFixed(0)}M`} />
                <YAxis type="category" dataKey="state_code" tick={{ fill:A,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} width={28} />
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>) => (
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Total amount: <b>{f$(d.total_amount as number)}</b></div>
                    <div>Payments: {fN(d.total_payments as number)}</div>
                    <div>Physicians: {fN(d.unique_physicians as number)}</div>
                    <div>Companies: {fN(d.unique_companies as number)}</div>
                    <div>Avg payment: {f$(d.avg_payment as number)}</div>
                  </div>
                )} />} />
                <Bar dataKey="total_M" barSize={10} radius={[0,3,3,0]}>
                  {opChart.map((_, i) => <Cell key={i} fill={WARN} opacity={0.7} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
          </div>
          <div style={{ padding:"4px 14px 10px",display:"flex",justifyContent:"flex-end" }}>
            <ExportBtn label="Export Open Payments CSV" onClick={() => {
              if (!openPayData?.by_state) return;
              downloadCSV("open_payments_by_state.csv",
                ["State","Total Payments","Total Amount","Avg Payment","Unique Physicians","Unique Companies"],
                openPayData.by_state.map(r => [STATE_NAMES[r.state_code]||r.state_code,r.total_payments,r.total_amount,r.avg_payment,r.unique_physicians,r.unique_companies])
              );
            }} />
          </div>
        </Card>}
      </>}

      {/* ═══════════════════════════════════════════════════════════════
           TAB 3: MFCU & PERM
         ═══════════════════════════════════════════════════════════════ */}
      {tab === "mfcu-perm" && <>
        {/* MFCU national summary */}
        {mfcuNational && <Card accent={POS}>
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>Medicaid Fraud Control Units (MFCU)</div>
            <div style={{ fontSize:10,color:AL }}>FY 2024 activity across {mfcuNational.stateCount} state MFCUs</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l="Total Recoveries" v={f$(mfcuNational.totalRecoveries)} cl={POS} />
            <Met l="Investigations" v={fN(mfcuNational.totalInvestigations)} />
            <Met l="Convictions" v={fN(mfcuNational.totalConvictions)} />
            <Met l="Staff" v={fN(mfcuNational.totalStaff)} sub="Investigators on board" />
          </div>
        </Card>}

        {/* MFCU recoveries chart */}
        {mfcuChart.length > 0 && <Card x>
          <CH t="MFCU Recoveries by State" b={`Top ${mfcuChart.length} states`} r="FY 2024" />
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="mfcu-recoveries">
            <ResponsiveContainer width="100%" height={Math.max(280, mfcuChart.length * 16)}>
              <BarChart data={mfcuChart} layout="vertical" margin={{ left:60,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number) => `$${v.toFixed(0)}M`} />
                <YAxis type="category" dataKey="state_code" tick={{ fill:A,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} width={28} />
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>) => (
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Recoveries: <b>{f$(d.total_recoveries as number)}</b></div>
                    <div>Investigations: {fN(d.total_investigations as number)}</div>
                    <div>Convictions: {fN(d.total_convictions as number)}</div>
                    <div>Staff: {fN(d.staff_on_board as number)}</div>
                    {(d.roi_ratio as number) != null && <div>ROI: {(d.roi_ratio as number).toFixed(1)}x grant expenditure</div>}
                  </div>
                )} />} />
                <Bar dataKey="recoveries_M" barSize={10} radius={[0,3,3,0]}>
                  {mfcuChart.map((_, i) => <Cell key={i} fill={POS} opacity={0.7} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
          </div>
          <div style={{ padding:"4px 14px 10px",display:"flex",justifyContent:"flex-end" }}>
            <ExportBtn label="Export MFCU CSV" onClick={() => {
              if (!mfcuData?.rows) return;
              downloadCSV("mfcu_stats_fy2024.csv",
                ["State","Investigations","Fraud Inv.","Abuse/Neglect Inv.","Convictions","Recoveries","Staff","ROI"],
                mfcuData.rows.map(r => [STATE_NAMES[r.state_code]||r.state_code,r.total_investigations,r.fraud_investigations,r.abuse_neglect_investigations,r.total_convictions,r.total_recoveries,r.staff_on_board,r.roi_ratio?.toFixed(1)])
              );
            }} />
          </div>
        </Card>}

        {/* MFCU detail table */}
        {mfcuData?.rows && mfcuData.rows.length > 0 && <Card x>
          <CH t="MFCU Activity Detail" b={`${mfcuData.rows.length} state MFCUs`} r="FY 2024" />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Investigations","Convictions","Recoveries","Grant Exp.","ROI","Staff"].map(h => (
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {mfcuData.rows.filter(r => STATE_NAMES[r.state_code]).sort((a, b) => b.total_recoveries - a.total_recoveries).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:cB,cursor:"pointer",textDecoration:"underline",textDecorationColor:`${cB}40` }} onClick={() => setSelectedState(r.state_code)}>{STATE_NAMES[r.state_code]||r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.total_investigations.toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.total_convictions.toLocaleString()}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:POS }}>{f$(r.total_recoveries)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{f$(r.mfcu_grant_expenditures)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:r.roi_ratio != null && r.roi_ratio >= 1 ? POS : WARN }}>{r.roi_ratio != null ? `${r.roi_ratio.toFixed(1)}x` : "--"}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.staff_on_board}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}

        {/* PERM Section */}
        {permLatest && <Card accent={NEG}>
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>Payment Error Rate Measurement (PERM)</div>
            <div style={{ fontSize:10,color:AL }}>National rolling 3-year improper payment rates (2020-2025)</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l={`Medicaid Error Rate (${permLatest.year})`} v={`${permLatest.overall_rate_pct}%`} cl={NEG} />
            <Met l="Improper Payments" v={`$${permLatest.estimated_improper_payments_billions.toFixed(1)}B`} cl={NEG} sub={`FY ${permLatest.year} estimate`} />
            <Met l="FFS Rate" v={`${permLatest.ffs_rate_pct}%`} sub="Fee-for-service" />
            <Met l="Eligibility Rate" v={`${permLatest.eligibility_rate_pct}%`} sub="Eligibility errors" />
          </div>
        </Card>}

        {/* PERM trend chart */}
        {permMedicaid.length > 0 && <Card x>
          <CH t="PERM Error Rate Trend" b="Medicaid & CHIP" r="2020-2025" />
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="perm-trend">
            <ResponsiveContainer width="100%" height={260}>
              <LineChart margin={{ left:8,right:16,top:8,bottom:4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="year" type="number" domain={["dataMin","dataMax"]} tick={{ fill:AL,fontSize:9,fontFamily:FM }} axisLine={false} tickLine={false} allowDuplicatedCategory={false} />
                <YAxis tick={{ fill:AL,fontSize:9,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number) => `${v}%`} />
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>) => (
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.program ?? "")} {String(d.year ?? "")}</div>
                    <div>Overall rate: <b>{String(d.overall_rate_pct ?? "")}%</b></div>
                    <div>FFS rate: {String(d.ffs_rate_pct ?? "")}%</div>
                    <div>Eligibility rate: {String(d.eligibility_rate_pct ?? "")}%</div>
                    <div>Improper payments: ${String(d.estimated_improper_payments_billions ?? "")}B</div>
                  </div>
                )} />} />
                <Line data={permMedicaid} type="monotone" dataKey="overall_rate_pct" name="Medicaid" stroke={NEG} strokeWidth={2} dot={{ fill:NEG,r:4 }} />
                <Line data={permChip} type="monotone" dataKey="overall_rate_pct" name="CHIP" stroke={WARN} strokeWidth={2} dot={{ fill:WARN,r:4 }} />
              </LineChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0" }}>
              <span><span style={{ display:"inline-block",width:12,height:2,background:NEG,verticalAlign:"middle",marginRight:4 }}/>Medicaid</span>
              <span><span style={{ display:"inline-block",width:12,height:2,background:WARN,verticalAlign:"middle",marginRight:4 }}/>CHIP</span>
            </div>
          </div>
        </Card>}

        {/* PERM detail table */}
        {permData?.rows && permData.rows.length > 0 && <Card x>
          <CH t="PERM Rates Detail" b={`${permData.rows.length} year-program records`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["Program","Year","Overall Rate","FFS Rate","MC Rate","Eligibility Rate","Improper Payments"].map(h => (
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {permData.rows.sort((a, b) => a.program.localeCompare(b.program) || b.year - a.year).map((r, i) => (
                  <tr key={i} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:r.program==="Medicaid"?NEG:WARN }}>{r.program}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.year}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600 }}>{r.overall_rate_pct}%</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.ffs_rate_pct}%</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.mc_rate_pct}%</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.eligibility_rate_pct}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600 }}>${r.estimated_improper_payments_billions.toFixed(1)}B</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ padding:"4px 14px 10px",display:"flex",justifyContent:"flex-end" }}>
            <ExportBtn label="Export PERM CSV" onClick={() => {
              if (!permData?.rows) return;
              downloadCSV("perm_rates.csv",
                ["Program","Year","Overall Rate %","FFS Rate %","MC Rate %","Eligibility Rate %","Improper Payments ($B)"],
                permData.rows.map(r => [r.program,r.year,r.overall_rate_pct,r.ffs_rate_pct,r.mc_rate_pct,r.eligibility_rate_pct,r.estimated_improper_payments_billions])
              );
            }} />
          </div>
        </Card>}
      </>}

      {/* About / Sources */}
      <Card><CH t="Data Sources & Methodology" /><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>LEIE:</b> OIG List of Excluded Individuals/Entities. Updated monthly. Individuals and entities currently excluded from federally funded health care programs. 82,749 exclusion records across 54 jurisdictions.<br/>
        <b>Open Payments:</b> CMS Open Payments Program Year 2024. Industry payments to physicians, including consulting fees, research grants, food/beverage, travel, and royalties. Aggregated from 15.4M individual payment records to state x specialty x payment type.<br/>
        <b>MFCU:</b> OIG Medicaid Fraud Control Unit Statistical Chart, FY 2024. State-level fraud investigations, convictions, civil settlements, criminal recoveries, and grant expenditures.<br/>
        <b>PERM:</b> CMS Payment Error Rate Measurement program. National rolling 3-year improper payment rates for Medicaid and CHIP, 2020-2025. Includes FFS claims, managed care, and eligibility determination errors.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Program Integrity v1.0 · LEIE + Open Payments + MFCU FY2024 + PERM</div>
    </div>
  );
}
