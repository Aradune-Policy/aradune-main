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
interface CompositeRisk { state_code: string; total_open_payments: number; payment_count: number; exclusion_count: number; total_enrollment: number; open_payments_per_enrollee: number; exclusions_per_100k: number }
interface OpenPayRow { state_code: string; total_amount: number; payment_count: number; unique_physicians: number; unique_companies: number; avg_per_physician: number }
interface EnforcementRow { state_code: string; fiscal_year: number; cases_opened: number; convictions: number; civil_settlements: number; recoveries_total: number; program_expenditures: number; roi: number }
interface PermRow { fiscal_year: number; improper_payment_rate_pct: number; ffs_rate_pct: number; managed_care_rate_pct: number; eligibility_error_rate_pct: number }

type Tab = "composite" | "financial" | "enforcement" | "accuracy";

export default function IntegrityRisk() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();

  const [tab, setTab] = useState<Tab>("composite");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [compositeData, setCompositeData] = useState<CompositeRisk[]>([]);
  const [openPayData, setOpenPayData] = useState<OpenPayRow[]>([]);
  const [enforcementData, setEnforcementData] = useState<EnforcementRow[]>([]);
  const [permData, setPermData] = useState<PermRow[]>([]);

  useEffect(() => {
    setLoading(true);
    setLoadError(null);
    const endpoints: Record<Tab, string> = {
      composite: "/api/research/integrity-risk/composite",
      financial: "/api/research/integrity-risk/open-payments",
      enforcement: "/api/research/integrity-risk/enforcement",
      accuracy: "/api/research/integrity-risk/perm",
    };
    fetch(`${API_BASE}${endpoints[tab]}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        const rows = Array.isArray(d) ? d : d.rows || d.data || [];
        if (tab === "composite") setCompositeData(rows);
        else if (tab === "financial") setOpenPayData(rows);
        else if (tab === "enforcement") setEnforcementData(rows);
        else setPermData(rows);
      })
      .catch(e => setLoadError(e.message))
      .finally(() => setLoading(false));
  }, [tab]);

  // -- Composite chart --
  const compositeChart = useMemo(() =>
    [...compositeData].sort((a, b) => b.open_payments_per_enrollee - a.open_payments_per_enrollee)
      .slice(0, 30).map(r => ({
        name: r.state_code, perEnrollee: r.open_payments_per_enrollee, excl: r.exclusions_per_100k,
      })), [compositeData]);

  const compositeStats = useMemo(() => {
    if (!compositeData.length) return { totalPayments: 0, totalExcl: 0, avgPerEnrollee: 0 };
    const totalPayments = compositeData.reduce((s, r) => s + r.total_open_payments, 0);
    const totalExcl = compositeData.reduce((s, r) => s + r.exclusion_count, 0);
    const avgPerEnrollee = compositeData.reduce((s, r) => s + r.open_payments_per_enrollee, 0) / compositeData.length;
    return { totalPayments, totalExcl, avgPerEnrollee };
  }, [compositeData]);

  // -- Open Payments chart --
  const openPayChart = useMemo(() =>
    [...openPayData].sort((a, b) => b.total_amount - a.total_amount)
      .slice(0, 30).map(r => ({
        name: r.state_code, total: r.total_amount, avgPhysician: r.avg_per_physician,
      })), [openPayData]);

  const openPayStats = useMemo(() => {
    if (!openPayData.length) return { totalAmount: 0, avgPerPhys: 0, uniqueCompanies: 0 };
    const totalAmount = openPayData.reduce((s, r) => s + r.total_amount, 0);
    const totalPhys = openPayData.reduce((s, r) => s + r.unique_physicians, 0);
    const avgPerPhys = totalPhys > 0 ? totalAmount / totalPhys : 0;
    const uniqueCompanies = Math.max(...openPayData.map(r => r.unique_companies));
    return { totalAmount, avgPerPhys, uniqueCompanies };
  }, [openPayData]);

  // -- Enforcement chart --
  const latestFY = useMemo(() => {
    if (!enforcementData.length) return 0;
    return Math.max(...enforcementData.map(r => r.fiscal_year));
  }, [enforcementData]);

  const enforcementFiltered = useMemo(() =>
    enforcementData.filter(r => r.fiscal_year === latestFY), [enforcementData, latestFY]);

  const enforcementChart = useMemo(() =>
    [...enforcementFiltered].sort((a, b) => b.roi - a.roi)
      .slice(0, 25).map(r => ({
        name: r.state_code, roi: r.roi, recoveries: r.recoveries_total,
      })), [enforcementFiltered]);

  // -- PERM chart --
  const permChart = useMemo(() =>
    [...permData].sort((a, b) => a.fiscal_year - b.fiscal_year).map(r => ({
      name: `FY${r.fiscal_year}`, overall: r.improper_payment_rate_pct, ffs: r.ffs_rate_pct,
      mc: r.managed_care_rate_pct, elig: r.eligibility_error_rate_pct,
    })), [permData]);

  const latestPerm = useMemo(() => {
    if (!permData.length) return null;
    return [...permData].sort((a, b) => b.fiscal_year - a.fiscal_year)[0];
  }, [permData]);

  const permTrend = useMemo(() => {
    if (permData.length < 2) return "stable";
    const sorted = [...permData].sort((a, b) => a.fiscal_year - b.fiscal_year);
    const recent = sorted[sorted.length - 1].improper_payment_rate_pct;
    const prior = sorted[sorted.length - 2].improper_payment_rate_pct;
    return recent > prior ? "increasing" : recent < prior ? "decreasing" : "stable";
  }, [permData]);

  const tabs: { key: Tab; label: string }[] = [
    { key: "composite", label: "Composite Index" },
    { key: "financial", label: "Financial Influence" },
    { key: "enforcement", label: "Enforcement" },
    { key: "accuracy", label: "Payment Accuracy" },
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
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(220,38,38,0.1)",color:"#DC2626",fontWeight:600 }}>RESEARCH</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>Open Payments + LEIE + PERM + MFCU</span>
          </div>
          <button onClick={() => openIntelligence({ summary: `User is viewing Program Integrity Risk Index -- ${tab} tab. ${compositeData.length} states with composite risk data.` })} style={{
            padding:"5px 12px",borderRadius:6,border:"none",background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
          }}>Ask Aradune</button>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(220,38,38,0.03)",borderLeft:"3px solid #DC2626" }}>
        <span style={{ fontWeight:700,color:A }}>Program Integrity Risk Index.</span> Composite state-level integrity risk scoring combining financial influence patterns, provider exclusions, payment error rates, and fraud enforcement capacity.
      </div></Card>

      {/* Tab pills */}
      <div style={{ display:"flex",gap:6,marginTop:10,marginBottom:12,flexWrap:"wrap" }}>
        {tabs.map(t => <Pill key={t.key} active={tab===t.key} label={t.label} onClick={() => setTab(t.key)} />)}
      </div>

      {/* Composite Index */}
      {tab === "composite" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Total Open Payments" v={fmtD(compositeStats.totalPayments)} cl={NEG} /></Card>
          <Card accent={WARN}><Met l="Total Exclusions" v={fmtK(compositeStats.totalExcl)} /></Card>
          <Card accent={POS}><Met l="Avg Payments / Enrollee" v={fmtD(compositeStats.avgPerEnrollee)} /></Card>
        </div>

        {compositeChart.length > 0 ? <Card>
          <CH t="States by Open Payments per Enrollee" b="Top 30, with exclusions per 100K overlay" r={`${compositeData.length} states`} />
          <ChartActions filename="integrity-composite">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={compositeChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis yAxisId="left" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                <YAxis yAxisId="right" orientation="right" tick={{ fontSize:9,fill:AL }} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:NEG }}>Payments/Enrollee: {fmtD(d.perEnrollee)}</div>
                    <div style={{ color:WARN }}>Exclusions/100K: {fmt(d.excl)}</div>
                  </div>;
                }} />
                <Bar yAxisId="left" dataKey="perEnrollee" name="$/Enrollee" fill={NEG} radius={[3,3,0,0]} />
                <Bar yAxisId="right" dataKey="excl" name="Excl/100K" fill={WARN} radius={[3,3,0,0]} opacity={0.5} />
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No composite risk data available</div></Card>}

        {compositeData.length > 0 && <Card>
          <CH t="Composite Risk Detail" b={`${compositeData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Open Payments","Payment Count","Exclusions","Enrollment","$/Enrollee","Excl/100K"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...compositeData].sort((a,b) => b.open_payments_per_enrollee - a.open_payments_per_enrollee).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.total_open_payments)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.payment_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:WARN }}>{fmtK(r.exclusion_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.total_enrollment)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:NEG }}>{fmtD(r.open_payments_per_enrollee)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.exclusions_per_100k>50?NEG:AL }}>{fmt(r.exclusions_per_100k)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Financial Influence */}
      {tab === "financial" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Total Industry Payments" v={fmtD(openPayStats.totalAmount)} cl={NEG} sub="PY2024 Open Payments" /></Card>
          <Card accent={WARN}><Met l="Avg per Physician" v={fmtD(openPayStats.avgPerPhys)} /></Card>
          <Card accent={POS}><Met l="Max Unique Companies" v={fmtK(openPayStats.uniqueCompanies)} sub="Companies making payments" /></Card>
        </div>

        {openPayChart.length > 0 ? <Card>
          <CH t="Top 30 States by Total Industry Payments" r={`${openPayData.length} states`} />
          <ChartActions filename="open-payments-bar">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={openPayChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:NEG }}>Total: {fmtD(d.total)}</div>
                    <div style={{ color:AL }}>Avg/Physician: {fmtD(d.avgPhysician)}</div>
                  </div>;
                }} />
                <Bar dataKey="total" name="Total Amount" fill={NEG} radius={[3,3,0,0]}>
                  {openPayChart.map((_, i) => <Cell key={i} fill={i < 5 ? NEG : i < 15 ? WARN : cB} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No open payments data available</div></Card>}

        {openPayData.length > 0 && <Card>
          <CH t="Financial Influence Detail" b={`${openPayData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Total Amount","Payment Count","Physicians","Companies","Avg/Physician"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...openPayData].sort((a,b) => b.total_amount - a.total_amount).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:NEG }}>{fmtD(r.total_amount)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.payment_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.unique_physicians)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.unique_companies)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.avg_per_physician)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Enforcement */}
      {tab === "enforcement" && <>
        {latestFY > 0 && <div style={{ fontSize:10,color:AL,fontFamily:FM,marginBottom:8 }}>Showing FY{latestFY} data ({enforcementFiltered.length} states)</div>}

        {enforcementChart.length > 0 ? <Card>
          <CH t="Enforcement ROI by State" b={`FY${latestFY} -- Recoveries / Expenditures`} r={`${enforcementFiltered.length} states`} />
          <ChartActions filename="enforcement-roi">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={enforcementChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v.toFixed(0)}x`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:POS }}>ROI: {fmt(d.roi)}x</div>
                    <div style={{ color:AL }}>Recoveries: {fmtD(d.recoveries)}</div>
                  </div>;
                }} />
                <Bar dataKey="roi" name="ROI" radius={[3,3,0,0]}>
                  {enforcementChart.map((d, i) => <Cell key={i} fill={d.roi > 3 ? POS : d.roi > 1 ? WARN : NEG} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No enforcement data available</div></Card>}

        {enforcementFiltered.length > 0 && <Card>
          <CH t="Enforcement Detail" b={`FY${latestFY}`} r={`${enforcementFiltered.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Cases Opened","Convictions","Civil Settlements","Recoveries","Expenditures","ROI"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...enforcementFiltered].sort((a,b) => b.roi - a.roi).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.cases_opened)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.convictions)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.civil_settlements)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:POS }}>{fmtD(r.recoveries_total)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.program_expenditures)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:r.roi>3?POS:r.roi>1?WARN:NEG }}>{fmt(r.roi)}x</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Payment Accuracy */}
      {tab === "accuracy" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={latestPerm && latestPerm.improper_payment_rate_pct > 10 ? NEG : WARN}>
            <Met l="Latest Improper Payment Rate" v={latestPerm ? `${fmt(latestPerm.improper_payment_rate_pct)}%` : "\u2014"} cl={latestPerm && latestPerm.improper_payment_rate_pct > 10 ? NEG : WARN} sub={latestPerm ? `FY${latestPerm.fiscal_year}` : ""} />
          </Card>
          <Card accent={WARN}><Met l="Trend Direction" v={permTrend} cl={permTrend === "increasing" ? NEG : permTrend === "decreasing" ? POS : AL} /></Card>
          <Card accent={POS}><Met l="Data Points" v={permData.length} sub="Fiscal years tracked" /></Card>
        </div>

        {permChart.length > 0 ? <Card>
          <CH t="PERM Improper Payment Rates Over Time" b="Medicaid FFS, MC, and eligibility error rates" r={`FY${permChart[0]?.name || ""}-${permChart[permChart.length-1]?.name || ""}`} />
          <ChartActions filename="perm-trends">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={permChart} margin={{ top:10,right:20,bottom:4,left:10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{label}</div>
                    {payload.map((p: any, i: number) => (
                      <div key={i} style={{ color:p.color }}>{p.name}: {p.value != null ? `${p.value.toFixed(1)}%` : "\u2014"}</div>
                    ))}
                  </div>;
                }} />
                <Line type="monotone" dataKey="overall" name="Overall" stroke={NEG} strokeWidth={2} dot={{ r:3 }} />
                <Line type="monotone" dataKey="ffs" name="FFS" stroke={WARN} strokeWidth={1.5} dot={{ r:2 }} />
                <Line type="monotone" dataKey="mc" name="Managed Care" stroke={cB} strokeWidth={1.5} dot={{ r:2 }} />
                <Line type="monotone" dataKey="elig" name="Eligibility" stroke={AL} strokeWidth={1.5} strokeDasharray="4 4" dot={{ r:2 }} />
              </LineChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ display:"flex",gap:16,padding:"4px 14px 8px",fontSize:9,fontFamily:FM,flexWrap:"wrap" }}>
            <span><span style={{ display:"inline-block",width:12,height:2,background:NEG,verticalAlign:"middle",marginRight:4 }}/>Overall</span>
            <span><span style={{ display:"inline-block",width:12,height:2,background:WARN,verticalAlign:"middle",marginRight:4 }}/>FFS</span>
            <span><span style={{ display:"inline-block",width:12,height:2,background:cB,verticalAlign:"middle",marginRight:4 }}/>Managed Care</span>
            <span><span style={{ display:"inline-block",width:12,height:2,background:AL,verticalAlign:"middle",marginRight:4 }}/>Eligibility</span>
          </div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No PERM data available</div></Card>}

        {permData.length > 0 && <Card>
          <CH t="PERM Rate Detail" b={`${permData.length} fiscal years`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["Fiscal Year","Overall Rate","FFS Rate","MC Rate","Eligibility Rate"].map(h => (
                  <th key={h} style={{ textAlign:h==="Fiscal Year"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...permData].sort((a,b) => b.fiscal_year - a.fiscal_year).map(r => (
                  <tr key={r.fiscal_year} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>FY{r.fiscal_year}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:r.improper_payment_rate_pct>10?NEG:WARN }}>{fmt(r.improper_payment_rate_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.ffs_rate_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.managed_care_rate_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.eligibility_error_rate_pct)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Ask Aradune */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing Program Integrity Risk Index -- ${tab} tab. ${compositeData.length} composite, ${openPayData.length} open pay, ${enforcementData.length} enforcement, ${permData.length} PERM records.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Aradune about this
        </button>
      </div>

      {/* Sources */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS Open Payments (PY2024, $13B) . OIG LEIE Exclusion List . CMS PERM Error Rates . MFCU Statistical Reports
      </div>
    </div>
  );
}
