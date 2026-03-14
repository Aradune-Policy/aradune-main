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
interface HospitalStress { state_code: string; total_hospitals: number; negative_margin_count: number; pct_negative_margin: number; avg_operating_margin: number; avg_uncompensated_care: number; avg_dsh_payment: number; avg_medicaid_day_pct: number }
interface LtssPressure { state_code: string; total_waitlist: number; avg_nursing_rating: number; avg_staffing_rating: number; facility_count: number; total_enrollment: number; waitlist_per_1000: number }
interface StaffingCrisis { state_code: string; avg_total_hprd: number; avg_rn_hprd: number; avg_cna_hprd: number; contract_rn_pct: number; facilities_reporting: number; below_minimum_count: number }
interface CompositeRow { state_code: string; hospital_stress: number; hcbs_pressure: number; nursing_deficit: number; fmap_rate: number }

type Tab = "hospital" | "ltss" | "staffing" | "composite";

export default function SafetyNetStress() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();

  const [tab, setTab] = useState<Tab>("hospital");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [hospitalData, setHospitalData] = useState<HospitalStress[]>([]);
  const [ltssData, setLtssData] = useState<LtssPressure[]>([]);
  const [staffingData, setStaffingData] = useState<StaffingCrisis[]>([]);
  const [compositeData, setCompositeData] = useState<CompositeRow[]>([]);

  useEffect(() => {
    setLoading(true);
    setLoadError(null);
    const endpoints: Record<Tab, string> = {
      hospital: "/api/research/safety-net/hospital-stress",
      ltss: "/api/research/safety-net/ltss-pressure",
      staffing: "/api/research/safety-net/staffing-crisis",
      composite: "/api/research/safety-net/composite",
    };
    fetch(`${API_BASE}${endpoints[tab]}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        const rows = Array.isArray(d) ? d : d.rows || d.data || [];
        if (tab === "hospital") setHospitalData(rows);
        else if (tab === "ltss") setLtssData(rows);
        else if (tab === "staffing") setStaffingData(rows);
        else setCompositeData(rows);
      })
      .catch(e => setLoadError(e.message))
      .finally(() => setLoading(false));
  }, [tab]);

  // -- Memoized chart data --
  const hospitalChart = useMemo(() =>
    [...hospitalData].sort((a, b) => b.pct_negative_margin - a.pct_negative_margin).slice(0, 30).map(r => ({
      name: r.state_code, pct: r.pct_negative_margin, margin: r.avg_operating_margin,
    })), [hospitalData]);

  const ltssScatter = useMemo(() =>
    ltssData.filter(r => r.waitlist_per_1000 != null && r.avg_nursing_rating != null).map(r => ({
      name: r.state_code, x: r.waitlist_per_1000, y: r.avg_nursing_rating, z: r.total_waitlist,
    })), [ltssData]);

  const ltssBar = useMemo(() =>
    [...ltssData].sort((a, b) => b.total_waitlist - a.total_waitlist).slice(0, 25).map(r => ({
      name: r.state_code, waitlist: r.total_waitlist,
    })), [ltssData]);

  const staffingChart = useMemo(() =>
    [...staffingData].sort((a, b) => a.avg_total_hprd - b.avg_total_hprd).slice(0, 30).map(r => ({
      name: r.state_code, hprd: r.avg_total_hprd, below: r.below_minimum_count,
    })), [staffingData]);

  const compositeChart = useMemo(() => {
    return [...compositeData].map(r => ({
      name: r.state_code, score: r.hospital_stress + r.hcbs_pressure + r.nursing_deficit,
      hospital: r.hospital_stress, hcbs: r.hcbs_pressure, nursing: r.nursing_deficit, fmap: r.fmap_rate,
    })).sort((a, b) => b.score - a.score).slice(0, 35);
  }, [compositeData]);

  const hospitalStats = useMemo(() => {
    if (!hospitalData.length) return { total: 0, avgPctNeg: 0, avgMargin: 0 };
    const total = hospitalData.reduce((s, r) => s + r.total_hospitals, 0);
    const avgPctNeg = hospitalData.reduce((s, r) => s + r.pct_negative_margin, 0) / hospitalData.length;
    const avgMargin = hospitalData.reduce((s, r) => s + r.avg_operating_margin, 0) / hospitalData.length;
    return { total, avgPctNeg, avgMargin };
  }, [hospitalData]);

  const ltssStats = useMemo(() => {
    if (!ltssData.length) return { totalWait: 0, avgRating: 0 };
    const totalWait = ltssData.reduce((s, r) => s + r.total_waitlist, 0);
    const avgRating = ltssData.reduce((s, r) => s + r.avg_nursing_rating, 0) / ltssData.length;
    return { totalWait, avgRating };
  }, [ltssData]);

  const staffingStats = useMemo(() => {
    if (!staffingData.length) return { belowMin: 0, avgContract: 0, avgHprd: 0 };
    const belowMin = staffingData.reduce((s, r) => s + r.below_minimum_count, 0);
    const avgContract = staffingData.reduce((s, r) => s + r.contract_rn_pct, 0) / staffingData.length;
    const avgHprd = staffingData.reduce((s, r) => s + r.avg_total_hprd, 0) / staffingData.length;
    return { belowMin, avgContract, avgHprd };
  }, [staffingData]);

  const stressColor = (score: number, max: number) => {
    const pct = max > 0 ? score / max : 0;
    if (pct > 0.7) return NEG;
    if (pct > 0.4) return WARN;
    return POS;
  };

  const tabs: { key: Tab; label: string }[] = [
    { key: "hospital", label: "Hospital Financial Stress" },
    { key: "ltss", label: "LTSS Pressure" },
    { key: "staffing", label: "Staffing Crisis" },
    { key: "composite", label: "Composite Index" },
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
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(46,107,74,0.1)",color:cB,fontWeight:600 }}>RESEARCH</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>HCRIS + Five-Star + PBJ + HCBS + CMS-64</span>
          </div>
          <button onClick={() => openIntelligence({ summary: `User is viewing Safety Net Stress Test -- ${tab} tab. ${hospitalData.length} states with hospital data.` })} style={{
            padding:"5px 12px",borderRadius:6,border:"none",background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
          }}>Ask Aradune</button>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(46,107,74,0.03)",borderLeft:`3px solid ${cB}` }}>
        <span style={{ fontWeight:700,color:A }}>Safety Net Stress Test.</span> Multi-dimensional assessment of safety net strain across hospitals, nursing facilities, HCBS programs, and enrollment stability. Identifies states where the entire care continuum is under simultaneous pressure.
      </div></Card>

      {/* Tab pills */}
      <div style={{ display:"flex",gap:6,marginTop:10,marginBottom:12,flexWrap:"wrap" }}>
        {tabs.map(t => <Pill key={t.key} active={tab===t.key} label={t.label} onClick={() => setTab(t.key)} />)}
      </div>

      {/* Hospital Financial Stress */}
      {tab === "hospital" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Total Hospitals" v={fmtK(hospitalStats.total)} /></Card>
          <Card accent={WARN}><Met l="Avg % Negative Margin" v={`${fmt(hospitalStats.avgPctNeg)}%`} cl={hospitalStats.avgPctNeg > 30 ? NEG : WARN} /></Card>
          <Card accent={POS}><Met l="Avg Operating Margin" v={`${fmt(hospitalStats.avgMargin)}%`} cl={hospitalStats.avgMargin < 0 ? NEG : POS} /></Card>
        </div>

        {hospitalChart.length > 0 ? <Card>
          <CH t="States by % Hospitals with Negative Margin" b="Top 30" r={`${hospitalData.length} states`} />
          <ChartActions filename="hospital-negative-margin">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={hospitalChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={<SafeTip formatter={(v: number, k: string) => k === "pct" ? `${v.toFixed(1)}%` : `${v.toFixed(1)}%`} />} />
                <Bar dataKey="pct" name="% Negative Margin" radius={[3,3,0,0]}>
                  {hospitalChart.map((d, i) => <Cell key={i} fill={d.pct > 50 ? NEG : d.pct > 30 ? WARN : POS} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No hospital stress data available</div></Card>}

        {hospitalData.length > 0 && <Card>
          <CH t="Hospital Financial Stress Detail" b={`${hospitalData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Hospitals","Neg Margin","% Neg","Avg Margin","Avg Uncomp Care","Avg DSH","Med Day %"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...hospitalData].sort((a,b) => b.pct_negative_margin - a.pct_negative_margin).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{r.total_hospitals}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:NEG }}>{r.negative_margin_count}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:r.pct_negative_margin>50?NEG:r.pct_negative_margin>30?WARN:POS }}>{fmt(r.pct_negative_margin)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.avg_operating_margin<0?NEG:POS }}>{fmt(r.avg_operating_margin)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.avg_uncompensated_care)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtD(r.avg_dsh_payment)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.avg_medicaid_day_pct)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* LTSS Pressure */}
      {tab === "ltss" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(2,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Total People on Waitlists" v={fmtK(ltssStats.totalWait)} cl={NEG} /></Card>
          <Card accent={WARN}><Met l="Avg Nursing Quality Rating" v={fmt(ltssStats.avgRating)} sub="1-5 scale, higher is better" /></Card>
        </div>

        {ltssScatter.length > 0 ? <Card>
          <CH t="HCBS Waitlist vs Nursing Quality" b="Each dot = state" r={`${ltssScatter.length} states`} />
          <ChartActions filename="ltss-scatter">
            <ResponsiveContainer width="100%" height={300}>
              <ScatterChart margin={{ top:10,right:20,bottom:10,left:10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="x" name="Waitlist / 1K enrollees" tick={{ fontSize:9,fill:AL }} label={{ value:"Waitlist per 1,000",position:"bottom",fontSize:9,fill:AL }} />
                <YAxis dataKey="y" name="Nursing Rating" tick={{ fontSize:9,fill:AL }} domain={[1,5]} label={{ value:"Avg Nursing Rating",angle:-90,position:"insideLeft",fontSize:9,fill:AL }} />
                <ZAxis dataKey="z" range={[40,400]} />
                <Tooltip content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[d.name] || d.name}</div>
                    <div style={{ color:AL }}>Waitlist/1K: {fmt(d.x)}</div>
                    <div style={{ color:AL }}>Nursing Rating: {fmt(d.y)}</div>
                    <div style={{ color:AL }}>Total Waitlist: {fmtK(d.z)}</div>
                  </div>;
                }} />
                <Scatter data={ltssScatter} fill={cB}>
                  {ltssScatter.map((d, i) => <Cell key={i} fill={d.y < 2.5 && d.x > 5 ? NEG : d.y < 3 ? WARN : cB} />)}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No LTSS scatter data available</div></Card>}

        {ltssBar.length > 0 && <Card>
          <CH t="States by Total HCBS Waitlist" b="Top 25" />
          <ChartActions filename="ltss-waitlist-bar">
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={ltssBar} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtK(v)} />
                <Tooltip content={<SafeTip formatter={(v: number) => fmtK(v)} />} />
                <Bar dataKey="waitlist" name="Total Waitlist" fill={NEG} radius={[3,3,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card>}

        {ltssData.length > 0 && <Card>
          <CH t="LTSS Pressure Detail" b={`${ltssData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Waitlist","Wait/1K","Nursing Rating","Staffing Rating","Facilities","Enrollment"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...ltssData].sort((a,b) => b.total_waitlist - a.total_waitlist).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:NEG }}>{fmtK(r.total_waitlist)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.waitlist_per_1000)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.avg_nursing_rating<3?NEG:POS }}>{fmt(r.avg_nursing_rating)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.avg_staffing_rating<3?NEG:POS }}>{fmt(r.avg_staffing_rating)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.facility_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.total_enrollment)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Staffing Crisis */}
      {tab === "staffing" && <>
        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="Facilities Below Minimum" v={fmtK(staffingStats.belowMin)} cl={NEG} sub="Below 3.48 HPRD threshold" /></Card>
          <Card accent={WARN}><Met l="Avg Contract RN %" v={`${fmt(staffingStats.avgContract)}%`} cl={staffingStats.avgContract > 20 ? NEG : WARN} /></Card>
          <Card accent={POS}><Met l="Avg Total HPRD" v={fmt(staffingStats.avgHprd)} cl={staffingStats.avgHprd < 3.48 ? NEG : POS} sub="CMS minimum: 3.48" /></Card>
        </div>

        {staffingChart.length > 0 ? <Card>
          <CH t="States by Avg Total HPRD" b="Lowest first; red line = 3.48 CMS minimum" r={`${staffingData.length} states`} />
          <ChartActions filename="staffing-hprd">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={staffingChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} domain={[0, "auto"]} />
                <Tooltip content={<SafeTip formatter={(v: number, k: string) => k === "hprd" ? v.toFixed(2) : fmtK(v)} />} />
                {/* Threshold line at 3.48 */}
                <Bar dataKey="hprd" name="Avg HPRD" radius={[3,3,0,0]}>
                  {staffingChart.map((d, i) => <Cell key={i} fill={d.hprd < 3.48 ? NEG : POS} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ padding:"2px 14px 8px",fontSize:9,color:AL,fontFamily:FM }}>CMS proposed minimum staffing standard: 3.48 total HPRD (0.55 RN + 2.45 nurse aide)</div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No staffing data available</div></Card>}

        {staffingData.length > 0 && <Card>
          <CH t="Staffing Detail" b={`${staffingData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Total HPRD","RN HPRD","CNA HPRD","Contract RN %","Facilities","Below Min"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...staffingData].sort((a,b) => a.avg_total_hprd - b.avg_total_hprd).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:r.avg_total_hprd<3.48?NEG:POS }}>{fmt(r.avg_total_hprd,2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.avg_rn_hprd,2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.avg_cna_hprd,2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.contract_rn_pct>20?NEG:AL }}>{fmt(r.contract_rn_pct)}%</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.facilities_reporting)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:NEG }}>{fmtK(r.below_minimum_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Composite Index */}
      {tab === "composite" && <>
        {compositeChart.length > 0 ? <Card>
          <CH t="Composite Safety Net Stress Index" b="Hospital + HCBS + Nursing deficit scores" r={`${compositeData.length} states`} />
          <ChartActions filename="composite-stress">
            <ResponsiveContainer width="100%" height={360}>
              <BarChart data={compositeChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:AL }}>Composite: {fmt(d.score)}</div>
                    <div style={{ color:AL }}>Hospital Stress: {fmt(d.hospital)}</div>
                    <div style={{ color:AL }}>HCBS Pressure: {fmt(d.hcbs)}</div>
                    <div style={{ color:AL }}>Nursing Deficit: {fmt(d.nursing)}</div>
                    <div style={{ color:AL }}>FMAP: {fmt(d.fmap ? d.fmap * 100 : null)}%</div>
                  </div>;
                }} />
                <Bar dataKey="score" name="Composite Score" radius={[3,3,0,0]}>
                  {compositeChart.map((d, i) => {
                    const maxScore = compositeChart[0]?.score || 1;
                    return <Cell key={i} fill={stressColor(d.score, maxScore)} />;
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No composite data available</div></Card>}

        {compositeData.length > 0 && <Card>
          <CH t="Composite Sub-Scores" b={`${compositeData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Hospital Stress","HCBS Pressure","Nursing Deficit","Composite","FMAP"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...compositeData].map(r => ({ ...r, score: r.hospital_stress + r.hcbs_pressure + r.nursing_deficit }))
                  .sort((a, b) => b.score - a.score).map(r => {
                    const maxScore = compositeChart[0]?.score || 1;
                    return (
                      <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                        <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                        <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.hospital_stress)}</td>
                        <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.hcbs_pressure)}</td>
                        <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.nursing_deficit)}</td>
                        <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:stressColor(r.score, maxScore) }}>{fmt(r.score)}</td>
                        <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:AL }}>{r.fmap_rate ? `${(r.fmap_rate * 100).toFixed(1)}%` : "\u2014"}</td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing Safety Net Stress Test -- ${tab} tab. ${hospitalData.length} states hospital data, ${ltssData.length} LTSS, ${staffingData.length} staffing, ${compositeData.length} composite.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Sources */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS HCRIS Cost Reports . Care Compare Five-Star . Payroll-Based Journal Staffing . HCBS Waiver Waitlists . CMS-64 Expenditure
      </div>
    </div>
  );
}
