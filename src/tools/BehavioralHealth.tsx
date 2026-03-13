import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import { API_BASE } from "../lib/api";
import { LoadingBar } from "../components/LoadingBar";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";

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
interface NsduhRow { state: string; estimate_pct: number; ci_lower_pct: number; ci_upper_pct: number }
interface NsduhMeasure { id: string; name: string }
interface FacilitySummary { state: string; total_facilities: number; mh_facilities: number; su_facilities: number; hospital_beds: number; psych_beds: number; residential_beds: number; crisis_beds: number; detox_facilities: number }
interface IpfSummary { state: string; facilities: number; avg_readm30_rate: number; avg_smd_pct: number; avg_imm2_pct: number }
interface BlockGrant { state: string; program: string; fiscal_year: string; allotment: number }
interface OpioidRow { state: string; year: number; plan_type: string; opioid_prescribing_rate: number; opioid_claims: number; total_claims: number }
interface ConditionSummary { condition: string; states: number; total_beneficiaries: number; avg_pct: number }
interface ServiceSummary { state: string; condition: string; total_services: number; avg_rate_per_1000: number; service_types: number }

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

const fmt = (n: number | null | undefined, d = 1) => n == null ? "—" : n.toFixed(d);
const fmtK = (n: number | null | undefined) => n == null ? "—" : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}K` : n.toLocaleString();
const fmtD = (n: number | null | undefined) => n == null ? "—" : `$${n >= 1e9 ? (n / 1e9).toFixed(1) + "B" : n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "K" : n.toLocaleString()}`;

const TABS = ["Prevalence", "Treatment Network", "Opioid Crisis", "Conditions & Services"] as const;
type Tab = typeof TABS[number];

// ── Custom Tooltip ───────────────────────────────────────────────────
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

// ═══════════════════════════════════════════════════════════════════════
//  BEHAVIORAL HEALTH & SUD MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function BehavioralHealth() {
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Prevalence");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const isMobile = typeof window !== "undefined" && window.innerWidth < 768;

  // ── Prevalence state ──
  const [measures, setMeasures] = useState<NsduhMeasure[]>([]);
  const [selectedMeasure, setSelectedMeasure] = useState("any_mental_illness");
  const [ranking, setRanking] = useState<NsduhRow[]>([]);

  // ── Treatment Network state ──
  const [facilitySummary, setFacilitySummary] = useState<FacilitySummary[]>([]);
  const [ipfSummary, setIpfSummary] = useState<IpfSummary[]>([]);
  const [blockGrants, setBlockGrants] = useState<BlockGrant[]>([]);

  // ── Opioid state ──
  const [opioidData, setOpioidData] = useState<OpioidRow[]>([]);

  // ── Conditions state ──
  const [conditionsSummary, setConditionsSummary] = useState<ConditionSummary[]>([]);
  const [servicesSummary, setServicesSummary] = useState<ServiceSummary[]>([]);

  // ── Fetch helpers ─────────────────────────────────────────────────
  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load measures on mount ──
  useEffect(() => {
    fetchJson("/api/behavioral-health/nsduh/measures")
      .then(d => { if (d.measures?.length) setMeasures(d.measures); })
      .catch(() => {});
  }, [fetchJson]);

  // ── Load tab data ──
  useEffect(() => {
    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        if (tab === "Prevalence") {
          const d = await fetchJson(`/api/behavioral-health/nsduh/ranking?measure=${selectedMeasure}&age_group=18%2B`);
          setRanking(d.rows || []);
        } else if (tab === "Treatment Network") {
          const [fac, ipf, bg] = await Promise.all([
            fetchJson("/api/behavioral-health/facilities/summary"),
            fetchJson("/api/behavioral-health/ipf-facility/summary"),
            fetchJson("/api/behavioral-health/block-grants"),
          ]);
          setFacilitySummary(fac.rows || []);
          setIpfSummary(ipf.rows || []);
          setBlockGrants(bg.rows || []);
        } else if (tab === "Opioid Crisis") {
          const d = await fetchJson("/api/opioid/prescribing/summary");
          setOpioidData(d.rows || []);
        } else if (tab === "Conditions & Services") {
          const [cond, svc] = await Promise.all([
            fetchJson("/api/behavioral-health/conditions/summary"),
            fetchJson("/api/behavioral-health/services/summary"),
          ]);
          setConditionsSummary(cond.rows || []);
          setServicesSummary(svc.rows || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, selectedMeasure, fetchJson]);

  // ── Computed ──────────────────────────────────────────────────────
  const rankingChart = useMemo(() =>
    ranking.slice(0, 30).map(r => ({
      state: r.state,
      name: STATE_NAMES[r.state] || r.state,
      pct: r.estimate_pct,
      lo: r.ci_lower_pct,
      hi: r.ci_upper_pct,
    })),
  [ranking]);

  const nationalAvg = useMemo(() => {
    if (!ranking.length) return 0;
    return ranking.reduce((s, r) => s + r.estimate_pct, 0) / ranking.length;
  }, [ranking]);

  const facilityChart = useMemo(() =>
    facilitySummary.slice(0, 25).map(r => ({
      state: r.state,
      name: STATE_NAMES[r.state] || r.state,
      total: r.total_facilities,
      mh: r.mh_facilities,
      su: r.su_facilities,
    })),
  [facilitySummary]);

  const totalFacilities = useMemo(() => facilitySummary.reduce((s, r) => s + r.total_facilities, 0), [facilitySummary]);
  const totalPsychBeds = useMemo(() => facilitySummary.reduce((s, r) => s + r.psych_beds, 0), [facilitySummary]);
  const totalCrisisBeds = useMemo(() => facilitySummary.reduce((s, r) => s + r.crisis_beds, 0), [facilitySummary]);

  // Block grants aggregated by program
  const grantsByProgram = useMemo(() => {
    const map: Record<string, number> = {};
    blockGrants.forEach(g => { map[g.program] = (map[g.program] || 0) + g.allotment; });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [blockGrants]);

  // Opioid: latest year, sort by rate
  const latestOpioidYear = useMemo(() => {
    if (!opioidData.length) return null;
    return Math.max(...opioidData.map(r => r.year));
  }, [opioidData]);

  const opioidLatest = useMemo(() =>
    opioidData
      .filter(r => r.year === latestOpioidYear)
      .sort((a, b) => b.opioid_prescribing_rate - a.opioid_prescribing_rate)
      .slice(0, 30),
  [opioidData, latestOpioidYear]);

  const opioidNatAvg = useMemo(() => {
    if (!opioidLatest.length) return 0;
    return opioidLatest.reduce((s, r) => s + r.opioid_prescribing_rate, 0) / opioidLatest.length;
  }, [opioidLatest]);

  // ── Render ────────────────────────────────────────────────────────
  const measureName = measures.find(m => m.id === selectedMeasure)?.name || selectedMeasure;

  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Behavioral Health & SUD</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>NSDUH + N-SUMHSS + IPF + Opioid</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Mental health prevalence, substance use treatment access, opioid prescribing patterns, and behavioral health service utilization across all states.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: PREVALENCE                                             */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Prevalence" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Measure selector */}
          <Card>
            <CH t="NSDUH Measure" b="Select a behavioral health indicator" />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:6,flexWrap:"wrap" }}>
              {measures.slice(0, 12).map(m => (
                <Pill key={m.id} label={m.name.length > 40 ? m.name.slice(0, 38) + "..." : m.name}
                  active={selectedMeasure === m.id} onClick={() => setSelectedMeasure(m.id)} />
              ))}
              {measures.length > 12 && (
                <select value={selectedMeasure} onChange={e => setSelectedMeasure(e.target.value)}
                  style={{ fontSize:10,fontFamily:FM,padding:"4px 8px",borderRadius:6,border:`1px solid ${BD}`,color:AL,background:WH }}>
                  {measures.map(m => <option key={m.id} value={m.id}>{m.name}</option>)}
                </select>
              )}
            </div>
          </Card>

          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States Reporting" v={ranking.length} />
            </Card>
            <Card accent={cB}>
              <Met l="National Avg" v={`${fmt(nationalAvg)}%`} />
            </Card>
            <Card accent={POS}>
              <Met l="Lowest" v={ranking.length ? `${fmt(ranking[ranking.length - 1].estimate_pct)}%` : "—"} sub={ranking.length ? STATE_NAMES[ranking[ranking.length - 1].state] : ""} />
            </Card>
            <Card accent={NEG}>
              <Met l="Highest" v={ranking.length ? `${fmt(ranking[0].estimate_pct)}%` : "—"} sub={ranking.length ? STATE_NAMES[ranking[0].state] : ""} />
            </Card>
          </div>

          {/* Bar chart */}
          <Card>
            <CH t="State Rankings" b={measureName} r={`Top 30 of ${ranking.length} states`} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename={`bh-prevalence-${selectedMeasure}`}>
                <div style={{ width:"100%",height:Math.max(360, rankingChart.length * 18) }}>
                  <ResponsiveContainer>
                    <BarChart data={rankingChart} layout="vertical" margin={{ left: isMobile ? 40 : 70, right: 20, top: 4, bottom: 4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile ? 36 : 66} />
                      <Tooltip content={<SafeTip formatter={(v) => `${v.toFixed(1)}%`} />} />
                      <Bar dataKey="pct" radius={[0,3,3,0]} maxBarSize={14}>
                        {rankingChart.map((d, i) => (
                          <Cell key={i} fill={d.pct > nationalAvg ? NEG : POS} fillOpacity={0.8} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartActions>
            </div>
          </Card>

          {/* Table */}
          <Card>
            <CH t="Full Rankings" r={`${ranking.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["#","State","Estimate %","95% CI Lower","95% CI Upper","vs National"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {ranking.map((r, i) => {
                    const diff = r.estimate_pct - nationalAvg;
                    return (
                      <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmt(r.estimate_pct)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.ci_lower_pct)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.ci_upper_pct)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:diff > 0 ? NEG : POS,fontWeight:500 }}>
                          {diff > 0 ? "+" : ""}{fmt(diff, 1)}pp
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: TREATMENT NETWORK                                      */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Treatment Network" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total Facilities" v={fmtK(totalFacilities)} sub="MH + SUD treatment" />
            </Card>
            <Card accent="#6366F1">
              <Met l="Psych Beds" v={fmtK(totalPsychBeds)} sub="Inpatient psychiatric" />
            </Card>
            <Card accent={WARN}>
              <Met l="Crisis Beds" v={fmtK(totalCrisisBeds)} sub="Crisis stabilization" />
            </Card>
            <Card accent={POS}>
              <Met l="States Reporting" v={facilitySummary.length} />
            </Card>
          </div>

          {/* Facility chart */}
          <Card>
            <CH t="Treatment Facilities by State" b="N-SUMHSS" r={`Top 25 of ${facilitySummary.length}`} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename="bh-facilities">
                <div style={{ width:"100%",height:Math.max(360, facilityChart.length * 20) }}>
                  <ResponsiveContainer>
                    <BarChart data={facilityChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                      <Tooltip content={<SafeTip formatter={(v, k) => `${v.toLocaleString()} ${k === "mh" ? "MH" : k === "su" ? "SUD" : "total"}`} />} />
                      <Bar dataKey="mh" stackId="a" fill={cB} radius={[0,0,0,0]} maxBarSize={14} name="MH" />
                      <Bar dataKey="su" stackId="a" fill="#6366F1" radius={[0,3,3,0]} maxBarSize={14} name="SUD" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartActions>
            </div>
          </Card>

          {/* Block Grants */}
          {grantsByProgram.length > 0 && (
            <Card>
              <CH t="SAMHSA Block Grants" b="Mental Health + SAPT allotments" />
              <div style={{ padding:"8px 14px 14px" }}>
                <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"1fr 1fr",gap:10,marginBottom:12 }}>
                  {grantsByProgram.map(([prog, amt]) => (
                    <div key={prog} style={{ padding:"8px 12px",background:SF,borderRadius:6,border:`1px solid ${BD}` }}>
                      <div style={{ fontSize:9,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>{prog}</div>
                      <div style={{ fontSize:16,fontWeight:300,color:A,fontFamily:FM }}>{fmtD(amt)}</div>
                    </div>
                  ))}
                </div>
              </div>
            </Card>
          )}

          {/* IPF Quality Summary */}
          {ipfSummary.length > 0 && (
            <Card>
              <CH t="Inpatient Psychiatric Facility Quality" b="CMS IPF Quality Reporting" r={`${ipfSummary.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["State","IPFs","Avg 30-Day Readmission","Avg Screening %","Avg Immunization %"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {ipfSummary.slice(0, 30).map(r => (
                      <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{r.facilities}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_readm30_rate > 20 ? NEG : A }}>{fmt(r.avg_readm30_rate)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_smd_pct)}%</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_imm2_pct)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {/* Facility table */}
          <Card>
            <CH t="Facility & Bed Capacity by State" r={`${facilitySummary.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["State","Total","MH","SUD","Hospital Beds","Psych Beds","Residential","Crisis","Detox"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {facilitySummary.map(r => (
                    <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{r.total_facilities.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.mh_facilities.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.su_facilities.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.hospital_beds.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.psych_beds.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.residential_beds.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.crisis_beds.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.detox_facilities.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: OPIOID CRISIS                                          */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Opioid Crisis" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={NEG}>
              <Met l="Highest Rate" v={opioidLatest.length ? `${fmt(opioidLatest[0].opioid_prescribing_rate)}%` : "—"} sub={opioidLatest.length ? STATE_NAMES[opioidLatest[0].state] : ""} />
            </Card>
            <Card accent={POS}>
              <Met l="Lowest Rate" v={opioidLatest.length ? `${fmt(opioidLatest[opioidLatest.length - 1].opioid_prescribing_rate)}%` : "—"} sub={opioidLatest.length ? STATE_NAMES[opioidLatest[opioidLatest.length - 1].state] : ""} />
            </Card>
            <Card accent={cB}>
              <Met l="National Avg" v={`${fmt(opioidNatAvg)}%`} sub={latestOpioidYear ? `CY${latestOpioidYear}` : ""} />
            </Card>
            <Card accent={WARN}>
              <Met l="States Reporting" v={opioidLatest.length} sub={latestOpioidYear ? `Year: ${latestOpioidYear}` : ""} />
            </Card>
          </div>

          {/* Bar chart */}
          <Card>
            <CH t="Opioid Prescribing Rate by State" b="Medicaid claims, all plan types" r={latestOpioidYear ? `CY${latestOpioidYear}` : ""} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename="bh-opioid">
                <div style={{ width:"100%",height:Math.max(360, opioidLatest.length * 18) }}>
                  <ResponsiveContainer>
                    <BarChart data={opioidLatest.map(r => ({ ...r, name: STATE_NAMES[r.state] || r.state }))} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                      <Tooltip content={<SafeTip formatter={(v) => `${v.toFixed(2)}%`} />} />
                      <Bar dataKey="opioid_prescribing_rate" radius={[0,3,3,0]} maxBarSize={14}>
                        {opioidLatest.map((d, i) => (
                          <Cell key={i} fill={d.opioid_prescribing_rate > opioidNatAvg ? NEG : POS} fillOpacity={0.8} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartActions>
            </div>
          </Card>

          {/* Full table */}
          <Card>
            <CH t="Opioid Prescribing Detail" r={`${opioidLatest.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["#","State","Prescribing Rate","Opioid Claims","Total Claims","Opioid Share"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"||h==="State"?(h==="#"?"center":"left"):"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {opioidLatest.map((r, i) => (
                    <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:r.opioid_prescribing_rate > opioidNatAvg ? NEG : A }}>{fmt(r.opioid_prescribing_rate, 2)}%</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.opioid_claims)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_claims)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>
                        {r.total_claims ? fmt((r.opioid_claims / r.total_claims) * 100, 2) + "%" : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 4: CONDITIONS & SERVICES                                  */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Conditions & Services" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Conditions summary */}
          {conditionsSummary.length > 0 && (
            <Card>
              <CH t="BH Conditions Among Medicaid Beneficiaries" b="T-MSIS aggregated" r={`${conditionsSummary.length} conditions`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="bh-conditions">
                  <div style={{ width:"100%",height:Math.max(280, conditionsSummary.length * 28) }}>
                    <ResponsiveContainer>
                      <BarChart data={conditionsSummary.map(c => ({ ...c, label: c.condition.length > 35 ? c.condition.slice(0, 33) + "..." : c.condition }))} layout="vertical" margin={{ left:isMobile?80:160,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtK(v)} />
                        <YAxis type="category" dataKey="label" tick={{ fontSize:9,fill:AL }} width={isMobile?76:156} />
                        <Tooltip content={<SafeTip formatter={(v) => v.toLocaleString()} />} />
                        <Bar dataKey="total_beneficiaries" fill={cB} radius={[0,3,3,0]} maxBarSize={16} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
              {/* Condition table */}
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["Condition","States","Total Beneficiaries","Avg % of BH Pop"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="Condition"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {conditionsSummary.map(r => (
                      <tr key={r.condition} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.condition}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.states}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtK(r.total_beneficiaries)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_pct)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {/* Services by state + condition */}
          {servicesSummary.length > 0 && (
            <Card>
              <CH t="BH Service Utilization" b="By state and condition" r={`${servicesSummary.length} records`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["State","Condition","Total Services","Avg Rate / 1K","Service Types"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"||h==="Condition"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {servicesSummary.slice(0, 100).map((r, i) => (
                      <tr key={`${r.state}-${r.condition}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.state}</td>
                        <td style={{ padding:"5px 10px",color:AL }}>{r.condition}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtK(r.total_services)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_rate_per_1000)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.service_types}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing BH/SUD ${tab} data. ${ranking.length} states in NSDUH ranking. ${facilitySummary.length} states with facility data.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: SAMHSA NSDUH (2023-2024) · N-SUMHSS Facility Directory · CMS IPF Quality Reporting · Medicaid Opioid Prescribing · T-MSIS BH Services
      </div>
    </div>
  );
}
