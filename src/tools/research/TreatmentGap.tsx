import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, ScatterChart, Scatter, ZAxis, LineChart, Line } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

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
interface DemandSupply { state_code: string; oud_prevalence_pct: number; sud_facility_count: number; detox_facilities: number; residential_beds: number; total_enrollment: number; facilities_per_100k: number }
interface MatRow { state_code: string; mat_total_spending: number; mat_prescriptions: number; mat_units: number }
interface PrescribingRow { state_code: string; year: number; opioid_prescribing_rate: number; opioid_claims: number; total_claims: number }
interface FundingRow { state_code: string; prevalence_pct: number; total_block_grant: number; total_enrollment: number; grant_per_enrollee: number }

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

const fmt = (n: number | null | undefined, d = 1) => n == null ? "\u2014" : n.toFixed(d);
const fmtK = (n: number | null | undefined) => n == null ? "\u2014" : n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : n.toLocaleString();
const fmtD = (n: number | null | undefined) => n == null ? "\u2014" : `$${n >= 1e9 ? (n/1e9).toFixed(1) + "B" : n >= 1e6 ? (n/1e6).toFixed(1) + "M" : n >= 1e3 ? (n/1e3).toFixed(0) + "K" : n.toLocaleString()}`;

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

// ── Tabs ──────────────────────────────────────────────────────────────
const TABS = ["Demand-Supply Map", "MAT Utilization", "Prescribing Patterns", "Funding Alignment"] as const;
type Tab = typeof TABS[number];

// ═══════════════════════════════════════════════════════════════════════
//  OPIOID TREATMENT GAP MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function TreatmentGap() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Demand-Supply Map");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Demand-Supply state ──
  const [demandSupply, setDemandSupply] = useState<DemandSupply[]>([]);

  // ── MAT state ──
  const [matData, setMatData] = useState<MatRow[]>([]);

  // ── Prescribing state ──
  const [prescribingData, setPrescribingData] = useState<PrescribingRow[]>([]);

  // ── Funding state ──
  const [fundingData, setFundingData] = useState<FundingRow[]>([]);

  // ── Fetch helpers ─────────────────────────────────────────────────
  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load tab data ──
  useEffect(() => {
    setLoading(true);
    setError(null);
    const load = async () => {
      try {
        if (tab === "Demand-Supply Map") {
          const d = await fetchJson("/api/research/treatment-gap/demand-supply");
          setDemandSupply(d.rows || d.data || []);
        } else if (tab === "MAT Utilization") {
          const d = await fetchJson("/api/research/treatment-gap/mat-utilization");
          setMatData(d.rows || d.data || []);
        } else if (tab === "Prescribing Patterns") {
          const d = await fetchJson("/api/research/treatment-gap/prescribing");
          setPrescribingData(d.rows || d.data || []);
        } else if (tab === "Funding Alignment") {
          const d = await fetchJson("/api/research/treatment-gap/funding");
          setFundingData(d.rows || d.data || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, fetchJson]);

  // ── Computed: Demand-Supply ───────────────────────────────────────
  const dsSorted = useMemo(() =>
    [...demandSupply].sort((a, b) => b.oud_prevalence_pct - a.oud_prevalence_pct),
  [demandSupply]);

  const dsChart = useMemo(() =>
    dsSorted.slice(0, 30).map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [dsSorted]);

  const dsAvgPrevalence = useMemo(() => {
    if (!demandSupply.length) return 0;
    return demandSupply.reduce((s, r) => s + r.oud_prevalence_pct, 0) / demandSupply.length;
  }, [demandSupply]);

  const dsAvgFacilities = useMemo(() => {
    if (!demandSupply.length) return 0;
    return demandSupply.reduce((s, r) => s + r.facilities_per_100k, 0) / demandSupply.length;
  }, [demandSupply]);

  // States with biggest gap: high prevalence + low facilities
  const gapStates = useMemo(() =>
    dsSorted.filter(r => r.oud_prevalence_pct > dsAvgPrevalence && r.facilities_per_100k < dsAvgFacilities)
      .slice(0, 10),
  [dsSorted, dsAvgPrevalence, dsAvgFacilities]);

  const totalFacilities = useMemo(() => demandSupply.reduce((s, r) => s + r.sud_facility_count, 0), [demandSupply]);
  const totalDetox = useMemo(() => demandSupply.reduce((s, r) => s + (r.detox_facilities || 0), 0), [demandSupply]);

  // ── Computed: MAT ─────────────────────────────────────────────────
  const matSorted = useMemo(() =>
    [...matData].sort((a, b) => b.mat_total_spending - a.mat_total_spending),
  [matData]);

  const matChart = useMemo(() =>
    matSorted.slice(0, 30).map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [matSorted]);

  const matTotalSpending = useMemo(() => matData.reduce((s, r) => s + (r.mat_total_spending || 0), 0), [matData]);
  const matTotalRx = useMemo(() => matData.reduce((s, r) => s + (r.mat_prescriptions || 0), 0), [matData]);

  // MAT spending per enrollee (approximate) -- requires enrollment from demand-supply
  const matWithPerEnrollee = useMemo(() => {
    const enrollmentMap: Record<string, number> = {};
    demandSupply.forEach(r => { if (r.total_enrollment) enrollmentMap[r.state_code] = r.total_enrollment; });
    return matSorted.map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
      per_enrollee: enrollmentMap[r.state_code] ? r.mat_total_spending / enrollmentMap[r.state_code] : null,
    }));
  }, [matSorted, demandSupply]);

  // ── Computed: Prescribing ─────────────────────────────────────────
  const latestYear = useMemo(() => {
    if (!prescribingData.length) return null;
    return Math.max(...prescribingData.map(r => r.year));
  }, [prescribingData]);

  const prescribingLatest = useMemo(() =>
    prescribingData
      .filter(r => r.year === latestYear)
      .sort((a, b) => b.opioid_prescribing_rate - a.opioid_prescribing_rate),
  [prescribingData, latestYear]);

  const prescribingChart = useMemo(() =>
    prescribingLatest.slice(0, 30).map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [prescribingLatest]);

  const prescribingNatAvg = useMemo(() => {
    if (!prescribingLatest.length) return 0;
    return prescribingLatest.reduce((s, r) => s + r.opioid_prescribing_rate, 0) / prescribingLatest.length;
  }, [prescribingLatest]);

  const statesAboveAvg = useMemo(() => prescribingLatest.filter(r => r.opioid_prescribing_rate > prescribingNatAvg).length, [prescribingLatest, prescribingNatAvg]);

  // ── Computed: Funding ─────────────────────────────────────────────
  const fundingChart = useMemo(() =>
    fundingData.map(r => ({
      ...r,
      name: STATE_NAMES[r.state_code] || r.state_code,
    })),
  [fundingData]);

  const fundingAvgPrevalence = useMemo(() => {
    if (!fundingData.length) return 0;
    return fundingData.reduce((s, r) => s + r.prevalence_pct, 0) / fundingData.length;
  }, [fundingData]);

  const fundingAvgGrant = useMemo(() => {
    if (!fundingData.length) return 0;
    return fundingData.reduce((s, r) => s + r.grant_per_enrollee, 0) / fundingData.length;
  }, [fundingData]);

  const fundingTotalGrant = useMemo(() => fundingData.reduce((s, r) => s + (r.total_block_grant || 0), 0), [fundingData]);

  // States with high prevalence but low funding
  const underfundedStates = useMemo(() =>
    fundingData.filter(r => r.prevalence_pct > fundingAvgPrevalence && r.grant_per_enrollee < fundingAvgGrant)
      .sort((a, b) => b.prevalence_pct - a.prevalence_pct)
      .slice(0, 5),
  [fundingData, fundingAvgPrevalence, fundingAvgGrant]);

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Opioid Treatment Gap</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>NSDUH + SDUD + N-SUMHSS + TEDS</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Mapping the full demand-supply-spending pipeline for opioid use disorder treatment. Identifies states where prevalence outstrips treatment capacity, MAT access, and federal funding.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: DEMAND-SUPPLY MAP                                      */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Demand-Supply Map" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={demandSupply.length} />
            </Card>
            <Card accent={NEG}>
              <Met l="Avg OUD Prevalence" v={`${fmt(dsAvgPrevalence)}%`} sub="Of adult population" />
            </Card>
            <Card accent={cB}>
              <Met l="Total SUD Facilities" v={fmtK(totalFacilities)} sub={`${fmtK(totalDetox)} detox`} />
            </Card>
            <Card accent={WARN}>
              <Met l="Biggest Gap States" v={gapStates.length} sub="High prevalence, low capacity" />
            </Card>
          </div>

          {/* Gap alert */}
          {gapStates.length > 0 && (
            <Card accent={NEG}>
              <CH t="Treatment Gap Alert" b="High prevalence + below-average facilities per capita" />
              <div style={{ padding:"8px 14px 12px",display:"flex",gap:6,flexWrap:"wrap" }}>
                {gapStates.map(r => (
                  <span key={r.state_code} style={{
                    fontSize:10,fontFamily:FM,padding:"3px 8px",borderRadius:4,
                    background:`${NEG}10`,border:`1px solid ${NEG}30`,color:NEG,fontWeight:600,
                  }}>
                    {STATE_NAMES[r.state_code] || r.state_code} ({fmt(r.oud_prevalence_pct)}% / {fmt(r.facilities_per_100k, 1)} per 100K)
                  </span>
                ))}
              </div>
            </Card>
          )}

          {/* Bar chart */}
          {dsChart.length > 0 ? (
            <Card>
              <CH t="OUD Prevalence by State" b="Bars = prevalence, color = facility capacity" r={`Top 30 of ${demandSupply.length}`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="treatment-gap-demand">
                  <div style={{ width:"100%",height:Math.max(360, dsChart.length * 18) }}>
                    <ResponsiveContainer>
                      <BarChart data={dsChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>OUD Prevalence: {fmt(d.oud_prevalence_pct)}%</div>
                              <div style={{ color:AL }}>SUD Facilities: {fmtK(d.sud_facility_count)}</div>
                              <div style={{ color:AL }}>Facilities/100K: {fmt(d.facilities_per_100k, 1)}</div>
                              <div style={{ color:AL }}>Detox: {fmtK(d.detox_facilities)}</div>
                              <div style={{ color:AL }}>Residential Beds: {fmtK(d.residential_beds)}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="oud_prevalence_pct" radius={[0,3,3,0]} maxBarSize={14}>
                          {dsChart.map((d, i) => (
                            <Cell key={i} fill={d.facilities_per_100k >= dsAvgFacilities ? POS : NEG} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
                <div style={{ fontSize:9,color:AL,fontFamily:FM,marginTop:4,textAlign:"center" }}>
                  Green = above-average facility capacity per 100K. Red = below-average.
                </div>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No demand-supply data available.</div></Card>
          )}

          {/* Full table */}
          {demandSupply.length > 0 && (
            <Card>
              <CH t="Demand-Supply Detail" r={`${demandSupply.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","OUD Prevalence","SUD Facilities","Facilities/100K","Detox","Residential Beds","Enrollment","Gap Risk"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {dsSorted.map((r, i) => {
                      const highPrev = r.oud_prevalence_pct > dsAvgPrevalence;
                      const lowFac = r.facilities_per_100k < dsAvgFacilities;
                      const risk = highPrev && lowFac ? "High" : highPrev || lowFac ? "Medium" : "Low";
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:highPrev ? NEG : A }}>{fmt(r.oud_prevalence_pct)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.sud_facility_count)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:lowFac ? NEG : POS }}>{fmt(r.facilities_per_100k, 1)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.detox_facilities)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.residential_beds)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_enrollment)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",fontWeight:600,color:risk === "High" ? NEG : risk === "Medium" ? WARN : POS }}>{risk}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: MAT UTILIZATION                                        */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "MAT Utilization" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={matData.length} />
            </Card>
            <Card accent={cB}>
              <Met l="Total MAT Spending" v={fmtD(matTotalSpending)} sub="Medicaid MAT claims" />
            </Card>
            <Card accent={cB}>
              <Met l="Total Prescriptions" v={fmtK(matTotalRx)} sub="MAT prescriptions" />
            </Card>
            <Card accent={POS}>
              <Met l="Highest Spending" v={matSorted.length ? fmtD(matSorted[0].mat_total_spending) : "\u2014"} sub={matSorted.length ? STATE_NAMES[matSorted[0].state_code] || "" : ""} />
            </Card>
          </div>

          {/* MAT spending bar chart */}
          {matChart.length > 0 ? (
            <Card>
              <CH t="MAT Spending by State" b="Medication-assisted treatment" r={`Top 30 of ${matData.length}`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="treatment-gap-mat">
                  <div style={{ width:"100%",height:Math.max(360, matChart.length * 18) }}>
                    <ResponsiveContainer>
                      <BarChart data={matChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Spending: {fmtD(d.mat_total_spending)}</div>
                              <div style={{ color:AL }}>Prescriptions: {fmtK(d.mat_prescriptions)}</div>
                              <div style={{ color:AL }}>Units: {fmtK(d.mat_units)}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="mat_total_spending" radius={[0,3,3,0]} maxBarSize={14} fill={cB}>
                          {matChart.map((_, i) => (
                            <Cell key={i} fill={cB} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No MAT utilization data available.</div></Card>
          )}

          {/* MAT table with per-enrollee */}
          {matWithPerEnrollee.length > 0 && (
            <Card>
              <CH t="MAT Spending Detail" b="With per-enrollee estimates" r={`${matData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","MAT Spending","Prescriptions","Units","MAT per Enrollee"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {matWithPerEnrollee.map((r, i) => (
                      <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.name}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.mat_total_spending)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.mat_prescriptions)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.mat_units)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.per_enrollee != null ? A : AL }}>{r.per_enrollee != null ? fmtD(r.per_enrollee) : "\u2014"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: PRESCRIBING PATTERNS                                   */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Prescribing Patterns" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={NEG}>
              <Met l="Highest Rate" v={prescribingLatest.length ? `${fmt(prescribingLatest[0].opioid_prescribing_rate, 2)}%` : "\u2014"} sub={prescribingLatest.length ? STATE_NAMES[prescribingLatest[0].state_code] || "" : ""} />
            </Card>
            <Card accent={POS}>
              <Met l="Lowest Rate" v={prescribingLatest.length ? `${fmt(prescribingLatest[prescribingLatest.length - 1].opioid_prescribing_rate, 2)}%` : "\u2014"} sub={prescribingLatest.length ? STATE_NAMES[prescribingLatest[prescribingLatest.length - 1].state_code] || "" : ""} />
            </Card>
            <Card accent={cB}>
              <Met l="National Avg" v={`${fmt(prescribingNatAvg, 2)}%`} sub={latestYear ? `CY${latestYear}` : ""} />
            </Card>
            <Card accent={WARN}>
              <Met l="States Above Avg" v={statesAboveAvg} sub={`of ${prescribingLatest.length} reporting`} />
            </Card>
          </div>

          {/* Prescribing bar chart */}
          {prescribingChart.length > 0 ? (
            <Card>
              <CH t="Opioid Prescribing Rate by State" b="Medicaid claims" r={latestYear ? `CY${latestYear} - Top 30` : ""} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="treatment-gap-prescribing">
                  <div style={{ width:"100%",height:Math.max(360, prescribingChart.length * 18) }}>
                    <ResponsiveContainer>
                      <BarChart data={prescribingChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Prescribing Rate: {fmt(d.opioid_prescribing_rate, 2)}%</div>
                              <div style={{ color:AL }}>Opioid Claims: {fmtK(d.opioid_claims)}</div>
                              <div style={{ color:AL }}>Total Claims: {fmtK(d.total_claims)}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="opioid_prescribing_rate" radius={[0,3,3,0]} maxBarSize={14}>
                          {prescribingChart.map((d, i) => (
                            <Cell key={i} fill={d.opioid_prescribing_rate > prescribingNatAvg ? NEG : POS} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No prescribing data available.</div></Card>
          )}

          {/* Prescribing table */}
          {prescribingLatest.length > 0 && (
            <Card>
              <CH t="Opioid Prescribing Detail" r={`${prescribingLatest.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Prescribing Rate","Opioid Claims","Total Claims","Opioid Share","vs Avg"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {prescribingLatest.map((r, i) => {
                      const diff = r.opioid_prescribing_rate - prescribingNatAvg;
                      const share = r.total_claims ? (r.opioid_claims / r.total_claims) * 100 : null;
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:r.opioid_prescribing_rate > prescribingNatAvg ? NEG : A }}>{fmt(r.opioid_prescribing_rate, 2)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.opioid_claims)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_claims)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{share != null ? `${fmt(share, 2)}%` : "\u2014"}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:diff > 0 ? NEG : POS,fontWeight:500 }}>
                            {diff > 0 ? "+" : ""}{fmt(diff, 2)}pp
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 4: FUNDING ALIGNMENT                                      */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Funding Alignment" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={fundingData.length} />
            </Card>
            <Card accent={cB}>
              <Met l="Total Block Grants" v={fmtD(fundingTotalGrant)} sub="SAMHSA SAPT allotments" />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Grant/Enrollee" v={fmtD(fundingAvgGrant)} />
            </Card>
            <Card accent={NEG}>
              <Met l="Underfunded States" v={underfundedStates.length} sub="High prevalence, low funding" />
            </Card>
          </div>

          {/* Underfunded alert */}
          {underfundedStates.length > 0 && (
            <Card accent={NEG}>
              <CH t="Funding Mismatch" b="States with above-average prevalence but below-average funding per enrollee" />
              <div style={{ padding:"8px 14px 12px",display:"flex",gap:6,flexWrap:"wrap" }}>
                {underfundedStates.map(r => (
                  <span key={r.state_code} style={{
                    fontSize:10,fontFamily:FM,padding:"3px 8px",borderRadius:4,
                    background:`${NEG}10`,border:`1px solid ${NEG}30`,color:NEG,fontWeight:600,
                  }}>
                    {STATE_NAMES[r.state_code] || r.state_code} ({fmt(r.prevalence_pct)}% / {fmtD(r.grant_per_enrollee)}/enrollee)
                  </span>
                ))}
              </div>
            </Card>
          )}

          {/* Scatter chart */}
          {fundingChart.length > 0 ? (
            <Card>
              <CH t="OUD Prevalence vs Federal Funding" b="Does money follow need?" r={`${fundingChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="treatment-gap-funding">
                  <div style={{ width:"100%",height:400 }}>
                    <ResponsiveContainer>
                      <ScatterChart margin={{ left:20,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis type="number" dataKey="prevalence_pct" name="OUD Prevalence" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} label={{ value:"OUD Prevalence %",position:"insideBottom",offset:-5,fontSize:9,fill:AL }} />
                        <YAxis type="number" dataKey="grant_per_enrollee" name="Grant/Enrollee" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} label={{ value:"Block Grant per Enrollee",angle:-90,position:"insideLeft",offset:10,fontSize:9,fill:AL }} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.name}</div>
                              <div style={{ color:AL }}>Prevalence: {fmt(d.prevalence_pct)}%</div>
                              <div style={{ color:AL }}>Grant/Enrollee: {fmtD(d.grant_per_enrollee)}</div>
                              <div style={{ color:AL }}>Total Grant: {fmtD(d.total_block_grant)}</div>
                              <div style={{ color:AL }}>Enrollment: {fmtK(d.total_enrollment)}</div>
                            </div>
                          );
                        }} />
                        <Scatter data={fundingChart} fillOpacity={0.7}>
                          {fundingChart.map((d, i) => {
                            const highPrev = d.prevalence_pct > fundingAvgPrevalence;
                            const lowFund = d.grant_per_enrollee < fundingAvgGrant;
                            return <Cell key={i} fill={highPrev && lowFund ? NEG : highPrev || lowFund ? WARN : POS} fillOpacity={0.7} />;
                          })}
                        </Scatter>
                      </ScatterChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
                <div style={{ fontSize:9,color:AL,fontFamily:FM,marginTop:4,textAlign:"center" }}>
                  Red = high prevalence + low funding. Yellow = one concern. Green = well-aligned.
                </div>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No funding data available.</div></Card>
          )}

          {/* Funding table */}
          {fundingData.length > 0 && (
            <Card>
              <CH t="Funding Alignment Detail" r={`${fundingData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","OUD Prevalence","Total Block Grant","Enrollment","Grant/Enrollee","Alignment"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...fundingData].sort((a, b) => b.prevalence_pct - a.prevalence_pct).map((r, i) => {
                      const highPrev = r.prevalence_pct > fundingAvgPrevalence;
                      const lowFund = r.grant_per_enrollee < fundingAvgGrant;
                      const alignment = highPrev && lowFund ? "Underfunded" : !highPrev && !lowFund ? "Well-funded" : highPrev ? "Watch" : "Adequate";
                      const color = alignment === "Underfunded" ? NEG : alignment === "Watch" ? WARN : POS;
                      return (
                        <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:highPrev ? NEG : A }}>{fmt(r.prevalence_pct)}%</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.total_block_grant)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_enrollment)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:lowFund ? NEG : A }}>{fmtD(r.grant_per_enrollee)}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",fontWeight:600,color }}>{alignment}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}

      {/* Ask Aradune */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Opioid Treatment Gap ${tab} data. ${demandSupply.length} states in demand-supply analysis. ${gapStates.length} states identified with treatment gaps. ${matData.length} states with MAT data. ${prescribingLatest.length} states in prescribing data.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Aradune about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: SAMHSA NSDUH (2023-2024) &middot; State Drug Utilization Data (2025) &middot; N-SUMHSS Facility Directory &middot; TEDS Admissions &middot; SAMHSA Block Grants
      </div>
    </div>
  );
}
