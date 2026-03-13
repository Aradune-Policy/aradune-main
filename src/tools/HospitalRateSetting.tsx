import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import { API_BASE } from "../lib/api";
import { LoadingBar } from "../components/LoadingBar";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";

// ── Design System ─────────────────────────────────────────────────────
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

// ── Interfaces ────────────────────────────────────────────────────────
interface HospSummary { state_code: string; hospital_count: number; total_beds: number; total_medicaid_days: number; total_patient_days: number; medicaid_day_pct: number; total_medicaid_revenue: number; total_uncompensated_care: number; total_dsh: number; median_ccr: number; report_year: number }
interface DshSummary { state: string; total_hospitals: number; dsh_recipients: number; total_dsh_m: number; total_ime_m: number; total_uc_m: number; avg_medicaid_day_pct: number; high_medicaid_hospitals: number }
interface SuppSummary { state: string; fiscal_year: number; total_hospital_payments_m: number; dsh_payments_m: number; non_dsh_supplemental_m: number; sec_1115_waiver_m: number; supplemental_pct: number }
interface SdpRow { state: string; program_name: string; service_category: string; payment_type: string; fiscal_year: string; authority: string }

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

const fmtD = (n: number | null | undefined) => {
  if (n == null) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toLocaleString()}`;
};
const fmtK = (n: number | null | undefined) => n == null ? "—" : n >= 1e9 ? `${(n / 1e9).toFixed(1)}B` : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}K` : n.toLocaleString();
const fmt = (n: number | null | undefined, d = 1) => n == null ? "—" : n.toFixed(d);

const SafeTip = ({ active, payload, label, formatter }: { active?: boolean; payload?: Array<{ value: number; dataKey: string }>; label?: string; formatter?: (v: number) => string }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
      <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{label}</div>
      {payload.map((p, i) => <div key={i} style={{ color:AL }}>{formatter ? formatter(p.value) : p.value}</div>)}
    </div>
  );
};

const TABS = ["Hospital Financials", "DSH & Supplemental", "State Directed Payments"] as const;
type Tab = typeof TABS[number];

// ═══════════════════════════════════════════════════════════════════════
//  HOSPITAL / INSTITUTIONAL RATE SETTING MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function HospitalRateSetting() {
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Hospital Financials");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const isMobile = typeof window !== "undefined" && window.innerWidth < 768;

  const [hospSummary, setHospSummary] = useState<HospSummary[]>([]);
  const [dshSummary, setDshSummary] = useState<DshSummary[]>([]);
  const [suppSummary, setSuppSummary] = useState<SuppSummary[]>([]);
  const [sdpData, setSdpData] = useState<SdpRow[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    const load = async () => {
      try {
        if (tab === "Hospital Financials") {
          const d = await fetchJson("/api/hospitals/summary");
          // Returns array, not {rows}
          setHospSummary(Array.isArray(d) ? d : d.rows || []);
        } else if (tab === "DSH & Supplemental") {
          const [dsh, supp] = await Promise.all([
            fetchJson("/api/supplemental/dsh/summary"),
            fetchJson("/api/supplemental/summary"),
          ]);
          setDshSummary(dsh.rows || []);
          setSuppSummary(supp.states || []);
        } else if (tab === "State Directed Payments") {
          const d = await fetchJson("/api/supplemental/sdp");
          setSdpData(d.rows || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, fetchJson]);

  // ── Computed ──
  const totalHospitals = useMemo(() => hospSummary.reduce((s, r) => s + (r.hospital_count || 0), 0), [hospSummary]);
  const totalBeds = useMemo(() => hospSummary.reduce((s, r) => s + (r.total_beds || 0), 0), [hospSummary]);
  const totalMedicaidRev = useMemo(() => hospSummary.reduce((s, r) => s + (r.total_medicaid_revenue || 0), 0), [hospSummary]);
  const totalUC = useMemo(() => hospSummary.reduce((s, r) => s + (r.total_uncompensated_care || 0), 0), [hospSummary]);

  const hospChart = useMemo(() =>
    [...hospSummary]
      .sort((a, b) => (b.total_medicaid_revenue || 0) - (a.total_medicaid_revenue || 0))
      .slice(0, 25)
      .map(r => ({
        state: r.state_code,
        name: STATE_NAMES[r.state_code] || r.state_code,
        rev: r.total_medicaid_revenue || 0,
      })),
  [hospSummary]);

  const totalDshAllStates = useMemo(() => dshSummary.reduce((s, r) => s + (r.total_dsh_m || 0), 0), [dshSummary]);
  const totalImeAllStates = useMemo(() => dshSummary.reduce((s, r) => s + (r.total_ime_m || 0), 0), [dshSummary]);
  const totalUcAllStates = useMemo(() => dshSummary.reduce((s, r) => s + (r.total_uc_m || 0), 0), [dshSummary]);

  const dshChart = useMemo(() =>
    dshSummary.slice(0, 25).map(r => ({
      state: r.state,
      name: STATE_NAMES[r.state] || r.state,
      dsh: r.total_dsh_m || 0,
    })),
  [dshSummary]);

  const sdpByState = useMemo(() => {
    const map: Record<string, number> = {};
    sdpData.forEach(r => { map[r.state] = (map[r.state] || 0) + 1; });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [sdpData]);

  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Hospital & Institutional Rate Setting</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>HCRIS + DSH + FMR + SDP</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Hospital cost reports, DSH allotments, supplemental payments, and state directed payment programs across all states.
        </p>
      </div>

      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: HOSPITAL FINANCIALS                                    */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Hospital Financials" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Hospitals" v={fmtK(totalHospitals)} sub="HCRIS cost reports" />
            </Card>
            <Card accent="#6366F1">
              <Met l="Total Beds" v={fmtK(totalBeds)} />
            </Card>
            <Card accent={WARN}>
              <Met l="Medicaid Revenue" v={fmtD(totalMedicaidRev)} sub="Net patient revenue" />
            </Card>
            <Card accent={NEG}>
              <Met l="Uncompensated Care" v={fmtD(totalUC)} />
            </Card>
          </div>

          <Card>
            <CH t="Medicaid Net Revenue by State" b="HCRIS" r={`Top 25 of ${hospSummary.length}`} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename="hosp-medicaid-revenue">
                <div style={{ width:"100%",height:Math.max(360, hospChart.length * 20) }}>
                  <ResponsiveContainer>
                    <BarChart data={hospChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                      <Tooltip content={<SafeTip formatter={v => fmtD(v)} />} />
                      <Bar dataKey="rev" fill={cB} radius={[0,3,3,0]} maxBarSize={14}>
                        {hospChart.map((_, i) => <Cell key={i} fill={i < 5 ? "#C4590A" : cB} fillOpacity={0.8} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartActions>
            </div>
          </Card>

          <Card>
            <CH t="Hospital Financial Summary" r={`${hospSummary.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["State","Hospitals","Beds","Medicaid Days","Medicaid %","Medicaid Rev","UC Cost","DSH","Median CCR"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {hospSummary.map(r => (
                    <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.hospital_count}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_beds)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_medicaid_days)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:r.medicaid_day_pct > 20 ? WARN : AL }}>{fmt(r.medicaid_day_pct)}%</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmtD(r.total_medicaid_revenue)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:NEG }}>{fmtD(r.total_uncompensated_care)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD(r.total_dsh)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.median_ccr, 4)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: DSH & SUPPLEMENTAL                                     */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "DSH & Supplemental" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total DSH" v={`$${fmt(totalDshAllStates / 1e3, 1)}B`} sub="All states (millions)" />
            </Card>
            <Card accent="#6366F1">
              <Met l="Total IME" v={`$${fmt(totalImeAllStates / 1e3, 1)}B`} sub="Indirect Medical Education" />
            </Card>
            <Card accent={NEG}>
              <Met l="Uncompensated Care" v={`$${fmt(totalUcAllStates / 1e3, 1)}B`} />
            </Card>
            <Card accent={POS}>
              <Met l="States" v={dshSummary.length} />
            </Card>
          </div>

          <Card>
            <CH t="DSH Payments by State" b="Hospital-level HCRIS" r={`Top 25 of ${dshSummary.length}`} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename="hosp-dsh-by-state">
                <div style={{ width:"100%",height:Math.max(360, dshChart.length * 20) }}>
                  <ResponsiveContainer>
                    <BarChart data={dshChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => `$${v}M`} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                      <Tooltip content={<SafeTip formatter={v => `$${v.toFixed(1)}M`} />} />
                      <Bar dataKey="dsh" fill="#C4590A" radius={[0,3,3,0]} maxBarSize={14} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </ChartActions>
            </div>
          </Card>

          {/* MACPAC Supplemental */}
          {suppSummary.length > 0 && (
            <Card>
              <CH t="MACPAC Hospital Supplemental Payments" b="Exhibit 24" r={`${suppSummary.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["State","Total Hosp ($M)","DSH ($M)","Non-DSH Supp ($M)","1115 Waiver ($M)","Supp %"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {suppSummary.map(r => (
                      <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmtD((r.total_hospital_payments_m || 0) * 1e6)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD((r.dsh_payments_m || 0) * 1e6)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD((r.non_dsh_supplemental_m || 0) * 1e6)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD((r.sec_1115_waiver_m || 0) * 1e6)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.supplemental_pct > 50 ? NEG : AL }}>{fmt(r.supplemental_pct)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {/* DSH detail table */}
          <Card>
            <CH t="DSH & IME Detail by State" r={`${dshSummary.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["State","Hospitals","DSH Recipients","DSH ($M)","IME ($M)","UC ($M)","Avg Medicaid %","High Medicaid"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dshSummary.map(r => (
                    <tr key={r.state} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.total_hospitals}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.dsh_recipients}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>${fmt(r.total_dsh_m)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>${fmt(r.total_ime_m)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:NEG }}>${fmt(r.total_uc_m)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_medicaid_day_pct > 20 ? WARN : AL }}>{fmt(r.avg_medicaid_day_pct)}%</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.high_medicaid_hospitals}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: STATE DIRECTED PAYMENTS                                */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "State Directed Payments" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(3,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total SDP Programs" v={sdpData.length} sub="CMS-approved" />
            </Card>
            <Card accent="#6366F1">
              <Met l="States with SDPs" v={sdpByState.length} />
            </Card>
            <Card accent={WARN}>
              <Met l="Most Programs" v={sdpByState.length ? `${sdpByState[0][1]}` : "—"} sub={sdpByState.length ? STATE_NAMES[sdpByState[0][0]] || sdpByState[0][0] : ""} />
            </Card>
          </div>

          {/* SDP by state chart */}
          {sdpByState.length > 0 && (
            <Card>
              <CH t="State Directed Payment Programs by State" r={`${sdpByState.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="hosp-sdp-by-state">
                  <div style={{ width:"100%",height:Math.max(300, Math.min(sdpByState.length, 25) * 20) }}>
                    <ResponsiveContainer>
                      <BarChart data={sdpByState.slice(0, 25).map(([st, ct]) => ({ name: STATE_NAMES[st] || st, count: ct }))} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={<SafeTip formatter={v => `${v} programs`} />} />
                        <Bar dataKey="count" fill="#6366F1" radius={[0,3,3,0]} maxBarSize={14} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          )}

          {/* Full SDP table */}
          <Card>
            <CH t="State Directed Payment Programs" r={`${sdpData.length} programs`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["State","Program Name","Service Category","Payment Type","FY","Authority"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:"left",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sdpData.map((r, i) => (
                    <tr key={`${r.state}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state] || r.state}</td>
                      <td style={{ padding:"5px 10px",color:A,maxWidth:220,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.program_name}</td>
                      <td style={{ padding:"5px 10px",color:AL }}>{r.service_category}</td>
                      <td style={{ padding:"5px 10px",color:AL }}>{r.payment_type}</td>
                      <td style={{ padding:"5px 10px",color:AL }}>{r.fiscal_year}</td>
                      <td style={{ padding:"5px 10px",color:AL }}>{r.authority}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Hospital Rate Setting - ${tab}. ${hospSummary.length} states in HCRIS, ${dshSummary.length} states with DSH data, ${sdpData.length} SDP programs.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS HCRIS Cost Reports (6,103 hospitals) · DSH Hospital-Level Data · MACPAC Exhibit 24 · CMS State Directed Payment Preprints
      </div>
    </div>
  );
}
