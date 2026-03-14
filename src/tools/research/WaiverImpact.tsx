import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, LineChart, Line, Legend, ReferenceLine } from "recharts";
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
const STATE_CODES = Object.keys(STATE_NAMES).sort();

// ── Interfaces ─────────────────────────────────────────────────────────
interface WaiverRow { state_code: string; waiver_title: string; waiver_type: string; approval_date: string; effective_date: string; expiration_date: string; waiver_status: string; key_provisions: string }
interface EnrollmentRow { year: number; month: number; total_enrollment: number; chip_enrollment: number; ffs_enrollment: number; mc_enrollment: number }
interface SpendingRow { fiscal_year: number; total_spending: number; federal_share: number; state_share: number }
interface QualityRow { data_year: number; measure_id: string; measure_name: string; measure_rate: number }

// ── Shared Components ─────────────────────────────────────────────────
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
        <div key={i} style={{ color:AL }}>{p.name || p.dataKey}: {formatter ? formatter(p.value, p.dataKey) : p.value}</div>
      ))}
    </div>
  );
};

// ── Tabs ──────────────────────────────────────────────────────────────
const TABS = ["Waiver Catalog", "Enrollment Impact", "Spending Impact", "Quality Trajectory"] as const;
type Tab = typeof TABS[number];

const STATUS_FILTERS = ["All", "Approved", "Pending", "Expired"] as const;
type StatusFilter = typeof STATUS_FILTERS[number];

// ═══════════════════════════════════════════════════════════════════════
//  SECTION 1115 WAIVER IMPACT MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function WaiverImpact() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Waiver Catalog");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Shared state: selected state for tabs 2-4 ──
  const [selectedState, setSelectedState] = useState<string>("");

  // ── Catalog state ──
  const [catalog, setCatalog] = useState<WaiverRow[]>([]);
  const [catalogSearch, setCatalogSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("All");

  // ── Enrollment state ──
  const [enrollmentData, setEnrollmentData] = useState<EnrollmentRow[]>([]);

  // ── Spending state ──
  const [spendingData, setSpendingData] = useState<SpendingRow[]>([]);

  // ── Quality state ──
  const [qualityData, setQualityData] = useState<QualityRow[]>([]);
  const [selectedMeasure, setSelectedMeasure] = useState<string>("");

  // ── Fetch helper ──
  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load catalog on mount ──
  useEffect(() => {
    setLoading(true);
    setError(null);
    fetchJson("/api/research/waiver-impact/catalog")
      .then(d => setCatalog(d.rows || d.data || []))
      .catch(e => setError(e instanceof Error ? e.message : "Failed to load catalog"))
      .finally(() => setLoading(false));
  }, [fetchJson]);

  // ── Load state-dependent data ──
  useEffect(() => {
    if (tab === "Waiver Catalog") return;
    if (!selectedState) return;
    setLoading(true);
    setError(null);
    const load = async () => {
      try {
        if (tab === "Enrollment Impact") {
          const d = await fetchJson(`/api/research/waiver-impact/enrollment/${selectedState}`);
          setEnrollmentData(d.rows || d.data || []);
        } else if (tab === "Spending Impact") {
          const d = await fetchJson(`/api/research/waiver-impact/spending/${selectedState}`);
          setSpendingData(d.rows || d.data || []);
        } else if (tab === "Quality Trajectory") {
          const d = await fetchJson(`/api/research/waiver-impact/quality/${selectedState}`);
          const rows = d.rows || d.data || [];
          setQualityData(rows);
          if (rows.length && !selectedMeasure) {
            setSelectedMeasure(rows[0].measure_id);
          }
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, selectedState, fetchJson]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Computed: Filtered catalog ──
  const filteredCatalog = useMemo(() => {
    let rows = catalog;
    if (statusFilter !== "All") {
      rows = rows.filter(r => r.waiver_status?.toLowerCase().includes(statusFilter.toLowerCase()));
    }
    if (catalogSearch.trim()) {
      const q = catalogSearch.toLowerCase();
      rows = rows.filter(r =>
        r.waiver_title?.toLowerCase().includes(q) ||
        r.state_code?.toLowerCase().includes(q) ||
        (STATE_NAMES[r.state_code] || "").toLowerCase().includes(q) ||
        r.key_provisions?.toLowerCase().includes(q)
      );
    }
    return rows;
  }, [catalog, statusFilter, catalogSearch]);

  const catalogMetrics = useMemo(() => {
    const approved = catalog.filter(r => r.waiver_status?.toLowerCase().includes("approved")).length;
    const pending = catalog.filter(r => r.waiver_status?.toLowerCase().includes("pending")).length;
    const expired = catalog.filter(r => r.waiver_status?.toLowerCase().includes("expired")).length;
    return { total: catalog.length, approved, pending, expired };
  }, [catalog]);

  // ── Computed: Enrollment chart ──
  const enrollmentChart = useMemo(() =>
    [...enrollmentData]
      .sort((a, b) => (a.year * 100 + a.month) - (b.year * 100 + b.month))
      .map(r => ({
        ...r,
        label: `${r.year}-${String(r.month).padStart(2, "0")}`,
      })),
  [enrollmentData]);

  // ── Computed: Waiver approval date for reference line ──
  const waiverApprovalDate = useMemo(() => {
    if (!selectedState) return null;
    const stateWaivers = catalog.filter(r => r.state_code === selectedState && r.approval_date);
    if (!stateWaivers.length) return null;
    const sorted = [...stateWaivers].sort((a, b) => a.approval_date.localeCompare(b.approval_date));
    const d = sorted[0].approval_date;
    return d ? d.slice(0, 7) : null;
  }, [catalog, selectedState]);

  // ── Computed: Spending chart ──
  const spendingChart = useMemo(() =>
    [...spendingData].sort((a, b) => a.fiscal_year - b.fiscal_year),
  [spendingData]);

  const spendingGrowth = useMemo(() => {
    if (spendingChart.length < 2) return { total: 0, state: 0 };
    const first = spendingChart[0];
    const last = spendingChart[spendingChart.length - 1];
    const years = last.fiscal_year - first.fiscal_year || 1;
    const totalGrowth = first.total_spending > 0 ? ((last.total_spending / first.total_spending) ** (1 / years) - 1) * 100 : 0;
    const stateGrowth = first.state_share > 0 ? ((last.state_share / first.state_share) ** (1 / years) - 1) * 100 : 0;
    return { total: totalGrowth, state: stateGrowth };
  }, [spendingChart]);

  // ── Computed: Quality measures list ──
  const qualityMeasures = useMemo(() => {
    const seen = new Map<string, string>();
    qualityData.forEach(r => { if (!seen.has(r.measure_id)) seen.set(r.measure_id, r.measure_name); });
    return Array.from(seen.entries()).map(([id, name]) => ({ id, name }));
  }, [qualityData]);

  // ── Computed: Quality chart for selected measure ──
  const qualityChart = useMemo(() => {
    if (!selectedMeasure) return [];
    return [...qualityData]
      .filter(r => r.measure_id === selectedMeasure)
      .sort((a, b) => a.data_year - b.data_year);
  }, [qualityData, selectedMeasure]);

  const selectedMeasureName = qualityMeasures.find(m => m.id === selectedMeasure)?.name || selectedMeasure;

  // ── State selector component ──
  const StateSelector = () => (
    <Card>
      <CH t="Select State" b="Choose a state to view impact analysis" />
      <div style={{ padding:"8px 14px 12px" }}>
        <select
          value={selectedState}
          onChange={e => setSelectedState(e.target.value)}
          style={{ fontSize:11,fontFamily:FM,padding:"6px 10px",borderRadius:6,border:`1px solid ${BD}`,color:A,background:WH,minWidth:200 }}
        >
          <option value="">-- Select a state --</option>
          {STATE_CODES.map(sc => (
            <option key={sc} value={sc}>{STATE_NAMES[sc]} ({sc})</option>
          ))}
        </select>
      </div>
    </Card>
  );

  const NoStateMessage = () => (
    <Card>
      <div style={{ padding:32,textAlign:"center",fontSize:12,color:AL,fontFamily:FM }}>
        Select a state above to view impact analysis
      </div>
    </Card>
  );

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Section 1115 Waiver Impact</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>KFF Waivers + CMS-64 + Core Set + Enrollment</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Quasi-experimental evaluation of Section 1115 waiver effectiveness. Before/after analysis of enrollment, spending, and quality outcomes for expansion, work requirement, SUD, and HCBS waivers.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {/* State selector for tabs 2-4 */}
      {tab !== "Waiver Catalog" && (
        <div style={{ marginBottom:14 }}>
          <StateSelector />
        </div>
      )}

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: WAIVER CATALOG                                         */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Waiver Catalog" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total Waivers" v={catalogMetrics.total} />
            </Card>
            <Card accent={POS}>
              <Met l="Approved" v={catalogMetrics.approved} cl={POS} />
            </Card>
            <Card accent={WARN}>
              <Met l="Pending" v={catalogMetrics.pending} cl={WARN} />
            </Card>
            <Card accent={AL}>
              <Met l="Expired" v={catalogMetrics.expired} />
            </Card>
          </div>

          {/* Search and filter */}
          <Card>
            <CH t="Search & Filter" b={`${filteredCatalog.length} of ${catalog.length} waivers`} />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:10,flexWrap:"wrap",alignItems:"center" }}>
              <input
                type="text"
                value={catalogSearch}
                onChange={e => setCatalogSearch(e.target.value)}
                placeholder="Search by title, state, or provisions..."
                style={{ flex:1,minWidth:200,padding:"6px 10px",borderRadius:6,border:`1px solid ${BD}`,fontSize:11,fontFamily:FM,color:A,outline:"none" }}
              />
              <div style={{ display:"flex",gap:4 }}>
                {STATUS_FILTERS.map(s => (
                  <Pill key={s} label={s} active={statusFilter === s} onClick={() => setStatusFilter(s)} />
                ))}
              </div>
            </div>
          </Card>

          {/* Waiver table */}
          {filteredCatalog.length > 0 ? (
            <Card>
              <CH t="Waiver Catalog" r={`${filteredCatalog.length} waivers`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Title","Type","Approval","Effective","Expiration","Status"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="Title"||h==="State"||h==="Type"||h==="Status"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredCatalog.slice(0, 200).map((r, i) => {
                      const statusColor = r.waiver_status?.toLowerCase().includes("approved") ? POS
                        : r.waiver_status?.toLowerCase().includes("pending") ? WARN
                        : r.waiver_status?.toLowerCase().includes("expired") ? AL : A;
                      return (
                        <tr key={`${r.state_code}-${r.waiver_title}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                          <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                          <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                          <td style={{ padding:"5px 10px",color:A,maxWidth:280 }}>
                            <div style={{ overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.waiver_title}</div>
                            {r.key_provisions && <div style={{ fontSize:8,color:AL,marginTop:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:260 }}>{r.key_provisions}</div>}
                          </td>
                          <td style={{ padding:"5px 10px",color:AL }}>{r.waiver_type}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL,fontSize:9 }}>{r.approval_date || "\u2014"}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL,fontSize:9 }}>{r.effective_date || "\u2014"}</td>
                          <td style={{ padding:"5px 10px",textAlign:"right",color:AL,fontSize:9 }}>{r.expiration_date || "\u2014"}</td>
                          <td style={{ padding:"5px 10px",color:statusColor,fontWeight:500 }}>{r.waiver_status}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                {filteredCatalog.length > 200 && (
                  <div style={{ padding:"8px 14px",fontSize:10,color:AL,fontFamily:FM,textAlign:"center" }}>
                    Showing 200 of {filteredCatalog.length} waivers. Refine your search for more specific results.
                  </div>
                )}
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No waivers match your search.</div></Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: ENROLLMENT IMPACT                                      */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Enrollment Impact" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {!selectedState ? <NoStateMessage /> : (
            <>
              {/* Metrics */}
              <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
                <Card accent={cB}>
                  <Met l="State" v={STATE_NAMES[selectedState] || selectedState} />
                </Card>
                <Card accent={cB}>
                  <Met l="Data Points" v={enrollmentChart.length} sub="Monthly observations" />
                </Card>
                <Card accent={cB}>
                  <Met l="Latest Enrollment" v={enrollmentChart.length ? fmtK(enrollmentChart[enrollmentChart.length - 1]?.total_enrollment) : "\u2014"} sub={enrollmentChart.length ? enrollmentChart[enrollmentChart.length - 1]?.label : ""} />
                </Card>
                <Card accent={waiverApprovalDate ? POS : AL}>
                  <Met l="Waiver Approval" v={waiverApprovalDate || "Not found"} sub={waiverApprovalDate ? "Earliest 1115 approval" : "No waiver in catalog"} />
                </Card>
              </div>

              {/* Line chart */}
              {enrollmentChart.length > 0 ? (
                <Card>
                  <CH t={`${STATE_NAMES[selectedState] || selectedState} Enrollment Trajectory`} b="Monthly total, FFS, and managed care" r={`${enrollmentChart.length} months`} />
                  <div style={{ padding:"8px 14px 14px" }}>
                    <ChartActions filename={`waiver-enrollment-${selectedState}`}>
                      <div style={{ width:"100%",height:360 }}>
                        <ResponsiveContainer>
                          <LineChart data={enrollmentChart} margin={{ left:20,right:20,top:10,bottom:10 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                            <XAxis dataKey="label" tick={{ fontSize:8,fill:AL }} interval={Math.max(0, Math.floor(enrollmentChart.length / 12))} angle={-45} textAnchor="end" height={50} />
                            <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtK(v)} />
                            <Tooltip content={<SafeTip formatter={(v: number) => fmtK(v)} />} />
                            <Legend wrapperStyle={{ fontSize:9,fontFamily:FM }} />
                            {waiverApprovalDate && (
                              <ReferenceLine x={waiverApprovalDate} stroke={WARN} strokeDasharray="5 5" label={{ value:"Waiver",fill:WARN,fontSize:9,position:"top" }} />
                            )}
                            <Line type="monotone" dataKey="total_enrollment" name="Total" stroke={A} strokeWidth={2} dot={false} />
                            <Line type="monotone" dataKey="mc_enrollment" name="Managed Care" stroke={cB} strokeWidth={1.5} dot={false} />
                            <Line type="monotone" dataKey="ffs_enrollment" name="FFS" stroke="#3B82F6" strokeWidth={1.5} dot={false} />
                          </LineChart>
                        </ResponsiveContainer>
                      </div>
                    </ChartActions>
                  </div>
                </Card>
              ) : (
                !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No enrollment data available for {STATE_NAMES[selectedState] || selectedState}.</div></Card>
              )}
            </>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: SPENDING IMPACT                                        */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Spending Impact" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {!selectedState ? <NoStateMessage /> : (
            <>
              {/* Metrics */}
              <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
                <Card accent={cB}>
                  <Met l="State" v={STATE_NAMES[selectedState] || selectedState} />
                </Card>
                <Card accent={cB}>
                  <Met l="Fiscal Years" v={spendingChart.length} sub={spendingChart.length >= 2 ? `FY${spendingChart[0]?.fiscal_year}-${spendingChart[spendingChart.length - 1]?.fiscal_year}` : ""} />
                </Card>
                <Card accent={spendingGrowth.total > 5 ? NEG : spendingGrowth.total > 0 ? WARN : POS}>
                  <Met l="Spending CAGR" v={`${fmt(spendingGrowth.total)}%`} sub="Total spending growth" cl={spendingGrowth.total > 5 ? NEG : WARN} />
                </Card>
                <Card accent={spendingGrowth.state > 5 ? NEG : WARN}>
                  <Met l="State Share CAGR" v={`${fmt(spendingGrowth.state)}%`} sub="State share growth" cl={spendingGrowth.state > 5 ? NEG : WARN} />
                </Card>
              </div>

              {/* Stacked bar chart */}
              {spendingChart.length > 0 ? (
                <Card>
                  <CH t={`${STATE_NAMES[selectedState] || selectedState} Spending by Fiscal Year`} b="Federal share (green) and state share (blue)" r={`FY${spendingChart[0]?.fiscal_year}-${spendingChart[spendingChart.length - 1]?.fiscal_year}`} />
                  <div style={{ padding:"8px 14px 14px" }}>
                    <ChartActions filename={`waiver-spending-${selectedState}`}>
                      <div style={{ width:"100%",height:320 }}>
                        <ResponsiveContainer>
                          <BarChart data={spendingChart} margin={{ left:20,right:20,top:10,bottom:10 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                            <XAxis dataKey="fiscal_year" tick={{ fontSize:10,fill:AL }} tickFormatter={v => `FY${v}`} />
                            <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                            <Tooltip content={({ active, payload, label }) => {
                              if (!active || !payload?.length) return null;
                              const d = payload[0]?.payload;
                              if (!d) return null;
                              return (
                                <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                                  <div style={{ fontWeight:600,color:A,marginBottom:2 }}>FY{d.fiscal_year}</div>
                                  <div style={{ color:AL }}>Total: {fmtD(d.total_spending)}</div>
                                  <div style={{ color:cB }}>Federal: {fmtD(d.federal_share)}</div>
                                  <div style={{ color:"#3B82F6" }}>State: {fmtD(d.state_share)}</div>
                                  <div style={{ color:AL }}>Federal %: {d.total_spending > 0 ? fmt((d.federal_share / d.total_spending) * 100) : "\u2014"}%</div>
                                </div>
                              );
                            }} />
                            <Legend wrapperStyle={{ fontSize:9,fontFamily:FM }} />
                            <Bar dataKey="federal_share" name="Federal Share" fill={cB} stackId="spending" radius={[0,0,0,0]} maxBarSize={40} />
                            <Bar dataKey="state_share" name="State Share" fill="#3B82F6" stackId="spending" radius={[3,3,0,0]} maxBarSize={40} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </ChartActions>
                  </div>
                </Card>
              ) : (
                !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No spending data available for {STATE_NAMES[selectedState] || selectedState}.</div></Card>
              )}

              {/* Spending table */}
              {spendingChart.length > 0 && (
                <Card>
                  <CH t="Annual Spending Detail" r={`${spendingChart.length} fiscal years`} />
                  <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                    <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                      <thead>
                        <tr style={{ borderBottom:`1px solid ${BD}` }}>
                          {["Fiscal Year","Total Spending","Federal Share","State Share","Federal %","YoY Change"].map(h => (
                            <th key={h} style={{ padding:"6px 10px",textAlign:h==="Fiscal Year"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {spendingChart.map((r, i) => {
                          const prev = i > 0 ? spendingChart[i - 1] : null;
                          const yoy = prev && prev.total_spending > 0 ? ((r.total_spending - prev.total_spending) / prev.total_spending) * 100 : null;
                          const fedPct = r.total_spending > 0 ? (r.federal_share / r.total_spending) * 100 : 0;
                          return (
                            <tr key={r.fiscal_year} style={{ borderBottom:`1px solid ${BD}20` }}>
                              <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>FY{r.fiscal_year}</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.total_spending)}</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:cB }}>{fmtD(r.federal_share)}</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:"#3B82F6" }}>{fmtD(r.state_share)}</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(fedPct)}%</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:yoy != null ? (yoy > 0 ? NEG : POS) : AL,fontWeight:500 }}>
                                {yoy != null ? `${yoy > 0 ? "+" : ""}${fmt(yoy)}%` : "\u2014"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </Card>
              )}
            </>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 4: QUALITY TRAJECTORY                                     */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Quality Trajectory" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {!selectedState ? <NoStateMessage /> : (
            <>
              {/* Measure selector */}
              {qualityMeasures.length > 0 && (
                <Card>
                  <CH t="Quality Measure" b="Select a Core Set measure to chart" />
                  <div style={{ padding:"8px 14px 12px" }}>
                    <select
                      value={selectedMeasure}
                      onChange={e => setSelectedMeasure(e.target.value)}
                      style={{ fontSize:10,fontFamily:FM,padding:"4px 8px",borderRadius:6,border:`1px solid ${BD}`,color:AL,background:WH,maxWidth:isMobile?"100%":500,width:"100%" }}
                    >
                      {qualityMeasures.map(m => <option key={m.id} value={m.id}>{m.name} ({m.id})</option>)}
                    </select>
                  </div>
                </Card>
              )}

              {/* Metrics */}
              <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
                <Card accent={cB}>
                  <Met l="State" v={STATE_NAMES[selectedState] || selectedState} />
                </Card>
                <Card accent={cB}>
                  <Met l="Measures Available" v={qualityMeasures.length} />
                </Card>
                <Card accent={cB}>
                  <Met l="Data Points" v={qualityChart.length} sub={selectedMeasureName} />
                </Card>
                <Card accent={qualityChart.length >= 2 && qualityChart[qualityChart.length - 1]?.measure_rate > qualityChart[0]?.measure_rate ? POS : qualityChart.length >= 2 ? NEG : AL}>
                  <Met l="Trend" v={qualityChart.length >= 2
                    ? `${(qualityChart[qualityChart.length - 1].measure_rate - qualityChart[0].measure_rate) > 0 ? "+" : ""}${fmt(qualityChart[qualityChart.length - 1].measure_rate - qualityChart[0].measure_rate)}pp`
                    : "\u2014"}
                    sub={qualityChart.length >= 2 ? `${qualityChart[0].data_year} to ${qualityChart[qualityChart.length - 1].data_year}` : ""}
                    cl={qualityChart.length >= 2 && qualityChart[qualityChart.length - 1].measure_rate > qualityChart[0].measure_rate ? POS : NEG}
                  />
                </Card>
              </div>

              {/* Line chart */}
              {qualityChart.length > 0 ? (
                <Card>
                  <CH t={`${STATE_NAMES[selectedState] || selectedState}: ${selectedMeasureName}`} b="Quality measure rate over time" r={`${qualityChart.length} years`} />
                  <div style={{ padding:"8px 14px 14px" }}>
                    <ChartActions filename={`waiver-quality-${selectedState}-${selectedMeasure}`}>
                      <div style={{ width:"100%",height:320 }}>
                        <ResponsiveContainer>
                          <LineChart data={qualityChart} margin={{ left:20,right:20,top:10,bottom:10 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                            <XAxis dataKey="data_year" tick={{ fontSize:10,fill:AL }} />
                            <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                            <Tooltip content={({ active, payload }) => {
                              if (!active || !payload?.length) return null;
                              const d = payload[0]?.payload;
                              if (!d) return null;
                              return (
                                <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                                  <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.data_year}</div>
                                  <div style={{ color:AL }}>{d.measure_name}: {fmt(d.measure_rate)}%</div>
                                </div>
                              );
                            }} />
                            {waiverApprovalDate && (
                              <ReferenceLine x={parseInt(waiverApprovalDate.slice(0, 4))} stroke={WARN} strokeDasharray="5 5" label={{ value:"Waiver",fill:WARN,fontSize:9,position:"top" }} />
                            )}
                            <Line type="monotone" dataKey="measure_rate" name={selectedMeasureName} stroke={cB} strokeWidth={2} dot={{ r:3,fill:cB }} activeDot={{ r:5 }} />
                          </LineChart>
                        </ResponsiveContainer>
                      </div>
                    </ChartActions>
                  </div>
                </Card>
              ) : (
                !loading && qualityMeasures.length === 0 && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No quality data available for {STATE_NAMES[selectedState] || selectedState}.</div></Card>
              )}

              {/* Quality data table */}
              {qualityChart.length > 0 && (
                <Card>
                  <CH t="Quality Measure Data" r={`${qualityChart.length} observations`} />
                  <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                    <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                      <thead>
                        <tr style={{ borderBottom:`1px solid ${BD}` }}>
                          {["Year","Measure","Rate","YoY Change"].map(h => (
                            <th key={h} style={{ padding:"6px 10px",textAlign:h==="Year"||h==="Measure"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {qualityChart.map((r, i) => {
                          const prev = i > 0 ? qualityChart[i - 1] : null;
                          const change = prev ? r.measure_rate - prev.measure_rate : null;
                          return (
                            <tr key={r.data_year} style={{ borderBottom:`1px solid ${BD}20` }}>
                              <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.data_year}</td>
                              <td style={{ padding:"5px 10px",color:AL }}>{r.measure_name}</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmt(r.measure_rate)}%</td>
                              <td style={{ padding:"5px 10px",textAlign:"right",color:change != null ? (change > 0 ? POS : change < 0 ? NEG : AL) : AL,fontWeight:500 }}>
                                {change != null ? `${change > 0 ? "+" : ""}${fmt(change)}pp` : "\u2014"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </Card>
              )}
            </>
          )}
        </div>
      )}

      {/* Ask Aradune */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Section 1115 Waiver Impact: ${tab}. ${catalog.length} waivers in catalog. Selected state: ${selectedState ? STATE_NAMES[selectedState] || selectedState : 'none'}. ${enrollmentData.length} enrollment points. ${spendingData.length} spending years. ${qualityData.length} quality observations. ${selectedMeasure ? `Selected measure: ${selectedMeasureName}.` : ''}` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Aradune about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: KFF 1115 Waiver Tracker &middot; CMS-64 Expenditure (FY2018-2024) &middot; Medicaid Core Set (2017-2024) &middot; CMS Monthly Enrollment Reports
      </div>
    </div>
  );
}
