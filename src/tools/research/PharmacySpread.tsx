import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
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
interface SpreadRow { ndc: string; drug_name: string; nadac_per_unit: number; reimbursement_per_unit: number; spread_per_unit: number; total_units: number; total_reimbursed: number; total_spread_dollars: number; total_prescriptions: number }
interface StateSpread { state_code: string; drugs_matched: number; total_reimbursement: number; total_acquisition_cost: number; total_spread: number; spread_pct: number }
interface TopDrug { ndc: string; drug_name: string; nadac_per_unit: number; avg_reimbursement_per_unit: number; spread_per_unit: number; total_units: number; total_reimbursed: number; total_overpayment: number; total_rx: number; state_count: number }
interface SpreadStats { drugs_analyzed: number; avg_spread_per_unit: number; median_spread_per_unit: number; p90_spread_per_unit: number; total_overpayment: number; total_underpayment: number; drugs_overpaid: number; drugs_underpaid: number }

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
        <div key={i} style={{ color:AL }}>{p.dataKey}: {formatter ? formatter(p.value, p.dataKey) : p.value}</div>
      ))}
    </div>
  );
};

// ── Tabs ──────────────────────────────────────────────────────────────
const TABS = ["Spread Overview", "State Variation", "Top Overpayment Drugs", "Drug Detail"] as const;
type Tab = typeof TABS[number];

type DrugSortKey = "drug_name" | "nadac_per_unit" | "avg_reimbursement_per_unit" | "spread_per_unit" | "total_units" | "total_overpayment" | "state_count";
type SortDir = "asc" | "desc";

// ═══════════════════════════════════════════════════════════════════════
//  PHARMACY SPREAD ANALYSIS MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function PharmacySpread() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Spread Overview");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Data state ──
  const [stats, setStats] = useState<SpreadStats | null>(null);
  const [overview, setOverview] = useState<SpreadRow[]>([]);
  const [stateData, setStateData] = useState<StateSpread[]>([]);
  const [topDrugs, setTopDrugs] = useState<TopDrug[]>([]);
  const [searchTerm, setSearchTerm] = useState("");

  // ── Sort state (Top Drugs) ──
  const [drugSortKey, setDrugSortKey] = useState<DrugSortKey>("total_overpayment");
  const [drugSortDir, setDrugSortDir] = useState<SortDir>("desc");

  // ── Fetch helper ──
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
        if (tab === "Spread Overview") {
          const [s, o] = await Promise.all([
            fetchJson("/api/research/pharmacy-spread/stats"),
            fetchJson("/api/research/pharmacy-spread/overview"),
          ]);
          setStats(s.stats || s.data || s);
          setOverview(s.rows || o.rows || o.data || []);
        } else if (tab === "State Variation") {
          const d = await fetchJson("/api/research/pharmacy-spread/by-state");
          setStateData(d.rows || d.data || []);
        } else if (tab === "Top Overpayment Drugs") {
          const d = await fetchJson("/api/research/pharmacy-spread/top-drugs?limit=50");
          setTopDrugs(d.rows || d.data || []);
        } else if (tab === "Drug Detail") {
          if (!overview.length) {
            const o = await fetchJson("/api/research/pharmacy-spread/overview");
            setOverview(o.rows || o.data || []);
          }
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, fetchJson]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Computed: Overview chart (top 30 by spread) ──
  const overviewChart = useMemo(() =>
    [...overview]
      .sort((a, b) => Math.abs(b.total_spread_dollars) - Math.abs(a.total_spread_dollars))
      .slice(0, 30)
      .map(r => ({ ...r, name: r.drug_name?.length > 28 ? r.drug_name.slice(0, 26) + "..." : r.drug_name })),
  [overview]);

  // ── Computed: State chart ──
  const stateChart = useMemo(() =>
    [...stateData]
      .sort((a, b) => b.total_spread - a.total_spread)
      .map(r => ({ ...r, name: STATE_NAMES[r.state_code] || r.state_code })),
  [stateData]);

  const maxSpreadPct = useMemo(() => Math.max(...stateData.map(r => r.spread_pct || 0), 1), [stateData]);

  // ── Computed: Sorted top drugs ──
  const sortedDrugs = useMemo(() => {
    const sorted = [...topDrugs].sort((a, b) => {
      const av = a[drugSortKey] ?? 0;
      const bv = b[drugSortKey] ?? 0;
      if (typeof av === "string" && typeof bv === "string") return drugSortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      return drugSortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });
    return sorted;
  }, [topDrugs, drugSortKey, drugSortDir]);

  const topDrugMetrics = useMemo(() => {
    if (!topDrugs.length) return { topOverpay: 0, avgSpread: 0 };
    return {
      topOverpay: Math.max(...topDrugs.map(r => r.total_overpayment || 0)),
      avgSpread: topDrugs.reduce((s, r) => s + (r.spread_per_unit || 0), 0) / topDrugs.length,
    };
  }, [topDrugs]);

  // ── Computed: Filtered drug detail ──
  const filteredDetail = useMemo(() => {
    if (!searchTerm.trim()) return overview;
    const q = searchTerm.toLowerCase();
    return overview.filter(r => r.drug_name?.toLowerCase().includes(q) || r.ndc?.includes(q));
  }, [overview, searchTerm]);

  const toggleDrugSort = (key: DrugSortKey) => {
    if (drugSortKey === key) setDrugSortDir(d => d === "asc" ? "desc" : "asc");
    else { setDrugSortKey(key); setDrugSortDir("desc"); }
  };
  const sortArrow = (key: DrugSortKey) => drugSortKey === key ? (drugSortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Pharmacy Spread Analysis</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>NADAC + SDUD + Drug Rebate</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          The gap between what pharmacies pay for drugs (NADAC) and what Medicaid reimburses (SDUD). Identifies overpayment hotspots by drug, state, and therapeutic class before rebates are applied.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: SPREAD OVERVIEW                                        */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Spread Overview" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(5,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Drugs Analyzed" v={stats ? fmtK(stats.drugs_analyzed) : "\u2014"} />
            </Card>
            <Card accent={cB}>
              <Met l="Avg Spread/Unit" v={stats ? `$${fmt(stats.avg_spread_per_unit, 2)}` : "\u2014"} sub="Reimbursement minus NADAC" />
            </Card>
            <Card accent={WARN}>
              <Met l="Median Spread/Unit" v={stats ? `$${fmt(stats.median_spread_per_unit, 2)}` : "\u2014"} />
            </Card>
            <Card accent={NEG}>
              <Met l="Total Overpayment" v={stats ? fmtD(stats.total_overpayment) : "\u2014"} cl={NEG} sub={stats ? `${fmtK(stats.drugs_overpaid)} drugs overpaid` : ""} />
            </Card>
            <Card accent={POS}>
              <Met l="Total Underpayment" v={stats ? fmtD(stats.total_underpayment) : "\u2014"} cl={POS} sub={stats ? `${fmtK(stats.drugs_underpaid)} drugs underpaid` : ""} />
            </Card>
          </div>

          {/* Distribution insight */}
          {stats && (
            <Card>
              <CH t="Spread Distribution" b={`${fmtK(stats.drugs_overpaid)} overpaid vs ${fmtK(stats.drugs_underpaid)} underpaid`} r={`P90: $${fmt(stats.p90_spread_per_unit, 2)}/unit`} />
              <div style={{ padding:"8px 14px 12px",fontSize:11,color:AL,lineHeight:1.6 }}>
                {stats.drugs_overpaid > stats.drugs_underpaid
                  ? `${((stats.drugs_overpaid / (stats.drugs_overpaid + stats.drugs_underpaid)) * 100).toFixed(0)}% of matched drugs are reimbursed above acquisition cost. The 90th percentile spread is $${fmt(stats.p90_spread_per_unit, 2)}/unit, indicating significant concentration in high-spread generics.`
                  : `${((stats.drugs_underpaid / (stats.drugs_overpaid + stats.drugs_underpaid)) * 100).toFixed(0)}% of matched drugs are reimbursed below acquisition cost, suggesting tight reimbursement overall.`
                }
              </div>
            </Card>
          )}

          {/* Bar chart: top 30 by spread */}
          {overviewChart.length > 0 ? (
            <Card>
              <CH t="Top 30 Drugs by Total Spread" b="Overpayment (red) vs underpayment (green)" r={`${overview.length} drugs total`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="pharmacy-spread-overview">
                  <div style={{ width:"100%",height:Math.max(400, overviewChart.length * 16) }}>
                    <ResponsiveContainer>
                      <BarChart data={overviewChart} layout="vertical" margin={{ left:isMobile?60:120,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:8,fill:AL }} width={isMobile?56:116} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.drug_name}</div>
                              <div style={{ color:AL }}>NDC: {d.ndc}</div>
                              <div style={{ color:AL }}>NADAC: ${fmt(d.nadac_per_unit, 4)}/unit</div>
                              <div style={{ color:AL }}>Reimb: ${fmt(d.reimbursement_per_unit, 4)}/unit</div>
                              <div style={{ color:AL }}>Spread: ${fmt(d.spread_per_unit, 4)}/unit</div>
                              <div style={{ color:AL }}>Total Spread: {fmtD(d.total_spread_dollars)}</div>
                              <div style={{ color:AL }}>Units: {fmtK(d.total_units)}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="total_spread_dollars" radius={[0,3,3,0]} maxBarSize={14}>
                          {overviewChart.map((d, i) => (
                            <Cell key={i} fill={d.total_spread_dollars > 0 ? NEG : POS} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No spread data available.</div></Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 2: STATE VARIATION                                        */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "State Variation" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="States" v={stateData.length} />
            </Card>
            <Card accent={NEG}>
              <Met l="Highest Spread" v={stateChart.length ? fmtD(stateChart[0]?.total_spread) : "\u2014"} sub={stateChart.length ? STATE_NAMES[stateChart[0]?.state_code] || "" : ""} cl={NEG} />
            </Card>
            <Card accent={cB}>
              <Met l="Total Reimbursement" v={fmtD(stateData.reduce((s, r) => s + (r.total_reimbursement || 0), 0))} sub="All states" />
            </Card>
            <Card accent={WARN}>
              <Met l="Total Spread" v={fmtD(stateData.reduce((s, r) => s + (r.total_spread || 0), 0))} sub="All states combined" cl={WARN} />
            </Card>
          </div>

          {/* Bar chart: states by total spread */}
          {stateChart.length > 0 ? (
            <Card>
              <CH t="Total Pharmacy Spread by State" b="Overpayment above acquisition cost" r={`${stateChart.length} states`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="pharmacy-spread-by-state">
                  <div style={{ width:"100%",height:Math.max(400, stateChart.length * 16) }}>
                    <ResponsiveContainer>
                      <BarChart data={stateChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{STATE_NAMES[d.state_code] || d.state_code}</div>
                              <div style={{ color:AL }}>Total Spread: {fmtD(d.total_spread)}</div>
                              <div style={{ color:AL }}>Spread %: {fmt(d.spread_pct)}%</div>
                              <div style={{ color:AL }}>Reimbursement: {fmtD(d.total_reimbursement)}</div>
                              <div style={{ color:AL }}>Acquisition: {fmtD(d.total_acquisition_cost)}</div>
                              <div style={{ color:AL }}>Drugs Matched: {fmtK(d.drugs_matched)}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="total_spread" radius={[0,3,3,0]} maxBarSize={14}>
                          {stateChart.map((d, i) => {
                            const intensity = Math.min(1, (d.spread_pct || 0) / maxSpreadPct);
                            return <Cell key={i} fill={`rgba(164, 38, 44, ${0.3 + intensity * 0.6})`} />;
                          })}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No state spread data available.</div></Card>
          )}

          {/* State table */}
          {stateData.length > 0 && (
            <Card>
              <CH t="State Spread Detail" r={`${stateData.length} states`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","State","Drugs Matched","Total Reimbursement","Acquisition Cost","Total Spread","Spread %"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {stateChart.map((r, i) => (
                      <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.drugs_matched)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.total_reimbursement)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.total_acquisition_cost)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.total_spread > 0 ? NEG : POS,fontWeight:500 }}>{fmtD(r.total_spread)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.spread_pct > 10 ? NEG : r.spread_pct > 5 ? WARN : POS,fontWeight:500 }}>{fmt(r.spread_pct)}%</td>
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
      {/* TAB 3: TOP OVERPAYMENT DRUGS                                  */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Top Overpayment Drugs" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Drugs Shown" v={topDrugs.length} sub="Top overpayment drugs" />
            </Card>
            <Card accent={NEG}>
              <Met l="Top Drug Overpayment" v={fmtD(topDrugMetrics.topOverpay)} cl={NEG} />
            </Card>
            <Card accent={WARN}>
              <Met l="Avg Spread/Unit" v={`$${fmt(topDrugMetrics.avgSpread, 2)}`} />
            </Card>
            <Card accent={NEG}>
              <Met l="Total Overpayment" v={fmtD(topDrugs.reduce((s, r) => s + (r.total_overpayment || 0), 0))} cl={NEG} sub="Across top drugs" />
            </Card>
          </div>

          {/* Sortable table */}
          {sortedDrugs.length > 0 ? (
            <Card>
              <CH t="Top Overpayment Drugs" b="Click column headers to sort" r={`${sortedDrugs.length} drugs`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {[
                        { key: "drug_name" as DrugSortKey, label: "Drug Name", align: "left" },
                        { key: "nadac_per_unit" as DrugSortKey, label: "NADAC/Unit", align: "right" },
                        { key: "avg_reimbursement_per_unit" as DrugSortKey, label: "Reimb/Unit", align: "right" },
                        { key: "spread_per_unit" as DrugSortKey, label: "Spread/Unit", align: "right" },
                        { key: "total_units" as DrugSortKey, label: "Total Units", align: "right" },
                        { key: "total_overpayment" as DrugSortKey, label: "Total Overpayment", align: "right" },
                        { key: "state_count" as DrugSortKey, label: "States", align: "right" },
                      ].map(col => (
                        <th key={col.key}
                          onClick={() => toggleDrugSort(col.key)}
                          style={{ padding:"6px 10px",textAlign:col.align as "left"|"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap",cursor:"pointer",userSelect:"none" }}>
                          {col.label}{sortArrow(col.key)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedDrugs.map((r, i) => (
                      <tr key={`${r.ndc}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A,maxWidth:200 }}>
                          <div>{r.drug_name}</div>
                          <div style={{ fontSize:8,color:AL,fontFamily:FM }}>{r.ndc}</div>
                        </td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>${fmt(r.nadac_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>${fmt(r.avg_reimbursement_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.spread_per_unit > 0 ? NEG : POS,fontWeight:500 }}>${fmt(r.spread_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_units)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:NEG,fontWeight:600 }}>{fmtD(r.total_overpayment)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.state_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No top drug data available.</div></Card>
          )}
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 4: DRUG DETAIL                                            */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Drug Detail" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Search box */}
          <Card>
            <CH t="Drug Search" b="Filter by drug name or NDC" r={`${filteredDetail.length} of ${overview.length} drugs`} />
            <div style={{ padding:"8px 14px 12px" }}>
              <input
                type="text"
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                placeholder="Search drug name or NDC..."
                style={{ width:"100%",maxWidth:400,padding:"6px 10px",borderRadius:6,border:`1px solid ${BD}`,fontSize:11,fontFamily:FM,color:A,outline:"none" }}
              />
            </div>
          </Card>

          {/* Full table */}
          {filteredDetail.length > 0 ? (
            <Card>
              <CH t="Spread Detail" r={`${filteredDetail.length} drugs`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","Drug Name","NDC","NADAC/Unit","Reimb/Unit","Spread/Unit","Total Units","Total Reimbursed","Total Spread","Rx Count"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="Drug Name"||h==="NDC"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredDetail.slice(0, 200).map((r, i) => (
                      <tr key={`${r.ndc}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A,maxWidth:180,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.drug_name}</td>
                        <td style={{ padding:"5px 10px",color:AL,fontSize:9 }}>{r.ndc}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>${fmt(r.nadac_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>${fmt(r.reimbursement_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.spread_per_unit > 0 ? NEG : POS,fontWeight:500 }}>${fmt(r.spread_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_units)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtD(r.total_reimbursed)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.total_spread_dollars > 0 ? NEG : POS,fontWeight:500 }}>{fmtD(r.total_spread_dollars)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_prescriptions)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {filteredDetail.length > 200 && (
                  <div style={{ padding:"8px 14px",fontSize:10,color:AL,fontFamily:FM,textAlign:"center" }}>
                    Showing 200 of {filteredDetail.length} results. Refine your search for more specific results.
                  </div>
                )}
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>{searchTerm ? "No drugs match your search." : "No drug detail data available."}</div></Card>
          )}
        </div>
      )}

      {/* Ask Aradune */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Pharmacy Spread Analysis: ${tab}. ${overview.length} drugs in overview. ${stateData.length} states in state variation. ${topDrugs.length} top overpayment drugs. Stats: ${stats ? `${fmtK(stats.drugs_analyzed)} drugs, ${fmtD(stats.total_overpayment)} overpayment.` : 'loading.'}` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Aradune about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS NADAC (Mar 2026) &middot; State Drug Utilization Data (2025) &middot; Medicaid Drug Rebate Product List
      </div>
    </div>
  );
}
