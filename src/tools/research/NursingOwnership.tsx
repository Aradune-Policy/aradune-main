import React, { useState, useMemo, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, Legend } from "recharts";
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
interface QualityByType { ownership_type: string; facility_count: number; avg_overall_rating: number; avg_inspection_rating: number; avg_qm_rating: number; avg_staffing_rating: number; avg_total_hprd: number; avg_rn_hprd: number; avg_deficiencies: number; avg_fine_dollars: number }
interface ChainVsIndep { affiliation: string; ownership_type: string; facility_count: number; avg_overall: number; avg_staffing: number; avg_hprd: number; avg_deficiencies: number; avg_fines: number; avg_rn_turnover: number }
interface DeficiencyPattern { ownership_type: string; affiliation: string; tag_number: string; deficiency_description: string; citation_count: number; avg_severity: number }
interface ChainRow { chain_name: string; facility_count: number; avg_overall_rating: number; avg_staffing_rating: number; avg_qm_rating: number; avg_hprd: number; avg_deficiencies: number; total_fines: number; avg_rn_turnover: number }

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
const TABS = ["Quality by Ownership", "Chain vs Independent", "Deficiency Patterns", "Chain Scoreboard"] as const;
type Tab = typeof TABS[number];

// Color mapping for ownership types
const OWN_COLORS: Record<string, string> = {
  "For-profit": NEG,
  "for-profit": NEG,
  "For profit": NEG,
  "Non-profit": POS,
  "non-profit": POS,
  "Non profit": POS,
  "Nonprofit": POS,
  "Government": "#3B82F6",
  "government": "#3B82F6",
};
const getOwnerColor = (t: string) => OWN_COLORS[t] || AL;

const RATING_COLORS = ["#A4262C", "#D4622B", "#B8860B", "#6B8E5A", "#2E6B4A"];
const ratingColor = (r: number) => r <= 1.5 ? RATING_COLORS[0] : r <= 2.5 ? RATING_COLORS[1] : r <= 3.0 ? RATING_COLORS[2] : r <= 4.0 ? RATING_COLORS[3] : RATING_COLORS[4];

type ChainSortKey = "chain_name" | "facility_count" | "avg_overall_rating" | "avg_staffing_rating" | "avg_qm_rating" | "avg_hprd" | "avg_deficiencies" | "total_fines" | "avg_rn_turnover";
type SortDir = "asc" | "desc";

// ═══════════════════════════════════════════════════════════════════════
//  NURSING HOME OWNERSHIP & QUALITY MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function NursingOwnership() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Quality by Ownership");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ── Data state ──
  const [qualityData, setQualityData] = useState<QualityByType[]>([]);
  const [chainData, setChainData] = useState<ChainVsIndep[]>([]);
  const [deficiencyData, setDeficiencyData] = useState<DeficiencyPattern[]>([]);
  const [scoreboard, setScoreboard] = useState<ChainRow[]>([]);

  // ── Sort state (Chain Scoreboard) ──
  const [chainSortKey, setChainSortKey] = useState<ChainSortKey>("avg_overall_rating");
  const [chainSortDir, setChainSortDir] = useState<SortDir>("asc");

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
        if (tab === "Quality by Ownership") {
          const d = await fetchJson("/api/research/nursing-ownership/quality-by-type");
          setQualityData(d.rows || d.data || []);
        } else if (tab === "Chain vs Independent") {
          const d = await fetchJson("/api/research/nursing-ownership/chain-vs-independent");
          setChainData(d.rows || d.data || []);
        } else if (tab === "Deficiency Patterns") {
          const d = await fetchJson("/api/research/nursing-ownership/deficiency-patterns");
          setDeficiencyData(d.rows || d.data || []);
        } else if (tab === "Chain Scoreboard") {
          const d = await fetchJson("/api/research/nursing-ownership/chain-scoreboard?limit=50");
          setScoreboard(d.rows || d.data || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, fetchJson]);

  // ── Computed: Quality by type chart data ──
  const qualityChart = useMemo(() =>
    qualityData.map(r => ({
      ...r,
      name: r.ownership_type,
    })),
  [qualityData]);

  const totalFacilities = useMemo(() => qualityData.reduce((s, r) => s + (r.facility_count || 0), 0), [qualityData]);

  // ── Computed: Chain vs Independent chart ──
  const chainCompare = useMemo(() =>
    chainData.map(r => ({
      ...r,
      name: `${r.affiliation} ${r.ownership_type}`,
    })),
  [chainData]);

  // ── Computed: Deficiency top 20 ──
  const defTop20 = useMemo(() =>
    [...deficiencyData]
      .sort((a, b) => b.citation_count - a.citation_count)
      .slice(0, 20)
      .map(r => ({
        ...r,
        name: r.deficiency_description?.length > 40 ? r.deficiency_description.slice(0, 38) + "..." : r.deficiency_description,
      })),
  [deficiencyData]);

  // ── Computed: Sorted scoreboard ──
  const sortedScoreboard = useMemo(() => {
    return [...scoreboard].sort((a, b) => {
      const av = a[chainSortKey] ?? 0;
      const bv = b[chainSortKey] ?? 0;
      if (typeof av === "string" && typeof bv === "string") return chainSortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      return chainSortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });
  }, [scoreboard, chainSortKey, chainSortDir]);

  const scoreboardMetrics = useMemo(() => {
    if (!scoreboard.length) return { worst: null as ChainRow | null, best: null as ChainRow | null, avgOverall: 0 };
    const sorted = [...scoreboard].sort((a, b) => a.avg_overall_rating - b.avg_overall_rating);
    const avg = scoreboard.reduce((s, r) => s + (r.avg_overall_rating || 0), 0) / scoreboard.length;
    return { worst: sorted[0], best: sorted[sorted.length - 1], avgOverall: avg };
  }, [scoreboard]);

  const toggleChainSort = (key: ChainSortKey) => {
    if (chainSortKey === key) setChainSortDir(d => d === "asc" ? "desc" : "asc");
    else { setChainSortKey(key); setChainSortDir(key === "chain_name" ? "asc" : "asc"); }
  };
  const sortArrow = (key: ChainSortKey) => chainSortKey === key ? (chainSortDir === "asc" ? " \u25B2" : " \u25BC") : "";

  // ── Render ────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Nursing Home Ownership & Quality</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>Five-Star + PBJ + Deficiencies + HCRIS</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Systematic comparison of quality, staffing, deficiency citations, and costs across for-profit chain, independent, nonprofit, and government nursing facilities.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: QUALITY BY OWNERSHIP                                   */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Quality by Ownership" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":`repeat(${Math.min(qualityData.length + 1, 4)},1fr)`,gap:10 }}>
            <Card accent={cB}>
              <Met l="Total Facilities" v={fmtK(totalFacilities)} sub={`${qualityData.length} ownership types`} />
            </Card>
            {qualityData.map(r => (
              <Card key={r.ownership_type} accent={getOwnerColor(r.ownership_type)}>
                <Met l={r.ownership_type} v={fmtK(r.facility_count)} sub={`Avg rating: ${fmt(r.avg_overall_rating)}`} cl={getOwnerColor(r.ownership_type)} />
              </Card>
            ))}
          </div>

          {/* Grouped bar chart */}
          {qualityChart.length > 0 ? (
            <Card>
              <CH t="Quality Ratings by Ownership Type" b="Average Five-Star ratings (1-5 scale)" r={`${totalFacilities} facilities`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="nursing-quality-by-ownership">
                  <div style={{ width:"100%",height:320 }}>
                    <ResponsiveContainer>
                      <BarChart data={qualityChart} margin={{ left:10,right:20,top:10,bottom:10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                        <XAxis dataKey="name" tick={{ fontSize:10,fill:AL }} />
                        <YAxis tick={{ fontSize:9,fill:AL }} domain={[0, 5]} />
                        <Tooltip content={<SafeTip formatter={(v: number, k: string) => k.includes("hprd") ? `${v.toFixed(2)} hrs` : v.toFixed(2)} />} />
                        <Legend wrapperStyle={{ fontSize:9,fontFamily:FM }} />
                        <Bar dataKey="avg_overall_rating" name="Overall" fill="#2E6B4A" radius={[3,3,0,0]} maxBarSize={28} />
                        <Bar dataKey="avg_staffing_rating" name="Staffing" fill="#3B82F6" radius={[3,3,0,0]} maxBarSize={28} />
                        <Bar dataKey="avg_qm_rating" name="Quality Measures" fill="#B8860B" radius={[3,3,0,0]} maxBarSize={28} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No quality data available.</div></Card>
          )}

          {/* Full table */}
          {qualityData.length > 0 && (
            <Card>
              <CH t="Full Breakdown" r={`${qualityData.length} ownership types`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["Ownership","Facilities","Overall","Inspection","QM","Staffing","Total HPRD","RN HPRD","Avg Deficiencies","Avg Fines"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="Ownership"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {qualityData.map(r => (
                      <tr key={r.ownership_type} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:getOwnerColor(r.ownership_type) }}>{r.ownership_type}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A }}>{fmtK(r.facility_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_overall_rating),fontWeight:600 }}>{fmt(r.avg_overall_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_inspection_rating) }}>{fmt(r.avg_inspection_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_qm_rating) }}>{fmt(r.avg_qm_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_staffing_rating) }}>{fmt(r.avg_staffing_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_total_hprd, 2)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_rn_hprd, 2)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_deficiencies > 8 ? NEG : r.avg_deficiencies > 5 ? WARN : POS }}>{fmt(r.avg_deficiencies)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD(r.avg_fine_dollars)}</td>
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
      {/* TAB 2: CHAIN VS INDEPENDENT                                   */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Chain vs Independent" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Groups" v={chainData.length} sub="Affiliation x ownership combos" />
            </Card>
            <Card accent={cB}>
              <Met l="Total Facilities" v={fmtK(chainData.reduce((s, r) => s + (r.facility_count || 0), 0))} />
            </Card>
            {chainData.filter(r => r.affiliation?.toLowerCase().includes("chain")).length > 0 && (
              <Card accent={WARN}>
                <Met l="Chain Avg Overall" v={fmt(chainData.filter(r => r.affiliation?.toLowerCase().includes("chain")).reduce((s, r) => s + r.avg_overall, 0) / chainData.filter(r => r.affiliation?.toLowerCase().includes("chain")).length)} sub="Five-Star rating" />
              </Card>
            )}
            {chainData.filter(r => r.affiliation?.toLowerCase().includes("indep")).length > 0 && (
              <Card accent={POS}>
                <Met l="Independent Avg Overall" v={fmt(chainData.filter(r => r.affiliation?.toLowerCase().includes("indep")).reduce((s, r) => s + r.avg_overall, 0) / chainData.filter(r => r.affiliation?.toLowerCase().includes("indep")).length)} sub="Five-Star rating" />
              </Card>
            )}
          </div>

          {/* Comparison bar chart */}
          {chainCompare.length > 0 ? (
            <Card>
              <CH t="Chain vs Independent Quality Comparison" b="By ownership type and affiliation" r={`${chainCompare.length} groups`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="nursing-chain-vs-independent">
                  <div style={{ width:"100%",height:Math.max(280, chainCompare.length * 36) }}>
                    <ResponsiveContainer>
                      <BarChart data={chainCompare} layout="vertical" margin={{ left:isMobile?80:140,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} domain={[0, 5]} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?76:136} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>{d.affiliation} - {d.ownership_type}</div>
                              <div style={{ color:AL }}>Facilities: {fmtK(d.facility_count)}</div>
                              <div style={{ color:AL }}>Overall: {fmt(d.avg_overall)}</div>
                              <div style={{ color:AL }}>Staffing: {fmt(d.avg_staffing)}</div>
                              <div style={{ color:AL }}>HPRD: {fmt(d.avg_hprd, 2)}</div>
                              <div style={{ color:AL }}>Deficiencies: {fmt(d.avg_deficiencies)}</div>
                              <div style={{ color:AL }}>Avg Fines: {fmtD(d.avg_fines)}</div>
                              <div style={{ color:AL }}>RN Turnover: {fmt(d.avg_rn_turnover)}%</div>
                            </div>
                          );
                        }} />
                        <Legend wrapperStyle={{ fontSize:9,fontFamily:FM }} />
                        <Bar dataKey="avg_overall" name="Overall Rating" fill="#2E6B4A" radius={[0,3,3,0]} maxBarSize={14} />
                        <Bar dataKey="avg_staffing" name="Staffing Rating" fill="#3B82F6" radius={[0,3,3,0]} maxBarSize={14} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No chain comparison data available.</div></Card>
          )}

          {/* Detail table */}
          {chainData.length > 0 && (
            <Card>
              <CH t="Chain vs Independent Detail" b="Is the quality gap driven by ownership or affiliation?" r={`${chainData.length} groups`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["Affiliation","Ownership","Facilities","Overall","Staffing","HPRD","Deficiencies","Avg Fines","RN Turnover"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="Affiliation"||h==="Ownership"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {chainData.map((r, i) => (
                      <tr key={`${r.affiliation}-${r.ownership_type}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{r.affiliation}</td>
                        <td style={{ padding:"5px 10px",color:getOwnerColor(r.ownership_type) }}>{r.ownership_type}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.facility_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_overall),fontWeight:600 }}>{fmt(r.avg_overall)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_staffing) }}>{fmt(r.avg_staffing)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_hprd, 2)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_deficiencies > 8 ? NEG : r.avg_deficiencies > 5 ? WARN : POS }}>{fmt(r.avg_deficiencies)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD(r.avg_fines)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_rn_turnover > 50 ? NEG : r.avg_rn_turnover > 30 ? WARN : POS }}>{fmt(r.avg_rn_turnover)}%</td>
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
      {/* TAB 3: DEFICIENCY PATTERNS                                    */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Deficiency Patterns" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Deficiency Tags" v={deficiencyData.length} />
            </Card>
            <Card accent={NEG}>
              <Met l="Total Citations" v={fmtK(deficiencyData.reduce((s, r) => s + (r.citation_count || 0), 0))} />
            </Card>
            <Card accent={WARN}>
              <Met l="Avg Severity" v={fmt(deficiencyData.length ? deficiencyData.reduce((s, r) => s + (r.avg_severity || 0), 0) / deficiencyData.length : 0)} sub="1=lowest, 4=highest" />
            </Card>
            <Card accent={NEG}>
              <Met l="Most Cited" v={defTop20.length ? defTop20[0]?.tag_number : "\u2014"} sub={defTop20.length ? `${fmtK(defTop20[0]?.citation_count)} citations` : ""} cl={NEG} />
            </Card>
          </div>

          {/* Top 20 bar chart */}
          {defTop20.length > 0 ? (
            <Card>
              <CH t="Top 20 Deficiency Citations" b="Colored by ownership type" r={`${deficiencyData.length} total patterns`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename="nursing-deficiency-patterns">
                  <div style={{ width:"100%",height:Math.max(400, defTop20.length * 20) }}>
                    <ResponsiveContainer>
                      <BarChart data={defTop20} layout="vertical" margin={{ left:isMobile?80:180,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:8,fill:AL }} width={isMobile?76:176} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.length) return null;
                          const d = payload[0]?.payload;
                          if (!d) return null;
                          return (
                            <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                              <div style={{ fontWeight:600,color:A,marginBottom:2 }}>Tag {d.tag_number}</div>
                              <div style={{ color:AL,maxWidth:280,lineHeight:1.4 }}>{d.deficiency_description}</div>
                              <div style={{ color:AL,marginTop:2 }}>Citations: {fmtK(d.citation_count)}</div>
                              <div style={{ color:AL }}>Avg Severity: {fmt(d.avg_severity)}</div>
                              <div style={{ color:AL }}>Ownership: {d.ownership_type}</div>
                              <div style={{ color:AL }}>Affiliation: {d.affiliation}</div>
                            </div>
                          );
                        }} />
                        <Bar dataKey="citation_count" radius={[0,3,3,0]} maxBarSize={14}>
                          {defTop20.map((d, i) => (
                            <Cell key={i} fill={d.avg_severity > 2.5 ? NEG : d.avg_severity > 1.5 ? WARN : POS} fillOpacity={0.8} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No deficiency data available.</div></Card>
          )}

          {/* Deficiency table */}
          {deficiencyData.length > 0 && (
            <Card>
              <CH t="Deficiency Detail by Ownership Type" r={`${deficiencyData.length} patterns`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["#","Ownership","Affiliation","Tag","Description","Citations","Avg Severity"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="Description"||h==="Ownership"||h==="Affiliation"||h==="Tag"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...deficiencyData].sort((a, b) => b.citation_count - a.citation_count).slice(0, 100).map((r, i) => (
                      <tr key={`${r.tag_number}-${r.ownership_type}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                        <td style={{ padding:"5px 10px",color:getOwnerColor(r.ownership_type),fontWeight:500 }}>{r.ownership_type}</td>
                        <td style={{ padding:"5px 10px",color:AL }}>{r.affiliation}</td>
                        <td style={{ padding:"5px 10px",color:A,fontWeight:600 }}>{r.tag_number}</td>
                        <td style={{ padding:"5px 10px",color:AL,maxWidth:280,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.deficiency_description}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmtK(r.citation_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_severity > 2.5 ? NEG : r.avg_severity > 1.5 ? WARN : POS,fontWeight:500 }}>{fmt(r.avg_severity)}</td>
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
      {/* TAB 4: CHAIN SCOREBOARD                                       */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Chain Scoreboard" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Metrics */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Chains" v={scoreboard.length} sub="Top chains by facility count" />
            </Card>
            <Card accent={NEG}>
              <Met l="Worst Chain" v={scoreboardMetrics.worst?.chain_name || "\u2014"} cl={NEG} sub={scoreboardMetrics.worst ? `Rating: ${fmt(scoreboardMetrics.worst.avg_overall_rating)}` : ""} />
            </Card>
            <Card accent={POS}>
              <Met l="Best Chain" v={scoreboardMetrics.best?.chain_name || "\u2014"} cl={POS} sub={scoreboardMetrics.best ? `Rating: ${fmt(scoreboardMetrics.best.avg_overall_rating)}` : ""} />
            </Card>
            <Card accent={WARN}>
              <Met l="Avg Across Chains" v={fmt(scoreboardMetrics.avgOverall)} sub="Overall Five-Star rating" />
            </Card>
          </div>

          {/* Sortable table */}
          {sortedScoreboard.length > 0 ? (
            <Card>
              <CH t="Chain Scoreboard" b="Click column headers to sort -- ascending = worst first" r={`${sortedScoreboard.length} chains`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {[
                        { key: "chain_name" as ChainSortKey, label: "Chain Name", align: "left" },
                        { key: "facility_count" as ChainSortKey, label: "Facilities", align: "right" },
                        { key: "avg_overall_rating" as ChainSortKey, label: "Overall", align: "right" },
                        { key: "avg_staffing_rating" as ChainSortKey, label: "Staffing", align: "right" },
                        { key: "avg_qm_rating" as ChainSortKey, label: "QM", align: "right" },
                        { key: "avg_hprd" as ChainSortKey, label: "HPRD", align: "right" },
                        { key: "avg_deficiencies" as ChainSortKey, label: "Deficiencies", align: "right" },
                        { key: "total_fines" as ChainSortKey, label: "Total Fines", align: "right" },
                        { key: "avg_rn_turnover" as ChainSortKey, label: "RN Turnover", align: "right" },
                      ].map(col => (
                        <th key={col.key}
                          onClick={() => toggleChainSort(col.key)}
                          style={{ padding:"6px 10px",textAlign:col.align as "left"|"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap",cursor:"pointer",userSelect:"none" }}>
                          {col.label}{sortArrow(col.key)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedScoreboard.map((r, i) => (
                      <tr key={`${r.chain_name}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.chain_name}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.facility_count)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_overall_rating),fontWeight:600 }}>{fmt(r.avg_overall_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_staffing_rating) }}>{fmt(r.avg_staffing_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:ratingColor(r.avg_qm_rating) }}>{fmt(r.avg_qm_rating)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmt(r.avg_hprd, 2)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_deficiencies > 8 ? NEG : r.avg_deficiencies > 5 ? WARN : POS }}>{fmt(r.avg_deficiencies)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.total_fines > 100000 ? NEG : WARN }}>{fmtD(r.total_fines)}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:r.avg_rn_turnover > 50 ? NEG : r.avg_rn_turnover > 30 ? WARN : POS }}>{fmt(r.avg_rn_turnover)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          ) : (
            !loading && <Card><div style={{ padding:20,textAlign:"center",fontSize:11,color:AL,fontFamily:FM }}>No chain scoreboard data available.</div></Card>
          )}
        </div>
      )}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Nursing Home Ownership & Quality: ${tab}. ${qualityData.length} ownership types. ${chainData.length} chain comparison groups. ${deficiencyData.length} deficiency patterns. ${scoreboard.length} chains in scoreboard. Total facilities: ${fmtK(totalFacilities)}.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Source */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS Five-Star Quality Rating &middot; Payroll-Based Journal &middot; CMS Deficiency Citations &middot; HCRIS SNF Cost Reports &middot; CMS Ownership Data
      </div>
    </div>
  );
}
