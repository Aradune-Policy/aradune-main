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
interface StateSummary { state_code: string; drug_count: number; total_prescriptions: number; total_reimbursed: number; medicaid_reimbursed: number }
interface TopDrug { product_name: string; ndc: string; total_medicaid_spend?: number; total_spend?: number; total_medicaid_spend_2024?: number; total_prescriptions: number; total_units: number; state_count: number }
interface NadacRow { ndc: string; ndc_description: string; nadac_per_unit: number; effective_date: string; pricing_unit: string; is_otc: boolean }

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
      {payload.map((p, i) => (
        <div key={i} style={{ color:AL }}>{formatter ? formatter(p.value) : p.value}</div>
      ))}
    </div>
  );
};

const TABS = ["Spending Overview", "Top Drugs", "NADAC Pricing"] as const;
type Tab = typeof TABS[number];

// ═══════════════════════════════════════════════════════════════════════
//  PHARMACY INTELLIGENCE MODULE
// ═══════════════════════════════════════════════════════════════════════
export default function PharmacyIntelligence() {
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<Tab>("Spending Overview");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const isMobile = typeof window !== "undefined" && window.innerWidth < 768;

  // ── State ──
  const [stateSummary, setStateSummary] = useState<StateSummary[]>([]);
  const [topDrugs, setTopDrugs] = useState<TopDrug[]>([]);
  const [nadacResults, setNadacResults] = useState<NadacRow[]>([]);
  const [nadacSearch, setNadacSearch] = useState("");
  const [selectedState, setSelectedState] = useState<string>("");

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  // ── Load tab data ──
  useEffect(() => {
    if (tab === "NADAC Pricing") return; // manual search
    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        if (tab === "Spending Overview") {
          const d = await fetchJson("/api/pharmacy/sdud-2025/state-summary");
          setStateSummary(d.rows || []);
        } else if (tab === "Top Drugs") {
          const params = selectedState ? `?state=${selectedState}&limit=50` : "?limit=50";
          const d = await fetchJson(`/api/pharmacy/sdud-2025/top-drugs${params}`);
          setTopDrugs(d.rows || []);
        }
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    };
    load();
  }, [tab, selectedState, fetchJson]);

  // ── NADAC search handler ──
  const searchNadac = useCallback(async () => {
    if (!nadacSearch.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const d = await fetchJson(`/api/pharmacy/nadac?search=${encodeURIComponent(nadacSearch)}&limit=100`);
      // Old endpoint returns array, not {rows}
      setNadacResults(Array.isArray(d) ? d : d.rows || []);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to search NADAC");
    }
    setLoading(false);
  }, [nadacSearch, fetchJson]);

  // ── Computed ──
  const totalSpend = useMemo(() => stateSummary.reduce((s, r) => s + (r.total_reimbursed || 0), 0), [stateSummary]);
  const totalMedicaid = useMemo(() => stateSummary.reduce((s, r) => s + (r.medicaid_reimbursed || 0), 0), [stateSummary]);
  const totalRx = useMemo(() => stateSummary.reduce((s, r) => s + (r.total_prescriptions || 0), 0), [stateSummary]);
  const totalDrugs = useMemo(() => stateSummary.reduce((s, r) => s + (r.drug_count || 0), 0), [stateSummary]);

  const spendChart = useMemo(() =>
    stateSummary.slice(0, 25).map(r => ({
      state: r.state_code,
      name: STATE_NAMES[r.state_code] || r.state_code,
      spend: r.total_reimbursed,
    })),
  [stateSummary]);

  return (
    <div style={{ maxWidth:1080,margin:"0 auto",padding:isMobile?"12px":"20px 20px 60px" }}>
      {/* Header */}
      <div style={{ marginBottom:16 }}>
        <div style={{ display:"flex",alignItems:"baseline",gap:8,flexWrap:"wrap" }}>
          <h1 style={{ fontSize:isMobile?18:22,fontWeight:800,color:A,margin:0,letterSpacing:-0.5 }}>Pharmacy Intelligence</h1>
          <span style={{ fontSize:9,fontFamily:FM,color:AL,background:SF,padding:"2px 8px",borderRadius:4,border:`1px solid ${BD}` }}>SDUD + NADAC + Drug Rebate</span>
        </div>
        <p style={{ fontSize:12,color:AL,margin:"4px 0 0",lineHeight:1.5,maxWidth:640 }}>
          Medicaid drug spending, utilization patterns, NADAC pricing benchmarks, and top drugs by reimbursement across all states.
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display:"flex",gap:6,marginBottom:16,flexWrap:"wrap" }}>
        {TABS.map(t => <Pill key={t} label={t} active={tab===t} onClick={() => setTab(t)} />)}
      </div>

      {loading && <LoadingBar />}
      {error && <div style={{ padding:12,background:"#FFF5F5",border:`1px solid ${NEG}30`,borderRadius:8,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 1: SPENDING OVERVIEW                                      */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Spending Overview" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* Summary */}
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr 1fr":"repeat(4,1fr)",gap:10 }}>
            <Card accent={cB}>
              <Met l="Total Reimbursed" v={fmtD(totalSpend)} sub="SDUD 2025, all states" />
            </Card>
            <Card accent="#6366F1">
              <Met l="Medicaid Portion" v={fmtD(totalMedicaid)} sub="Federal + state share" />
            </Card>
            <Card accent={WARN}>
              <Met l="Total Prescriptions" v={fmtK(totalRx)} />
            </Card>
            <Card accent={POS}>
              <Met l="States" v={stateSummary.length} sub={`${fmtK(totalDrugs)} unique NDCs`} />
            </Card>
          </div>

          {/* Bar chart */}
          <Card>
            <CH t="Total Drug Reimbursement by State" b="SDUD 2025" r={`Top 25 of ${stateSummary.length}`} />
            <div style={{ padding:"8px 14px 14px" }}>
              <ChartActions filename="pharmacy-spend-by-state">
                <div style={{ width:"100%",height:Math.max(360, spendChart.length * 20) }}>
                  <ResponsiveContainer>
                    <BarChart data={spendChart} layout="vertical" margin={{ left:isMobile?40:70,right:20,top:4,bottom:4 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                      <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                      <YAxis type="category" dataKey="name" tick={{ fontSize:9,fill:AL }} width={isMobile?36:66} />
                      <Tooltip content={<SafeTip formatter={v => fmtD(v)} />} />
                      <Bar dataKey="spend" fill={cB} radius={[0,3,3,0]} maxBarSize={14}>
                        {spendChart.map((_, i) => (
                          <Cell key={i} fill={i < 5 ? "#6366F1" : cB} fillOpacity={0.8} />
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
            <CH t="State Drug Spending Detail" r={`${stateSummary.length} states`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["#","State","Unique NDCs","Prescriptions","Total Reimbursed","Medicaid Share","Avg / Rx"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="State"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {stateSummary.map((r, i) => (
                    <tr key={r.state_code} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{r.drug_count.toLocaleString()}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(r.total_prescriptions)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmtD(r.total_reimbursed)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtD(r.medicaid_reimbursed)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>
                        {r.total_prescriptions > 0 ? fmtD(r.total_reimbursed / r.total_prescriptions) : "—"}
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
      {/* TAB 2: TOP DRUGS                                              */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "Top Drugs" && !loading && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          {/* State filter */}
          <Card>
            <CH t="Filter by State" b="Optional — leave blank for national" />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:8,alignItems:"center",flexWrap:"wrap" }}>
              <select value={selectedState} onChange={e => setSelectedState(e.target.value)}
                style={{ fontSize:10,fontFamily:FM,padding:"4px 8px",borderRadius:6,border:`1px solid ${BD}`,color:AL,background:WH,minWidth:140 }}>
                <option value="">All States</option>
                {Object.entries(STATE_NAMES).sort((a, b) => a[1].localeCompare(b[1])).map(([k, v]) => (
                  <option key={k} value={k}>{v}</option>
                ))}
              </select>
              {selectedState && <Pill label="Clear" active={false} onClick={() => setSelectedState("")} />}
            </div>
          </Card>

          {/* Top drugs chart */}
          {topDrugs.length > 0 && (
            <Card>
              <CH t="Top Drugs by Medicaid Spending" b="SDUD 2025" r={`${topDrugs.length} drugs${selectedState ? ` in ${STATE_NAMES[selectedState] || selectedState}` : ""}`} />
              <div style={{ padding:"8px 14px 14px" }}>
                <ChartActions filename={`pharmacy-top-drugs${selectedState ? `-${selectedState}` : ""}`}>
                  <div style={{ width:"100%",height:Math.max(360, Math.min(topDrugs.length, 25) * 22) }}>
                    <ResponsiveContainer>
                      <BarChart data={topDrugs.slice(0, 25).map(d => ({
                        name: (d.product_name || "Unknown").length > 30 ? (d.product_name || "").slice(0, 28) + "..." : d.product_name || "Unknown",
                        spend: d.total_spend || d.total_medicaid_spend || 0,
                      }))} layout="vertical" margin={{ left:isMobile?60:120,right:20,top:4,bottom:4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" tick={{ fontSize:9,fill:AL }} tickFormatter={v => fmtD(v)} />
                        <YAxis type="category" dataKey="name" tick={{ fontSize:8,fill:AL }} width={isMobile?56:116} />
                        <Tooltip content={<SafeTip formatter={v => fmtD(v)} />} />
                        <Bar dataKey="spend" fill="#6366F1" radius={[0,3,3,0]} maxBarSize={14} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </ChartActions>
              </div>
            </Card>
          )}

          {/* Top drugs table */}
          <Card>
            <CH t="Drug Spending Detail" r={`${topDrugs.length} drugs`} />
            <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                <thead>
                  <tr style={{ borderBottom:`1px solid ${BD}` }}>
                    {["#","Drug Name","NDC","Total Spend","Prescriptions","Units","States"].map(h => (
                      <th key={h} style={{ padding:"6px 10px",textAlign:h==="#"?"center":h==="Drug Name"||h==="NDC"?"left":"right",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {topDrugs.map((d, i) => (
                    <tr key={`${d.ndc}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                      <td style={{ padding:"5px 10px",textAlign:"center",color:AL }}>{i + 1}</td>
                      <td style={{ padding:"5px 10px",fontWeight:600,color:A,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{d.product_name || "—"}</td>
                      <td style={{ padding:"5px 10px",color:AL }}>{d.ndc}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>{fmtD(d.total_spend || d.total_medicaid_spend || 0)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(d.total_prescriptions)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{fmtK(d.total_units)}</td>
                      <td style={{ padding:"5px 10px",textAlign:"right",color:AL }}>{d.state_count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        </div>
      )}

      {/* ══════════════════════════════════════════════════════════════ */}
      {/* TAB 3: NADAC PRICING                                          */}
      {/* ══════════════════════════════════════════════════════════════ */}
      {tab === "NADAC Pricing" && (
        <div style={{ display:"flex",flexDirection:"column",gap:14 }}>
          <Card>
            <CH t="NADAC Drug Pricing Search" b="National Average Drug Acquisition Cost" />
            <div style={{ padding:"8px 14px 12px",display:"flex",gap:8,alignItems:"center" }}>
              <input
                type="text" value={nadacSearch} onChange={e => setNadacSearch(e.target.value)}
                onKeyDown={e => e.key === "Enter" && searchNadac()}
                placeholder="Search drug name (e.g., metformin, lisinopril)..."
                style={{ flex:1,fontSize:11,fontFamily:FM,padding:"6px 10px",borderRadius:6,border:`1px solid ${BD}`,color:A,background:WH,outline:"none" }}
              />
              <button onClick={searchNadac} disabled={loading || !nadacSearch.trim()}
                style={{ padding:"6px 14px",borderRadius:6,fontSize:10,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:cB,color:WH,cursor:"pointer",whiteSpace:"nowrap",opacity:loading?0.5:1 }}>
                Search
              </button>
            </div>
          </Card>

          {loading && <LoadingBar />}

          {nadacResults.length > 0 && (
            <Card>
              <CH t="NADAC Results" r={`${nadacResults.length} results`} />
              <div style={{ overflowX:"auto",padding:"0 0 8px" }}>
                <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10,fontFamily:FM }}>
                  <thead>
                    <tr style={{ borderBottom:`1px solid ${BD}` }}>
                      {["NDC","Drug Name","NADAC / Unit","Unit","Effective Date","OTC"].map(h => (
                        <th key={h} style={{ padding:"6px 10px",textAlign:h==="NADAC / Unit"?"right":"left",color:AL,fontWeight:600,fontSize:9,whiteSpace:"nowrap" }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {nadacResults.map((r, i) => (
                      <tr key={`${r.ndc}-${i}`} style={{ borderBottom:`1px solid ${BD}20` }}>
                        <td style={{ padding:"5px 10px",color:AL }}>{r.ndc}</td>
                        <td style={{ padding:"5px 10px",fontWeight:600,color:A,maxWidth:280,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.ndc_description}</td>
                        <td style={{ padding:"5px 10px",textAlign:"right",color:A,fontWeight:500 }}>${fmt(r.nadac_per_unit, 4)}</td>
                        <td style={{ padding:"5px 10px",color:AL }}>{r.pricing_unit}</td>
                        <td style={{ padding:"5px 10px",color:AL }}>{r.effective_date}</td>
                        <td style={{ padding:"5px 10px",color:r.is_otc?WARN:AL }}>{r.is_otc?"OTC":"Rx"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          )}

          {!loading && nadacResults.length === 0 && nadacSearch && (
            <div style={{ textAlign:"center",padding:40,color:AL,fontSize:12 }}>No results. Try a different drug name.</div>
          )}

          {!nadacSearch && (
            <div style={{ textAlign:"center",padding:40,color:AL,fontSize:12 }}>
              Search for a drug name to see NADAC pricing data.
            </div>
          )}
        </div>
      )}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary:`User is viewing Pharmacy Intelligence - ${tab}. ${stateSummary.length} states, ${fmtD(totalSpend)} total drug reimbursement.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CMS SDUD 2025 Q1-Q2 (2.64M rows) · SDUD 2024 · NADAC Weekly · Drug Rebate Program · All amounts pre-rebate
      </div>
    </div>
  );
}
