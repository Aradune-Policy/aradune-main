import React, { useState, useMemo, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import type { SafeTipProps } from "../types";
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

const isMobile = typeof window !== "undefined" && window.innerWidth < 768;

// ── Data Shape Interfaces ─────────────────────────────────────────────
interface FiveStarSummary {
  state_code: string;
  facility_count: number;
  avg_overall: number;
  avg_health: number;
  avg_staffing: number;
  avg_qm: number;
  avg_hprd: number;
  avg_hprd_rn: number;
  avg_turnover: number | null;
  total_beds: number;
  one_star: number;
  five_star: number;
  abuse_count: number;
  total_fines: number;
}

interface StaffingSummary {
  state_code: string;
  facility_count: number;
  avg_nursing_hprd: number;
  median_nursing_hprd: number;
  avg_rn_pct: number | null;
  avg_contract_pct: number | null;
  total_resident_days: number;
}

interface FiveStarFacility {
  provider_ccn: string;
  facility_name: string;
  city: string;
  county: string;
  certified_beds: number;
  avg_residents_per_day: number;
  ownership_type: string;
  overall_rating: number;
  health_inspection_rating: number;
  qm_rating: number;
  staffing_rating: number;
  hprd_total: number;
  hprd_rn: number;
  hprd_cna: number;
  turnover_total_pct: number | null;
  turnover_rn_pct: number | null;
  deficiency_count: number;
  fine_count: number;
  fine_total_dollars: number;
  total_penalties: number;
  abuse_flag: boolean;
  special_focus_status: string | null;
  chain_name: string | null;
  chain_size: number | null;
}

interface StaffingFacility {
  provider_ccn: string;
  facility_name: string;
  city: string;
  county: string;
  avg_nursing_hprd: number;
  avg_rn_hprd: number;
  avg_cna_hprd: number;
  avg_census: number;
  rn_contract_pct: number | null;
  days_reported: number;
}

// ── Shared Components ─────────────────────────────────────────────────
const Card = ({ children, accent, x }: { children: React.ReactNode; accent?: string; x?: boolean }) => (
  <div style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",borderTop:accent?`3px solid ${accent}`:"none",border:x?"none":`1px solid ${BD}` }}>{children}</div>
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

const safe = (v: number | null | undefined, fb: number = 0): number => (v==null||isNaN(v))?fb:v;
const fN = (v: number): string => {
  if(v==null||isNaN(v)||!isFinite(v)) return "0";
  if(v>=1e6)return `${(v/1e6).toFixed(1)}M`;
  if(v>=1e3)return `${(v/1e3).toFixed(0)}K`;
  return `${v}`;
};
const f$ = (v: number): string => {
  if (v==null||isNaN(v)||!isFinite(v)) return "$0";
  const abs=Math.abs(v),sign=v<0?"-":"";
  if(abs>=1e9)return `${sign}$${(abs/1e9).toFixed(1)}B`;
  if(abs>=1e6)return `${sign}$${(abs/1e6).toFixed(1)}M`;
  if(abs>=1e3)return `${sign}$${abs.toLocaleString(undefined,{maximumFractionDigits:0})}`;
  if(abs<10)return `${sign}$${abs.toFixed(2)}`;
  return `${sign}$${abs.toFixed(0)}`;
};

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

// ── Stars rendering ───────────────────────────────────────────────────
function Stars({ rating }: { rating: number }) {
  const stars: React.ReactNode[] = [];
  for (let i = 1; i <= 5; i++) {
    stars.push(
      <span key={i} style={{ color: i <= rating ? "#D97706" : BD, fontSize: 12 }}>
        {i <= rating ? "\u2605" : "\u2606"}
      </span>
    );
  }
  return <span style={{ letterSpacing: 1 }}>{stars}</span>;
}

// ── Rating color ──────────────────────────────────────────────────────
function ratingColor(v: number): string {
  if (v >= 4) return POS;
  if (v >= 3) return WARN;
  return NEG;
}

// ── Main Component ──────────────────────────────────────────────────────
export default function NursingFacility() {
  const { openIntelligence } = useAradune();
  const [tab, setTab] = useState<"quality" | "staffing" | "detail">("quality");
  const [detailState, setDetailState] = useState("FL");

  // Data
  const [fiveStarSummary, setFiveStarSummary] = useState<FiveStarSummary[] | null>(null);
  const [staffingSummary, setStaffingSummary] = useState<StaffingSummary[] | null>(null);
  const [detailFiveStar, setDetailFiveStar] = useState<FiveStarFacility[] | null>(null);
  const [detailStaffing, setDetailStaffing] = useState<StaffingFacility[] | null>(null);

  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  // ── Load summary data ─────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [fsRes, stRes] = await Promise.all([
          fetch(`${API_BASE}/api/five-star/summary`),
          fetch(`${API_BASE}/api/staffing/summary`),
        ]);
        if (cancelled) return;
        if (fsRes.ok) {
          const fsData = await fsRes.json();
          setFiveStarSummary(fsData as FiveStarSummary[]);
        }
        if (stRes.ok) {
          const stData = await stRes.json();
          setStaffingSummary(stData as StaffingSummary[]);
        }
        if (!fsRes.ok && !stRes.ok) {
          setLoadError("Failed to load nursing facility data. API may be unavailable.");
        }
      } catch (e) {
        console.error(e);
        if (!cancelled) setLoadError("Failed to load nursing facility data. Please try again.");
      }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  // ── Load detail data for selected state ───────────────────────────
  useEffect(() => {
    if (tab !== "detail") return;
    let cancelled = false;
    async function loadDetail() {
      setDetailLoading(true);
      try {
        const [fsRes, stRes] = await Promise.all([
          fetch(`${API_BASE}/api/five-star/${detailState}`),
          fetch(`${API_BASE}/api/staffing/${detailState}`),
        ]);
        if (cancelled) return;
        if (fsRes.ok) setDetailFiveStar(await fsRes.json() as FiveStarFacility[]);
        if (stRes.ok) setDetailStaffing(await stRes.json() as StaffingFacility[]);
      } catch (e) {
        console.error(e);
      }
      if (!cancelled) setDetailLoading(false);
    }
    loadDetail();
    return () => { cancelled = true; };
  }, [tab, detailState]);

  // ── Sorted states list ────────────────────────────────────────────
  const stateList = useMemo(() => {
    if (!fiveStarSummary) return [];
    return fiveStarSummary
      .map(r => r.state_code)
      .filter(s => STATE_NAMES[s])
      .sort((a, b) => (STATE_NAMES[a] || a).localeCompare(STATE_NAMES[b] || b));
  }, [fiveStarSummary]);

  // ── Five Star chart data ──────────────────────────────────────────
  const qualityChartData = useMemo(() => {
    if (!fiveStarSummary) return [];
    return [...fiveStarSummary]
      .filter(r => STATE_NAMES[r.state_code])
      .sort((a, b) => a.avg_overall - b.avg_overall)
      .map(r => ({
        st: r.state_code,
        name: STATE_NAMES[r.state_code] || r.state_code,
        avg: r.avg_overall,
        facilities: r.facility_count,
        beds: r.total_beds,
        fiveStar: r.five_star,
        oneStar: r.one_star,
        fines: r.total_fines,
        abuse: r.abuse_count,
      }));
  }, [fiveStarSummary]);

  // ── Staffing chart data ───────────────────────────────────────────
  const staffingChartData = useMemo(() => {
    if (!staffingSummary) return [];
    return [...staffingSummary]
      .filter(r => STATE_NAMES[r.state_code])
      .sort((a, b) => a.avg_nursing_hprd - b.avg_nursing_hprd)
      .map(r => ({
        st: r.state_code,
        name: STATE_NAMES[r.state_code] || r.state_code,
        hprd: r.avg_nursing_hprd,
        median: r.median_nursing_hprd,
        facilities: r.facility_count,
        rnPct: r.avg_rn_pct,
        contractPct: r.avg_contract_pct,
        residentDays: r.total_resident_days,
      }));
  }, [staffingSummary]);

  // ── National aggregates ───────────────────────────────────────────
  const natlQuality = useMemo(() => {
    if (!fiveStarSummary || fiveStarSummary.length === 0) return null;
    const total = fiveStarSummary.reduce((acc, r) => ({
      facilities: acc.facilities + r.facility_count,
      beds: acc.beds + r.total_beds,
      oneStar: acc.oneStar + r.one_star,
      fiveStar: acc.fiveStar + r.five_star,
      abuse: acc.abuse + r.abuse_count,
      fines: acc.fines + r.total_fines,
      ratingSum: acc.ratingSum + r.avg_overall * r.facility_count,
    }), { facilities: 0, beds: 0, oneStar: 0, fiveStar: 0, abuse: 0, fines: 0, ratingSum: 0 });
    return { ...total, avgOverall: total.ratingSum / total.facilities };
  }, [fiveStarSummary]);

  const natlStaffing = useMemo(() => {
    if (!staffingSummary || staffingSummary.length === 0) return null;
    const total = staffingSummary.reduce((acc, r) => ({
      facilities: acc.facilities + r.facility_count,
      residentDays: acc.residentDays + r.total_resident_days,
      hprdSum: acc.hprdSum + r.avg_nursing_hprd * r.facility_count,
    }), { facilities: 0, residentDays: 0, hprdSum: 0 });
    return { ...total, avgHprd: total.hprdSum / total.facilities };
  }, [staffingSummary]);

  if (loading) return <LoadingBar text="Loading nursing facility data" detail="Five-Star ratings + PBJ staffing" />;

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
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(217,119,6,0.1)",color:"#D97706",fontWeight:600 }}>CMS Five-Star + PBJ</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{fiveStarSummary?.length || 0} states</span>
          </div>
          <div style={{ display:"flex",gap:8,alignItems:"center" }}>
            <button onClick={() => openIntelligence({ summary: "User is viewing Nursing Facility quality and staffing data" })} style={{
              padding:"5px 12px",borderRadius:6,border:"none",
              background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
            }}>Ask Aradune</button>
          </div>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(217,119,6,0.03)",borderLeft:"3px solid #D97706" }}>
        <span style={{ fontWeight:700,color:A }}>Nursing Facility Intelligence.</span> Five-Star quality ratings from CMS Care Compare and nurse staffing hours per resident day from PBJ (Payroll-Based Journal) data. Covers all Medicare/Medicaid certified skilled nursing facilities.
      </div></Card>

      {/* Tab Pills */}
      <div style={{ display:"flex",gap:4,margin:"10px 0",flexWrap:"wrap" }}>
        <Pill on={tab==="quality"} onClick={()=>setTab("quality")}>Quality Ratings</Pill>
        <Pill on={tab==="staffing"} onClick={()=>setTab("staffing")}>Staffing</Pill>
        <Pill on={tab==="detail"} onClick={()=>setTab("detail")}>State Detail</Pill>
      </div>

      {/* ── TAB 1: Quality Ratings ─────────────────────────────────────── */}
      {tab === "quality" && fiveStarSummary && <>
        {/* National summary metrics */}
        {natlQuality && <Card accent="#D97706">
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>National Overview</div>
            <div style={{ fontSize:10,color:AL }}>CMS Five-Star Quality Rating System</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"repeat(2,1fr)":"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l="Facilities" v={fN(natlQuality.facilities)} />
            <Met l="Certified Beds" v={fN(natlQuality.beds)} />
            <Met l="Avg Overall Rating" v={natlQuality.avgOverall.toFixed(2)} cl={ratingColor(natlQuality.avgOverall)} sub="Weighted by facility count" />
            <Met l="Five-Star Facilities" v={fN(natlQuality.fiveStar)} cl={POS} sub={`${((natlQuality.fiveStar/natlQuality.facilities)*100).toFixed(1)}%`} />
            <Met l="One-Star Facilities" v={fN(natlQuality.oneStar)} cl={NEG} sub={`${((natlQuality.oneStar/natlQuality.facilities)*100).toFixed(1)}%`} />
            <Met l="Total Fines" v={f$(natlQuality.fines)} cl={NEG} />
          </div>
        </Card>}

        {/* Avg Overall Rating by State */}
        {qualityChartData.length > 0 && <Card x>
          <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"10px 14px 2px" }}>
            <CH t="Average Overall Rating by State" b={`${qualityChartData.length} states`} />
            <ExportBtn label="Export Quality CSV" onClick={() => {
              if (!fiveStarSummary) return;
              downloadCSV("nursing_quality_by_state.csv",
                ["State","Facilities","Avg Overall","Avg Health","Avg Staffing","Avg QM","Five-Star","One-Star","Abuse Count","Total Fines"],
                fiveStarSummary.map(r=>[STATE_NAMES[r.state_code]||r.state_code,r.facility_count,r.avg_overall,r.avg_health,r.avg_staffing,r.avg_qm,r.five_star,r.one_star,r.abuse_count,r.total_fines])
              );
            }} />
          </div>
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="nursing-quality-ratings">
            <ResponsiveContainer width="100%" height={Math.max(240, qualityChartData.length * 13)}>
              <BarChart data={qualityChartData} layout="vertical" margin={{ left:52,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
                <XAxis type="number" domain={[0,5]} tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false}/>
                <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} axisLine={false} tickLine={false} width={28}/>
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Avg overall: <b>{safe(d.avg as number).toFixed(2)}</b></div>
                    <div>{fN(d.facilities as number)} facilities, {fN(d.beds as number)} beds</div>
                    <div style={{ color:"#99ff99" }}>{fN(d.fiveStar as number)} five-star ({((d.fiveStar as number)/safe(d.facilities as number,1)*100).toFixed(0)}%)</div>
                    <div style={{ color:"#ff9999" }}>{fN(d.oneStar as number)} one-star ({((d.oneStar as number)/safe(d.facilities as number,1)*100).toFixed(0)}%)</div>
                    {(d.fines as number) > 0 && <div>Fines: {f$(d.fines as number)}</div>}
                  </div>
                )}/>}/>
                <Bar dataKey="avg" barSize={8} radius={[0,3,3,0]}>
                  {qualityChartData.map((d,i)=><Cell key={i} fill={ratingColor(d.avg)} opacity={d.avg < 3 ? 0.8 : 0.6}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>4+ Stars</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:WARN,verticalAlign:"middle",marginRight:3 }}/>3-4 Stars</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>Below 3 Stars</span>
            </div>
          </div>
        </Card>}

        {/* State table */}
        <Card x>
          <CH t="Quality Ratings by State" b="CMS Five-Star" r={`${fiveStarSummary.length} states`}/>
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Facilities","Avg Overall","Avg Health","Avg Staffing","Avg QM","Five-Star","One-Star","Fines"].map(h=>(
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {fiveStarSummary.map(r=>(
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}`,cursor:"pointer" }} onClick={()=>{setDetailState(r.state_code);setTab("detail");}}>
                    <td style={{ padding:"4px",fontWeight:600,color:cB }}>{STATE_NAMES[r.state_code]||r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{fN(r.facility_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:ratingColor(r.avg_overall) }}>{r.avg_overall.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_health.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_staffing.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_qm.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,color:POS }}>{r.five_star}</td>
                    <td style={{ padding:"4px",fontFamily:FM,color:NEG }}>{r.one_star}</td>
                    <td style={{ padding:"4px",fontFamily:FM,color:r.total_fines>0?NEG:AL }}>{f$(r.total_fines)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </>}

      {/* ── TAB 2: Staffing ────────────────────────────────────────────── */}
      {tab === "staffing" && staffingSummary && <>
        {/* National staffing summary */}
        {natlStaffing && <Card accent="#D97706">
          <div style={{ padding:"14px 16px 4px" }}>
            <div style={{ fontSize:18,fontWeight:300 }}>Staffing Overview</div>
            <div style={{ fontSize:10,color:AL }}>PBJ Payroll-Based Journal staffing data</div>
          </div>
          <div style={{ display:"grid",gridTemplateColumns:isMobile?"repeat(2,1fr)":"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
            <Met l="Facilities Reporting" v={fN(natlStaffing.facilities)} />
            <Met l="Avg Nursing HPRD" v={natlStaffing.avgHprd.toFixed(2)} sub="Hours per resident day" />
            <Met l="Total Resident Days" v={fN(natlStaffing.residentDays)} />
          </div>
        </Card>}

        {/* Staffing HPRD by state chart */}
        {staffingChartData.length > 0 && <Card x>
          <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"10px 14px 2px" }}>
            <CH t="Avg Nursing Hours per Resident Day" b={`${staffingChartData.length} states`} />
            <ExportBtn label="Export Staffing CSV" onClick={() => {
              if (!staffingSummary) return;
              downloadCSV("nursing_staffing_by_state.csv",
                ["State","Facilities","Avg HPRD","Median HPRD","RN %","Contract %","Resident Days"],
                staffingSummary.map(r=>[STATE_NAMES[r.state_code]||r.state_code,r.facility_count,r.avg_nursing_hprd,r.median_nursing_hprd,r.avg_rn_pct,r.avg_contract_pct,r.total_resident_days])
              );
            }} />
          </div>
          <div style={{ padding:"0 14px 8px" }}>
            <ChartActions filename="nursing-staffing-hprd">
            <ResponsiveContainer width="100%" height={Math.max(240, staffingChartData.length * 13)}>
              <BarChart data={staffingChartData} layout="vertical" margin={{ left:52,right:16 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false}/>
                <XAxis type="number" tick={{ fill:AL,fontSize:8,fontFamily:FM }} axisLine={false} tickLine={false} tickFormatter={(v: number)=>`${v.toFixed(1)}`}/>
                <YAxis type="category" dataKey="st" tick={{ fill:A,fontSize:7,fontFamily:FM }} axisLine={false} tickLine={false} width={28}/>
                <Tooltip content={<SafeTip render={(d: Record<string, unknown>)=>(
                  <div>
                    <div style={{ fontWeight:600 }}>{String(d.name ?? "")}</div>
                    <div>Avg HPRD: <b>{safe(d.hprd as number).toFixed(2)}</b></div>
                    <div>Median HPRD: {safe(d.median as number).toFixed(2)}</div>
                    <div>{fN(d.facilities as number)} facilities</div>
                    {(d.rnPct as number) != null && <div>RN share: {safe(d.rnPct as number).toFixed(1)}%</div>}
                    {(d.contractPct as number) != null && <div>Contract staff: {safe(d.contractPct as number).toFixed(1)}%</div>}
                  </div>
                )}/>}/>
                <Bar dataKey="hprd" barSize={8} radius={[0,3,3,0]}>
                  {staffingChartData.map((d,i)=><Cell key={i} fill={d.hprd >= 4 ? POS : d.hprd >= 3 ? WARN : NEG} opacity={0.65}/>)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            </ChartActions>
            <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"4px 0",flexWrap:"wrap" }}>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>4+ HPRD</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:WARN,verticalAlign:"middle",marginRight:3 }}/>3-4 HPRD</span>
              <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>Below 3 HPRD</span>
              <span>CMS minimum standard: 3.48 total HPRD</span>
            </div>
          </div>
        </Card>}

        {/* State table */}
        <Card x>
          <CH t="Staffing by State" b="PBJ data" r={`${staffingSummary.length} states`}/>
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Facilities","Avg HPRD","Median HPRD","RN %","Contract %","Resident Days"].map(h=>(
                  <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {staffingSummary.map(r=>(
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}`,cursor:"pointer" }} onClick={()=>{setDetailState(r.state_code);setTab("detail");}}>
                    <td style={{ padding:"4px",fontWeight:600,color:cB }}>{STATE_NAMES[r.state_code]||r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{fN(r.facility_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:r.avg_nursing_hprd>=4?POS:r.avg_nursing_hprd>=3?WARN:NEG }}>{r.avg_nursing_hprd.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.median_nursing_hprd.toFixed(2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_rn_pct != null ? `${r.avg_rn_pct.toFixed(1)}%` : "--"}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_contract_pct != null ? `${r.avg_contract_pct.toFixed(1)}%` : "--"}</td>
                    <td style={{ padding:"4px",fontFamily:FM }}>{fN(r.total_resident_days)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </>}

      {/* ── TAB 3: State Detail ────────────────────────────────────────── */}
      {tab === "detail" && <>
        <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap",margin:"0 0 10px" }}>
          <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>State</span>
            <select value={detailState} onChange={e=>setDetailState(e.currentTarget.value)} style={{ background:SF,border:`1px solid ${BD}`,padding:"5px 10px",borderRadius:6,fontSize:11,color:A }}>
              {(stateList.length > 0 ? stateList : Object.keys(STATE_NAMES).sort((a,b)=>(STATE_NAMES[a]||a).localeCompare(STATE_NAMES[b]||b))).map(k=><option key={k} value={k}>{STATE_NAMES[k]||k}</option>)}
            </select>
          </div>
        </div>

        {detailLoading && <LoadingBar text={`Loading ${STATE_NAMES[detailState]||detailState} facilities`} detail="Five-Star + PBJ staffing" />}

        {!detailLoading && <>
          {/* State summary from summary data */}
          {(() => {
            const fsSt = fiveStarSummary?.find(r => r.state_code === detailState);
            const stSt = staffingSummary?.find(r => r.state_code === detailState);
            if (!fsSt && !stSt) return null;
            return (
              <Card accent="#D97706">
                <div style={{ padding:"14px 16px 4px" }}>
                  <div style={{ fontSize:18,fontWeight:300 }}>{STATE_NAMES[detailState] || detailState}</div>
                  <div style={{ fontSize:10,color:AL }}>Nursing facility summary</div>
                </div>
                <div style={{ display:"grid",gridTemplateColumns:isMobile?"repeat(2,1fr)":"repeat(auto-fit,minmax(120px,1fr))",padding:"0 6px 12px" }}>
                  {fsSt && <>
                    <Met l="Facilities" v={fN(fsSt.facility_count)} />
                    <Met l="Avg Overall" v={fsSt.avg_overall.toFixed(2)} cl={ratingColor(fsSt.avg_overall)} />
                    <Met l="Five-Star" v={fsSt.five_star} cl={POS} sub={`${((fsSt.five_star/fsSt.facility_count)*100).toFixed(0)}%`} />
                    <Met l="One-Star" v={fsSt.one_star} cl={NEG} sub={`${((fsSt.one_star/fsSt.facility_count)*100).toFixed(0)}%`} />
                  </>}
                  {stSt && <>
                    <Met l="Avg Nursing HPRD" v={stSt.avg_nursing_hprd.toFixed(2)} cl={stSt.avg_nursing_hprd >= 4 ? POS : stSt.avg_nursing_hprd >= 3 ? WARN : NEG} />
                    <Met l="RN Share" v={stSt.avg_rn_pct != null ? `${stSt.avg_rn_pct.toFixed(1)}%` : "--"} />
                  </>}
                </div>
              </Card>
            );
          })()}

          {/* Five-Star facility table */}
          {detailFiveStar && detailFiveStar.length > 0 && <Card x>
            <div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline",padding:"10px 14px 2px" }}>
              <CH t="Five-Star Ratings" b={`${detailFiveStar.length} facilities in ${STATE_NAMES[detailState]||detailState}`} />
              <ExportBtn label="Export Facilities" onClick={() => {
                downloadCSV(`nursing_fivestar_${detailState}.csv`,
                  ["CCN","Facility","City","Beds","Overall","Health","Staffing","QM","HPRD Total","HPRD RN","Turnover","Deficiencies","Fines","Abuse","Chain"],
                  detailFiveStar.map(r=>[r.provider_ccn,r.facility_name,r.city,r.certified_beds,r.overall_rating,r.health_inspection_rating,r.staffing_rating,r.qm_rating,r.hprd_total,r.hprd_rn,r.turnover_total_pct,r.deficiency_count,r.fine_total_dollars,r.abuse_flag?"Y":"",r.chain_name||""])
                );
              }} />
            </div>
            <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
                <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                  {["Facility","City","Beds","Overall","Health","Staff","QM","HPRD","Turnover","Defic.","Fines"].map(h=>(
                    <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {detailFiveStar.slice(0, 100).map(r=>(
                    <tr key={r.provider_ccn} style={{ borderBottom:`1px solid ${SF}` }}>
                      <td style={{ padding:"4px",maxWidth:160,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>
                        <span style={{ fontWeight:600,color:A }}>{r.facility_name}</span>
                        {r.abuse_flag && <span style={{ fontSize:8,color:NEG,marginLeft:4 }} title="Abuse cited">!</span>}
                        {r.special_focus_status && <span style={{ fontSize:7,color:WARN,marginLeft:4,fontFamily:FM }}>SFF</span>}
                      </td>
                      <td style={{ padding:"4px",color:AL,fontSize:9 }}>{r.city}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.certified_beds}</td>
                      <td style={{ padding:"4px" }}><Stars rating={r.overall_rating} /></td>
                      <td style={{ padding:"4px",fontFamily:FM,color:ratingColor(r.health_inspection_rating) }}>{r.health_inspection_rating}</td>
                      <td style={{ padding:"4px",fontFamily:FM,color:ratingColor(r.staffing_rating) }}>{r.staffing_rating}</td>
                      <td style={{ padding:"4px",fontFamily:FM,color:ratingColor(r.qm_rating) }}>{r.qm_rating}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{safe(r.hprd_total).toFixed(2)}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.turnover_total_pct != null ? `${r.turnover_total_pct.toFixed(0)}%` : "--"}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.deficiency_count}</td>
                      <td style={{ padding:"4px",fontFamily:FM,color:r.fine_total_dollars>0?NEG:AL }}>{r.fine_total_dollars > 0 ? f$(r.fine_total_dollars) : "--"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {detailFiveStar.length > 100 && <div style={{ fontSize:9,color:AL,padding:"6px 0" }}>Showing 100 of {detailFiveStar.length} facilities.</div>}
            </div>
          </Card>}

          {/* Staffing facility table */}
          {detailStaffing && detailStaffing.length > 0 && <Card x>
            <CH t="Staffing Detail" b={`${detailStaffing.length} facilities · PBJ data`}/>
            <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
              <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
                <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                  {["Facility","City","Nursing HPRD","RN HPRD","CNA HPRD","Avg Census","RN Contract %","Days"].map(h=>(
                    <th key={h} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {detailStaffing.slice(0, 100).map(r=>(
                    <tr key={r.provider_ccn} style={{ borderBottom:`1px solid ${SF}` }}>
                      <td style={{ padding:"4px",fontWeight:600,color:A,maxWidth:160,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.facility_name}</td>
                      <td style={{ padding:"4px",color:AL,fontSize:9 }}>{r.city}</td>
                      <td style={{ padding:"4px",fontFamily:FM,fontWeight:600,color:r.avg_nursing_hprd>=4?POS:r.avg_nursing_hprd>=3?WARN:NEG }}>{r.avg_nursing_hprd.toFixed(2)}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{safe(r.avg_rn_hprd).toFixed(2)}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{safe(r.avg_cna_hprd).toFixed(2)}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.avg_census}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.rn_contract_pct != null ? `${r.rn_contract_pct.toFixed(1)}%` : "--"}</td>
                      <td style={{ padding:"4px",fontFamily:FM }}>{r.days_reported}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {detailStaffing.length > 100 && <div style={{ fontSize:9,color:AL,padding:"6px 0" }}>Showing 100 of {detailStaffing.length} facilities.</div>}
            </div>
          </Card>}

          {!detailFiveStar && !detailStaffing && !detailLoading && (
            <Card><div style={{ padding:24,textAlign:"center",color:AL,fontSize:12 }}>No facility data found for {STATE_NAMES[detailState]||detailState}.</div></Card>
          )}
        </>}
      </>}

      {/* About */}
      <Card><CH t="Data Sources & Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
        <b>Five-Star Quality:</b> CMS Care Compare Five-Star Quality Rating System for nursing homes. Overall rating (1-5 stars) based on health inspections, staffing, and quality measures. Updated monthly.<br/>
        <b>Staffing:</b> CMS Payroll-Based Journal (PBJ) daily nurse staffing data. Facilities report actual hours worked by nursing staff category (RN, LPN, CNA). Hours per resident day (HPRD) is the primary staffing adequacy metric.<br/>
        <b>CMS staffing standard:</b> Final rule (2024) requires minimum 3.48 total nursing HPRD, including 0.55 RN HPRD and 2.45 nurse aide HPRD.<br/>
        <b>Limitations:</b> PBJ data is self-reported by facilities. Five-Star ratings reflect a point-in-time composite. State-level averages mask within-state variation. Contract staff data may undercount actual agency use.
      </div></Card>

      <div style={{ fontSize:10,color:AL,marginTop:8 }}>Aradune Nursing Facility Intelligence v1.0 -- CMS Five-Star + PBJ</div>
    </div>
  );
}
