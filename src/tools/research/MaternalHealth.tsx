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
interface MortalityRow { state_code: string; year: number; maternal_mortality_rate: number; maternal_deaths: number; live_births: number }
interface AccessRow { state_code: string; hpsa_count: number; avg_svi_score: number; county_count: number }
interface QualityRow { state_code: string; measure_id: string; measure_name: string; measure_rate: number }
interface InfantRow { state_code: string; year: number; infant_mortality_rate: number }
interface CompositeRow { state_code: string; maternal_mortality_rate: number; hpsa_count: number; avg_svi_score: number; avg_maternal_quality: number }

type Tab = "mortality" | "access" | "quality" | "composite";

export default function MaternalHealth() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();

  const [tab, setTab] = useState<Tab>("mortality");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [mortalityData, setMortalityData] = useState<MortalityRow[]>([]);
  const [accessData, setAccessData] = useState<AccessRow[]>([]);
  const [qualityData, setQualityData] = useState<QualityRow[]>([]);
  const [compositeData, setCompositeData] = useState<CompositeRow[]>([]);

  useEffect(() => {
    setLoading(true);
    setLoadError(null);
    const endpoints: Record<Tab, string> = {
      mortality: "/api/research/maternal-health/mortality",
      access: "/api/research/maternal-health/access",
      quality: "/api/research/maternal-health/quality",
      composite: "/api/research/maternal-health/composite",
    };
    fetch(`${API_BASE}${endpoints[tab]}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        const rows = Array.isArray(d) ? d : d.rows || d.data || [];
        if (tab === "mortality") setMortalityData(rows);
        else if (tab === "access") setAccessData(rows);
        else if (tab === "quality") setQualityData(rows);
        else setCompositeData(rows);
      })
      .catch(e => setLoadError(e.message))
      .finally(() => setLoading(false));
  }, [tab]);

  // -- Mortality --
  const latestYear = useMemo(() => {
    if (!mortalityData.length) return 0;
    return Math.max(...mortalityData.map(r => r.year));
  }, [mortalityData]);

  const mortalityLatest = useMemo(() =>
    mortalityData.filter(r => r.year === latestYear), [mortalityData, latestYear]);

  const mortalityChart = useMemo(() =>
    [...mortalityLatest].sort((a, b) => b.maternal_mortality_rate - a.maternal_mortality_rate)
      .slice(0, 30).map(r => ({
        name: r.state_code, rate: r.maternal_mortality_rate, deaths: r.maternal_deaths,
      })), [mortalityLatest]);

  const mortalityStats = useMemo(() => {
    if (!mortalityLatest.length) return { natRate: 0, highest: "", highestRate: 0, lowest: "", lowestRate: 0 };
    const totalDeaths = mortalityLatest.reduce((s, r) => s + r.maternal_deaths, 0);
    const totalBirths = mortalityLatest.reduce((s, r) => s + r.live_births, 0);
    const natRate = totalBirths > 0 ? (totalDeaths / totalBirths) * 100000 : 0;
    const sorted = [...mortalityLatest].sort((a, b) => b.maternal_mortality_rate - a.maternal_mortality_rate);
    const valid = sorted.filter(r => r.maternal_mortality_rate > 0);
    return {
      natRate,
      highest: valid[0]?.state_code || "",
      highestRate: valid[0]?.maternal_mortality_rate || 0,
      lowest: valid[valid.length - 1]?.state_code || "",
      lowestRate: valid[valid.length - 1]?.maternal_mortality_rate || 0,
    };
  }, [mortalityLatest]);

  // -- Access Barriers --
  const accessScatter = useMemo(() =>
    accessData.filter(r => r.hpsa_count != null && r.avg_svi_score != null).map(r => ({
      name: r.state_code, x: r.hpsa_count, y: r.avg_svi_score, z: r.county_count,
    })), [accessData]);

  // -- Quality --
  const qualityMeasures = useMemo(() => {
    const ids = new Set<string>();
    qualityData.forEach(r => ids.add(r.measure_id));
    return Array.from(ids);
  }, [qualityData]);

  const [selectedMeasure, setSelectedMeasure] = useState<string>("");

  useEffect(() => {
    if (qualityMeasures.length > 0 && !selectedMeasure) {
      setSelectedMeasure(qualityMeasures[0]);
    }
  }, [qualityMeasures, selectedMeasure]);

  const qualityFiltered = useMemo(() =>
    qualityData.filter(r => r.measure_id === selectedMeasure), [qualityData, selectedMeasure]);

  const qualityChart = useMemo(() =>
    [...qualityFiltered].sort((a, b) => a.measure_rate - b.measure_rate)
      .slice(0, 30).map(r => ({
        name: r.state_code, rate: r.measure_rate,
      })), [qualityFiltered]);

  const qualityMeasureName = useMemo(() => {
    const match = qualityData.find(r => r.measure_id === selectedMeasure);
    return match?.measure_name || selectedMeasure;
  }, [qualityData, selectedMeasure]);

  // -- Composite --
  const compositeChart = useMemo(() => {
    // Normalize scores: higher = worse
    if (!compositeData.length) return [];
    const maxMort = Math.max(...compositeData.map(r => r.maternal_mortality_rate || 0), 1);
    const maxHpsa = Math.max(...compositeData.map(r => r.hpsa_count || 0), 1);
    const maxSvi = Math.max(...compositeData.map(r => r.avg_svi_score || 0), 1);
    const minQual = Math.min(...compositeData.filter(r => r.avg_maternal_quality > 0).map(r => r.avg_maternal_quality), 100);
    const maxQual = Math.max(...compositeData.map(r => r.avg_maternal_quality || 0), 1);

    return [...compositeData].map(r => {
      const mortScore = maxMort > 0 ? (r.maternal_mortality_rate / maxMort) * 25 : 0;
      const hpsaScore = maxHpsa > 0 ? (r.hpsa_count / maxHpsa) * 25 : 0;
      const sviScore = maxSvi > 0 ? (r.avg_svi_score / maxSvi) * 25 : 0;
      const qualScore = maxQual > 0 ? ((maxQual - r.avg_maternal_quality) / (maxQual - minQual + 1)) * 25 : 0;
      return {
        name: r.state_code,
        score: mortScore + hpsaScore + sviScore + qualScore,
        mortality: r.maternal_mortality_rate,
        hpsa: r.hpsa_count,
        svi: r.avg_svi_score,
        quality: r.avg_maternal_quality,
      };
    }).sort((a, b) => b.score - a.score).slice(0, 35);
  }, [compositeData]);

  const topQuartile = useMemo(() => {
    if (!compositeChart.length) return new Set<string>();
    const cutoff = Math.ceil(compositeChart.length * 0.25);
    return new Set(compositeChart.slice(0, cutoff).map(r => r.name));
  }, [compositeChart]);

  const tabs: { key: Tab; label: string }[] = [
    { key: "mortality", label: "Mortality Landscape" },
    { key: "access", label: "Access Barriers" },
    { key: "quality", label: "Quality Gaps" },
    { key: "composite", label: "Composite Risk" },
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
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(164,38,44,0.1)",color:NEG,fontWeight:600 }}>RESEARCH</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>CDC Mortality + SVI + HPSA + Core Set</span>
          </div>
          <button onClick={() => openIntelligence({ summary: `User is viewing Maternal Health Deserts -- ${tab} tab. ${mortalityData.length} mortality, ${accessData.length} access, ${qualityData.length} quality, ${compositeData.length} composite records.` })} style={{
            padding:"5px 12px",borderRadius:6,border:"none",background:cB,color:"#fff",fontSize:11,cursor:"pointer",fontWeight:600,
          }}>Ask Aradune</button>
        </div>
      </div>

      {/* Guide */}
      <Card><div style={{ padding:"10px 14px",fontSize:11,color:AL,lineHeight:1.6,background:"rgba(164,38,44,0.03)",borderLeft:`3px solid ${NEG}` }}>
        <span style={{ fontWeight:700,color:A }}>Maternal Health Deserts.</span> Multi-dimensional mapping of maternal health risk across social vulnerability, provider access, quality measure performance, and mortality outcomes.
      </div></Card>

      {/* Tab pills */}
      <div style={{ display:"flex",gap:6,marginTop:10,marginBottom:12,flexWrap:"wrap" }}>
        {tabs.map(t => <Pill key={t.key} active={tab===t.key} label={t.label} onClick={() => setTab(t.key)} />)}
      </div>

      {/* Mortality Landscape */}
      {tab === "mortality" && <>
        {latestYear > 0 && <div style={{ fontSize:10,color:AL,fontFamily:FM,marginBottom:8 }}>Showing {latestYear} data ({mortalityLatest.length} states)</div>}

        <div style={{ display:"grid",gridTemplateColumns:isMobile?"1fr":"repeat(3,1fr)",gap:8,marginBottom:10 }}>
          <Card accent={NEG}><Met l="National Rate" v={`${fmt(mortalityStats.natRate)} / 100K`} cl={NEG} sub="Maternal deaths per 100K live births" /></Card>
          <Card accent={NEG}><Met l="Highest State" v={`${STATE_NAMES[mortalityStats.highest] || mortalityStats.highest}`} cl={NEG} sub={`${fmt(mortalityStats.highestRate)} / 100K`} /></Card>
          <Card accent={POS}><Met l="Lowest State" v={`${STATE_NAMES[mortalityStats.lowest] || mortalityStats.lowest}`} cl={POS} sub={`${fmt(mortalityStats.lowestRate)} / 100K`} /></Card>
        </div>

        {mortalityChart.length > 0 ? <Card>
          <CH t="Maternal Mortality Rate by State" b={`${latestYear} -- deaths per 100K live births`} r={`${mortalityLatest.length} states`} />
          <ChartActions filename="maternal-mortality">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={mortalityChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:NEG }}>Rate: {fmt(d.rate)} / 100K</div>
                    <div style={{ color:AL }}>Deaths: {d.deaths}</div>
                  </div>;
                }} />
                <Bar dataKey="rate" name="Rate / 100K" radius={[3,3,0,0]}>
                  {mortalityChart.map((d, i) => {
                    const aboveNat = d.rate > mortalityStats.natRate;
                    return <Cell key={i} fill={aboveNat ? NEG : POS} />;
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ padding:"2px 14px 8px",fontSize:9,color:AL,fontFamily:FM }}>
            National average: {fmt(mortalityStats.natRate)} per 100,000 live births. States above average shown in red.
          </div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No mortality data available</div></Card>}

        {mortalityLatest.length > 0 && <Card>
          <CH t="Mortality Detail" b={`${latestYear}`} r={`${mortalityLatest.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","MMR / 100K","Deaths","Live Births"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...mortalityLatest].sort((a,b) => b.maternal_mortality_rate - a.maternal_mortality_rate).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:700,color:r.maternal_mortality_rate>mortalityStats.natRate?NEG:POS }}>{fmt(r.maternal_mortality_rate)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.maternal_deaths)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmtK(r.live_births)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Access Barriers */}
      {tab === "access" && <>
        {accessScatter.length > 0 ? <Card>
          <CH t="HPSA Shortages vs Social Vulnerability" b="High SVI + many HPSAs = critical access barrier" r={`${accessData.length} states`} />
          <ChartActions filename="access-scatter">
            <ResponsiveContainer width="100%" height={320}>
              <ScatterChart margin={{ top:10,right:20,bottom:10,left:10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="x" name="HPSA Count" tick={{ fontSize:9,fill:AL }} label={{ value:"HPSA Designations",position:"bottom",fontSize:9,fill:AL }} />
                <YAxis dataKey="y" name="SVI Score" tick={{ fontSize:9,fill:AL }} domain={[0,1]} label={{ value:"Avg SVI Score",angle:-90,position:"insideLeft",fontSize:9,fill:AL }} />
                <ZAxis dataKey="z" range={[40,300]} />
                <Tooltip content={({ active, payload }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[d.name] || d.name}</div>
                    <div style={{ color:AL }}>HPSA Count: {d.x}</div>
                    <div style={{ color:AL }}>Avg SVI: {fmt(d.y,2)}</div>
                    <div style={{ color:AL }}>Counties: {d.z}</div>
                  </div>;
                }} />
                <Scatter data={accessScatter} fill={cB}>
                  {accessScatter.map((d, i) => <Cell key={i} fill={d.y > 0.6 && d.x > 50 ? NEG : d.y > 0.4 ? WARN : cB} />)}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ padding:"2px 14px 8px",fontSize:9,color:AL,fontFamily:FM }}>
            Red dots: states with high social vulnerability (SVI &gt; 0.6) AND many provider shortages (HPSA &gt; 50)
          </div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No access data available</div></Card>}

        {accessData.length > 0 && <Card>
          <CH t="Access Barriers Detail" b={`${accessData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","HPSA Count","Avg SVI Score","Counties"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {[...accessData].sort((a,b) => b.avg_svi_score - a.avg_svi_score).map(r => (
                  <tr key={r.state_code} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.hpsa_count>50?NEG:AL }}>{fmtK(r.hpsa_count)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600,color:r.avg_svi_score>0.6?NEG:r.avg_svi_score>0.4?WARN:POS }}>{fmt(r.avg_svi_score,2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{r.county_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Quality Gaps */}
      {tab === "quality" && <>
        {qualityMeasures.length > 0 && <Card>
          <CH t="Select Quality Measure" r={`${qualityMeasures.length} measures`} />
          <div style={{ padding:"4px 14px 10px",display:"flex",gap:4,flexWrap:"wrap" }}>
            {qualityMeasures.map(m => (
              <Pill key={m} active={selectedMeasure===m} label={m} onClick={() => setSelectedMeasure(m)} />
            ))}
          </div>
        </Card>}

        {qualityChart.length > 0 ? <Card>
          <CH t={qualityMeasureName || "Quality Measure Rates"} b={`${selectedMeasure} -- states ranked by rate`} r={`${qualityFiltered.length} states`} />
          <ChartActions filename="quality-rates">
            <ResponsiveContainer width="100%" height={320}>
              <BarChart data={qualityChart} margin={{ top:10,right:16,bottom:4,left:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                <XAxis dataKey="name" tick={{ fontSize:9,fill:AL }} interval={0} angle={-45} textAnchor="end" height={50} />
                <YAxis tick={{ fontSize:9,fill:AL }} tickFormatter={v => `${v}%`} />
                <Tooltip content={({ active, payload, label }) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  return <div style={{ background:WH,border:`1px solid ${BD}`,borderRadius:6,padding:"6px 10px",fontSize:10,fontFamily:FM,boxShadow:SH }}>
                    <div style={{ fontWeight:600,color:A }}>{STATE_NAMES[label] || label}</div>
                    <div style={{ color:AL }}>Rate: {fmt(d.rate)}%</div>
                  </div>;
                }} />
                <Bar dataKey="rate" name="Measure Rate" radius={[3,3,0,0]}>
                  {qualityChart.map((d, i) => {
                    const avg = qualityFiltered.reduce((s, r) => s + r.measure_rate, 0) / qualityFiltered.length;
                    return <Cell key={i} fill={d.rate < avg * 0.8 ? NEG : d.rate < avg ? WARN : POS} />;
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No quality data available for this measure</div></Card>}

        {qualityData.length > 0 && <Card>
          <CH t="Quality Measures Full Data" b={`${qualityData.length} state-measure records`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","Measure ID","Measure Name","Rate"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"||h==="Measure ID"||h==="Measure Name"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {qualityData.slice(0, 200).map((r, i) => (
                  <tr key={`${r.state_code}-${r.measure_id}-${i}`} style={{ borderBottom:`1px solid ${SF}` }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding:"4px",fontFamily:FM,color:AL }}>{r.measure_id}</td>
                    <td style={{ padding:"4px",color:AL,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{r.measure_name}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",fontWeight:600 }}>{fmt(r.measure_rate)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {qualityData.length > 200 && <div style={{ padding:"4px 14px 8px",fontSize:9,color:AL,fontFamily:FM }}>Showing first 200 of {qualityData.length} records</div>}
        </Card>}
      </>}

      {/* Composite Risk */}
      {tab === "composite" && <>
        {compositeChart.length > 0 ? <Card>
          <CH t="Maternal Health Risk Index" b="Multi-factor score: mortality + access + vulnerability + quality" r={`${compositeData.length} states`} />
          <ChartActions filename="maternal-composite">
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
                    <div style={{ color:AL }}>Composite Score: {fmt(d.score)}</div>
                    <div style={{ color:AL }}>MMR: {fmt(d.mortality)} / 100K</div>
                    <div style={{ color:AL }}>HPSAs: {d.hpsa}</div>
                    <div style={{ color:AL }}>SVI: {fmt(d.svi,2)}</div>
                    <div style={{ color:AL }}>Quality: {fmt(d.quality)}%</div>
                  </div>;
                }} />
                <Bar dataKey="score" name="Risk Score" radius={[3,3,0,0]}>
                  {compositeChart.map((d, i) => (
                    <Cell key={i} fill={topQuartile.has(d.name) ? NEG : i < compositeChart.length * 0.5 ? WARN : POS} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartActions>
          <div style={{ padding:"2px 14px 8px",fontSize:9,color:AL,fontFamily:FM }}>
            Top quartile of risk highlighted in red. Score combines mortality rate, HPSA shortages, social vulnerability, and quality measure performance.
          </div>
        </Card> : <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:11 }}>No composite data available</div></Card>}

        {compositeData.length > 0 && <Card>
          <CH t="Composite Sub-Scores" b={`${compositeData.length} states`} />
          <div style={{ padding:"0 14px 10px",overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${BD}` }}>
                {["State","MMR / 100K","HPSA Count","Avg SVI","Avg Quality","Risk Level"].map(h => (
                  <th key={h} style={{ textAlign:h==="State"||h==="Risk Level"?"left":"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {compositeChart.map(r => (
                  <tr key={r.name} style={{ borderBottom:`1px solid ${SF}`,background:topQuartile.has(r.name)?"rgba(164,38,44,0.03)":"transparent" }}>
                    <td style={{ padding:"4px",fontWeight:600,color:A }}>{STATE_NAMES[r.name] || r.name}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.mortality>mortalityStats.natRate?NEG:POS }}>{fmt(r.mortality)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{r.hpsa}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right",color:r.svi>0.6?NEG:r.svi>0.4?WARN:POS }}>{fmt(r.svi,2)}</td>
                    <td style={{ padding:"4px",fontFamily:FM,textAlign:"right" }}>{fmt(r.quality)}%</td>
                    <td style={{ padding:"4px",fontSize:9,fontWeight:600,color:topQuartile.has(r.name)?NEG:POS }}>{topQuartile.has(r.name)?"HIGH RISK":"Lower Risk"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>}
      </>}

      {/* Ask Intelligence */}
      <div style={{ marginTop:20,textAlign:"center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing Maternal Health Deserts -- ${tab} tab. ${mortalityData.length} mortality, ${accessData.length} access, ${qualityData.length} quality, ${compositeData.length} composite records.` })}
          style={{ padding:"8px 20px",borderRadius:8,fontSize:11,fontWeight:600,fontFamily:FM,border:`1px solid ${cB}`,background:WH,color:cB,cursor:"pointer" }}>
          Ask Intelligence about this
        </button>
      </div>

      {/* Sources */}
      <div style={{ marginTop:16,textAlign:"center",fontSize:9,color:AL,fontFamily:FM }}>
        Sources: CDC/NCHS Vital Statistics . CDC/ATSDR Social Vulnerability Index . HRSA HPSA Designations . Medicaid Core Set (2024)
      </div>
    </div>
  );
}
