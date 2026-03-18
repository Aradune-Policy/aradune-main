/**
 * Rate Browse & Compare
 * Unified tool for browsing Medicaid rates across 54 jurisdictions.
 * Replaces 5 overlapping rate tools with 3 clean views:
 *   1. Dashboard -- state-level rate summary with rankings
 *   2. Code Lookup -- search any procedure code, see rates in every state
 *   3. State Compare -- side-by-side rate comparison for 2-3 states
 *
 * Data: fact_rate_comparison_v2 (483K rows), CY2026 Medicare PFS, published fee schedules.
 */
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";
import { C, FONT, SHADOW, useIsMobile } from "../design";
import { useAradune } from "../context/AraduneContext";
import { getAuthHeaders, API_BASE } from "../lib/api";
import { STATE_NAMES } from "../data/states";

// ── Design tokens ───────────────────────────────────────────────────────
const A = C.ink, AL = C.inkLight, POS = C.pos || C.brand, NEG = C.neg || "#A4262C";
const WARN = C.warn || "#B8860B";
const SF = C.surface, BD = C.border, WH = C.white, cB = C.brand;
const FM = FONT.mono, FB = FONT.body, SH = SHADOW;

// ── Types ───────────────────────────────────────────────────────────────
type View = "dashboard" | "lookup" | "compare";
type DashSortKey = "state" | "codes" | "median" | "below60" | "below80" | "source";
type LookupSortKey = "state" | "medicaid" | "pct";
type CompareSortKey = "code" | "medicare" | "s1" | "s2" | "s3";

interface StateSummary {
  state_code: string;
  state_name: string;
  code_count: number;
  median_pct_medicare: number;
  codes_below_60: number;
  codes_below_80: number;
  rate_source: string;
}

interface SearchResult {
  procedure_code: string;
  description: string;
  category: string;
  is_em_code: boolean;
  medicare_rate_nonfac: number | null;
}

interface RateRow {
  state_code: string;
  medicaid_rate: number;
  medicare_rate: number;
  pct_of_medicare: number;
  rate_source: string;
}

interface CompareRow {
  procedure_code: string;
  description: string;
  medicare_rate: number;
  states: Record<string, { rate: number; pct: number }>;
}

// ── Categories for compare filter ───────────────────────────────────────
const CATEGORIES = [
  "All", "E/M", "Surgery", "Radiology", "Pathology", "Medicine",
  "Anesthesia", "MH/SUD", "OB/GYN", "Primary Care",
];

// ── Color helpers ───────────────────────────────────────────────────────
function pctColor(pct: number): string {
  if (pct < 60) return NEG;
  if (pct < 80) return WARN;
  return POS;
}

function barFill(pct: number): string {
  if (pct < 60) return NEG;
  if (pct < 80) return "#D97706";
  if (pct <= 120) return POS;
  return "#2563EB";
}

function sourceColor(src: string): string {
  const s = (src || "").toLowerCase();
  if (s.includes("published") || s.includes("fee_schedule")) return POS;
  if (s.includes("cf_x_rvu") || s.includes("cf")) return "#D97706";
  return "#94A3B8";
}

// ── Shared primitives ───────────────────────────────────────────────────
const Card = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{
    background: WH, border: `1px solid ${BD}`, borderRadius: 10,
    boxShadow: SH, overflow: "hidden", marginBottom: 16, ...style,
  }}>{children}</div>
);

const CardBody = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{ padding: "16px 20px", ...style }}>{children}</div>
);

const Met = ({ label, value, color }: { label: string; value: string; color?: string }) => (
  <div style={{ textAlign: "center", minWidth: 90, padding: "8px 12px" }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
  </div>
);

const SortArrow = ({ active, asc }: { active: boolean; asc: boolean }) =>
  active ? <span style={{ marginLeft: 3 }}>{asc ? "\u25B4" : "\u25BE"}</span> : null;

const Th = ({ children, onClick, active, asc, align, style }: {
  children: React.ReactNode; onClick?: () => void; active?: boolean; asc?: boolean;
  align?: string; style?: React.CSSProperties;
}) => (
  <th onClick={onClick} style={{
    padding: "8px 6px", textAlign: (align || "left") as any, color: AL, fontWeight: 600,
    fontSize: 11, cursor: onClick ? "pointer" : "default", userSelect: "none",
    whiteSpace: "nowrap", ...style,
  }}>
    {children}
    {active !== undefined && <SortArrow active={!!active} asc={!!asc} />}
  </th>
);

// ── CSV export ──────────────────────────────────────────────────────────
function downloadCSV(rows: Record<string, unknown>[], filename: string) {
  if (!rows.length) return;
  const cols = Object.keys(rows[0]);
  const csv = [
    cols.join(","),
    ...rows.map(r => cols.map(c => JSON.stringify(r[c] ?? "")).join(",")),
  ].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

// ── Btn helper ──────────────────────────────────────────────────────────
const Btn = ({ children, onClick, primary, small, style }: {
  children: React.ReactNode; onClick: () => void; primary?: boolean;
  small?: boolean; style?: React.CSSProperties;
}) => (
  <button onClick={onClick} style={{
    padding: small ? "5px 12px" : "8px 16px", borderRadius: 8,
    border: primary ? "none" : `1px solid ${BD}`, fontFamily: FB,
    background: primary ? cB : WH, color: primary ? WH : AL,
    fontSize: small ? 11 : 12, cursor: "pointer", fontWeight: 600,
    ...style,
  }}>{children}</button>
);

// ═════════════════════════════════════════════════════════════════════════
export default function RateBrowse() {
  const { openIntelligence } = useAradune();
  const mobile = useIsMobile();

  // ── View state ──────────────────────────────────────────────────────
  const [view, setView] = useState<View>("dashboard");

  // ── Dashboard state ─────────────────────────────────────────────────
  const [stateSummary, setStateSummary] = useState<StateSummary[]>([]);
  const [dashLoading, setDashLoading] = useState(false);
  const [dashSort, setDashSort] = useState<DashSortKey>("median");
  const [dashAsc, setDashAsc] = useState(true);

  // ── Lookup state ────────────────────────────────────────────────────
  const [lookupQuery, setLookupQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const [selectedCode, setSelectedCode] = useState<SearchResult | null>(null);
  const [codeRates, setCodeRates] = useState<RateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);
  const [lookupSort, setLookupSort] = useState<LookupSortKey>("pct");
  const [lookupAsc, setLookupAsc] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── Compare state ───────────────────────────────────────────────────
  const [cmpStates, setCmpStates] = useState<(string | "")[]>(["", "", ""]);
  const [cmpCategory, setCmpCategory] = useState("All");
  const [compareRows, setCompareRows] = useState<CompareRow[]>([]);
  const [cmpLoading, setCmpLoading] = useState(false);
  const [cmpSort, setCmpSort] = useState<CompareSortKey>("code");
  const [cmpAsc, setCmpAsc] = useState(true);

  // ── Helper: navigate to lookup with a pre-selected state ───────────
  const goToLookupForState = useCallback((stateCode: string) => {
    setView("lookup");
    // Pre-populate compare or lookup can be extended later
  }, []);

  // ═══════════════════════════════════════════════════════════════════
  // DASHBOARD DATA
  // ═══════════════════════════════════════════════════════════════════
  useEffect(() => {
    if (view !== "dashboard" || stateSummary.length > 0) return;
    setDashLoading(true);
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/rates/state-summary`);
        if (res.ok) {
          const data = await res.json();
          setStateSummary(data.rows || data.states || data || []);
        }
      } catch { /* ignore */ }
      setDashLoading(false);
    })();
  }, [view, stateSummary.length]);

  // ── Dashboard summary stats ────────────────────────────────────────
  const dashStats = useMemo(() => {
    if (!stateSummary.length) return null;
    const medians = stateSummary.map(s => s.median_pct_medicare).filter(Boolean).sort((a, b) => a - b);
    const totalCodes = stateSummary.reduce((s, r) => s + (r.code_count || 0), 0);
    const published = stateSummary.filter(s => (s.rate_source || "").toLowerCase().includes("published") ||
      (s.rate_source || "").toLowerCase().includes("fee_schedule")).length;
    return {
      jurisdictions: stateSummary.length,
      medianOfMedians: medians.length ? medians[Math.floor(medians.length / 2)] : 0,
      pctPublished: stateSummary.length ? Math.round((published / stateSummary.length) * 100) : 0,
      totalCodes,
    };
  }, [stateSummary]);

  // ── Dashboard chart data (always sorted by median desc) ───────────
  const dashChartData = useMemo(() =>
    [...stateSummary]
      .filter(s => s.median_pct_medicare > 0)
      .sort((a, b) => b.median_pct_medicare - a.median_pct_medicare)
      .map(s => ({ name: s.state_code, pct: Math.round(s.median_pct_medicare) })),
    [stateSummary],
  );

  // ── Dashboard sorted table ─────────────────────────────────────────
  const dashSorted = useMemo(() => {
    const arr = [...stateSummary];
    arr.sort((a, b) => {
      let cmp = 0;
      switch (dashSort) {
        case "state": cmp = (a.state_name || a.state_code).localeCompare(b.state_name || b.state_code); break;
        case "codes": cmp = (a.code_count || 0) - (b.code_count || 0); break;
        case "median": cmp = (a.median_pct_medicare || 0) - (b.median_pct_medicare || 0); break;
        case "below60": cmp = (a.codes_below_60 || 0) - (b.codes_below_60 || 0); break;
        case "below80": cmp = (a.codes_below_80 || 0) - (b.codes_below_80 || 0); break;
        case "source": cmp = (a.rate_source || "").localeCompare(b.rate_source || ""); break;
      }
      return dashAsc ? cmp : -cmp;
    });
    return arr;
  }, [stateSummary, dashSort, dashAsc]);

  const handleDashSort = useCallback((key: DashSortKey) => {
    if (dashSort === key) setDashAsc(!dashAsc);
    else { setDashSort(key); setDashAsc(key === "state"); }
  }, [dashSort, dashAsc]);

  // ═══════════════════════════════════════════════════════════════════
  // CODE LOOKUP DATA
  // ═══════════════════════════════════════════════════════════════════
  useEffect(() => {
    if (lookupQuery.length < 2) {
      setSearchResults([]);
      setShowDropdown(false);
      return;
    }
    setSearchLoading(true);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`${API_BASE}/api/rate-explorer/search?q=${encodeURIComponent(lookupQuery)}`);
        if (res.ok) {
          const data = await res.json();
          setSearchResults(data.results || []);
          setShowDropdown(true);
        }
      } catch { /* ignore */ }
      setSearchLoading(false);
    }, 250);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [lookupQuery]);

  useEffect(() => {
    if (!selectedCode) { setCodeRates([]); return; }
    setRatesLoading(true);
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/rate-explorer?code=${encodeURIComponent(selectedCode.procedure_code)}`);
        if (res.ok) {
          const data = await res.json();
          setCodeRates(data.rows || []);
        }
      } catch { /* ignore */ }
      setRatesLoading(false);
    })();
  }, [selectedCode]);

  const handleSelectCode = useCallback((r: SearchResult) => {
    setSelectedCode(r);
    setLookupQuery(r.procedure_code);
    setShowDropdown(false);
    setLookupSort("pct");
    setLookupAsc(false);
  }, []);

  const lookupSorted = useMemo(() => {
    const arr = [...codeRates];
    arr.sort((a, b) => {
      let cmp = 0;
      if (lookupSort === "state") cmp = (STATE_NAMES[a.state_code] || a.state_code).localeCompare(STATE_NAMES[b.state_code] || b.state_code);
      else if (lookupSort === "medicaid") cmp = a.medicaid_rate - b.medicaid_rate;
      else cmp = a.pct_of_medicare - b.pct_of_medicare;
      return lookupAsc ? cmp : -cmp;
    });
    return arr;
  }, [codeRates, lookupSort, lookupAsc]);

  const lookupChart = useMemo(() =>
    [...codeRates].sort((a, b) => b.pct_of_medicare - a.pct_of_medicare)
      .map(r => ({ ...r, name: r.state_code, pct: Math.round(r.pct_of_medicare) })),
    [codeRates],
  );

  const lookupStats = useMemo(() => {
    if (!codeRates.length) return null;
    const pcts = codeRates.map(r => r.pct_of_medicare).sort((a, b) => a - b);
    const meds = codeRates.map(r => r.medicaid_rate).sort((a, b) => a - b);
    return {
      states: codeRates.length,
      medianPct: pcts[Math.floor(pcts.length / 2)],
      minRate: meds[0],
      maxRate: meds[meds.length - 1],
      below60: pcts.filter(p => p < 60).length,
    };
  }, [codeRates]);

  const handleLookupSort = useCallback((key: LookupSortKey) => {
    if (lookupSort === key) setLookupAsc(!lookupAsc);
    else { setLookupSort(key); setLookupAsc(key === "state"); }
  }, [lookupSort, lookupAsc]);

  // ═══════════════════════════════════════════════════════════════════
  // STATE COMPARE DATA
  // ═══════════════════════════════════════════════════════════════════
  const activeCompareStates = useMemo(() => cmpStates.filter(Boolean) as string[], [cmpStates]);

  const runCompare = useCallback(async () => {
    if (activeCompareStates.length < 2) return;
    setCmpLoading(true);
    try {
      const params = new URLSearchParams();
      params.set("states", activeCompareStates.join(","));
      if (cmpCategory !== "All") params.set("category", cmpCategory);
      const res = await fetch(`${API_BASE}/api/rates/compare-states?${params.toString()}`);
      if (res.ok) {
        const data = await res.json();
        setCompareRows(data.rows || []);
      }
    } catch { /* ignore */ }
    setCmpLoading(false);
  }, [activeCompareStates, cmpCategory]);

  useEffect(() => {
    if (view === "compare" && activeCompareStates.length >= 2) {
      runCompare();
    }
  }, [view, activeCompareStates.join(","), cmpCategory]);

  const cmpSorted = useMemo(() => {
    const arr = [...compareRows];
    arr.sort((a, b) => {
      let cmp = 0;
      switch (cmpSort) {
        case "code": cmp = a.procedure_code.localeCompare(b.procedure_code); break;
        case "medicare": cmp = (a.medicare_rate || 0) - (b.medicare_rate || 0); break;
        case "s1": {
          const k = activeCompareStates[0];
          cmp = (a.states[k]?.pct || 0) - (b.states[k]?.pct || 0);
          break;
        }
        case "s2": {
          const k = activeCompareStates[1];
          cmp = (a.states[k]?.pct || 0) - (b.states[k]?.pct || 0);
          break;
        }
        case "s3": {
          const k = activeCompareStates[2];
          if (k) cmp = (a.states[k]?.pct || 0) - (b.states[k]?.pct || 0);
          break;
        }
      }
      return cmpAsc ? cmp : -cmp;
    });
    return arr;
  }, [compareRows, cmpSort, cmpAsc, activeCompareStates]);

  const cmpAvgs = useMemo(() => {
    const out: Record<string, number> = {};
    for (const sc of activeCompareStates) {
      const vals = compareRows.map(r => r.states[sc]?.pct).filter((v): v is number => v != null && v > 0);
      out[sc] = vals.length ? vals.reduce((s, v) => s + v, 0) / vals.length : 0;
    }
    return out;
  }, [compareRows, activeCompareStates]);

  const handleCmpSort = useCallback((key: CompareSortKey) => {
    if (cmpSort === key) setCmpAsc(!cmpAsc);
    else { setCmpSort(key); setCmpAsc(key === "code"); }
  }, [cmpSort, cmpAsc]);

  // ── Intelligence context builder ──────────────────────────────────
  const buildIntelContext = useCallback(() => {
    let summary = `User is viewing Rate Browse. View: ${view}.`;
    if (view === "dashboard" && dashStats) {
      summary += ` ${dashStats.jurisdictions} jurisdictions, median ${dashStats.medianOfMedians.toFixed(0)}% of Medicare.`;
    }
    if (view === "lookup" && selectedCode) {
      summary += ` Code: ${selectedCode.procedure_code} (${selectedCode.description}). ${codeRates.length} states loaded.`;
    }
    if (view === "compare" && activeCompareStates.length >= 2) {
      summary += ` Comparing: ${activeCompareStates.join(", ")}. Category: ${cmpCategory}. ${compareRows.length} codes.`;
    }
    return {
      summary,
      state: (view === "lookup" ? undefined : activeCompareStates[0]) || undefined,
      table: "fact_rate_comparison_v2",
    };
  }, [view, dashStats, selectedCode, codeRates, activeCompareStates, cmpCategory, compareRows]);

  // ── State list for dropdowns ──────────────────────────────────────
  const stateOptions = useMemo(() =>
    Object.entries(STATE_NAMES).sort((a, b) => a[1].localeCompare(b[1])),
    [],
  );

  // ═══════════════════════════════════════════════════════════════════
  // RENDER
  // ═══════════════════════════════════════════════════════════════════
  const chartHeight = (data: { name: string }[]) => Math.max(300, data.length * 22 + 40);

  return (
    <div style={{ maxWidth: 1060, margin: "0 auto", padding: mobile ? "16px 12px" : "20px 16px", fontFamily: FB }}>

      {/* ── Header ─────────────────────────────────────────────────── */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20, flexWrap: "wrap", gap: 12 }}>
        <div>
          <h2 style={{ fontSize: 22, fontWeight: 800, color: A, margin: "0 0 4px", fontFamily: FB }}>
            <span style={{ color: cB, marginRight: 8 }}>&#9671;</span>
            Rate Browse &amp; Compare
          </h2>
          <p style={{ fontSize: 13, color: AL, margin: 0, maxWidth: 600, lineHeight: 1.5 }}>
            Medicaid rates across 54 jurisdictions. Published fee schedules, CF x RVU, and T-MSIS claims.
          </p>
        </div>
        <Btn primary onClick={() => openIntelligence(buildIntelContext())}>Ask Aradune</Btn>
      </div>

      {/* ── View toggle (pill) ─────────────────────────────────────── */}
      <div style={{
        display: "inline-flex", borderRadius: 10, border: `1px solid ${BD}`,
        overflow: "hidden", marginBottom: 20, background: SF,
      }}>
        {([
          { key: "dashboard" as View, label: "Dashboard", icon: "\u25C9" },
          { key: "lookup" as View, label: "Code Lookup", icon: "\u25C7" },
          { key: "compare" as View, label: "State Compare", icon: "\u25B3" },
        ]).map(v => (
          <button key={v.key} onClick={() => setView(v.key)} style={{
            padding: mobile ? "8px 14px" : "9px 22px", border: "none", cursor: "pointer",
            fontSize: 12, fontWeight: 600, fontFamily: FB,
            background: view === v.key ? WH : "transparent",
            color: view === v.key ? A : AL,
            boxShadow: view === v.key ? "0 1px 3px rgba(0,0,0,.08)" : "none",
            transition: "all 0.15s ease",
          }}>
            <span style={{ marginRight: 5 }}>{v.icon}</span>{v.label}
          </button>
        ))}
      </div>

      {/* ═══════════════════════════════════════════════════════════ */}
      {/* VIEW 1: DASHBOARD                                          */}
      {/* ═══════════════════════════════════════════════════════════ */}
      {view === "dashboard" && (
        <>
          {dashLoading && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading state summaries...</p></CardBody></Card>
          )}

          {!dashLoading && dashStats && (
            <>
              {/* Summary cards */}
              <div style={{ display: "grid", gridTemplateColumns: mobile ? "1fr 1fr" : "repeat(4, 1fr)", gap: 12, marginBottom: 16 }}>
                <Card><Met label="Jurisdictions" value={`${dashStats.jurisdictions}`} color={cB} /></Card>
                <Card><Met label="Median % of Medicare" value={`${dashStats.medianOfMedians.toFixed(0)}%`} color={pctColor(dashStats.medianOfMedians)} /></Card>
                <Card><Met label="Published Rates" value={`${dashStats.pctPublished}%`} color={POS} /></Card>
                <Card><Met label="Total Codes" value={dashStats.totalCodes.toLocaleString()} /></Card>
              </div>

              {/* Bar chart: states ranked by median % MCR */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12 }}>
                    <div>
                      <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A }}>States Ranked by Median % of Medicare</h3>
                      <p style={{ margin: "2px 0 0", fontSize: 11, color: AL }}>All jurisdictions with rate data</p>
                    </div>
                  </div>
                  <div style={{ overflowY: "auto", maxHeight: mobile ? 400 : 700 }}>
                    <ResponsiveContainer width="100%" height={chartHeight(dashChartData)}>
                      <BarChart data={dashChartData} layout="vertical" margin={{ top: 4, right: 40, left: 4, bottom: 4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" domain={[0, (dm: number) => Math.max(dm, 150)]}
                          tick={{ fontSize: 10, fontFamily: FM, fill: AL }}
                          tickFormatter={(v: number) => `${v}%`} />
                        <YAxis type="category" dataKey="name" width={32}
                          tick={{ fontSize: 10, fontFamily: FM, fill: A, fontWeight: 600 }} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.[0]) return null;
                          const d = payload[0].payload as { name: string; pct: number };
                          return (
                            <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 8, padding: "10px 14px", fontSize: 12, fontFamily: FM, boxShadow: SH }}>
                              <div style={{ fontWeight: 700, marginBottom: 2 }}>{d.name} {STATE_NAMES[d.name] || ""}</div>
                              <div style={{ color: barFill(d.pct), fontWeight: 700 }}>{d.pct}% of Medicare</div>
                            </div>
                          );
                        }} />
                        <ReferenceLine x={60} stroke={NEG} strokeDasharray="4 3" strokeWidth={1} opacity={0.5} />
                        <ReferenceLine x={80} stroke={WARN} strokeDasharray="4 3" strokeWidth={1} opacity={0.5} />
                        <ReferenceLine x={100} stroke={A} strokeDasharray="4 3" strokeWidth={1.5} opacity={0.5} />
                        <Bar dataKey="pct" radius={[0, 3, 3, 0]} barSize={14}>
                          {dashChartData.map((d, i) => (
                            <Cell key={i} fill={barFill(d.pct)} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div style={{ display: "flex", gap: 14, marginTop: 10, fontSize: 10, color: AL, flexWrap: "wrap" }}>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: NEG, marginRight: 3 }} />Below 60%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#D97706", marginRight: 3 }} />60 to 80%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: POS, marginRight: 3 }} />80 to 120%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#2563EB", marginRight: 3 }} />Above 120%</span>
                    <span style={{ marginLeft: "auto" }}>Dashed lines at 60%, 80%, and 100% (parity)</span>
                  </div>
                </CardBody>
              </Card>

              {/* State summary table */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12 }}>
                    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A }}>{dashSorted.length} Jurisdictions</h3>
                    <Btn small onClick={() => downloadCSV(dashSorted.map(r => ({
                      state_code: r.state_code,
                      state_name: r.state_name || STATE_NAMES[r.state_code] || r.state_code,
                      codes: r.code_count,
                      median_pct_medicare: r.median_pct_medicare?.toFixed(1),
                      codes_below_60: r.codes_below_60,
                      codes_below_80: r.codes_below_80,
                      rate_source: r.rate_source,
                    })), "rate_browse_dashboard.csv")}>Export CSV</Btn>
                  </div>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          <Th onClick={() => handleDashSort("state")} active={dashSort === "state"} asc={dashAsc}>State</Th>
                          <Th>Name</Th>
                          <Th onClick={() => handleDashSort("codes")} active={dashSort === "codes"} asc={dashAsc} align="right">Codes</Th>
                          <Th onClick={() => handleDashSort("median")} active={dashSort === "median"} asc={dashAsc} align="right">Median % MCR</Th>
                          <Th onClick={() => handleDashSort("below60")} active={dashSort === "below60"} asc={dashAsc} align="right">Below 60%</Th>
                          <Th onClick={() => handleDashSort("below80")} active={dashSort === "below80"} asc={dashAsc} align="right">Below 80%</Th>
                          <Th onClick={() => handleDashSort("source")} active={dashSort === "source"} asc={dashAsc}>Rate Source</Th>
                        </tr>
                      </thead>
                      <tbody>
                        {dashSorted.map(r => (
                          <tr key={r.state_code}
                            onClick={() => {
                              setCmpStates([r.state_code, "", ""]);
                              setView("compare");
                            }}
                            style={{ borderBottom: `1px solid ${BD}`, cursor: "pointer" }}
                            onMouseEnter={e => (e.currentTarget.style.background = SF)}
                            onMouseLeave={e => (e.currentTarget.style.background = WH)}
                          >
                            <td style={{ padding: "8px 6px", fontWeight: 700, color: cB }}>{r.state_code}</td>
                            <td style={{ padding: "8px 6px", color: A }}>{r.state_name || STATE_NAMES[r.state_code] || ""}</td>
                            <td style={{ padding: "8px 6px", textAlign: "right" }}>{(r.code_count || 0).toLocaleString()}</td>
                            <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 700, color: pctColor(r.median_pct_medicare || 0) }}>
                              {r.median_pct_medicare ? `${r.median_pct_medicare.toFixed(1)}%` : "--"}
                            </td>
                            <td style={{ padding: "8px 6px", textAlign: "right", color: (r.codes_below_60 || 0) > 0 ? NEG : AL }}>
                              {r.codes_below_60 ?? "--"}
                            </td>
                            <td style={{ padding: "8px 6px", textAlign: "right", color: (r.codes_below_80 || 0) > 0 ? WARN : AL }}>
                              {r.codes_below_80 ?? "--"}
                            </td>
                            <td style={{ padding: "8px 6px", fontSize: 10, color: AL }}>
                              <span style={{
                                display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                                background: sourceColor(r.rate_source || ""), marginRight: 4, verticalAlign: "middle",
                              }} />
                              {r.rate_source || "--"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardBody>
              </Card>
            </>
          )}

          {!dashLoading && !dashStats && stateSummary.length === 0 && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>No state summary data available. Ensure /api/rates/state-summary is configured.</p></CardBody></Card>
          )}
        </>
      )}

      {/* ═══════════════════════════════════════════════════════════ */}
      {/* VIEW 2: CODE LOOKUP                                        */}
      {/* ═══════════════════════════════════════════════════════════ */}
      {view === "lookup" && (
        <>
          {/* Search input */}
          <Card>
            <CardBody>
              <div style={{ position: "relative" }}>
                <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                  <input
                    value={lookupQuery}
                    onChange={e => {
                      setLookupQuery(e.target.value);
                      if (e.target.value.length < 2) { setSelectedCode(null); setShowDropdown(false); }
                    }}
                    placeholder="Search by code or description (e.g. 99213, office visit)..."
                    style={{
                      flex: 1, minWidth: 260, padding: "10px 14px", borderRadius: 8,
                      border: `1px solid ${BD}`, fontSize: 14, fontFamily: FM, outline: "none", color: A,
                    }}
                    onFocus={() => { if (searchResults.length > 0 && !selectedCode) setShowDropdown(true); }}
                    onKeyDown={e => { if (e.key === "Enter" && searchResults.length > 0) handleSelectCode(searchResults[0]); }}
                  />
                  {searchLoading && <span style={{ fontSize: 11, color: AL }}>Searching...</span>}
                </div>

                {/* Search dropdown */}
                {showDropdown && searchResults.length > 0 && (
                  <div style={{
                    position: "absolute", left: 0, right: 0, top: "100%", zIndex: 10,
                    marginTop: 4, maxHeight: 280, overflowY: "auto",
                    border: `1px solid ${BD}`, borderRadius: 8, background: WH, boxShadow: SH,
                  }}>
                    {searchResults.map(r => (
                      <div key={r.procedure_code}
                        onClick={() => handleSelectCode(r)}
                        style={{
                          padding: "8px 12px", cursor: "pointer", borderBottom: `1px solid ${SF}`,
                          display: "flex", gap: 12, alignItems: "center",
                        }}
                        onMouseEnter={e => (e.currentTarget.style.background = SF)}
                        onMouseLeave={e => (e.currentTarget.style.background = WH)}
                      >
                        <span style={{ fontFamily: FM, fontWeight: 700, color: cB, minWidth: 60 }}>{r.procedure_code}</span>
                        <span style={{ flex: 1, fontSize: 12, color: AL, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {r.description}
                        </span>
                        {r.category && <span style={{ fontSize: 10, color: AL, flexShrink: 0 }}>{r.category}</span>}
                        {r.medicare_rate_nonfac != null && (
                          <span style={{ fontSize: 10, color: AL, flexShrink: 0, fontFamily: FM }}>MCR ${r.medicare_rate_nonfac.toFixed(2)}</span>
                        )}
                      </div>
                    ))}
                  </div>
                )}
                {showDropdown && lookupQuery.length >= 2 && searchResults.length === 0 && !searchLoading && (
                  <div style={{ marginTop: 8, fontSize: 12, color: AL }}>No matching codes found.</div>
                )}
              </div>
            </CardBody>
          </Card>

          {/* Loading */}
          {ratesLoading && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading rate data...</p></CardBody></Card>
          )}

          {/* Results */}
          {selectedCode && !ratesLoading && codeRates.length > 0 && lookupStats && (
            <>
              {/* Code info + stats */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12, flexWrap: "wrap", gap: 8 }}>
                    <div>
                      <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: A, fontFamily: FM }}>{selectedCode.procedure_code}</h3>
                      <p style={{ margin: "2px 0 0", fontSize: 12, color: AL }}>{selectedCode.description}</p>
                      <div style={{ display: "flex", gap: 12, marginTop: 4, fontSize: 11, color: AL }}>
                        {selectedCode.category && <span>Category: {selectedCode.category}</span>}
                        {selectedCode.medicare_rate_nonfac != null && (
                          <span>Medicare non-fac: <strong style={{ color: A, fontFamily: FM }}>${selectedCode.medicare_rate_nonfac.toFixed(2)}</strong></span>
                        )}
                      </div>
                    </div>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-around", flexWrap: "wrap", gap: 12 }}>
                    <Met label="Jurisdictions" value={`${lookupStats.states}`} />
                    <Met label="Lowest Rate" value={`$${lookupStats.minRate.toFixed(2)}`} color={NEG} />
                    <Met label="Highest Rate" value={`$${lookupStats.maxRate.toFixed(2)}`} color={POS} />
                    <Met label="Median % MCR" value={`${lookupStats.medianPct.toFixed(0)}%`} color={pctColor(lookupStats.medianPct)} />
                    {lookupStats.below60 > 0 && (
                      <Met label="Below 60% MCR" value={`${lookupStats.below60}`} color={NEG} />
                    )}
                  </div>
                </CardBody>
              </Card>

              {/* Horizontal bar chart */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12 }}>
                    <div>
                      <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A }}>Rates by Jurisdiction</h3>
                      <p style={{ margin: "2px 0 0", fontSize: 11, color: AL }}>Ranked by % of Medicare</p>
                    </div>
                  </div>
                  <div style={{ overflowY: "auto", maxHeight: mobile ? 400 : 600 }}>
                    <ResponsiveContainer width="100%" height={chartHeight(lookupChart)}>
                      <BarChart data={lookupChart} layout="vertical" margin={{ top: 4, right: 40, left: 4, bottom: 4 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
                        <XAxis type="number" domain={[0, (dm: number) => Math.max(dm, 150)]}
                          tick={{ fontSize: 10, fontFamily: FM, fill: AL }}
                          tickFormatter={(v: number) => `${v}%`} />
                        <YAxis type="category" dataKey="name" width={32}
                          tick={{ fontSize: 10, fontFamily: FM, fill: A, fontWeight: 600 }} />
                        <Tooltip content={({ active, payload }) => {
                          if (!active || !payload?.[0]) return null;
                          const d = payload[0].payload as RateRow & { name: string; pct: number };
                          return (
                            <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 8, padding: "10px 14px", fontSize: 12, fontFamily: FM, boxShadow: SH }}>
                              <div style={{ fontWeight: 700, marginBottom: 2 }}>{d.name} {STATE_NAMES[d.state_code] || ""}</div>
                              <div>Medicaid: ${d.medicaid_rate.toFixed(2)}</div>
                              <div>Medicare: ${d.medicare_rate.toFixed(2)}</div>
                              <div style={{ fontWeight: 700, color: barFill(d.pct_of_medicare) }}>{d.pct_of_medicare.toFixed(1)}% of Medicare</div>
                              {d.rate_source && <div style={{ fontSize: 10, color: AL, marginTop: 4 }}>Source: {d.rate_source}</div>}
                            </div>
                          );
                        }} />
                        <ReferenceLine x={60} stroke={NEG} strokeDasharray="4 3" strokeWidth={1} opacity={0.4} />
                        <ReferenceLine x={80} stroke={WARN} strokeDasharray="4 3" strokeWidth={1} opacity={0.4} />
                        <ReferenceLine x={100} stroke={A} strokeDasharray="4 3" strokeWidth={1.5} opacity={0.5} />
                        <Bar dataKey="pct" radius={[0, 3, 3, 0]} barSize={16}>
                          {lookupChart.map((d, i) => (
                            <Cell key={i} fill={barFill(d.pct_of_medicare)} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div style={{ display: "flex", gap: 14, marginTop: 10, fontSize: 10, color: AL, flexWrap: "wrap" }}>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: NEG, marginRight: 3 }} />Below 60%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#D97706", marginRight: 3 }} />60 to 80%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: POS, marginRight: 3 }} />80 to 120%</span>
                    <span><span style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "#2563EB", marginRight: 3 }} />Above 120%</span>
                  </div>
                </CardBody>
              </Card>

              {/* Rate table */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12, flexWrap: "wrap", gap: 8 }}>
                    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A }}>{codeRates.length} Jurisdictions</h3>
                    <Btn small onClick={() => downloadCSV(lookupSorted.map(r => ({
                      state_code: r.state_code,
                      state_name: STATE_NAMES[r.state_code] || r.state_code,
                      medicaid_rate: r.medicaid_rate.toFixed(2),
                      pct_of_medicare: r.pct_of_medicare.toFixed(1),
                      rate_source: r.rate_source,
                    })), `rate_browse_${selectedCode.procedure_code}.csv`)}>Export CSV</Btn>
                  </div>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          <Th onClick={() => handleLookupSort("state")} active={lookupSort === "state"} asc={lookupAsc}>State</Th>
                          <Th onClick={() => handleLookupSort("medicaid")} active={lookupSort === "medicaid"} asc={lookupAsc} align="right">Medicaid Rate</Th>
                          <Th onClick={() => handleLookupSort("pct")} active={lookupSort === "pct"} asc={lookupAsc} align="right">% of Medicare</Th>
                          <Th align="left">Rate Source</Th>
                        </tr>
                      </thead>
                      <tbody>
                        {lookupSorted.map(r => (
                          <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}` }}>
                            <td style={{ padding: "8px 6px", fontWeight: 600, color: A }}>
                              {r.state_code} <span style={{ fontWeight: 400, color: AL, fontSize: 11 }}>{STATE_NAMES[r.state_code] || ""}</span>
                            </td>
                            <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 600 }}>${r.medicaid_rate.toFixed(2)}</td>
                            <td style={{ padding: "8px 6px", textAlign: "right", fontWeight: 700, color: pctColor(r.pct_of_medicare) }}>
                              {r.pct_of_medicare.toFixed(1)}%
                            </td>
                            <td style={{ padding: "8px 6px", fontSize: 10, color: AL }}>
                              <span style={{
                                display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                                background: sourceColor(r.rate_source || ""), marginRight: 4, verticalAlign: "middle",
                              }} />
                              {r.rate_source || "--"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardBody>
              </Card>

              {/* Ask Aradune about this code */}
              <div style={{ display: "flex", gap: 12, justifyContent: "flex-end", marginBottom: 16 }}>
                <Btn primary onClick={() => openIntelligence({
                  summary: `User wants to understand code ${selectedCode.procedure_code} (${selectedCode.description}). ${codeRates.length} states loaded. Median ${lookupStats.medianPct.toFixed(0)}% of Medicare. ${lookupStats.below60} states below 60%.`,
                  table: "fact_rate_comparison_v2",
                })}>
                  Ask Aradune about {selectedCode.procedure_code}
                </Btn>
              </div>
            </>
          )}

          {/* No results */}
          {selectedCode && !ratesLoading && codeRates.length === 0 && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>
              No rate data found for {selectedCode.procedure_code} in fact_rate_comparison_v2.
            </p></CardBody></Card>
          )}

          {/* Quick-pick codes when nothing selected */}
          {!selectedCode && !ratesLoading && (
            <Card>
              <CardBody>
                <h3 style={{ margin: "0 0 10px", fontSize: 14, fontWeight: 700, color: A }}>Common Codes</h3>
                <p style={{ margin: "0 0 12px", fontSize: 12, color: AL }}>Select a code to see rates across all jurisdictions</p>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {["99213", "99214", "99215", "99203", "99204", "90834", "90837", "97110", "92507", "99385"].map(code => (
                    <button key={code}
                      onClick={() => {
                        setLookupQuery(code);
                        (async () => {
                          try {
                            const res = await fetch(`${API_BASE}/api/rate-explorer/search?q=${encodeURIComponent(code)}`);
                            if (res.ok) {
                              const data = await res.json();
                              const match = (data.results || []).find((r: SearchResult) => r.procedure_code === code);
                              if (match) handleSelectCode(match);
                            }
                          } catch { /* ignore */ }
                        })();
                      }}
                      style={{
                        padding: "8px 16px", borderRadius: 8, border: `1px solid ${BD}`,
                        background: WH, cursor: "pointer", fontFamily: FM, fontSize: 12,
                        fontWeight: 700, color: cB,
                      }}
                      onMouseEnter={e => (e.currentTarget.style.borderColor = cB)}
                      onMouseLeave={e => (e.currentTarget.style.borderColor = BD)}
                    >{code}</button>
                  ))}
                </div>
              </CardBody>
            </Card>
          )}
        </>
      )}

      {/* ═══════════════════════════════════════════════════════════ */}
      {/* VIEW 3: STATE COMPARE                                      */}
      {/* ═══════════════════════════════════════════════════════════ */}
      {view === "compare" && (
        <>
          {/* State selectors + category */}
          <Card>
            <CardBody>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "flex-end" }}>
                {[0, 1, 2].map(i => (
                  <div key={i} style={{ minWidth: 160 }}>
                    <label style={{ display: "block", fontSize: 11, color: AL, marginBottom: 4, fontWeight: 600 }}>
                      State {i + 1}{i === 2 ? " (optional)" : ""}
                    </label>
                    <select
                      value={cmpStates[i]}
                      onChange={e => {
                        const next = [...cmpStates];
                        next[i] = e.target.value;
                        setCmpStates(next);
                      }}
                      style={{
                        width: "100%", padding: "8px 10px", borderRadius: 8,
                        border: `1px solid ${BD}`, fontSize: 13, fontFamily: FM,
                        color: cmpStates[i] ? A : AL, background: WH, outline: "none",
                      }}
                    >
                      <option value="">Select...</option>
                      {stateOptions.map(([code, name]) => (
                        <option key={code} value={code}>{code} - {name}</option>
                      ))}
                    </select>
                  </div>
                ))}
                <div style={{ minWidth: 140 }}>
                  <label style={{ display: "block", fontSize: 11, color: AL, marginBottom: 4, fontWeight: 600 }}>Category</label>
                  <select
                    value={cmpCategory}
                    onChange={e => setCmpCategory(e.target.value)}
                    style={{
                      width: "100%", padding: "8px 10px", borderRadius: 8,
                      border: `1px solid ${BD}`, fontSize: 13, fontFamily: FB,
                      color: A, background: WH, outline: "none",
                    }}
                  >
                    {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <Btn primary onClick={runCompare} style={{ height: 37 }}>Compare</Btn>
              </div>
            </CardBody>
          </Card>

          {cmpLoading && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>Loading comparison data...</p></CardBody></Card>
          )}

          {!cmpLoading && activeCompareStates.length < 2 && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>
              Select at least 2 states to compare rates side by side.
            </p></CardBody></Card>
          )}

          {!cmpLoading && compareRows.length > 0 && (
            <>
              {/* Summary cards for each state */}
              <div style={{ display: "grid", gridTemplateColumns: mobile ? "1fr" : `repeat(${activeCompareStates.length}, 1fr)`, gap: 12, marginBottom: 16 }}>
                {activeCompareStates.map(sc => (
                  <Card key={sc}>
                    <CardBody style={{ textAlign: "center", padding: "14px 16px" }}>
                      <div style={{ fontSize: 13, fontWeight: 700, color: A, marginBottom: 4 }}>
                        {sc} {STATE_NAMES[sc] || ""}
                      </div>
                      <div style={{ fontSize: 24, fontWeight: 700, fontFamily: FM, color: pctColor(cmpAvgs[sc] || 0) }}>
                        {cmpAvgs[sc] ? `${cmpAvgs[sc].toFixed(0)}%` : "--"}
                      </div>
                      <div style={{ fontSize: 11, color: AL }}>Avg % of Medicare</div>
                    </CardBody>
                  </Card>
                ))}
              </div>

              {/* Compare table */}
              <Card>
                <CardBody>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 12, flexWrap: "wrap", gap: 8 }}>
                    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A }}>
                      {compareRows.length} Codes{cmpCategory !== "All" ? ` (${cmpCategory})` : ""}
                    </h3>
                    <div style={{ display: "flex", gap: 8 }}>
                      <Btn small onClick={() => downloadCSV(compareRows.map(r => {
                        const row: Record<string, unknown> = {
                          code: r.procedure_code,
                          description: r.description,
                          medicare_rate: r.medicare_rate?.toFixed(2) || "",
                        };
                        for (const sc of activeCompareStates) {
                          row[`${sc}_rate`] = r.states[sc]?.rate?.toFixed(2) || "";
                          row[`${sc}_pct`] = r.states[sc]?.pct?.toFixed(1) || "";
                        }
                        return row;
                      }), `rate_compare_${activeCompareStates.join("_")}.csv`)}>Export CSV</Btn>
                      <Btn primary onClick={() => openIntelligence({
                        summary: `User is comparing Medicaid rates: ${activeCompareStates.join(" vs ")}. Category: ${cmpCategory}. ${compareRows.length} codes. Avg % MCR: ${activeCompareStates.map(sc => `${sc}=${cmpAvgs[sc]?.toFixed(0) || "?"}%`).join(", ")}.`,
                        state: activeCompareStates[0],
                        table: "fact_rate_comparison_v2",
                      })}>Ask Aradune</Btn>
                    </div>
                  </div>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          <Th onClick={() => handleCmpSort("code")} active={cmpSort === "code"} asc={cmpAsc}>Code</Th>
                          <Th style={{ minWidth: 160 }}>Description</Th>
                          <Th onClick={() => handleCmpSort("medicare")} active={cmpSort === "medicare"} asc={cmpAsc} align="right">Medicare</Th>
                          {activeCompareStates.map((sc, i) => (
                            <Th key={sc}
                              onClick={() => handleCmpSort((`s${i + 1}`) as CompareSortKey)}
                              active={cmpSort === `s${i + 1}`} asc={cmpAsc} align="right"
                            >{sc}</Th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {cmpSorted.map(r => {
                          // Find max and min pct among compared states to detect significant gaps
                          const pcts = activeCompareStates.map(sc => r.states[sc]?.pct || 0).filter(p => p > 0);
                          const maxPct = Math.max(...pcts, 0);
                          const minPct = Math.min(...(pcts.length ? pcts : [0]));
                          const hasGap = pcts.length >= 2 && (maxPct - minPct) > 15;

                          return (
                            <tr key={r.procedure_code} style={{ borderBottom: `1px solid ${BD}` }}>
                              <td style={{ padding: "8px 6px", fontWeight: 700, color: cB }}>{r.procedure_code}</td>
                              <td style={{ padding: "8px 6px", fontSize: 11, color: AL, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {r.description}
                              </td>
                              <td style={{ padding: "8px 6px", textAlign: "right", color: AL }}>
                                {r.medicare_rate ? `$${r.medicare_rate.toFixed(2)}` : "--"}
                              </td>
                              {activeCompareStates.map(sc => {
                                const d = r.states[sc];
                                if (!d) return <td key={sc} style={{ padding: "8px 6px", textAlign: "right", color: AL }}>--</td>;
                                const isLow = hasGap && d.pct === minPct;
                                return (
                                  <td key={sc} style={{
                                    padding: "8px 6px", textAlign: "right",
                                    background: isLow ? "rgba(164,38,44,0.06)" : "transparent",
                                  }}>
                                    <div style={{ fontWeight: 600 }}>${d.rate.toFixed(2)}</div>
                                    <div style={{ fontSize: 10, fontWeight: 700, color: pctColor(d.pct) }}>{d.pct.toFixed(0)}%</div>
                                  </td>
                                );
                              })}
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                  {compareRows.length > 0 && (
                    <div style={{ marginTop: 12, fontSize: 10, color: AL }}>
                      Highlighted cells indicate a state paying more than 15 percentage points below peer(s).
                    </div>
                  )}
                </CardBody>
              </Card>
            </>
          )}

          {!cmpLoading && activeCompareStates.length >= 2 && compareRows.length === 0 && (
            <Card><CardBody><p style={{ color: AL, fontSize: 13, textAlign: "center", padding: 40 }}>
              No comparison data returned. Try a different category or state pair.
            </p></CardBody></Card>
          )}
        </>
      )}

      {/* ── Footer ─────────────────────────────────────────────────── */}
      <div style={{ fontSize: 10, color: AL, marginTop: 8, lineHeight: 1.6 }}>
        <p style={{ margin: 0 }}>
          <strong>Medicaid rates</strong> from fact_rate_comparison_v2 (483K rows, 54 jurisdictions).
          Sources: state-published fee schedules (88%), CF x RVU (11%), T-MSIS paid claims (1.1%).
        </p>
        <p style={{ margin: "4px 0 0" }}>
          <strong>Medicare benchmark</strong> is the CY2026 PFS non-facility rate. % of Medicare = (Medicaid / Medicare) x 100.
        </p>
      </div>
    </div>
  );
}
