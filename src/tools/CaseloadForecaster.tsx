/**
 * Caseload Forecaster — Upload monthly enrollment data and forecast trends.
 * Uses SARIMAX + ETS model competition with intervention detection,
 * economic enrichment, and 80/95% confidence intervals.
 */
import { useState, useMemo, useCallback } from "react";
import {
  ComposedChart, Area, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Legend,
} from "recharts";
import { STATE_NAMES } from "../data/states";
import { API_BASE } from "../lib/api";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Local UI components ─────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{
    background: WH, borderRadius: 10, padding: "20px 24px", marginBottom: 16,
    boxShadow: SH, borderTop: accent ? `3px solid ${accent}` : undefined,
  }}>{children}</div>
);

const CH = ({ title, sub }: { title: string; sub?: string }) => (
  <div style={{ marginBottom: 12 }}>
    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A, letterSpacing: -0.2 }}>{title}</h3>
    {sub && <p style={{ margin: "3px 0 0", fontSize: 11, color: AL }}>{sub}</p>}
  </div>
);

const Met = ({ label, value, color, mono }: { label: string; value: string | number; color?: string; mono?: boolean }) => (
  <div style={{ textAlign: "center" }}>
    <div style={{ fontSize: mono ? 20 : 18, fontWeight: 700, color: color || A, fontFamily: mono ? FM : FB, letterSpacing: -0.5 }}>{value}</div>
    <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>{label}</div>
  </div>
);

const Pill = ({ label, active, onClick, color }: { label: string; active: boolean; onClick: () => void; color?: string }) => (
  <button onClick={onClick} style={{
    padding: "6px 14px", borderRadius: 20, border: `1px solid ${active ? (color || cB) : BD}`,
    background: active ? (color || cB) : WH, color: active ? WH : AL,
    fontSize: 11, fontWeight: 600, fontFamily: FB, cursor: "pointer",
    transition: "all .15s",
  }}>{label}</button>
);

// ── CSV Export ──────────────────────────────────────────────────────────
function downloadCSV(headers: string[], rows: (string | number)[][], filename: string) {
  const csv = [headers.join(","), ...rows.map(r => r.map(c => {
    const s = String(c ?? "");
    return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g, '""')}"` : s;
  }).join(","))].join("\n");
  const a = document.createElement("a");
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// ── Types ────────────────────────────────────────────────────────────────
interface TimePoint { month: string; enrollment: number }
interface ForecastPoint { month: string; point: number; lower_80: number; upper_80: number; lower_95: number; upper_95: number }
interface EventEntry { date: string; type: string; description: string; source: string }

interface CategoryForecast {
  category: string;
  model_used: string;
  model_order: string;
  fit_aic: number | null;
  fit_mape: number | null;
  history_months: number;
  forecast_months: number;
  actuals: TimePoint[];
  forecasts: ForecastPoint[];
  events: EventEntry[];
  intervention_effects: Record<string, number>;
  warnings: string[];
}

interface ForecastResult {
  meta: {
    state_code: string;
    state_name: string;
    forecast_date: string;
    horizon_months: number;
    n_categories: number;
    total_history_months: number;
    has_regional_data: boolean;
    has_delivery_system: boolean;
    economic_covariates_used: string[];
    warnings: string[];
  };
  categories: CategoryForecast[];
  aggregate: {
    actuals: TimePoint[];
    forecasts: ForecastPoint[];
  };
}

// ── Helpers ──────────────────────────────────────────────────────────────
const STATES = Object.keys(STATE_NAMES).sort();
const fmtNum = (n: number) => n >= 1_000_000 ? (n / 1_000_000).toFixed(2) + "M" : n >= 1_000 ? (n / 1_000).toFixed(1) + "K" : n.toFixed(0);
const fmtDollars = (n: number) => n >= 1e9 ? "$" + (n / 1e9).toFixed(2) + "B" : n >= 1e6 ? "$" + (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? "$" + (n / 1e3).toFixed(1) + "K" : "$" + n.toFixed(0);
const fmtPct = (n: number | null) => n === null ? "—" : n.toFixed(2) + "%";
const modelLabel = (m: string) => m === "sarimax" ? "SARIMAX" : m === "ets" ? "ETS" : m === "auto_arima" ? "Auto ARIMA" : "Naive";
const eventIcon = (type: string) => type.includes("phe") ? "●" : type.includes("unwinding") ? "▼" : type.includes("mc_launch") ? "◆" : "○";

// ── Custom tooltip ──────────────────────────────────────────────────────
const ForecastTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, fontFamily: FM, boxShadow: SH }}>
      <div style={{ fontWeight: 600, color: A, marginBottom: 4 }}>{label}</div>
      {payload.map((p: any) => {
        if (p.dataKey === "ci95_band" || p.dataKey === "ci80_band" || p.dataKey === "ci95_base" || p.dataKey === "ci80_base") return null;
        const label2 = p.dataKey === "actual" ? "Actual" : p.dataKey === "forecast" ? "Forecast" : p.name;
        return (
          <div key={p.dataKey} style={{ color: p.color || AL }}>
            {label2}: <strong>{fmtNum(p.value)}</strong>
          </div>
        );
      })}
      {payload.find((p: any) => p.dataKey === "ci80_band") && (
        <div style={{ color: AL, fontSize: 10, marginTop: 2 }}>
          80% CI: {fmtNum(payload.find((p: any) => p.dataKey === "ci80_base")?.value || 0)} – {fmtNum((payload.find((p: any) => p.dataKey === "ci80_base")?.value || 0) + (payload.find((p: any) => p.dataKey === "ci80_band")?.value || 0))}
        </div>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════
// Main component
// ═══════════════════════════════════════════════════════════════════════
export default function CaseloadForecaster() {
  const { openIntelligence, addReportSection } = useAradune();
  // Upload state
  const [state, setState] = useState("FL");
  const [caseloadFile, setCaseloadFile] = useState<File | null>(null);
  const [eventsFile, setEventsFile] = useState<File | null>(null);
  const [horizonMonths, setHorizonMonths] = useState(36);
  const [includeSeasonality, setIncludeSeasonality] = useState(true);
  const [includeEconomic, setIncludeEconomic] = useState(true);

  // Result state
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ForecastResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Expenditure state
  const [paramsFile, setParamsFile] = useState<File | null>(null);
  const [expLoading, setExpLoading] = useState(false);
  const [expResult, setExpResult] = useState<any>(null);
  const [expError, setExpError] = useState<string | null>(null);

  // View state
  const [selectedCategory, setSelectedCategory] = useState("aggregate");
  const [showCI, setShowCI] = useState<"95" | "80" | "none">("80");
  const [activeTab, setActiveTab] = useState<"caseload" | "expenditure" | "scenario">("caseload");

  // Scenario sliders
  const [scenUnemployment, setScenUnemployment] = useState(0);      // pp change
  const [scenEligibility, setScenEligibility] = useState(0);        // % enrollment change
  const [scenRateChange, setScenRateChange] = useState(0);          // % rate adjustment
  const [scenMcShift, setScenMcShift] = useState(0);                // pp FFS→MC shift

  // ── Generate handler ──────────────────────────────────────────────────
  const handleGenerate = useCallback(async () => {
    if (!caseloadFile) return;
    setLoading(true);
    setError(null);
    setResult(null);
    setSelectedCategory("aggregate");

    const form = new FormData();
    form.append("state", state);
    form.append("caseload", caseloadFile);
    if (eventsFile) form.append("events", eventsFile);
    form.append("horizon_months", String(horizonMonths));
    form.append("include_seasonality", String(includeSeasonality));
    form.append("include_economic", String(includeEconomic));

    try {
      const res = await fetch(`${API_BASE}/api/forecast/generate`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(
          typeof err.detail === "string" ? err.detail
            : err.detail?.errors?.[0]?.message || err.errors?.[0]?.message
              || JSON.stringify(err.detail || err)
        );
      }
      const data = await res.json();
      setResult(data);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [state, caseloadFile, eventsFile, horizonMonths, includeSeasonality, includeEconomic]);

  // ── CSV download handler ──────────────────────────────────────────────
  const handleDownloadCSV = useCallback(async () => {
    if (!caseloadFile) return;
    setLoading(true);

    const form = new FormData();
    form.append("state", state);
    form.append("caseload", caseloadFile);
    if (eventsFile) form.append("events", eventsFile);
    form.append("horizon_months", String(horizonMonths));
    form.append("include_seasonality", String(includeSeasonality));
    form.append("include_economic", String(includeEconomic));

    try {
      const res = await fetch(`${API_BASE}/api/forecast/generate/csv`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) throw new Error("CSV generation failed");
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `caseload_forecast_${state}.csv`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [state, caseloadFile, eventsFile, horizonMonths, includeSeasonality, includeEconomic]);

  // ── Expenditure handler ────────────────────────────────────────────────
  const handleExpenditure = useCallback(async () => {
    if (!caseloadFile || !paramsFile) return;
    setExpLoading(true);
    setExpError(null);
    setExpResult(null);

    const form = new FormData();
    form.append("state", state);
    form.append("caseload", caseloadFile);
    form.append("params", paramsFile);
    if (eventsFile) form.append("events", eventsFile);
    form.append("horizon_months", String(horizonMonths));
    form.append("include_seasonality", String(includeSeasonality));
    form.append("include_economic", String(includeEconomic));

    try {
      const res = await fetch(`${API_BASE}/api/forecast/expenditure`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(
          typeof err.detail === "string" ? err.detail
            : err.detail?.errors?.[0]?.message || err.errors?.[0]?.message
              || JSON.stringify(err.detail || err)
        );
      }
      const data = await res.json();
      setResult(data.forecast);
      setExpResult(data.expenditure);
      setActiveTab("expenditure");
    } catch (e: unknown) {
      setExpError(e instanceof Error ? e.message : String(e));
    } finally {
      setExpLoading(false);
    }
  }, [state, caseloadFile, eventsFile, paramsFile, horizonMonths, includeSeasonality, includeEconomic]);

  // ── Build chart data ──────────────────────────────────────────────────
  const chartData = useMemo(() => {
    if (!result) return [];

    const src = selectedCategory === "aggregate"
      ? result.aggregate
      : result.categories.find(c => c.category === selectedCategory);
    if (!src) return [];

    const points: any[] = [];

    // Actuals
    for (const a of src.actuals) {
      points.push({ month: a.month, actual: a.enrollment });
    }

    // Forecasts — use stacked area technique for confidence bands
    for (const f of src.forecasts) {
      points.push({
        month: f.month,
        forecast: f.point,
        ci80_base: f.lower_80,
        ci80_band: f.upper_80 - f.lower_80,
        ci95_base: f.lower_95,
        ci95_band: f.upper_95 - f.lower_95,
      });
    }

    return points;
  }, [result, selectedCategory]);

  // ── Get events for selected category ──────────────────────────────────
  const selectedEvents = useMemo(() => {
    if (!result) return [];
    if (selectedCategory === "aggregate") {
      // Collect all unique events
      const seen = new Set<string>();
      const events: EventEntry[] = [];
      for (const cat of result.categories) {
        for (const e of cat.events) {
          const key = `${e.date}-${e.type}`;
          if (!seen.has(key)) { seen.add(key); events.push(e); }
        }
      }
      return events;
    }
    const cat = result.categories.find(c => c.category === selectedCategory);
    return cat?.events || [];
  }, [result, selectedCategory]);

  // ── Selected category metadata ────────────────────────────────────────
  const selectedCatData = useMemo(() => {
    if (!result || selectedCategory === "aggregate") return null;
    return result.categories.find(c => c.category === selectedCategory) || null;
  }, [result, selectedCategory]);

  // ═══ RENDER ═══════════════════════════════════════════════════════════
  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px 48px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ padding: "28px 0 20px", display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
            Caseload Forecaster
          </h2>
          <p style={{ margin: "4px 0 0", fontSize: 12, color: AL }}>
            Upload monthly enrollment data by category. SARIMAX + ETS model competition with intervention detection and economic enrichment.
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => openIntelligence({ summary: `User is viewing Caseload Forecaster for ${STATE_NAMES[state] || state}` })} style={{
            padding: "8px 14px", borderRadius: 8, border: "none",
            background: cB, color: WH, fontSize: 12, cursor: "pointer", fontFamily: FB, fontWeight: 600,
          }}>Ask Aradune</button>
          {result && <button onClick={() => {
            const r = result as any;
            const summary = [
              `Caseload Forecast: ${STATE_NAMES[state] || state}`,
              r.categories?.[0]?.model_used ? `Model: ${r.categories[0].model_used}` : null,
              r.categories?.[0]?.fit_mape ? `MAPE: ${(r.categories[0].fit_mape * 100).toFixed(1)}%` : null,
              r.aggregate?.forecasts?.length ? `${r.aggregate.forecasts.length}-month forecast` : null,
            ].filter(Boolean).join(". ");
            addReportSection({
              id: crypto.randomUUID(),
              prompt: `Caseload forecast for ${STATE_NAMES[state] || state}`,
              response: summary,
              queries: [],
              createdAt: new Date(),
            });
          }} style={{
            padding: "8px 14px", borderRadius: 8, border: `1px solid ${BD}`,
            background: WH, color: AL, fontSize: 12, cursor: "pointer", fontFamily: FM,
          }}>+ Report</button>}
        </div>
      </div>

      {/* ─── Upload Form ──────────────────────────────────────────────── */}
      <Card accent={cB}>
        <CH title="Upload Caseload Data" sub="Fill in the template with your state's monthly enrollment by category (min 24 months). Optionally add structural events (MC launches, eligibility changes)." />
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginTop: 12 }}>
          {/* State selector */}
          <div>
            <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: A, marginBottom: 4 }}>State</label>
            <select value={state} onChange={e => setState(e.target.value)} style={{
              width: "100%", padding: "7px 10px", borderRadius: 6, border: `1px solid ${BD}`,
              fontSize: 12, fontFamily: FB, color: A, background: WH,
            }}>
              {STATES.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
            </select>
          </div>
          {/* Horizon */}
          <div>
            <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: A, marginBottom: 4 }}>Forecast Horizon</label>
            <select value={horizonMonths} onChange={e => setHorizonMonths(Number(e.target.value))} style={{
              width: "100%", padding: "7px 10px", borderRadius: 6, border: `1px solid ${BD}`,
              fontSize: 12, fontFamily: FB, color: A, background: WH,
            }}>
              {[12, 24, 36, 48, 60].map(m => <option key={m} value={m}>{m} months ({(m / 12).toFixed(1)} years)</option>)}
            </select>
          </div>
          {/* Caseload file */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
              <label style={{ fontSize: 11, fontWeight: 600, color: A }}>Caseload CSV</label>
              <a href={`${API_BASE}/api/forecast/templates/caseload`} style={{ fontSize: 10, color: cB }}>Download template</a>
            </div>
            <input type="file" accept=".csv" onChange={e => setCaseloadFile(e.target.files?.[0] || null)} style={{
              width: "100%", padding: "6px", borderRadius: 6, border: `1px solid ${BD}`,
              fontSize: 11, fontFamily: FB, background: SF,
            }} />
            <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>Required: month, category, enrollment</div>
          </div>
          {/* Events file (optional) */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
              <label style={{ fontSize: 11, fontWeight: 600, color: A }}>Events CSV <span style={{ fontWeight: 400, color: AL }}>(optional)</span></label>
              <a href={`${API_BASE}/api/forecast/templates/events`} style={{ fontSize: 10, color: cB }}>Download template</a>
            </div>
            <input type="file" accept=".csv" onChange={e => setEventsFile(e.target.files?.[0] || null)} style={{
              width: "100%", padding: "6px", borderRadius: 6, border: `1px solid ${BD}`,
              fontSize: 11, fontFamily: FB, background: SF,
            }} />
            <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>Structural changes: MC launches, eligibility changes, COVID PHE</div>
          </div>
        </div>
        {/* Expenditure params (optional — for combined pipeline) */}
        <div style={{ marginTop: 12 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
            <label style={{ fontSize: 11, fontWeight: 600, color: A }}>Expenditure Parameters CSV <span style={{ fontWeight: 400, color: AL }}>(optional — enables expenditure projection)</span></label>
            <a href={`${API_BASE}/api/forecast/templates/expenditure-params`} style={{ fontSize: 10, color: cB }}>Download template</a>
          </div>
          <input type="file" accept=".csv" onChange={e => setParamsFile(e.target.files?.[0] || null)} style={{
            width: "100%", padding: "6px", borderRadius: 6, border: `1px solid ${BD}`,
            fontSize: 11, fontFamily: FB, background: SF,
          }} />
          <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>Cap rates (PMPM) for managed care, cost-per-eligible for FFS, with trend factors</div>
        </div>
        {/* Options row */}
        <div style={{ display: "flex", gap: 20, marginTop: 14, alignItems: "center" }}>
          <label style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: A, cursor: "pointer" }}>
            <input type="checkbox" checked={includeSeasonality} onChange={e => setIncludeSeasonality(e.target.checked)} />
            Include seasonality
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: A, cursor: "pointer" }}>
            <input type="checkbox" checked={includeEconomic} onChange={e => setIncludeEconomic(e.target.checked)} />
            Include economic covariates
          </label>
        </div>
        {/* Action buttons */}
        <div style={{ display: "flex", gap: 8, marginTop: 16 }}>
          <button
            onClick={handleGenerate}
            disabled={!caseloadFile || loading}
            style={{
              padding: "8px 20px", borderRadius: 6, border: "none",
              background: caseloadFile ? cB : BD, color: WH,
              fontSize: 12, fontWeight: 600, cursor: caseloadFile ? "pointer" : "default",
              opacity: loading ? 0.6 : 1,
            }}
          >
            {loading ? "Running forecast..." : "Generate Forecast"}
          </button>
          {result && (
            <button
              onClick={handleDownloadCSV}
              disabled={loading}
              style={{
                padding: "8px 20px", borderRadius: 6, border: `1px solid ${BD}`,
                background: WH, color: A, fontSize: 12, fontWeight: 600, cursor: "pointer",
              }}
            >
              Download CSV
            </button>
          )}
          {paramsFile && (
            <button
              onClick={handleExpenditure}
              disabled={!caseloadFile || expLoading}
              style={{
                padding: "8px 20px", borderRadius: 6, border: "none",
                background: caseloadFile && paramsFile ? "#C4590A" : BD, color: WH,
                fontSize: 12, fontWeight: 600, cursor: caseloadFile ? "pointer" : "default",
                opacity: expLoading ? 0.6 : 1,
              }}
            >
              {expLoading ? "Projecting expenditure..." : "Forecast + Expenditure"}
            </button>
          )}
        </div>
        {error && (
          <div style={{ marginTop: 12, padding: "8px 12px", background: "#FEE2E2", borderRadius: 6, fontSize: 12, color: NEG }}>{error}</div>
        )}
        {expError && (
          <div style={{ marginTop: 8, padding: "8px 12px", background: "#FEE2E2", borderRadius: 6, fontSize: 12, color: NEG }}>{expError}</div>
        )}
      </Card>

      {/* ─── Results ──────────────────────────────────────────────────── */}
      {result && (() => {
        const m = result.meta;
        const cats = result.categories;

        return (
          <>
            {/* Summary metrics */}
            <Card accent={cB}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <CH title={`Forecast: ${m.state_name}`} sub={`Generated ${m.forecast_date} | ${m.total_history_months} months history | ${m.horizon_months} months forecast`} />
                <button onClick={() => {
                  const allRows: (string | number)[][] = [];
                  for (const cat of cats) {
                    for (const a of cat.actuals) allRows.push([cat.category, a.month, "actual", a.enrollment, "", "", "", ""]);
                    for (const f of cat.forecasts) allRows.push([cat.category, f.month, "forecast", f.point, f.lower_80, f.upper_80, f.lower_95, f.upper_95]);
                  }
                  downloadCSV(
                    ["Category", "Month", "Type", "Enrollment", "Lower 80%", "Upper 80%", "Lower 95%", "Upper 95%"],
                    allRows,
                    `caseload_forecast_${m.state_code}.csv`,
                  );
                }} style={{
                  padding: "5px 12px", borderRadius: 6, border: `1px solid ${BD}`,
                  background: WH, color: AL, fontSize: 11, cursor: "pointer", fontFamily: FM, whiteSpace: "nowrap",
                }}>Export CSV</button>
              </div>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0" }}>
                <Met label="Categories" value={m.n_categories} mono />
                <Met label="History" value={`${m.total_history_months} mo`} mono />
                <Met label="Horizon" value={`${m.horizon_months} mo`} mono />
                <Met label="Economic Covariates" value={m.economic_covariates_used.length > 0 ? m.economic_covariates_used.join(", ") : "None"} />
              </div>
              {m.warnings.length > 0 && (
                <div style={{ marginTop: 8, padding: "6px 10px", background: "#FEF3CD", borderRadius: 6, fontSize: 10, color: WARN }}>
                  {m.warnings.map((w, i) => <div key={i}>{w}</div>)}
                </div>
              )}
            </Card>

            {/* Tab bar — visible when we have results */}
            <div style={{ display: "flex", gap: 4, marginBottom: 16 }}>
              <Pill label="Caseload Forecast" active={activeTab === "caseload"} onClick={() => setActiveTab("caseload")} />
              {expResult && <Pill label="Expenditure Projection" active={activeTab === "expenditure"} onClick={() => setActiveTab("expenditure")} color="#C4590A" />}
              <Pill label="Scenario Builder" active={activeTab === "scenario"} onClick={() => setActiveTab("scenario")} color="#5B6E8A" />
            </div>

            {/* ─── Caseload forecast view ────────────────────────── */}
            {(activeTab === "caseload" || !expResult) && (<>
            {/* Category pills */}
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 16 }}>
              <Pill label="Aggregate" active={selectedCategory === "aggregate"} onClick={() => setSelectedCategory("aggregate")} />
              {cats.map(c => (
                <Pill key={c.category} label={c.category} active={selectedCategory === c.category} onClick={() => setSelectedCategory(c.category)} />
              ))}
            </div>

            {/* Fan chart */}
            <Card>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <CH title={selectedCategory === "aggregate" ? "Total Enrollment Forecast" : selectedCategory} sub={selectedCatData ? `Model: ${modelLabel(selectedCatData.model_used)} ${selectedCatData.model_order} | AIC: ${selectedCatData.fit_aic?.toFixed(1) ?? "—"} | MAPE: ${fmtPct(selectedCatData.fit_mape)}` : undefined} />
                <div style={{ display: "flex", gap: 4 }}>
                  <Pill label="95% CI" active={showCI === "95"} onClick={() => setShowCI("95")} />
                  <Pill label="80% CI" active={showCI === "80"} onClick={() => setShowCI("80")} />
                  <Pill label="No CI" active={showCI === "none"} onClick={() => setShowCI("none")} />
                </div>
              </div>
              <ChartActions filename={`${state || "forecast"}-caseload`}>
              <ResponsiveContainer width="100%" height={360}>
                <ComposedChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
                  <XAxis
                    dataKey="month"
                    tick={{ fontSize: 10, fill: AL, fontFamily: FM }}
                    interval="preserveStartEnd"
                    tickFormatter={v => { const parts = v.split("-"); return parts.length === 2 ? `${parts[1]}/${parts[0].slice(2)}` : v; }}
                  />
                  <YAxis
                    tick={{ fontSize: 11, fill: AL, fontFamily: FM }}
                    tickFormatter={v => fmtNum(v)}
                    width={60}
                  />

                  {/* 95% confidence band (stacked area: base + band) */}
                  {showCI === "95" && (
                    <>
                      <Area type="monotone" dataKey="ci95_base" stackId="ci95" fill="transparent" stroke="transparent" />
                      <Area type="monotone" dataKey="ci95_band" stackId="ci95" fill={`${cB}08`} stroke={`${cB}18`} strokeWidth={0.5} name="95% CI" />
                    </>
                  )}

                  {/* 80% confidence band */}
                  {showCI !== "none" && (
                    <>
                      <Area type="monotone" dataKey="ci80_base" stackId="ci80" fill="transparent" stroke="transparent" />
                      <Area type="monotone" dataKey="ci80_band" stackId="ci80" fill={`${cB}15`} stroke={`${cB}30`} strokeWidth={0.5} name="80% CI" />
                    </>
                  )}

                  {/* Actual line */}
                  <Line type="monotone" dataKey="actual" stroke={A} strokeWidth={2} dot={false} name="Actual" connectNulls={false} />

                  {/* Forecast line */}
                  <Line type="monotone" dataKey="forecast" stroke={cB} strokeWidth={2} strokeDasharray="6 3" dot={false} name="Forecast" connectNulls={false} />

                  {/* Event markers */}
                  {selectedEvents.map(e => (
                    <ReferenceLine
                      key={`${e.date}-${e.type}`}
                      x={e.date}
                      stroke={WARN}
                      strokeDasharray="4 2"
                      strokeWidth={1.5}
                    />
                  ))}

                  <Tooltip content={<ForecastTooltip />} />
                  <Legend
                    wrapperStyle={{ fontSize: 10, fontFamily: FB }}
                    formatter={(value: string) => value === "95% CI" || value === "80% CI" ? value : value}
                  />
                </ComposedChart>
              </ResponsiveContainer>
              </ChartActions>
              {/* Event legend below chart */}
              {selectedEvents.length > 0 && (
                <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 8, paddingTop: 8, borderTop: `1px solid ${BD}` }}>
                  {selectedEvents.map(e => (
                    <div key={`${e.date}-${e.type}`} style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 10, color: AL }}>
                      <span style={{ color: WARN, fontWeight: 700 }}>{eventIcon(e.type)}</span>
                      <span style={{ fontFamily: FM }}>{e.date}</span>
                      <span>{e.description}</span>
                      <span style={{
                        padding: "1px 5px", borderRadius: 8, fontSize: 9, fontWeight: 600,
                        background: e.source === "system" ? SF : "#FEF3CD",
                        color: e.source === "system" ? AL : WARN,
                      }}>{e.source}</span>
                    </div>
                  ))}
                </div>
              )}
            </Card>

            {/* Intervention effects (if any) */}
            {selectedCatData && Object.keys(selectedCatData.intervention_effects).length > 0 && (
              <Card>
                <CH title="Intervention Effects" sub="Estimated enrollment impact of structural events detected by SARIMAX" />
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12 }}>
                  {Object.entries(selectedCatData.intervention_effects).map(([event, effect]) => {
                    const positive = effect > 0;
                    return (
                      <div key={event} style={{
                        padding: "10px 14px", borderRadius: 8, background: SF,
                        border: `1px solid ${BD}`,
                      }}>
                        <div style={{ fontSize: 10, color: AL, marginBottom: 4, textTransform: "capitalize" }}>
                          {event.replace(/_/g, " ")}
                        </div>
                        <div style={{
                          fontSize: 16, fontWeight: 700, fontFamily: FM,
                          color: positive ? POS : NEG,
                        }}>
                          {positive ? "+" : ""}{fmtNum(effect)}
                        </div>
                        <div style={{ fontSize: 9, color: AL }}>enrollment impact</div>
                      </div>
                    );
                  })}
                </div>
              </Card>
            )}

            {/* Model comparison table */}
            <Card>
              <CH title="Model Selection Summary" sub="Best model selected per category by AIC with holdout MAPE validation" />
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FB }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${BD}` }}>
                      {["Category", "Model", "Order", "AIC", "MAPE", "History", "Interventions"].map(h => (
                        <th key={h} style={{ padding: "8px 10px", textAlign: "left", fontWeight: 600, color: A, fontSize: 10, letterSpacing: 0.3 }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {cats.map((c, i) => (
                      <tr
                        key={c.category}
                        style={{
                          background: i % 2 === 0 ? WH : SF,
                          borderBottom: `1px solid ${BD}`,
                          cursor: "pointer",
                        }}
                        onClick={() => setSelectedCategory(c.category)}
                      >
                        <td style={{ padding: "7px 10px", fontWeight: selectedCategory === c.category ? 700 : 400, color: selectedCategory === c.category ? cB : A }}>{c.category}</td>
                        <td style={{ padding: "7px 10px" }}>
                          <span style={{
                            padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
                            background: c.model_used === "sarimax" ? `${POS}15` : c.model_used === "ets" ? `${WARN}15` : `${NEG}15`,
                            color: c.model_used === "sarimax" ? POS : c.model_used === "ets" ? WARN : NEG,
                          }}>
                            {modelLabel(c.model_used)}
                          </span>
                        </td>
                        <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: AL }}>{c.model_order || "—"}</td>
                        <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10 }}>{c.fit_aic?.toFixed(1) ?? "—"}</td>
                        <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: (c.fit_mape ?? 100) < 5 ? POS : (c.fit_mape ?? 100) < 10 ? WARN : NEG }}>
                          {fmtPct(c.fit_mape)}
                        </td>
                        <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10 }}>{c.history_months} mo</td>
                        <td style={{ padding: "7px 10px", fontSize: 10, color: AL }}>
                          {Object.keys(c.intervention_effects).length > 0
                            ? Object.keys(c.intervention_effects).map(k => k.replace(/_/g, " ")).join(", ")
                            : "—"
                          }
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
            </>)}

            {/* ─── Expenditure projection view ───────────────────── */}
            {activeTab === "expenditure" && expResult && (() => {
              const em = expResult.meta;
              const eCats = expResult.categories;
              const selectedExpCat = eCats.find((c: any) => c.category === selectedCategory);

              // Build expenditure chart data
              const expChartSource = selectedCategory === "aggregate"
                ? expResult.aggregate.projections
                : selectedExpCat?.projections || [];

              const expChartData = expChartSource.map((p: any) => ({
                month: p.month,
                expenditure: p.expenditure,
                ci80_base: p.lower_80,
                ci80_band: p.upper_80 - p.lower_80,
                ci95_base: p.lower_95,
                ci95_band: p.upper_95 - p.lower_95,
              }));

              return (<>
                {/* Expenditure summary */}
                <Card accent="#C4590A">
                  <CH title="Expenditure Projection" sub={`${em.state_code} | ${em.projection_date} | ${em.horizon_months} months | ${em.n_categories} categories`} />
                  <div style={{ display: "flex", gap: 24, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0" }}>
                    <Met label="Total Projected" value={fmtDollars(em.total_projected)} color="#C4590A" mono />
                    <Met label="Managed Care" value={fmtDollars(em.total_mc_projected)} color={POS} mono />
                    <Met label="Fee-for-Service" value={fmtDollars(em.total_ffs_projected)} color={A} mono />
                    <Met label="Categories" value={em.n_categories} mono />
                  </div>
                  {em.warnings?.length > 0 && (
                    <div style={{ marginTop: 8, padding: "6px 10px", background: "#FEF3CD", borderRadius: 6, fontSize: 10, color: WARN }}>
                      {em.warnings.map((w: string, i: number) => <div key={i}>{w}</div>)}
                    </div>
                  )}
                </Card>

                {/* Category pills */}
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 16 }}>
                  <Pill label="Aggregate" active={selectedCategory === "aggregate"} onClick={() => setSelectedCategory("aggregate")} color="#C4590A" />
                  {eCats.map((c: any) => (
                    <Pill key={c.category} label={c.category} active={selectedCategory === c.category} onClick={() => setSelectedCategory(c.category)} color="#C4590A" />
                  ))}
                </div>

                {/* Expenditure fan chart */}
                <Card>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                    <CH
                      title={selectedCategory === "aggregate" ? "Total Expenditure Projection" : `${selectedCategory} Expenditure`}
                      sub={selectedExpCat ? `${selectedExpCat.payment_type === "capitation" ? "Capitation" : "FFS"} | Base rate: ${fmtDollars(selectedExpCat.base_rate)}/mo | Trend: ${selectedExpCat.annual_trend_pct}%/yr` : undefined}
                    />
                    <div style={{ display: "flex", gap: 4 }}>
                      <Pill label="95% CI" active={showCI === "95"} onClick={() => setShowCI("95")} color="#C4590A" />
                      <Pill label="80% CI" active={showCI === "80"} onClick={() => setShowCI("80")} color="#C4590A" />
                      <Pill label="No CI" active={showCI === "none"} onClick={() => setShowCI("none")} color="#C4590A" />
                    </div>
                  </div>
                  <ChartActions filename={`${state || "forecast"}-expenditure`}>
                  <ResponsiveContainer width="100%" height={360}>
                    <ComposedChart data={expChartData} margin={{ left: 10, right: 20, top: 10, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
                      <XAxis
                        dataKey="month"
                        tick={{ fontSize: 10, fill: AL, fontFamily: FM }}
                        interval="preserveStartEnd"
                        tickFormatter={(v: string) => { const parts = v.split("-"); return parts.length === 2 ? `${parts[1]}/${parts[0].slice(2)}` : v; }}
                      />
                      <YAxis
                        tick={{ fontSize: 11, fill: AL, fontFamily: FM }}
                        tickFormatter={(v: number) => fmtDollars(v)}
                        width={80}
                      />
                      {showCI === "95" && (
                        <>
                          <Area type="monotone" dataKey="ci95_base" stackId="ci95" fill="transparent" stroke="transparent" />
                          <Area type="monotone" dataKey="ci95_band" stackId="ci95" fill="#C4590A08" stroke="#C4590A18" strokeWidth={0.5} name="95% CI" />
                        </>
                      )}
                      {showCI !== "none" && (
                        <>
                          <Area type="monotone" dataKey="ci80_base" stackId="ci80" fill="transparent" stroke="transparent" />
                          <Area type="monotone" dataKey="ci80_band" stackId="ci80" fill="#C4590A15" stroke="#C4590A30" strokeWidth={0.5} name="80% CI" />
                        </>
                      )}
                      <Line type="monotone" dataKey="expenditure" stroke="#C4590A" strokeWidth={2} dot={false} name="Expenditure" />
                      <Tooltip
                        content={({ active, payload, label }: any) => {
                          if (!active || !payload?.length) return null;
                          const exp = payload.find((p: any) => p.dataKey === "expenditure");
                          return (
                            <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "8px 12px", fontSize: 11, fontFamily: FM, boxShadow: SH }}>
                              <div style={{ fontWeight: 600, color: A, marginBottom: 4 }}>{label}</div>
                              {exp && <div style={{ color: "#C4590A" }}>Expenditure: <strong>{fmtDollars(exp.value)}</strong></div>}
                            </div>
                          );
                        }}
                      />
                      <Legend wrapperStyle={{ fontSize: 10, fontFamily: FB }} />
                    </ComposedChart>
                  </ResponsiveContainer>
                  </ChartActions>
                </Card>

                {/* Per-category expenditure table */}
                <Card>
                  <CH title="Category Expenditure Summary" sub="Projected total expenditure by enrollment category over the forecast horizon" />
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FB }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Category", "Type", "Base Rate", "Trend", "Admin", "Risk", "Total Projected", "Avg Monthly", "Peak Month"].map(h => (
                            <th key={h} style={{ padding: "8px 10px", textAlign: "left", fontWeight: 600, color: A, fontSize: 10, letterSpacing: 0.3 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {eCats.map((c: any, i: number) => (
                          <tr
                            key={c.category}
                            style={{
                              background: i % 2 === 0 ? WH : SF,
                              borderBottom: `1px solid ${BD}`,
                              cursor: "pointer",
                            }}
                            onClick={() => setSelectedCategory(c.category)}
                          >
                            <td style={{ padding: "7px 10px", fontWeight: selectedCategory === c.category ? 700 : 400, color: selectedCategory === c.category ? "#C4590A" : A }}>{c.category}</td>
                            <td style={{ padding: "7px 10px" }}>
                              <span style={{
                                padding: "2px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600,
                                background: c.payment_type === "capitation" ? `${POS}15` : `${A}15`,
                                color: c.payment_type === "capitation" ? POS : A,
                              }}>
                                {c.payment_type === "capitation" ? "MC Cap" : "FFS"}
                              </span>
                            </td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10 }}>{fmtDollars(c.base_rate)}</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: AL }}>{c.annual_trend_pct}%</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: AL }}>{c.admin_load_pct > 0 ? `${c.admin_load_pct}%` : "—"}</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: AL }}>{c.risk_margin_pct > 0 ? `${c.risk_margin_pct}%` : "—"}</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, fontWeight: 700, color: "#C4590A" }}>{fmtDollars(c.total_projected)}</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10 }}>{fmtDollars(c.avg_monthly)}</td>
                            <td style={{ padding: "7px 10px", fontFamily: FM, fontSize: 10, color: AL }}>{c.peak_month}</td>
                          </tr>
                        ))}
                        {/* Totals row */}
                        <tr style={{ borderTop: `2px solid ${BD}`, background: SF }}>
                          <td style={{ padding: "8px 10px", fontWeight: 700, color: A }}>TOTAL</td>
                          <td colSpan={5} />
                          <td style={{ padding: "8px 10px", fontFamily: FM, fontSize: 11, fontWeight: 700, color: "#C4590A" }}>{fmtDollars(em.total_projected)}</td>
                          <td style={{ padding: "8px 10px", fontFamily: FM, fontSize: 10 }}>{fmtDollars(em.total_projected / Math.max(em.horizon_months, 1))}</td>
                          <td />
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </Card>

                {/* MC vs FFS breakdown bar */}
                <Card>
                  <CH title="Payment Type Breakdown" sub="Managed care (capitation) vs fee-for-service expenditure share" />
                  <div style={{ display: "flex", gap: 20, alignItems: "center", padding: "8px 0" }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", height: 28, borderRadius: 6, overflow: "hidden" }}>
                        <div style={{
                          width: `${(em.total_mc_projected / Math.max(em.total_projected, 1)) * 100}%`,
                          background: POS, display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: 10, fontWeight: 700, color: WH, fontFamily: FM, minWidth: 40,
                        }}>
                          {((em.total_mc_projected / Math.max(em.total_projected, 1)) * 100).toFixed(1)}%
                        </div>
                        <div style={{
                          flex: 1, background: `${A}30`, display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: 10, fontWeight: 700, color: A, fontFamily: FM, minWidth: 40,
                        }}>
                          {((em.total_ffs_projected / Math.max(em.total_projected, 1)) * 100).toFixed(1)}%
                        </div>
                      </div>
                      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 6 }}>
                        <div style={{ fontSize: 10, color: POS }}>
                          <span style={{ fontWeight: 700 }}>Managed Care:</span> {fmtDollars(em.total_mc_projected)}
                        </div>
                        <div style={{ fontSize: 10, color: A }}>
                          <span style={{ fontWeight: 700 }}>FFS:</span> {fmtDollars(em.total_ffs_projected)}
                        </div>
                      </div>
                    </div>
                  </div>
                </Card>
              </>);
            })()}
          </>
        );
      })()}

      {/* ─── Scenario Builder view ─────────────────────────────── */}
      {activeTab === "scenario" && (() => {
        // Apply scenario adjustments to the aggregate forecast
        const unemploymentMultiplier = 1 + (scenUnemployment * 0.02); // ~2% enrollment per 1pp unemployment
        const eligibilityMultiplier = 1 + (scenEligibility / 100);
        const enrollmentMultiplier = unemploymentMultiplier * eligibilityMultiplier;
        const rateMultiplier = 1 + (scenRateChange / 100);
        // MC shift: each pp shift to MC reduces per-capita cost ~0.5% (MC capitation typically 90-95% of FFS)
        const mcCostMultiplier = 1 - (scenMcShift * 0.005);

        const agg = result?.aggregate;
        if (!agg) return null;
        const baselineTotal = agg.forecasts.reduce((s, f) => s + f.point, 0);
        const scenarioTotal = baselineTotal * enrollmentMultiplier;
        const deltaEnrollment = scenarioTotal - baselineTotal;

        // Build chart data with baseline and scenario lines
        const scenChartData = [
          ...agg.actuals.map(a => ({ month: a.month, actual: a.enrollment })),
          ...agg.forecasts.map(f => ({
            month: f.month,
            baseline: f.point,
            scenario: Math.round(f.point * enrollmentMultiplier),
            ci80_base: f.lower_80 * enrollmentMultiplier,
            ci80_band: (f.upper_80 - f.lower_80) * enrollmentMultiplier,
          })),
        ];

        // Expenditure impact (MC shift affects cost, not enrollment)
        const hasExp = !!expResult;
        const baseExp = hasExp ? expResult.meta.total_projected : 0;
        const scenExp = baseExp * enrollmentMultiplier * rateMultiplier * mcCostMultiplier;

        const SB = "#5B6E8A";
        const SC = "#8B5CF6";

        return (
          <>
            <Card accent={SB}>
              <CH title="Scenario Builder" sub="Adjust parameters and see projected impact on caseload and expenditure" />

              {/* Sliders */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 16, marginTop: 8 }}>
                {/* Unemployment */}
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: A, fontWeight: 600 }}>Unemployment change</span>
                    <span style={{ fontSize: 12, fontWeight: 700, color: scenUnemployment !== 0 ? SC : AL, fontFamily: FM }}>
                      {scenUnemployment > 0 ? "+" : ""}{scenUnemployment} pp
                    </span>
                  </div>
                  <input
                    type="range" min={-3} max={5} step={0.5} value={scenUnemployment}
                    onChange={e => setScenUnemployment(Number(e.target.value))}
                    style={{ width: "100%", accentColor: SB }}
                  />
                  <div style={{ fontSize: 10, color: AL }}>Each +1pp ~ +2% enrollment (Medicaid elasticity)</div>
                </div>

                {/* Eligibility expansion */}
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: A, fontWeight: 600 }}>Eligibility expansion</span>
                    <span style={{ fontSize: 12, fontWeight: 700, color: scenEligibility !== 0 ? SC : AL, fontFamily: FM }}>
                      {scenEligibility > 0 ? "+" : ""}{scenEligibility}%
                    </span>
                  </div>
                  <input
                    type="range" min={-10} max={20} step={1} value={scenEligibility}
                    onChange={e => setScenEligibility(Number(e.target.value))}
                    style={{ width: "100%", accentColor: SB }}
                  />
                  <div style={{ fontSize: 10, color: AL }}>% change in eligible population (expansion, contraction)</div>
                </div>

                {/* Rate change */}
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: A, fontWeight: 600 }}>Rate adjustment</span>
                    <span style={{ fontSize: 12, fontWeight: 700, color: scenRateChange !== 0 ? SC : AL, fontFamily: FM }}>
                      {scenRateChange > 0 ? "+" : ""}{scenRateChange}%
                    </span>
                  </div>
                  <input
                    type="range" min={-20} max={30} step={1} value={scenRateChange}
                    onChange={e => setScenRateChange(Number(e.target.value))}
                    style={{ width: "100%", accentColor: SB }}
                  />
                  <div style={{ fontSize: 10, color: AL }}>Across-the-board rate change (affects expenditure only)</div>
                </div>

                {/* MC shift */}
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                    <span style={{ fontSize: 11, color: A, fontWeight: 600 }}>FFS → MC shift</span>
                    <span style={{ fontSize: 12, fontWeight: 700, color: scenMcShift !== 0 ? SC : AL, fontFamily: FM }}>
                      {scenMcShift > 0 ? "+" : ""}{scenMcShift} pp
                    </span>
                  </div>
                  <input
                    type="range" min={-10} max={20} step={1} value={scenMcShift}
                    onChange={e => setScenMcShift(Number(e.target.value))}
                    style={{ width: "100%", accentColor: SB }}
                  />
                  <div style={{ fontSize: 10, color: AL }}>Shift enrollment from FFS to managed care</div>
                </div>
              </div>

              {/* Preset scenarios */}
              <div style={{ marginTop: 16, display: "flex", gap: 6, flexWrap: "wrap" }}>
                <span style={{ fontSize: 10, fontWeight: 600, color: AL, alignSelf: "center" }}>PRESETS:</span>
                {[
                  { label: "Recession (+2pp UE)", fn: () => { setScenUnemployment(2); setScenEligibility(0); setScenRateChange(0); setScenMcShift(0); }},
                  { label: "Expansion (+10% elig)", fn: () => { setScenUnemployment(0); setScenEligibility(10); setScenRateChange(0); setScenMcShift(0); }},
                  { label: "Rate increase (+15%)", fn: () => { setScenUnemployment(0); setScenEligibility(0); setScenRateChange(15); setScenMcShift(0); }},
                  { label: "MC transition (+10pp)", fn: () => { setScenUnemployment(0); setScenEligibility(0); setScenRateChange(0); setScenMcShift(10); }},
                  { label: "Reset", fn: () => { setScenUnemployment(0); setScenEligibility(0); setScenRateChange(0); setScenMcShift(0); }},
                ].map(p => (
                  <button key={p.label} onClick={p.fn} style={{
                    padding: "4px 10px", borderRadius: 12, border: `1px solid ${BD}`,
                    background: WH, color: p.label === "Reset" ? NEG : A,
                    fontSize: 10, fontWeight: 500, cursor: "pointer", fontFamily: FB,
                  }}>{p.label}</button>
                ))}
              </div>
            </Card>

            {/* Impact summary */}
            <Card accent={SC}>
              <CH title="Scenario Impact" sub="Compared to baseline forecast" />
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 20, padding: "8px 0" }}>
                <Met label="Baseline Enrollment" value={fmtNum(agg.forecasts.length ? baselineTotal / agg.forecasts.length : 0)} mono />
                <Met label="Scenario Enrollment" value={fmtNum(agg.forecasts.length ? scenarioTotal / agg.forecasts.length : 0)} color={enrollmentMultiplier > 1 ? WARN : POS} mono />
                <Met label="Enrollment Delta" value={(deltaEnrollment >= 0 ? "+" : "") + fmtNum(agg.forecasts.length ? deltaEnrollment / agg.forecasts.length : 0) + "/mo"} color={deltaEnrollment > 0 ? NEG : POS} mono />
                {hasExp && <>
                  <Met label="Baseline Expenditure" value={fmtDollars(baseExp)} mono />
                  <Met label="Scenario Expenditure" value={fmtDollars(scenExp)} color={scenExp > baseExp ? WARN : POS} mono />
                  <Met label="Expenditure Delta" value={(scenExp >= baseExp ? "+" : "") + fmtDollars(scenExp - baseExp)} color={scenExp > baseExp ? NEG : POS} mono />
                </>}
              </div>
            </Card>

            {/* Scenario chart */}
            <Card>
              <CH title="Baseline vs Scenario" sub="Purple = scenario projection, green = baseline" />
              <div style={{ height: 340, marginTop: 8 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={scenChartData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="month" tick={{ fontSize: 10, fill: AL }} interval="preserveStartEnd" />
                    <YAxis tickFormatter={(v: number) => fmtNum(v)} tick={{ fontSize: 10, fill: AL }} width={60} />
                    <Tooltip content={<ForecastTooltip />} />
                    {/* CI band for scenario */}
                    <Area type="monotone" dataKey="ci80_base" stackId="sci" fill="transparent" stroke="none" />
                    <Area type="monotone" dataKey="ci80_band" stackId="sci" fill={`${SC}15`} stroke="none" />
                    {/* Lines */}
                    <Line type="monotone" dataKey="actual" stroke={cB} strokeWidth={2} dot={false} name="Actual" />
                    <Line type="monotone" dataKey="baseline" stroke={cB} strokeWidth={1.5} strokeDasharray="6 3" dot={false} name="Baseline" />
                    <Line type="monotone" dataKey="scenario" stroke={SC} strokeWidth={2.5} dot={false} name="Scenario" />
                    <Legend wrapperStyle={{ fontSize: 10 }} />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            </Card>

            {/* Per-category impact table */}
            <Card>
              <CH title="Category Breakdown" sub="Scenario adjustment applied uniformly across categories" />
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FM }}>
                  <thead>
                    <tr style={{ borderBottom: `2px solid ${BD}` }}>
                      {["Category", "Model", "Baseline Avg/Mo", "Scenario Avg/Mo", "Delta/Mo", "Delta %"].map(h => (
                        <th key={h} style={{ padding: "8px 10px", textAlign: h === "Category" || h === "Model" ? "left" : "right", color: AL, fontWeight: 600, fontSize: 10 }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(result?.categories ?? []).map((c: { category: string; model_used: string; forecasts: { point: number }[] }, i: number) => {
                      const baseAvg = c.forecasts.length ? c.forecasts.reduce((s: number, f: { point: number }) => s + f.point, 0) / c.forecasts.length : 0;
                      const scenAvg = baseAvg * enrollmentMultiplier;
                      const delta = scenAvg - baseAvg;
                      return (
                        <tr key={c.category} style={{ background: i % 2 === 0 ? WH : SF, borderBottom: `1px solid ${BD}` }}>
                          <td style={{ padding: "8px 10px", fontWeight: 600, color: A }}>{c.category}</td>
                          <td style={{ padding: "8px 10px", color: AL }}>{modelLabel(c.model_used)}</td>
                          <td style={{ padding: "8px 10px", textAlign: "right" }}>{fmtNum(baseAvg)}</td>
                          <td style={{ padding: "8px 10px", textAlign: "right", color: SC, fontWeight: 600 }}>{fmtNum(scenAvg)}</td>
                          <td style={{ padding: "8px 10px", textAlign: "right", color: delta > 0 ? NEG : POS }}>
                            {delta >= 0 ? "+" : ""}{fmtNum(delta)}
                          </td>
                          <td style={{ padding: "8px 10px", textAlign: "right", color: delta > 0 ? NEG : POS }}>
                            {((enrollmentMultiplier - 1) * 100).toFixed(1)}%
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          </>
        );
      })()}
    </div>
  );
}
