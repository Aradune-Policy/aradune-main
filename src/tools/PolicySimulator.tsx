/**
 * Policy Simulator - System Dynamics Module
 *
 * Models downstream effects of Medicaid policy changes using
 * interconnected feedback loops, stocks and flows.
 * Route: /#/policy-simulator
 */
import { useState, useMemo, useCallback } from "react";
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, ReferenceLine,
} from "recharts";
import { C, FONT, SHADOW, useIsMobile } from "../design";
import { useAradune } from "../context/AraduneContext";
import { getAuthHeaders, API_BASE } from "../lib/api";
import { STATE_NAMES, STATES_LIST } from "../data/states";
import StateContextBar from "../components/StateContextBar";
import ChartActions from "../components/ChartActions";
import { LoadingBar } from "../components/LoadingBar";

// ── Design tokens (matches Aradune v14) ──────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const WARN = "#B8860B";
const SF = "#F5F7F5";
const BD = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Shared UI Primitives ──────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{
    background: WH, borderRadius: 10, padding: "20px 24px", marginBottom: 16,
    boxShadow: SH, borderTop: accent ? `3px solid ${accent}` : undefined,
    border: `1px solid ${BD}`,
  }}>{children}</div>
);

const CH = ({ title, sub }: { title: string; sub?: string }) => (
  <div style={{ marginBottom: 12 }}>
    <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A, letterSpacing: -0.2 }}>{title}</h3>
    {sub && <p style={{ margin: "3px 0 0", fontSize: 11, color: AL }}>{sub}</p>}
  </div>
);

const Met = ({ label, value, color, mono }: { label: string; value: string | number; color?: string; mono?: boolean }) => (
  <div style={{ textAlign: "center", minWidth: 80 }}>
    <div style={{ fontSize: mono ? 20 : 18, fontWeight: 700, color: color || A, fontFamily: mono ? FM : FB, letterSpacing: -0.5 }}>{value}</div>
    <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>{label}</div>
  </div>
);

const Pill = ({ on, onClick, children }: { on: boolean; onClick: () => void; children: React.ReactNode }) => (
  <button onClick={onClick} style={{
    padding: "4px 10px", fontSize: 10, fontWeight: on ? 700 : 400,
    color: on ? WH : AL, background: on ? cB : "transparent",
    border: `1px solid ${on ? cB : BD}`, borderRadius: 5, cursor: "pointer",
    transition: "all .15s",
  }}>{children}</button>
);

// ── Types ──────────────────────────────────────────────────────────────────
interface Intervention {
  type: "rate_change" | "wage_increase" | "hcbs_funding" | "unemployment_shock" | "policy_change";
  value: number;
  start_month: number;
  duration_months?: number;
  label?: string;
}

interface MonthData {
  month: number;
  enrolled: number;
  eligible_pool: number;
  active_workers: number;
  access_score: number;
  community_pop: number;
  institutional_pop: number;
  avg_cost: number;
  provider_count: number;
  waitlist: number;
}

interface FeedbackLoop {
  name: string;
  symbol: string;
  description: string;
  type: "reinforcing" | "balancing";
  lag_months: number;
}

interface SimulationResult {
  baseline: { months: MonthData[] };
  scenario: { months: MonthData[] };
  impact: {
    enrollment_delta: number;
    enrollment_delta_pct: number;
    spending_delta: number;
    spending_delta_pct: number;
    provider_count_delta: number;
    provider_count_delta_pct: number;
    waitlist_delta: number;
    waitlist_delta_pct: number;
  };
  feedback_loops_active: FeedbackLoop[];
}

type TabKey = "enrollment" | "spending" | "access" | "workforce" | "hcbs";

// ── Intervention metadata ─────────────────────────────────────────────────
const INTERVENTION_TYPES: Record<Intervention["type"], { label: string; unit: string; defaultValue: number }> = {
  rate_change: { label: "Rate Change", unit: "%", defaultValue: 10 },
  wage_increase: { label: "Wage Increase", unit: "$/hr", defaultValue: 2 },
  hcbs_funding: { label: "HCBS Funding", unit: "%", defaultValue: 15 },
  unemployment_shock: { label: "Unemployment Shock", unit: "pp", defaultValue: 2 },
  policy_change: { label: "Policy Change", unit: "index", defaultValue: 1 },
};

// ── Presets ────────────────────────────────────────────────────────────────
const PRESETS: Record<string, { label: string; interventions: Intervention[] }> = {
  parity: {
    label: "Rate Parity",
    interventions: [{ type: "rate_change", value: 30, start_month: 0 }],
  },
  recession: {
    label: "Recession",
    interventions: [{ type: "unemployment_shock", value: 3, start_month: 3, duration_months: 18 }],
  },
  hcbs: {
    label: "HCBS Expansion",
    interventions: [
      { type: "hcbs_funding", value: 25, start_month: 0 },
      { type: "wage_increase", value: 2, start_month: 0 },
    ],
  },
  austerity: {
    label: "Austerity",
    interventions: [{ type: "rate_change", value: -5, start_month: 0 }],
  },
  workforce: {
    label: "Workforce Investment",
    interventions: [{ type: "wage_increase", value: 4, start_month: 0 }],
  },
};

// ── Tab definitions ───────────────────────────────────────────────────────
const TABS: { key: TabKey; label: string }[] = [
  { key: "enrollment", label: "Enrollment" },
  { key: "spending", label: "Spending" },
  { key: "access", label: "Access" },
  { key: "workforce", label: "Workforce" },
  { key: "hcbs", label: "HCBS" },
];

// ── Format helpers ────────────────────────────────────────────────────────
const fmtK = (n: number | null | undefined): string =>
  n == null ? "--" : Math.abs(n) >= 1e6 ? `${(n / 1e6).toFixed(1)}M`
  : Math.abs(n) >= 1e3 ? `${(n / 1e3).toFixed(1)}K`
  : n.toLocaleString();

const fmtPct = (n: number | null | undefined): string =>
  n == null ? "--" : `${n > 0 ? "+" : ""}${Number(n).toFixed(1)}%`;

const fmtDelta = (n: number | null | undefined): string =>
  n == null ? "--" : `${n > 0 ? "+" : ""}${fmtK(n)}`;

const fmtDollar = (n: number | null | undefined): string => {
  if (n == null || isNaN(n) || !isFinite(n)) return "$0";
  const abs = Math.abs(n), sign = n < 0 ? "-" : "+";
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

// ── Feedback loop symbols ─────────────────────────────────────────────────
const LOOP_SYMBOLS: Record<string, string> = {
  reinforcing: "\u25C6",  // ◆
  balancing: "\u25CB",     // ○
};

// ── Component ─────────────────────────────────────────────────────────────
export default function PolicySimulator() {
  const { openIntelligence } = useAradune();
  const mobile = useIsMobile();

  // State
  const [state, setState] = useState("FL");
  const [horizon, setHorizon] = useState(60);
  const [interventions, setInterventions] = useState<Intervention[]>([]);
  const [result, setResult] = useState<SimulationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("enrollment");

  // ── Intervention CRUD ─────────────────────────────────────────────────
  const addIntervention = useCallback(() => {
    setInterventions(prev => [
      ...prev,
      { type: "rate_change", value: 10, start_month: 0 },
    ]);
  }, []);

  const removeIntervention = useCallback((idx: number) => {
    setInterventions(prev => prev.filter((_, i) => i !== idx));
  }, []);

  const updateIntervention = useCallback((idx: number, patch: Partial<Intervention>) => {
    setInterventions(prev => prev.map((it, i) => i === idx ? { ...it, ...patch } : it));
  }, []);

  const applyPreset = useCallback((key: string) => {
    const preset = PRESETS[key];
    if (preset) setInterventions(preset.interventions.map(i => ({ ...i })));
  }, []);

  // ── Run simulation ────────────────────────────────────────────────────
  const runSimulation = useCallback(async () => {
    if (interventions.length === 0) {
      setError("Add at least one intervention to run the simulation.");
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const headers = await getAuthHeaders();
      const res = await fetch(`${API_BASE}/api/dynamics/policy-simulator`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...headers },
        body: JSON.stringify({
          state_code: state,
          horizon_months: horizon,
          interventions,
        }),
      });
      if (!res.ok) {
        const msg = await res.text().catch(() => "Simulation failed");
        throw new Error(msg);
      }
      const data: SimulationResult = await res.json();
      setResult(data);
    } catch (err: any) {
      setError(err?.message || "Simulation request failed. Check network and try again.");
    } finally {
      setLoading(false);
    }
  }, [state, horizon, interventions]);

  // ── Chart data builders ───────────────────────────────────────────────
  const chartData = useMemo(() => {
    if (!result) return [];
    const base = result.baseline.months;
    const scen = result.scenario.months;
    const len = Math.min(base.length, scen.length);
    return Array.from({ length: len }, (_, i) => ({
      month: base[i].month,
      // Enrollment
      baseline_enrolled: base[i].enrolled,
      scenario_enrolled: scen[i].enrolled,
      // Spending (enrolled * avg_cost)
      baseline_spending: base[i].enrolled * base[i].avg_cost,
      scenario_spending: scen[i].enrolled * scen[i].avg_cost,
      // Access
      baseline_access: base[i].access_score,
      scenario_access: scen[i].access_score,
      // Workforce
      baseline_workers: base[i].active_workers,
      scenario_workers: scen[i].active_workers,
      // HCBS %
      baseline_hcbs: base[i].community_pop + base[i].institutional_pop > 0
        ? (base[i].community_pop / (base[i].community_pop + base[i].institutional_pop)) * 100
        : 0,
      scenario_hcbs: scen[i].community_pop + scen[i].institutional_pop > 0
        ? (scen[i].community_pop / (scen[i].community_pop + scen[i].institutional_pop)) * 100
        : 0,
    }));
  }, [result]);

  // ── Chart config per tab ──────────────────────────────────────────────
  const chartConfig: Record<TabKey, { baseKey: string; scenKey: string; label: string; formatter: (v: number) => string }> = {
    enrollment: { baseKey: "baseline_enrolled", scenKey: "scenario_enrolled", label: "Enrolled", formatter: fmtK },
    spending: { baseKey: "baseline_spending", scenKey: "scenario_spending", label: "Monthly Spending", formatter: (v) => fmtDollar(v).replace("+", "") },
    access: { baseKey: "baseline_access", scenKey: "scenario_access", label: "Access Score", formatter: (v) => v.toFixed(1) },
    workforce: { baseKey: "baseline_workers", scenKey: "scenario_workers", label: "Active Workers", formatter: fmtK },
    hcbs: { baseKey: "baseline_hcbs", scenKey: "scenario_hcbs", label: "HCBS %", formatter: (v) => `${v.toFixed(1)}%` },
  };

  // ── Export CSV ────────────────────────────────────────────────────────
  const exportCsv = useCallback(() => {
    if (!chartData.length) return;
    const keys = Object.keys(chartData[0]) as (keyof typeof chartData[0])[];
    const header = keys.join(",");
    const rows = chartData.map(r => keys.map(k => r[k]).join(","));
    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `policy_sim_${state}_${horizon}mo.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [chartData, state, horizon]);

  // ── Ask Aradune ───────────────────────────────────────────────────────
  const askAradune = useCallback(() => {
    if (!result) {
      openIntelligence({
        summary: `Policy Simulator: ${STATE_NAMES[state] || state}. No simulation run yet.`,
        state,
      });
      return;
    }
    const { impact, feedback_loops_active } = result;
    const feedbackLoops = feedback_loops_active.map(l => l.name);
    openIntelligence({
      summary: `Policy Simulator: ${STATE_NAMES[state] || state}, ${horizon} months, ${interventions.length} interventions. ` +
        `Enrollment: ${impact.enrollment_delta_pct > 0 ? "+" : ""}${impact.enrollment_delta_pct.toFixed(1)}%. ` +
        `Spending: ${impact.spending_delta_pct > 0 ? "+" : ""}${impact.spending_delta_pct.toFixed(1)}%. ` +
        `Providers: ${impact.provider_count_delta > 0 ? "+" : ""}${impact.provider_count_delta}. ` +
        `Feedback loops: ${feedbackLoops.join(", ")}.`,
      state,
    });
  }, [result, state, horizon, interventions, openIntelligence]);

  // ── Impact card helper ────────────────────────────────────────────────
  const impactColor = (delta: number, positive: "up" | "down") => {
    if (delta === 0) return AL;
    if (positive === "up") return delta > 0 ? POS : NEG;
    return delta < 0 ? POS : NEG;
  };

  const cfg = chartConfig[activeTab];

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth: 1060, margin: "0 auto", padding: mobile ? "16px 12px" : "20px 16px" }}>

      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
            Policy Simulator
          </h1>
          <p style={{ margin: "4px 0 0", fontSize: 12, color: AL, maxWidth: 560 }}>
            Model the downstream effects of Medicaid policy changes through interconnected feedback loops
          </p>
        </div>
        <button onClick={askAradune} style={{
          padding: "6px 14px", fontSize: 11, fontWeight: 600,
          color: WH, background: cB, border: "none", borderRadius: 6,
          cursor: "pointer", whiteSpace: "nowrap", flexShrink: 0,
        }}>
          Ask Aradune
        </button>
      </div>

      {/* ── State + Horizon selectors ──────────────────────────────────── */}
      <Card>
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap", alignItems: "flex-end" }}>
          <div style={{ flex: 1, minWidth: 180 }}>
            <label style={{ fontSize: 10, fontWeight: 600, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 4, fontFamily: FM }}>
              State
            </label>
            <select
              value={state}
              onChange={e => setState(e.target.value)}
              style={{
                width: "100%", padding: "6px 8px", fontSize: 13, border: `1px solid ${BD}`,
                borderRadius: 5, background: WH, color: A, fontFamily: FB,
              }}
            >
              {STATES_LIST.map(s => (
                <option key={s} value={s}>{STATE_NAMES[s] || s}</option>
              ))}
            </select>
          </div>
          <div style={{ minWidth: 120 }}>
            <label style={{ fontSize: 10, fontWeight: 600, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 4, fontFamily: FM }}>
              Horizon (months)
            </label>
            <input
              type="number"
              min={6}
              max={120}
              value={horizon}
              onChange={e => setHorizon(Number(e.target.value))}
              style={{
                width: 90, padding: "6px 8px", fontSize: 13, border: `1px solid ${BD}`,
                borderRadius: 5, background: WH, color: A, fontFamily: FM, textAlign: "right",
              }}
            />
          </div>
        </div>
      </Card>

      {/* ── Intervention Builder ──────────────────────────────────────── */}
      <Card>
        <CH title="Interventions" sub="Define policy changes to model" />
        {interventions.length === 0 && (
          <div style={{ fontSize: 12, color: AL, padding: "8px 0", fontStyle: "italic" }}>
            No interventions added. Use a preset below or add one manually.
          </div>
        )}
        {interventions.map((iv, idx) => {
          const meta = INTERVENTION_TYPES[iv.type];
          return (
            <div key={idx} style={{
              display: "flex", gap: 8, alignItems: "flex-end", flexWrap: "wrap",
              padding: "8px 0", borderBottom: idx < interventions.length - 1 ? `1px solid ${BD}` : "none",
            }}>
              {/* Type */}
              <div style={{ flex: 2, minWidth: 140 }}>
                {idx === 0 && (
                  <label style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 3, fontFamily: FM }}>Type</label>
                )}
                <select
                  value={iv.type}
                  onChange={e => updateIntervention(idx, { type: e.target.value as Intervention["type"], value: INTERVENTION_TYPES[e.target.value as Intervention["type"]].defaultValue })}
                  style={{
                    width: "100%", padding: "5px 6px", fontSize: 12, border: `1px solid ${BD}`,
                    borderRadius: 4, background: WH, color: A, fontFamily: FB,
                  }}
                >
                  {Object.entries(INTERVENTION_TYPES).map(([k, v]) => (
                    <option key={k} value={k}>{v.label}</option>
                  ))}
                </select>
              </div>
              {/* Value */}
              <div style={{ flex: 1, minWidth: 80 }}>
                {idx === 0 && (
                  <label style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 3, fontFamily: FM }}>
                    Value ({meta.unit})
                  </label>
                )}
                <input
                  type="number"
                  value={iv.value}
                  onChange={e => updateIntervention(idx, { value: Number(e.target.value) })}
                  style={{
                    width: "100%", padding: "5px 6px", fontSize: 12, border: `1px solid ${BD}`,
                    borderRadius: 4, background: WH, color: A, fontFamily: FM, textAlign: "right",
                  }}
                />
              </div>
              {/* Start month */}
              <div style={{ flex: 1, minWidth: 70 }}>
                {idx === 0 && (
                  <label style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 3, fontFamily: FM }}>Start Mo.</label>
                )}
                <input
                  type="number"
                  min={0}
                  max={horizon}
                  value={iv.start_month}
                  onChange={e => updateIntervention(idx, { start_month: Number(e.target.value) })}
                  style={{
                    width: "100%", padding: "5px 6px", fontSize: 12, border: `1px solid ${BD}`,
                    borderRadius: 4, background: WH, color: A, fontFamily: FM, textAlign: "right",
                  }}
                />
              </div>
              {/* Duration (for shocks) */}
              {(iv.type === "unemployment_shock" || iv.type === "policy_change") && (
                <div style={{ flex: 1, minWidth: 70 }}>
                  {idx === 0 && (
                    <label style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, display: "block", marginBottom: 3, fontFamily: FM }}>Duration</label>
                  )}
                  <input
                    type="number"
                    min={1}
                    max={horizon}
                    value={iv.duration_months ?? 12}
                    onChange={e => updateIntervention(idx, { duration_months: Number(e.target.value) })}
                    style={{
                      width: "100%", padding: "5px 6px", fontSize: 12, border: `1px solid ${BD}`,
                      borderRadius: 4, background: WH, color: A, fontFamily: FM, textAlign: "right",
                    }}
                  />
                </div>
              )}
              {/* Remove */}
              <button
                onClick={() => removeIntervention(idx)}
                title="Remove intervention"
                style={{
                  padding: "4px 8px", fontSize: 14, color: NEG, background: "transparent",
                  border: `1px solid ${BD}`, borderRadius: 4, cursor: "pointer", lineHeight: 1,
                  marginBottom: 1,
                }}
              >
                x
              </button>
            </div>
          );
        })}
        <div style={{ marginTop: 10 }}>
          <button onClick={addIntervention} style={{
            padding: "5px 12px", fontSize: 11, fontWeight: 600, color: cB,
            background: "transparent", border: `1px solid ${cB}`, borderRadius: 5,
            cursor: "pointer",
          }}>
            + Add Intervention
          </button>
        </div>
      </Card>

      {/* ── Presets ────────────────────────────────────────────────────── */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 16 }}>
        <span style={{ fontSize: 10, color: AL, lineHeight: "24px", marginRight: 4, fontFamily: FM }}>PRESETS</span>
        {Object.entries(PRESETS).map(([k, v]) => (
          <Pill key={k} on={false} onClick={() => applyPreset(k)}>{v.label}</Pill>
        ))}
      </div>

      {/* ── Run Button ─────────────────────────────────────────────────── */}
      <div style={{ display: "flex", gap: 10, marginBottom: 20 }}>
        <button
          onClick={runSimulation}
          disabled={loading || interventions.length === 0}
          style={{
            padding: "8px 24px", fontSize: 13, fontWeight: 700,
            color: WH, background: loading ? AL : cB, border: "none",
            borderRadius: 6, cursor: loading ? "not-allowed" : "pointer",
            opacity: interventions.length === 0 ? 0.5 : 1,
            transition: "background .15s",
          }}
        >
          {loading ? "Running..." : "Run Simulation"}
        </button>
      </div>

      {/* ── Error ──────────────────────────────────────────────────────── */}
      {error && (
        <div style={{
          padding: "10px 14px", fontSize: 12, color: NEG, background: "#FEF2F2",
          border: `1px solid #FCA5A5`, borderRadius: 6, marginBottom: 16,
        }}>
          {error}
        </div>
      )}

      {/* ── Loading ────────────────────────────────────────────────────── */}
      {loading && <LoadingBar text="Running policy simulation..." detail={`${STATE_NAMES[state] || state}, ${horizon}-month horizon, ${interventions.length} interventions`} />}

      {/* ── Results ────────────────────────────────────────────────────── */}
      {result && !loading && (
        <>
          {/* Impact summary cards */}
          <div style={{
            display: "grid",
            gridTemplateColumns: mobile ? "1fr 1fr" : "1fr 1fr 1fr 1fr",
            gap: 10, marginBottom: 16,
          }}>
            {/* Enrollment */}
            <Card accent={impactColor(result.impact.enrollment_delta, "up")}>
              <div style={{ textAlign: "center", padding: "4px 0" }}>
                <div style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, fontFamily: FM }}>Enrollment</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: impactColor(result.impact.enrollment_delta, "up"), fontFamily: FM }}>
                  {fmtDelta(result.impact.enrollment_delta)}
                </div>
                <div style={{ fontSize: 10, color: AL }}>{fmtPct(result.impact.enrollment_delta_pct)}</div>
              </div>
            </Card>
            {/* Spending */}
            <Card accent={impactColor(-result.impact.spending_delta, "up")}>
              <div style={{ textAlign: "center", padding: "4px 0" }}>
                <div style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, fontFamily: FM }}>Spending</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: impactColor(-result.impact.spending_delta, "up"), fontFamily: FM }}>
                  {fmtDollar(result.impact.spending_delta)}
                </div>
                <div style={{ fontSize: 10, color: AL }}>{fmtPct(result.impact.spending_delta_pct)}</div>
              </div>
            </Card>
            {/* Providers */}
            <Card accent={impactColor(result.impact.provider_count_delta, "up")}>
              <div style={{ textAlign: "center", padding: "4px 0" }}>
                <div style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, fontFamily: FM }}>Providers</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: impactColor(result.impact.provider_count_delta, "up"), fontFamily: FM }}>
                  {fmtDelta(result.impact.provider_count_delta)}
                </div>
                <div style={{ fontSize: 10, color: AL }}>{fmtPct(result.impact.provider_count_delta_pct)}</div>
              </div>
            </Card>
            {/* Waitlist */}
            <Card accent={impactColor(result.impact.waitlist_delta, "down")}>
              <div style={{ textAlign: "center", padding: "4px 0" }}>
                <div style={{ fontSize: 9, color: AL, textTransform: "uppercase", letterSpacing: 0.5, fontFamily: FM }}>Waitlist</div>
                <div style={{ fontSize: 18, fontWeight: 700, color: impactColor(result.impact.waitlist_delta, "down"), fontFamily: FM }}>
                  {fmtDelta(result.impact.waitlist_delta)}
                </div>
                <div style={{ fontSize: 10, color: AL }}>{fmtPct(result.impact.waitlist_delta_pct)}</div>
              </div>
            </Card>
          </div>

          {/* ── Chart tabs ──────────────────────────────────────────────── */}
          <Card>
            <div style={{ display: "flex", gap: 4, marginBottom: 14, flexWrap: "wrap" }}>
              {TABS.map(t => (
                <Pill key={t.key} on={activeTab === t.key} onClick={() => setActiveTab(t.key)}>
                  {t.label}
                </Pill>
              ))}
            </div>
            <ChartActions filename={`policy_sim_${activeTab}_${state}`}>
              <ResponsiveContainer width="100%" height={320}>
                <ComposedChart data={chartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                  <XAxis
                    dataKey="month"
                    tick={{ fontSize: 10, fill: AL }}
                    label={{ value: "Month", position: "insideBottom", offset: -2, fontSize: 10, fill: AL }}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: AL, fontFamily: FM }}
                    tickFormatter={cfg.formatter}
                    width={60}
                  />
                  <Tooltip
                    contentStyle={{ fontSize: 11, fontFamily: FM, border: `1px solid ${BD}`, borderRadius: 6 }}
                    formatter={(value: number) => [cfg.formatter(value), ""]}
                    labelFormatter={(label: number) => `Month ${label}`}
                  />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                  <Line
                    type="monotone"
                    dataKey={cfg.baseKey}
                    name="Baseline"
                    stroke={AL}
                    strokeDasharray="6 3"
                    strokeWidth={1.5}
                    dot={false}
                  />
                  <Line
                    type="monotone"
                    dataKey={cfg.scenKey}
                    name="Scenario"
                    stroke={POS}
                    strokeWidth={2}
                    dot={false}
                  />
                  {interventions.map((iv, i) => (
                    <ReferenceLine
                      key={i}
                      x={iv.start_month}
                      stroke={WARN}
                      strokeDasharray="4 2"
                      label={{ value: INTERVENTION_TYPES[iv.type].label, position: "top", fontSize: 8, fill: WARN }}
                    />
                  ))}
                </ComposedChart>
              </ResponsiveContainer>
            </ChartActions>
          </Card>

          {/* ── Feedback loops panel ─────────────────────────────────────── */}
          {result.feedback_loops_active.length > 0 && (
            <Card>
              <CH title="Active Feedback Loops" sub={`${result.feedback_loops_active.length} loops detected in this scenario`} />
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {result.feedback_loops_active.map((loop, i) => (
                  <div key={i} style={{
                    display: "flex", gap: 10, alignItems: "flex-start",
                    padding: "8px 10px", background: SF, borderRadius: 6,
                    border: `1px solid ${BD}`,
                  }}>
                    <span style={{
                      fontSize: 16, lineHeight: 1,
                      color: loop.type === "reinforcing" ? POS : WARN,
                      flexShrink: 0, marginTop: 1,
                    }}>
                      {loop.symbol || LOOP_SYMBOLS[loop.type] || "\u25C6"}
                    </span>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 600, color: A }}>{loop.name}</div>
                      <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{loop.description}</div>
                      <div style={{ fontSize: 9, color: AL, fontFamily: FM, marginTop: 3 }}>
                        {loop.type === "reinforcing" ? "Reinforcing" : "Balancing"} / {loop.lag_months}-month lag
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {/* ── State Context Bar ───────────────────────────────────────── */}
          <StateContextBar stateCode={state} mode="compact" />

          {/* ── Export + Ask Aradune ─────────────────────────────────────── */}
          <div style={{ display: "flex", gap: 10, marginTop: 16, flexWrap: "wrap" }}>
            <button onClick={exportCsv} style={{
              padding: "6px 14px", fontSize: 11, fontWeight: 600,
              color: A, background: WH, border: `1px solid ${BD}`,
              borderRadius: 5, cursor: "pointer",
            }}>
              Export CSV
            </button>
            <button onClick={askAradune} style={{
              padding: "6px 14px", fontSize: 11, fontWeight: 600,
              color: WH, background: cB, border: "none",
              borderRadius: 5, cursor: "pointer",
            }}>
              Ask Aradune
            </button>
          </div>
        </>
      )}

      {/* ── Empty state (no result yet, not loading) ───────────────────── */}
      {!result && !loading && !error && (
        <Card>
          <div style={{ textAlign: "center", padding: "30px 20px" }}>
            <div style={{ fontSize: 28, color: BD, marginBottom: 10 }}>{"\u25C9"}</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 6 }}>
              Configure and run a simulation
            </div>
            <div style={{ fontSize: 12, color: AL, maxWidth: 400, margin: "0 auto" }}>
              Add interventions above or select a preset, then click "Run Simulation" to model
              the downstream effects across enrollment, spending, access, workforce, and HCBS.
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}
