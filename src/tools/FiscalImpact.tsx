/**
 * Fiscal Impact Engine — Phase 4 Forecasting
 *
 * Rate increase % -> FMAP federal match -> UPL headroom check
 * -> biennial budget impact with state/federal cost split.
 *
 * Reads from the Aradune data lake (CMS-64, FMAP, enrollment, rates).
 */
import { useState, useMemo } from "react";
import {
  ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { STATE_NAMES, STATES_LIST } from "../data/states";
import { API_BASE } from "../lib/api";
import { useAradune } from "../context/AraduneContext";
import ChartActions from "../components/ChartActions";
import { useIsMobile } from "../design";

// -- Design tokens --
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

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

// -- Types --
interface Adjustment {
  service_type: string;
  increase_pct: number;
  description: string;
}

interface MonthRow {
  month: string;
  fiscal_year: number;
  baseline_expenditure: number;
  adjusted_expenditure: number;
  incremental: number;
  enrollment: number;
  fmap_rate: number;
  state_share: number;
  federal_share: number;
}

interface UPLCheck {
  service_type: string;
  current_medicaid_rate: number;
  medicare_benchmark: number;
  proposed_rate: number;
  headroom_pct: number;
  compliant: boolean;
  warning: string | null;
}

interface FYSummary {
  baseline: number;
  adjusted: number;
  incremental: number;
  state_share: number;
  federal_share: number;
  fmap_rate: number;
}

interface FiscalResult {
  state_code: string;
  analysis_date: string;
  fy_start: number;
  fy_end: number;
  rate_adjustments: { service_type: string; increase_pct: number; description: string }[];
  monthly_projections: MonthRow[];
  fy_summaries: Record<string, FYSummary>;
  biennial_total: { baseline: number; adjusted: number; incremental: number; state_share: number; federal_share: number; years: number };
  upl_checks: UPLCheck[];
  fmap_detail: { fiscal_year: number; fmap_rate: number; total_expenditure: number; incremental_cost: number; state_share: number; federal_share: number }[];
  warnings: string[];
  data_sources: string[];
}

const fmt = (n: number): string => {
  if (Math.abs(n) >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
};

const pct = (n: number): string => `${(n * 100).toFixed(1)}%`;

const SERVICE_TYPES = [
  "All Services", "E&M", "HCBS", "FFS Only", "Managed Care",
  "Behavioral Health", "Dental", "Pharmacy",
];

// -- Component --
export default function FiscalImpact() {
  const isMobile = useIsMobile();
  const { addReportSection } = useAradune();

  const [state, setState] = useState("FL");
  const currentYear = new Date().getFullYear();
  const [fyStart, setFyStart] = useState(currentYear + 1);
  const [fyEnd, setFyEnd] = useState(currentYear + 2);
  const [adjustments, setAdjustments] = useState<Adjustment[]>([
    { service_type: "All Services", increase_pct: 5, description: "" },
  ]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<FiscalResult | null>(null);

  const addAdjustment = () => {
    setAdjustments(prev => [...prev, { service_type: "E&M", increase_pct: 5, description: "" }]);
  };

  const removeAdjustment = (idx: number) => {
    setAdjustments(prev => prev.filter((_, i) => i !== idx));
  };

  const updateAdjustment = (idx: number, field: keyof Adjustment, value: string | number) => {
    setAdjustments(prev => prev.map((a, i) => i === idx ? { ...a, [field]: value } : a));
  };

  const runAnalysis = async () => {
    setLoading(true);
    setError("");
    setResult(null);

    const form = new FormData();
    form.append("state", state);
    form.append("fy_start", String(fyStart));
    form.append("fy_end", String(fyEnd));
    form.append("adjustments", JSON.stringify(adjustments));

    try {
      const res = await fetch(`${API_BASE}/api/forecast/fiscal-impact`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) {
        const body = await res.text();
        setError(body || `Error ${res.status}`);
        return;
      }
      const data = await res.json();
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Connection error");
    } finally {
      setLoading(false);
    }
  };

  const exportCSV = async () => {
    const form = new FormData();
    form.append("state", state);
    form.append("fy_start", String(fyStart));
    form.append("fy_end", String(fyEnd));
    form.append("adjustments", JSON.stringify(adjustments));

    const res = await fetch(`${API_BASE}/api/forecast/fiscal-impact/csv`, {
      method: "POST",
      body: form,
    });
    if (!res.ok) return;
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `fiscal_impact_${state}_FY${fyStart}-${fyEnd}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const chartData = useMemo(() => {
    if (!result) return [];
    return result.monthly_projections.map(m => ({
      month: m.month,
      baseline: m.baseline_expenditure / 1e6,
      adjusted: m.adjusted_expenditure / 1e6,
      incremental: m.incremental / 1e6,
      state_share: m.state_share / 1e6,
      federal_share: m.federal_share / 1e6,
    }));
  }, [result]);

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: isMobile ? "24px 12px" : "32px 20px" }}>
      <div style={{ marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 800, color: A, letterSpacing: -0.5 }}>
          Fiscal Impact Engine
        </h2>
        <p style={{ margin: "4px 0 0", fontSize: 13, color: AL }}>
          Model the budget impact of rate changes: FMAP federal match, UPL headroom, and biennial cost projections
        </p>
      </div>

      {/* -- Input Panel -- */}
      <Card accent={cB}>
        <CH title="Policy Scenario" sub="Select state, biennium, and rate adjustments" />
        <div style={{
          display: "grid",
          gridTemplateColumns: isMobile ? "1fr" : "1fr 1fr 1fr",
          gap: 16, marginBottom: 16,
        }}>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: AL, display: "block", marginBottom: 4 }}>State</label>
            <select
              value={state}
              onChange={e => setState(e.target.value)}
              style={{ width: "100%", padding: "8px 10px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 13, fontFamily: FB }}
            >
              {STATES_LIST.map(s => (
                <option key={s} value={s}>{STATE_NAMES[s] || s}</option>
              ))}
            </select>
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: AL, display: "block", marginBottom: 4 }}>Biennium Start (FY)</label>
            <input
              type="number"
              value={fyStart}
              onChange={e => { const v = parseInt(e.target.value); setFyStart(v); if (v >= fyEnd) setFyEnd(v + 1); }}
              style={{ width: "100%", padding: "8px 10px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 13, fontFamily: FM }}
            />
          </div>
          <div>
            <label style={{ fontSize: 11, fontWeight: 600, color: AL, display: "block", marginBottom: 4 }}>Biennium End (FY)</label>
            <input
              type="number"
              value={fyEnd}
              onChange={e => setFyEnd(parseInt(e.target.value))}
              min={fyStart + 1}
              style={{ width: "100%", padding: "8px 10px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 13, fontFamily: FM }}
            />
          </div>
        </div>

        <div style={{ marginBottom: 12 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <label style={{ fontSize: 12, fontWeight: 700, color: A }}>Rate Adjustments</label>
            <button onClick={addAdjustment} style={{
              padding: "4px 12px", borderRadius: 6, border: `1px solid ${BD}`,
              background: WH, fontSize: 11, fontWeight: 600, color: cB, cursor: "pointer",
            }}>+ Add</button>
          </div>

          {adjustments.map((adj, i) => (
            <div key={i} style={{
              display: "grid",
              gridTemplateColumns: isMobile ? "1fr" : "2fr 1fr auto",
              gap: 8, marginBottom: 8, alignItems: "end",
            }}>
              <div>
                <label style={{ fontSize: 10, color: AL }}>Service Type</label>
                <select
                  value={adj.service_type}
                  onChange={e => updateAdjustment(i, "service_type", e.target.value)}
                  style={{ width: "100%", padding: "6px 8px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 12, fontFamily: FB }}
                >
                  {SERVICE_TYPES.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: AL }}>Increase %</label>
                <input
                  type="number"
                  step={0.5}
                  value={adj.increase_pct}
                  onChange={e => updateAdjustment(i, "increase_pct", parseFloat(e.target.value) || 0)}
                  style={{ width: "100%", padding: "6px 8px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 12, fontFamily: FM }}
                />
              </div>
              {adjustments.length > 1 && (
                <button onClick={() => removeAdjustment(i)} style={{
                  padding: "6px 10px", borderRadius: 6, border: `1px solid ${BD}`,
                  background: WH, fontSize: 11, color: NEG, cursor: "pointer", marginBottom: 1,
                }}>Remove</button>
              )}
            </div>
          ))}
        </div>

        <button onClick={runAnalysis} disabled={loading} style={{
          padding: "10px 28px", borderRadius: 8, border: "none",
          background: loading ? BD : cB, color: WH, fontSize: 14,
          fontWeight: 700, fontFamily: FB, cursor: loading ? "default" : "pointer",
        }}>
          {loading ? "Calculating..." : "Calculate Fiscal Impact"}
        </button>
      </Card>

      {error && (
        <Card>
          <div style={{ color: NEG, fontSize: 13 }}>{error}</div>
        </Card>
      )}

      {/* -- Results -- */}
      {result && (
        <>
          {/* Biennial Summary */}
          <Card accent={cB}>
            <CH
              title={`Biennial Budget Impact: ${STATE_NAMES[result.state_code] || result.state_code}`}
              sub={`FY${result.fy_start} - FY${result.fy_end} | ${result.rate_adjustments.map(a => `${a.service_type} +${a.increase_pct}%`).join(", ")}`}
            />
            <div style={{
              display: "grid",
              gridTemplateColumns: isMobile ? "repeat(2, 1fr)" : "repeat(5, 1fr)",
              gap: 16, marginBottom: 16,
            }}>
              <Met label="Baseline" value={fmt(result.biennial_total.baseline)} mono />
              <Met label="After Adjustment" value={fmt(result.biennial_total.adjusted)} mono />
              <Met label="Incremental Cost" value={fmt(result.biennial_total.incremental)} color={NEG} mono />
              <Met label="State Share" value={fmt(result.biennial_total.state_share)} color={WARN} mono />
              <Met label="Federal Share" value={fmt(result.biennial_total.federal_share)} color={POS} mono />
            </div>
          </Card>

          {/* FY Breakdown */}
          <Card>
            <CH title="Fiscal Year Breakdown" sub="Cost split by year with FMAP rates" />
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${BD}` }}>
                    <th style={{ textAlign: "left", padding: "8px 10px", color: AL, fontWeight: 600 }}>FY</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>FMAP</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>Baseline</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>Adjusted</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>Incremental</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>State Share</th>
                    <th style={{ textAlign: "right", padding: "8px 10px", color: AL, fontWeight: 600 }}>Federal Share</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(result.fy_summaries).map(([fy, s]) => (
                    <tr key={fy} style={{ borderBottom: `1px solid ${BD}` }}>
                      <td style={{ padding: "8px 10px", fontWeight: 700, color: A }}>{fy}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right" }}>{pct(s.fmap_rate)}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right" }}>{fmt(s.baseline)}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right" }}>{fmt(s.adjusted)}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right", color: NEG }}>{fmt(s.incremental)}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right", color: WARN }}>{fmt(s.state_share)}</td>
                      <td style={{ padding: "8px 10px", textAlign: "right", color: POS }}>{fmt(s.federal_share)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>

          {/* UPL Compliance */}
          {result.upl_checks.length > 0 && (
            <Card accent={result.upl_checks.every(u => u.compliant) ? POS : NEG}>
              <CH title="UPL Headroom Analysis" sub="Upper Payment Limit compliance check against Medicare benchmarks" />
              {result.upl_checks.map((u, i) => (
                <div key={i} style={{
                  display: "grid",
                  gridTemplateColumns: isMobile ? "1fr" : "repeat(4, 1fr) auto",
                  gap: 12, padding: "10px 0",
                  borderBottom: i < result.upl_checks.length - 1 ? `1px solid ${BD}` : undefined,
                  alignItems: "center",
                }}>
                  <div>
                    <div style={{ fontSize: 10, color: AL }}>Service</div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: A }}>{u.service_type}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: AL }}>Current Rate</div>
                    <div style={{ fontSize: 13, fontFamily: FM }}>${u.current_medicaid_rate.toFixed(2)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: AL }}>Proposed Rate</div>
                    <div style={{ fontSize: 13, fontFamily: FM, color: u.compliant ? A : NEG }}>${u.proposed_rate.toFixed(2)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: AL }}>Medicare Benchmark</div>
                    <div style={{ fontSize: 13, fontFamily: FM }}>${u.medicare_benchmark.toFixed(2)}</div>
                  </div>
                  <div style={{
                    padding: "4px 12px", borderRadius: 20, fontSize: 11, fontWeight: 700,
                    background: u.compliant ? "#E8F5E9" : "#FFEBEE",
                    color: u.compliant ? POS : NEG,
                    textAlign: "center",
                  }}>
                    {u.compliant ? `${u.headroom_pct.toFixed(1)}% headroom` : "Exceeds UPL"}
                  </div>
                </div>
              ))}
              {result.upl_checks.filter(u => u.warning).map((u, i) => (
                <div key={i} style={{ marginTop: 8, padding: "8px 12px", borderRadius: 6, background: "#FFF8E1", fontSize: 12, color: WARN }}>
                  {u.warning}
                </div>
              ))}
            </Card>
          )}

          {/* Monthly Chart */}
          <Card>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 12 }}>
              <CH title="Monthly Expenditure Projection" sub="Baseline vs adjusted expenditure with cost split" />
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={exportCSV} style={{
                  padding: "6px 14px", borderRadius: 6, border: `1px solid ${BD}`,
                  background: WH, fontSize: 11, fontWeight: 600, color: cB, cursor: "pointer",
                }}>Export CSV</button>
                <button onClick={() => addReportSection({
                  id: `fiscal-${Date.now()}`, prompt: `Fiscal impact: ${state} FY${fyStart}-${fyEnd}`,
                  response: `Biennial incremental cost: ${fmt(result.biennial_total.incremental)}. State share: ${fmt(result.biennial_total.state_share)}, Federal share: ${fmt(result.biennial_total.federal_share)}.`,
                  queries: [], createdAt: new Date(),
                })} style={{
                  padding: "6px 14px", borderRadius: 6, border: `1px solid ${BD}`,
                  background: WH, fontSize: 11, fontWeight: 600, color: cB, cursor: "pointer",
                }}>+ Report</button>
              </div>
            </div>
            <ChartActions filename={`fiscal_impact_${state}`}>
              <ResponsiveContainer width="100%" height={360}>
                <ComposedChart data={chartData} margin={{ top: 5, right: 20, bottom: 5, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                  <XAxis dataKey="month" tick={{ fontSize: 10, fill: AL }} interval={2} angle={-30} textAnchor="end" height={50} />
                  <YAxis tick={{ fontSize: 10, fill: AL }} tickFormatter={(v: number) => `$${v.toFixed(0)}M`} />
                  <Tooltip
                    formatter={(v: number, name: string) => [`$${v.toFixed(1)}M`, name]}
                    contentStyle={{ fontSize: 11, fontFamily: FM, borderRadius: 6 }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <Bar dataKey="federal_share" name="Federal Share" fill={POS} opacity={0.5} stackId="split" />
                  <Bar dataKey="state_share" name="State Share" fill={WARN} opacity={0.5} stackId="split" />
                  <Line dataKey="baseline" name="Baseline" stroke={AL} strokeWidth={2} dot={false} strokeDasharray="5 5" />
                  <Line dataKey="adjusted" name="Adjusted" stroke={cB} strokeWidth={2} dot={false} />
                </ComposedChart>
              </ResponsiveContainer>
            </ChartActions>
          </Card>

          {/* Warnings & Data Sources */}
          {(result.warnings.length > 0 || result.data_sources.length > 0) && (
            <Card>
              {result.warnings.length > 0 && (
                <div style={{ marginBottom: 12 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: WARN, marginBottom: 6 }}>Caveats</div>
                  {result.warnings.map((w, i) => (
                    <div key={i} style={{ fontSize: 12, color: AL, padding: "3px 0", lineHeight: 1.5 }}>{w}</div>
                  ))}
                </div>
              )}
              {result.data_sources.length > 0 && (
                <div>
                  <div style={{ fontSize: 12, fontWeight: 700, color: AL, marginBottom: 6 }}>Data Sources</div>
                  {result.data_sources.map((s, i) => (
                    <div key={i} style={{ fontSize: 11, color: AL, fontFamily: FM, padding: "2px 0" }}>{s}</div>
                  ))}
                </div>
              )}
            </Card>
          )}
        </>
      )}
    </div>
  );
}
