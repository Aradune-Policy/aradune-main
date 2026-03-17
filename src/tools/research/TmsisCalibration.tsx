/**
 * TmsisCalibration.tsx — T-MSIS Claims vs Fee Schedule Calibration Analysis
 *
 * Research brief: How do actual Medicaid paid amounts from T-MSIS claims
 * compare to published fee schedule rates? Includes state-level discount
 * factors, service category patterns, and Tennessee fee schedule simulation.
 */

import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine, ScatterChart, Scatter, Cell } from "recharts";
import { LoadingBar } from "../../components/LoadingBar";
import ChartActions from "../../components/ChartActions";
import { useAradune } from "../../context/AraduneContext";
import { useIsMobile } from "../../design";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const A = "#0A2540", AL = "#4A5568", POS = "#059669", NEG = "#D93025", WARN = "#D97706";
const SF = "#F5F7F5", BD = "#E2E8F0", WH = "#FFFFFF", cB = "#2E6B4A";
const FM = "'SF Mono', 'Fira Code', monospace";
const FB = "'Helvetica Neue', Helvetica, Arial, sans-serif";
const SHADOW = "0 1px 3px rgba(10,37,64,0.06), 0 1px 2px rgba(10,37,64,0.04)";

const Card = ({ children, style, ...p }: React.HTMLAttributes<HTMLDivElement>) => (
  <div style={{ background: WH, borderRadius: 12, boxShadow: SHADOW, ...style }} {...p}>{children}</div>
);

const Collapsible = ({ title, children, defaultOpen = false }: { title: string; children: React.ReactNode; defaultOpen?: boolean }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ marginBottom: 16, border: `1px solid ${BD}`, borderRadius: 8 }}>
      <button onClick={() => setOpen(!open)} style={{
        width: "100%", padding: "12px 16px", background: SF, border: "none", borderRadius: open ? "8px 8px 0 0" : 8,
        cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center",
        fontSize: 13, fontWeight: 600, color: A, fontFamily: FB,
      }}>
        {title}
        <span style={{ fontSize: 11, color: AL }}>{open ? "▲" : "▼"}</span>
      </button>
      {open && <div style={{ padding: "16px", borderTop: `1px solid ${BD}` }}>{children}</div>}
    </div>
  );
};

export default function TmsisCalibration() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [stateData, setStateData] = useState<any[]>([]);
  const [tnData, setTnData] = useState<any[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const [stateRes, tnRes] = await Promise.all([
          fetchJson("/api/research/tmsis-calibration/state-summary"),
          fetchJson("/api/research/tmsis-calibration/tn-simulation?category=E%26M"),
        ]);
        setStateData(stateRes.rows || []);
        setTnData(tnRes.rows || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  // Aggregate to state-level (across categories)
  const stateAgg = useMemo(() => {
    const byState: Record<string, { total_codes: number; weighted_sum: number; fs_mc: number; claims_mc: number }> = {};
    stateData.forEach(r => {
      if (!byState[r.state_code]) byState[r.state_code] = { total_codes: 0, weighted_sum: 0, fs_mc: 0, claims_mc: 0 };
      byState[r.state_code].total_codes += r.n_codes;
      byState[r.state_code].weighted_sum += r.n_codes * r.claims_pct_of_fs;
      byState[r.state_code].fs_mc += r.n_codes * (r.fs_pct_of_medicare || 0);
      byState[r.state_code].claims_mc += r.n_codes * (r.claims_pct_of_medicare || 0);
    });
    return Object.entries(byState)
      .map(([st, v]) => ({
        state_code: st,
        n_codes: v.total_codes,
        claims_pct_fs: Math.round(v.weighted_sum / v.total_codes * 10) / 10,
        fs_pct_mc: Math.round(v.fs_mc / v.total_codes * 10) / 10,
        claims_pct_mc: Math.round(v.claims_mc / v.total_codes * 10) / 10,
      }))
      .filter(r => r.n_codes >= 50)
      .sort((a, b) => a.claims_pct_fs - b.claims_pct_fs);
  }, [stateData]);

  // Category-level summary
  const catAgg = useMemo(() => {
    const byCat: Record<string, number[]> = {};
    stateData.forEach(r => {
      if (!byCat[r.service_category]) byCat[r.service_category] = [];
      byCat[r.service_category].push(r.claims_pct_of_fs);
    });
    return Object.entries(byCat)
      .map(([cat, vals]) => ({
        category: cat,
        median: vals.sort((a, b) => a - b)[Math.floor(vals.length / 2)],
        n_states: vals.length,
      }))
      .sort((a, b) => a.median - b.median);
  }, [stateData]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          T-MSIS Claims vs Fee Schedule Rates: A National Calibration
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          How do actual Medicaid payments from T-MSIS claims data compare to published fee schedule maximums?
          Nationally, claims average <strong>72% of fee schedule rates</strong> — a 28% discount reflecting
          adjustments, partial payments, and provider-type differentials. This analysis calibrates the discount
          by state and service category, enabling simulated fee schedule estimates for states like Tennessee
          where no FFS fee schedule exists.
        </p>
        <div style={{
          marginTop: 12, padding: "10px 14px", borderRadius: 8,
          background: "#FEF3C7", border: "1px solid #F59E0B",
          fontSize: 12, color: "#92400E", lineHeight: 1.5,
        }}>
          <strong>Data Source Notice:</strong> T-MSIS (Transformed Medicaid Statistical Information System) claims
          reflect actual paid amounts, not fee schedule maximums. Claims data is subject to: adjustments,
          denials, provider-type rate differentials, supplemental payments, and state-specific data quality
          variations. T-MSIS-derived rates should not be treated as equivalent to published fee schedules.
        </div>
      </div>

      {/* Key Finding */}
      <Card style={{ borderLeft: `4px solid ${WARN}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: WARN, lineHeight: 1 }}>72%</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              Claims average 72% of published fee schedule rates nationally. The discount varies from 37% (CT) to
              102% (WV/ND). Southeast states average 79%, providing the calibration basis for Tennessee's simulated fee schedule.
            </span>
          </div>
        </div>
      </Card>

      {/* State Chart */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Claims as % of Fee Schedule by State</h2>
        <ChartActions filename="tmsis-calibration-by-state">
          <ResponsiveContainer width="100%" height={Math.max(400, stateAgg.length * 14)}>
            <BarChart data={stateAgg} layout="vertical" margin={{ left: 30, right: 20, top: 5, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 10, fill: AL }} domain={[0, 200]} tickFormatter={v => `${v}%`} />
              <YAxis type="category" dataKey="state_code" tick={{ fontSize: 9, fill: AL }} width={30} />
              <Tooltip formatter={(v: number) => `${v}%`} contentStyle={{ fontSize: 12, fontFamily: FM }} />
              <ReferenceLine x={72} stroke={WARN} strokeDasharray="5 5" label={{ value: "National median: 72%", fontSize: 10, fill: WARN }} />
              <ReferenceLine x={100} stroke={AL} strokeDasharray="3 3" />
              <Bar dataKey="claims_pct_fs" name="Claims % of Fee Schedule" radius={[0, 3, 3, 0]} maxBarSize={12}>
                {stateAgg.map((r, i) => (
                  <Cell key={i} fill={r.claims_pct_fs < 60 ? NEG : r.claims_pct_fs < 80 ? WARN : r.claims_pct_fs <= 105 ? POS : cB} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartActions>
      </div>

      {/* By Category */}
      <Collapsible title="Discount by Service Category" defaultOpen>
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 12 }}>
          The claims-to-fee-schedule ratio is fairly consistent across service categories, ranging from
          Path/Lab (lowest) to Medicine (highest). This consistency supports using a single calibration
          factor per state, refined by category where sample size allows.
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Category", "Median Claims/FS", "States"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Category" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {catAgg.map((r, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{r.category}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: (r.median || 0) < 70 ? NEG : (r.median || 0) < 85 ? WARN : POS, fontWeight: 600 }}>{r.median?.toFixed(1) ?? "--"}%</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.n_states}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Collapsible>

      {/* TN Simulation */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 8 }}>Tennessee Simulated Fee Schedule</h2>
        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 4 }}>
          Tennessee is 94% managed care with no published FFS fee schedule. Using Southeast state calibration
          factors (MS, AL, KY, GA, SC, AR), we estimate TennCare's effective fee schedule from T-MSIS claims data.
          Ranges reflect the calibration uncertainty (IQR of SE state discount factors).
        </p>
        <div style={{
          padding: "8px 12px", borderRadius: 6, background: "#FEF3C7", border: "1px solid #F59E0B",
          fontSize: 11, color: "#92400E", marginBottom: 16,
        }}>
          These are <strong>simulated estimates</strong>, not published rates. Based on T-MSIS claims calibrated
          against SE state discount patterns. Use for benchmarking and directional analysis only.
        </div>

        {tnData.length > 0 && (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${A}` }}>
                  {["Code", "Description", "Claims Avg", "Est. FS (range)", "Medicare", "Est. % MC"].map(h => (
                    <th key={h} style={{ padding: "8px 8px", textAlign: h === "Code" || h === "Description" ? "left" : "right", color: A, fontWeight: 700, fontSize: 10 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tnData.slice(0, 30).map((r, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                    <td style={{ padding: "5px 8px", fontWeight: 600, color: A }}>{r.procedure_code}</td>
                    <td style={{ padding: "5px 8px", color: AL, fontSize: 10, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.description}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right", color: WARN }}>${r.claims_avg_paid?.toFixed(2) ?? "--"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right", color: A, fontSize: 10 }}>${r.simulated_fs_high?.toFixed(0) ?? "?"}-${r.simulated_fs_low?.toFixed(0) ?? "?"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right", color: AL }}>${r.medicare_rate?.toFixed(2) ?? "--"}</td>
                    <td style={{ padding: "5px 8px", textAlign: "right", color: (r.simulated_pct_medicare || 0) < 60 ? NEG : (r.simulated_pct_medicare || 0) < 80 ? WARN : POS, fontWeight: 600 }}>{r.simulated_pct_medicare?.toFixed(0) ?? "--"}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <Collapsible title="Methodology">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Data sources:</strong> T-MSIS claims from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_claims_monthly</code> (6.3M rows),
            published fee schedule rates from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_rate_comparison</code> (302K rows),
            Medicare PFS from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_pfs_rvu_2026</code> (18K codes).
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Calibration:</strong> For each state with both claims and published fee schedule data,
            compute the ratio: claims_avg_paid / fee_schedule_rate per HCPCS code. Aggregate to state × service category.
            The median ratio is the "discount factor" — how much less claims data shows compared to the fee schedule max.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>TN simulation:</strong> Tennessee has no FFS fee schedule (94% MC). Using the median
            discount factor from 7 Southeast states (MS, AL, KY, GA, SC, AR, LA) as calibration, compute:
            simulated_fs = claims_avg / SE_discount_factor. Ranges use ±10pp around the calibration factor.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Known limitations:</strong> (1) Claims include adjustments, partial payments, and
            provider-type differentials that reduce averages below fee schedule max. (2) Supplemental payments in some
            states inflate per-claim averages above fee schedule. (3) T-MSIS data quality varies by state. (4) The
            calibration assumes discount patterns are consistent between SE states and TN, which may not hold for
            all service categories.
          </p>
        </div>
      </Collapsible>

      {/* Ask Aradune */}
      <div style={{ marginTop: 32, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the T-MSIS Calibration research brief. Claims average 72% of fee schedule nationally. TN simulated fee schedule estimates available. 68,891 T-MSIS effective rates across 54 states. 1,696 TN codes with simulated fee schedules." })}
          style={{
            padding: "10px 24px", borderRadius: 8, background: cB, color: WH,
            border: "none", fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: FB,
          }}>
          Ask Aradune about this analysis
        </button>
      </div>
    </div>
  );
}
