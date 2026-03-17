/**
 * MepsAnalysis.tsx — MEPS Household Component: Expenditure & Utilization by Insurance Type
 *
 * Individual-level analysis from the Medical Expenditure Panel Survey (2022).
 * Compares Medicaid, Medicare, private, and uninsured populations on spending,
 * utilization, and out-of-pocket burden.
 */

import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from "recharts";
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
      <button onClick={() => setOpen(!open)} style={{ width: "100%", padding: "12px 16px", background: SF, border: "none", borderRadius: open ? "8px 8px 0 0" : 8, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13, fontWeight: 600, color: A, fontFamily: FB }}>
        {title}<span style={{ fontSize: 11, color: AL }}>{open ? "▲" : "▼"}</span>
      </button>
      {open && <div style={{ padding: 16, borderTop: `1px solid ${BD}` }}>{children}</div>}
    </div>
  );
};

const fmt = (n: number) => n >= 1000 ? `$${(n / 1000).toFixed(1)}K` : `$${n.toFixed(0)}`;

const COVERAGE_COLORS: Record<string, string> = {
  "Dual (Medicaid+Medicare)": "#9333EA",
  "Medicaid": cB,
  "Medicare Only": "#3A7CC4",
  "Private/Other": "#6366F1",
  "Uninsured (6+ months)": NEG,
};

export default function MepsAnalysis() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expData, setExpData] = useState<any[]>([]);
  const [povData, setPovData] = useState<any[]>([]);
  const [profileData, setProfileData] = useState<any[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const [exp, pov, prof] = await Promise.all([
          fetchJson("/api/research/meps/expenditure-by-insurance"),
          fetchJson("/api/research/meps/utilization-by-poverty"),
          fetchJson("/api/research/meps/medicaid-profile"),
        ]);
        setExpData(exp.rows || []);
        setPovData(pov.rows || []);
        setProfileData(prof.rows || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const chartData = useMemo(() =>
    expData.map(r => ({ ...r, name: r.coverage_group, exp: r.mean_total_exp, oop: r.mean_oop })),
  [expData]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Medicaid Expenditure and Utilization: Individual-Level Evidence from MEPS
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          The Medical Expenditure Panel Survey (MEPS) provides individual-level data on healthcare spending,
          utilization, and insurance coverage for 22,431 respondents in 2022. This analysis compares Medicaid
          enrollees to other coverage groups on total expenditure, out-of-pocket burden, office visits, ER use,
          and prescription fills — revealing how coverage type shapes both the level and composition of healthcare spending.
        </p>
      </div>

      {/* Key Findings */}
      <Card style={{ borderLeft: `4px solid ${cB}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 12 }}>Key Findings</div>
          {expData.length > 0 && (() => {
            const medicaid = expData.find(r => r.coverage_group === "Medicaid");
            const priv = expData.find(r => r.coverage_group === "Private/Other");
            const unins = expData.find(r => r.coverage_group?.includes("Uninsured"));
            return (
              <div style={{ display: "grid", gap: 12 }}>
                {medicaid && priv && (
                  <div style={{ fontSize: 14, color: A, lineHeight: 1.6 }}>
                    Medicaid enrollees average <strong>${medicaid.mean_total_exp?.toLocaleString() ?? "N/A"}</strong> in total annual expenditure
                    vs <strong>${priv.mean_total_exp?.toLocaleString() ?? "N/A"}</strong> for privately insured — but pay only{" "}
                    <strong style={{ color: POS }}>${medicaid.mean_oop?.toLocaleString() ?? "N/A"}</strong> out of pocket
                    ({medicaid.oop_pct_of_total?.toFixed(0) ?? "N/A"}% of total) compared to{" "}
                    <strong>${priv.mean_oop?.toLocaleString() ?? "N/A"}</strong> ({priv.oop_pct_of_total?.toFixed(0) ?? "N/A"}%) for private coverage.
                  </div>
                )}
                {unins && (
                  <div style={{ fontSize: 13, color: AL, lineHeight: 1.6 }}>
                    Uninsured individuals average <strong style={{ color: NEG }}>${unins.mean_total_exp?.toLocaleString()}</strong> — dramatically lower
                    spending reflecting deferred care, not better health. They pay {unins.oop_pct_of_total?.toFixed(0)}% out of pocket.
                  </div>
                )}
              </div>
            );
          })()}
        </div>
      </Card>

      {/* Expenditure Chart */}
      <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Mean Annual Expenditure by Coverage Type</h2>
      <ChartActions filename="meps-expenditure-by-coverage">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData} margin={{ left: 10, right: 10, top: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={BD} vertical={false} />
            <XAxis dataKey="name" tick={{ fontSize: isMobile ? 8 : 10, fill: AL }} interval={0} angle={isMobile ? -30 : 0} textAnchor={isMobile ? "end" : "middle"} height={isMobile ? 60 : 30} />
            <YAxis tick={{ fontSize: 10, fill: AL }} tickFormatter={v => fmt(v)} />
            <Tooltip formatter={(v: number) => `$${v.toLocaleString()}`} contentStyle={{ fontSize: 12, fontFamily: FM }} />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="exp" name="Total Expenditure" radius={[4, 4, 0, 0]} maxBarSize={50}>
              {chartData.map((r, i) => (
                <rect key={i} fill={COVERAGE_COLORS[r.name] || AL} />
              ))}
            </Bar>
            <Bar dataKey="oop" name="Out-of-Pocket" radius={[4, 4, 0, 0]} fill={WARN} maxBarSize={50} />
          </BarChart>
        </ResponsiveContainer>
      </ChartActions>

      {/* Expenditure Table */}
      <div style={{ overflowX: "auto", marginTop: 24 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
          <thead>
            <tr style={{ borderBottom: `2px solid ${A}` }}>
              {["Coverage", "N", "Total Exp", "OOP", "OOP %", "Office Visits", "ER Visits", "Rx Fills"].map(h => (
                <th key={h} style={{ padding: "8px 8px", textAlign: h === "Coverage" ? "left" : "right", color: A, fontWeight: 700, fontSize: 10 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {expData.map((r, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: r.coverage_group === "Medicaid" ? `${cB}08` : "transparent" }}>
                <td style={{ padding: "6px 8px", fontWeight: 600, color: COVERAGE_COLORS[r.coverage_group] || A, fontSize: 11 }}>{r.coverage_group}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.respondents?.toLocaleString()}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: A, fontWeight: 600 }}>${r.mean_total_exp?.toLocaleString()}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: WARN }}>${r.mean_oop?.toLocaleString()}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.oop_pct_of_total?.toFixed(0)}%</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.mean_office_visits?.toFixed(1)}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: (r.mean_er_visits || 0) > 0.3 ? NEG : AL }}>{r.mean_er_visits?.toFixed(2)}</td>
                <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.mean_rx_fills?.toFixed(1)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Poverty Table */}
      <Collapsible title="Utilization by Poverty Level (FPL)" defaultOpen>
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 12 }}>
          How does healthcare utilization vary across income levels — and how does Medicaid coverage
          correlate with the poverty distribution?
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Poverty Group", "N", "Total Exp", "OOP", "Office", "ER", "Rx", "% Medicaid"].map(h => (
                  <th key={h} style={{ padding: "8px 8px", textAlign: h === "Poverty Group" ? "left" : "right", color: A, fontWeight: 700, fontSize: 10 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {povData.map((r, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}`, background: r.poverty_group?.includes("expansion") ? `${cB}08` : "transparent" }}>
                  <td style={{ padding: "6px 8px", fontWeight: 600, color: A, fontSize: 11 }}>{r.poverty_group}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.respondents?.toLocaleString()}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: A }}>${r.mean_total_exp?.toLocaleString()}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: WARN }}>${r.mean_oop?.toLocaleString()}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.mean_office_visits?.toFixed(1)}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.mean_er_visits?.toFixed(2)}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: AL }}>{r.mean_rx_fills?.toFixed(1)}</td>
                  <td style={{ padding: "6px 8px", textAlign: "right", color: cB, fontWeight: 600 }}>{r.pct_with_medicaid?.toFixed(0)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Collapsible>

      <Collapsible title="Methodology & Data Source">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Data source:</strong> AHRQ Medical Expenditure Panel Survey, Household Component,
            Full Year Consolidated File HC-243 (2022). 22,431 respondents representing the U.S. civilian
            noninstitutionalized population. Person-level weights applied for national estimates.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Coverage classification:</strong> Based on months of coverage reported in 2022.
            "Medicaid" = any months with Medicaid coverage. "Dual" = both Medicaid and Medicare months.
            "Uninsured" = 6+ months without any coverage. "Private/Other" = all remaining.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Limitations:</strong> (1) Self-reported insurance status may have recall errors.
            (2) Expenditure includes all payers, not just the respondent's primary coverage.
            (3) Institutional populations (nursing homes, prisons) excluded from MEPS.
            (4) Single year (2022) — pre-unwinding enrollment composition.
          </p>
        </div>
      </Collapsible>

      <div style={{ marginTop: 32, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the MEPS analysis. 22,431 respondents (2022). Comparing expenditure, utilization, and OOP burden across Medicaid, Medicare, private, dual, and uninsured populations. Data from AHRQ MEPS HC-243." })}
          style={{ padding: "10px 24px", borderRadius: 8, background: cB, color: WH, border: "none", fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: FB }}>
          Ask Aradune about this analysis
        </button>
      </div>
    </div>
  );
}
