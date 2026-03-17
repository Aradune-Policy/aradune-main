/**
 * NetworkAdequacy.tsx — Network Adequacy: Access Designation Scoring
 *
 * Research brief combining primary care, dental, and mental health HPSA
 * designations with MUA/MUP and FQHC coverage to map access gaps.
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

export default function NetworkAdequacy() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [data, setData] = useState<any[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetchJson("/api/research/network-adequacy/composite");
        setData(res.rows || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const chartData = useMemo(() =>
    data.filter(r => r.enrollment > 50000).map(r => ({
      name: r.state_code,
      pc: r.pc_hpsa_count,
      dental: r.dental_hpsa_count,
      mh: r.mh_hpsa_count,
      shortage: r.shortage_per_100k_enrollees,
    })).sort((a, b) => b.shortage - a.shortage).slice(0, 30),
  [data]);

  const totals = useMemo(() => {
    if (!data.length) return null;
    return {
      pc: data.reduce((s, r) => s + (r.pc_hpsa_count || 0), 0),
      dental: data.reduce((s, r) => s + (r.dental_hpsa_count || 0), 0),
      mh: data.reduce((s, r) => s + (r.mh_hpsa_count || 0), 0),
      mua: data.reduce((s, r) => s + (r.mua_count || 0), 0),
    };
  }, [data]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          Network Adequacy: Mapping Access Gaps Across Medicaid
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          CMS network adequacy standards require managed care plans to demonstrate sufficient provider
          access across specialties and geographies. This analysis combines HRSA shortage designations
          (primary care, dental, mental health), medically underserved areas, and FQHC safety net
          coverage to create a comprehensive access vulnerability map for all 54 Medicaid jurisdictions.
        </p>
      </div>

      {/* Key Finding */}
      {totals && (
        <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
          <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
            <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 12 }}>National Totals</div>
            <div style={{ display: "grid", gridTemplateColumns: isMobile ? "1fr 1fr" : "repeat(4, 1fr)", gap: 16 }}>
              {[
                { label: "Primary Care HPSAs", val: totals.pc.toLocaleString(), color: NEG },
                { label: "Dental HPSAs", val: totals.dental.toLocaleString(), color: WARN },
                { label: "Mental Health HPSAs", val: totals.mh.toLocaleString(), color: "#6366F1" },
                { label: "Medically Underserved", val: totals.mua.toLocaleString(), color: AL },
              ].map(m => (
                <div key={m.label}>
                  <div style={{ fontSize: 24, fontWeight: 700, color: m.color, fontFamily: FM }}>{m.val}</div>
                  <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>{m.label}</div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      )}

      {/* Chart */}
      <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Shortage Designations per 100K Medicaid Enrollees</h2>
      <ChartActions filename="network-adequacy-by-state">
        <ResponsiveContainer width="100%" height={Math.max(400, chartData.length * 16)}>
          <BarChart data={chartData} layout="vertical" margin={{ left: 30, right: 20, top: 5, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={BD} horizontal={false} />
            <XAxis type="number" tick={{ fontSize: 10, fill: AL }} />
            <YAxis type="category" dataKey="name" tick={{ fontSize: 9, fill: AL }} width={30} />
            <Tooltip contentStyle={{ fontSize: 12, fontFamily: FM }} />
            <Legend wrapperStyle={{ fontSize: 10 }} />
            <Bar dataKey="pc" name="Primary Care" fill={NEG} stackId="a" maxBarSize={12} />
            <Bar dataKey="dental" name="Dental" fill={WARN} stackId="a" maxBarSize={12} />
            <Bar dataKey="mh" name="Mental Health" fill="#6366F1" stackId="a" maxBarSize={12} />
          </BarChart>
        </ResponsiveContainer>
      </ChartActions>

      {/* Table */}
      <Collapsible title="Full State Data" defaultOpen>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["State", "PC HPSAs", "Dental", "MH", "MUA/MUP", "Enrollment", "Shortage/100K"].map(h => (
                  <th key={h} style={{ padding: "6px 8px", textAlign: h === "State" ? "left" : "right", color: A, fontWeight: 700, fontSize: 10 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.filter(r => r.enrollment > 0).sort((a: any, b: any) => b.shortage_per_100k_enrollees - a.shortage_per_100k_enrollees).map((r, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                  <td style={{ padding: "5px 8px", fontWeight: 600, color: A }}>{r.state_code}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", color: r.pc_hpsa_count > 100 ? NEG : AL }}>{r.pc_hpsa_count}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", color: r.dental_hpsa_count > 50 ? WARN : AL }}>{r.dental_hpsa_count}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", color: AL }}>{r.mh_hpsa_count}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", color: AL }}>{r.mua_count}</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", color: AL }}>{(r.enrollment / 1e6).toFixed(2)}M</td>
                  <td style={{ padding: "5px 8px", textAlign: "right", fontWeight: 600, color: r.shortage_per_100k_enrollees > 200 ? NEG : r.shortage_per_100k_enrollees > 100 ? WARN : POS }}>{r.shortage_per_100k_enrollees}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Collapsible>

      <Collapsible title="Methodology">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Data sources:</strong> HRSA primary care HPSAs (fact_hpsa), HRSA dental HPSAs (fact_dental_hpsa),
            HRSA mental health HPSAs (fact_mental_health_hpsa), HRSA MUA/MUP (fact_mua_mup), FQHC service delivery sites (fact_fqhc_sites_v2),
            CMS Medicaid enrollment (fact_enrollment).
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Shortage per 100K:</strong> (PC HPSAs + Dental HPSAs + MH HPSAs) x 100,000 / Medicaid enrollment.
            Higher values indicate more shortage designations relative to the Medicaid population served.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Limitations:</strong> HPSA designations reflect provider-to-population ratios for the general population,
            not specifically Medicaid enrollees. Managed care network directories (which would show actual provider availability for Medicaid
            beneficiaries) are not publicly available in bulk. This analysis approximates access gaps using federal designation data.
          </p>
        </div>
      </Collapsible>

      <div style={{ marginTop: 32, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Network Adequacy research brief. Shows primary care, dental, and MH HPSA designations + MUA/MUP + FQHC coverage per state relative to Medicaid enrollment." })}
          style={{ padding: "10px 24px", borderRadius: 8, background: cB, color: WH, border: "none", fontSize: 13, fontWeight: 600, cursor: "pointer", fontFamily: FB }}>
          Ask Aradune about this analysis
        </button>
      </div>
    </div>
  );
}
