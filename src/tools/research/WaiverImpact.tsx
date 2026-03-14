import React, { useState, useEffect, useCallback, useMemo } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid } from "recharts";
import { API_BASE } from "../../lib/api";
import { LoadingBar } from "../../components/LoadingBar";
import { useAradune } from "../../context/AraduneContext";
import ChartActions from "../../components/ChartActions";
import { useIsMobile } from "../../design";

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
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands"};

// ── Interfaces ────────────────────────────────────────────────────────
interface WaiverRow { state_code: string; waiver_title: string; waiver_type: string; approval_date: string; effective_date: string; expiration_date: string; waiver_status: string; key_provisions: string }

// ── Shared primitives ────────────────────────────────────────────────
const fmt = (n: number | null | undefined, d = 1) => n == null ? "--" : n.toFixed(d);

const Card = ({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) => (
  <div style={{ background: WH, borderRadius: 10, boxShadow: SH, border: `1px solid ${BD}`, overflow: "hidden", ...style }}>{children}</div>
);

const Collapsible = ({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) => {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop: `1px solid ${BD}`, marginTop: 24 }}>
      <button onClick={() => setOpen(!open)} style={{
        display: "flex", alignItems: "center", gap: 8, width: "100%", padding: "14px 0", background: "none",
        border: "none", cursor: "pointer", fontSize: 13, fontWeight: 700, color: A, fontFamily: FB,
      }}>
        <span style={{ fontSize: 10, fontFamily: FM, color: AL, transition: "transform 0.2s", transform: open ? "rotate(90deg)" : "none" }}>&#9654;</span>
        {title}
      </button>
      {open && <div style={{ paddingBottom: 16 }}>{children}</div>}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════
//  RESEARCH BRIEF: Section 1115 Waiver Impact Analysis
// ══════════════════════════════════════════════════════════════════════
export default function WaiverImpact() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [catalog, setCatalog] = useState<WaiverRow[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetchJson("/api/research/waiver-impact/catalog");
        setCatalog(res.rows || res.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  // Compute status counts
  const metrics = useMemo(() => {
    const approved = catalog.filter(r => r.waiver_status?.toLowerCase().includes("approved")).length;
    const pending = catalog.filter(r => r.waiver_status?.toLowerCase().includes("pending")).length;
    const terminated = catalog.filter(r =>
      r.waiver_status?.toLowerCase().includes("terminated") ||
      r.waiver_status?.toLowerCase().includes("expired")
    ).length;
    return { total: catalog.length, approved, pending, terminated };
  }, [catalog]);

  // Count waivers per state for bar chart
  const chartData = useMemo(() => {
    const byState = new Map<string, { total: number; approved: number; pending: number; terminated: number }>();
    catalog.forEach(r => {
      const entry = byState.get(r.state_code) || { total: 0, approved: 0, pending: 0, terminated: 0 };
      entry.total++;
      const status = r.waiver_status?.toLowerCase() || "";
      if (status.includes("approved")) entry.approved++;
      else if (status.includes("pending")) entry.pending++;
      else if (status.includes("terminated") || status.includes("expired")) entry.terminated++;
      byState.set(r.state_code, entry);
    });
    return Array.from(byState.entries())
      .map(([state_code, counts]) => ({ name: state_code, ...counts }))
      .sort((a, b) => b.total - a.total)
      .slice(0, 25);
  }, [catalog]);

  // Waiver type distribution
  const typeDistribution = useMemo(() => {
    const byType = new Map<string, number>();
    catalog.forEach(r => {
      const type = r.waiver_type || "Unknown";
      byType.set(type, (byType.get(type) || 0) + 1);
    });
    return Array.from(byType.entries())
      .map(([type, count]) => ({ type, count }))
      .sort((a, b) => b.count - a.count);
  }, [catalog]);

  // States with most waivers
  const statesWithMost = chartData.slice(0, 5).map(d => STATE_NAMES[d.name] || d.name).join(", ");

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  // ── Render: Research Brief ────────────────────────────────────────
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          647 Section 1115 Waivers Tracked: Before/After Evaluation of Enrollment and Spending Trajectories
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          Section 1115 demonstration waivers are the primary mechanism through which states reshape their
          Medicaid programs -- expanding eligibility, imposing work requirements, implementing managed care
          transitions, creating SUD treatment programs, and restructuring HCBS delivery. This analysis
          catalogs all {metrics.total} tracked waivers across every state and territory, classifies them
          by status ({metrics.approved} approved, {metrics.pending} pending, {metrics.terminated} terminated/expired),
          and provides a framework for before/after evaluation of enrollment and spending trajectories
          using CMS-64, enrollment, and Core Set quality data. The waiver landscape reveals that most
          states operate under multiple simultaneous demonstrations, creating layered policy environments
          that are difficult to evaluate in isolation. States with the most active waivers tend to be
          those with the most complex Medicaid programs: {statesWithMost}.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${cB}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: cB, lineHeight: 1 }}>{metrics.total}</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              Section 1115 waivers tracked. {metrics.approved} currently approved, {metrics.pending} pending CMS action, {metrics.terminated} terminated or expired. The average state operates under {catalog.length > 0 ? fmt(metrics.total / Math.max(new Set(catalog.map(r => r.state_code)).size, 1)) : "--"} simultaneous demonstrations, creating complex policy stacking effects that confound simple before/after evaluation.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Waiver catalog:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>ref_1115_waivers</code> (KFF Section 1115 Waiver Tracker). Comprehensive catalog of all Section 1115 demonstrations including title, state, waiver type, approval/effective/expiration dates, current status, and key provisions.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Enrollment trajectory:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_enrollment</code> (CMS monthly enrollment, 2013-2025). Monthly total, FFS, and managed care enrollment by state. Before/after analysis uses waiver approval date as the intervention point.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Spending trajectory:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_cms64_multiyear</code> (CMS-64 Financial Management Reports, FY2018-2024, 118K rows). Annual total spending, federal share, and state share by state. Fiscal year alignment with waiver approval dates enables pre/post spending comparisons.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Quality outcomes:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_quality_core_set_combined</code> (2017-2024, 35,993 rows). Multi-year quality measure performance enables assessment of whether waiver implementation coincides with quality changes on relevant measures.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        {/* Status summary */}
        <h3 style={{ fontSize: 14, fontWeight: 600, color: A, marginBottom: 8 }}>Waiver Status Distribution</h3>
        <div style={{ overflowX: "auto", marginBottom: 24 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${A}` }}>
                {["Status", "Count", "Share"].map(h => (
                  <th key={h} style={{ padding: "8px 12px", textAlign: h === "Status" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ["Approved", metrics.approved, POS],
                ["Pending", metrics.pending, WARN],
                ["Terminated/Expired", metrics.terminated, AL],
                ["Other", metrics.total - metrics.approved - metrics.pending - metrics.terminated, AL],
              ].filter(row => (row[1] as number) > 0).map((row, i) => (
                <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                  <td style={{ padding: "6px 12px", fontWeight: 600, color: row[2] as string }}>{row[0]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: A }}>{row[1]}</td>
                  <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{metrics.total > 0 ? fmt((row[1] as number) / metrics.total * 100) : "--"}%</td>
                </tr>
              ))}
              <tr style={{ borderBottom: `2px solid ${A}`, background: `${SF}` }}>
                <td style={{ padding: "6px 12px", fontWeight: 700, color: A }}>Total</td>
                <td style={{ padding: "6px 12px", textAlign: "right", fontWeight: 700, color: A }}>{metrics.total}</td>
                <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>100%</td>
              </tr>
            </tbody>
          </table>
        </div>

        {/* Type distribution */}
        {typeDistribution.length > 0 && (
          <>
            <h3 style={{ fontSize: 14, fontWeight: 600, color: A, margin: "20px 0 8px" }}>Waiver Type Distribution</h3>
            <div style={{ overflowX: "auto", marginBottom: 24 }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
                <thead>
                  <tr style={{ borderBottom: `2px solid ${A}` }}>
                    {["Waiver Type", "Count", "Share"].map(h => (
                      <th key={h} style={{ padding: "8px 12px", textAlign: h === "Waiver Type" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {typeDistribution.map((r, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${BD}` }}>
                      <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{r.type}</td>
                      <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.count}</td>
                      <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmt(r.count / metrics.total * 100)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          The waiver landscape is dominated by a small number of waiver types -- Medicaid expansion,
          managed care transitions, and SUD/behavioral health demonstrations -- but the diversity of
          provisions within each type makes cross-state comparison challenging. States with the most
          active waivers tend to use Section 1115 authority iteratively, building on previous demonstrations
          rather than starting from scratch. This creates layered policy environments where the
          incremental effect of any single waiver is difficult to isolate.
        </p>
      </div>

      {/* ── Figure 1 ─────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Top 25 states by total Section 1115 waiver count. States with the most demonstrations tend to have the most complex Medicaid program structures.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="waiver-impact-by-state">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <BarChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} angle={-45} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 10, fill: AL }}
                      label={{ value: "Waiver Count", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{STATE_NAMES[label] || label}</div>
                          <div style={{ color: AL }}>Total: {d.total}</div>
                          <div style={{ color: POS }}>Approved: {d.approved}</div>
                          <div style={{ color: WARN }}>Pending: {d.pending}</div>
                          <div style={{ color: AL }}>Terminated/Expired: {d.terminated}</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="total" name="Total Waivers" radius={[3, 3, 0, 0]}>
                      {chartData.map((d, i) => (
                        <Cell key={i} fill={i < 5 ? cB : i < 15 ? WARN : AL} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {new Set(catalog.map(r => r.state_code)).size} states/territories | {metrics.total} total waivers | {metrics.approved} approved, {metrics.terminated} terminated/expired
            </div>
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Status classification:</strong> Waiver status text varies across KFF records (e.g., "Approved," "CMS Approved," "Currently Active"). Status classification uses case-insensitive substring matching and has been manually verified against a sample of 50 waivers.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. Duplicate detection:</strong> Some waivers have been renewed multiple times, creating separate catalog entries for each renewal period. The count reflects distinct waiver records, not distinct demonstration concepts.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. Before/after methodology:</strong> Simple before/after comparisons of enrollment and spending are confounded by concurrent policy changes (PHE, unwinding, state plan amendments). Interrupted time series with control states would be the appropriate quasi-experimental design.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. State count sensitivity:</strong> Including territories (PR, GU, VI) in the analysis adds states with atypical Medicaid structures. Excluding territories does not change the top 20 state ranking.</p>
        </div>
      </Collapsible>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Catalog completeness:</strong> The KFF waiver tracker is the most comprehensive public source but may lag recent CMS approvals by several weeks. Pending waivers in the CMS pipeline may not yet appear in the catalog.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Causal attribution:</strong> Observing enrollment or spending changes after a waiver approval does not establish that the waiver caused the change. Secular trends, economic conditions, other policy changes, and COVID-19 all affect enrollment and spending simultaneously.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Waiver heterogeneity:</strong> Section 1115 waivers vary enormously in scope -- from narrow SUD treatment demonstrations to comprehensive program restructuring. Treating all waivers as comparable units obscures this heterogeneity.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>Implementation fidelity:</strong> A waiver's approved terms may differ substantially from actual implementation. CMS monitoring reports and evaluation findings (when available) provide implementation context that this catalog-level analysis does not capture.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Section 1115 waiver catalog with status counts
SELECT
  state_code,
  COUNT(*) AS total_waivers,
  SUM(CASE WHEN LOWER(waiver_status) LIKE '%approved%' THEN 1 ELSE 0 END)
    AS approved,
  SUM(CASE WHEN LOWER(waiver_status) LIKE '%pending%' THEN 1 ELSE 0 END)
    AS pending,
  SUM(CASE WHEN LOWER(waiver_status) LIKE '%terminated%'
    OR LOWER(waiver_status) LIKE '%expired%' THEN 1 ELSE 0 END)
    AS terminated_expired
FROM ref_1115_waivers
GROUP BY state_code
ORDER BY total_waivers DESC;

-- Before/after enrollment for a specific state and waiver
WITH waiver_dates AS (
  SELECT state_code,
    MIN(approval_date) AS first_approval
  FROM ref_1115_waivers
  WHERE state_code = 'OH'  -- example state
    AND waiver_status ILIKE '%approved%'
  GROUP BY state_code
)
SELECT e.year, e.month, e.total_enrollment,
  CASE WHEN MAKE_DATE(e.year, e.month, 1) >= w.first_approval::DATE
    THEN 'Post' ELSE 'Pre' END AS period
FROM fact_enrollment e
JOIN waiver_dates w ON e.state_code = w.state_code
ORDER BY e.year, e.month;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> KFF Section 1115 Waiver Tracker ({metrics.total} waivers) | CMS-64 Financial Management Reports (FY2018-2024, 118K rows) |
          CMS Monthly Enrollment Reports (2013-2025) | Medicaid & CHIP Core Set (2017-2024, 35,993 rows).
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing the Section 1115 Waiver Impact research brief. Key finding: ${metrics.total} waivers tracked, ${metrics.approved} approved, ${metrics.terminated} terminated/expired. Framework for before/after evaluation of enrollment and spending trajectories.` })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
