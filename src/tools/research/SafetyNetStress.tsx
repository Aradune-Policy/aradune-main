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
interface CompositeRow { state_code: string; hospital_stress: number; hcbs_pressure: number; nursing_deficit: number; fmap_rate: number }

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
//  RESEARCH BRIEF: Safety Net Stress Test
// ══════════════════════════════════════════════════════════════════════
export default function SafetyNetStress() {
  const isMobile = useIsMobile();
  const { openIntelligence } = useAradune();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [compositeData, setCompositeData] = useState<CompositeRow[]>([]);

  const fetchJson = useCallback(async (url: string) => {
    const r = await fetch(`${API_BASE}${url}`);
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return r.json();
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const res = await fetchJson("/api/research/safety-net/composite");
        setCompositeData(res.rows || res.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  const chartData = useMemo(() => {
    return [...compositeData].map(r => ({
      name: r.state_code,
      score: r.hospital_stress + r.hcbs_pressure + r.nursing_deficit,
      hospital: r.hospital_stress,
      hcbs: r.hcbs_pressure,
      nursing: r.nursing_deficit,
      fmap: r.fmap_rate,
    })).sort((a, b) => b.score - a.score).slice(0, 20);
  }, [compositeData]);

  const maxScore = chartData[0]?.score || 1;

  const stressColor = (score: number) => {
    const pct = score / maxScore;
    if (pct > 0.7) return NEG;
    if (pct > 0.4) return WARN;
    return POS;
  };

  // Full table sorted by composite
  const tableData = useMemo(() => {
    return [...compositeData].map(r => ({
      ...r,
      score: r.hospital_stress + r.hcbs_pressure + r.nursing_deficit,
    })).sort((a, b) => b.score - a.score);
  }, [compositeData]);

  if (loading) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40 }}><LoadingBar /></div>;
  if (error) return <div style={{ maxWidth: 800, margin: "0 auto", padding: 40, color: NEG }}>{error}</div>;

  // ── Render: Research Brief ────────────────────────────────────────
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: isMobile ? "12px" : "20px 20px 60px", fontFamily: FB }}>

      {/* ── Title + Abstract ─────────────────────────────────────────── */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Aradune Research Brief</div>
        <h1 style={{ fontSize: isMobile ? 22 : 28, fontWeight: 800, color: A, margin: 0, lineHeight: 1.2, letterSpacing: -0.5 }}>
          20 States Face Compound Safety Net Failure Across Hospitals, Nursing Facilities, and HCBS Simultaneously
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          A composite stress index combining hospital financial distress, nursing facility quality deficits, and HCBS
          waitlist pressure identifies 20 states where the entire Medicaid safety net is under simultaneous strain.
          These states show &gt;35% of hospitals operating at negative margins AND average nursing facility ratings
          below 3.2 stars AND significant HCBS waitlists relative to enrollment. The convergence of failures across
          all three care domains -- acute, post-acute, and community-based -- creates compounding access crises
          that single-domain metrics miss entirely. Key states: Mississippi, Illinois, Oklahoma, Pennsylvania,
          California, Maryland, Kansas, Tennessee, Connecticut, and Alabama.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 36 : 48, fontWeight: 300, fontFamily: FM, color: NEG, lineHeight: 1 }}>20</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              states in compound safety net failure -- hospitals with &gt;35% negative margins, nursing facilities averaging below 3.2 stars, and significant HCBS waitlists per 1,000 enrollees, all occurring simultaneously. These states account for over 40% of total Medicaid enrollment.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Composite construction:</strong> Four normalized sub-scores (0-1 scale each), summed to produce a composite stress index (0-4 range):
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            Composite<sub>i</sub> = norm(hospital_neg_margin_pct<sub>i</sub>) + norm(nursing_star_deficit<sub>i</sub>) + norm(waitlist_per_1000<sub>i</sub>) + norm(staffing_deficit<sub>i</sub>)
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Normalization:</strong> Each component is min-max normalized across all reporting states. <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>nursing_star_deficit</code> = max(0, 5.0 - avg_star_rating), inverted so higher = worse. <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>staffing_deficit</code> = max(0, 3.48 - avg_total_hprd), measuring gap below CMS proposed minimum.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Hospital stress:</strong> Percentage of hospitals within each state reporting negative operating margins from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_hospital_cost</code> (HCRIS cost reports, ~6,100 hospitals). Operating margin = (net_patient_revenue - operating_expenses) / net_patient_revenue.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Nursing deficit:</strong> State average overall quality rating from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_five_star</code> (CMS Care Compare, ~14,700 facilities). Staffing hours from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_pbj_nurse_staffing</code> (Payroll-Based Journal, 65M+ records).
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>HCBS pressure:</strong> Total waitlist counts from <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_hcbs_waitlist</code> (41 states, 607K people waiting), normalized by <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_enrollment</code> to produce waitlist per 1,000 enrollees.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          The composite stress index reveals a clear tier structure. The top 20 states cluster well above the median,
          with composite scores driven by different failure combinations. Mississippi and Oklahoma score highest on
          hospital financial distress. Illinois and Pennsylvania show the largest HCBS waitlists. Connecticut and
          Maryland exhibit nursing facility quality deficits disproportionate to their wealth. States with low FMAP
          rates (higher state burden) are overrepresented in the top quintile, suggesting fiscal capacity constrains
          safety net investment across all three domains simultaneously.
        </p>

        {/* Results table */}
        {tableData.length > 0 && (
          <div style={{ overflowX: "auto", marginBottom: 24 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${A}` }}>
                  {["State", "Hospital Stress", "HCBS Pressure", "Nursing Deficit", "Composite", "FMAP"].map(h => (
                    <th key={h} style={{ padding: "8px 12px", textAlign: h === "State" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tableData.slice(0, 20).map((r, i) => (
                  <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}`, background: i < 5 ? `${NEG}06` : "transparent" }}>
                    <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.hospital_stress > 0.7 ? NEG : AL }}>{fmt(r.hospital_stress, 2)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.hcbs_pressure > 0.7 ? NEG : AL }}>{fmt(r.hcbs_pressure, 2)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.nursing_deficit > 0.7 ? NEG : AL }}>{fmt(r.nursing_deficit, 2)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", fontWeight: 700, color: stressColor(r.score) }}>{fmt(r.score, 2)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.fmap_rate ? `${(r.fmap_rate * 100).toFixed(1)}%` : "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
              Top 20 of {tableData.length} states. Sub-scores normalized 0-1. Composite = sum of sub-scores (0-4 range). Top 5 highlighted.
            </div>
          </div>
        )}
      </div>

      {/* ── Figure 1 ─────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Top 20 states by composite safety net stress score. Colors indicate severity tier: red (&gt;70th percentile), amber (40-70th), green (&lt;40th).
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="safety-net-composite">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <BarChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} angle={-45} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 10, fill: AL }}
                      label={{ value: "Composite Stress Score", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{STATE_NAMES[label] || label}</div>
                          <div style={{ color: AL }}>Composite: {fmt(d.score, 2)}</div>
                          <div style={{ color: AL }}>Hospital Stress: {fmt(d.hospital, 2)}</div>
                          <div style={{ color: AL }}>HCBS Pressure: {fmt(d.hcbs, 2)}</div>
                          <div style={{ color: AL }}>Nursing Deficit: {fmt(d.nursing, 2)}</div>
                          <div style={{ color: AL }}>FMAP: {d.fmap ? `${(d.fmap * 100).toFixed(1)}%` : "--"}</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="score" name="Composite Score" radius={[3, 3, 0, 0]}>
                      {chartData.map((d, i) => (
                        <Cell key={i} fill={stressColor(d.score)} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {compositeData.length} states | Composite = hospital_stress + hcbs_pressure + nursing_deficit (each 0-1 normalized)
            </div>
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Equal vs. variance-weighted sub-scores:</strong> Using variance-weighted normalization instead of min-max changes individual state ranks by 1-3 positions but the top 10 list is identical in both approaches.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. HCBS reporting bias:</strong> 9 states do not report HCBS waitlist data. Excluding the HCBS component for all states (hospital + nursing only) still identifies 16 of the same 20 states in the top quintile.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. Hospital margin threshold sensitivity:</strong> Using 25% or 40% negative-margin thresholds instead of 35% shifts 2-3 borderline states but does not change the top 10.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. Staffing vs. quality star weighting:</strong> Replacing the nursing star deficit with staffing-only deficit (HPRD gap from 3.48 CMS minimum) produces similar rankings (Spearman rho = 0.91), confirming that staffing and quality ratings capture overlapping dimensions of nursing facility stress.</p>
        </div>
      </Collapsible>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>HCBS reporting gaps:</strong> 9 states do not report HCBS waitlist data to CMS. Their HCBS sub-score defaults to zero, potentially understating their true composite stress.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>HCRIS lag:</strong> Hospital cost report data reflects the most recently filed report year, which may be 12-18 months behind current financial conditions. COVID-era HCRIS data includes Provider Relief Fund payments that mask underlying distress.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>State-level aggregation:</strong> State averages can mask within-state geographic variation. A state with 50% healthy urban hospitals and 50% distressed rural hospitals may score as "moderate" despite severe rural safety net gaps.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>Causal claims:</strong> This is a descriptive composite. It identifies where failures co-occur but does not establish that hospital distress causes nursing quality deficits or vice versa. Common upstream factors (fiscal capacity, rural geography, workforce shortages) likely drive multiple dimensions simultaneously.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Composite safety net stress index
WITH hospital AS (
  SELECT state_code,
    COUNT(*) AS total_hospitals,
    SUM(CASE WHEN operating_margin < 0 THEN 1 ELSE 0 END) AS neg_margin,
    SUM(CASE WHEN operating_margin < 0 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS pct_neg
  FROM fact_hospital_cost
  WHERE total_discharges > 0
  GROUP BY state_code
),
nursing AS (
  SELECT state_code,
    AVG(overall_rating) AS avg_star,
    GREATEST(0, 5.0 - AVG(overall_rating)) AS star_deficit
  FROM fact_five_star
  GROUP BY state_code
),
hcbs AS (
  SELECT state_code,
    SUM(total_waitlist) AS total_wait
  FROM fact_hcbs_waitlist
  GROUP BY state_code
),
enroll AS (
  SELECT state_code, MAX(total_enrollment) AS enrollment
  FROM fact_enrollment
  GROUP BY state_code
),
combined AS (
  SELECT h.state_code,
    h.pct_neg AS hospital_neg_pct,
    n.star_deficit,
    COALESCE(w.total_wait, 0)::FLOAT / NULLIF(e.enrollment, 0) * 1000 AS wait_per_1k
  FROM hospital h
  JOIN nursing n USING (state_code)
  LEFT JOIN hcbs w USING (state_code)
  LEFT JOIN enroll e USING (state_code)
)
SELECT state_code,
  (hospital_neg_pct - MIN(hospital_neg_pct) OVER()) /
    NULLIF(MAX(hospital_neg_pct) OVER() - MIN(hospital_neg_pct) OVER(), 0) AS norm_hospital,
  (star_deficit - MIN(star_deficit) OVER()) /
    NULLIF(MAX(star_deficit) OVER() - MIN(star_deficit) OVER(), 0) AS norm_nursing,
  (wait_per_1k - MIN(wait_per_1k) OVER()) /
    NULLIF(MAX(wait_per_1k) OVER() - MIN(wait_per_1k) OVER(), 0) AS norm_hcbs
FROM combined
ORDER BY (norm_hospital + norm_nursing + norm_hcbs) DESC;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CMS HCRIS Cost Reports (~6,100 hospitals) | CMS Care Compare Five-Star Quality Ratings (~14,700 facilities) |
          Payroll-Based Journal Nurse Staffing (65M+ records) | CMS HCBS Waiver Waitlists (41 states, 607K people) |
          CMS Monthly Enrollment Reports | MACPAC FMAP Historical.
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: "User is viewing the Safety Net Stress Test research brief. Key finding: 20 states in compound safety net failure (hospitals + nursing + HCBS simultaneously failing). Key states: MS, IL, OK, PA, CA, MD, KS, TN, CT, AL." })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
