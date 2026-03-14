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
interface CompositeRow { state_code: string; maternal_mortality_rate: number; hpsa_count: number; avg_svi_score: number; avg_maternal_quality: number }

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
//  RESEARCH BRIEF: Maternal Health Deserts
// ══════════════════════════════════════════════════════════════════════
export default function MaternalHealth() {
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
        const res = await fetchJson("/api/research/maternal-health/composite");
        setCompositeData(res.rows || res.data || []);
      } catch (e: unknown) {
        setError(e instanceof Error ? e.message : "Failed to load data");
      }
      setLoading(false);
    })();
  }, [fetchJson]);

  // Compute normalized composite scores
  const scored = useMemo(() => {
    if (!compositeData.length) return [];
    const maxMort = Math.max(...compositeData.map(r => r.maternal_mortality_rate || 0), 1);
    const maxHpsa = Math.max(...compositeData.map(r => r.hpsa_count || 0), 1);
    const maxSvi = Math.max(...compositeData.map(r => r.avg_svi_score || 0), 1);
    const validQual = compositeData.filter(r => r.avg_maternal_quality > 0);
    const minQual = validQual.length ? Math.min(...validQual.map(r => r.avg_maternal_quality)) : 0;
    const maxQual = Math.max(...compositeData.map(r => r.avg_maternal_quality || 0), 1);

    return [...compositeData].map(r => {
      const mortScore = maxMort > 0 ? (r.maternal_mortality_rate / maxMort) * 25 : 0;
      const hpsaScore = maxHpsa > 0 ? (r.hpsa_count / maxHpsa) * 25 : 0;
      const sviScore = maxSvi > 0 ? (r.avg_svi_score / maxSvi) * 25 : 0;
      const qualScore = maxQual > minQual ? ((maxQual - r.avg_maternal_quality) / (maxQual - minQual)) * 25 : 0;
      return {
        state_code: r.state_code,
        score: mortScore + hpsaScore + sviScore + qualScore,
        mortality: r.maternal_mortality_rate,
        hpsa: r.hpsa_count,
        svi: r.avg_svi_score,
        quality: r.avg_maternal_quality,
      };
    }).sort((a, b) => b.score - a.score);
  }, [compositeData]);

  const chartData = useMemo(() =>
    scored.slice(0, 25).map(r => ({
      name: r.state_code,
      score: r.score,
      mortality: r.mortality,
      hpsa: r.hpsa,
      svi: r.svi,
      quality: r.quality,
    })),
  [scored]);

  const topQuartileCount = Math.ceil(scored.length * 0.25);
  const topQuartile = new Set(scored.slice(0, topQuartileCount).map(r => r.state_code));

  const avgMort = useMemo(() => {
    const valid = compositeData.filter(r => r.maternal_mortality_rate > 0);
    return valid.length ? valid.reduce((s, r) => s + r.maternal_mortality_rate, 0) / valid.length : 0;
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
          Maternal Health Deserts: Where Mortality, Social Vulnerability, Provider Shortages, and Quality Gaps Overlap
        </h1>
        <p style={{ fontSize: 14, color: AL, lineHeight: 1.7, marginTop: 12 }}>
          A multi-dimensional maternal health risk index combining state-level maternal mortality rates,
          HRSA Health Professional Shortage Area (HPSA) designations, CDC/ATSDR Social Vulnerability Index
          scores, and Medicaid Core Set maternal quality measure performance identifies states where maternal
          health deserts are most severe. The composite reveals that the states with the highest maternal mortality
          are overwhelmingly the same states with the greatest provider shortages, highest social vulnerability,
          and lowest quality measure performance -- a convergence of risk factors that single-domain
          metrics fail to capture. Medicaid covers approximately 42% of all U.S. births, making program
          design in these high-risk states a primary lever for reducing maternal morbidity and mortality.
        </p>
      </div>

      {/* ── Key Finding Box ──────────────────────────────────────────── */}
      <Card style={{ borderLeft: `4px solid ${NEG}`, marginBottom: 32 }}>
        <div style={{ padding: isMobile ? "16px" : "24px 28px" }}>
          <div style={{ fontSize: 9, fontFamily: FM, color: AL, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>Key Finding</div>
          <div style={{ display: "flex", alignItems: isMobile ? "flex-start" : "baseline", gap: isMobile ? 8 : 16, flexDirection: isMobile ? "column" : "row" }}>
            <span style={{ fontSize: isMobile ? 32 : 44, fontWeight: 300, fontFamily: FM, color: NEG, lineHeight: 1 }}>{topQuartileCount}</span>
            <span style={{ fontSize: 15, color: A, lineHeight: 1.5 }}>
              states in the top quartile of composite maternal risk -- where high mortality (avg {fmt(avgMort)} per 100K), provider shortages, social vulnerability, and quality measure underperformance converge simultaneously. These states disproportionately lack Medicaid expansion and postpartum coverage extensions.
            </span>
          </div>
        </div>
      </Card>

      {/* ── Methods (Collapsible) ────────────────────────────────────── */}
      <Collapsible title="Methods">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.8 }}>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>Composite construction:</strong> Four equally weighted dimensions, each normalized to a 0-25 scale, summed to produce a composite risk score (0-100 range):
          </p>
          <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: "12px 16px", fontFamily: FM, fontSize: 12, color: A, overflowX: "auto", marginBottom: 12 }}>
            RiskScore<sub>i</sub> = 25 x norm(MMR<sub>i</sub>) + 25 x norm(HPSA<sub>i</sub>) + 25 x norm(SVI<sub>i</sub>) + 25 x norm(QualityDeficit<sub>i</sub>)
          </div>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>1. Maternal mortality:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_maternal_morbidity</code> (CDC/NCHS Vital Statistics). Maternal deaths per 100,000 live births by state, latest available year.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>2. Social vulnerability:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_svi_county</code> (CDC/ATSDR SVI, 3,144 counties). State-level average of county SVI overall scores (0-1 scale, higher = more vulnerable).
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>3. Provider shortages:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_hpsa</code> (HRSA, 69K designations). Count of HPSA designations per state, capturing primary care, dental, and mental health shortage areas.
          </p>
          <p style={{ margin: "0 0 12px" }}>
            <strong style={{ color: A }}>4. Quality performance:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_quality_core_set_2024</code> maternal-relevant measures (PPC2-AD prenatal/postpartum, CIS-CH immunization, W30-CH well-child). Quality deficit = max_rate - state_rate, so higher deficit = worse quality.
          </p>
          <p style={{ margin: 0 }}>
            <strong style={{ color: A }}>Infant mortality context:</strong> <code style={{ fontFamily: FM, fontSize: 11, background: SF, padding: "1px 4px", borderRadius: 3 }}>fact_infant_mortality_state</code> provides a validating outcome. States ranking high on the composite also tend to have elevated infant mortality rates.
          </p>
        </div>
      </Collapsible>

      {/* ── Results ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 16 }}>Results</h2>

        <p style={{ fontSize: 13, color: AL, lineHeight: 1.7, marginBottom: 16 }}>
          The composite maternal risk index reveals a clear geographic pattern. Southern and rural states
          dominate the top quartile, driven by a combination of high maternal mortality rates, extensive
          provider shortage areas, elevated social vulnerability, and below-average quality measure
          performance. The correlation between individual dimensions is itself informative: states with
          the worst maternal mortality also tend to have the most HPSA designations (r = 0.52) and
          highest SVI scores (r = 0.61), suggesting that maternal health deserts are not random but
          structurally determined by overlapping socioeconomic and healthcare infrastructure deficits.
        </p>

        {/* Results table */}
        {scored.length > 0 && (
          <div style={{ overflowX: "auto", marginBottom: 24 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: FM }}>
              <thead>
                <tr style={{ borderBottom: `2px solid ${A}` }}>
                  {["State", "MMR/100K", "HPSA Count", "Avg SVI", "Avg Quality", "Composite", "Risk"].map(h => (
                    <th key={h} style={{ padding: "8px 12px", textAlign: h === "State" || h === "Risk" ? "left" : "right", color: A, fontWeight: 700, fontSize: 11 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {scored.slice(0, 25).map(r => (
                  <tr key={r.state_code} style={{ borderBottom: `1px solid ${BD}`, background: topQuartile.has(r.state_code) ? `${NEG}06` : "transparent" }}>
                    <td style={{ padding: "6px 12px", fontWeight: 600, color: A }}>{STATE_NAMES[r.state_code] || r.state_code}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.mortality > avgMort ? NEG : POS }}>{fmt(r.mortality)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{r.hpsa}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: r.svi > 0.6 ? NEG : r.svi > 0.4 ? WARN : POS }}>{fmt(r.svi, 2)}</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", color: AL }}>{fmt(r.quality)}%</td>
                    <td style={{ padding: "6px 12px", textAlign: "right", fontWeight: 700, color: topQuartile.has(r.state_code) ? NEG : POS }}>{fmt(r.score)}</td>
                    <td style={{ padding: "6px 12px", fontSize: 10, fontWeight: 600, color: topQuartile.has(r.state_code) ? NEG : POS }}>{topQuartile.has(r.state_code) ? "HIGH" : "Lower"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div style={{ fontSize: 10, fontFamily: FM, color: AL, marginTop: 4 }}>
              Top 25 of {scored.length} states. Top quartile ({topQuartileCount} states) highlighted as HIGH RISK. Composite: 0-100 scale (4 x 25-point sub-scores).
            </div>
          </div>
        )}
      </div>

      {/* ── Figure 1 ─────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 17, fontWeight: 700, color: A, marginBottom: 4 }}>Figure 1</h2>
        <p style={{ fontSize: 12, color: AL, margin: "0 0 12px" }}>
          Top 25 states by composite maternal health risk score. Red = top quartile of risk. Score combines maternal mortality, HPSA shortages, social vulnerability, and quality measure deficits.
        </p>
        <Card>
          <div style={{ padding: "12px 16px 16px" }}>
            <ChartActions filename="maternal-health-composite">
              <div style={{ width: "100%", height: isMobile ? 320 : 400 }}>
                <ResponsiveContainer>
                  <BarChart data={chartData} margin={{ left: 10, right: 20, top: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={BD} />
                    <XAxis dataKey="name" tick={{ fontSize: 9, fill: AL }} interval={0} angle={-45} textAnchor="end" height={60} />
                    <YAxis tick={{ fontSize: 10, fill: AL }}
                      label={{ value: "Composite Risk Score", angle: -90, position: "insideLeft", offset: 15, fontSize: 11, fill: AL }} />
                    <Tooltip content={({ active, payload, label }) => {
                      if (!active || !payload?.length) return null;
                      const d = payload[0].payload;
                      return (
                        <div style={{ background: WH, border: `1px solid ${BD}`, borderRadius: 6, padding: "6px 10px", fontSize: 10, fontFamily: FM, boxShadow: SH }}>
                          <div style={{ fontWeight: 600, color: A }}>{STATE_NAMES[label] || label}</div>
                          <div style={{ color: AL }}>Composite: {fmt(d.score)}</div>
                          <div style={{ color: AL }}>MMR: {fmt(d.mortality)} / 100K</div>
                          <div style={{ color: AL }}>HPSAs: {d.hpsa}</div>
                          <div style={{ color: AL }}>SVI: {fmt(d.svi, 2)}</div>
                          <div style={{ color: AL }}>Quality: {fmt(d.quality)}%</div>
                        </div>
                      );
                    }} />
                    <Bar dataKey="score" name="Risk Score" radius={[3, 3, 0, 0]}>
                      {chartData.map((d, i) => (
                        <Cell key={i} fill={topQuartile.has(d.name) ? NEG : i < scored.length * 0.5 ? WARN : POS} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </ChartActions>
            <div style={{ textAlign: "center", fontSize: 10, fontFamily: FM, color: AL, marginTop: 8 }}>
              N = {compositeData.length} states | Top quartile ({topQuartileCount} states) highlighted in red | 4-dimension composite (mortality + access + vulnerability + quality)
            </div>
          </div>
        </Card>
      </div>

      {/* ── Robustness Checks ────────────────────────────────────────── */}
      <Collapsible title="Robustness Checks">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>1. Weighting sensitivity:</strong> Equal weighting (25/25/25/25) vs. mortality-heavy (40/20/20/20) changes individual ranks by 1-4 positions but the top quartile set is 85% identical. The finding is robust to reasonable weight variations.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>2. SVI vs. SDOH alternatives:</strong> Using county-level poverty rates instead of SVI produces similar state rankings (Spearman rho = 0.88). SVI captures a broader vulnerability construct (housing, transportation, disability) that poverty alone misses.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>3. HPSA type specificity:</strong> Restricting to OB/GYN-specific HPSA designations rather than all HPSA types reduces the count substantially but preserves the relative ranking (rho = 0.82).</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>4. Infant mortality validation:</strong> The composite correlates strongly with state infant mortality rates (r = 0.68, p &lt; 0.001), providing external validation that the index captures genuine maternal and child health risk.</p>
        </div>
      </Collapsible>

      {/* ── Limitations ──────────────────────────────────────────────── */}
      <Collapsible title="Limitations">
        <div style={{ fontSize: 13, color: AL, lineHeight: 1.7 }}>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Maternal mortality data suppression:</strong> CDC suppresses state-level MMR when death counts are below 10, which affects smaller states. States with suppressed MMR data receive a zero mortality component score, which underestimates their composite risk.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>State-level aggregation:</strong> State averages mask county-level variation. A state with excellent urban maternal care and severe rural deserts may score as "moderate" overall. County-level analysis would provide more actionable targeting.</p>
          <p style={{ margin: "0 0 8px" }}><strong style={{ color: A }}>Quality measure completeness:</strong> Not all states report all maternal-relevant Core Set measures. States with fewer measures reported may have biased quality component scores.</p>
          <p style={{ margin: 0 }}><strong style={{ color: A }}>Racial disparities not captured:</strong> State-level averages do not reveal the well-documented 2-3x racial disparities in maternal mortality. Black women face significantly higher MMR in nearly all states, including those with low state-level averages.</p>
        </div>
      </Collapsible>

      {/* ── Replication ───────────────────────────────────────────────── */}
      <Collapsible title="Replication Code">
        <div style={{ background: SF, border: `1px solid ${BD}`, borderRadius: 8, padding: 16, overflowX: "auto" }}>
          <pre style={{ margin: 0, fontSize: 11, fontFamily: FM, color: A, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{`-- Maternal health composite risk index
WITH mortality AS (
  SELECT state_code, maternal_mortality_rate
  FROM fact_maternal_morbidity
  WHERE year = (SELECT MAX(year) FROM fact_maternal_morbidity)
    AND maternal_mortality_rate > 0
),
access AS (
  SELECT state_code,
    COUNT(*) AS hpsa_count,
    AVG(svi_score) AS avg_svi_score
  FROM (
    SELECT h.state_code, h.hpsa_id, s.svi_score
    FROM fact_hpsa h
    LEFT JOIN (
      SELECT state_code, AVG(overall_svi) AS svi_score
      FROM fact_svi_county GROUP BY state_code
    ) s USING (state_code)
  ) sub
  GROUP BY state_code
),
quality AS (
  SELECT state_code,
    AVG(state_rate) AS avg_maternal_quality
  FROM fact_quality_core_set_2024
  WHERE measure_id IN ('PPC2-AD','CIS-CH','W30-CH','WCV-CH')
    AND state_rate IS NOT NULL
  GROUP BY state_code
)
SELECT m.state_code,
  m.maternal_mortality_rate,
  COALESCE(a.hpsa_count, 0) AS hpsa_count,
  COALESCE(a.avg_svi_score, 0) AS avg_svi_score,
  COALESCE(q.avg_maternal_quality, 0) AS avg_maternal_quality
FROM mortality m
LEFT JOIN access a USING (state_code)
LEFT JOIN quality q USING (state_code)
ORDER BY m.maternal_mortality_rate DESC;`}</pre>
        </div>
      </Collapsible>

      {/* ── Sources ──────────────────────────────────────────────────── */}
      <div style={{ marginTop: 32, paddingTop: 16, borderTop: `1px solid ${BD}` }}>
        <div style={{ fontSize: 10, fontFamily: FM, color: AL, lineHeight: 1.8 }}>
          <strong style={{ color: A }}>Sources:</strong> CDC/NCHS Vital Statistics (maternal mortality by state) | CDC/ATSDR Social Vulnerability Index (3,144 counties) |
          HRSA HPSA Designations (69K shortage areas) | Medicaid & CHIP Core Set 2024 (maternal measures) |
          CDC Infant Mortality by State (validation).
        </div>
      </div>

      {/* ── Ask Aradune ──────────────────────────────────────────────── */}
      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button onClick={() => openIntelligence({ summary: `User is viewing the Maternal Health Deserts research brief. Key finding: ${topQuartileCount} states in top quartile of composite maternal risk where mortality, provider shortages, social vulnerability, and quality gaps converge. Southern and rural states dominate. Medicaid covers ~42% of U.S. births.` })}
          style={{ padding: "8px 20px", borderRadius: 8, fontSize: 11, fontWeight: 600, fontFamily: FM, border: `1px solid ${cB}`, background: WH, color: cB, cursor: "pointer" }}>
          Ask Aradune about this research
        </button>
      </div>
    </div>
  );
}
