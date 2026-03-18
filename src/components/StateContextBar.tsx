/**
 * StateContextBar - Universal cross-dataset state context panel.
 * Drop into any module: <StateContextBar stateCode="FL" />
 */
import { C, FONT, SHADOW, useIsMobile } from "../design";
import { useStateContext } from "../hooks/useStateContext";
import { useAradune } from "../context/AraduneContext";
import { fmtB, fmtPct, fmtNum, fmtDollar, SYM, stateContextSummary, TMSIS_BG, TMSIS_BD } from "../utils/formatContext";
import type { StateContextData } from "../types";
import { STATE_NAMES } from "../data/states";

interface Props {
  stateCode: string | null | undefined;
  mode?: "compact" | "expanded";
  hideSections?: string[];
}

// Compact metric pill
function Metric({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <span style={{ fontSize: 11, fontFamily: FONT.body, color: C.inkLight, whiteSpace: "nowrap" }}>
      {label} <span style={{ fontFamily: FONT.mono, fontWeight: 600, color: color || C.ink }}>{value}</span>
    </span>
  );
}

// Section wrapper for expanded mode
function Sec({ sym, title, children, bg, bd }: { sym: string; title: string; children: React.ReactNode; bg?: string; bd?: string }) {
  return (
    <div style={{
      padding: "10px 12px", borderRadius: 6,
      background: bg || C.surface, border: `1px solid ${bd || C.border}`,
    }}>
      <div style={{ fontSize: 10, fontWeight: 700, color: C.inkLight, fontFamily: FONT.body, marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>
        <span style={{ marginRight: 5 }}>{sym}</span>{title}
      </div>
      {children}
    </div>
  );
}

// Value line for expanded mode
function V({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, lineHeight: 1.8 }}>
      <span style={{ color: C.inkLight, fontFamily: FONT.body }}>{label}</span>
      <span style={{ fontFamily: FONT.mono, fontWeight: 600, color: color || C.ink }}>{value}</span>
    </div>
  );
}

export default function StateContextBar({ stateCode, mode = "compact", hideSections = [] }: Props) {
  const { data, loading } = useStateContext(stateCode);
  const { openIntelligence } = useAradune();
  const isMobile = useIsMobile();

  if (!stateCode || loading || !data || Object.keys(data).length <= 2) return null;

  const show = (s: string) => !hideSections.includes(s);
  const stateName = data.state_name || STATE_NAMES[stateCode.toUpperCase()] || stateCode;

  // Compact mode: single row of key metrics
  if (mode === "compact") {
    return (
      <div style={{
        display: "flex", flexWrap: "wrap", gap: "6px 16px",
        padding: "8px 14px", background: C.surface, border: `1px solid ${C.border}`,
        borderRadius: 6, marginBottom: 12, alignItems: "center",
      }}>
        <span style={{ fontSize: 11, fontWeight: 700, color: C.brand, fontFamily: FONT.body, marginRight: 4 }}>
          {stateName}
        </span>
        {data.fiscal?.fmap != null && show("fiscal") && <Metric label="FMAP" value={fmtPct(data.fiscal.fmap * 100)} />}
        {data.enrollment?.total && show("enrollment") && <Metric label="Enrolled" value={fmtNum(data.enrollment.total)} />}
        {data.enrollment?.mc_pct != null && show("enrollment") && <Metric label="MC" value={`${data.enrollment.mc_pct.toFixed(0)}%`} />}
        {data.access?.hpsa_total && show("access") && <Metric label="HPSAs" value={String(data.access.hpsa_total)} color={data.access.hpsa_total > 100 ? C.neg : undefined} />}
        {data.rate_adequacy?.median_pct_medicare != null && show("rates") && (
          <Metric label="MCR" value={`${data.rate_adequacy.median_pct_medicare.toFixed(0)}%`}
            color={data.rate_adequacy.median_pct_medicare < 70 ? C.neg : data.rate_adequacy.median_pct_medicare < 85 ? C.warn : C.pos} />
        )}
        {data.fiscal?.cms64_total && show("fiscal") && <Metric label="CMS-64" value={fmtB(data.fiscal.cms64_total)} />}
        {data.quality && show("quality") && <Metric label="Quality" value={`${data.quality.below_median}/${data.quality.total_measures} below med`} />}
        <button
          onClick={() => openIntelligence({
            summary: `State context for ${stateName}: ${stateContextSummary(data)}`,
            state: stateCode.toUpperCase(),
          })}
          style={{
            marginLeft: "auto", background: "none", border: `1px solid ${C.border}`,
            borderRadius: 4, padding: "2px 8px", fontSize: 10, color: C.brand,
            cursor: "pointer", fontFamily: FONT.body, fontWeight: 600,
          }}
        >
          Ask Aradune
        </button>
      </div>
    );
  }

  // Expanded mode: grid with sections
  const cols = isMobile ? 1 : 3;

  return (
    <div style={{
      background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
      padding: 16, marginBottom: 16,
    }}>
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        marginBottom: 12, paddingBottom: 8, borderBottom: `1px solid ${C.border}`,
      }}>
        <div>
          <span style={{ fontSize: 13, fontWeight: 700, color: C.ink, fontFamily: FONT.body }}>{stateName}</span>
          <span style={{ fontSize: 11, color: C.inkLight, marginLeft: 8, fontFamily: FONT.body }}>Cross-Dataset Context</span>
        </div>
        <button
          onClick={() => openIntelligence({
            summary: `Full cross-dataset context for ${stateName}: ${stateContextSummary(data)}`,
            state: stateCode.toUpperCase(),
          })}
          style={{
            background: C.brand, color: C.white, border: "none", borderRadius: 5,
            padding: "4px 12px", fontSize: 11, cursor: "pointer", fontFamily: FONT.body, fontWeight: 600,
          }}
        >
          Ask Aradune
        </button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: `repeat(${cols}, 1fr)`, gap: 10 }}>
        {/* Fiscal */}
        {data.fiscal && show("fiscal") && (
          <Sec sym={SYM.fiscal} title="Fiscal">
            {data.fiscal.fmap != null && <V label="FMAP" value={fmtPct(data.fiscal.fmap * 100)} />}
            {data.fiscal.cms64_total != null && <V label={`CMS-64 FY${data.fiscal.cms64_fy || ""}`} value={fmtB(data.fiscal.cms64_total)} />}
            {data.fiscal.fmap != null && data.fiscal.cms64_total != null && (
              <V label="1% rate increase cost"
                value={fmtB(data.fiscal.cms64_total * 0.01 * (1 - data.fiscal.fmap))}
                color={C.accent} />
            )}
          </Sec>
        )}

        {/* Access */}
        {data.access && show("access") && (
          <Sec sym={SYM.access} title="Access">
            <V label="Total HPSAs" value={String(data.access.hpsa_total)} color={data.access.hpsa_total > 100 ? C.neg : undefined} />
            <V label="Primary Care" value={String(data.access.hpsa_primary_care)} />
            <V label="Dental" value={String(data.access.hpsa_dental)} />
            <V label="Mental Health" value={String(data.access.hpsa_mental_health)} />
          </Sec>
        )}

        {/* Quality */}
        {data.quality && show("quality") && (
          <Sec sym={SYM.quality} title="Quality">
            <V label="Measures below median" value={`${data.quality.below_median} of ${data.quality.total_measures}`}
              color={data.quality.pct_below > 50 ? C.neg : undefined} />
            <V label="Below median %" value={fmtPct(data.quality.pct_below)} />
          </Sec>
        )}

        {/* Enrollment */}
        {data.enrollment && show("enrollment") && (
          <Sec sym={SYM.enrollment} title="Enrollment">
            <V label="Total" value={fmtNum(data.enrollment.total)} />
            {data.enrollment.mc_pct != null && <V label="Managed Care" value={`${data.enrollment.mc_pct.toFixed(1)}%`} />}
            {data.enrollment.year && <V label="Vintage" value={`${data.enrollment.month}/${data.enrollment.year}`} />}
          </Sec>
        )}

        {/* Workforce */}
        {data.workforce && show("workforce") && (
          <Sec sym={SYM.workforce} title="Workforce">
            {data.workforce.cna_median_wage != null && (
              <V label="CNA median" value={fmtDollar(data.workforce.cna_median_wage) + "/hr"}
                color={data.workforce.cna_median_wage < 16 ? C.neg : undefined} />
            )}
            {data.workforce.hha_median_wage != null && <V label="HHA median" value={fmtDollar(data.workforce.hha_median_wage) + "/hr"} />}
            {data.workforce.rn_median_wage != null && <V label="RN median" value={fmtDollar(data.workforce.rn_median_wage) + "/hr"} />}
          </Sec>
        )}

        {/* Rate Adequacy */}
        {data.rate_adequacy && show("rates") && (
          <Sec sym={SYM.rates} title="Rate Adequacy">
            <V label="Median % of Medicare" value={fmtPct(data.rate_adequacy.median_pct_medicare)}
              color={data.rate_adequacy.median_pct_medicare < 70 ? C.neg : data.rate_adequacy.median_pct_medicare < 85 ? C.warn : C.pos} />
            <V label="Codes below 80%" value={String(data.rate_adequacy.codes_below_80)} />
            <V label="Total codes" value={String(data.rate_adequacy.code_count)} />
          </Sec>
        )}

        {/* HCBS Waitlist */}
        {data.hcbs_waitlist?.total_waiting && show("hcbs") && (
          <Sec sym={SYM.hcbs} title="HCBS Waitlist">
            <V label="Total waiting" value={fmtNum(data.hcbs_waitlist.total_waiting)} color={C.warn} />
            {data.hcbs_waitlist.idd_waiting && <V label="I/DD waiting" value={fmtNum(data.hcbs_waitlist.idd_waiting)} />}
          </Sec>
        )}

        {/* Demographics */}
        {data.demographics && show("demographic") && (
          <Sec sym={SYM.demographic} title="Demographics">
            {data.demographics.population && <V label="Population" value={fmtNum(data.demographics.population)} />}
            {data.demographics.pct_poverty != null && <V label="Poverty" value={fmtPct(data.demographics.pct_poverty)}
              color={data.demographics.pct_poverty > 15 ? C.neg : undefined} />}
            {data.demographics.pct_uninsured != null && <V label="Uninsured" value={fmtPct(data.demographics.pct_uninsured)}
              color={data.demographics.pct_uninsured > 10 ? C.neg : undefined} />}
          </Sec>
        )}

        {/* T-MSIS Claims */}
        {data.tmsis_claims && show("tmsis") && (
          <Sec sym={SYM.tmsis} title="Claims-Based Rates (T-MSIS)" bg={TMSIS_BG} bd={TMSIS_BD}>
            <V label="Median % of Medicare" value={fmtPct(data.tmsis_claims.median_pct_medicare)} />
            <V label="Avg paid rate" value={fmtDollar(data.tmsis_claims.avg_paid_rate)} />
            <div style={{ fontSize: 9, color: C.warn, marginTop: 4, fontStyle: "italic", fontFamily: FONT.body }}>
              {data.tmsis_claims.caveat}
            </div>
          </Sec>
        )}

        {/* Supplemental */}
        {data.supplemental && show("supplemental") && (
          <Sec sym={SYM.supplemental} title="Supplemental Payments">
            {data.supplemental.dsh_total != null && <V label="DSH allotment" value={fmtB(data.supplemental.dsh_total)} />}
            {data.supplemental.sdp_count != null && <V label="SDP programs" value={String(data.supplemental.sdp_count)} />}
            {data.supplemental.sdp_total != null && <V label="SDP total" value={fmtB(data.supplemental.sdp_total)} />}
          </Sec>
        )}
      </div>
    </div>
  );
}
