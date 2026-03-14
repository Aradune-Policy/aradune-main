/**
 * LandingMetrics.tsx — Headline stats for the Intelligence landing page.
 *
 * 4 hardcoded "only Aradune can tell you this" metrics.
 * Hardcoded values are fine for launch (per RESEARCH-HANDOFF.md).
 * This component is designed to be easily removable — single import in IntelligenceChat.
 */

import { C, FONT, SHADOW } from "../design";

const METRICS: {
  value: string;
  label: string;
  source: string;
  accent: string;
}[] = [
  {
    value: "$3.4B",
    label: "Annual pharmacy overpayment above acquisition cost",
    source: "NADAC vs. SDUD 2023",
    accent: C.brand,
  },
  {
    value: "$113B",
    label: "MCO administrative & profit retention per year",
    source: "MCO MLR reports, 8.5% of premiums",
    accent: "#3A7CC4",
  },
  {
    value: "606,895",
    label: "People on HCBS waitlists nationwide",
    source: "CMS 372 / KFF waiver data",
    accent: "#C4590A",
  },
  {
    value: "-1.3pp",
    label: "Annual quality score decline, 2017-2024",
    source: "Medicaid Core Set panel trend",
    accent: "#7B4EA3",
  },
];

export default function LandingMetrics() {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))",
        gap: 14,
        width: "100%",
        maxWidth: 720,
        marginBottom: 28,
      }}
    >
      {METRICS.map((m) => (
        <div
          key={m.label}
          style={{
            background: C.white,
            borderRadius: 8,
            boxShadow: SHADOW,
            borderTop: `3px solid ${m.accent}`,
            padding: "18px 16px 14px",
            textAlign: "left",
          }}
        >
          <div
            style={{
              fontSize: 26,
              fontWeight: 700,
              color: C.ink,
              fontFamily: FONT.mono,
              lineHeight: 1.1,
              marginBottom: 6,
            }}
          >
            {m.value}
          </div>
          <div
            style={{
              fontSize: 11,
              color: C.ink,
              fontFamily: FONT.body,
              lineHeight: 1.4,
              marginBottom: 8,
            }}
          >
            {m.label}
          </div>
          <div
            style={{
              fontSize: 9,
              color: C.inkLight,
              fontFamily: FONT.mono,
              lineHeight: 1.3,
            }}
          >
            {m.source}
          </div>
        </div>
      ))}
    </div>
  );
}
