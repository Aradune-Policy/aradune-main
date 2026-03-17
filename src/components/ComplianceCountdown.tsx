/**
 * ComplianceCountdown.tsx
 * 42 CFR 447.203 "Ensuring Access" compliance readiness overview.
 * Shows countdown to July 1, 2026 deadline and maps each subsection
 * of (b)(1)-(b)(5) to the Aradune tools that address it.
 *
 * Can be embedded in the landing page or rendered standalone at /#/compliance-countdown.
 */

import { useState, useEffect } from "react";
import { C, FONT, SHADOW, SHADOW_LG, useIsMobile } from "../design";

// ── Countdown calculation ────────────────────────────────────────────────
const DEADLINE = new Date("2026-07-01T00:00:00");

function useDaysRemaining() {
  const [days, setDays] = useState(() => {
    const now = new Date();
    return Math.ceil((DEADLINE.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
  });
  useEffect(() => {
    const interval = setInterval(() => {
      const now = new Date();
      setDays(Math.ceil((DEADLINE.getTime() - now.getTime()) / (1000 * 60 * 60 * 24)));
    }, 60_000); // update every minute
    return () => clearInterval(interval);
  }, []);
  return days;
}

// ── Subsection data ──────────────────────────────────────────────────────
interface AraduneTool {
  name: string;
  route: string;
}

interface Subsection {
  id: string;
  title: string;
  description: string;
  tools: AraduneTool[];
}

const SUBSECTIONS: Subsection[] = [
  {
    id: "(b)(1)",
    title: "Rate transparency",
    description:
      "States must publish all FFS payment rates and methodologies. Fee schedules must be publicly accessible and machine-readable.",
    tools: [
      { name: "Rate Explorer", route: "#/rate-explorer" },
      { name: "Fee Schedule Directory", route: "#/fees" },
      { name: "CPRA Generator", route: "#/cpra" },
    ],
  },
  {
    id: "(b)(2)",
    title: "Comparative payment rate analysis (CPRA)",
    description:
      "States must publish a Comparative Payment Rate Analysis comparing Medicaid rates to Medicare for E&M service categories. 68 procedure codes, $32.3465 CY2025 conversion factor, many-to-many code-category mapping.",
    tools: [
      { name: "CPRA Generator", route: "#/cpra" },
      { name: "Rate Explorer", route: "#/rate-explorer" },
      { name: "Fee Schedule Directory", route: "#/fees" },
    ],
  },
  {
    id: "(b)(3)",
    title: "Payment rate disclosure",
    description:
      "States must publicly disclose payment rates for each covered service, including base rates, supplemental payments, and rate-setting methodology.",
    tools: [
      { name: "Rate Explorer", route: "#/rate-explorer" },
      { name: "CPRA Generator", route: "#/cpra" },
      { name: "Fee Schedule Directory", route: "#/fees" },
    ],
  },
  {
    id: "(b)(4)",
    title: "Rate reduction analysis",
    description:
      "Before reducing or restructuring payment rates, states must analyze the effect on access to care, provider participation, and beneficiary utilization.",
    tools: [
      { name: "Rate Reduction Analyzer", route: "#/reduction" },
    ],
  },
  {
    id: "(b)(5)",
    title: "HCBS payment adequacy",
    description:
      "States must demonstrate that HCBS rates are sufficient to ensure adequate provider networks. Includes the 80% direct care worker compensation pass-through requirement.",
    tools: [
      { name: "HCBS Tracker", route: "#/hcbs8020" },
      { name: "Wage Adequacy", route: "#/wages" },
    ],
  },
];

// ── Urgency color based on days remaining ────────────────────────────────
function getUrgencyColor(days: number): string {
  if (days <= 0) return C.neg;
  if (days <= 90) return C.neg;
  if (days <= 180) return C.warn;
  return C.brand;
}

function getUrgencyLabel(days: number): string {
  if (days <= 0) return "PAST DUE";
  if (days <= 90) return "CRITICAL";
  if (days <= 180) return "APPROACHING";
  return "ON TRACK";
}

// ── Component ────────────────────────────────────────────────────────────
export default function ComplianceCountdown() {
  const days = useDaysRemaining();
  const isMobile = useIsMobile();
  const urgencyColor = getUrgencyColor(days);
  const urgencyLabel = getUrgencyLabel(days);

  return (
    <div style={{
      maxWidth: 780,
      margin: "0 auto",
      padding: isMobile ? "28px 14px 48px" : "40px 20px 60px",
    }}>
      {/* ── Header + Countdown ──────────────────────────────────────── */}
      <div style={{
        background: C.white,
        borderRadius: 12,
        boxShadow: SHADOW,
        padding: isMobile ? "20px 16px" : "28px 32px",
        marginBottom: 24,
        borderTop: `3px solid ${urgencyColor}`,
      }}>
        <div style={{
          display: "flex",
          alignItems: isMobile ? "flex-start" : "center",
          justifyContent: "space-between",
          flexDirection: isMobile ? "column" : "row",
          gap: isMobile ? 16 : 24,
        }}>
          <div style={{ flex: 1 }}>
            <div style={{
              fontSize: 10,
              fontWeight: 700,
              color: urgencyColor,
              fontFamily: FONT.mono,
              letterSpacing: 1.5,
              textTransform: "uppercase",
              marginBottom: 6,
            }}>
              42 CFR 447.203 -- Ensuring Access
            </div>
            <h1 style={{
              margin: 0,
              fontSize: isMobile ? 18 : 22,
              fontWeight: 800,
              color: C.ink,
              letterSpacing: -0.5,
              lineHeight: 1.3,
            }}>
              CPRA Compliance Deadline
            </h1>
            <p style={{
              margin: "8px 0 0",
              fontSize: 13,
              color: C.inkLight,
              lineHeight: 1.6,
              maxWidth: 440,
            }}>
              The first national transparency and adequacy requirements for
              Medicaid rate-setting. States must publish CPRAs, fee schedules,
              and HCBS payment adequacy demonstrations by July 1, 2026.
            </p>
          </div>

          {/* Countdown block */}
          <div style={{
            textAlign: "center",
            flexShrink: 0,
            minWidth: 140,
            padding: "16px 20px",
            background: C.surface,
            borderRadius: 10,
            border: `1px solid ${C.border}`,
          }}>
            <div style={{
              fontSize: 42,
              fontWeight: 800,
              color: urgencyColor,
              fontFamily: FONT.mono,
              lineHeight: 1,
              marginBottom: 4,
            }}>
              {days <= 0 ? "0" : days}
            </div>
            <div style={{
              fontSize: 12,
              color: C.inkLight,
              fontWeight: 500,
              marginBottom: 8,
            }}>
              days remaining
            </div>
            <div style={{
              display: "inline-block",
              fontSize: 9,
              fontWeight: 700,
              fontFamily: FONT.mono,
              color: C.white,
              background: urgencyColor,
              padding: "2px 8px",
              borderRadius: 4,
              letterSpacing: 0.5,
            }}>
              {urgencyLabel}
            </div>
          </div>
        </div>
      </div>

      {/* ── Subsections grid ────────────────────────────────────────── */}
      <div style={{ marginBottom: 8 }}>
        <h2 style={{
          fontSize: isMobile ? 15 : 17,
          fontWeight: 700,
          color: C.ink,
          margin: "0 0 4px",
        }}>
          Requirements by subsection
        </h2>
        <p style={{
          fontSize: 12,
          color: C.inkLight,
          margin: "0 0 16px",
          lineHeight: 1.6,
        }}>
          Each subsection of 447.203(b) imposes distinct obligations.
          Aradune tools map directly to the analytical work required for compliance.
        </p>
      </div>

      <div style={{ display: "grid", gap: 12 }}>
        {SUBSECTIONS.map((sub) => (
          <SubsectionCard key={sub.id} sub={sub} isMobile={isMobile} />
        ))}
      </div>

      {/* ── Bottom CTA ──────────────────────────────────────────────── */}
      <div style={{
        marginTop: 24,
        padding: isMobile ? "16px 14px" : "16px 24px",
        background: C.surface,
        borderRadius: 10,
        border: `1px solid ${C.border}`,
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        flexWrap: "wrap",
        gap: 12,
      }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.ink }}>
            Need help preparing for the deadline?
          </div>
          <div style={{ fontSize: 11, color: C.inkLight, marginTop: 2 }}>
            Rate studies, CPRA generation, fee schedule publication, HCBS adequacy analysis
          </div>
        </div>
        <a
          href="#/compliance"
          style={{
            padding: "8px 18px",
            background: C.brand,
            color: C.white,
            borderRadius: 6,
            fontSize: 12,
            fontWeight: 600,
            textDecoration: "none",
            whiteSpace: "nowrap",
          }}
        >
          Open Compliance Center
        </a>
      </div>

      {/* ── Source citation ──────────────────────────────────────────── */}
      <div style={{
        marginTop: 16,
        fontSize: 10,
        color: C.inkLight,
        lineHeight: 1.6,
      }}>
        Source: CMS-2442-F1 "Medicaid Program; Ensuring Access to Medicaid Services"
        (89 FR 40541, May 10, 2024). Subsection references: 42 CFR 447.203(b)(1)-(b)(5).
        Compliance deadline: July 1, 2026.
      </div>
    </div>
  );
}

// ── Subsection card ──────────────────────────────────────────────────────
function SubsectionCard({ sub, isMobile }: { sub: Subsection; isMobile: boolean }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div
      style={{
        background: C.white,
        borderRadius: 10,
        boxShadow: SHADOW,
        padding: isMobile ? "14px 14px" : "16px 22px",
        borderLeft: `3px solid ${C.brand}`,
        transition: "box-shadow 0.2s",
      }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLElement).style.boxShadow = SHADOW_LG;
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLElement).style.boxShadow = SHADOW;
      }}
    >
      {/* Header row */}
      <div
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 10,
          cursor: "pointer",
        }}
        onClick={() => setExpanded(!expanded)}
      >
        <span style={{
          fontSize: 11,
          fontWeight: 700,
          fontFamily: FONT.mono,
          color: C.brand,
          flexShrink: 0,
          minWidth: 40,
        }}>
          {sub.id}
        </span>
        <span style={{
          fontSize: 14,
          fontWeight: 600,
          color: C.ink,
          flex: 1,
        }}>
          {sub.title}
        </span>
        <span style={{
          fontSize: 11,
          color: C.inkLight,
          fontFamily: FONT.mono,
          flexShrink: 0,
          transform: expanded ? "rotate(180deg)" : "rotate(0deg)",
          transition: "transform 0.15s",
        }}>
          v
        </span>
      </div>

      {/* Expanded content */}
      {expanded && (
        <div style={{ marginTop: 12, paddingLeft: 50 }}>
          <p style={{
            margin: "0 0 12px",
            fontSize: 12,
            color: C.inkLight,
            lineHeight: 1.65,
            maxWidth: 560,
          }}>
            {sub.description}
          </p>

          {/* Aradune tools */}
          <div style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 6,
          }}>
            {sub.tools.map((tool) => (
              <a
                key={tool.name}
                href={tool.route}
                style={{
                  display: "inline-block",
                  padding: "5px 12px",
                  fontSize: 11,
                  fontWeight: 600,
                  color: C.brand,
                  background: `${C.brand}0A`,
                  border: `1px solid ${C.brand}20`,
                  borderRadius: 5,
                  textDecoration: "none",
                  transition: "background 0.15s, border-color 0.15s",
                }}
                onMouseEnter={(e) => {
                  (e.currentTarget as HTMLElement).style.background = `${C.brand}18`;
                  (e.currentTarget as HTMLElement).style.borderColor = `${C.brand}40`;
                }}
                onMouseLeave={(e) => {
                  (e.currentTarget as HTMLElement).style.background = `${C.brand}0A`;
                  (e.currentTarget as HTMLElement).style.borderColor = `${C.brand}20`;
                }}
              >
                {tool.name}
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
