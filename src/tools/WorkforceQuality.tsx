import { lazy, Suspense, useState, useMemo } from "react";
import { C, FONT } from "../design";
import { useAradune } from "../context/AraduneContext";

const WageAdequacy = lazy(() => import("./WageAdequacy"));
const QualityLinkage = lazy(() => import("./QualityLinkage"));
const HcbsTracker = lazy(() => import("./HcbsTracker"));
const ComplianceReport = lazy(() => import("./ComplianceReport"));
const WorkforceSupply = lazy(() => import("./WorkforceSupply"));
const ShortageAreas = lazy(() => import("./ShortageAreas"));

const TABS = [
  { key: "wages", label: "Wage Comparison", component: WageAdequacy },
  { key: "quality", label: "Quality Measures", component: QualityLinkage },
  { key: "hcbs", label: "HCBS Pass-Through", component: HcbsTracker },
  { key: "supply", label: "Workforce Supply", component: WorkforceSupply },
  { key: "shortage", label: "Shortage Areas", component: ShortageAreas },
  { key: "compliance", label: "Compliance", component: ComplianceReport },
] as const;

type TabKey = (typeof TABS)[number]["key"];

const ROUTE_TO_TAB: Record<string, TabKey> = {
  "/wages": "wages",
  "/adequacy": "wages",
  "/quality": "quality",
  "/hcbs8020": "hcbs",
  "/supply": "supply",
  "/shortage": "shortage",
  "/compliance": "compliance",
};

function parseTabFromHash(): TabKey {
  try {
    const hash = window.location.hash;
    const path = hash.slice(1).split("?")[0];
    if (ROUTE_TO_TAB[path]) return ROUTE_TO_TAB[path];
    const qIdx = hash.indexOf("?");
    if (qIdx !== -1) {
      const params = new URLSearchParams(hash.slice(qIdx + 1));
      const tab = params.get("tab");
      if (tab && TABS.some((t) => t.key === tab)) return tab as TabKey;
    }
  } catch {
    // ignore
  }
  return "wages";
}

export default function WorkforceQuality() {
  const [active, setActive] = useState<TabKey>(parseTabFromHash);
  const { openIntelligence, addReportSection } = useAradune();

  const tabBarStyle: React.CSSProperties = useMemo(
    () => ({
      display: "flex",
      alignItems: "center",
      gap: 0,
      borderBottom: `1px solid ${C.border}`,
      marginBottom: 16,
    }),
    [],
  );

  return (
    <div style={{ fontFamily: FONT.body }}>
      {/* Header row: tabs + intelligence button */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-end",
        }}
      >
        <div style={tabBarStyle}>
          {TABS.map((t) => {
            const isActive = t.key === active;
            return (
              <button
                key={t.key}
                onClick={() => setActive(t.key)}
                style={{
                  background: "none",
                  border: "none",
                  borderBottom: isActive
                    ? `2px solid ${C.brand}`
                    : "2px solid transparent",
                  padding: "6px 14px",
                  cursor: "pointer",
                  fontSize: 11,
                  fontFamily: FONT.body,
                  fontWeight: isActive ? 600 : 400,
                  color: isActive ? C.brand : C.inkLight,
                  transition: "color 0.15s, border-color 0.15s",
                }}
              >
                {t.label}
              </button>
            );
          })}
        </div>

        <div style={{ display: "flex", gap: 6, marginBottom: 6 }}>
          <button
            onClick={() =>
              openIntelligence({
                summary: `User is viewing Workforce & Quality — ${active} tab`,
              })
            }
            style={{
              background: C.brand,
              color: C.white,
              border: "none",
              borderRadius: 4,
              padding: "5px 12px",
              fontSize: 11,
              fontFamily: FONT.body,
              fontWeight: 500,
              cursor: "pointer",
              whiteSpace: "nowrap",
            }}
          >
            Ask Aradune
          </button>
          <button
            onClick={() =>
              addReportSection({
                id: crypto.randomUUID(),
                prompt: `Workforce & Quality — ${TABS.find(t => t.key === active)?.label || active}`,
                response: `Workforce & Quality module snapshot (${active} tab).`,
                queries: [],
                createdAt: new Date(),
              })
            }
            style={{
              background: "none",
              border: `1px solid ${C.border}`,
              borderRadius: 4,
              padding: "5px 12px",
              fontSize: 11,
              fontFamily: FONT.body,
              fontWeight: 500,
              cursor: "pointer",
              color: C.inkLight,
              whiteSpace: "nowrap",
            }}
          >
            + Report
          </button>
        </div>
      </div>

      {/* Tab content — display:none/block to preserve state */}
      {TABS.map((t) => (
        <div
          key={t.key}
          style={{ display: t.key === active ? "block" : "none" }}
        >
          <Suspense
            fallback={
              <div
                style={{
                  padding: 24,
                  fontSize: 12,
                  color: C.inkLight,
                  fontFamily: FONT.body,
                }}
              >
                Loading...
              </div>
            }
          >
            <t.component />
          </Suspense>
        </div>
      ))}
    </div>
  );
}
