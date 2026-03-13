import { lazy, Suspense, useState, useMemo } from "react";
import { C, FONT } from "../design";
import { useAradune } from "../context/AraduneContext";

const AheadReadiness = lazy(() => import("./AheadReadiness"));
const AheadCalculator = lazy(() => import("./AheadCalculator"));
const TmsisExplorer = lazy(() => import("./TmsisExplorer"));
const NursingFacilities = lazy(() => import("./NursingFacilities"));
const FacilityDirectory = lazy(() => import("./FacilityDirectory"));

const TABS = [
  { key: "readiness", label: "Hospital Readiness", component: AheadReadiness },
  { key: "ahead", label: "AHEAD Calculator", component: AheadCalculator },
  { key: "nursing", label: "Nursing Facilities", component: NursingFacilities },
  { key: "directory", label: "Facility Directory", component: FacilityDirectory },
  { key: "spending", label: "Spending Explorer", component: TmsisExplorer },
] as const;

type TabKey = (typeof TABS)[number]["key"];

const ROUTE_TO_TAB: Record<string, TabKey> = {
  "/hospitals": "readiness",
  "/ahead-readiness": "readiness",
  "/ahead": "ahead",
  "/nursing-facilities": "nursing",
  "/directory": "directory",
  "/explorer": "spending",
};

function parseInitialTab(): TabKey {
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
    /* ignore */
  }
  return "readiness";
}

export default function ProviderIntelligence() {
  const [active, setActive] = useState<TabKey>(parseInitialTab);
  const { openIntelligence, addReportSection } = useAradune();

  const tabElements = useMemo(
    () =>
      TABS.map(({ key, component: Comp }) => (
        <div
          key={key}
          style={{ display: active === key ? "block" : "none" }}
        >
          <Suspense
            fallback={
              <div style={{ padding: 24, fontSize: 12, color: C.inkLight, fontFamily: FONT.body }}>
                Loading…
              </div>
            }
          >
            <Comp />
          </Suspense>
        </div>
      )),
    [active],
  );

  return (
    <div style={{ fontFamily: FONT.body }}>
      {/* Tab bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: `1px solid ${C.border}`,
          padding: "0 16px",
        }}
      >
        <div style={{ display: "flex", gap: 0 }}>
          {TABS.map(({ key, label }) => {
            const isActive = active === key;
            return (
              <button
                key={key}
                onClick={() => setActive(key)}
                style={{
                  background: "none",
                  border: "none",
                  borderBottom: isActive
                    ? `2px solid ${C.brand}`
                    : "2px solid transparent",
                  padding: "10px 16px",
                  fontSize: 11,
                  fontFamily: FONT.body,
                  fontWeight: isActive ? 600 : 400,
                  color: isActive ? C.brand : C.inkLight,
                  cursor: "pointer",
                  transition: "color 0.15s, border-color 0.15s",
                }}
              >
                {label}
              </button>
            );
          })}
        </div>

        <div style={{ display: "flex", gap: 6 }}>
          <button
            onClick={() =>
              openIntelligence({
                summary: `User is viewing Provider Intelligence — ${active} tab`,
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
            }}
          >
            Ask Aradune
          </button>
          <button
            onClick={() =>
              addReportSection({
                id: crypto.randomUUID(),
                prompt: `Provider Intelligence — ${TABS.find(t => t.key === active)?.label || active}`,
                response: `Provider Intelligence module snapshot (${active} tab).`,
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
            }}
          >
            + Report
          </button>
        </div>
      </div>

      {/* Tab content — all rendered, toggled via display */}
      {tabElements}
    </div>
  );
}
