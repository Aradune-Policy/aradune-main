import { lazy, Suspense, useState, useMemo } from "react";
import { C, FONT } from "../design";
import { useAradune } from "../context/AraduneContext";

const TmsisExplorer = lazy(() => import("./TmsisExplorer"));
const FeeScheduleDir = lazy(() => import("./FeeScheduleDir"));
const RateBuilder = lazy(() => import("./RateBuilder"));
const CpraGenerator = lazy(() => import("./CpraGenerator"));
const RateReduction = lazy(() => import("./RateReduction"));

const TABS = [
  { key: "browse", label: "Browse & Compare", component: TmsisExplorer },
  { key: "fees", label: "Fee Schedules", component: FeeScheduleDir },
  { key: "builder", label: "Rate Builder", component: RateBuilder },
  { key: "cpra", label: "CPRA Compliance", component: CpraGenerator },
  { key: "impact", label: "Impact Analysis", component: RateReduction },
] as const;

type TabKey = (typeof TABS)[number]["key"];

const ROUTE_TO_TAB: Record<string, TabKey> = {
  "/cpra": "cpra",
  "/fees": "fees",
  "/builder": "builder",
  "/reduction": "impact",
  "/explorer": "browse",
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
    // ignore
  }
  return "browse";
}

const LOADING_FALLBACK = (
  <div
    style={{
      padding: 48,
      textAlign: "center",
      color: C.inkLight,
      fontFamily: FONT.body,
      fontSize: 13,
    }}
  >
    Loading...
  </div>
);

export default function RateAnalysis() {
  const [active, setActive] = useState<TabKey>(parseInitialTab);
  const { openIntelligence, addReportSection } = useAradune();

  // Track which tabs have been activated so we mount them once and keep them alive
  const [mounted, setMounted] = useState<Set<TabKey>>(
    () => new Set([parseInitialTab()])
  );

  const handleTab = (key: TabKey) => {
    setActive(key);
    setMounted((prev) => {
      if (prev.has(key)) return prev;
      const next = new Set(prev);
      next.add(key);
      return next;
    });
  };

  const tabElements = useMemo(
    () =>
      TABS.map(({ key, component: Component }) => (
        <div
          key={key}
          style={{ display: active === key ? "block" : "none" }}
        >
          {mounted.has(key) && (
            <Suspense fallback={LOADING_FALLBACK}>
              <Component />
            </Suspense>
          )}
        </div>
      )),
    [active, mounted]
  );

  return (
    <div style={{ fontFamily: FONT.body }}>
      {/* Header row: tabs + intelligence button */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          borderBottom: `1px solid ${C.border}`,
          marginBottom: 0,
        }}
      >
        {/* Tab bar */}
        <div style={{ display: "flex", gap: 0 }}>
          {TABS.map(({ key, label }) => {
            const isActive = active === key;
            return (
              <button
                key={key}
                onClick={() => handleTab(key)}
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
                  letterSpacing: "0.02em",
                  transition: "color 0.15s, border-color 0.15s",
                }}
              >
                {label}
              </button>
            );
          })}
        </div>

        {/* Action buttons */}
        <div style={{ display: "flex", gap: 6, marginRight: 8 }}>
          <button
            onClick={() =>
              openIntelligence({ summary: `User is viewing Rate Analysis — ${active} tab` })
            }
            style={{
              background: C.brand,
              color: C.white,
              border: "none",
              borderRadius: 4,
              padding: "6px 14px",
              fontSize: 11,
              fontFamily: FONT.body,
              fontWeight: 500,
              cursor: "pointer",
              letterSpacing: "0.02em",
            }}
          >
            Ask Aradune
          </button>
          <button
            onClick={() =>
              addReportSection({
                id: crypto.randomUUID(),
                prompt: `Rate Analysis — ${TABS.find(t => t.key === active)?.label || active}`,
                response: `Rate Analysis module snapshot (${active} tab).`,
                queries: [],
                createdAt: new Date(),
              })
            }
            style={{
              background: "none",
              border: `1px solid ${C.border}`,
              borderRadius: 4,
              padding: "6px 14px",
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

      {/* Tab content */}
      {tabElements}
    </div>
  );
}
