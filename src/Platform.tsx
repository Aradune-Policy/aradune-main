import { useState, useEffect, lazy, Suspense, Component } from "react";
import type { ReactNode, ErrorInfo } from "react";
import { C, FONT, SHADOW, SHADOW_LG } from "./design";
import type { ToolDef, NavGroup } from "./types";
import { STATES_LIST, STATE_NAMES } from "./data/states";
import Term from "./components/Term";
import NavDrop from "./components/NavDrop";
import NavSearch from "./components/NavSearch";
import Lottie from "lottie-react";

// ── Sword Loading Animation ──────────────────────────────────────────────
function SwordLoader({ text = "Loading..." }: { text?: string }) {
  const [animData, setAnimData] = useState<object | null>(null);
  useEffect(() => {
    fetch("/assets/sword-animation.json")
      .then(r => r.json())
      .then(setAnimData)
      .catch(() => {});
  }, []);
  return (
    <div style={{ maxWidth: 600, margin: "0 auto", padding: "60px 20px", textAlign: "center", display: "flex", flexDirection: "column", alignItems: "center" }}>
      {animData ? (
        <Lottie animationData={animData} loop style={{ width: 80, height: 140 }} />
      ) : null}
      <div style={{ fontSize: 12, color: C.inkLight, marginTop: 8, fontFamily: FONT.body }}>{text}</div>
    </div>
  );
}

// ── Error Boundary ──────────────────────────────────────────────────────
class ToolErrorBoundary extends Component<{ children: ReactNode }, { error: string | null }> {
  state = { error: null as string | null };
  static getDerivedStateFromError(err: Error) { return { error: err.message || "Unknown error" }; }
  componentDidCatch(err: Error, info: ErrorInfo) { console.error("Tool render error:", err, info); }
  render() {
    if (this.state.error) return (
      <div style={{ maxWidth: 600, margin: "0 auto", padding: "80px 20px", textAlign: "center" }}>
        <div style={{ fontSize: 15, fontWeight: 600, color: "#A4262C", marginBottom: 8 }}>Something went wrong</div>
        <div style={{ fontSize: 12, color: "#425A70", fontFamily: "'SF Mono',Menlo,monospace", marginBottom: 16 }}>{this.state.error}</div>
        <a href="#/" style={{ fontSize: 13, color: "#2E6B4A", textDecoration: "none" }}>Back to Aradune</a>
      </div>
    );
    return this.props.children;
  }
}

// ── Lazy-loaded tools (code-split per route) ────────────────────────────
const TmsisExplorer = lazy(() => import("./tools/TmsisExplorer"));
const WageAdequacy = lazy(() => import("./tools/WageAdequacy"));
const QualityLinkage = lazy(() => import("./tools/QualityLinkage"));
const RateDecay = lazy(() => import("./tools/RateDecay"));
const RateBuilder = lazy(() => import("./tools/RateBuilder"));
const PolicyAnalyst = lazy(() => import("./tools/PolicyAnalyst"));
const AheadCalculator = lazy(() => import("./tools/AheadCalculator"));
const RateReduction = lazy(() => import("./tools/RateReduction"));
const HcbsTracker = lazy(() => import("./tools/HcbsTracker"));
const FeeScheduleDir = lazy(() => import("./tools/FeeScheduleDir"));
const RateLookup = lazy(() => import("./tools/RateLookup"));
const ComplianceReport = lazy(() => import("./tools/ComplianceReport"));
const CpraGenerator = lazy(() => import("./tools/CpraGenerator"));
const AheadReadiness = lazy(() => import("./tools/AheadReadiness"));
const CaseloadForecaster = lazy(() => import("./tools/CaseloadForecaster"));
const StateProfile = lazy(() => import("./tools/StateProfile"));
const DataExplorer = lazy(() => import("./tools/DataExplorer"));
const DataCatalog = lazy(() => import("./tools/DataCatalog"));

// ── Hash Router ──────────────────────────────────────────────────────────
function useRoute() {
  const [route, setRoute] = useState(window.location.hash.slice(1) || "/");
  useEffect(() => {
    const handler = () => setRoute(window.location.hash.slice(1) || "/");
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);
  return route;
}

// ── Tool Registry ────────────────────────────────────────────────────────
const TOOLS: ToolDef[] = [
  // ── STATE PROFILE ─────────────────────────────────────────────────────
  {
    id: "state", group: "explore", name: "State Profile",
    tagline: "Everything Aradune knows about a state, in one view",
    desc: "Enrollment, rates, hospitals, quality, workforce, pharmacy, and economic context — unified from 250 fact tables.",
    status: "live", icon: "◉", color: C.brand,
  },
  // ── TRANSPARENCY ──────────────────────────────────────────────────────
  {
    id: "explorer", group: "explore", name: "Spending Explorer",
    tagline: "Search and compare Medicaid spending across every state",
    desc: "Query 190M+ FFS claims across 54 jurisdictions with cross-state rate comparisons, provider analysis, and a full SQL editor.",
    status: "live", icon: "⌕", color: C.brand,
  },
  {
    id: "decay", group: "explore", name: "Medicare Comparison",
    navLabel: "Medicare Comparison",
    tagline: "Medicaid rates as a percentage of Medicare, by code and state",
    desc: "Every HCPCS code as a percentage of the Medicare PFS, by state. Identify where Medicaid has fallen furthest behind.",
    status: "live", icon: "◧", color: C.brand,
  },
  {
    id: "fees", group: "explore", name: "State Fee Schedule Directory",
    tagline: "Fee schedules, methodologies, and access for every state",
    desc: "Every state's Medicaid fee schedule in one place: methodology type, format, access requirements, spending context, and CMS compliance readiness.",
    status: "live", icon: "⊞", color: C.brand,
  },
  {
    id: "lookup", group: "explore", name: "Rate Lookup",
    tagline: "Search any HCPCS code, compare fee schedule rates across states",
    desc: "Type a HCPCS code and instantly see every state's fee schedule rate vs Medicare. 16,000+ codes across 40+ states.",
    status: "live", icon: "⊘", color: C.brand,
  },
  {
    id: "compliance", group: "explore", name: "Compliance Report",
    tagline: "Access Rule compliance checklist and rate transparency package",
    desc: "Unified compliance package for 42 CFR §447.203: Medicare parity, rate reduction modeling, methodology documentation, and export-ready formats.",
    status: "live", icon: "◇", color: C.brand,
  },
  {
    id: "cpra", group: "explore", name: "CPRA Generator",
    tagline: "Comparative Payment Rate Analysis for 42 CFR 447.203 compliance",
    desc: "Generate the Comparative Payment Rate Analysis required by July 2026. Medicaid vs Medicare rates across primary care, OB/GYN, and MH/SUD with PDF, Excel, and HTML export.",
    status: "live", icon: "◆", color: C.brand,
  },
  // ── ADEQUACY ──────────────────────────────────────────────────────────
  {
    id: "wages", group: "analyze", name: "Rate & Wage Comparison",
    tagline: "BLS market wages vs Medicaid reimbursement rates",
    desc: "Medicaid reimbursement vs. BLS market wages for healthcare occupations across every state.",
    status: "live", icon: "⊿", color: C.accent,
  },
  {
    id: "quality", group: "analyze", name: "Quality Linkage",
    tagline: "CMS Core Set outcomes mapped to payment rates",
    desc: "55 CMS Core Set measures across 52 jurisdictions. See whether higher rates correlate with better outcomes.",
    status: "live", icon: "◈", color: C.accent,
  },
  {
    id: "reduction", group: "analyze", name: "Rate Reduction Analyzer",
    tagline: "Analyze the impact of proposed Medicaid rate reductions",
    desc: "Model proposed rate cuts against access thresholds and Medicare ratios, code by code.",
    status: "live", icon: "▼", color: C.accent,
  },
  {
    id: "hcbs8020", group: "analyze", name: "HCBS Compensation Tracker",
    tagline: "How much of HCBS spending reaches direct care workers?",
    desc: "Track HCBS payment share reaching direct care workers against the 80/20 pass-through benchmark, by state and code.",
    status: "live", icon: "⊕", color: C.accent,
  },
  // ── MODELING ──────────────────────────────────────────────────────────
  {
    id: "builder", group: "build", name: "Rate Builder",
    tagline: "Calculate Medicaid rates with full audit trails",
    desc: "RBRVS rate calculations with real conversion factors, RVU components, and state-specific rules.",
    status: "live", icon: "⬡", color: C.teal,
  },
  {
    id: "ahead", group: "build", name: "AHEAD Calculator",
    tagline: "Model hospital global budgets under CMS's AHEAD demonstration",
    desc: "Model hospital global budgets under AHEAD parameters. Compare participation scenarios and savings targets.",
    status: "live", icon: "△", color: C.teal,
  },
  {
    id: "ahead-readiness", group: "build", name: "AHEAD Readiness Score",
    tagline: "Scored dashboard: how ready is your hospital for a global budget?",
    desc: "Enter a CCN. Aradune scores your hospital across four dimensions — financial stability, revenue concentration, supplemental exposure, and volume stability — using public HCRIS and CMS data.",
    status: "live", icon: "⬢", color: C.teal,
  },
  {
    id: "forecast", group: "build", name: "Caseload Forecaster",
    tagline: "Upload enrollment data and forecast caseload trends",
    desc: "Upload your state's monthly enrollment by category. Aradune runs SARIMAX + ETS model competition with intervention detection, economic enrichment, and 80/95% confidence intervals.",
    status: "live", icon: "◐", color: C.teal,
  },
  {
    id: "ask", group: "explore", name: "Data Explorer",
    tagline: "Ask questions about Medicaid data in plain English",
    desc: "Type a question. Aradune translates it to SQL, runs it against 185 tables and 101M+ rows, and returns the answer with the query.",
    status: "live", icon: "⌗", color: C.brand,
  },
  {
    id: "catalog", group: "explore", name: "Data Catalog",
    tagline: "Browse all tables in the Aradune data lake",
    desc: "250 fact tables, 9 dimensions, and 9 reference tables — with row counts, column schemas, and descriptions. See what data is available.",
    status: "live", icon: "☰", color: C.brand,
  },
  {
    id: "analyst", group: "build", name: "Policy Analyst",
    tagline: "AI-powered rate analysis and SPA language drafting",
    desc: "Ask questions in plain English. Get answers grounded in real data: rates, comparisons, fiscal impact, and draft SPA language.",
    status: "beta", icon: "◎", color: C.teal,
  },
];

const NAV_GROUPS: NavGroup[] = [
  { key: "explore", label: "Explore", tools: TOOLS.filter(t => t.group === "explore") },
  { key: "analyze", label: "Analyze", tools: TOOLS.filter(t => t.group === "analyze") },
  { key: "build", label: "Build", tools: TOOLS.filter(t => t.group === "build") },
];

const GROUP_COLORS: Record<string, string> = { explore: C.brand, analyze: C.accent, build: C.teal };
const GROUP_DESCS: Record<string, string> = {
  explore: "Browse and query rate data across every state and service code.",
  analyze: "Three lenses on rate adequacy — workforce wages, health outcomes, and Medicare benchmarking.",
  build: "Calculate rates, model scenarios, draft policy, and build the case for change.",
};

// ── Responsive hook ──────────────────────────────────────────────────────
function useIsMobile(breakpoint = 768) {
  const [isMobile, setIsMobile] = useState(typeof window !== "undefined" ? window.innerWidth < breakpoint : false);
  useEffect(() => {
    const handler = () => setIsMobile(window.innerWidth < breakpoint);
    window.addEventListener("resize", handler);
    return () => window.removeEventListener("resize", handler);
  }, [breakpoint]);
  return isMobile;
}

// ── Platform Nav ─────────────────────────────────────────────────────────
function PlatformNav({ route }: { route: string }) {
  const activeTool = TOOLS.find(t => route === `/${t.id}`);
  const isMobile = useIsMobile();
  const [mobileOpen, setMobileOpen] = useState(false);

  // Close mobile menu on route change
  useEffect(() => { setMobileOpen(false); }, [route]);

  return (
    <nav style={{
      position: "sticky", top: 0, zIndex: 100,
      background: "rgba(250,251,250,0.92)",
      backdropFilter: "blur(12px)",
      WebkitBackdropFilter: "blur(12px)",
      borderBottom: `1px solid ${C.border}`,
    }}>
      <div style={{
        maxWidth: 1080, margin: "0 auto", padding: "0 12px",
        display: "flex", alignItems: "center", justifyContent: "space-between", height: 48,
        gap: 4,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, flexShrink: 0 }}>
          <a href="#/" style={{ textDecoration: "none", display: "flex", alignItems: "center" }}>
            <img src="/assets/logo-wordmark.svg" alt="Aradune" style={{ height: 28 }} />
          </a>
          {activeTool && (
            <>
              <span style={{ color: C.border, fontSize: 13 }}>/</span>
              <span style={{ fontSize: 12, color: C.inkLight, fontWeight: 500, fontFamily: FONT.body, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 120 }}>{activeTool.name}</span>
            </>
          )}
        </div>

        {/* Desktop nav */}
        {!isMobile && (
          <div style={{ display: "flex", gap: 2, alignItems: "center", flexShrink: 1, minWidth: 0 }}>
            <NavSearch tools={TOOLS} />
            {route !== "/" && (
              <a href="#/" style={{ fontSize: 11, color: C.inkLight, textDecoration: "none", padding: "4px 10px", borderRadius: 6, fontFamily: FONT.body, whiteSpace: "nowrap" }}>
                All Tools
              </a>
            )}
            {NAV_GROUPS.map(g => <NavDrop key={g.key} group={g} route={route} />)}
            <a href="#/about" style={{
              fontSize: 11, fontFamily: FONT.body,
              color: route === "/about" ? C.brand : C.inkLight,
              fontWeight: route === "/about" ? 600 : 400,
              textDecoration: "none", padding: "4px 10px", whiteSpace: "nowrap",
            }}>
              About
            </a>
          </div>
        )}

        {/* Mobile hamburger */}
        {isMobile && (
          <button aria-label={mobileOpen ? "Close navigation menu" : "Open navigation menu"} onClick={() => setMobileOpen(!mobileOpen)} style={{
            background: "none", border: "none", cursor: "pointer", padding: "6px",
            display: "flex", flexDirection: "column", gap: 4, justifyContent: "center",
          }}>
            <span style={{ display: "block", width: 18, height: 2, background: C.ink, borderRadius: 1, transition: "all .2s", transform: mobileOpen ? "rotate(45deg) translate(3px,3px)" : "none" }}/>
            <span style={{ display: "block", width: 18, height: 2, background: C.ink, borderRadius: 1, transition: "all .2s", opacity: mobileOpen ? 0 : 1 }}/>
            <span style={{ display: "block", width: 18, height: 2, background: C.ink, borderRadius: 1, transition: "all .2s", transform: mobileOpen ? "rotate(-45deg) translate(3px,-3px)" : "none" }}/>
          </button>
        )}
      </div>

      {/* Mobile dropdown menu */}
      {isMobile && mobileOpen && (
        <div style={{
          background: C.white, borderTop: `1px solid ${C.border}`,
          padding: "8px 0", boxShadow: SHADOW_LG,
        }}>
          {route !== "/" && (
            <a href="#/" onClick={() => setMobileOpen(false)} style={{
              display: "block", padding: "10px 20px", textDecoration: "none",
              fontSize: 13, fontWeight: 500, color: C.ink, fontFamily: FONT.body,
            }}>
              All Tools
            </a>
          )}
          {NAV_GROUPS.map(g => (
            <div key={g.key}>
              <div style={{ padding: "8px 20px 4px", fontSize: 9, fontWeight: 700, color: g.key === "transparency" ? C.brand : g.key === "adequacy" ? C.accent : C.teal, textTransform: "uppercase", letterSpacing: 1, fontFamily: FONT.mono }}>
                {g.label}
              </div>
              {g.tools.map(t => {
                const isLive = t.status === "live" || t.status === "beta";
                return (
                  <a key={t.id} href={`#/${t.id}`} onClick={() => setMobileOpen(false)} style={{
                    display: "flex", alignItems: "center", gap: 10, padding: "8px 20px 8px 28px",
                    textDecoration: "none", background: route === `/${t.id}` ? `${C.brand}08` : "transparent",
                  }}>
                    <span style={{ fontSize: 12, color: t.color, flexShrink: 0 }}>{t.icon}</span>
                    <span style={{ fontSize: 12, color: C.ink, fontWeight: route === `/${t.id}` ? 600 : 400, fontFamily: FONT.body }}>{t.name}</span>
                    {!isLive && <span style={{ fontSize: 8, color: C.inkLight, fontFamily: FONT.mono, marginLeft: "auto" }}>SOON</span>}
                  </a>
                );
              })}
            </div>
          ))}
          <a href="#/about" onClick={() => setMobileOpen(false)} style={{
            display: "block", padding: "10px 20px", textDecoration: "none",
            fontSize: 13, fontWeight: route === "/about" ? 600 : 400, color: route === "/about" ? C.brand : C.ink, fontFamily: FONT.body,
            borderTop: `1px solid ${C.border}`, marginTop: 4,
          }}>
            About
          </a>
        </div>
      )}
    </nav>
  );
}

// ── Landing Page ─────────────────────────────────────────────────────────
function Landing() {
  const isMobile = useIsMobile();
  const [startTab, setStartTab] = useState("state");
  const [st, setSt] = useState("");
  const [serviceQ, setServiceQ] = useState("");

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px", overflowX: "hidden", fontFamily: FONT.body }}>

      {/* 1. Hero */}
      <div style={{ padding: "56px 0 44px", maxWidth: 640 }}>
        <h1 style={{ fontSize: isMobile ? 22 : 30, fontWeight: 700, color: C.ink, lineHeight: 1.25, letterSpacing: -0.5, margin: 0 }}>
          Every Medicaid dataset, one platform.
        </h1>
        <p style={{ fontSize: 14, color: C.inkLight, lineHeight: 1.7, marginTop: 14, maxWidth: 540 }}>
          101 million rows of Medicaid data — rates, enrollment, hospitals, quality,
          workforce, pharmacy, and economics — normalized, cross-referenced, and
          queryable in plain English. Open and free.
        </p>
        <div style={{ display: "flex", gap: 10, marginTop: 22, flexWrap: "wrap" }}>
          <a href="#/ask" style={{
            display: "inline-flex", alignItems: "center", padding: "10px 20px",
            background: C.brand, color: C.white, borderRadius: 8,
            fontSize: 13, fontWeight: 600, textDecoration: "none",
          }}>
            Ask a Question
          </a>
          <a href="#/explorer" style={{
            display: "inline-flex", alignItems: "center", padding: "10px 20px",
            background: "transparent", color: C.inkLight, borderRadius: 8,
            fontSize: 13, fontWeight: 500, textDecoration: "none",
            border: `1px solid ${C.border}`,
          }}>
            Explore Data
          </a>
        </div>
      </div>

      {/* 2. Stats row */}
      <div style={{
        display: "grid", gridTemplateColumns: `repeat(auto-fit,minmax(${isMobile ? "70px" : "130px"},1fr))`,
        gap: isMobile ? 10 : 16, padding: "20px 0 36px", borderTop: `1px solid ${C.border}`,
      }}>
        {([["18", "analytical tools"], ["54", "states & territories"], ["115M+", "data lake rows"], ["250", "fact tables"]] as const).map(([val, label]) => (
          <div key={label}>
            <div style={{ fontSize: 20, fontWeight: 700, fontFamily: FONT.mono, color: C.brand, letterSpacing: -0.5 }}>{val}</div>
            <div style={{ fontSize: 11, color: C.inkLight, marginTop: 2 }}>{label}</div>
          </div>
        ))}
      </div>

      {/* 3. Start Here tabbed card */}
      <div style={{
        background: C.white, borderRadius: 12, boxShadow: SHADOW,
        padding: "16px 22px 18px", marginBottom: 32,
        borderLeft: `3px solid ${C.brand}`,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 14 }}>
          {([
            { key: "state", label: "Find a state", color: C.brand },
            { key: "service", label: "Search a service", color: C.accent },
            { key: "adequacy", label: "Check adequacy", color: C.teal },
          ] as const).map(tab => (
            <button key={tab.key} onClick={() => setStartTab(tab.key)} style={{
              background: startTab === tab.key ? `${tab.color}0D` : "none",
              border: "none", borderRadius: 6, padding: "5px 14px",
              fontSize: 11, fontWeight: startTab === tab.key ? 600 : 400, fontFamily: FONT.body,
              color: startTab === tab.key ? tab.color : C.inkLight,
              cursor: "pointer", transition: "all .15s",
            }}>
              {tab.label}
            </button>
          ))}
        </div>

        {startTab === "state" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>State:</span>
            <select value={st} onChange={e => setSt(e.target.value)} style={{
              flex: 1, maxWidth: isMobile ? "100%" : 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FONT.body, color: st ? C.ink : C.inkLight,
              background: C.white,
            }}>
              <option value="">Select a state...</option>
              {STATES_LIST.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
            </select>
            <a
              href={st ? `#/state/${st}` : undefined}
              onClick={e => { if (!st) e.preventDefault(); }}
              style={{
                padding: "8px 16px", borderRadius: 6, border: "none",
                background: st ? C.brand : C.border, color: C.white,
                fontSize: 11, fontWeight: 600, cursor: st ? "pointer" : "default",
                textDecoration: "none", display: "inline-block",
              }}
            >
              View state profile →
            </a>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Enrollment, rates, hospitals, quality, workforce</span>
          </div>
        )}

        {startTab === "service" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>Service:</span>
            <input value={serviceQ} onChange={e => setServiceQ(e.target.value)}
              placeholder="dental, office visits, 99213, therapy..."
              style={{
                flex: 1, maxWidth: isMobile ? "100%" : 320, padding: "8px 10px", borderRadius: 6, fontSize: 12,
                border: `1px solid ${C.border}`, fontFamily: FONT.body, color: C.ink,
              }}
            />
            <a
              href={serviceQ ? `#/explorer` : undefined}
              onClick={e => { if (!serviceQ) e.preventDefault(); }}
              style={{
                padding: "8px 16px", borderRadius: 6, border: "none",
                background: serviceQ ? C.accent : C.border, color: C.white,
                fontSize: 11, fontWeight: 600, cursor: serviceQ ? "pointer" : "default",
                textDecoration: "none", display: "inline-block",
              }}
            >
              Search →
            </a>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Name, category, or <Term>HCPCS</Term> code</span>
          </div>
        )}

        {startTab === "adequacy" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>State:</span>
            <select value={st} onChange={e => setSt(e.target.value)} style={{
              flex: 1, maxWidth: isMobile ? "100%" : 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FONT.body, color: st ? C.ink : C.inkLight,
              background: C.white,
            }}>
              <option value="">Select a state...</option>
              {STATES_LIST.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
            </select>
            <a
              href={st ? `#/wages` : undefined}
              onClick={e => { if (!st) e.preventDefault(); }}
              style={{
                padding: "8px 16px", borderRadius: 6, border: "none",
                background: st ? C.teal : C.border, color: C.white,
                fontSize: 11, fontWeight: 600, cursor: st ? "pointer" : "default",
                textDecoration: "none", display: "inline-block",
              }}
            >
              Check adequacy →
            </a>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Rate adequacy, wages, and Medicare comparison</span>
          </div>
        )}
      </div>

      {/* 4. Grouped tool sections */}
      {NAV_GROUPS.map(group => {
        const groupTools = TOOLS.filter(t => t.group === group.key);
        return (
          <div key={group.key} style={{ paddingBottom: 32 }}>
            <div style={{
              display: "flex", alignItems: "baseline", gap: 12,
              marginBottom: 12, paddingTop: 12, borderTop: `1px solid ${C.border}`,
            }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: GROUP_COLORS[group.key], textTransform: "uppercase", letterSpacing: 1, fontFamily: FONT.mono }}>{group.label}</span>
              <span style={{ fontSize: 12, color: C.inkLight }}>{GROUP_DESCS[group.key]}</span>
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: groupTools.length <= 2 ? "repeat(auto-fill,minmax(min(100%,460px),1fr))" : groupTools.length === 4 ? "repeat(auto-fill,minmax(min(100%,460px),1fr))" : "repeat(auto-fill,minmax(min(100%,320px),1fr))",
              gap: 12,
            }}>
              {groupTools.map(tool => {
                const isLive = tool.status === "live" || tool.status === "beta";
                return (
                  <div
                    key={tool.id}
                    onClick={() => { window.location.hash = `/${tool.id}`; }}
                    style={{
                      background: C.white, borderRadius: 12, boxShadow: SHADOW,
                      padding: "20px 22px 18px", borderLeft: `3px solid ${tool.color}`,
                      opacity: isLive ? 1 : 0.75,
                      cursor: "pointer",
                      transition: "box-shadow 0.2s, transform 0.15s",
                    }}
                    onMouseEnter={e => { if (isLive) { e.currentTarget.style.boxShadow = SHADOW_LG; e.currentTarget.style.transform = "translateY(-1px)"; } }}
                    onMouseLeave={e => { e.currentTarget.style.boxShadow = SHADOW; e.currentTarget.style.transform = "translateY(0)"; }}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                      <span style={{
                        fontSize: 16, width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center",
                        borderRadius: 8, background: `${tool.color}0D`, color: tool.color, flexShrink: 0,
                      }}>
                        {tool.icon}
                      </span>
                      <div style={{ minWidth: 0 }}>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.ink, letterSpacing: -0.2 }}>{tool.name}</div>
                        <div style={{ fontSize: 10, color: C.inkLight, marginTop: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{tool.tagline}</div>
                      </div>
                    </div>
                    <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.6, marginTop: 6 }}>{tool.desc}</div>
                    <div style={{ marginTop: 10 }}>
                      {isLive ? (
                        <span style={{ fontSize: 9, fontWeight: 600, color: tool.color, fontFamily: FONT.mono, textTransform: "uppercase", letterSpacing: 0.5 }}>→ Open tool</span>
                      ) : (
                        <span style={{ fontSize: 9, padding: "3px 10px", borderRadius: 10, fontWeight: 600, background: C.surface, color: C.inkLight, fontFamily: FONT.mono }}>COMING SOON</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}

      {/* 5. AHEAD dark card callout */}
      <div style={{
        background: C.ink, borderRadius: 14, padding: isMobile ? "20px 18px" : "28px 32px",
        marginBottom: 32, position: "relative", overflow: "hidden",
      }}>
        <div style={{ position: "absolute", top: -40, right: -40, width: 140, height: 140, borderRadius: "50%", background: "rgba(46,107,74,0.12)", pointerEvents: "none" }} />
        <div style={{ position: "relative" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 10 }}>
            <span style={{ fontSize: 18, width: 36, height: 36, display: "flex", alignItems: "center", justifyContent: "center", borderRadius: 8, background: "rgba(46,107,74,0.2)", color: "#7FD4A0" }}>△</span>
            <div>
              <div style={{ fontSize: 15, fontWeight: 600, color: C.white }}><Term term="AHEAD">AHEAD</Term> Hospital Global Budgets</div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,0.5)" }}>Live — readiness scoring and budget modeling</div>
            </div>
          </div>
          <div style={{ fontSize: 12, color: "rgba(255,255,255,0.6)", lineHeight: 1.7, maxWidth: 600 }}>
            CMS's AHEAD model replaces <Term term="FFS">fee-for-service</Term> with fixed hospital budgets.
            We model both the Medicare and Medicaid sides — budget projections, readiness scoring
            from public HCRIS data, and participation decision support.
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(160px,1fr))", gap: 10, marginTop: 18 }}>
            {([
              ["Readiness Score", "4 dimensions scored from HCRIS data"],
              ["Budget Calculator", "Revenue projection → global budget modeling"],
              ["Peer Comparison", "CCN lookup → benchmarked against peers"],
            ] as const).map(([title, desc]) => (
              <a key={title} href={title === "Readiness Score" ? "#/ahead-readiness" : "#/ahead"} style={{
                padding: "12px 14px", borderRadius: 8, background: "rgba(255,255,255,0.06)",
                border: "1px solid rgba(255,255,255,0.08)", textDecoration: "none",
                display: "block", transition: "background .15s",
              }}
                onMouseEnter={e => { e.currentTarget.style.background = "rgba(255,255,255,0.10)"; }}
                onMouseLeave={e => { e.currentTarget.style.background = "rgba(255,255,255,0.06)"; }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: C.white }}>{title}</div>
                <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", marginTop: 3 }}>{desc}</div>
              </a>
            ))}
          </div>
        </div>
      </div>

      {/* 6. Explore → Analyze → Build workflow */}
      <div style={{ padding: "36px 0 40px", borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 6 }}>
          Explore → Analyze → Build
        </div>
        <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7, maxWidth: 560, marginBottom: 20 }}>
          Start by understanding what your state spends and on what. Measure
          whether those rates are adequate. Then calculate what rates should be
          and build the case for change.
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: 14 }}>
          {([
            { num: "1", label: "Explore", q: "What are we spending?",
              desc: "Ask questions in plain English or browse 101M+ rows across 54 jurisdictions. State profiles, cross-state comparisons, fee schedules, and CPRA reports.",
              tools: "Data Explorer · State Profile · Spending Explorer · CPRA Generator", color: C.brand },
            { num: "2", label: "Analyze", q: "Are rates adequate?",
              desc: "Three lenses: compare against BLS market wages, map quality outcomes to payment levels, and track how far rates have eroded relative to Medicare.",
              tools: "Wage Adequacy · Quality Linkage · Rate Reduction · HCBS Tracker", color: C.accent },
            { num: "3", label: "Build", q: "What should we pay?",
              desc: "Calculate defensible rates, forecast caseload and expenditure, model hospital global budgets, and get AI-powered policy analysis.",
              tools: "Rate Builder · Caseload Forecaster · AHEAD Readiness · Policy Analyst", color: C.teal },
          ] as const).map(item => (
            <div key={item.num} style={{
              background: C.white, borderRadius: 12, boxShadow: SHADOW,
              padding: "20px 22px 18px", borderTop: `3px solid ${item.color}`,
              transition: "box-shadow 0.2s, transform 0.15s",
            }}
              onMouseEnter={e => { e.currentTarget.style.boxShadow = SHADOW_LG; e.currentTarget.style.transform = "translateY(-1px)"; }}
              onMouseLeave={e => { e.currentTarget.style.boxShadow = SHADOW; e.currentTarget.style.transform = "translateY(0)"; }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <span style={{
                  width: 26, height: 26, borderRadius: "50%",
                  background: `${item.color}12`, color: item.color,
                  fontSize: 12, fontWeight: 700, display: "flex", alignItems: "center",
                  justifyContent: "center", fontFamily: FONT.mono, flexShrink: 0,
                }}>
                  {item.num}
                </span>
                <div>
                  <div style={{ fontSize: 9, fontWeight: 700, color: item.color, fontFamily: FONT.mono, textTransform: "uppercase", letterSpacing: 1 }}>{item.label}</div>
                  <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginTop: 1 }}>{item.q}</div>
                </div>
              </div>
              <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.65 }}>{item.desc}</div>
              <div style={{ fontSize: 9, color: C.inkLight, marginTop: 10, fontFamily: FONT.mono, letterSpacing: 0.3 }}>{item.tools}</div>
            </div>
          ))}
        </div>
      </div>

      {/* 7. Why now: CMS Ensuring Access */}
      <div style={{
        padding: "16px 20px", background: C.surface, borderRadius: 10,
        borderLeft: `3px solid ${C.accent}`, marginBottom: 32,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
          <span style={{ fontSize: 14 }}>§</span>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.ink }}>CMS Ensuring Access Final Rule</div>
          <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>42 CFR 447.203 · July 2026</span>
        </div>
        <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7, maxWidth: 600 }}>
          The Ensuring Access rule creates the first national transparency and
          adequacy requirements for Medicaid rate-setting. Every tool on this
          platform supports compliance, but the analytical need is broader than
          any single regulation.{" "}
          <a href="#/compliance" style={{ color: C.brand, textDecoration: "none", fontWeight: 600 }}>
            View compliance tools →
          </a>
        </div>
      </div>

      {/* 8. Why / How columns */}
      <div style={{ padding: "0 0 40px", display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))", gap: 24 }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Why this exists</div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            Medicaid rate-setting is one of the most consequential policy processes
            in American healthcare: a $1T+ program, 50 states operating in
            isolation, and almost nobody has the data to do it well. Cross-state
            comparisons barely exist. Adequacy analysis is ad hoc. Fiscal modeling
            is locked inside consulting engagements. Aradune builds the shared
            infrastructure.
          </div>
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>How it works</div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            250 fact tables from 80+ federal sources: <Term>T-MSIS</Term> spending, Medicare PFS &amp; Part D,
            <Term>BLS</Term> wage surveys, CMS <Term>Core Set</Term> quality measures, HCRIS cost reports,
            PBJ staffing, Five Star ratings, NADAC pharmacy pricing, Care Compare,
            BRFSS, SAMHSA behavioral health, MSSP/ACO data, CDC mortality,
            MACPAC enrollment &amp; spending, HRSA workforce, and 47 state fee schedules.
            Served via a FastAPI backend on Fly.io with DuckDB over Parquet.
            <span style={{ display: "block", marginTop: 8, fontSize: 11, color: C.inkLight }}>
              Technical: Hive-partitioned Parquet lake synced to Cloudflare R2.
              In-memory DuckDB registers all tables as views on startup.
            </span>
          </div>
        </div>
      </div>

      {/* 9. Data sources bar */}
      <div style={{
        display: "flex", gap: 16, flexWrap: "wrap", padding: "16px 0 24px",
        borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}`, marginBottom: 32,
        alignItems: "center",
      }}>
        <span style={{ fontSize: 10, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.5, fontWeight: 600 }}>DATA:</span>
        {["CMS T-MSIS", "Medicare PFS", "BLS OES", "CMS Core Set", "HCRIS", "PBJ Staffing", "Five Star", "NADAC", "SDUD", "Care Compare", "BRFSS", "SAMHSA", "State Fee Schedules"].map(src => (
          <span key={src} style={{ fontSize: 10, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.3 }}>{src}</span>
        ))}
        <span style={{ marginLeft: "auto", fontSize: 9, fontFamily: FONT.mono, color: C.brand, letterSpacing: 0.3, fontWeight: 600 }}>Queryable via DuckDB-WASM</span>
      </div>

      {/* Footer links */}
      <div style={{ display: "flex", gap: 16, marginBottom: 40, fontSize: 12 }}>
        <a href="#/about" style={{ color: C.inkLight, textDecoration: "none", fontWeight: 500 }}>
          About the project
        </a>
      </div>
    </div>
  );
}

// ── About Page ───────────────────────────────────────────────────────────
function About() {
  return (
    <div style={{ maxWidth: 640, margin: "0 auto", padding: "40px 20px 60px" }}>
      <h1 style={{ fontSize: 24, fontWeight: 700, color: C.ink, margin: "0 0 24px" }}>
        About Aradune
      </h1>
      <div style={{ fontSize: 13, color: C.ink, lineHeight: 1.8, display: "grid", gap: 20 }}>
        <p style={{ margin: 0 }}>
          Aradune was the character name of Brad McQuaid, the co-creator of
          EverQuest. He played a paladin. A defender. That's the idea here.
        </p>
        <p style={{ margin: 0 }}>
          Medicaid spending data is public but practically inaccessible. The
          dataset on opendata.hhs.gov is over 3 GB. Too large for
          spreadsheets, too messy for most analytical tools, and buried behind
          enough friction that very few people ever look at it.
        </p>
        <p style={{ margin: 0 }}>
          Aradune exists to change that. Free, open tools for anyone who needs
          to understand how Medicaid dollars move: state policy analysts,
          academic researchers, journalists covering healthcare, advocates
          pushing for better rates, legislative staff scoring bills, and federal
          officials tracking program integrity. No paywall, no login.
        </p>

        <div style={{
          padding: "16px 20px", background: C.surface, borderRadius: 10,
          borderLeft: `3px solid ${C.brand}`,
        }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 6 }}>
            Built with AI as a force multiplier
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7 }}>
            This entire platform, from the data pipeline to the analysis
            tools to the interface, was built by one person using AI as a
            collaborator. Aradune is proof of what's possible when AI
            is used for public good instead of extraction.
          </div>
        </div>

        <div style={{
          padding: "16px 20px", background: C.surface, borderRadius: 10,
          borderLeft: `3px solid ${C.accent}`,
        }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, marginBottom: 6 }}>
            CMS Ensuring Access Final Rule (42 CFR 447.203)
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7 }}>
            The Ensuring Access final rule creates new transparency and adequacy
            requirements for every state Medicaid program, with deadlines starting
            July 2026. Aradune's tools map directly to these requirements:
            fee schedule publication (b)(1)-(3), rate reduction analysis (b)(4),
            and HCBS compensation reporting (b)(5). The platform is structured
            around this framework (Transparency, Adequacy, and Modeling) so
            states can track compliance and build the analytical case for their
            rate-setting decisions.
          </div>
        </div>

        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            Data sources
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.8 }}>
            <b>Claims data:</b> HHS Medicaid Provider Spending dataset from{" "}
            <a href="https://opendata.hhs.gov" style={{ color: C.brand }} target="_blank" rel="noopener">
              opendata.hhs.gov
            </a>
            <br />
            <b>Provider geography:</b> NPPES National Provider Identifier file from CMS
            <br />
            <b>Code descriptions:</b> CMS Physician Fee Schedule RVU files + HCPCS Level II Alpha-Numeric file
            <br />
            <b>Enrollment:</b> CMS Medicaid enrollment data (November 2024, Medicaid only; CHIP excluded; enriched with state exhibit data)
            <br />
            <b>FMAP:</b> FY2025 Federal Medical Assistance Percentages
          </div>
        </div>

        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            Methodology
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.8 }}>
            <b>Rates:</b> total paid ÷ total claims, per code per state. These are paid claims averages from T-MSIS data, not fee schedule amounts. Fee schedule rates (what states officially set) may differ from what is actually paid due to modifiers, managed care, and other adjustments. No risk adjustment, no modifier weighting.
            <br />
            <b>Fiscal impact:</b> (national avg − state rate) × state claims.
            <br />
            <b>Case mix:</b> Laspeyres decomposition into price index and mix index.
            <br />
            <b>Concentration:</b> Gini coefficient and top-percentile spending shares.
            <br />
            <b>Per enrollee:</b> total state spend ÷ CMS Medicaid enrollment.
          </div>
        </div>

        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            Contact
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.8 }}>
            The free tools are designed to cover most use cases. For custom
            rate studies, AHEAD global budget modeling, SPA fiscal impact
            analysis, or other methodology work beyond what the public tools
            provide, please get in touch.
          </div>
          <a
            href="mailto:aradune-medicaid@proton.me"
            style={{
              display: "inline-block", marginTop: 8,
              color: C.brand, fontSize: 12, fontWeight: 600, textDecoration: "none",
            }}
          >
            aradune-medicaid@proton.me
          </a>
        </div>

        <div style={{
          fontSize: 11, color: C.inkLight, paddingTop: 16,
          borderTop: `1px solid ${C.border}`,
        }}>
          Aradune is an independent project and is not affiliated with CMS, HHS, or any state Medicaid agency.
        </div>
      </div>
    </div>
  );
}

// ── Coming Soon Page ─────────────────────────────────────────────────────
function ComingSoon({ tool }: { tool: { name: string; icon: string; desc: string; color: string } }) {
  return (
    <div style={{ maxWidth: 520, margin: "0 auto", padding: "72px 20px", textAlign: "center" }}>
      <div style={{
        fontSize: 36, width: 64, height: 64,
        display: "flex", alignItems: "center", justifyContent: "center",
        borderRadius: 16, background: `${tool.color}0D`, color: tool.color,
        margin: "0 auto 16px",
      }}>
        {tool.icon}
      </div>
      <h1 style={{ fontSize: 20, fontWeight: 700, color: C.ink, margin: "0 0 8px" }}>
        {tool.name}
      </h1>
      <p style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.7, maxWidth: 400, margin: "0 auto 24px", textAlign: "left" }}>
        {tool.desc}
      </p>
      <div style={{
        display: "inline-block", padding: "8px 20px",
        background: C.surface, borderRadius: 8, fontSize: 12, color: C.inkLight, fontWeight: 500,
      }}>
        Coming soon
      </div>
      <div style={{ marginTop: 16 }}>
        <a href="#/explorer" style={{ fontSize: 12, color: C.brand, textDecoration: "none" }}>
          ← Explore Medicaid spending data in the meantime
        </a>
      </div>
    </div>
  );
}

// ── Pricing Page (removed — pricing kept flexible for partnerships) ──

// ── Platform Shell ───────────────────────────────────────────────────────
// ── Password Gate ───────────────────────────────────────────────────────
function PasswordGate({ onAuth }: { onAuth: () => void }) {
  const [pw, setPw] = useState("");
  const [error, setError] = useState(false);

  const submit = () => {
    if (pw === "mediquiad") {
      sessionStorage.setItem("aradune_auth", "1");
      try { localStorage.setItem("aradune_token", pw); } catch {}
      onAuth();
    } else {
      setError(true);
      setPw("");
    }
  };

  return (
    <div style={{
      minHeight: "100vh", display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center",
      fontFamily: FONT.body, background: C.bg, color: C.ink,
      padding: "0 20px",
    }}>
      <div style={{ width: "100%", maxWidth: 400 }}>
        {/* Logo as text */}
        <div style={{
          fontSize: 32, fontWeight: 700, letterSpacing: 3,
          color: C.brand, textTransform: "uppercase", marginBottom: 40,
        }}>
          ARADUNE
        </div>

        {/* Description */}
        <div style={{ marginBottom: 32 }}>
          <div style={{ fontSize: 20, fontWeight: 700, color: C.ink, letterSpacing: -0.5, marginBottom: 10 }}>
            Coming Soon
          </div>
          <p style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.7, margin: 0 }}>
            The Medicaid data intelligence platform. Rate transparency, adequacy
            measurement, and fiscal modeling across all 54 jurisdictions.
          </p>
        </div>

        {/* Access code input */}
        <div style={{ marginBottom: 8 }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: C.inkLight, marginBottom: 6, textTransform: "uppercase", letterSpacing: 0.5 }}>
            Access code
          </div>
          <form onSubmit={e => { e.preventDefault(); submit(); }} style={{ display: "flex", gap: 8 }}>
            <input
              type="password"
              value={pw}
              onChange={e => { setPw(e.target.value); setError(false); }}
              placeholder="Enter code"
              autoFocus
              style={{
                flex: 1, padding: "10px 14px", fontSize: 13, borderRadius: 6,
                border: `1px solid ${error ? C.neg : C.border}`,
                outline: "none", fontFamily: FONT.body, background: C.white,
              }}
            />
            <button type="submit" style={{
              padding: "10px 22px", fontSize: 13, fontWeight: 600,
              background: C.brand, color: C.white, border: "none",
              borderRadius: 6, cursor: "pointer", flexShrink: 0,
            }}>Enter</button>
          </form>
          {error && <div style={{ fontSize: 11, color: C.neg, marginTop: 6 }}>Incorrect code</div>}
        </div>

        {/* Contact */}
        <div style={{ marginTop: 48, fontSize: 11, color: C.inkLight }}>
          Questions? <a href="mailto:aradune-medicaid@proton.me" style={{ color: C.brand, textDecoration: "none" }}>aradune-medicaid@proton.me</a>
        </div>
      </div>
    </div>
  );
}

export default function Platform() {
  const [authed, setAuthed] = useState(() => sessionStorage.getItem("aradune_auth") === "1");
  const route = useRoute();

  if (!authed) return <PasswordGate onAuth={() => setAuthed(true)} />;

  const loadingFallback = <SwordLoader />;

  const renderRoute = () => {
    if (route === "/" || route === "") return <Landing />;
    if (route === "/about") return <About />;

    const tool = TOOLS.find(t => route === `/${t.id}`);
    if (tool && (tool.status === "coming")) return <ComingSoon tool={tool} />;

    // Lazy-loaded tool routes (code-split)
    const toolMap: Record<string, JSX.Element> = {
      "/state": <StateProfile />,
      "/explorer": <TmsisExplorer />,
      "/wages": <WageAdequacy />,
      "/quality": <QualityLinkage />,
      "/decay": <RateDecay />,
      "/builder": <RateBuilder />,
      "/analyst": <PolicyAnalyst />,
      "/ahead": <AheadCalculator />,
      "/ahead-readiness": <AheadReadiness />,
      "/fees": <FeeScheduleDir />,
      "/lookup": <RateLookup />,
      "/compliance": <ComplianceReport />,
      "/reduction": <RateReduction />,
      "/hcbs8020": <HcbsTracker />,
      "/cpra": <CpraGenerator />,
      "/forecast": <CaseloadForecaster />,
      "/ask": <DataExplorer />,
      "/catalog": <DataCatalog />,
      "/methods": <FeeScheduleDir />,
    };
    const toolRoute = toolMap[route] ?? (route.startsWith("/ahead?") ? toolMap["/ahead"] : route.startsWith("/state/") ? toolMap["/state"] : null);
    if (toolRoute) return <ToolErrorBoundary><Suspense fallback={loadingFallback}>{toolRoute}</Suspense></ToolErrorBoundary>;

    return (
      <div style={{ maxWidth: 400, margin: "0 auto", padding: "80px 20px", textAlign: "center" }}>
        <div style={{ fontSize: 14, color: C.inkLight, marginBottom: 12 }}>Page not found.</div>
        <a href="#/" style={{ fontSize: 13, color: C.brand, textDecoration: "none" }}>← Back to Aradune</a>
      </div>
    );
  };

  return (
    <div style={{ fontFamily: FONT.body, background: C.bg, minHeight: "100vh", color: C.ink, overflowX: "hidden" }}>
      <a href="#main-content" style={{
        position: "absolute", left: -9999, top: "auto", width: 1, height: 1, overflow: "hidden",
        zIndex: 1000, padding: "8px 16px", background: C.brand, color: C.white,
        fontSize: 13, fontWeight: 600, textDecoration: "none", borderRadius: 4,
      }} onFocus={e => { e.currentTarget.style.position = "fixed"; e.currentTarget.style.left = "8px"; e.currentTarget.style.top = "8px"; e.currentTarget.style.width = "auto"; e.currentTarget.style.height = "auto"; }}
         onBlur={e => { e.currentTarget.style.position = "absolute"; e.currentTarget.style.left = "-9999px"; e.currentTarget.style.width = "1px"; e.currentTarget.style.height = "1px"; }}>
        Skip to main content
      </a>
      <PlatformNav route={route} />
      <main id="main-content">{renderRoute()}</main>
      <footer style={{
        maxWidth: 1080, margin: "0 auto", padding: "24px 20px 32px",
        borderTop: `1px solid ${C.border}`,
        display: "flex", justifyContent: "space-between", alignItems: "center",
        flexWrap: "wrap", gap: 8,
      }}>
        <span style={{ fontSize: 10, color: C.inkLight }}>Aradune · aradune.co</span>
        <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>HHS Medicaid Provider Spending · opendata.hhs.gov</span>
      </footer>
    </div>
  );
}
