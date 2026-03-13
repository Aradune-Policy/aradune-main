import { useState, useEffect, lazy, Suspense, Component } from "react";
import type { ReactNode, ErrorInfo, ReactElement } from "react";
import { C, FONT, SHADOW, SHADOW_LG } from "./design";
import type { ToolDef, NavGroup } from "./types";
// STATES_LIST and STATE_NAMES available via lazy-loaded tools

import NavDrop from "./components/NavDrop";
import PlatformSearch from "./components/PlatformSearch";
import { AraduneProvider, useAradune } from "./context/AraduneContext";
import IntelligencePanel from "./components/IntelligencePanel";
import ReportBuilder from "./components/ReportBuilder";
import Lottie from "lottie-react";
import { ClerkAuthProvider, RequireAuth, UserNav, isClerkEnabled } from "./components/ClerkProvider";

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
const DataCatalog = lazy(() => import("./tools/DataCatalog"));
const IntelligenceChat = lazy(() => import("./tools/IntelligenceChat"));
const RateAnalysis = lazy(() => import("./tools/RateAnalysis"));
const ProviderIntelligence = lazy(() => import("./tools/ProviderIntelligence"));
const WorkforceQuality = lazy(() => import("./tools/WorkforceQuality"));

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

// ── Tool Registry (6 modules) ────────────────────────────────────────────
const TOOLS: ToolDef[] = [
  // ── STATES ────────────────────────────────────────────────────────────
  {
    id: "state", group: "states", name: "State Profile",
    tagline: "Everything Aradune knows about a state, in one view",
    desc: "Enrollment, rates, hospitals, quality, workforce, pharmacy, and economic context for any state, unified from 250 fact tables with cross-dataset insights.",
    status: "live", icon: "◉", color: C.brand,
  },
  // ── RATES ─────────────────────────────────────────────────────────────
  {
    id: "rates", group: "rates", name: "Rate Comparison",
    tagline: "Medicaid-to-Medicare rate parity across all states",
    desc: "16,000+ HCPCS codes across 47 states compared against the Medicare PFS. Cross-state rankings, rate erosion tracking, and impact analysis for proposed changes.",
    status: "live", icon: "◧", color: C.brand, navLabel: "Rates",
  },
  {
    id: "cpra", group: "rates", name: "CPRA Generator",
    tagline: "42 CFR 447.203 Comparative Payment Rate Analysis",
    desc: "Generate the Comparative Payment Rate Analysis required by July 2026. Pre-computed rates for 45 states or upload your own fee schedule.",
    status: "live", icon: "◆", color: C.brand,
  },
  {
    id: "fees", group: "rates", name: "Fee Schedule Directory",
    tagline: "State fee schedule sources and methodology documentation",
    desc: "Every state's Medicaid fee schedule source, effective dates, and methodology notes. Links to official state portals.",
    status: "live", icon: "◇", color: C.brand,
  },
  {
    id: "lookup", group: "rates", name: "Rate Lookup",
    tagline: "Search any HCPCS code across all states",
    desc: "Look up Medicaid reimbursement rates for any procedure code across all 47 states with fee schedule data.",
    status: "live", icon: "⌗", color: C.brand,
  },
  {
    id: "builder", group: "rates", name: "Rate Builder",
    tagline: "Model custom rate methodologies",
    desc: "Build Medicaid rates using different methodologies: percent of Medicare, RBRVS, state-specific conversion factors, and peer state benchmarks.",
    status: "live", icon: "◐", color: C.brand,
  },
  // ── FORECAST ──────────────────────────────────────────────────────────
  {
    id: "forecast", group: "forecast", name: "Caseload Forecaster",
    tagline: "Caseload and expenditure projections with scenario modeling",
    desc: "Upload monthly enrollment data. Aradune runs SARIMAX + ETS model competition, produces caseload forecasts with confidence intervals, and projects expenditure by category.",
    status: "live", icon: "◐", color: C.teal,
  },
  // ── PROVIDERS ─────────────────────────────────────────────────────────
  {
    id: "hospitals", group: "providers", name: "Hospital Intelligence",
    tagline: "AHEAD readiness, global budgets, and hospital financial data",
    desc: "Search any hospital by name. AHEAD readiness scoring from public HCRIS data, global budget modeling, peer comparison.",
    status: "live", icon: "△", color: C.accent,
  },
  {
    id: "ahead", group: "providers", name: "AHEAD Calculator",
    tagline: "AHEAD model savings and global budget projections",
    desc: "Model hospital participation in the CMS AHEAD model. Project savings, global budgets, and readiness.",
    status: "live", icon: "△", color: C.accent,
  },
  {
    id: "explorer", group: "providers", name: "Spending Explorer",
    tagline: "T-MSIS provider spending patterns across states",
    desc: "Explore provider-level Medicaid spending patterns from 227M T-MSIS claims. Filter by state, category, procedure code, and provider.",
    status: "live", icon: "◧", color: C.accent,
  },
  // ── WORKFORCE ─────────────────────────────────────────────────────────
  {
    id: "wages", group: "workforce", name: "Wage Adequacy",
    tagline: "BLS market wages vs Medicaid reimbursement",
    desc: "Compare Bureau of Labor Statistics market wages against Medicaid reimbursement rates for key healthcare occupations.",
    status: "live", icon: "⊿", color: C.teal,
  },
  {
    id: "quality", group: "workforce", name: "Quality Linkage",
    tagline: "CMS Core Set quality measures mapped to payment rates",
    desc: "Link CMS quality measures to Medicaid payment rates. Identify where low rates correlate with poor outcomes.",
    status: "live", icon: "⊿", color: C.teal,
  },
  {
    id: "hcbs8020", group: "workforce", name: "HCBS Tracker",
    tagline: "80/20 HCBS pass-through compliance tracking",
    desc: "Track HCBS payment pass-through rates against the 80% direct care worker compensation requirement.",
    status: "live", icon: "⊿", color: C.teal,
  },
  {
    id: "compliance", group: "workforce", name: "Compliance Center",
    tagline: "42 CFR 447.203 requirements and transparency",
    desc: "Everything for the July 2026 Ensuring Access deadline: compliance checklists, rate reduction modeling, methodology documentation.",
    status: "live", icon: "◇", color: C.teal,
  },
  {
    id: "reduction", group: "workforce", name: "Rate Reduction Analyzer",
    tagline: "Model the impact of proposed Medicaid rate reductions",
    desc: "Analyze how proposed rate reductions affect provider participation and beneficiary access.",
    status: "live", icon: "◇", color: C.teal,
  },
];

const NAV_GROUPS: NavGroup[] = [
  { key: "states", label: "States", tools: TOOLS.filter(t => t.group === "states") },
  { key: "rates", label: "Rates", tools: TOOLS.filter(t => t.group === "rates") },
  { key: "forecast", label: "Forecast", tools: TOOLS.filter(t => t.group === "forecast") },
  { key: "providers", label: "Providers", tools: TOOLS.filter(t => t.group === "providers") },
  { key: "workforce", label: "Workforce", tools: TOOLS.filter(t => t.group === "workforce") },
];

const GROUP_COLORS: Record<string, string> = {
  states: C.brand, rates: C.brand, forecast: C.teal,
  providers: C.accent, workforce: C.teal,
};
const GROUP_DESCS: Record<string, string> = {
  states: "State profiles with enrollment, rates, hospitals, quality, and economic context.",
  rates: "Fee schedule comparison, CPRA compliance, rate lookup, and rate modeling.",
  forecast: "Caseload and expenditure projections with scenario modeling.",
  providers: "Hospital intelligence, AHEAD readiness, and provider spending analysis.",
  workforce: "Wage adequacy, quality linkage, HCBS tracking, and compliance.",
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
  const { demoMode } = useAradune();

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
            <span style={{ fontSize: 16, fontWeight: 800, color: C.brand, letterSpacing: 2, fontFamily: FONT.body }}>ARADUNE</span>
          </a>
          {demoMode && (
            <span style={{
              fontSize: 9, fontWeight: 700, fontFamily: FONT.mono,
              color: C.accent, background: `${C.accent}14`, border: `1px solid ${C.accent}30`,
              padding: "1px 5px", borderRadius: 3, letterSpacing: 1, lineHeight: 1,
            }}>DEMO</span>
          )}
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
            <a href="#/intelligence" style={{
              fontSize: 11, fontFamily: FONT.body, fontWeight: 600,
              color: route === "/intelligence" ? C.brand : C.inkLight,
              textDecoration: "none", padding: "4px 10px", whiteSpace: "nowrap",
            }}>
              Ask Aradune
            </a>
            {NAV_GROUPS.map(g => <NavDrop key={g.key} group={g} route={route} />)}
            <PlatformSearch tools={TOOLS} />
            <UserNav />
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
          <a href="#/intelligence" onClick={() => setMobileOpen(false)} style={{
            display: "block", padding: "10px 20px", textDecoration: "none",
            fontSize: 13, fontWeight: 600, color: route === "/intelligence" ? C.brand : C.ink, fontFamily: FONT.body,
          }}>
            Ask Aradune
          </a>
          {NAV_GROUPS.map(g => (
            <div key={g.key}>
              <div style={{ padding: "8px 20px 4px", fontSize: 9, fontWeight: 700, color: GROUP_COLORS[g.key] || C.brand, textTransform: "uppercase", letterSpacing: 1, fontFamily: FONT.mono }}>
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
        </div>
      )}
    </nav>
  );
}

// ── Landing Page ─────────────────────────────────────────────────────────
function Landing() {
  const isMobile = useIsMobile();
  const [chatInput, setChatInput] = useState("");
  const [chatFocused, setChatFocused] = useState(false);

  const STARTERS = [
    { label: "Rate Parity", prompt: "Which states pay below 50% of Medicare for primary care E/M codes?" },
    { label: "Drug Spending", prompt: "What are the top 10 drugs by Medicaid spending in 2023?" },
    { label: "Workforce", prompt: "Which states have the most severe primary care HPSA designations?" },
    { label: "HCBS Waitlists", prompt: "Show me states with the longest HCBS waitlists and their FMAP rates" },
    { label: "Enrollment", prompt: "Compare Florida's Medicaid enrollment trend to the national average" },
    { label: "CPRA Deadline", prompt: "What is the CPRA requirement under 42 CFR 447.203 and when is it due?" },
  ];

  const MODULE_GROUPS = [
    {
      title: "States & Enrollment",
      color: C.brand,
      modules: [
        { id: "state", name: "State Profiles", desc: "54-jurisdiction dashboards with enrollment, rates, hospitals, quality, workforce, and economic context", route: "#/state/FL" },
      ],
    },
    {
      title: "Rates & Compliance",
      color: C.brand,
      modules: [
        { id: "rates", name: "Rate Comparison", desc: "Medicaid-to-Medicare parity across 47 states, 16,000+ procedure codes", route: "#/rates" },
        { id: "cpra", name: "CPRA Generator", desc: "42 CFR 447.203 Comparative Payment Rate Analysis for July 2026 deadline", route: "#/cpra" },
        { id: "lookup", name: "Rate Lookup", desc: "Search any HCPCS code across all states instantly", route: "#/lookup" },
        { id: "fees", name: "Fee Schedule Directory", desc: "Every state's fee schedule source, methodology, and effective dates", route: "#/fees" },
        { id: "builder", name: "Rate Builder", desc: "Model custom rate methodologies: % of Medicare, RBRVS, conversion factors", route: "#/builder" },
      ],
    },
    {
      title: "Fiscal & Forecasting",
      color: C.teal,
      modules: [
        { id: "forecast", name: "Caseload Forecaster", desc: "SARIMAX + ETS model competition with scenario modeling and expenditure projections", route: "#/forecast" },
      ],
    },
    {
      title: "Providers & Hospitals",
      color: C.accent,
      modules: [
        { id: "hospitals", name: "Hospital Intelligence", desc: "HCRIS financials, AHEAD readiness, global budget modeling, peer benchmarks", route: "#/hospitals" },
        { id: "ahead", name: "AHEAD Calculator", desc: "CMS AHEAD model participation scoring and savings projections", route: "#/ahead" },
        { id: "explorer", name: "Spending Explorer", desc: "Provider-level Medicaid spending patterns from T-MSIS claims data", route: "#/explorer" },
      ],
    },
    {
      title: "Workforce & Access",
      color: C.teal,
      modules: [
        { id: "wages", name: "Wage Adequacy", desc: "BLS market wages vs Medicaid reimbursement for healthcare occupations", route: "#/wages" },
        { id: "quality", name: "Quality Linkage", desc: "CMS Core Set quality measures correlated with payment rates", route: "#/quality" },
        { id: "hcbs8020", name: "HCBS Tracker", desc: "80% direct care worker compensation pass-through tracking", route: "#/hcbs8020" },
        { id: "compliance", name: "Compliance Center", desc: "Ensuring Access requirements, rate reduction modeling, methodology docs", route: "#/compliance" },
      ],
    },
  ];

  const LANDSCAPE = [
    { name: "Aradune", tables: "667+", rows: "400M+", domains: "18", states: "54", crossRef: true, ai: true, compliance: true },
    { name: "data.medicaid.gov", tables: "~40", rows: "~30M", domains: "6", states: "54", crossRef: false, ai: false, compliance: false },
    { name: "CMS Data.gov", tables: "~50", rows: "~50M", domains: "8", states: "54", crossRef: false, ai: false, compliance: false },
    { name: "KFF / MACPAC", tables: "~30", rows: "~500K", domains: "4", states: "54", crossRef: false, ai: false, compliance: false },
    { name: "ResDAC / VRDC", tables: "100+", rows: "Billions", domains: "5", states: "54", crossRef: false, ai: false, compliance: false },
  ];

  const handleChatSubmit = () => {
    if (!chatInput.trim()) return;
    window.location.hash = `#/intelligence?q=${encodeURIComponent(chatInput.trim())}`;
  };

  return (
    <div style={{ overflowX: "hidden", fontFamily: FONT.body }}>

      {/* ── HERO ────────────────────────────────────────────────── */}
      <div style={{
        background: `linear-gradient(180deg, ${C.bg} 0%, #EEF2EE 100%)`,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <div style={{ maxWidth: 1080, margin: "0 auto", padding: isMobile ? "48px 20px 40px" : "72px 20px 56px" }}>
          <div style={{
            fontSize: 10, fontWeight: 700, color: C.brand, fontFamily: FONT.mono,
            letterSpacing: 2, textTransform: "uppercase", marginBottom: 16,
            display: "flex", alignItems: "center", gap: 8,
          }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.brand, display: "inline-block" }} />
            THE MEDICAID INTELLIGENCE PLATFORM
          </div>
          <h1 style={{
            fontSize: isMobile ? 28 : 42, fontWeight: 800, color: C.ink,
            lineHeight: 1.15, letterSpacing: -1.2, margin: 0, maxWidth: 700,
          }}>
            Every public Medicaid dataset.<br />
            <span style={{ color: C.brand }}>One AI-powered platform.</span>
          </h1>
          <p style={{
            fontSize: isMobile ? 14 : 16, color: C.inkLight, lineHeight: 1.7,
            marginTop: 18, maxWidth: 620,
          }}>
            667+ tables across 18 data domains: rates, enrollment, hospitals, quality,
            workforce, pharmacy, expenditure, and more. Ingested, normalized, and
            cross-referenced into a single queryable layer with an AI analyst that
            reasons across the entire dataset. Not a dashboard. Not a search tool.
            An intelligence system that reads the data, connects the dots, and
            writes the analysis. Built for anyone who needs to understand Medicaid.
          </p>

          {/* Stats row */}
          <div style={{
            display: "flex", gap: isMobile ? 16 : 32, marginTop: 32,
            flexWrap: "wrap",
          }}>
            {([
              ["667+", "fact tables"],
              ["400M+", "rows"],
              ["54", "jurisdictions"],
              ["18", "data domains"],
              ["100+", "ETL pipelines"],
            ] as const).map(([val, label]) => (
              <div key={label}>
                <div style={{ fontSize: isMobile ? 20 : 26, fontWeight: 800, fontFamily: FONT.mono, color: C.ink, letterSpacing: -0.5 }}>{val}</div>
                <div style={{ fontSize: 11, color: C.inkLight, marginTop: 2 }}>{label}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── ARADUNE CHAT BLOCK ─────────────────────────────────── */}
      <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px" }}>
        <div style={{
          marginTop: -28,
          background: C.ink, borderRadius: 16,
          padding: isMobile ? "24px 20px" : "32px 36px",
          position: "relative", overflow: "hidden",
          boxShadow: "0 8px 32px rgba(10,37,64,0.18), 0 2px 8px rgba(10,37,64,0.08)",
        }}>
          {/* Decorative circles */}
          <div style={{ position: "absolute", top: -80, right: -80, width: 260, height: 260, borderRadius: "50%", background: "rgba(46,107,74,0.06)", pointerEvents: "none" }} />
          <div style={{ position: "absolute", bottom: -50, left: -50, width: 160, height: 160, borderRadius: "50%", background: "rgba(196,89,10,0.04)", pointerEvents: "none" }} />

          <div style={{ position: "relative" }}>
            {/* Header */}
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
              <div style={{
                width: 36, height: 36, borderRadius: 10,
                background: "rgba(46,107,74,0.2)",
                display: "flex", alignItems: "center", justifyContent: "center",
              }}>
                <img src="/assets/icon-bot.png" alt="" style={{ width: 22, height: 22, borderRadius: 5 }}
                  onError={e => { (e.target as HTMLImageElement).style.display = "none"; }} />
              </div>
              <div>
                <div style={{ fontSize: 18, fontWeight: 700, color: C.white, letterSpacing: -0.3 }}>Ask Aradune anything</div>
                <div style={{ fontSize: 12, color: "rgba(255,255,255,0.5)", marginTop: 2 }}>
                  AI analyst with direct query access to 667+ tables, policy corpus, and web search
                </div>
              </div>
            </div>

            {/* Chat input */}
            <div style={{
              display: "flex", gap: 10, alignItems: "flex-end",
              background: chatFocused ? "rgba(255,255,255,0.12)" : "rgba(255,255,255,0.07)",
              border: `1px solid ${chatFocused ? "rgba(46,107,74,0.5)" : "rgba(255,255,255,0.1)"}`,
              borderRadius: 12, padding: "12px 14px",
              transition: "all .2s",
            }}>
              <textarea
                value={chatInput}
                onChange={e => setChatInput(e.target.value)}
                onFocus={() => setChatFocused(true)}
                onBlur={() => setChatFocused(false)}
                onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleChatSubmit(); } }}
                placeholder="What does Florida pay for primary care E/M codes vs Medicare?"
                rows={1}
                style={{
                  flex: 1, border: "none", outline: "none", resize: "none",
                  fontSize: 14, fontFamily: FONT.body, color: C.white,
                  background: "transparent", lineHeight: 1.5,
                  minHeight: 22, maxHeight: 80,
                  caretColor: C.brand,
                }}
              />
              <button
                onClick={handleChatSubmit}
                disabled={!chatInput.trim()}
                style={{
                  background: chatInput.trim() ? C.brand : "rgba(255,255,255,0.15)",
                  color: C.white, border: "none", borderRadius: 8,
                  padding: "8px 18px", fontSize: 13, fontWeight: 600,
                  cursor: chatInput.trim() ? "pointer" : "default",
                  fontFamily: FONT.body, whiteSpace: "nowrap",
                  transition: "background .15s",
                  flexShrink: 0,
                }}
              >
                Ask
              </button>
            </div>

            {/* Starter chips */}
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 14 }}>
              {STARTERS.map(s => (
                <button
                  key={s.label}
                  onClick={() => { window.location.hash = `#/intelligence?q=${encodeURIComponent(s.prompt)}`; }}
                  style={{
                    padding: "6px 14px", borderRadius: 20,
                    background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.1)",
                    fontSize: 11, color: "rgba(255,255,255,0.65)", cursor: "pointer",
                    fontFamily: FONT.body,
                    transition: "background .15s, color .15s, border-color .15s",
                  }}
                  onMouseEnter={e => { e.currentTarget.style.background = "rgba(255,255,255,0.12)"; e.currentTarget.style.color = "rgba(255,255,255,0.9)"; e.currentTarget.style.borderColor = "rgba(255,255,255,0.2)"; }}
                  onMouseLeave={e => { e.currentTarget.style.background = "rgba(255,255,255,0.06)"; e.currentTarget.style.color = "rgba(255,255,255,0.65)"; e.currentTarget.style.borderColor = "rgba(255,255,255,0.1)"; }}
                >
                  {s.label}
                </button>
              ))}
            </div>

            {/* Capability badges */}
            <div style={{ display: "flex", gap: 16, marginTop: 18, flexWrap: "wrap" }}>
              {[
                "DuckDB query engine",
                "Policy corpus (1,039 CMS docs)",
                "Web search",
                "File upload & cross-reference",
              ].map(cap => (
                <span key={cap} style={{
                  fontSize: 10, color: "rgba(255,255,255,0.35)", fontFamily: FONT.mono,
                  display: "flex", alignItems: "center", gap: 5,
                }}>
                  <span style={{ width: 4, height: 4, borderRadius: "50%", background: C.brand, display: "inline-block" }} />
                  {cap}
                </span>
              ))}
            </div>
          </div>
        </div>

        {/* ── MODULE GRID ─────────────────────────────────────────── */}
        <div style={{ marginTop: 48 }}>
          <div style={{ marginBottom: 28 }}>
            <h2 style={{ fontSize: isMobile ? 20 : 24, fontWeight: 800, color: C.ink, letterSpacing: -0.5, margin: "0 0 6px" }}>
              Structured tools
            </h2>
            <p style={{ fontSize: 13, color: C.inkLight, margin: 0, maxWidth: 520, lineHeight: 1.6 }}>
              Purpose-built workflows for recurring Medicaid analysis. Every tool connects back to Aradune for deeper investigation.
            </p>
          </div>

          {MODULE_GROUPS.map(group => (
            <div key={group.title} style={{ marginBottom: 28 }}>
              <div style={{
                fontSize: 10, fontWeight: 700, color: group.color, fontFamily: FONT.mono,
                letterSpacing: 1.5, textTransform: "uppercase", marginBottom: 10,
              }}>
                {group.title}
              </div>
              <div style={{
                display: "grid",
                gridTemplateColumns: `repeat(auto-fill, minmax(${isMobile ? "260px" : "240px"}, 1fr))`,
                gap: 10,
              }}>
                {group.modules.map(mod => (
                  <a
                    key={mod.id}
                    href={mod.route}
                    style={{
                      background: C.white, borderRadius: 10, padding: "16px 18px",
                      textDecoration: "none", borderLeft: `3px solid ${group.color}`,
                      boxShadow: SHADOW,
                      transition: "box-shadow .2s, transform .15s",
                      display: "block",
                    }}
                    onMouseEnter={e => { e.currentTarget.style.boxShadow = SHADOW_LG; e.currentTarget.style.transform = "translateY(-1px)"; }}
                    onMouseLeave={e => { e.currentTarget.style.boxShadow = SHADOW; e.currentTarget.style.transform = "translateY(0)"; }}
                  >
                    <div style={{ fontSize: 13, fontWeight: 700, color: C.ink, marginBottom: 4 }}>{mod.name}</div>
                    <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.55 }}>{mod.desc}</div>
                  </a>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* ── ENSURING ACCESS / COMPLIANCE CALLOUT ────────────────── */}
        <div style={{
          padding: "20px 24px", background: C.surface, borderRadius: 12,
          borderLeft: `3px solid ${C.accent}`, marginBottom: 40,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6, flexWrap: "wrap" }}>
            <span style={{ fontSize: 14, fontWeight: 700, color: C.ink }}>42 CFR 447.203: Ensuring Access</span>
            <span style={{
              fontSize: 10, color: C.white, background: C.accent, fontFamily: FONT.mono,
              fontWeight: 700, padding: "2px 8px", borderRadius: 4, letterSpacing: 0.5,
            }}>JULY 2026</span>
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7, maxWidth: 700 }}>
            The first national transparency and adequacy requirements for Medicaid rate-setting.
            Aradune's CPRA generator, fee schedule directory, rate reduction analyzer, and HCBS tracker
            map directly to subsections (b)(1) through (b)(5).{" "}
            <a href="#/compliance" style={{ color: C.brand, textDecoration: "none", fontWeight: 600 }}>
              View compliance tools
            </a>
          </div>
        </div>

        {/* ── ARCHITECTURE VISUAL ────────────────────────────────── */}
        <div style={{ marginBottom: 56 }}>
          <h2 style={{ fontSize: isMobile ? 18 : 22, fontWeight: 800, color: C.ink, letterSpacing: -0.5, margin: "0 0 6px" }}>
            How it works
          </h2>
          <p style={{ fontSize: 13, color: C.inkLight, margin: "0 0 24px", maxWidth: 580, lineHeight: 1.6 }}>
            Three layers turn scattered public data into actionable Medicaid intelligence.
          </p>

          {/* Architecture: frozen lake + entity network + intelligence */}
          {(() => {
            const ents = [
              { x: 165, y: 155, l: 'State' }, { x: 305, y: 140, l: 'Procedure' },
              { x: 440, y: 152, l: 'Provider' }, { x: 570, y: 138, l: 'Hospital' },
              { x: 235, y: 210, l: 'Drug' }, { x: 370, y: 202, l: 'Rate Cell' },
              { x: 500, y: 212, l: 'Quality' }, { x: 630, y: 198, l: 'MCO' },
              { x: 115, y: 242, l: 'Geography' }, { x: 285, y: 250, l: 'HCBS' },
              { x: 435, y: 246, l: 'Workforce' }, { x: 570, y: 255, l: 'Nursing' },
            ];
            const edges: [number, number][] = [
              [0,1],[0,2],[0,3],[0,8],[1,2],[1,4],[1,5],
              [2,3],[2,7],[2,11],[3,6],[3,7],[4,5],[5,0],
              [6,10],[7,11],[8,9],[9,10],[10,11],
            ];
            const surfLabels = [
              { x: 175, y: 374, l: 'Rates' }, { x: 310, y: 370, l: 'Enrollment' },
              { x: 440, y: 366, l: 'Claims' }, { x: 560, y: 362, l: 'Hospitals' },
              { x: 660, y: 358, l: 'Pharmacy' },
              { x: 195, y: 393, l: 'Medicare' }, { x: 330, y: 389, l: 'Quality' },
              { x: 460, y: 385, l: 'Expenditure' }, { x: 575, y: 381, l: 'BH' },
              { x: 680, y: 377, l: 'Workforce' },
              { x: 225, y: 408, l: 'Providers' }, { x: 370, y: 404, l: 'HCBS' },
              { x: 510, y: 400, l: 'Economic' }, { x: 640, y: 396, l: 'Nursing' },
            ];
            const lakeBackY = (x: number) => 352 - (x - 80) * 15 / 600;
            const particles = [
              { sx: 180, sy: 372, dur: 3.8 },
              { sx: 340, sy: 367, dur: 4.2 },
              { sx: 490, sy: 362, dur: 3.5 },
              { sx: 620, sy: 358, dur: 4.6 },
            ];

            return (
              <div style={{ marginBottom: 32 }}>
                <style>{`
                  @keyframes archFlow{0%{stroke-dashoffset:16;opacity:.08}50%{opacity:.15}100%{stroke-dashoffset:0;opacity:.08}}
                `}</style>
                <svg viewBox="0 0 1060 500" width="100%" style={{ display: 'block' }}>
                  <defs>
                    <linearGradient id="lkTop" x1="0%" y1="0%" x2="100%" y2="100%">
                      <stop offset="0%" stopColor="#ECF2F8" />
                      <stop offset="100%" stopColor="#E2EBF4" />
                    </linearGradient>
                    <linearGradient id="lkFr" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#D4DEE8" />
                      <stop offset="100%" stopColor="#C5D2DF" />
                    </linearGradient>
                    <filter id="glw">
                      <feGaussianBlur stdDeviation="4" result="blur" />
                      <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
                    </filter>
                  </defs>

                  {/* ── Connection lines: entities to lake surface (behind everything) ── */}
                  {ents.map((e, i) => (
                    <line key={`el${i}`}
                      x1={e.x} y1={e.y + 14} x2={e.x} y2={lakeBackY(e.x) + 8}
                      stroke="#2E6B4A" strokeWidth={0.7} opacity={0.1}
                      strokeDasharray="3 5"
                      style={{ animation: `archFlow 3.5s linear infinite`, animationDelay: `${i * 0.3}s` }}
                    />
                  ))}

                  {/* ── Connection lines: entities to intelligence badge ── */}
                  {ents.map((e, i) => (
                    <line key={`ei${i}`}
                      x1={e.x} y1={e.y - 14} x2={390} y2={68}
                      stroke="#2E6B4A" strokeWidth={0.4} opacity={0.06}
                    />
                  ))}

                  {/* ══ FROZEN LAKE SLAB ══ */}
                  {/* Right side face */}
                  <path d="M 680,337 L 720,400 L 720,452 L 680,389 Z"
                    fill="#BFD0DF" stroke="#B0C2D2" strokeWidth={0.5} />
                  {/* Front face with strata lines */}
                  <path d="M 120,417 L 720,400 L 720,452 L 120,469 Z"
                    fill="url(#lkFr)" stroke="#B0C2D2" strokeWidth={0.5} />
                  {[0.3, 0.55, 0.8].map((f, i) => (
                    <line key={`st${i}`}
                      x1={120} y1={417 + (469 - 417) * f}
                      x2={720} y2={400 + (452 - 400) * f}
                      stroke="#B8C8D8" strokeWidth={0.3} opacity={0.5} />
                  ))}
                  {/* Top surface */}
                  <path d="M 80,352 L 680,337 L 720,400 L 120,417 Z"
                    fill="url(#lkTop)" stroke="#C0D0DF" strokeWidth={0.5} />

                  {/* Grid lines on lake surface */}
                  {[0.2, 0.4, 0.6, 0.8].map((f, i) => (
                    <line key={`gh${i}`}
                      x1={80 + 40 * f} y1={352 + 65 * f}
                      x2={680 + 40 * f} y2={337 + 63 * f}
                      stroke="#CBD8E5" strokeWidth={0.3} />
                  ))}
                  {[0.17, 0.33, 0.5, 0.67, 0.83].map((f, i) => (
                    <line key={`gv${i}`}
                      x1={80 + 600 * f} y1={352 - 15 * f}
                      x2={120 + 600 * f} y2={417 - 17 * f}
                      stroke="#CBD8E5" strokeWidth={0.3} />
                  ))}

                  {/* Domain labels etched on surface */}
                  {surfLabels.map((d, i) => (
                    <text key={`sl${i}`} x={d.x} y={d.y} textAnchor="middle"
                      fill="#8A9DB0" fontSize={7}
                      fontFamily="'SF Mono',Menlo,monospace" fontWeight={500}>
                      {d.l}
                    </text>
                  ))}

                  {/* ══ ENTITY EDGES ══ */}
                  {edges.map(([a, b], i) => (
                    <line key={`ee${i}`}
                      x1={ents[a].x} y1={ents[a].y} x2={ents[b].x} y2={ents[b].y}
                      stroke="#2E6B4A" strokeWidth={0.8} opacity={0.18} />
                  ))}

                  {/* ══ ENTITY NODES ══ */}
                  {ents.map((e, i) => (
                    <g key={`en${i}`}>
                      <circle cx={e.x} cy={e.y} r={14}
                        fill="white" stroke="#2E6B4A" strokeWidth={1.2} />
                      <text x={e.x} y={e.y + 1} textAnchor="middle" dominantBaseline="middle"
                        fill="#2E6B4A" fontSize={7} fontWeight={600}
                        fontFamily="'SF Mono',Menlo,monospace">{e.l}</text>
                    </g>
                  ))}

                  {/* ══ INTELLIGENCE BADGE ══ */}
                  <rect x={280} y={18} width={220} height={48} rx={10}
                    fill="#0A2540" stroke="#2E6B4A" strokeWidth={1.2} filter="url(#glw)" />
                  <text x={390} y={36} textAnchor="middle" dominantBaseline="middle"
                    fill="white" fontSize={11} fontWeight={700}
                    fontFamily="'Helvetica Neue',sans-serif">Aradune Intelligence</text>
                  <text x={390} y={52} textAnchor="middle" dominantBaseline="middle"
                    fill="rgba(255,255,255,0.5)" fontSize={7}
                    fontFamily="'SF Mono',Menlo,monospace">{`Claude \u00B7 NL\u2192SQL \u00B7 RAG \u00B7 Thinking \u00B7 Web`}</text>

                  {/* ── Animated particles flowing lake → intelligence ── */}
                  {particles.map((pp, i) => (
                    <circle key={`ap${i}`} r={2.5} fill="#2E6B4A">
                      <animateMotion dur={`${pp.dur}s`} repeatCount="indefinite"
                        path={`M ${pp.sx} ${pp.sy} Q ${pp.sx} 200 390 68`} />
                      <animate attributeName="opacity" values="0;0.4;0.4;0"
                        dur={`${pp.dur}s`} repeatCount="indefinite" />
                    </circle>
                  ))}

                  {/* ══ RIGHT-SIDE LABELS ══ */}
                  <text x={760} y={32} fill="#0A2540" fontSize={15} fontWeight={800}
                    fontFamily="'Helvetica Neue',sans-serif">Intelligence</text>
                  <text x={760} y={50} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">Ask in natural language. Claude</text>
                  <text x={760} y={64} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">queries 18 domains, reasons with</text>
                  <text x={760} y={78} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">extended thinking, cites sources.</text>
                  <text x={760} y={96} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">Generates CPRAs, rate analyses,</text>
                  <text x={760} y={109} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">fiscal models, compliance docs.</text>

                  <text x={760} y={170} fill="#0A2540" fontSize={15} fontWeight={800}
                    fontFamily="'Helvetica Neue',sans-serif">Entity Registry</text>
                  <text x={760} y={188} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">16 entities, 19 deterministic</text>
                  <text x={760} y={202} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">metrics, auto-generated joins.</text>
                  <text x={760} y={220} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">state_code, procedure_code, NPI,</text>
                  <text x={760} y={233} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">FIPS link everything across domains.</text>

                  <text x={760} y={358} fill="#0A2540" fontSize={15} fontWeight={800}
                    fontFamily="'Helvetica Neue',sans-serif">Data Lake</text>
                  <text x={760} y={376} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">667+ tables, 400M+ rows</text>
                  <text x={760} y={390} fill="#425A70" fontSize={10}
                    fontFamily="'SF Mono',Menlo,monospace">3.8 GB Parquet, 54 jurisdictions</text>
                  <text x={760} y={408} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">CMS, BLS, CDC, HRSA, HCRIS,</text>
                  <text x={760} y={421} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">SDUD, NPPES, KFF, state fee</text>
                  <text x={760} y={434} fill="#7A8FA0" fontSize={9}
                    fontFamily="'SF Mono',Menlo,monospace">schedules, and more.</text>

                  {/* ══ DATA SOURCES (bottom pipeline) ══ */}
                  {[
                    { x: 150, l: 'data.cms.gov' }, { x: 280, l: 'data.medicaid.gov' },
                    { x: 420, l: 'BLS/Census' }, { x: 540, l: 'CDC/SAMHSA' },
                    { x: 660, l: 'KFF/MACPAC' },
                  ].map((s, i) => (
                    <g key={`src${i}`}>
                      <rect x={s.x - 48} y={472} width={96} height={18} rx={4}
                        fill="none" stroke="#B0C2D2" strokeWidth={0.7} strokeDasharray="3 2" />
                      <text x={s.x} y={483} textAnchor="middle" dominantBaseline="middle"
                        fill="#8A9DB0" fontSize={6.5}
                        fontFamily="'SF Mono',Menlo,monospace" fontWeight={500}>{s.l}</text>
                      <line x1={s.x} y1={472} x2={s.x} y2={lakeBackY(s.x) + 65}
                        stroke="#2E6B4A" strokeWidth={0.5} opacity={0.12}
                        strokeDasharray="2 3"
                        style={{ animation: `archFlow 4s linear infinite`, animationDelay: `${i * 0.5}s` }} />
                    </g>
                  ))}
                  <text x={60} y={483} fill="#7A8FA0" fontSize={7} fontWeight={600}
                    fontFamily="'SF Mono',Menlo,monospace">100+ ETL</text>
                </svg>
              </div>
            );
          })()}


          {/* ── DATABASE COMPARISON ──────────────────────────────────── */}
          <h3 style={{ fontSize: 15, fontWeight: 700, color: C.ink, margin: "0 0 4px", letterSpacing: -0.3 }}>
            Publicly available Medicaid data sources
          </h3>
          <p style={{ fontSize: 12, color: C.inkLight, margin: "0 0 16px", maxWidth: 540, lineHeight: 1.55 }}>
            No other platform normalizes and cross-references this breadth of Medicaid data into a single queryable layer.
          </p>
          <div style={{ overflowX: "auto" }}>
            <table style={{
              borderCollapse: "collapse", width: "100%", minWidth: 640,
              fontSize: 12, fontFamily: FONT.body,
            }}>
              <thead>
                <tr>
                  {["", "Tables", "Rows", "Domains", "States", "Cross-referenced", "AI query", "Compliance output"].map(h => (
                    <th key={h} style={{
                      padding: "10px 14px", borderBottom: `2px solid ${C.border}`,
                      textAlign: "left", fontWeight: 600, color: C.ink, fontSize: 11,
                      whiteSpace: "nowrap",
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {LANDSCAPE.map((row, i) => (
                  <tr key={row.name} style={{ background: i === 0 ? `${C.brand}08` : "transparent" }}>
                    <td style={{
                      padding: "10px 14px", borderBottom: `1px solid ${C.border}`,
                      fontWeight: i === 0 ? 700 : 400, color: i === 0 ? C.brand : C.ink,
                      whiteSpace: "nowrap",
                    }}>{row.name}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, fontFamily: FONT.mono, fontSize: 11 }}>{row.tables}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, fontFamily: FONT.mono, fontSize: 11 }}>{row.rows}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, fontFamily: FONT.mono, fontSize: 11 }}>{row.domains}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, fontFamily: FONT.mono, fontSize: 11 }}>{row.states}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, color: row.crossRef ? C.brand : C.inkLight }}>{row.crossRef ? "Yes" : "--"}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, color: row.ai ? C.brand : C.inkLight }}>{row.ai ? "Yes" : "--"}</td>
                    <td style={{ padding: "10px 14px", borderBottom: `1px solid ${C.border}`, color: row.compliance ? C.brand : C.inkLight }}>{row.compliance ? "Yes" : "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p style={{ fontSize: 10.5, color: C.inkLight, margin: "10px 0 0", lineHeight: 1.55, maxWidth: 600 }}>
            ResDAC/VRDC provides access to CMS claims microdata (billions of rows) but requires a Data Use Agreement, IRB approval, and per-project fees.
            Aradune assembles only publicly available data but normalizes and cross-references it across 18 domains for immediate query access.
          </p>
        </div>

        {/* ── WHY / HOW ──────────────────────────────────────────── */}
        <div style={{
          padding: "32px 0", borderTop: `1px solid ${C.border}`,
          display: "grid", gridTemplateColumns: `repeat(auto-fit,minmax(${isMobile ? "280px" : "300px"},1fr))`, gap: 32,
        }}>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.ink, marginBottom: 10 }}>Why this exists</div>
            <div style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.75 }}>
              Medicaid is a $1 trillion program with 50 states operating in
              isolation. Cross-state rate comparisons barely exist. Adequacy
              analysis is ad hoc. Fiscal modeling is locked inside consulting
              engagements that cost six figures per state. Aradune assembles the
              data infrastructure that should be public and makes it queryable
              through natural language.
            </div>
          </div>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.ink, marginBottom: 10 }}>How it works</div>
            <div style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.75 }}>
              100+ ETL pipelines ingest data from 80+ federal sources into a
              Hive-partitioned Parquet lake. DuckDB serves 667+ fact tables via
              FastAPI. Aradune translates natural-language questions into SQL,
              searches a 1,039-document policy corpus, and returns answers
              grounded in real data with full citation and query transparency.
            </div>
          </div>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.ink, marginBottom: 10 }}>Who it's for</div>
            <div style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.75 }}>
              State Medicaid agencies, health policy researchers, consulting
              firms, MCOs, hospitals, provider associations, journalists,
              advocates, legislative staff, and federal officials. Anyone who
              needs to understand how Medicaid dollars move and whether rates
              are adequate.
            </div>
          </div>
        </div>

        {/* ── DATA SOURCES ───────────────────────────────────────── */}
        <div style={{
          display: "flex", gap: 12, flexWrap: "wrap", padding: "16px 0 20px",
          borderTop: `1px solid ${C.border}`, marginBottom: 32,
          alignItems: "center",
        }}>
          <span style={{ fontSize: 9, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.5, fontWeight: 700 }}>SOURCES:</span>
          {["T-MSIS/TAF", "Medicare PFS", "BLS OEWS", "CMS Core Set", "HCRIS", "PBJ", "Five Star", "NADAC", "SDUD", "BRFSS", "SAMHSA", "CDC PLACES", "MACPAC", "HRSA", "NPPES", "CMS-64 FMR", "DOGE Spending", "KFF", "Census/BEA", "HUD FMR", "47 State Fee Schedules"].map(src => (
            <span key={src} style={{ fontSize: 9, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.2 }}>{src}</span>
          ))}
        </div>

        {/* ── CTA ────────────────────────────────────────────────── */}
        <div style={{
          padding: "24px 28px", background: C.white, borderRadius: 12, boxShadow: SHADOW,
          marginBottom: 48, display: "flex", alignItems: "center", justifyContent: "space-between",
          flexWrap: "wrap", gap: 16,
        }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: C.ink, marginBottom: 4 }}>Need a custom analysis?</div>
            <div style={{ fontSize: 12, color: C.inkLight }}>Rate studies, AHEAD modeling, SPA methodology, fiscal impact, CPRA compliance</div>
          </div>
          <a href="mailto:aradune-medicaid@proton.me" style={{
            padding: "10px 22px", background: C.brand, color: C.white, borderRadius: 8,
            fontSize: 12, fontWeight: 600, textDecoration: "none", whiteSpace: "nowrap",
          }}>Get in touch</a>
        </div>
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

// ── PlatformInner — the actual app shell (used by both auth modes) ─────
function PlatformInner() {
  const route = useRoute();

  const loadingFallback = <SwordLoader />;

  const renderRoute = () => {
    // Landing page is the home route
    if (route === "/" || route === "") return <Landing />;
    // Intelligence chat
    if (route === "/intelligence" || route.startsWith("/intelligence?")) {
      return <ToolErrorBoundary><Suspense fallback={loadingFallback}><IntelligenceChat /></Suspense></ToolErrorBoundary>;
    }
    if (route === "/about") return <About />;

    const tool = TOOLS.find(t => route === `/${t.id}`);
    if (tool && (tool.status === "coming")) return <ComingSoon tool={tool} />;

    // Lazy-loaded tool routes (code-split)
    // 6-module routes + all legacy routes still work
    const toolMap: Record<string, ReactElement> = {
      // ── Module routes (6 modules) ────────────────────────────
      "/state": <StateProfile />,
      "/rates": <RateAnalysis />,
      "/forecast": <CaseloadForecaster />,
      "/providers": <ProviderIntelligence />,
      "/workforce": <WorkforceQuality />,
      // ── Standalone tools ─────────────────────────────────────
      "/ask": <ToolErrorBoundary><Suspense fallback={loadingFallback}><IntelligenceChat /></Suspense></ToolErrorBoundary>,
      "/catalog": <DataCatalog />,
      "/analyst": <IntelligenceChat />,  // Legacy: was PolicyAnalyst → now Intelligence
      // ── Legacy routes → module wrappers (old bookmarks work) ──
      "/cpra": <RateAnalysis />,
      "/fees": <RateAnalysis />,
      "/builder": <RateAnalysis />,
      "/explorer": <RateAnalysis />,
      "/reduction": <RateAnalysis />,
      // Standalone tools (no tab in module wrappers)
      "/lookup": <RateLookup />,
      "/decay": <RateDecay />,
      // Provider module
      "/hospitals": <ProviderIntelligence />,
      "/ahead": <ProviderIntelligence />,
      "/ahead-readiness": <ProviderIntelligence />,
      // Workforce module
      "/wages": <WorkforceQuality />,
      "/quality": <WorkforceQuality />,
      "/hcbs8020": <WorkforceQuality />,
      "/compliance": <WorkforceQuality />,
      "/adequacy": <WorkforceQuality />,
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
    <AraduneProvider>
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
      <IntelligencePanel />
      <ReportBuilder />
      <footer style={{
        maxWidth: 1080, margin: "0 auto", padding: "24px 20px 32px",
        borderTop: `1px solid ${C.border}`,
        display: "flex", justifyContent: "space-between", alignItems: "center",
        flexWrap: "wrap", gap: 8,
      }}>
        <span style={{ fontSize: 10, color: C.inkLight }}>Aradune · aradune.co</span>
        <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>667 tables · 400M+ rows · 80+ federal sources</span>
      </footer>
    </div>
    </AraduneProvider>
  );
}

// ── Platform — top-level component with auth ────────────────────────────
// When VITE_CLERK_PUBLISHABLE_KEY is set: Clerk handles authentication
// When not set: falls back to the legacy password gate ("mediquiad")
export default function Platform() {
  const [authed, setAuthed] = useState(() => sessionStorage.getItem("aradune_auth") === "1");

  // Clerk mode — Clerk handles everything
  if (isClerkEnabled) {
    return (
      <ClerkAuthProvider>
        <RequireAuth>
          <PlatformInner />
        </RequireAuth>
      </ClerkAuthProvider>
    );
  }

  // Legacy password gate mode (fallback when Clerk is not configured)
  if (!authed) return <PasswordGate onAuth={() => setAuthed(true)} />;

  return <PlatformInner />;
}
