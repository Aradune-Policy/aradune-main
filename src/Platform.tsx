import { useState, useEffect, lazy, Suspense, Component } from "react";
import type { ReactNode, ErrorInfo, ReactElement } from "react";
import { C, FONT, SHADOW, SHADOW_LG, useIsMobile } from "./design";
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
const FiscalImpact = lazy(() => import("./tools/FiscalImpact"));
const BehavioralHealth = lazy(() => import("./tools/BehavioralHealth"));
const PharmacyIntelligence = lazy(() => import("./tools/PharmacyIntelligence"));
const NursingFacility = lazy(() => import("./tools/NursingFacility"));
const SpendingEfficiency = lazy(() => import("./tools/SpendingEfficiency"));
const ProgramIntegrity = lazy(() => import("./tools/ProgramIntegrity"));
const HospitalRateSetting = lazy(() => import("./tools/HospitalRateSetting"));
// Module wrappers (RateAnalysis, ProviderIntelligence, WorkforceQuality) removed from routing
// All tools are standalone now — kept in src/tools/ for reference

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

// ── Tool Registry (standalone modules) ───────────────────────────────────
const TOOLS: ToolDef[] = [
  // ── STATES (direct link, no dropdown) ─────────────────────────────────
  {
    id: "state", group: "states", name: "State Profiles",
    tagline: "Everything Aradune knows about a state, in one view",
    desc: "Enrollment, rates, hospitals, quality, workforce, pharmacy, and economic context for any state, unified from 250 fact tables with cross-dataset insights.",
    status: "live", icon: "◉", color: C.brand,
  },
  // ── RATES ─────────────────────────────────────────────────────────────
  {
    id: "rates", group: "rates", name: "Rate Comparison",
    tagline: "Medicaid-to-Medicare rate parity across all states",
    desc: "16,000+ HCPCS codes across 47 states compared against the Medicare PFS. Cross-state rankings, rate erosion tracking, and impact analysis.",
    status: "live", icon: "◧", color: C.brand,
  },
  {
    id: "cpra", group: "rates", name: "CPRA Generator",
    tagline: "42 CFR 447.203 Comparative Payment Rate Analysis",
    desc: "Generate the Comparative Payment Rate Analysis required by July 2026. Pre-computed rates for 45 states or upload your own fee schedule.",
    status: "live", icon: "◆", color: C.brand,
  },
  {
    id: "lookup", group: "rates", name: "Rate Lookup",
    tagline: "Search any HCPCS code across all states",
    desc: "Look up Medicaid reimbursement rates for any procedure code across all 47 states with fee schedule data.",
    status: "live", icon: "⌗", color: C.brand,
  },
  // ── FORECAST (direct link, no dropdown) ───────────────────────────────
  {
    id: "forecast", group: "forecast", name: "Forecasting",
    tagline: "Caseload and expenditure projections with scenario modeling",
    desc: "Upload monthly enrollment data. SARIMAX + ETS model competition, caseload forecasts with confidence intervals, expenditure projections.",
    status: "live", icon: "◐", color: C.teal,
  },
  {
    id: "fiscal-impact", group: "forecast", name: "Fiscal Impact",
    tagline: "Rate change budget impact with FMAP and UPL analysis",
    desc: "Model rate increases: federal match at FMAP, UPL headroom check, biennial state/federal cost split from CMS-64 and enrollment data.",
    status: "live", icon: "◑", color: C.teal,
  },
  {
    id: "spending", group: "forecast", name: "Spending Efficiency",
    tagline: "Per-enrollee spending, CMS-64 expenditure, and efficiency metrics",
    desc: "Compare Medicaid spending across states: per-enrollee costs by eligibility group, CMS-64 federal/state split, and spending efficiency vs managed care penetration.",
    status: "live", icon: "◑", color: C.teal,
  },
  // ── PROVIDERS ─────────────────────────────────────────────────────────
  {
    id: "hospitals", group: "providers", name: "Hospital Intelligence",
    tagline: "AHEAD readiness, financials, and peer benchmarks",
    desc: "Search any hospital by name. HCRIS financials, AHEAD readiness scoring, global budget modeling, peer comparison.",
    status: "live", icon: "△", color: C.accent,
  },
  {
    id: "ahead", group: "providers", name: "AHEAD Calculator",
    tagline: "AHEAD model savings and global budget projections",
    desc: "Model hospital participation in the CMS AHEAD model. Project savings, global budgets, and readiness.",
    status: "live", icon: "△", color: C.accent,
  },
  {
    id: "hospital-rates", group: "providers", name: "Hospital Rate Setting",
    tagline: "HCRIS financials, DSH, supplemental payments, and SDPs",
    desc: "Hospital cost reports, DSH allotments, MACPAC supplemental payment analysis, and state directed payment programs across all states.",
    status: "live", icon: "▲", color: C.accent,
  },
  // ── WORKFORCE ─────────────────────────────────────────────────────────
  {
    id: "wages", group: "workforce", name: "Wage Adequacy",
    tagline: "BLS market wages vs Medicaid reimbursement",
    desc: "Compare Bureau of Labor Statistics market wages against Medicaid reimbursement rates for key healthcare occupations.",
    status: "live", icon: "⊿", color: C.teal,
  },
  {
    id: "compliance", group: "workforce", name: "Compliance Center",
    tagline: "42 CFR 447.203 requirements and transparency",
    desc: "Rate transparency, HCBS pass-through tracking, rate reduction analysis for the July 2026 Ensuring Access deadline.",
    status: "live", icon: "◇", color: C.teal,
  },
  // ── PHARMACY ─────────────────────────────────────────────────────────
  {
    id: "pharmacy", group: "pharmacy", name: "Pharmacy Intelligence",
    tagline: "Medicaid drug spending, utilization, and NADAC pricing",
    desc: "SDUD drug utilization across all states, top drugs by Medicaid spending, NADAC pricing benchmarks, and drug rebate program data. 28.3M rows of SDUD data.",
    status: "live", icon: "◎", color: "#7C3AED",
  },
  // ── BEHAVIORAL HEALTH ───────────────────────────────────────────────
  {
    id: "behavioral-health", group: "behavioral-health", name: "Behavioral Health & SUD",
    tagline: "Mental health prevalence, treatment access, and opioid prescribing",
    desc: "NSDUH prevalence rankings, MH/SUD treatment facility network, opioid prescribing patterns, SAMHSA block grants, IPF quality, and BH service utilization across all states.",
    status: "live", icon: "◈", color: "#6366F1",
  },
  // ── NURSING ───────────────────────────────────────────────────────
  {
    id: "nursing", group: "nursing", name: "Nursing Facility",
    tagline: "Five-Star quality ratings and PBJ staffing data",
    desc: "CMS Five-Star nursing home quality ratings, PBJ nurse staffing hours per resident day, deficiency tracking, and facility-level detail for all certified SNFs.",
    status: "live", icon: "\u25EB", color: "#D97706",
  },
  // ── INTEGRITY ───────────────────────────────────────────────────────
  {
    id: "integrity", group: "integrity", name: "Program Integrity",
    tagline: "LEIE exclusions, Open Payments, MFCU stats, and PERM error rates",
    desc: "OIG LEIE exclusion list (82K+ records), CMS Open Payments ($13B+ in industry payments), MFCU fraud investigation statistics, and PERM improper payment rates.",
    status: "live", icon: "\u25C7", color: "#DC2626",
  },
];

const NAV_GROUPS: NavGroup[] = [
  { key: "states", label: "States", tools: TOOLS.filter(t => t.group === "states") },
  { key: "rates", label: "Rates", tools: TOOLS.filter(t => t.group === "rates") },
  { key: "forecast", label: "Forecast", tools: TOOLS.filter(t => t.group === "forecast") },
  { key: "providers", label: "Providers", tools: TOOLS.filter(t => t.group === "providers") },
  { key: "workforce", label: "Workforce", tools: TOOLS.filter(t => t.group === "workforce") },
  { key: "pharmacy", label: "Pharmacy", tools: TOOLS.filter(t => t.group === "pharmacy") },
  { key: "behavioral-health", label: "BH/SUD", tools: TOOLS.filter(t => t.group === "behavioral-health") },
  { key: "nursing", label: "Nursing", tools: TOOLS.filter(t => t.group === "nursing") },
  { key: "integrity", label: "Integrity", tools: TOOLS.filter(t => t.group === "integrity") },
];

const GROUP_COLORS: Record<string, string> = {
  states: C.brand, rates: C.brand, forecast: C.teal,
  providers: C.accent, workforce: C.teal, pharmacy: "#7C3AED", "behavioral-health": "#6366F1",
  nursing: "#D97706", integrity: "#DC2626",
};
const GROUP_DESCS: Record<string, string> = {
  states: "State profiles with enrollment, rates, hospitals, quality, and economic context.",
  rates: "Fee schedule comparison, CPRA compliance, rate lookup, and rate modeling.",
  forecast: "Caseload and expenditure projections with scenario modeling.",
  providers: "Hospital intelligence, AHEAD readiness, and provider spending analysis.",
  workforce: "Wage adequacy, quality linkage, HCBS tracking, and compliance.",
  pharmacy: "Medicaid drug spending, utilization, NADAC pricing, and drug rebate data.",
  "behavioral-health": "Mental health prevalence, SUD treatment, opioid prescribing, and BH services.",
  nursing: "Nursing facility Five-Star quality ratings, PBJ staffing, and facility-level detail.",
  integrity: "LEIE exclusions, Open Payments, MFCU fraud statistics, and PERM error rates.",
};

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
      title: "States",
      color: C.brand,
      modules: [
        { id: "state", name: "State Profiles", desc: "54-jurisdiction dashboards: enrollment, rates, hospitals, quality, workforce, pharmacy, and economic context with cross-dataset insights", route: "#/state/FL" },
      ],
    },
    {
      title: "Rates & Compliance",
      color: C.brand,
      modules: [
        { id: "rates", name: "Rate Comparison", desc: "Medicaid-to-Medicare parity across 47 states, 16,000+ procedure codes", route: "#/rates" },
        { id: "cpra", name: "CPRA Generator", desc: "42 CFR 447.203 Comparative Payment Rate Analysis for July 2026 deadline", route: "#/cpra" },
        { id: "lookup", name: "Rate Lookup", desc: "Search any HCPCS code across all states instantly", route: "#/lookup" },
        { id: "compliance", name: "Compliance Center", desc: "Ensuring Access requirements, rate transparency, HCBS pass-through, rate reduction analysis", route: "#/compliance" },
      ],
    },
    {
      title: "Forecasting",
      color: C.teal,
      modules: [
        { id: "forecast", name: "Caseload & Expenditure", desc: "SARIMAX + ETS model competition with scenario modeling and expenditure projections", route: "#/forecast" },
        { id: "fiscal-impact", name: "Fiscal Impact", desc: "Rate change budget impact: FMAP federal match, UPL headroom, biennial state/federal cost split", route: "#/fiscal-impact" },
      ],
    },
    {
      title: "Providers & Hospitals",
      color: C.accent,
      modules: [
        { id: "hospitals", name: "Hospital Intelligence", desc: "HCRIS financials, AHEAD readiness, global budget modeling, peer benchmarks", route: "#/hospitals" },
        { id: "ahead", name: "AHEAD Calculator", desc: "CMS AHEAD model participation scoring and savings projections", route: "#/ahead" },
      ],
    },
    {
      title: "Workforce",
      color: C.teal,
      modules: [
        { id: "wages", name: "Wage Adequacy", desc: "BLS market wages vs Medicaid reimbursement for healthcare occupations", route: "#/wages" },
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
    try { sessionStorage.setItem("aradune_pending_query", chatInput.trim()); } catch {}
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
            667+ tables across 18 data domains. Upload your own data to cross-reference
            against the national layer. An AI analyst that reads the data, connects the
            dots, and writes the analysis. Full audit trails. Submission-ready compliance
            output. Not a dashboard. An intelligence system built for anyone who needs
            to understand Medicaid.
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
                  onClick={() => { try { sessionStorage.setItem("aradune_pending_query", s.prompt); } catch {} window.location.hash = `#/intelligence?q=${encodeURIComponent(s.prompt)}`; }}
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

          <div style={{ maxWidth: 680, margin: "0 auto" }}>

            {/* ── Intelligence Layer ── */}
            <div style={{
              borderRadius: 12, padding: isMobile ? "14px 12px" : "14px 16px",
              background: "#085041", color: "#E1F5EE", marginBottom: 10,
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>
                Intelligence layer
              </div>
              <div style={{
                display: "flex", gap: isMobile ? 6 : 8, flexWrap: "wrap", justifyContent: "center", marginBottom: 10,
              }}>
                {[
                  { label: "Extended\nthinking", icon: <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#9FE1CB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 8v4l2 2"/></svg> },
                  { label: "DuckDB\ntools", icon: <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#9FE1CB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"/><path d="M8 8h8M8 12h5"/></svg> },
                  { label: "AI nucleus\nClaude 4.6", icon: <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#9FE1CB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3a7 7 0 017 7c0 3-2 5-4 7l-3 4-3-4c-2-2-4-4-4-7a7 7 0 017-7z"/><circle cx="12" cy="10" r="2"/></svg> },
                  { label: "RAG\nengine", icon: <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#9FE1CB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M4 6h16M4 12h16M4 18h10"/><circle cx="19" cy="18" r="3"/></svg> },
                  { label: "Web\nsearch", icon: <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="#9FE1CB" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3a15 15 0 014 9 15 15 0 01-4 9 15 15 0 01-4-9 15 15 0 014-9z"/></svg> },
                ].map((cap, i) => (
                  <div key={i} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4, minWidth: 60 }}>
                    <div style={{
                      width: 40, height: 40, borderRadius: "50%", background: "rgba(255,255,255,0.12)",
                      display: "flex", alignItems: "center", justifyContent: "center",
                    }}>{cap.icon}</div>
                    <div style={{ fontSize: 10, opacity: 0.7, textAlign: "center", lineHeight: 1.2, whiteSpace: "pre-line" }}>{cap.label}</div>
                  </div>
                ))}
              </div>
              <div style={{
                background: "rgba(255,255,255,0.1)", borderRadius: 6, padding: "6px 12px",
                textAlign: "center", fontSize: 11, letterSpacing: 0.5,
              }}>
                {`Sonnet for analysis \u00B7 Haiku for routing \u00B7 Opus for complex reasoning \u00B7 1,039 CMS policy docs \u00B7 6,058 RAG chunks`}
              </div>
            </div>

            {/* Connector */}
            <div style={{ height: 16, display: "flex", justifyContent: "center", alignItems: "center" }}>
              <div style={{ display: "flex", gap: 3 }}>
                {["#5DCAA5", "#1D9E75", "#0F6E56", "#085041"].map((c, i) => (
                  <div key={i} style={{ width: 2, height: 16, borderRadius: 1, background: c }} />
                ))}
              </div>
            </div>

            {/* ── Structured Tools ── */}
            <div style={{
              borderRadius: 12, padding: isMobile ? "14px 12px" : "14px 16px",
              background: "#0F6E56", color: "#E1F5EE", marginBottom: 10,
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>
                {`Structured tools \u00B7 258+ API endpoints \u00B7 Fly.io`}
              </div>
              <div style={{
                display: "flex", gap: 6, flexWrap: "wrap", justifyContent: "center",
              }}>
                {[
                  { name: "States", sub: "50-state profiles" },
                  { name: "Rates", sub: "Fee schedules" },
                  { name: "CPRA", sub: "Compliance" },
                  { name: "Forecast", sub: "Caseload/spend" },
                  { name: "AHEAD", sub: "Readiness" },
                  { name: "Providers", sub: "Network gaps" },
                  { name: "Workforce", sub: "HCBS supply" },
                  { name: "Lookup", sub: "Rate search" },
                ].map(t => (
                  <div key={t.name} style={{
                    background: "rgba(255,255,255,0.1)", borderRadius: 8,
                    padding: "8px 10px", flex: 1, minWidth: 70, maxWidth: 110, textAlign: "center",
                  }}>
                    <div style={{ fontSize: 11, fontWeight: 500, marginBottom: 2 }}>{t.name}</div>
                    <div style={{ fontSize: 9, opacity: 0.6 }}>{t.sub}</div>
                  </div>
                ))}
              </div>
              <div style={{ textAlign: "center", fontSize: 10, opacity: 0.5, marginTop: 6 }}>
                {`Every tool has: Ask Intelligence sidebar \u00B7 export (DOCX/PDF/Excel/CSV) \u00B7 user data import`}
              </div>
            </div>

            {/* Connector */}
            <div style={{ height: 16, display: "flex", justifyContent: "center", alignItems: "center" }}>
              <div style={{ display: "flex", gap: 3 }}>
                {["#5DCAA5", "#1D9E75", "#0F6E56", "#085041"].map((c, i) => (
                  <div key={i} style={{ width: 2, height: 16, borderRadius: 1, background: c }} />
                ))}
              </div>
            </div>

            {/* ── Entity Registry / Ontology ── */}
            <div style={{
              borderRadius: 12, padding: isMobile ? "14px 12px" : "14px 16px",
              background: "#1D9E75", color: "#E1F5EE", marginBottom: 10,
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>
                {`Entity registry \u00B7 Ontology layer`}
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {[
                  { title: "YAML entities", items: "State \u00B7 Procedure \u00B7 Provider\nHospital \u00B7 MCO \u00B7 Rate Cell\nDrug \u00B7 Quality \u00B7 Policy Doc" },
                  { title: "DuckPGQ graph", items: "Auto-generated property graph\nSQL/PGQ queries over\nsame underlying tables" },
                  { title: "Named metrics", items: "Deterministic calcs\npct_of_medicare \u00B7 per_enrollee\ncpra_ratio \u00B7 rate_decay" },
                ].map(b => (
                  <div key={b.title} style={{
                    flex: 1, minWidth: 130, background: "rgba(255,255,255,0.1)",
                    borderRadius: 6, padding: "8px 10px",
                  }}>
                    <div style={{ fontSize: 11, fontWeight: 500, marginBottom: 3 }}>{b.title}</div>
                    <div style={{ fontSize: 10, opacity: 0.7, lineHeight: 1.4, whiteSpace: "pre-line" }}>{b.items}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Connector */}
            <div style={{ height: 16, display: "flex", justifyContent: "center", alignItems: "center" }}>
              <div style={{ display: "flex", gap: 3 }}>
                {["#5DCAA5", "#1D9E75", "#0F6E56", "#085041"].map((c, i) => (
                  <div key={i} style={{ width: 2, height: 16, borderRadius: 1, background: c }} />
                ))}
              </div>
            </div>

            {/* ── Data Lake ── */}
            <div style={{
              borderRadius: 12, padding: isMobile ? "14px 12px" : "14px 16px",
              background: C.ink, color: "#E1F5EE", marginBottom: 10,
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, textTransform: "uppercase", opacity: 0.7, marginBottom: 8 }}>
                The data lake
              </div>
              <div style={{
                display: "flex", alignItems: "center", justifyContent: "center", gap: 6, marginBottom: 10,
              }}>
                <div style={{ borderRadius: 6, padding: "6px 14px", fontSize: 11, fontWeight: 500, textAlign: "center", background: "#412402", color: "#FAC775" }}>
                  Bronze<br /><span style={{ fontSize: 9, fontWeight: 400, opacity: 0.7 }}>Raw ingestion</span>
                </div>
                <div style={{ fontSize: 14, opacity: 0.5 }}>{"\u2192"}</div>
                <div style={{ borderRadius: 6, padding: "6px 14px", fontSize: 11, fontWeight: 500, textAlign: "center", background: "#2C2C2A", color: "#D3D1C7" }}>
                  Silver<br /><span style={{ fontSize: 9, fontWeight: 400, opacity: 0.7 }}>Normalized</span>
                </div>
                <div style={{ fontSize: 14, opacity: 0.5 }}>{"\u2192"}</div>
                <div style={{ borderRadius: 6, padding: "6px 14px", fontSize: 11, fontWeight: 500, textAlign: "center", background: "#854F0B", color: "#FAEEDA" }}>
                  Gold<br /><span style={{ fontSize: 9, fontWeight: 400, opacity: 0.7 }}>Analytics-ready</span>
                </div>
              </div>
              <div style={{
                display: "flex", gap: 8, justifyContent: "center", flexWrap: "wrap", marginBottom: 6,
              }}>
                {[
                  { val: "667+", dim: "tables" },
                  { val: "400M+", dim: "rows" },
                  { val: "4.9", dim: "GB" },
                  { val: "50", dim: "states" },
                ].map(s => (
                  <div key={s.dim} style={{ fontSize: 13, fontWeight: 500, opacity: 0.9 }}>
                    {s.val}<span style={{ fontSize: 11, opacity: 0.5, marginLeft: 2 }}>{s.dim}</span>
                  </div>
                ))}
              </div>
              <div style={{
                background: "rgba(255,255,255,0.1)", borderRadius: 6, padding: "6px 12px",
                textAlign: "center", fontSize: 11, opacity: 0.6,
              }}>
                {`Hive-partitioned Parquet \u00B7 DuckDB in-memory \u00B7 Cloudflare R2 \u00B7 User session data (isolated)`}
              </div>
            </div>

            {/* Connector */}
            <div style={{ height: 16, display: "flex", justifyContent: "center", alignItems: "center" }}>
              <div style={{ display: "flex", gap: 3 }}>
                {["#5DCAA5", "#1D9E75", "#0F6E56", "#085041"].map((c, i) => (
                  <div key={i} style={{ width: 2, height: 16, borderRadius: 1, background: c }} />
                ))}
              </div>
            </div>

            {/* ── Infrastructure ── */}
            <div style={{
              borderRadius: 12, padding: isMobile ? "14px 12px" : "14px 16px",
              background: C.surface, color: C.ink, border: `0.5px solid ${C.border}`,
            }}>
              <div style={{ fontSize: 10, letterSpacing: 2, textTransform: "uppercase", opacity: 0.7, marginBottom: 8, color: C.inkLight }}>
                Infrastructure + validation
              </div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap", justifyContent: "center" }}>
                {["115+ ETL scripts", "Dagster", "Soda Core", "dbt", "Pandera", "React 18 \u00B7 Vercel", "FastAPI \u00B7 Fly.io", "Clerk auth", "GitHub CI/CD"].map(tag => (
                  <div key={tag} style={{
                    fontSize: 10, padding: "4px 10px", borderRadius: 4,
                    background: C.white, color: C.inkLight, border: `0.5px solid ${C.border}`,
                  }}>{tag}</div>
                ))}
              </div>
            </div>

            {/* Tagline */}
            <div style={{
              textAlign: "center", marginTop: 12, fontSize: 10,
              color: C.inkLight, letterSpacing: 1, fontStyle: "italic",
            }}>
              {`The data is the moat \u00B7 Intelligence is the interface \u00B7 Compliance is the wedge`}
            </div>

          </div>

          {/* ── YOUR DATA + OUR INTELLIGENCE ─────────────────────── */}
          <div style={{
            margin: "0 0 40px",
            padding: isMobile ? "24px 18px 20px" : "28px 28px 24px",
            background: C.white, borderRadius: 12, boxShadow: SHADOW,
            border: `1px solid ${C.brand}15`,
          }}>
            <div style={{ marginBottom: 20 }}>
              <div style={{
                fontSize: 10, fontWeight: 700, color: C.brand, fontFamily: FONT.mono,
                letterSpacing: 1.5, textTransform: "uppercase", marginBottom: 8,
              }}>YOUR DATA + OUR INTELLIGENCE</div>
              <h3 style={{ fontSize: isMobile ? 15 : 17, fontWeight: 800, color: C.ink, margin: "0 0 6px", letterSpacing: -0.3 }}>
                Upload your data. Cross-reference everything.
              </h3>
              <p style={{ fontSize: 13, color: C.inkLight, margin: 0, lineHeight: 1.6, maxWidth: 560 }}>
                Bring your fee schedules, enrollment projections, or claims data.
                Aradune cross-references your files against 667+ national tables in real time,
                inside your session.
              </p>
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: `repeat(auto-fit, minmax(${isMobile ? "260px" : "200px"}, 1fr))`,
              gap: 12,
            }}>
              {[
                {
                  title: "Upload & Cross-Reference",
                  desc: "CSV, Excel, or JSON up to 50 MB. Your data becomes a queryable table alongside the entire national layer.",
                },
                {
                  title: "Full Audit Trail",
                  desc: "Every query logged. Every number traceable to source table, row, and data vintage. Nothing is a black box.",
                },
                {
                  title: "Professional Output",
                  desc: "Generate submission-ready CPRAs, SPA methodology documents, fiscal impact analyses, and rate adequacy reports.",
                },
                {
                  title: "Session-Scoped & Secure",
                  desc: "Your data never persists beyond your session. Never shared. Encrypted in transit. Fully isolated.",
                },
              ].map(item => (
                <div key={item.title} style={{
                  padding: "14px 16px",
                  background: C.surface, borderRadius: 8, border: `1px solid ${C.border}`,
                }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: C.ink, marginBottom: 4 }}>{item.title}</div>
                  <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.55 }}>{item.desc}</div>
                </div>
              ))}
            </div>
          </div>

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
      // ── Standalone modules (each does one thing) ─────────────
      "/state": <StateProfile />,
      "/rates": <TmsisExplorer />,
      "/cpra": <CpraGenerator />,
      "/lookup": <RateLookup />,
      "/forecast": <CaseloadForecaster />,
      "/fiscal-impact": <FiscalImpact />,
      "/spending": <SpendingEfficiency />,
      "/hospitals": <AheadReadiness />,
      "/ahead": <AheadCalculator />,
      "/wages": <WageAdequacy />,
      "/compliance": <ComplianceReport />,
      "/behavioral-health": <BehavioralHealth />,
      "/pharmacy": <PharmacyIntelligence />,
      "/nursing": <NursingFacility />,
      "/hospital-rates": <HospitalRateSetting />,
      "/integrity": <ProgramIntegrity />,
      // ── Utility tools ──────────────────────────────────────────
      "/ask": <ToolErrorBoundary><Suspense fallback={loadingFallback}><IntelligenceChat /></Suspense></ToolErrorBoundary>,
      "/catalog": <DataCatalog />,
      // ── Legacy routes (old bookmarks still work) ───────────────
      "/analyst": <IntelligenceChat />,
      "/fees": <FeeScheduleDir />,
      "/builder": <RateBuilder />,
      "/explorer": <TmsisExplorer />,
      "/reduction": <RateReduction />,
      "/decay": <RateDecay />,
      "/ahead-readiness": <AheadReadiness />,
      "/quality": <QualityLinkage />,
      "/hcbs8020": <HcbsTracker />,
      "/adequacy": <WageAdequacy />,
      "/providers": <AheadReadiness />,
      "/workforce": <WageAdequacy />,
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
