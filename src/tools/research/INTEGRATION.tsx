/**
 * Research Module Integration Guide
 * ==================================
 * Add these sections to Platform.tsx to activate the research modules.
 * Do NOT modify this file — it is a reference only.
 */

// Step 1: Add lazy imports (near other lazy imports in Platform.tsx):

import { lazy } from "react";

const RateQualityNexus = lazy(() => import("./RateQualityNexus"));
const ManagedCareValue = lazy(() => import("./ManagedCareValue"));
const TreatmentGap = lazy(() => import("./TreatmentGap"));
const SafetyNetStress = lazy(() => import("./SafetyNetStress"));
const IntegrityRisk = lazy(() => import("./IntegrityRisk"));
const FiscalCliff = lazy(() => import("./FiscalCliff"));
const MaternalHealth = lazy(() => import("./MaternalHealth"));
const PharmacySpread = lazy(() => import("./PharmacySpread"));
const NursingOwnership = lazy(() => import("./NursingOwnership"));
const WaiverImpact = lazy(() => import("./WaiverImpact"));

// Step 2: Add to TOOLS array:

const RESEARCH_TOOLS = [
  {
    id: "rate-quality", group: "research", name: "Rate-Quality Nexus",
    tagline: "Does paying more improve outcomes?",
    desc: "Cross-domain analysis of Medicaid rate adequacy, quality measures, workforce supply, and provider access. OLS with controls, panel fixed effects, and difference-in-differences.",
    status: "live" as const, icon: "R", color: "#2E6B4A",
  },
  {
    id: "mc-value", group: "research", name: "Managed Care Value",
    tagline: "Is managed care saving money?",
    desc: "Evaluates whether Medicaid managed care delivers lower costs and better outcomes. MCO MLR analysis, spending regression, quality panel fixed effects.",
    status: "live" as const, icon: "M", color: "#3A7D5C",
  },
  {
    id: "treatment-gap", group: "research", name: "Opioid Treatment Gap",
    tagline: "Where does prevalence outstrip treatment?",
    desc: "Maps the demand-supply-spending pipeline for OUD treatment. NSDUH prevalence, MAT drug spending, facility capacity, and block grant alignment.",
    status: "live" as const, icon: "T", color: "#6366F1",
  },
  {
    id: "safety-net", group: "research", name: "Safety Net Stress Test",
    tagline: "Which states are buckling?",
    desc: "Multi-dimensional safety net strain: hospital margins, nursing quality, PBJ staffing, HCBS waitlists, and composite stress index.",
    status: "live" as const, icon: "S", color: "#A4262C",
  },
  {
    id: "integrity-risk", group: "research", name: "Integrity Risk Index",
    tagline: "Composite fraud risk scoring",
    desc: "State-level program integrity risk combining Open Payments ($13B), LEIE exclusions, PERM error rates, and MFCU enforcement capacity.",
    status: "live" as const, icon: "I", color: "#B8860B",
  },
  {
    id: "fiscal-cliff", group: "research", name: "Fiscal Cliff Analysis",
    tagline: "Which states hit the wall first?",
    desc: "Comparative fiscal pressure as enhanced FMAP expires. CMS-64 state share vs tax revenue, GDP growth, FMAP trends, vulnerability ranking.",
    status: "live" as const, icon: "F", color: "#C4590A",
  },
  {
    id: "maternal-health", group: "research", name: "Maternal Health Deserts",
    tagline: "Multi-factor maternal risk",
    desc: "Maternal mortality, social vulnerability, HPSA shortages, and quality measure performance. Identifies compound maternal health deserts.",
    status: "live" as const, icon: "H", color: "#9333EA",
  },
  {
    id: "pharmacy-spread", group: "research", name: "Pharmacy Spread Analysis",
    tagline: "The $3B overpayment gap",
    desc: "NADAC acquisition cost vs SDUD Medicaid reimbursement. Drug-level, state-level, and therapeutic tier analysis with full robustness checks.",
    status: "live" as const, icon: "P", color: "#0891B2",
  },
  {
    id: "nursing-ownership", group: "research", name: "Nursing Ownership & Quality",
    tagline: "For-profit chain quality gap",
    desc: "Ownership-quality relationship with state FE, PSM (10,737 matched pairs), and interaction models. Cohen's d=0.50 across 14,710 facilities.",
    status: "live" as const, icon: "N", color: "#059669",
  },
  {
    id: "waiver-impact", group: "research", name: "Section 1115 Waiver Impact",
    tagline: "Do waivers actually work?",
    desc: "Before/after evaluation of 647 Section 1115 waivers. Enrollment, spending, and quality trajectories with waiver-type comparisons.",
    status: "live" as const, icon: "W", color: "#4F46E5",
  },
];

// Step 3: Add NAV_GROUP:

const RESEARCH_NAV_GROUP = {
  key: "research",
  label: "Research",
  tools: RESEARCH_TOOLS,
};

// Step 4: Add to toolMap:

const RESEARCH_TOOL_MAP: Record<string, React.ReactElement> = {
  "/research/rate-quality": <RateQualityNexus />,
  "/research/mc-value": <ManagedCareValue />,
  "/research/treatment-gap": <TreatmentGap />,
  "/research/safety-net": <SafetyNetStress />,
  "/research/integrity-risk": <IntegrityRisk />,
  "/research/fiscal-cliff": <FiscalCliff />,
  "/research/maternal-health": <MaternalHealth />,
  "/research/pharmacy-spread": <PharmacySpread />,
  "/research/nursing-ownership": <NursingOwnership />,
  "/research/waiver-impact": <WaiverImpact />,
};

// Merge into existing: { ...existingToolMap, ...RESEARCH_TOOL_MAP }

export { RESEARCH_TOOLS, RESEARCH_NAV_GROUP, RESEARCH_TOOL_MAP };
