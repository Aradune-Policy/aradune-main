import React, { useState, useEffect } from "react";
import { C, FONT, SHADOW, SHADOW_LG } from "./design.js";
import TmsisExplorer from "./tools/TmsisExplorer.jsx";

import WageAdequacy from "./tools/WageAdequacy.jsx";
import QualityLinkage from "./tools/QualityLinkage.jsx";
import RateDecay from "./tools/RateDecay.jsx";
import RateBuilder from "./tools/RateBuilder.jsx";
import PolicyAnalyst from "./tools/PolicyAnalyst.jsx";
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

function navigate(path) {
  window.location.hash = path;
}

// ── Tool Registry ────────────────────────────────────────────────────────
const TOOLS = [
  {
    id: "explorer",
    name: "Spending Explorer",
    tagline: "Search and compare Medicaid spending across every state",
    desc: "227 million rows of claims data processed into a searchable interface. Cross-state rate comparisons, provider concentration, spending trends, and fiscal impact estimates for every HCPCS code.",
    status: "live",
    icon: "⌕",
    color: C.brand,
  },
  {
    id: "fees",
    name: "Fee Schedule Comparator",
    tagline: "Compare state Medicaid fee schedules side by side",
    desc: "See how rates compare across states for the same codes. Filter by methodology, service category, and provider type. Identify where your state is an outlier.",
    status: "coming",
    icon: "⇌",
    color: C.accent,
  },
  {
    id: "ahead",
    name: "AHEAD Budget Calculator",
    tagline: "Model hospital global budgets under CMS's AHEAD framework",
    desc: "Project what global budgets would look like under AHEAD parameters. Compare participation scenarios and estimate savings targets over five years.",
    status: "coming",
    icon: "◎",
    color: C.teal,
  },
  {
    id: "network",
    name: "Network Adequacy Analyzer",
    tagline: "Map provider networks against enrollee needs",
    desc: "Cross-reference provider data with enrollment density. Identify where networks are thin and connect gaps back to rate levels.",
    status: "coming",
    icon: "⬡",
    color: C.brandDeep,
  },
  {
    id: "impact",
    name: "Policy Impact Modeler",
    tagline: "Estimate fiscal impact before you file the SPA",
    desc: "Model rate changes against real claims volume. See projected cost shifts, federal share changes, and affected provider counts.",
    status: "coming",
    icon: "△",
    color: "#5B6E8A",
  },

  { id:"wages", name:"Rate-Wage Adequacy", tagline:"Can Medicaid rates sustain a workforce?",
    desc:"Compare Medicaid rates to BLS market wages. See where rates can't sustain competitive wages.",
    status:"live", icon:"⚖", color:C.accent },
  { id:"quality", name:"Quality ↔ Rate Linkage", tagline:"Does paying more produce better outcomes?",
    desc:"Connect CMS Core Set quality measures to reimbursement rates for the services that drive them.",
    status:"live", icon:"📊", color:C.accent },
  { id:"decay", name:"Rate Decay Tracker", tagline:"How stale is your fee schedule?",
    desc:"Measure how far Medicaid rates have drifted from Medicare. See which codes are most eroded.",
    status:"live", icon:"📉", color:C.accent },
  { id:"builder", name:"Rate Builder", tagline:"Calculate a rate with full documentation.",
    desc:"Enter a code, pick a methodology, get a rate. Free. RBRVS, custom CF, peer median, or manual.",
    status:"live", icon:"🧮", color:C.accent },
  { id:"analyst", name:"Policy Analyst", tagline:"AI-powered Medicaid rate analysis.",
    desc:"Ask complex rate-setting questions. Grounded in Aradune's complete dataset.",
    status:"beta", icon:"🤖", color:C.orange },
];

// ── Platform Nav ─────────────────────────────────────────────────────────
function PlatformNav({ route }) {
  const activeTool = TOOLS.find((t) => route === `/${t.id}`);
  return (
    <nav
      style={{
        position: "sticky",
        top: 0,
        zIndex: 100,
        background: "rgba(250,251,250,0.92)",
        backdropFilter: "blur(12px)",
        WebkitBackdropFilter: "blur(12px)",
        borderBottom: `1px solid ${C.border}`,
      }}
    >
      <div
        style={{
          maxWidth: 1080,
          margin: "0 auto",
          padding: "0 20px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          height: 48,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <a
            href="#/"
            style={{
              textDecoration: "none",
              fontSize: 15,
              fontWeight: 700,
              color: C.ink,
              letterSpacing: -0.3,
            }}
          >
            Aradune
          </a>
          {activeTool && (
            <>
              <span style={{ color: C.border, fontSize: 13 }}>/</span>
              <span style={{ fontSize: 12, color: C.inkLight, fontWeight: 500 }}>
                {activeTool.name}
              </span>
            </>
          )}
        </div>
        <div style={{ display: "flex", gap: 2, alignItems: "center" }}>
          {route !== "/" && (
            <a
              href="#/"
              style={{
                fontSize: 11,
                color: C.inkLight,
                textDecoration: "none",
                padding: "4px 10px",
                borderRadius: 6,
              }}
            >
              All Tools
            </a>
          )}
          {TOOLS.filter((t) => t.status === "live").map((t) => (
            <a
              key={t.id}
              href={`#/${t.id}`}
              style={{
                fontSize: 11,
                fontWeight: route === `/${t.id}` ? 600 : 400,
                color: route === `/${t.id}` ? C.brand : C.inkLight,
                textDecoration: "none",
                padding: "4px 10px",
                borderRadius: 6,
                background:
                  route === `/${t.id}` ? "rgba(46,107,74,0.06)" : "transparent",
              }}
            >
              {t.name}
            </a>
          ))}
          <a
            href="#/about"
            style={{
              fontSize: 11,
              color: route === "/about" ? C.brand : C.inkLight,
              fontWeight: route === "/about" ? 600 : 400,
              textDecoration: "none",
              padding: "4px 10px",
            }}
          >
            About
          </a>
        </div>
      </div>
    </nav>
  );
}

// ── Landing Page ─────────────────────────────────────────────────────────
function Landing() {
  const liveTool = TOOLS.find((t) => t.status === "live");
  const comingTools = TOOLS.filter((t) => t.status === "coming");

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px", overflowX: "hidden" }}>

      {/* Hero */}
      <div style={{ padding: "56px 0 44px", maxWidth: 640 }}>
        <h1
          style={{
            fontSize: 30,
            fontWeight: 700,
            color: C.ink,
            lineHeight: 1.25,
            letterSpacing: -0.5,
            margin: 0,
          }}
        >
          Medicaid data that's actually usable.
        </h1>
        <p
          style={{
            fontSize: 14,
            color: C.inkLight,
            lineHeight: 1.7,
            marginTop: 14,
            maxWidth: 540,
          }}
        >
          Aradune is a suite of free, open tools that turn massive federal
          datasets into cross-state rate comparisons, spending analysis, provider
          network maps, and fiscal impact models. For policy analysts,
          researchers, journalists, advocates, and anyone who needs to understand
          how Medicaid dollars move.
        </p>
        <div style={{ display: "flex", gap: 10, marginTop: 22, flexWrap: "wrap" }}>
          <a
            href="#/explorer"
            style={{
              display: "inline-flex",
              alignItems: "center",
              padding: "10px 20px",
              background: C.brand,
              color: C.white,
              borderRadius: 8,
              fontSize: 13,
              fontWeight: 600,
              textDecoration: "none",
              transition: "background 0.15s",
            }}
            onMouseEnter={(e) => (e.target.style.background = C.brandDeep)}
            onMouseLeave={(e) => (e.target.style.background = C.brand)}
          >
            Explore Spending Data
          </a>
          <a
            href="#/about"
            style={{
              display: "inline-flex",
              alignItems: "center",
              padding: "10px 20px",
              background: "transparent",
              color: C.inkLight,
              borderRadius: 8,
              fontSize: 13,
              fontWeight: 500,
              textDecoration: "none",
              border: `1px solid ${C.border}`,
            }}
          >
            About the project
          </a>
        </div>
      </div>

      {/* Stats row */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit,minmax(130px,1fr))",
          gap: 16,
          padding: "20px 0 36px",
          borderTop: `1px solid ${C.border}`,
        }}
      >
        {[
          ["227M+", "claims rows"],
          ["54", "jurisdictions"],
          ["9,500+", "HCPCS codes"],
          ["$1.1T", "total spending"],
        ].map(([val, label]) => (
          <div key={label}>
            <div
              style={{
                fontSize: 20,
                fontWeight: 700,
                fontFamily: FONT.mono,
                color: C.brand,
                letterSpacing: -0.5,
              }}
            >
              {val}
            </div>
            <div style={{ fontSize: 11, color: C.inkLight, marginTop: 2 }}>
              {label}
            </div>
          </div>
        ))}
      </div>

      {/* Featured tool (live) */}
      {liveTool && (
        <div style={{ paddingBottom: 24 }}>
          <div
            style={{
              fontSize: 11,
              fontWeight: 700,
              color: C.inkLight,
              textTransform: "uppercase",
              letterSpacing: 1,
              marginBottom: 12,
            }}
          >
            Available now
          </div>
          <div
            onClick={() => navigate(`/${liveTool.id}`)}
            style={{
              background: C.white,
              borderRadius: 14,
              boxShadow: SHADOW,
              padding: "28px 32px 24px",
              cursor: "pointer",
              borderLeft: `4px solid ${liveTool.color}`,
              transition: "box-shadow 0.2s, transform 0.15s",
              display: "grid",
              gridTemplateColumns: "1fr auto",
              gap: 24,
              alignItems: "center",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.boxShadow = SHADOW_LG;
              e.currentTarget.style.transform = "translateY(-1px)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.boxShadow = SHADOW;
              e.currentTarget.style.transform = "translateY(0)";
            }}
          >
            <div>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <span
                  style={{
                    fontSize: 22,
                    width: 40,
                    height: 40,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    borderRadius: 10,
                    background: `${liveTool.color}0F`,
                    color: liveTool.color,
                  }}
                >
                  {liveTool.icon}
                </span>
                <div>
                  <div style={{ fontSize: 17, fontWeight: 600, color: C.ink, letterSpacing: -0.2 }}>
                    {liveTool.name}
                  </div>
                  <div style={{ fontSize: 12, color: C.inkLight, marginTop: 1 }}>
                    {liveTool.tagline}
                  </div>
                </div>
              </div>
              <div style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.65, maxWidth: 560, marginTop: 4 }}>
                {liveTool.desc}
              </div>
            </div>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
              <span
                style={{
                  fontSize: 9,
                  padding: "3px 10px",
                  borderRadius: 10,
                  fontWeight: 600,
                  background: "rgba(46,107,74,0.08)",
                  color: C.pos,
                  whiteSpace: "nowrap",
                }}
              >
                LIVE
              </span>
              <span style={{ fontSize: 12, fontWeight: 600, color: liveTool.color, whiteSpace: "nowrap" }}>
                Open tool →
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Coming-soon tools — 2×2 grid */}
      {comingTools.length > 0 && (
        <div style={{ paddingBottom: 24 }}>
          <div
            style={{
              fontSize: 11,
              fontWeight: 700,
              color: C.inkLight,
              textTransform: "uppercase",
              letterSpacing: 1,
              marginBottom: 12,
            }}
          >
            In development
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(min(100%,460px),1fr))", gap: 12 }}>
            {comingTools.map((tool) => (
              <div
                key={tool.id}
                onClick={() => navigate("/" + tool.id)}
                style={{
                  background: C.white,
                  borderRadius: 12,
                  boxShadow: SHADOW,
                  padding: "20px 22px 18px",
                  borderLeft: `3px solid ${tool.color}`,
                  opacity: 0.88,
                  cursor: "pointer",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                  <span
                    style={{
                      fontSize: 16,
                      width: 32,
                      height: 32,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                      borderRadius: 8,
                      background: `${tool.color}0D`,
                      color: tool.color,
                      flexShrink: 0,
                    }}
                  >
                    {tool.icon}
                  </span>
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontSize: 13, fontWeight: 600, color: C.ink, letterSpacing: -0.2 }}>
                      {tool.name}
                    </div>
                    <div
                      style={{
                        fontSize: 10,
                        color: C.inkLight,
                        marginTop: 1,
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                      }}
                    >
                      {tool.tagline}
                    </div>
                  </div>
                </div>
                <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.6, marginTop: 6 }}>
                  {tool.desc}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Three questions workflow */}
      <div
        style={{
          padding: "36px 0 40px",
          borderTop: `1px solid ${C.border}`,
        }}
      >
        <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 6 }}>
          Three questions, one dataset.
        </div>
        <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7, maxWidth: 560, marginBottom: 20 }}>
          Almost everyone looking at Medicaid spending data is asking some version
          of the same three questions. Aradune is built around that workflow.
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: 14 }}>
          {[
            {
              num: "1",
              q: "What are we paying?",
              desc: "See your state's total spend, per-enrollee costs, provider concentration, and regional breakdowns — all from actual claims data.",
              tab: "Dashboard",
              color: C.brand,
            },
            {
              num: "2",
              q: "How does it compare?",
              desc: "Compare rates against every other state, the national average, Medicare benchmarks, and your own fee schedule — by code or by category.",
              tab: "Rate Engine",
              color: C.accent,
            },
            {
              num: "3",
              q: "What would it cost to change?",
              desc: "Model rate adjustments and see projected fiscal impact, broken down by code, with FFS and managed care estimates.",
              tab: "Simulator",
              color: C.teal || "#4A7C6F",
            },
          ].map((item) => (
            <div
              key={item.num}
              onClick={() => navigate("/explorer")}
              style={{
                background: C.white,
                borderRadius: 12,
                boxShadow: SHADOW,
                padding: "20px 22px 18px",
                borderTop: `3px solid ${item.color}`,
                cursor: "pointer",
                transition: "box-shadow 0.2s, transform 0.15s",
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.boxShadow = SHADOW_LG;
                e.currentTarget.style.transform = "translateY(-1px)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.boxShadow = SHADOW;
                e.currentTarget.style.transform = "translateY(0)";
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <span
                  style={{
                    width: 26,
                    height: 26,
                    borderRadius: "50%",
                    background: `${item.color}12`,
                    color: item.color,
                    fontSize: 12,
                    fontWeight: 700,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontFamily: FONT.mono,
                    flexShrink: 0,
                  }}
                >
                  {item.num}
                </span>
                <div style={{ fontSize: 14, fontWeight: 600, color: C.ink }}>{item.q}</div>
              </div>
              <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.65 }}>
                {item.desc}
              </div>
              <div style={{ fontSize: 9, fontWeight: 600, color: item.color, marginTop: 10, fontFamily: FONT.mono, textTransform: "uppercase", letterSpacing: 0.5 }}>
                → {item.tab}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* How it works */}
      <div
        style={{
          padding: "0 0 40px",
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))",
          gap: 24,
        }}
      >
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            Why this exists
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            The HHS Medicaid Provider Spending dataset is 3.4 GB. Most
            people can't open it, let alone analyze it. Cross-state Medicaid
            analysis has historically required custom data work that's
            expensive and slow. Aradune makes those analyses available to
            everyone, for free.
          </div>
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            How it works
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            A DuckDB pipeline processes the full dataset on disk. No
            cloud compute, no database, no ongoing costs. The output is static
            JSON served from a CDN. Every number traces back to actual claims
            data published by CMS, and the code and methodology are open.
          </div>
        </div>
      </div>

      {/* Consulting CTA */}
      <div
        style={{
          padding: "28px 32px",
          background: C.white,
          borderRadius: 12,
          boxShadow: SHADOW,
          marginBottom: 40,
          display: "grid",
          gridTemplateColumns: "1fr auto",
          gap: 24,
          alignItems: "center",
        }}
      >
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 6 }}>
            Need something more specific?
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7 }}>
            The free tools are designed to cover most use cases. If you need
            custom rate studies, AHEAD global budget modeling with your state's
            hospital data, or other methodology work, reach out and we can
            discuss a tailored solution.
          </div>
        </div>
        <a
          href="mailto:aradune-medicaid@proton.me"
          style={{
            padding: "10px 20px",
            background: C.brand,
            color: C.white,
            borderRadius: 8,
            fontSize: 12,
            fontWeight: 600,
            textDecoration: "none",
            whiteSpace: "nowrap",
            flexShrink: 0,
          }}
        >
          Get in touch
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

        <div
          style={{
            padding: "16px 20px",
            background: C.surface,
            borderRadius: 10,
            borderLeft: `3px solid ${C.brand}`,
          }}
        >
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
            <b>Enrollment:</b> CMS Medicaid enrollment data (November 2024, Medicaid only; CHIP excluded)
            <br />
            <b>FMAP:</b> FY2025 Federal Medical Assistance Percentages
          </div>
        </div>

        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>
            Methodology
          </div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.8 }}>
            <b>Rates:</b> total paid ÷ total claims, per code per state. No risk adjustment, no modifier weighting.
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
              display: "inline-block",
              marginTop: 8,
              color: C.brand,
              fontSize: 12,
              fontWeight: 600,
              textDecoration: "none",
            }}
          >
            aradune-medicaid@proton.me
          </a>
        </div>

        <div
          style={{
            fontSize: 11,
            color: C.inkLight,
            paddingTop: 16,
            borderTop: `1px solid ${C.border}`,
          }}
        >
          Aradune is an independent project and is not affiliated with CMS, HHS, or any state Medicaid agency.
        </div>
      </div>
    </div>
  );
}

// ── Coming Soon Page ─────────────────────────────────────────────────────
function ComingSoon({ tool }) {
  return (
    <div style={{ maxWidth: 520, margin: "0 auto", padding: "72px 20px", textAlign: "center" }}>
      <div
        style={{
          fontSize: 36,
          width: 64,
          height: 64,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          borderRadius: 16,
          background: `${tool.color}0D`,
          color: tool.color,
          margin: "0 auto 16px",
        }}
      >
        {tool.icon}
      </div>
      <h1 style={{ fontSize: 20, fontWeight: 700, color: C.ink, margin: "0 0 8px" }}>
        {tool.name}
      </h1>
      <p style={{ fontSize: 13, color: C.inkLight, lineHeight: 1.7, maxWidth: 400, margin: "0 auto 24px", textAlign: "left" }}>
        {tool.desc}
      </p>
      <div
        style={{
          display: "inline-block",
          padding: "8px 20px",
          background: C.surface,
          borderRadius: 8,
          fontSize: 12,
          color: C.inkLight,
          fontWeight: 500,
        }}
      >
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

// ── Platform Shell ───────────────────────────────────────────────────────
export default function Platform() {
  const route = useRoute();

  const renderRoute = () => {
    if (route === "/" || route === "") return <Landing />;
    if (route === "/explorer") return <TmsisExplorer />;
    if (route === "/about") return <About />;
    if (route === "/wages") return <WageAdequacy />;
    if (route === "/quality") return <QualityLinkage />;
    if (route === "/decay") return <RateDecay />;
    if (route === "/builder") return <RateBuilder />;
    if (route === "/analyst") return <PolicyAnalyst />;
    const tool = TOOLS.find((t) => route === `/${t.id}`);
    if (tool && tool.status === "coming") return <ComingSoon tool={tool} />;
    return (
      <div style={{ maxWidth: 400, margin: "0 auto", padding: "80px 20px", textAlign: "center" }}>
        <div style={{ fontSize: 14, color: C.inkLight, marginBottom: 12 }}>Page not found.</div>
        <a href="#/" style={{ fontSize: 13, color: C.brand, textDecoration: "none" }}>← Back to Aradune</a>
      </div>
    );
  };

  return (
    <div style={{ fontFamily: FONT.body, background: C.bg, minHeight: "100vh", color: C.ink }}>
      <PlatformNav route={route} />
      {renderRoute()}
      <footer
        style={{
          maxWidth: 1080,
          margin: "0 auto",
          padding: "24px 20px 32px",
          borderTop: `1px solid ${C.border}`,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          flexWrap: "wrap",
          gap: 8,
        }}
      >
        <span style={{ fontSize: 10, color: C.inkLight }}>Aradune · aradune.co</span>
        <span style={{ fontSize: 10, color: C.inkLight, fontFamily: FONT.mono }}>HHS Medicaid Provider Spending · opendata.hhs.gov</span>
      </footer>
    </div>
  );
}
