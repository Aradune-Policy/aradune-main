import { useState, useEffect } from "react";
import { C, FONT, SHADOW, SHADOW_LG } from "./design";
import type { ToolDef, NavGroup } from "./types";
import { STATES_LIST, STATE_NAMES } from "./data/states";
import Term from "./components/Term";
import NavDrop from "./components/NavDrop";
import NavSearch from "./components/NavSearch";
import TmsisExplorer from "./tools/TmsisExplorer";
import WageAdequacy from "./tools/WageAdequacy";
import QualityLinkage from "./tools/QualityLinkage";
import RateDecay from "./tools/RateDecay";
import RateBuilder from "./tools/RateBuilder";
import PolicyAnalyst from "./tools/PolicyAnalyst";

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
  {
    id: "explorer", group: "explore", name: "Spending Explorer",
    tagline: "Search and compare Medicaid spending across every state",
    desc: "227 million rows of claims data processed into a searchable interface. Cross-state rate comparisons, provider concentration, spending trends, and fiscal impact estimates for every HCPCS code.",
    status: "live", icon: "⌕", color: C.brand,
  },
  {
    id: "explore", group: "explore", name: "Data Explorer",
    tagline: "Filter, group, and export T-MSIS data yourself",
    desc: "Multi-select filters by state, code, and category. Group by any dimension. Bar charts, scatter plots, and full data tables with CSV export.",
    status: "coming", icon: "⊞", color: C.brand,
  },
  {
    id: "wages", group: "analyze", name: "Wage Adequacy",
    tagline: "BLS market wages vs Medicaid reimbursement rates",
    desc: "Compare what Medicaid rates can support against actual BLS wage data for 16 healthcare occupations across every state.",
    status: "live", icon: "⊿", color: C.accent,
  },
  {
    id: "quality", group: "analyze", name: "Quality Linkage",
    tagline: "CMS Core Set outcomes mapped to payment rates",
    desc: "55 quality measures across 52 jurisdictions. See whether states that pay more get better outcomes.",
    status: "live", icon: "◈", color: C.accent,
  },
  {
    id: "decay", group: "analyze", name: "Rate Decay",
    tagline: "Track how far state rates have fallen behind Medicare",
    desc: "Every HCPCS code as a percentage of the Medicare PFS. Identify which services have decayed most and which states are furthest behind.",
    status: "live", icon: "◧", color: C.accent,
  },
  {
    id: "builder", group: "build", name: "Rate Builder",
    tagline: "Calculate Medicaid rates with full audit trails",
    desc: "RBRVS rate calculations using real conversion factors, RVU components, and state-specific rules. Compare methodologies side by side.",
    status: "live", icon: "⬡", color: C.teal,
  },
  {
    id: "analyst", group: "build", name: "Policy Analyst",
    tagline: "AI-powered rate analysis and SPA language drafting",
    desc: "Ask questions in plain English. Get answers grounded in real data — rates, comparisons, fiscal impact estimates, and draft SPA methodology language.",
    status: "beta", icon: "◎", color: C.teal,
  },
  {
    id: "ahead", group: "build", name: "AHEAD Calculator",
    tagline: "Model hospital global budgets under CMS's AHEAD framework",
    desc: "Project what global budgets would look like under AHEAD parameters. Compare participation scenarios and estimate savings targets.",
    status: "coming", icon: "△", color: C.teal,
  },
  {
    id: "methods", group: "build", name: "Methodology Library",
    tagline: "How each state sets Medicaid rates, in one place",
    desc: "State-by-state reference: methodology type, conversion factors, base year, last update, SPA numbers.",
    status: "coming", icon: "≡", color: "#5B6E8A",
  },
];

const NAV_GROUPS: NavGroup[] = [
  { key: "explore", label: "Explore", tools: TOOLS.filter(t => t.group === "explore") },
  { key: "analyze", label: "Analyze", tools: TOOLS.filter(t => t.group === "analyze") },
  { key: "build", label: "Build", tools: TOOLS.filter(t => t.group === "build") },
];

const GROUP_COLORS: Record<string, string> = { explore: C.brand, analyze: C.accent, build: C.teal };
const GROUP_DESCS: Record<string, string> = {
  explore: "Browse and query T-MSIS claims data across every state and service code.",
  analyze: "Three lenses on rate adequacy — workforce viability, health outcomes, and Medicare benchmarking.",
  build: "Calculate rates, estimate fiscal impact, draft SPA language, and model hospital global budgets.",
};

// ── Platform Nav ─────────────────────────────────────────────────────────
function PlatformNav({ route }: { route: string }) {
  const activeTool = TOOLS.find(t => route === `/${t.id}`);
  return (
    <nav style={{
      position: "sticky", top: 0, zIndex: 100,
      background: "rgba(250,251,250,0.92)",
      backdropFilter: "blur(12px)",
      WebkitBackdropFilter: "blur(12px)",
      borderBottom: `1px solid ${C.border}`,
    }}>
      <div style={{
        maxWidth: 1080, margin: "0 auto", padding: "0 20px",
        display: "flex", alignItems: "center", justifyContent: "space-between", height: 48,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <a href="#/" style={{ textDecoration: "none", fontSize: 15, fontWeight: 700, color: C.ink, letterSpacing: -0.3, fontFamily: FONT.body }}>
            Aradune
          </a>
          {activeTool && (
            <>
              <span style={{ color: C.border, fontSize: 13 }}>/</span>
              <span style={{ fontSize: 12, color: C.inkLight, fontWeight: 500, fontFamily: FONT.body }}>{activeTool.name}</span>
            </>
          )}
        </div>
        <div style={{ display: "flex", gap: 2, alignItems: "center" }}>
          <NavSearch tools={TOOLS} />
          {route !== "/" && (
            <a href="#/" style={{ fontSize: 11, color: C.inkLight, textDecoration: "none", padding: "4px 10px", borderRadius: 6, fontFamily: FONT.body }}>
              All Tools
            </a>
          )}
          {NAV_GROUPS.map(g => <NavDrop key={g.key} group={g} route={route} />)}
          <a href="#/about" style={{
            fontSize: 11, fontFamily: FONT.body,
            color: route === "/about" ? C.brand : C.inkLight,
            fontWeight: route === "/about" ? 600 : 400,
            textDecoration: "none", padding: "4px 10px",
          }}>
            About
          </a>
        </div>
      </div>
    </nav>
  );
}

// ── Landing Page ─────────────────────────────────────────────────────────
function Landing() {
  const [startTab, setStartTab] = useState("state");
  const [st, setSt] = useState("");
  const [serviceQ, setServiceQ] = useState("");

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px", overflowX: "hidden", fontFamily: FONT.body }}>

      {/* 1. Hero */}
      <div style={{ padding: "56px 0 44px", maxWidth: 640 }}>
        <h1 style={{ fontSize: 30, fontWeight: 700, color: C.ink, lineHeight: 1.25, letterSpacing: -0.5, margin: 0 }}>
          Medicaid data that's actually usable.
        </h1>
        <p style={{ fontSize: 14, color: C.inkLight, lineHeight: 1.7, marginTop: 14, maxWidth: 540 }}>
          Free, open tools that turn <Term>T-MSIS</Term> <Term>claims data</Term> into
          cross-state rate comparisons, adequacy analysis, fiscal impact models, and
          draft <Term>SPA</Term> language. For policy analysts, researchers, advocates,
          and anyone who needs to understand how Medicaid dollars move.
        </p>
        <div style={{ display: "flex", gap: 10, marginTop: 22, flexWrap: "wrap" }}>
          <a href="#/explorer" style={{
            display: "inline-flex", alignItems: "center", padding: "10px 20px",
            background: C.brand, color: C.white, borderRadius: 8,
            fontSize: 13, fontWeight: 600, textDecoration: "none",
          }}>
            Explore Spending Data
          </a>
          <a href="#/about" style={{
            display: "inline-flex", alignItems: "center", padding: "10px 20px",
            background: "transparent", color: C.inkLight, borderRadius: 8,
            fontSize: 13, fontWeight: 500, textDecoration: "none",
            border: `1px solid ${C.border}`,
          }}>
            About the project
          </a>
        </div>
      </div>

      {/* 2. Stats row */}
      <div style={{
        display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(130px,1fr))",
        gap: 16, padding: "20px 0 36px", borderTop: `1px solid ${C.border}`,
      }}>
        {([["227M+", "claims rows"], ["54", "jurisdictions"], ["9,500+", "HCPCS codes"], ["$1.1T", "total spending"]] as const).map(([val, label]) => (
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
              flex: 1, maxWidth: 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FONT.body, color: st ? C.ink : C.inkLight,
              background: C.white, outline: "none",
            }}>
              <option value="">Select a state...</option>
              {STATES_LIST.map(s => <option key={s} value={s}>{s} — {STATE_NAMES[s]}</option>)}
            </select>
            <a
              href={st ? `#/explorer` : undefined}
              onClick={e => { if (!st) e.preventDefault(); }}
              style={{
                padding: "8px 16px", borderRadius: 6, border: "none",
                background: st ? C.brand : C.border, color: C.white,
                fontSize: 11, fontWeight: 600, cursor: st ? "pointer" : "default",
                textDecoration: "none", display: "inline-block",
              }}
            >
              View spending data →
            </a>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Spending, rates, adequacy, methodology</span>
          </div>
        )}

        {startTab === "service" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>Service:</span>
            <input value={serviceQ} onChange={e => setServiceQ(e.target.value)}
              placeholder="dental, office visits, 99213, therapy..."
              style={{
                flex: 1, maxWidth: 320, padding: "8px 10px", borderRadius: 6, fontSize: 12,
                border: `1px solid ${C.border}`, fontFamily: FONT.body, color: C.ink, outline: "none",
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
              flex: 1, maxWidth: 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FONT.body, color: st ? C.ink : C.inkLight,
              background: C.white, outline: "none",
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
              Check wages →
            </a>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Rates vs <Term>BLS</Term> wages and Medicare</span>
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
              gridTemplateColumns: groupTools.length <= 2 ? "repeat(auto-fill,minmax(min(100%,460px),1fr))" : "repeat(auto-fill,minmax(min(100%,320px),1fr))",
              gap: 12,
            }}>
              {groupTools.map(tool => {
                const isLive = tool.status === "live" || tool.status === "beta";
                return (
                  <div
                    key={tool.id}
                    onClick={() => { if (isLive) window.location.hash = `/${tool.id}`; else window.location.hash = `/${tool.id}`; }}
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

      {/* 5. AHEAD dark callout */}
      <div style={{
        background: C.ink, borderRadius: 14, padding: "28px 32px",
        marginBottom: 32, position: "relative", overflow: "hidden",
      }}>
        <div style={{ position: "absolute", top: -40, right: -40, width: 140, height: 140, borderRadius: "50%", background: "rgba(46,107,74,0.12)", pointerEvents: "none" }} />
        <div style={{ position: "relative" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 10 }}>
            <span style={{ fontSize: 18, width: 36, height: 36, display: "flex", alignItems: "center", justifyContent: "center", borderRadius: 8, background: "rgba(46,107,74,0.2)", color: "#7FD4A0" }}>△</span>
            <div>
              <div style={{ fontSize: 15, fontWeight: 600, color: C.white }}><Term term="AHEAD">AHEAD</Term> Hospital Global Budgets</div>
              <div style={{ fontSize: 11, color: "rgba(255,255,255,0.5)" }}>Coming soon — dual-payer budget modeling</div>
            </div>
          </div>
          <div style={{ fontSize: 12, color: "rgba(255,255,255,0.6)", lineHeight: 1.7, maxWidth: 600 }}>
            CMS's AHEAD model replaces <Term term="FFS">fee-for-service</Term> with fixed hospital budgets.
            We'll model both the Medicare and Medicaid sides — budget projections,
            scenario analysis, and participation decision support for ~300 hospitals across six states.
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(160px,1fr))", gap: 10, marginTop: 18 }}>
            {([
              ["Budget Calculator", "Historical revenue → projected HGB"],
              ["Scenario Modeling", "Volume shifts, quality, service lines"],
              ["Dual-Payer Analysis", "Medicare + Medicaid side by side"],
            ] as const).map(([title, desc]) => (
              <div key={title} style={{ padding: "12px 14px", borderRadius: 8, background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.08)" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: C.white }}>{title}</div>
                <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", marginTop: 3 }}>{desc}</div>
              </div>
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
          Aradune organizes nine tools around one workflow. Start by understanding
          what your state spends and on what. Measure whether those rates are adequate.
          Then calculate what rates should be and draft the policy to get there.
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: 14 }}>
          {([
            { num: "1", label: "Explore", q: "What are we spending?",
              desc: "Browse 227M+ claims across 54 jurisdictions. Compare rates by code, category, or state. Export raw data or use guided dashboards.",
              tools: "Spending Explorer · Data Explorer", color: C.brand },
            { num: "2", label: "Analyze", q: "Are rates adequate?",
              desc: "Three lenses: compare against BLS market wages, map quality outcomes to payment levels, and track how far rates have eroded relative to Medicare.",
              tools: "Wage Adequacy · Quality Linkage · Rate Decay", color: C.accent },
            { num: "3", label: "Build", q: "What should we pay?",
              desc: "Calculate defensible rates using real RVUs and conversion factors. Estimate fiscal impact. Draft SPA methodology language. Model hospital global budgets.",
              tools: "Rate Builder · Policy Analyst · AHEAD Calculator", color: C.teal },
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

      {/* 7. Why / How columns */}
      <div style={{ padding: "0 0 40px", display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))", gap: 24 }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>Why this exists</div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            Medicaid rate-setting is one of the most consequential policy processes in
            American healthcare, and almost nobody has the data infrastructure to do it
            well. States set rates in isolation, using methodologies that haven't been
            updated in years, with no easy way to see what other states pay, whether
            rates support a viable workforce, or what the fiscal impact of a change would
            be. Aradune puts all of that in one place, for free.
          </div>
        </div>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 8 }}>How it works</div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.75 }}>
            Data pipelines process the full <Term>T-MSIS</Term> spending dataset, Medicare
            Physician Fee Schedule, <Term>BLS</Term> wage surveys, and CMS <Term>Core Set</Term> quality
            measures. The output is static JSON served from a CDN — no cloud compute,
            no database, no ongoing costs. The code and methodology are open. An
            AI-powered policy analyst can answer complex questions by grounding
            responses in this data.
          </div>
        </div>
      </div>

      {/* 8. Data sources bar */}
      <div style={{
        display: "flex", gap: 16, flexWrap: "wrap", padding: "16px 0 24px",
        borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}`, marginBottom: 32,
      }}>
        <span style={{ fontSize: 10, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.5, fontWeight: 600 }}>DATA:</span>
        {["CMS T-MSIS", "Medicare PFS CY2025", "BLS OEWS May 2024", "CMS Core Set", "NPPES NPI", "FMAP FY2025"].map(src => (
          <span key={src} style={{ fontSize: 10, fontFamily: FONT.mono, color: C.inkLight, letterSpacing: 0.3 }}>{src}</span>
        ))}
      </div>

      {/* 9. Consulting CTA */}
      <div style={{
        padding: "28px 32px", background: C.white, borderRadius: 12, boxShadow: SHADOW,
        marginBottom: 40, display: "grid", gridTemplateColumns: "1fr auto", gap: 24, alignItems: "center",
      }}>
        <div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 6 }}>Need something more specific?</div>
          <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7 }}>
            The free tools cover most use cases. If you need custom rate studies,
            <Term>AHEAD</Term> global budget modeling with your hospital data, or <Term>SPA</Term> methodology
            consulting, reach out and we can discuss a tailored solution.
          </div>
        </div>
        <a href="mailto:aradune-medicaid@proton.me" style={{
          padding: "10px 20px", background: C.brand, color: C.white, borderRadius: 8,
          fontSize: 12, fontWeight: 600, textDecoration: "none", whiteSpace: "nowrap", flexShrink: 0,
        }}>
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
    const tool = TOOLS.find(t => route === `/${t.id}`);
    if (tool && (tool.status === "coming")) return <ComingSoon tool={tool} />;
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
