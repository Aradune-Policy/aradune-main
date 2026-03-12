import { useState, useEffect, useRef } from "react";

// ── Design tokens (exact match to design.js) ────────────────────────
const C = {
  ink: "#0A2540", inkLight: "#425A70", pos: "#2E6B4A", neg: "#A4262C",
  warn: "#B8860B", surface: "#F5F7F5", bg: "#FAFBFA", border: "#E4EAE4",
  white: "#fff", brand: "#2E6B4A", brandDeep: "#1B5E3A", accent: "#C4590A",
  teal: "#3A7D5C"
};
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";
const SH_LG = "0 4px 16px rgba(0,0,0,.06),0 12px 40px rgba(0,0,0,.04)";

// ── States ──────────────────────────────────────────────────────────
const STATES_LIST = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA",
  "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM",
  "NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
];
const SN = {
  FL:"Florida",TX:"Texas",CA:"California",NY:"New York",OH:"Ohio",GA:"Georgia",
  PA:"Pennsylvania",IL:"Illinois",MI:"Michigan",NC:"North Carolina",NJ:"New Jersey",
  VA:"Virginia",WA:"Washington",AZ:"Arizona",MA:"Massachusetts",TN:"Tennessee",
  IN:"Indiana",MO:"Missouri",MD:"Maryland",WI:"Wisconsin",CO:"Colorado",MN:"Minnesota",
  SC:"South Carolina",AL:"Alabama",LA:"Louisiana",KY:"Kentucky",OR:"Oregon",
  OK:"Oklahoma",CT:"Connecticut",UT:"Utah",IA:"Iowa",NV:"Nevada",AR:"Arkansas",
  MS:"Mississippi",KS:"Kansas",NM:"New Mexico",NE:"Nebraska",ID:"Idaho",WV:"West Virginia",
  HI:"Hawaii",NH:"New Hampshire",ME:"Maine",RI:"Rhode Island",MT:"Montana",
  DE:"Delaware",SD:"South Dakota",ND:"North Dakota",AK:"Alaska",VT:"Vermont",
  WY:"Wyoming",DC:"District of Columbia"
};

// ── Glossary ────────────────────────────────────────────────────────
const GLOSSARY = {
  "HCPCS": "Healthcare Common Procedure Coding System — the code set used to bill for medical services.",
  "T-MSIS": "Transformed Medicaid Statistical Information System — the federal database where states report every Medicaid claim.",
  "SPA": "State Plan Amendment — the document a state files with CMS when changing its Medicaid reimbursement methodology.",
  "FMAP": "Federal Medical Assistance Percentage — the federal government's share of Medicaid costs.",
  "RBRVS": "Resource-Based Relative Value Scale — the system Medicare uses to price physician services.",
  "FFS": "Fee-for-service — providers bill per service, as opposed to capitated or global budget models.",
  "Core Set": "Standardized quality measures states report to CMS — well-child visits, diabetes management, maternal care, etc.",
  "claims data": "Records of services billed to and paid by Medicaid — code, provider, date, amount billed, amount paid.",
  "BLS": "Bureau of Labor Statistics — publishes wage data used to compare Medicaid rates against market wages.",
  "AHEAD": "Achieving Healthcare Efficiency through Accountable Design — a CMS model replacing fee-for-service with fixed hospital budgets.",
  "RVU": "Relative Value Unit — a measure of the resources a service requires. Multiplied by a conversion factor to set rates.",
  "conversion factor": "A dollar amount multiplied by RVUs to set a rate. Medicare's is ~$33; state Medicaid CFs are typically $20–28.",
};

function Term({ children, term }) {
  const key = (term || children).toLowerCase();
  const def = Object.entries(GLOSSARY).find(([k]) => k.toLowerCase() === key)?.[1];
  const [show, setShow] = useState(false);
  const ref = useRef(null);
  const [above, setAbove] = useState(false);
  useEffect(() => { if (show && ref.current) setAbove(ref.current.getBoundingClientRect().top > 240); }, [show]);
  if (!def) return <>{children}</>;
  return (
    <span ref={ref} style={{ position: "relative", display: "inline" }}
      onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}
      onClick={() => setShow(!show)}>
      <span style={{ borderBottom: "1.5px dotted rgba(66,90,112,0.4)", cursor: "help", paddingBottom: 1 }}>{children}</span>
      {show && (
        <span style={{
          position: "absolute", [above ? "bottom" : "top"]: "calc(100% + 8px)",
          left: "50%", transform: "translateX(-50%)",
          background: C.ink, color: "#fff", padding: "12px 16px", borderRadius: 10,
          fontSize: 12, lineHeight: 1.6, width: 290, maxWidth: "85vw",
          boxShadow: "0 8px 30px rgba(0,0,0,.25)", zIndex: 1000, fontFamily: FB, pointerEvents: "none"
        }}>
          <span style={{ fontWeight: 600, color: "#7FD4A0", fontSize: 10, fontFamily: FM, display: "block", marginBottom: 4, letterSpacing: 0.5, textTransform: "uppercase" }}>{children}</span>
          {def}
        </span>
      )}
    </span>
  );
}

// ── Tool registry ───────────────────────────────────────────────────
const TOOLS = [
  { id:"explorer", group:"explore", name:"Spending Explorer",
    tagline:"Search and compare Medicaid spending across every state",
    desc:"227 million rows of claims data processed into a searchable interface. Cross-state rate comparisons, provider concentration, spending trends, and fiscal impact estimates for every HCPCS code.",
    status:"live", icon:"⌕", color:C.brand },
  { id:"explore", group:"explore", name:"Data Explorer",
    tagline:"Filter, group, and export T-MSIS data yourself",
    desc:"Multi-select filters by state, code, and category. Group by any dimension. Bar charts, scatter plots, and full data tables with CSV export.",
    status:"live", icon:"⊞", color:C.brand },
  { id:"wages", group:"analyze", name:"Wage Adequacy",
    tagline:"BLS market wages vs Medicaid reimbursement rates",
    desc:"Compare what Medicaid rates can support against actual BLS wage data for 16 healthcare occupations across every state.",
    status:"live", icon:"⊿", color:C.accent },
  { id:"quality", group:"analyze", name:"Quality Linkage",
    tagline:"CMS Core Set outcomes mapped to payment rates",
    desc:"55 quality measures across 52 jurisdictions. See whether states that pay more get better outcomes.",
    status:"live", icon:"◈", color:C.accent },
  { id:"decay", group:"analyze", name:"Rate Decay",
    tagline:"Track how far state rates have fallen behind Medicare",
    desc:"Every HCPCS code as a percentage of the Medicare PFS. Identify which services have decayed most and which states are furthest behind.",
    status:"live", icon:"◧", color:C.accent },
  { id:"builder", group:"build", name:"Rate Builder",
    tagline:"Calculate Medicaid rates with full audit trails",
    desc:"RBRVS rate calculations using real conversion factors, RVU components, and state-specific rules. Compare methodologies side by side.",
    status:"live", icon:"⬡", color:C.teal },
  { id:"analyst", group:"build", name:"Policy Analyst",
    tagline:"AI-powered rate analysis and SPA language drafting",
    desc:"Ask questions in plain English. Get answers grounded in real data — rates, comparisons, fiscal impact estimates, and draft SPA methodology language.",
    status:"live", icon:"◎", color:C.teal },
  { id:"ahead", group:"build", name:"AHEAD Calculator",
    tagline:"Model hospital global budgets under CMS's AHEAD framework",
    desc:"Project what global budgets would look like under AHEAD parameters. Compare participation scenarios and estimate savings targets.",
    status:"coming", icon:"△", color:C.teal },
  { id:"methods", group:"build", name:"Methodology Library",
    tagline:"How each state sets Medicaid rates, in one place",
    desc:"State-by-state reference: methodology type, conversion factors, base year, last update, SPA numbers.",
    status:"coming", icon:"≡", color:"#5B6E8A" },
];

const NAV_GROUPS = [
  { key:"explore", label:"Explore", tools: TOOLS.filter(t => t.group === "explore") },
  { key:"analyze", label:"Analyze", tools: TOOLS.filter(t => t.group === "analyze") },
  { key:"build", label:"Build", tools: TOOLS.filter(t => t.group === "build") },
];

// ── Nav dropdown ────────────────────────────────────────────────────
function NavDrop({ group, route }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const isActive = group.tools.some(t => route === `/${t.id}`);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  return (
    <div ref={ref} style={{ position: "relative" }}
      onMouseEnter={() => setOpen(true)} onMouseLeave={() => setOpen(false)}>
      <button onClick={() => setOpen(!open)} style={{
        background: isActive ? "rgba(46,107,74,0.06)" : "none",
        border: "none", borderRadius: 6, color: isActive ? C.brand : C.inkLight,
        fontSize: 11, fontFamily: FB, fontWeight: isActive ? 600 : 400,
        cursor: "pointer", padding: "4px 10px",
        display: "flex", alignItems: "center", gap: 4, transition: "all .15s"
      }}>
        {group.label}
        <svg width="8" height="5" viewBox="0 0 8 5" style={{ opacity: 0.4, transition: "transform .15s", transform: open ? "rotate(180deg)" : "none" }}>
          <path d="M1 1l3 3 3-3" stroke="currentColor" strokeWidth="1.2" fill="none"/>
        </svg>
      </button>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 4px)", left: 0, minWidth: 260,
          background: C.white, border: `1px solid ${C.border}`, borderRadius: 10,
          boxShadow: SH_LG, padding: "4px 0", zIndex: 200
        }}>
          {group.tools.map(t => (
            <a key={t.id} href={`#/${t.id}`} onClick={() => setOpen(false)} style={{
              display: "block", padding: "10px 16px", textDecoration: "none", transition: "background .1s",
              background: route === `/${t.id}` ? "rgba(46,107,74,0.04)" : "transparent"
            }}
              onMouseEnter={e => e.currentTarget.style.background = C.surface}
              onMouseLeave={e => e.currentTarget.style.background = route === `/${t.id}` ? "rgba(46,107,74,0.04)" : "transparent"}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 13, width: 24, height: 24, display: "flex", alignItems: "center", justifyContent: "center", borderRadius: 6, background: `${t.color}0D`, color: t.color, flexShrink: 0 }}>{t.icon}</span>
                <div style={{ minWidth: 0 }}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: C.ink, fontFamily: FB }}>{t.name}</div>
                  <div style={{ fontSize: 10, color: C.inkLight, marginTop: 1, fontFamily: FB }}>{t.tagline.substring(0, 55)}</div>
                </div>
                {t.status === "coming" && <span style={{ fontSize: 8, fontFamily: FM, color: C.inkLight, marginLeft: "auto", flexShrink: 0 }}>SOON</span>}
              </div>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Nav search ──────────────────────────────────────────────────────
const SEARCH_MAP = {};
Object.entries(SN).forEach(([abbr, name]) => {
  SEARCH_MAP[name.toLowerCase()] = [{ label: `${name} state profile`, route: `/state/${abbr}` }, { label: `${name} spending data`, route: `/explorer?state=${abbr}` }];
  SEARCH_MAP[abbr.toLowerCase()] = SEARCH_MAP[name.toLowerCase()];
});
Object.assign(SEARCH_MAP, {
  "dental": [{ label: 'Search "dental" — D-codes', route: "/explorer?q=dental" }],
  "office": [{ label: "Office visits — 99211-99215", route: "/explorer?q=office+visits" }],
  "99213": [{ label: "Look up 99213", route: "/explorer?code=99213" }, { label: "Calculate rate for 99213", route: "/builder?code=99213" }],
  "therapy": [{ label: "Therapy codes — 97110+", route: "/explorer?q=therapy" }],
  "mental": [{ label: "Mental health services", route: "/explorer?q=mental+health" }],
});

function NavSearch() {
  const [q, setQ] = useState("");
  const [focused, setFocused] = useState(false);
  const ref = useRef(null);
  const match = Object.entries(SEARCH_MAP).find(([k]) =>
    q.length >= 2 && (k.includes(q.toLowerCase()) || q.toLowerCase().includes(k.substring(0, Math.max(2, q.length))))
  );
  const results = match ? match[1] : [];
  return (
    <div style={{ position: "relative" }} ref={ref}>
      <div style={{
        display: "flex", alignItems: "center", gap: 6,
        background: focused ? C.white : C.surface,
        border: `1px solid ${focused ? C.brand : "transparent"}`,
        borderRadius: 6, padding: "3px 8px", transition: "all .2s ease",
        width: focused || q ? 180 : 28, overflow: "hidden"
      }}>
        <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0, cursor: "pointer" }}
          onClick={() => { setFocused(true); ref.current?.querySelector("input")?.focus(); }}>⌕</span>
        <input value={q} onChange={e => setQ(e.target.value)}
          onFocus={() => setFocused(true)} onBlur={() => setTimeout(() => setFocused(false), 200)}
          placeholder="Search..."
          style={{ border: "none", outline: "none", background: "transparent", fontSize: 11, color: C.ink, fontFamily: FB, width: "100%", opacity: focused || q ? 1 : 0 }} />
      </div>
      {focused && results.length > 0 && (
        <div style={{
          position: "absolute", top: "calc(100% + 6px)", right: 0, width: 260,
          background: C.white, border: `1px solid ${C.border}`, borderRadius: 10,
          boxShadow: SH_LG, padding: "4px 0", zIndex: 200
        }}>
          {results.map((r, i) => (
            <div key={i} style={{ padding: "10px 14px", cursor: "pointer", transition: "background .1s" }}
              onMouseEnter={e => e.currentTarget.style.background = C.surface}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
              <div style={{ fontSize: 12, fontWeight: 500, color: C.ink }}>{r.label}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Platform Nav ────────────────────────────────────────────────────
function PlatformNav({ route }) {
  const activeTool = TOOLS.find(t => route === `/${t.id}`);
  return (
    <nav style={{
      position: "sticky", top: 0, zIndex: 100,
      background: "rgba(250,251,250,0.92)", backdropFilter: "blur(12px)",
      WebkitBackdropFilter: "blur(12px)", borderBottom: `1px solid ${C.border}`
    }}>
      <div style={{
        maxWidth: 1080, margin: "0 auto", padding: "0 20px",
        display: "flex", alignItems: "center", justifyContent: "space-between", height: 48
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <a href="#/" style={{ textDecoration: "none", fontSize: 15, fontWeight: 700, color: C.ink, letterSpacing: -0.3, fontFamily: FB }}>Aradune</a>
          {activeTool && (
            <>
              <span style={{ color: C.border, fontSize: 13 }}>/</span>
              <span style={{ fontSize: 12, color: C.inkLight, fontWeight: 500, fontFamily: FB }}>{activeTool.name}</span>
            </>
          )}
        </div>
        <div style={{ display: "flex", gap: 2, alignItems: "center" }}>
          <NavSearch />
          {route !== "/" && <a href="#/" style={{ fontSize: 11, color: C.inkLight, textDecoration: "none", padding: "4px 10px", borderRadius: 6, fontFamily: FB }}>All Tools</a>}
          {NAV_GROUPS.map(g => <NavDrop key={g.key} group={g} route={route} />)}
          <a href="#/about" style={{ fontSize: 11, fontFamily: FB, color: route === "/about" ? C.brand : C.inkLight, fontWeight: route === "/about" ? 600 : 400, textDecoration: "none", padding: "4px 10px" }}>About</a>
        </div>
      </div>
    </nav>
  );
}

// ═══════════════════════════════════════════════════════════════════
// LANDING PAGE
// ═══════════════════════════════════════════════════════════════════
function Landing() {
  const [startTab, setStartTab] = useState("state");
  const [st, setSt] = useState("");
  const [serviceQ, setServiceQ] = useState("");

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px", overflowX: "hidden", fontFamily: FB }}>

      {/* Hero */}
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
            fontSize: 13, fontWeight: 600, textDecoration: "none"
          }}>Explore Spending Data</a>
          <a href="#/about" style={{
            display: "inline-flex", alignItems: "center", padding: "10px 20px",
            background: "transparent", color: C.inkLight, borderRadius: 8,
            fontSize: 13, fontWeight: 500, textDecoration: "none",
            border: `1px solid ${C.border}`
          }}>About the project</a>
        </div>
      </div>

      {/* Stats row */}
      <div style={{
        display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(130px,1fr))",
        gap: 16, padding: "20px 0 36px", borderTop: `1px solid ${C.border}`
      }}>
        {[["227M+","claims rows"],["54","jurisdictions"],["9,500+","HCPCS codes"],["$1.1T","total spending"]].map(([val,label]) => (
          <div key={label}>
            <div style={{ fontSize: 20, fontWeight: 700, fontFamily: FM, color: C.brand, letterSpacing: -0.5 }}>{val}</div>
            <div style={{ fontSize: 11, color: C.inkLight, marginTop: 2 }}>{label}</div>
          </div>
        ))}
      </div>

      {/* ── COMPACT START HERE — single card with tabs ─────── */}
      <div style={{
        background: C.white, borderRadius: 12, boxShadow: SH,
        padding: "16px 22px 18px", marginBottom: 32,
        borderLeft: `3px solid ${C.brand}`
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 0, marginBottom: 14 }}>
          {[
            { key:"state", label:"Find a state", color: C.brand },
            { key:"service", label:"Search a service", color: C.accent },
            { key:"adequacy", label:"Check adequacy", color: C.teal },
          ].map(tab => (
            <button key={tab.key} onClick={() => setStartTab(tab.key)} style={{
              background: startTab === tab.key ? `${tab.color}0D` : "none",
              border: "none", borderRadius: 6, padding: "5px 14px",
              fontSize: 11, fontWeight: startTab === tab.key ? 600 : 400, fontFamily: FB,
              color: startTab === tab.key ? tab.color : C.inkLight,
              cursor: "pointer", transition: "all .15s"
            }}>{tab.label}</button>
          ))}
        </div>

        {startTab === "state" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>State:</span>
            <select value={st} onChange={e => setSt(e.target.value)} style={{
              flex: 1, maxWidth: 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FB, color: st ? C.ink : C.inkLight,
              background: C.white, outline: "none"
            }}>
              <option value="">Select a state...</option>
              {STATES_LIST.map(s => <option key={s} value={s}>{s} — {SN[s]}</option>)}
            </select>
            <button disabled={!st} style={{
              padding: "8px 16px", borderRadius: 6, border: "none",
              background: st ? C.brand : C.border, color: C.white,
              fontSize: 11, fontWeight: 600, cursor: st ? "pointer" : "default"
            }}>View state profile →</button>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Spending, rates, adequacy, methodology</span>
          </div>
        )}

        {startTab === "service" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>Service:</span>
            <input value={serviceQ} onChange={e => setServiceQ(e.target.value)}
              placeholder="dental, office visits, 99213, therapy..."
              style={{
                flex: 1, maxWidth: 320, padding: "8px 10px", borderRadius: 6, fontSize: 12,
                border: `1px solid ${C.border}`, fontFamily: FB, color: C.ink, outline: "none"
              }} />
            <button disabled={!serviceQ} style={{
              padding: "8px 16px", borderRadius: 6, border: "none",
              background: serviceQ ? C.accent : C.border, color: C.white,
              fontSize: 11, fontWeight: 600, cursor: serviceQ ? "pointer" : "default"
            }}>Search →</button>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Name, category, or <Term>HCPCS</Term> code</span>
          </div>
        )}

        {startTab === "adequacy" && (
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <span style={{ fontSize: 12, color: C.inkLight, flexShrink: 0 }}>State:</span>
            <select value={st} onChange={e => setSt(e.target.value)} style={{
              flex: 1, maxWidth: 280, padding: "8px 10px", borderRadius: 6, fontSize: 12,
              border: `1px solid ${C.border}`, fontFamily: FB, color: st ? C.ink : C.inkLight,
              background: C.white, outline: "none"
            }}>
              <option value="">Select a state...</option>
              {STATES_LIST.map(s => <option key={s} value={s}>{s} — {SN[s]}</option>)}
            </select>
            <button disabled={!st} style={{
              padding: "8px 16px", borderRadius: 6, border: "none",
              background: st ? C.teal : C.border, color: C.white,
              fontSize: 11, fontWeight: 600, cursor: st ? "pointer" : "default"
            }}>Check wages →</button>
            <span style={{ fontSize: 10, color: C.inkLight, marginLeft: 4 }}>Rates vs <Term>BLS</Term> wages and Medicare</span>
          </div>
        )}
      </div>

      {/* ── GROUPED TOOL SECTIONS ─────────────────────────────── */}
      {NAV_GROUPS.map(group => {
        const groupTools = TOOLS.filter(t => t.group === group.key);
        const groupColors = { explore: C.brand, analyze: C.accent, build: C.teal };
        const groupDescs = {
          explore: "Browse and query T-MSIS claims data across every state and service code.",
          analyze: "Three lenses on rate adequacy — workforce viability, health outcomes, and Medicare benchmarking.",
          build: "Calculate rates, estimate fiscal impact, draft SPA language, and model hospital global budgets."
        };
        return (
          <div key={group.key} style={{ paddingBottom: 32 }}>
            <div style={{
              display: "flex", alignItems: "baseline", gap: 12,
              marginBottom: 12, paddingTop: 12, borderTop: `1px solid ${C.border}`
            }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: groupColors[group.key], textTransform: "uppercase", letterSpacing: 1, fontFamily: FM }}>{group.label}</span>
              <span style={{ fontSize: 12, color: C.inkLight }}>{groupDescs[group.key]}</span>
            </div>
            <div style={{
              display: "grid",
              gridTemplateColumns: groupTools.length <= 2 ? "repeat(auto-fill,minmax(min(100%,460px),1fr))" : "repeat(auto-fill,minmax(min(100%,320px),1fr))",
              gap: 12
            }}>
              {groupTools.map(tool => {
                const isLive = tool.status === "live";
                return (
                  <div key={tool.id}
                    onClick={() => { if (isLive) window.location.hash = `/${tool.id}`; }}
                    style={{
                      background: C.white, borderRadius: 12, boxShadow: SH,
                      padding: "20px 22px 18px", borderLeft: `3px solid ${tool.color}`,
                      opacity: isLive ? 1 : 0.75,
                      cursor: isLive ? "pointer" : "default",
                      transition: "box-shadow 0.2s, transform 0.15s"
                    }}
                    onMouseEnter={e => { if(isLive){ e.currentTarget.style.boxShadow = SH_LG; e.currentTarget.style.transform = "translateY(-1px)"; }}}
                    onMouseLeave={e => { e.currentTarget.style.boxShadow = SH; e.currentTarget.style.transform = "translateY(0)"; }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                      <span style={{
                        fontSize: 16, width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center",
                        borderRadius: 8, background: `${tool.color}0D`, color: tool.color, flexShrink: 0
                      }}>{tool.icon}</span>
                      <div style={{ minWidth: 0 }}>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.ink, letterSpacing: -0.2 }}>{tool.name}</div>
                        <div style={{ fontSize: 10, color: C.inkLight, marginTop: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{tool.tagline}</div>
                      </div>
                    </div>
                    <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.6, marginTop: 6 }}>{tool.desc}</div>
                    <div style={{ marginTop: 10 }}>
                      {isLive ? (
                        <span style={{ fontSize: 9, fontWeight: 600, color: tool.color, fontFamily: FM, textTransform: "uppercase", letterSpacing: 0.5 }}>→ Open tool</span>
                      ) : (
                        <span style={{ fontSize: 9, padding: "3px 10px", borderRadius: 10, fontWeight: 600, background: C.surface, color: C.inkLight, fontFamily: FM }}>COMING SOON</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}

      {/* ── AHEAD callout (dark card) ─────────────────────────── */}
      <div style={{
        background: C.ink, borderRadius: 14, padding: "28px 32px",
        marginBottom: 32, position: "relative", overflow: "hidden"
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
            {[
              ["Budget Calculator","Historical revenue → projected HGB"],
              ["Scenario Modeling","Volume shifts, quality, service lines"],
              ["Dual-Payer Analysis","Medicare + Medicaid side by side"],
            ].map(([title, desc]) => (
              <div key={title} style={{ padding: "12px 14px", borderRadius: 8, background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.08)" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: C.white }}>{title}</div>
                <div style={{ fontSize: 10, color: "rgba(255,255,255,0.4)", marginTop: 3 }}>{desc}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── THE WORKFLOW (replaces "Three questions, one dataset") */}
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
          {[
            { num:"1", label:"Explore", q:"What are we spending?",
              desc:"Browse 227M+ claims across 54 jurisdictions. Compare rates by code, category, or state. Export raw data or use guided dashboards.",
              tools:"Spending Explorer · Data Explorer", color:C.brand },
            { num:"2", label:"Analyze", q:"Are rates adequate?",
              desc:"Three lenses: compare against BLS market wages, map quality outcomes to payment levels, and track how far rates have eroded relative to Medicare.",
              tools:"Wage Adequacy · Quality Linkage · Rate Decay", color:C.accent },
            { num:"3", label:"Build", q:"What should we pay?",
              desc:"Calculate defensible rates using real RVUs and conversion factors. Estimate fiscal impact. Draft SPA methodology language. Model hospital global budgets.",
              tools:"Rate Builder · Policy Analyst · AHEAD Calculator", color:C.teal },
          ].map(item => (
            <div key={item.num} style={{
              background: C.white, borderRadius: 12, boxShadow: SH,
              padding: "20px 22px 18px", borderTop: `3px solid ${item.color}`,
              transition: "box-shadow 0.2s, transform 0.15s"
            }}
              onMouseEnter={e => { e.currentTarget.style.boxShadow = SH_LG; e.currentTarget.style.transform = "translateY(-1px)"; }}
              onMouseLeave={e => { e.currentTarget.style.boxShadow = SH; e.currentTarget.style.transform = "translateY(0)"; }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                <span style={{
                  width: 26, height: 26, borderRadius: "50%",
                  background: `${item.color}12`, color: item.color,
                  fontSize: 12, fontWeight: 700, display: "flex", alignItems: "center",
                  justifyContent: "center", fontFamily: FM, flexShrink: 0
                }}>{item.num}</span>
                <div>
                  <div style={{ fontSize: 9, fontWeight: 700, color: item.color, fontFamily: FM, textTransform: "uppercase", letterSpacing: 1 }}>{item.label}</div>
                  <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginTop: 1 }}>{item.q}</div>
                </div>
              </div>
              <div style={{ fontSize: 11, color: C.inkLight, lineHeight: 1.65 }}>{item.desc}</div>
              <div style={{ fontSize: 9, color: C.inkLight, marginTop: 10, fontFamily: FM, letterSpacing: 0.3 }}>{item.tools}</div>
            </div>
          ))}
        </div>
      </div>

      {/* ── WHY + HOW (updated for full platform) ─────────────── */}
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

      {/* ── Data sources ──────────────────────────────────────── */}
      <div style={{
        display: "flex", gap: 16, flexWrap: "wrap", padding: "16px 0 24px",
        borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}`, marginBottom: 32
      }}>
        <span style={{ fontSize: 10, fontFamily: FM, color: C.inkLight, letterSpacing: 0.5, fontWeight: 600 }}>DATA:</span>
        {["CMS T-MSIS","Medicare PFS CY2025","BLS OEWS May 2024","CMS Core Set","NPPES NPI","FMAP FY2025"].map(src => (
          <span key={src} style={{ fontSize: 10, fontFamily: FM, color: C.inkLight, letterSpacing: 0.3 }}>{src}</span>
        ))}
      </div>

      {/* ── Consulting CTA ────────────────────────────────────── */}
      <div style={{
        padding: "28px 32px", background: C.white, borderRadius: 12, boxShadow: SH,
        marginBottom: 40, display: "grid", gridTemplateColumns: "1fr auto", gap: 24, alignItems: "center"
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
          fontSize: 12, fontWeight: 600, textDecoration: "none", whiteSpace: "nowrap", flexShrink: 0
        }}>Get in touch</a>
      </div>
    </div>
  );
}

// ── Main Export ──────────────────────────────────────────────────────
export default function AraduneHybrid() {
  const [route, setRoute] = useState(window.location.hash.slice(1) || "/");
  useEffect(() => {
    const handler = () => setRoute(window.location.hash.slice(1) || "/");
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, []);
  return (
    <div style={{ fontFamily: FB, background: C.bg, minHeight: "100vh", color: C.ink }}>
      <PlatformNav route={route} />
      <Landing />
    </div>
  );
}
