# Aradune — UX Transparency Features Spec
## Implementation guide for Claude Code

These five features collectively shift Aradune from "tool for Medicaid insiders" to "tool anyone can use to understand Medicaid." They share infrastructure (glossary data, synonym map, state data) and should be built in this order since each builds on the previous.

---

## 1. Glossary Tooltip System

**Priority:** Build first — every other feature uses it.

**What it is:** A `<Term>` component that wraps jargon terms with a dotted underline and shows a plain-English definition on hover/tap. Zero API cost. Used everywhere across all tools.

### Component

```tsx
// src/components/Term.tsx
import { useState, useRef, useEffect } from "react";
import { C, FONT } from "../design.js";

const GLOSSARY: Record<string, string> = {
  "HCPCS": "Healthcare Common Procedure Coding System — the code set used to bill for medical services. Each code represents a specific procedure, visit type, or supply.",
  "T-MSIS": "Transformed Medicaid Statistical Information System — the federal database where states report every Medicaid claim. This is where Aradune's rate data comes from.",
  "RVU": "Relative Value Unit — a measure of the resources needed to provide a service. Medicare uses RVUs multiplied by a dollar conversion factor to set rates.",
  "SPA": "State Plan Amendment — the formal document a state submits to CMS when changing its Medicaid reimbursement methodology. CMS must approve it before changes take effect.",
  "FMAP": "Federal Medical Assistance Percentage — the share of Medicaid costs the federal government pays. Ranges from 50% (wealthier states) to ~77% (Mississippi). The state pays the remainder.",
  "CMS": "Centers for Medicare & Medicaid Services — the federal agency that oversees Medicare, Medicaid, and the ACA marketplace.",
  "RBRVS": "Resource-Based Relative Value Scale — the methodology Medicare uses to price physician services. Many states base their Medicaid fee schedules on this system with a different conversion factor.",
  "conversion factor": "A dollar amount multiplied by RVUs to produce a payment rate. Medicare's is ~$33. State Medicaid conversion factors are typically lower, often $20–28.",
  "fee schedule": "A list of set prices a payer will reimburse for each service code. Most states publish a Medicaid fee schedule that providers can look up.",
  "FFS": "Fee-for-service — a payment model where providers bill for each service separately, as opposed to capitated or global budget models.",
  "MCO": "Managed Care Organization — a health plan that contracts with the state to provide Medicaid benefits to enrollees. Most Medicaid enrollees are in managed care.",
  "capitation": "A fixed per-member-per-month payment to an MCO, regardless of how many services enrollees actually use.",
  "encounter data": "Claims data submitted by MCOs showing what services were provided to Medicaid managed care enrollees. Similar to FFS claims but from managed care.",
  "AHEAD": "Achieving Healthcare Efficiency through Accountable Design — a CMS payment model replacing fee-for-service with fixed hospital global budgets.",
  "HGB": "Hospital Global Budget — a fixed annual payment to a hospital regardless of service volume. Used in Maryland and being expanded through the AHEAD model.",
  "HCBS": "Home and Community-Based Services — Medicaid services that help people live at home instead of in institutions. Includes personal care, respite, day programs.",
  "BLS": "Bureau of Labor Statistics — federal agency that publishes wage data. Aradune uses BLS data to compare Medicaid rates against market wages for healthcare workers.",
  "OEWS": "Occupational Employment and Wage Statistics — the BLS survey that provides wage data by occupation and geography. Updated annually.",
  "Core Set": "CMS Medicaid Core Set — standardized quality measures that states report to CMS. Includes metrics like well-child visits, diabetes management, and maternal care.",
  "PC/TC": "Professional Component / Technical Component — a split that separates the physician's work (PC) from the equipment and facility costs (TC) for services like imaging.",
  "modifier": "A two-character code appended to HCPCS codes that changes how a service is paid. Examples: -26 (professional component only), -TC (technical component only), -59 (distinct procedure).",
  "budget neutrality": "A requirement that a methodology change doesn't increase total spending. If you raise rates for some codes, you must lower others to compensate.",
  "claims data": "Records of services billed to and paid by Medicaid. Each claim shows the code, provider, date, amount billed, and amount paid.",
  "per enrollee": "A metric calculated by dividing total spending by the number of people enrolled in Medicaid. Useful for comparing states of different sizes.",
  "Gini coefficient": "A measure of concentration from 0 (perfectly equal) to 1 (one entity has everything). Used here to show how concentrated spending is among providers.",
};

export default function Term({ children, term }: { children: React.ReactNode; term?: string }) {
  const key = (term || (typeof children === "string" ? children : "")).toLowerCase();
  const def = Object.entries(GLOSSARY).find(([k]) => k.toLowerCase() === key)?.[1];
  const [show, setShow] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const [pos, setPos] = useState<"above" | "below">("below");

  useEffect(() => {
    if (show && ref.current) {
      const rect = ref.current.getBoundingClientRect();
      setPos(rect.top > 200 ? "above" : "below");
    }
  }, [show]);

  if (!def) return <>{children}</>;

  return (
    <span ref={ref} style={{ position: "relative", display: "inline" }}
      onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}
      onClick={() => setShow(!show)}>
      <span style={{
        borderBottom: "1.5px dotted rgba(66,90,112,0.4)",
        cursor: "help", transition: "border-color .15s"
      }}>{children}</span>
      {show && (
        <span style={{
          position: "absolute",
          [pos === "above" ? "bottom" : "top"]: "calc(100% + 6px)",
          left: "50%", transform: "translateX(-50%)",
          background: "#0A2540", color: "#fff",
          padding: "10px 14px", borderRadius: 8,
          fontSize: 12, lineHeight: 1.55, fontWeight: 400,
          width: 280, maxWidth: "80vw",
          boxShadow: "0 4px 20px rgba(0,0,0,.2)",
          zIndex: 1000, fontFamily: "'Helvetica Neue',Arial,sans-serif",
          pointerEvents: "none"
        }}>
          <span style={{ fontWeight: 600, color: "#7FD4A0", fontSize: 11,
            fontFamily: "'SF Mono',monospace", display: "block", marginBottom: 4
          }}>{typeof children === "string" ? children : term}</span>
          {def}
        </span>
      )}
    </span>
  );
}
```

### Usage

```tsx
// In any tool or page:
import Term from "../components/Term";

<p>
  Rates are derived from <Term>T-MSIS</Term> claims data and compared against
  the <Term>RBRVS</Term> methodology using Medicare's <Term>conversion factor</Term>.
</p>
```

### Where to apply it

Walk through every tool and wrap first occurrences of glossary terms:
- Landing page: T-MSIS, HCPCS, SPA, fee schedule
- Spending Explorer: claims data, per enrollee, Gini coefficient, FMAP
- Wage Adequacy: BLS, OEWS, HCBS
- Quality Linkage: Core Set, MCO
- Rate Decay: RBRVS, RVU, conversion factor, Medicare PFS
- Rate Builder: RVU, PC/TC, modifier, budget neutrality
- Policy Analyst: SPA, AHEAD, HGB
- Data Explorer: T-MSIS, HCPCS, encounter data

Rule: only annotate the FIRST occurrence per page/tool, not every instance.

---

## 2. Plain-Language Global Search Bar

**Priority:** Build second — the synonym map already exists in TmsisExplorer.

**What it is:** A search input in the nav bar that accepts plain English ("dental in Florida", "office visits", "autism therapy") and routes users to the right tool with filters pre-applied.

### Architecture

```
User types → parse intent → route to tool with query params in hash
```

Extract the SYNONYMS map from TmsisExplorer into a shared module (`src/search.ts`) and extend it with state names and analysis concepts.

### Search parser

```tsx
// src/search.ts

import { SYNONYMS } from "./synonyms"; // extracted from TmsisExplorer
import { STATE_NAMES } from "./states";

interface SearchResult {
  route: string;          // hash route
  params: URLSearchParams;
  label: string;          // human-readable description of where we're going
}

export function parseSearch(query: string): SearchResult[] {
  const q = query.toLowerCase().trim();
  const results: SearchResult[] = [];
  
  // Detect state references
  const stateMatch = Object.entries(STATE_NAMES).find(([abbr, name]) =>
    q.includes(name.toLowerCase()) || q.includes(abbr.toLowerCase())
  );
  const stateAbbr = stateMatch?.[0];
  const stateName = stateMatch?.[1];
  
  // Remove state from query to get the service/code part
  const serviceQuery = stateAbbr
    ? q.replace(stateName?.toLowerCase() || "", "").replace(stateAbbr.toLowerCase(), "").trim()
    : q;

  // Check for direct HCPCS code (5-char alphanumeric)
  const codeMatch = q.match(/\b([0-9]{5}|[A-Z][0-9]{4}|[A-Z]{1}[0-9]{4})\b/i);
  
  // Check for analysis-oriented queries
  const isAdequacy = /adequate|enough|sufficient|underpaid|wage|salary|worker/i.test(q);
  const isQuality = /quality|outcome|measure|performance|core set/i.test(q);
  const isDecay = /decay|erosion|behind|percent of medicare|% of medicare/i.test(q);
  const isCompare = /compare|versus|vs|difference|rank/i.test(q);

  // Route: state profile
  if (stateAbbr && !serviceQuery && !codeMatch) {
    results.push({
      route: `/state/${stateAbbr}`,
      params: new URLSearchParams(),
      label: `${stateName} state profile`
    });
  }

  // Route: specific code lookup
  if (codeMatch) {
    results.push({
      route: "/explorer",
      params: new URLSearchParams({ code: codeMatch[1].toUpperCase(), ...(stateAbbr ? { state: stateAbbr } : {}) }),
      label: `Look up ${codeMatch[1].toUpperCase()}${stateName ? ` in ${stateName}` : ""}`
    });
  }

  // Route: adequacy analysis
  if (isAdequacy) {
    results.push({
      route: "/wages",
      params: new URLSearchParams(stateAbbr ? { state: stateAbbr } : {}),
      label: `Wage adequacy${stateName ? ` in ${stateName}` : ""}`
    });
  }

  // Route: quality
  if (isQuality) {
    results.push({
      route: "/quality",
      params: new URLSearchParams(stateAbbr ? { state: stateAbbr } : {}),
      label: `Quality outcomes${stateName ? ` in ${stateName}` : ""}`
    });
  }

  // Route: decay
  if (isDecay) {
    results.push({
      route: "/decay",
      params: new URLSearchParams(stateAbbr ? { state: stateAbbr } : {}),
      label: `Rate decay${stateName ? ` in ${stateName}` : ""}`
    });
  }

  // Route: synonym-based service search
  if (serviceQuery && !codeMatch) {
    const hasSynonym = Object.keys(SYNONYMS).some(term =>
      serviceQuery.includes(term) || term.includes(serviceQuery)
    );
    if (hasSynonym || serviceQuery.length >= 2) {
      results.push({
        route: "/explorer",
        params: new URLSearchParams({ q: serviceQuery, ...(stateAbbr ? { state: stateAbbr } : {}) }),
        label: `Search "${serviceQuery}"${stateName ? ` in ${stateName}` : ""} in Spending Explorer`
      });
    }
  }

  // Fallback: just search in explorer
  if (results.length === 0) {
    results.push({
      route: "/explorer",
      params: new URLSearchParams({ q }),
      label: `Search "${q}" in Spending Explorer`
    });
  }

  return results;
}
```

### Nav search UI

Add to PlatformNav, right side, before the dropdown groups:

```tsx
// Compact search — expands on focus
const [searchOpen, setSearchOpen] = useState(false);
const [searchQ, setSearchQ] = useState("");
const searchResults = searchQ.length >= 2 ? parseSearch(searchQ) : [];

// Renders as a small magnifying glass icon that expands into an input
// On submit or result click: navigate(result.route + "?" + result.params)
// Dropdown shows parsed results as clickable options
```

Keep it small. The icon sits in the nav at 11px like everything else. Clicking it expands to ~200px input. Results appear in a dropdown below. Enter key goes to the first result. Escape closes.

### Query param handling in tools

Each tool needs to read URL params on mount and pre-apply them:

```tsx
// In TmsisExplorer, on mount:
const params = new URLSearchParams(window.location.hash.split("?")[1] || "");
const initCode = params.get("code");
const initState = params.get("state");
const initQ = params.get("q");
if (initCode) setDetailCode(initCode);
if (initState) setSelectedState(initState);
if (initQ) setSearch(initQ);
```

Same pattern for WageAdequacy, QualityLinkage, RateDecay — read `?state=XX` and pre-select.

---

## 3. Guided Entry Points on Landing Page

**Priority:** Build alongside or after global search — they use the same routing logic.

**What it is:** Replace (or supplement) the "Three questions" section with interactive question cards that have inline inputs and route users directly.

### Design

Add a section ABOVE the tool grid (but below the stats row) that says:

```
QUICK START

What do you want to know?
```

Then three interactive cards:

**Card 1: "How does my state compare?"**
- Inline state picker dropdown (all 51 jurisdictions)
- User picks a state → navigates to `#/state/FL` (state profile) or `#/explorer?state=FL`
- Subtext: "See spending, rates, provider networks, and adequacy"

**Card 2: "What does Medicaid pay for ___?"**
- Inline text input with placeholder "dental, office visits, 99213..."
- Uses synonym map to resolve → navigates to `#/explorer?q=dental`
- Subtext: "Search by service name, category, or HCPCS code"

**Card 3: "Are providers paid enough in ___?"**
- Inline state picker
- Navigates to a combined adequacy view or `#/wages?state=FL`
- Subtext: "Compare rates against market wages and Medicare benchmarks"

### Implementation

```tsx
function QuickStart() {
  const [state1, setState1] = useState("");
  const [serviceQ, setServiceQ] = useState("");
  const [state2, setState2] = useState("");

  return (
    <div style={{ padding: "32px 0", borderTop: `1px solid ${C.border}` }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: C.inkLight,
        textTransform: "uppercase", letterSpacing: 1, marginBottom: 12,
        fontFamily: FM }}>
        Quick start
      </div>
      <div style={{ display: "grid",
        gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))", gap: 14 }}>

        {/* Card 1 */}
        <QuickCard
          question="How does my state compare?"
          sub="Spending, rates, provider networks, and adequacy"
          color={C.brand}>
          <StateSelect value={state1} onChange={setState1}
            onSelect={st => navigate(`/explorer?state=${st}`)} />
        </QuickCard>

        {/* Card 2 */}
        <QuickCard
          question="What does Medicaid pay for ___?"
          sub="Search by service name, category, or code"
          color={C.accent}>
          <SearchInput value={serviceQ} onChange={setServiceQ}
            placeholder="dental, office visits, 99213..."
            onSubmit={q => navigate(`/explorer?q=${encodeURIComponent(q)}`)} />
        </QuickCard>

        {/* Card 3 */}
        <QuickCard
          question="Are providers paid enough?"
          sub="Compare rates against market wages and Medicare"
          color={C.teal}>
          <StateSelect value={state2} onChange={setState2}
            onSelect={st => navigate(`/wages?state=${st}`)} />
        </QuickCard>

      </div>
    </div>
  );
}
```

These cards have the same visual treatment as the existing "Three questions" cards — white bg, top border accent, same border-radius. The only difference is the inline input/dropdown.

---

## 4. State Profile Pages

**Priority:** Build after glossary and search — this is the "pull it all together" feature.

**What it is:** A single route `#/state/:abbr` that combines everything Aradune knows about a state into one page. The most shareable page on the site.

### Route

```tsx
// In Platform.jsx routing:
if (route.startsWith("/state/")) {
  const abbr = route.split("/")[2]?.toUpperCase();
  return <StateProfile state={abbr} />;
}
```

### Page sections

```
┌────────────────────────────────────────────────┐
│ Florida Medicaid Profile                       │
│ Population: 22.6M · Medicaid enrollment: 5.2M  │
│ Total spending: $28.1B · Per enrollee: $5,404   │
│ FMAP: 58.93%                                   │
├────────────────────────────────────────────────┤
│ AT A GLANCE                                    │
│ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐           │
│ │ Rank │ │ Rate │ │ Wage │ │ Qual │           │
│ │ 38th │ │ 62%  │ │ $14/ │ │ 67%  │           │
│ │ spend│ │ mcr  │ │ hr   │ │ rptd │           │
│ └──────┘ └──────┘ └──────┘ └──────┘           │
├────────────────────────────────────────────────┤
│ RATE ADEQUACY SUMMARY                          │
│ [mini bar chart: FL rates as % of Medicare     │
│  for top 10 codes by spending]                 │
│ "Florida pays an average of 62% of Medicare    │
│  across its top 20 codes by volume."           │ ← templated explain
├────────────────────────────────────────────────┤
│ TOP 10 CODES BY SPENDING                       │
│ [table: code, desc, FL rate, natl avg,         │
│  % of medicare, FL rank]                       │
├────────────────────────────────────────────────┤
│ WORKFORCE                                      │
│ "Medicaid rates for home health aides in FL    │
│  support a wage of $13.80/hr. The BLS median   │
│  is $14.15/hr."                                │
│ [mini chart from WageAdequacy data]            │
├────────────────────────────────────────────────┤
│ QUALITY                                        │
│ "FL reports on 67% of Core Set measures.       │
│  Performance is above national median on 12    │
│  of 37 reported measures."                     │
│ [mini chart from QualityLinkage data]          │
├────────────────────────────────────────────────┤
│ METHODOLOGY                                    │
│ How Florida sets rates: RBRVS with state CF    │
│ CF: $24.98 · Base year: CY2025 Medicare PFS   │
│ Last SPA: [number] · Filed: [date]             │
│ (from Methodology Library when available)      │
├────────────────────────────────────────────────┤
│ EXPLORE FURTHER                                │
│ [links to each tool pre-filtered for FL]       │
│ Spending Explorer · Wage Adequacy · Quality ·  │
│ Rate Decay · Rate Builder                      │
└────────────────────────────────────────────────┘
```

### Data sources

All data already exists in the loaded JSON files:
- `states.json` → enrollment, spending, providers
- `hcpcs.json` → rates by state, filter `.r[abbr]` for state-specific
- `bls_wages.json` → wage data by state
- `quality_measures.json` → Core Set measures by state
- `medicare_rates.json` → Medicare rates for % comparison

No new data needed. This is purely a new view over existing data.

### Shareable URLs

`aradune.co/#/state/FL` — bookmarkable, sharable, linkable from testimony docs.
The global search routes "Florida" → this page.
The landing page state picker routes here.

---

## 5. "Explain This" Templated Annotations

**Priority:** Build last — uses patterns from all other features.

**What it is:** A button next to charts and key metrics that generates a plain-English sentence or paragraph explaining what the data shows. No API cost — purely templated from the data.

### Template engine

```tsx
// src/explain.ts

interface ExplainContext {
  state?: string;
  stateName?: string;
  code?: string;
  codeDesc?: string;
  rate?: number;
  medicareRate?: number;
  pctMedicare?: number;
  nationalAvg?: number;
  rank?: number;
  totalStates?: number;
  category?: string;
}

export function explain(template: string, ctx: ExplainContext): string {
  // Simple template: "{{stateName}} pays {{rate}} for {{codeDesc}}"
  return template.replace(/\{\{(\w+)\}\}/g, (_, key) => {
    const val = ctx[key as keyof ExplainContext];
    if (val === undefined) return "—";
    if (typeof val === "number") {
      if (key.includes("pct")) return `${val.toFixed(1)}%`;
      if (key.includes("rate") || key.includes("Rate") || key.includes("Avg")) return `$${val.toFixed(2)}`;
      return val.toLocaleString();
    }
    return String(val);
  });
}

// Pre-built templates for common views:

export const TEMPLATES = {
  stateRateComparison:
    "{{stateName}} pays an average of {{pctMedicare}} of Medicare rates " +
    "across its highest-volume codes, ranking {{rank}} out of {{totalStates}} jurisdictions. " +
    "The national average is {{nationalAvg}} per service.",

  codeStateRate:
    "For {{codeDesc}} ({{code}}), {{stateName}} pays {{rate}} — " +
    "{{pctMedicare}} of the Medicare rate of {{medicareRate}}.",

  wageAdequacy:
    "Medicaid rates for {{category}} in {{stateName}} support an implied wage of " +
    "{{rate}}/hr. The BLS median wage for this occupation is {{medicareRate}}/hr, " +
    "a gap of {{pctMedicare}}.",

  rateDecay:
    "{{stateName}}'s average reimbursement across {{category}} codes is " +
    "{{pctMedicare}} of Medicare — {{rank}} out of {{totalStates}} states.",

  qualitySummary:
    "{{stateName}} reports on {{rank}} of {{totalStates}} Core Set measures. " +
    "Performance is above the national median on {{rate}} reported measures.",
};
```

### UI component

```tsx
function ExplainButton({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  return (
    <div style={{ position: "relative", display: "inline-block" }}>
      <button onClick={() => setShow(!show)} style={{
        background: "none", border: `1px solid ${C.border}`, borderRadius: 4,
        padding: "2px 8px", fontSize: 10, fontFamily: FM, color: C.inkLight,
        cursor: "pointer", display: "flex", alignItems: "center", gap: 4
      }}>
        <span style={{ fontSize: 12 }}>💬</span> Explain
      </button>
      {show && (
        <div style={{
          marginTop: 8, padding: "12px 16px", background: C.surface,
          borderRadius: 8, borderLeft: `3px solid ${C.brand}`,
          fontSize: 13, lineHeight: 1.65, color: C.ink, maxWidth: 480
        }}>
          {text}
        </div>
      )}
    </div>
  );
}
```

### Where to use it

Place next to:
- State comparison bar charts (Spending Explorer dashboard view)
- Rate decay charts (% of Medicare visualization)
- Wage adequacy scatter plots
- Quality linkage charts
- State profile "At a Glance" section
- Any summary metric card

The template populates from the same data already rendered in the chart. No additional data fetching needed.

### Copy button

Include a small "Copy" link inside the explanation so users can paste it directly into testimony, emails, or reports:

```tsx
<button onClick={() => navigator.clipboard.writeText(text)}
  style={{ fontSize: 10, color: C.brand, cursor: "pointer", ... }}>
  Copy text
</button>
```

This is the killer feature for advocates — they can click "Explain," get a ready-to-use sentence with real numbers, and copy it straight into their testimony.

---

## Shared Infrastructure

These features share several modules that should be extracted from TmsisExplorer into shared files:

```
src/
├── components/
│   ├── Term.tsx           # Glossary tooltips
│   ├── ExplainButton.tsx  # Templated explanations
│   ├── StateSelect.tsx    # Reusable state picker dropdown
│   └── SearchInput.tsx    # Plain-language search with autocomplete
├── data/
│   ├── glossary.ts        # GLOSSARY definitions
│   ├── synonyms.ts        # SYNONYMS map (extracted from TmsisExplorer)
│   └── states.ts          # STATE_NAMES, ALL_STATES
├── search.ts              # parseSearch() — global search routing logic
├── explain.ts             # explain() + TEMPLATES
└── design.js              # existing design tokens
```

TmsisExplorer should import SYNONYMS from the shared module instead of defining them inline. This lets the global search and Data Explorer use the same synonym resolution.

---

## Implementation Order

1. **Extract shared modules** — SYNONYMS, STATE_NAMES out of TmsisExplorer into `src/data/`
2. **Build Term component** + glossary data → apply across all tools
3. **Build global search** — parseSearch + nav search UI + query param handling in tools
4. **Build QuickStart section** on landing page — state picker + service search cards
5. **Build StateProfile page** — `#/state/:abbr` route, combines all data
6. **Build ExplainButton** + templates → apply to charts across all tools

Each step is independently shippable. The glossary works without search. Search works without state profiles. State profiles work without explain buttons. Ship incrementally.
