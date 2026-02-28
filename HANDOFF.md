# Aradune v0.6.0 — Platform Architecture Handoff

## What Changed (v0.5.1 → v0.6.0)

Aradune is now a **platform** — a suite of free Medicaid policy tools with a shared shell, routing, and landing page. The T-MSIS Explorer (the original dashboard) is now one tool within the ecosystem.

### New Architecture
```
src/
  main.jsx              ← Entry point, renders Platform
  Platform.jsx          ← Shell: landing page, nav, routing, about page
  design.js             ← Shared design system constants
  tools/
    TmsisExplorer.jsx   ← The original dashboard (refactored from App.jsx)
```

### What was done
1. **Platform shell** (`Platform.jsx`): Landing page, sticky nav, hash-based routing, tool directory, about page, consulting CTA
2. **Hash router**: `#/` = landing, `#/explorer` = T-MSIS tool, `#/about` = about, `#/fees` etc. = coming-soon pages
3. **Tool registry**: 5 tools defined (Explorer live, 4 coming soon), each with status, icon, description
4. **Design system** (`design.js`): Extracted shared colors, fonts, shadows
5. **TmsisExplorer refactored**: Removed platform chrome (header branding, footer), kept all tool tabs/logic, simplified About tab with link to platform about
6. **Updated meta**: OG tags, proper title, favicon, SEO description
7. **Vercel config**: SPA fallback routing, data file caching headers

### What was NOT changed
- The T-MSIS Explorer's internal logic, tabs, charts, and data loading are **identical** to v0.5.1
- The R pipeline is **unchanged**
- All JSON data files are compatible as-is

## Routing

| Route | Page |
|-------|------|
| `#/` | Landing page (tool directory, stats, value prop) |
| `#/explorer` | T-MSIS Explorer (the full dashboard) |
| `#/fees` | Fee Schedule Comparator (coming soon placeholder) |
| `#/ahead` | AHEAD Budget Calculator (coming soon placeholder) |
| `#/network` | Network Adequacy Analyzer (coming soon placeholder) |
| `#/impact` | Policy Impact Modeler (coming soon placeholder) |
| `#/about` | About Aradune (origin story, methodology, data sources, consulting) |

## Platform Tools (Registry)

1. **T-MSIS Explorer** — LIVE. Cross-state rate lookup, spending analysis, provider concentration, fiscal impact. The original Aradune dashboard.
2. **Fee Schedule Comparator** — COMING. Compare state fee schedules by methodology. Links R rate-validation work.
3. **AHEAD Budget Calculator** — COMING. Model hospital global budgets under CMS AHEAD parameters.
4. **Network Adequacy Analyzer** — COMING. Provider-to-enrollee ratios by specialty by geography via NPPES.
5. **Policy Impact Modeler** — COMING. Estimate fiscal impact of rate changes against T-MSIS claims volume.

## Build & Deploy

```bash
# Development
cd ~/Desktop/Aradune
npm install
npm run dev

# Pipeline (same as before)
cd data/
Rscript tmsis_pipeline_duckdb.R medicaid-provider-spending.csv

# Deploy
cd ~/Desktop/Aradune
npm run build
npx vercel --prod
```

## Fix: State Government Firewall Blocking

The site is being blocked on state government networks (likely Zscaler/Palo Alto web filtering). Steps:

1. **Domain categorization** (free, do first):
   - Zscaler: https://sitereview.zscaler.com/ → submit aradune.co as "Government/Legal" or "Health/Medicine"
   - Brightcloud/Webroot: https://www.brightcloud.com/tools/url-ip-lookup.php
   - McAfee/Trellix: https://www.trustedsource.org/
   - Fortinet: https://www.fortiguard.com/webfilter
   - Bluecoat/Symantec: https://sitereview.bluecoat.com/

2. **Vercel Pro** ($20/month, optional):
   - Dedicated IP, better SSL configuration
   - Helps with enterprise firewall reputation
   - Worth it if categorization alone doesn't solve it

3. **SSL/Headers** (already configured):
   - vercel.json has proper headers
   - HTTPS enforced by Vercel

## File Layout

```
aradune/
├── index.html              # Entry HTML with OG tags
├── package.json            # v0.6.0
├── vite.config.js          # Vite + React plugin
├── vercel.json             # Routing, caching headers
├── .vercelignore           # Exclude /data, CSVs, R files
├── .gitignore
├── public/
│   ├── favicon.svg
│   └── data/               # Pipeline JSON output (states, hcpcs, trends, etc.)
├── src/
│   ├── main.jsx            # ReactDOM entry
│   ├── Platform.jsx        # Shell, landing page, router, about
│   ├── design.js           # Shared design constants
│   └── tools/
│       └── TmsisExplorer.jsx  # The T-MSIS dashboard tool
└── data/                   # Pipeline scripts + raw data (not deployed)
    └── tmsis_pipeline_duckdb.R
```

## Revenue Strategy

### Free tier (all tools)
- T-MSIS Explorer: full cross-state rate comparison, spending analysis, concentration
- Fee Schedule Comparator: compare published state fee schedules
- AHEAD Calculator: basic global budget modeling with default assumptions
- Network Adequacy: provider density maps and ratio rankings
- Policy Impact: simple rate change scenarios

### Consulting (the real revenue)
- Custom rate studies (what Mercer/Milliman charge $200-500K for)
- AHEAD modeling with state-specific hospital data and custom assumptions
- SPA fiscal impact analysis
- Methodology design and documentation
- Pitch: "You've been using our free tools. We do the same work the big firms do, at a fraction of the cost, with full transparency."

### Optional Pro tier ($99-199/month)
- Bulk CSV export
- API access for integration with state systems
- Custom AHEAD scenarios (compute-heavy)
- Priced to be expensable without procurement

## Next Steps

### Immediate
- [ ] Submit domain for categorization with web filter vendors
- [ ] Deploy v0.6.0 to Vercel
- [ ] Write 2-3 research posts for the blog section (Phase 2)
- [ ] Consider Vercel Pro if categorization doesn't fix blocking

### Phase 2: Blog/Research
- Add markdown renderer to Platform.jsx
- Write: T-MSIS methodology explainer, cross-state rate analysis, AHEAD overview
- Helps with SEO and domain reputation

### Phase 3: Fee Schedule Comparator
- Start with FL fee schedule + 5-10 other states
- Build comparison interface as new tool module

### Phase 4: AHEAD Calculator
- Basic calculator with default CMS assumptions
- Input: state hospital spending, participating hospitals, target growth rate
- Output: projected global budgets over 5 years

## Known Policy Rules (carried forward)
- FL Medicaid: rates cannot have both a facility rate AND a PC/TC split. Flagged codes: 46924, 91124, 91125.
- CHIP excluded from per-enrollee calculations
- CMS enrollment data: Nov 2024, Medicaid only
- FMAP: FY2025 rates
