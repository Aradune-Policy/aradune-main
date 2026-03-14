# Research Modules Handoff

> For the other Claude Code terminal. This documents everything the research session built so you can integrate it cleanly after your audit is complete.

## What Was Done

A separate terminal built 10 research modules as standalone files. **Zero existing files were modified.** Everything lives in new `research/` subdirectories. Nothing was committed.

## Files Created

### Backend Routes: `server/routes/research/`

| File | Lines | Endpoints | Purpose |
|------|-------|-----------|---------|
| `__init__.py` | 0 | — | Package init |
| `rate_quality.py` | 164 | 5 | Rate-Quality Nexus: does paying more improve outcomes? |
| `mc_value.py` | 180 | 5 | Managed Care Value: is MC saving money? |
| `treatment_gap.py` | 208 | 4 | Opioid Treatment Gap: prevalence vs treatment capacity |
| `safety_net.py` | 215 | 4 | Safety Net Stress: hospital + nursing + HCBS composite |
| `integrity_risk.py` | 191 | 4 | Integrity Risk Index: Open Payments + LEIE + PERM |
| `fiscal_cliff.py` | 198 | 4 | Fiscal Cliff: spending vs revenue + FMAP impact |
| `maternal_health.py` | 236 | 5 | Maternal Health Deserts: mortality + SVI + HPSA + quality |
| `pharmacy_spread.py` | 192 | 4 | Pharmacy Spread: NADAC vs SDUD overpayment |
| `nursing_ownership.py` | 164 | 5 | Nursing Ownership: for-profit chain quality gap |
| `waiver_impact.py` | 223 | 5 | 1115 Waiver Impact: before/after evaluation |
| `INTEGRATION.py` | 54 | — | Reference: exact imports + router registrations for main.py |

**Total: 2,025 lines, 45 API endpoints**

All routes follow the existing pattern: `router = APIRouter()`, `get_cursor()`, `$1/$2` parameterized DuckDB queries, `{"rows": [...], "count": N}` responses, try/except with HTTPException(500).

Several routes have defensive schema-discovery fallbacks for tables where exact column names were uncertain (fact_mfcu_stats, fact_perm_rates, ref_1115_waivers, fact_pbj_nurse_staffing).

### Frontend Components: `src/tools/research/`

| File | Lines | Tabs | Purpose |
|------|-------|------|---------|
| `RateQualityNexus.tsx` | 678 | 4 | Scatter charts, measure selector, sortable table |
| `ManagedCareValue.tsx` | 681 | 4 | Penetration scatter, MLR bars, quality by tier, trends |
| `TreatmentGap.tsx` | 717 | 4 | Demand-supply gap, MAT utilization, prescribing, funding |
| `SafetyNetStress.tsx` | 467 | 4 | Hospital stress, LTSS pressure, staffing crisis, composite |
| `IntegrityRisk.tsx` | 471 | 4 | Composite index, Open Payments, enforcement, PERM |
| `FiscalCliff.tsx` | 498 | 4 | Spending vs revenue, FMAP impact, budget pressure |
| `MaternalHealth.tsx` | 507 | 4 | Mortality, access barriers, quality gaps, composite |
| `PharmacySpread.tsx` | 533 | 4 | Spread overview, state variation, top drugs, detail |
| `NursingOwnership.tsx` | 584 | 4 | Quality by ownership, chain vs independent, deficiencies |
| `WaiverImpact.tsx` | 666 | 4 | Waiver catalog, enrollment/spending/quality trajectories |
| `INTEGRATION.tsx` | 113 | — | Reference: lazy imports, TOOLS, NAV_GROUP, toolMap |

**Total: 5,915 lines, 40 tabs**

All components follow the exact production pattern from BehavioralHealth.tsx/PharmacyIntelligence.tsx: same design tokens (A/AL/POS/NEG/WARN/SF/BD/WH/cB/FM/SH), same shared components (Card/CH/Met/Pill/SafeTip), same imports (`../../lib/api`, `../../components/LoadingBar`, `../../context/AraduneContext`, `../../design`), same responsive behavior, same ChartActions wrappers, same "Ask Intelligence" footer.

### Documentation: `docs/`

| File | Lines | Purpose |
|------|-------|---------|
| `RESEARCH-MODULES.md` | 510 | Full planning doc: research questions, tables, joins, tabs, copy, sources for all 10 modules |
| `RESEARCH-MODULES-BUILD-SUMMARY.md` | 405 | Build summary: what was built, how, findings, integration guide |
| `RESEARCH-FINDINGS.md` | 480 | Academic research paper with methods, regression tables, robustness checks, limitations |
| `RESEARCH-HANDOFF.md` | this file | Handoff for integration |

### Memory

`~/.claude/projects/-Users-jamestori/memory/project_research_modules.md` — persistent memory indexed in MEMORY.md.

## How to Integrate

### Step 1: Backend (server/main.py)

Add imports near line 6:
```python
from server.routes.research import (
    rate_quality, mc_value, treatment_gap, safety_net,
    integrity_risk, fiscal_cliff, maternal_health,
    pharmacy_spread, nursing_ownership, waiver_impact,
)
```

Add router registrations near line 54:
```python
app.include_router(rate_quality.router)
app.include_router(mc_value.router)
app.include_router(treatment_gap.router)
app.include_router(safety_net.router)
app.include_router(integrity_risk.router)
app.include_router(fiscal_cliff.router)
app.include_router(maternal_health.router)
app.include_router(pharmacy_spread.router)
app.include_router(nursing_ownership.router)
app.include_router(waiver_impact.router)
```

### Step 2: Frontend (Platform.tsx)

Add lazy imports:
```typescript
const RateQualityNexus = lazy(() => import("./tools/research/RateQualityNexus"));
const ManagedCareValue = lazy(() => import("./tools/research/ManagedCareValue"));
const TreatmentGap = lazy(() => import("./tools/research/TreatmentGap"));
const SafetyNetStress = lazy(() => import("./tools/research/SafetyNetStress"));
const IntegrityRisk = lazy(() => import("./tools/research/IntegrityRisk"));
const FiscalCliff = lazy(() => import("./tools/research/FiscalCliff"));
const MaternalHealth = lazy(() => import("./tools/research/MaternalHealth"));
const PharmacySpread = lazy(() => import("./tools/research/PharmacySpread"));
const NursingOwnership = lazy(() => import("./tools/research/NursingOwnership"));
const WaiverImpact = lazy(() => import("./tools/research/WaiverImpact"));
```

Add to TOOLS array (10 entries with `group: "research"`). See `src/tools/research/INTEGRATION.tsx` for the complete array.

Add NAV_GROUP:
```typescript
{ key: "research", label: "Research", tools: TOOLS.filter(t => t.group === "research") }
```

Add to toolMap:
```typescript
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
```

### Step 3: Verify

```bash
# Backend
curl http://localhost:8000/api/research/rate-quality/measures
curl http://localhost:8000/api/research/mc-value/mco-summary
curl http://localhost:8000/api/research/pharmacy-spread/stats
curl http://localhost:8000/api/research/nursing-ownership/quality-by-type

# Frontend
# Navigate to /#/research/rate-quality, /#/research/pharmacy-spread, etc.
```

## Key Findings (For Context)

These modules were built to answer specific research questions. The data was queried and analyzed with OLS, panel fixed effects, and difference-in-differences. Key results:

1. **Rates don't predict quality** after controlling for state wealth (p=0.18). Bivariate r=+0.19 is a confound.
2. **MC saves marginally** (-$16/enrollee/pp, p=0.058) but quality *declines* with MC expansion within states (p=0.002). Simpson's Paradox.
3. **For-profit nursing homes: -0.67 stars** with state FE, p<0.0001, Cohen's d=0.59. Strongest finding.
4. **$2-3B pharmacy overpayment** above NADAC acquisition costs. Survives all robustness checks. Concentrated in low-cost generics (2.75x median markup).
5. **Quality declining nationally** at 1.27pp/year regardless of state characteristics (p=0.006).

Full methods and results: `docs/RESEARCH-FINDINGS.md`

## Landing Page Key Metrics (Below Hero)

James wants a few headline stats near the top of the landing page — things that are uniquely possible because of the cross-domain data lake, not just numbers you can Google. These should feel like "only Aradune can tell you this." Placement: below the hero, above the module grid. Maybe 4-6 metric cards in a row.

**Recommended metrics (sourced from actual query results this session):**

| Metric | Value | Source Tables | Why It's Interesting |
|--------|-------|---------------|---------------------|
| **Pharmacy Overpayment** | $3.4B/year above acquisition cost | `fact_nadac` x `fact_sdud_2025` | Nobody else joins NADAC to SDUD. This number doesn't exist publicly. |
| **MCO Admin/Profit Retention** | $113B/year (8.5% of premiums) | `fact_mco_mlr` | Computed from 2,282 MCO plan-year MLR filings. The total isn't published anywhere. |
| **HCBS Waitlist** | 606,895 people waiting | `fact_hcbs_waitlist` | KFF publishes this but Aradune has it queryable and joinable to enrollment/spending. |
| **Nursing Home Quality Gap** | 0.67-star for-profit penalty | `fact_five_star` (14,710 facilities) | The controlled effect size. Headlines say "for-profit worse" but nobody publishes the regression-adjusted number. |
| **National Quality Trend** | -1.3pp/year decline (2017-2024) | `fact_quality_core_set_combined` | Panel FE across 51 states, 8 years. The direction of Medicaid quality is declining and nobody is reporting the trend line. |
| **Safety Net Stress** | 20 states with compound failure | `fact_hospital_cost` x `fact_five_star` x `fact_hcbs_waitlist` | States where hospitals, nursing homes, AND HCBS are all failing simultaneously. Only possible with cross-domain joins. |

**Alternative/additional options:**
- Total data lake: 700 tables, 400M+ rows, 60+ federal sources (impressive but more about Aradune than about Medicaid)
- Total Medicaid spend: ~$900B/year (CMS-64) — big number but publicly known
- Hospital distress: 42% of hospitals in CA have negative margins — state-specific shock value
- MAT treatment gap: MS has 3.3% OUD prevalence but isn't in top 10 for MAT spending — narrative stat, harder to show as a card
- Worst nursing chain: Reliant Care, 30 facilities, 1.17 stars — specific and damning but maybe too pointed for a landing page

**Suggested implementation:**
- 4 cards in a single row below hero
- Pick the most "only Aradune" metrics: Pharmacy Overpayment ($3.4B), MCO Retention ($113B), Quality Trend (-1.3pp/yr), HCBS Waitlist (607K)
- Each card: big number, one-line label, small source attribution
- Style: match existing Card/Met components, maybe with accent borders
- These could be live-queried or hardcoded initially (hardcoded is fine for launch, live-query is cooler but adds cold-start latency)

**James's note:** This might get struck, but he wants to see it. Build it as a separate component (`src/components/LandingMetrics.tsx` or similar) that can be dropped in or removed easily.

## What NOT to Touch

These files are self-contained. During integration:
- Do NOT modify any files in `server/routes/research/` or `src/tools/research/`
- Only modify `main.py` and `Platform.tsx` (adding imports and registrations)
- The research routes use the same `get_cursor()` and DuckDB connection as all other routes — no new dependencies

## Known Issues

1. Some routes have schema-discovery fallbacks (try a query, if column not found, try alternative). This is defensive but means first calls to those endpoints may be slightly slower.
2. The `fact_perm_rates` and `fact_mfcu_stats` tables have uncertain column names — routes will auto-discover them on first call.
3. The waiver catalog endpoint tries 3 table names in sequence (`ref_1115_waivers` -> `fact_kff_1115_waivers` -> `fact_section_1115_waivers`).
4. Frontend components import from `../../lib/api`, `../../components/LoadingBar`, `../../context/AraduneContext`, `../../design` — these paths are correct for `src/tools/research/` location.
5. No R2 sync needed — these modules query existing lake tables, no new data files.
