# Aradune: Medicaid Intelligence Platform
## Architecture Overview

### Three-Layer Architecture

**Layer 1: The Data Lake**
750+ tables | 400M+ rows | 20 domains | 90+ federal/state sources
DuckDB over Hive-partitioned Parquet | Official fee schedules for all 54 jurisdictions
Sources: CMS, HRSA, CDC, SAMHSA, BLS, BEA, Census, FDA, HUD, USDA, state Medicaid agencies

**Layer 2: The Ontology**
16 entity types | 28 relationship edges | 19 named metrics
YAML-defined, auto-generates Intelligence system prompt + DuckPGQ property graph
Add a dataset = add a YAML file + run a script

**Layer 3: Intelligence**
Claude-powered analytical engine with direct SQL access to the full lake
Tiered routing (Haiku/Sonnet/Opus) | RAG over 1,039 CMS policy documents
Self-improving Skillbook (24+ domain rules, auto-learns from every query)
Natural language in, compliance-ready analysis out

### Key Capabilities

| Capability | Status | Regulatory Driver |
|-----------|--------|-------------------|
| CPRA Compliance Generator | Production | 42 CFR 447.203, July 2026 |
| Cross-State Rate Comparison (54 jurisdictions) | Production | Ensuring Access Rule |
| Rate Explorer (any code, all states) | Production | Rate transparency |
| Caseload & Expenditure Forecasting | Production | SARIMAX + ETS |
| Fiscal Impact Modeling | Production | OBBBA, SDP caps |
| Hospital AHEAD Readiness | Production | CMS AHEAD model |
| 13 Research Briefs (OLS, panel FE, DiD, PSM, RF) | Production | Academic methods |
| T-MSIS Claims Calibration | Production | Rate validation |
| Self-Corrective Intelligence (Skillbook) | Production | Accuracy improvement |
| Network Adequacy Scoring | Production | Access standards |
| Data Validation Framework | Production | Data quality |

### Infrastructure

| Component | Technology | Deployment |
|-----------|-----------|------------|
| Frontend | React 18 + TypeScript + Vite | Vercel |
| Backend | Python FastAPI + DuckDB | Fly.io (10GB persistent volume) |
| Storage | Hive-partitioned Parquet | Cloudflare R2 (890+ files) |
| Auth | Clerk (JWT) | Clerk.com |
| AI | Claude API (Haiku/Sonnet/Opus) | Anthropic |
| CI/CD | GitHub Actions | Auto-deploy on push |
| Search | BM25 FTS over 1,039 CMS docs | DuckDB |

### Data Sensitivity

Currently Ring 0/0.5/1 only (public regulatory, economic, aggregated).
No PHI. No BAA required. SOC 2 Type II on roadmap for Ring 2+ (state-uploaded data).

### Replication Cost Estimate

| Component | Time | Cost |
|-----------|------|------|
| Data lake assembly (90+ sources, 115+ ETL scripts) | 12-24 months | $500K-$1M |
| Fee schedule scraping (54 jurisdictions, 17 new this session) | 3-6 months | $100-200K |
| Domain logic + ontology + Skillbook | 6-12 months | $200-400K |
| Intelligence engine + RAG + research modules | 2-4 months | $100-200K |
| Regulatory expertise (encoded rules, caveats, audit findings) | Ongoing | Irreplaceable |

### Contact

aradune-medicaid@proton.me | aradune.co
