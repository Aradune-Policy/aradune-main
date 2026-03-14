# Aradune Ontology & Graph Consistency Report (Prompt 4)

**Audit Date:** 2026-03-13
**Scope:** 16 entity YAMLs, 18 domain YAMLs, 669 db.py entries, 700 on-disk tables

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Entity YAML files | 16 |
| Domain YAML files | 18 |
| db.py registered tables | 668 (unique) |
| On-disk tables | 700 (669 fact + 9 dim + 22 ref) |
| Ontology-referenced tables | 659 (post-fix) |
| Tables invisible to Intelligence | **~23 remaining** (down from 50) |

---

## Issues Fixed (4)

1. **MCO entity disconnected** — `mco.yaml` had empty `fact_tables: []`. Added 8 managed care tables. MCO is now connected to the graph.

2. **27 orphan tables added to domain YAMLs** — Tables that were in db.py and on disk but had no domain coverage (invisible to Intelligence auto-generated prompt). Added across 5 domains: enrollment (+10), expenditure (+5), pharmacy (+4), program_integrity (+3), policy (+5).

3. **db.py duplicate `cms64_historical`** — Appeared twice in FACT_NAMES. Removed duplicate.

4. **Ontology prompt regenerated** — Now covers 659 tables (up from 632). Intelligence system prompt updated with expanded coverage.

---

## Issues Requiring Your Decision (3)

### 1. Five entities share a copy-pasted 107-table blast list
`economic_indicator`, `enrollment_record`, `expenditure_record`, `hcbs_program`, and `rate_cell` all have the EXACT SAME 107-table list. Only ~9 of those 107 are actually relevant to `economic_indicator`, for example. This degrades the ontology's semantic value.

**Options:**
- **A) Prune each list** to only semantically relevant tables (~15-30 per entity). Most accurate but time-intensive.
- **B) Accept as-is** — The blast lists don't cause incorrect behavior, just reduce ontology precision. Intelligence uses domain-level guidance more than entity-level fact_tables.

### 2. No relationship/edge definitions exist
The ONTOLOGY_SPEC.md calls for `relationships:` sections in each entity YAML (e.g., `state has_rates rate_cell via state_code`). None of the 16 entities have this section. This means:
- No DuckPGQ property graph can be generated
- Intelligence cannot do typed relationship traversal
- The ontology is a flat registry, not a graph

**Options:**
- **A) Define relationships** — Add `relationships:` to each entity YAML with join keys. ~2 hours of work. Enables graph traversal and better Intelligence routing.
- **B) Defer** — The flat registry works for current Intelligence needs. Graph traversal is a future capability. Ship now, add edges later.

### 3. ~23 tables still not in any domain YAML
Mostly niche variants (enrollment sub-tables, BLS MSA, promoting interoperability, MIPS performance). These are queryable via `list_tables` but won't appear in Intelligence's auto-generated prompt.

**Options:**
- **A) Add all** to their respective domains
- **B) Accept** — These are low-traffic tables. Intelligence can discover them via `list_tables` if asked.

---

## Graph Health Metrics

| Metric | Value | Status |
|--------|-------|--------|
| Total nodes (entities) | 16 | PASS |
| Orphan nodes (no fact_tables) | 0 (was 1: MCO) | **FIXED** |
| Total edges (relationships) | 0 | **FAIL** (none defined) |
| Entities with >100 fact_tables | 6 | WARNING (blast-list) |
| db.py → disk alignment | 668/668 | PASS |
| Disk → db.py alignment | 699/700 | PASS (1 deprecated) |
| Ontology → disk alignment | 657/659 | PASS (2 naming mismatches) |
| Domain coverage of db.py | 646/668 | WARNING (23 uncovered) |

---

## Validation Stack Status

The audit guide asks about Soda Core v4, dbt-duckdb, Pandera, and datacontract-cli. Status:

| Tool | Referenced In | Actually Running? |
|------|--------------|-------------------|
| Soda Core v4 | CLAUDE.md | **No** — no soda YAML configs found in codebase |
| dbt-duckdb | CLAUDE.md | **No** — no dbt_project.yml or models/ directory |
| Pandera | CLAUDE.md | **No** — not imported by any script |
| datacontract-cli | CLAUDE.md | **No** — no datacontract files found |

**Verdict:** The validation stack described in CLAUDE.md is **aspirational, not implemented**. Data quality is enforced through inline checks in ETL scripts (WHERE clauses, TRY_CAST, NULL filters) and the `tests/` pytest suite (135 tests), but the formal validation tools are not deployed. This should be documented as a known gap.
