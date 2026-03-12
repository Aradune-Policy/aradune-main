# ONTOLOGY_SPEC.md — Aradune Entity Registry Build Specification
> Hand this document to a Claude Code session alongside CLAUDE.md and the codebase.
> It will generate the complete entity registry, DuckPGQ graph definition, and auto-generated system prompt.
> Written: 2026-03-11

---

## What This Session Produces

By the end of this session, the following artifacts exist in the repo:

```
Aradune/
├── ontology/
│   ├── README.md                    ← What the entity registry is and how to extend it
│   ├── schema.yaml                  ← Master schema definition (entity types, conventions)
│   ├── entities/
│   │   ├── state.yaml
│   │   ├── procedure.yaml
│   │   ├── provider.yaml
│   │   ├── hospital.yaml
│   │   ├── mco.yaml
│   │   ├── rate_cell.yaml
│   │   ├── enrollment.yaml
│   │   ├── quality_measure.yaml
│   │   ├── policy_document.yaml
│   │   ├── geographic_area.yaml
│   │   ├── drug.yaml
│   │   ├── nursing_facility.yaml
│   │   ├── workforce.yaml
│   │   ├── hcbs_program.yaml
│   │   ├── expenditure.yaml
│   │   └── economic_context.yaml
│   ├── domains/
│   │   ├── rates.yaml               ← Domain grouping: which entities/tables belong to "Rates"
│   │   ├── enrollment.yaml
│   │   ├── hospitals.yaml
│   │   ├── quality.yaml
│   │   ├── workforce.yaml
│   │   ├── pharmacy.yaml
│   │   ├── behavioral_health.yaml
│   │   ├── ltss_hcbs.yaml
│   │   ├── expenditure.yaml
│   │   ├── economic.yaml
│   │   ├── medicare.yaml
│   │   ├── policy.yaml
│   │   └── public_health.yaml
│   └── metrics/
│       ├── rate_metrics.yaml         ← Named, deterministic metric definitions
│       ├── enrollment_metrics.yaml
│       ├── fiscal_metrics.yaml
│       ├── quality_metrics.yaml
│       └── access_metrics.yaml
│
├── scripts/
│   ├── generate_ontology.py          ← Reads YAML → generates system prompt + DuckPGQ SQL
│   ├── introspect_lake.py            ← Connects to DuckDB, introspects all tables, outputs raw inventory
│   └── validate_ontology.py          ← Validates YAML files against schema, checks for broken references
│
├── server/
│   ├── ontology/
│   │   ├── __init__.py
│   │   ├── registry.py               ← Python module that loads YAML at startup, provides lookup API
│   │   └── prompt_generator.py       ← Generates Intelligence system prompt section from registry
│   └── routes/
│       └── intelligence.py           ← Updated to use auto-generated prompt section
│
└── sql/
    └── property_graph.sql            ← Generated CREATE PROPERTY GRAPH statement for DuckPGQ
```

---

## Step 1: Introspect the Data Lake

### Script: `scripts/introspect_lake.py`

Connect to DuckDB with the lake loaded. For every registered view in `db.py`:

```python
import duckdb
import json
import yaml

def introspect_lake(db_path="data/lake"):
    """
    Introspect all registered DuckDB views.
    Output: JSON file with table name, columns (name, type), row count, sample values.
    """
    con = duckdb.connect()

    # Load all parquet views (same logic as db.py)
    # For each registered table:
    inventory = {}
    for table_name in registered_tables:
        try:
            schema = con.execute(f"DESCRIBE {table_name}").fetchall()
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            # Sample 5 rows for each column to detect patterns
            sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()

            inventory[table_name] = {
                "columns": [
                    {"name": col[0], "type": col[1], "nullable": col[2] == "YES"}
                    for col in schema
                ],
                "row_count": row_count,
                "sample": sample.to_dict(orient="records"),
            }
        except Exception as e:
            inventory[table_name] = {"error": str(e)}

    with open("ontology/raw_inventory.json", "w") as f:
        json.dump(inventory, f, indent=2, default=str)

    return inventory
```

**Output:** `ontology/raw_inventory.json` — the raw introspection of every table. This is the input for entity inference.

---

## Step 2: Infer Entities and Relationships

### Logic for Claude Code

Read the raw inventory. Apply these inference rules:

**Entity detection:**
- Every `dim_*` table IS an entity's canonical table
- `dim_state` → State entity (key: `state_code`)
- `dim_procedure` → Procedure entity (key: `cpt_hcpcs_code`)
- `dim_medicare_locality` → MedicareLocality entity (key: `locality_code`)
- Additional entities inferred from fact table clustering:
  - Tables with `provider_ccn` or `npi` columns → Provider/Hospital entity
  - Tables with `plan_id` or `mco_name` columns → MCO entity
  - Tables with `ndc` columns → Drug entity
  - Tables with `measure_id` or `measure_name` columns → QualityMeasure entity

**Relationship detection (join key patterns):**

| Column pattern | Links to entity | Join key |
|---------------|----------------|----------|
| `state_code` | State | `state_code` |
| `cpt_hcpcs_code`, `procedure_code`, `hcpcs_code` | Procedure | normalize to `cpt_hcpcs_code` |
| `provider_ccn`, `ccn` | Hospital | `provider_ccn` |
| `npi`, `billing_npi`, `srvc_prvdr_npi` | Provider | NPI |
| `locality_code` | MedicareLocality | `locality_code` |
| `ndc`, `ndc_code` | Drug | `ndc` |
| `plan_id` | MCO | `plan_id` |
| `fips_code`, `county_fips` | GeographicArea | FIPS |
| `soc_code` | Occupation (workforce) | `soc_code` |

**Ambiguity rules (SURFACE THESE TO THE USER):**

When a fact table has multiple entity joins, ALL are recorded but tagged:
- `primary: true` — the main entity this table describes
- `primary: false` — a secondary linkage

Example: `fact_rate_comparison` has `state_code` AND `cpt_hcpcs_code` AND `category_447`. The primary entity is "a rate comparison between a State's Medicaid rate and Medicare for a Procedure." All three are relationships, but the table's identity is the rate comparison itself.

**Claude Code should ask the user when:**
1. A table has no detectable join key to any known entity (orphan table)
2. A table could belong to multiple domains (e.g., `fact_medicaid_opioid_prescribing` — pharmacy or behavioral health?)
3. A column name is ambiguous (e.g., `rate` — is this a Medicaid rate, a Medicare rate, a wage rate?)
4. Two tables appear to contain overlapping data (e.g., `fact_mc_enrollment` vs `fact_mc_enrollment_summary`)

---

## Step 3: YAML Schema Definition

### Master schema: `ontology/schema.yaml`

```yaml
# Schema version — increment when breaking changes are made
version: "1.0"

# Valid entity types
entity_types:
  - state
  - procedure
  - provider
  - hospital
  - nursing_facility
  - mco
  - rate_cell
  - drug
  - quality_measure
  - policy_document
  - geographic_area
  - occupation
  - hcbs_program
  - enrollment_record
  - expenditure_record
  - economic_indicator

# Valid relationship cardinalities
cardinalities:
  - one_to_one
  - one_to_many
  - many_to_many

# Valid data quality tiers (aligned with COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md)
quality_tiers:
  - verified      # Cross-validated against external source (e.g., CMS-64 totals)
  - standard      # Standard ingestion pipeline, Soda Core + dbt-expectations passing
  - provisional   # Ingested but known quality issues (see per-dataset notes)
  - raw           # Minimal processing, use with caution
  - dq_conditional # Quality varies by state/year — requires DQ Atlas lookup

# Valid metric aggregation types
aggregation_types:
  - sum
  - avg
  - median
  - count
  - count_distinct
  - min
  - max
  - weighted_avg
  - pct_of_total
  - ratio
```

### Entity file schema: `ontology/entities/{entity}.yaml`

```yaml
# Example: ontology/entities/state.yaml
entity: state
display_name: "State Medicaid Program"
description: "A US state or territory's Medicaid program"

# Canonical dimension table
canonical_table: dim_state
primary_key: state_code
key_type: VARCHAR(2)

# Properties from the canonical table
properties:
  - name: state_name
    type: VARCHAR
    description: "Full state name"
  - name: region
    type: VARCHAR
    description: "Census region"
  - name: expansion_status
    type: BOOLEAN
    description: "Whether state expanded Medicaid under ACA"
  - name: fmap
    type: DECIMAL(5,4)
    description: "Federal Medical Assistance Percentage"
  - name: managed_care_model
    type: VARCHAR
    description: "Primary managed care delivery model"

# Relationships to other entities via fact tables
relationships:
  - name: has_rates
    target_entity: rate_cell
    via_table: fact_medicaid_rate
    join_key: state_code
    cardinality: one_to_many
    description: "Medicaid fee schedule rates for this state"

  - name: has_rate_comparisons
    target_entity: procedure
    via_table: fact_rate_comparison
    join_key: state_code
    cardinality: one_to_many
    description: "Medicaid vs Medicare rate comparisons"

  - name: has_enrollment
    target_entity: enrollment_record
    via_table: fact_enrollment
    join_key: state_code
    cardinality: one_to_many
    description: "Monthly enrollment totals"

  - name: has_hospitals
    target_entity: hospital
    via_table: fact_hospital_cost
    join_key: state_code
    cardinality: one_to_many
    description: "HCRIS hospital cost reports in this state"

  - name: has_quality_measures
    target_entity: quality_measure
    via_table: fact_quality_core_set_2024
    join_key: state_code
    cardinality: one_to_many
    description: "Core Set quality measure performance"

  - name: has_workforce
    target_entity: occupation
    via_table: fact_bls_wage
    join_key: state_code
    cardinality: one_to_many
    description: "Healthcare workforce wages"

  - name: has_expenditure
    target_entity: expenditure_record
    via_table: fact_expenditure
    join_key: state_code
    cardinality: one_to_many
    description: "CMS-64 expenditure by category"

  - name: has_hcbs_waitlist
    target_entity: hcbs_program
    via_table: fact_hcbs_waitlist
    join_key: state_code
    cardinality: one_to_many
    description: "HCBS waiting list data"

  - name: has_economic_context
    target_entity: economic_indicator
    via_table: fact_unemployment
    join_key: state_code
    cardinality: one_to_many
    description: "Economic indicators (unemployment, poverty, income)"

# All fact tables that reference this entity
fact_tables:
  - fact_medicaid_rate
  - fact_rate_comparison
  - fact_enrollment
  - fact_claims
  - fact_expenditure
  - fact_hospital_cost
  - fact_quality_measure
  - fact_quality_core_set_2024
  - fact_bls_wage
  - fact_drug_utilization
  - fact_hcbs_waitlist
  - fact_unwinding
  - fact_dsh_payment
  - fact_mc_enrollment_summary
  - fact_nsduh_prevalence
  - fact_acs_state
  - fact_unemployment
  - fact_scorecard
  # ... (Claude Code: list ALL fact tables containing state_code)
```

### Metric file schema: `ontology/metrics/{domain}_metrics.yaml`

```yaml
# Example: ontology/metrics/rate_metrics.yaml
domain: rates
metrics:
  - name: pct_of_medicare
    display_name: "Medicaid as % of Medicare"
    description: "Medicaid FFS rate divided by Medicare non-facility rate"
    formula: "medicaid_rate / medicare_nonfac_rate"
    source_table: fact_rate_comparison
    aggregation: avg
    unit: percentage
    range: [0, 5.0]  # Valid range (0% to 500% of Medicare)
    caveats:
      - "Uses non-facility Medicare rate per 42 CFR 447.203"
      - "Base rates only — excludes supplemental payments"
      - "State-level average; locality variation exists"
    quality_tier: standard

  - name: cpra_pct_of_medicare
    display_name: "CPRA Medicaid-to-Medicare Ratio"
    description: "Official CPRA calculation per 42 CFR 447.203"
    formula: "SUM(medicaid_rate * claim_count) / SUM(medicare_nonfac_rate * claim_count)"
    source_table: fact_rate_comparison
    aggregation: weighted_avg
    weight_column: claim_count
    unit: percentage
    filters:
      - "category_447 IN ('Primary Care', 'OB/GYN', 'MH/SUD')"
      - "Only 68 codes from CMS CY 2025 E/M Code List"
    conversion_factor: 32.3465  # CY2025 non-QPP
    compliance_rule: "42 CFR 447.203"
    deadline: "2026-07-01"
    quality_tier: verified

  - name: rate_decay_index
    display_name: "Rate Decay Index"
    description: "How far Medicaid rates have fallen behind Medicare over time"
    formula: "current_pct_of_medicare / baseline_pct_of_medicare"
    source_table: fact_rate_comparison
    aggregation: avg
    unit: ratio
    quality_tier: standard

  - name: implied_conversion_factor
    display_name: "Implied Conversion Factor"
    description: "Reverse-engineered state CF from Medicaid rates and RVUs"
    formula: "medicaid_rate / total_rvu"
    source_tables: [fact_medicaid_rate, dim_procedure]
    join: "cpt_hcpcs_code"
    aggregation: median
    unit: currency
    quality_tier: standard
    caveats:
      - "Only valid for RBRVS-based states"
      - "Excludes codes with $0 rates or missing RVUs"
```

### Domain file schema: `ontology/domains/{domain}.yaml`

```yaml
# Example: ontology/domains/rates.yaml
domain: rates
display_name: "Rates & Fee Schedules"
description: "Medicaid provider payment rates, Medicare benchmarks, and rate comparisons"

entities:
  - rate_cell
  - procedure

primary_tables:
  - table: fact_medicaid_rate
    description: "Medicaid fee schedule rates by state, code, modifier"
    row_count: 597000
    coverage: "47 states"
    vintage: "Current as of last state update"
    quality_tier: standard
    known_issues:
      - "4 states outstanding (KS, NJ portal login; TN MC only; WI manual)"
      - "FL: no facility + PC/TC split (codes 46924, 91124, 91125)"

  - table: fact_rate_comparison
    description: "Medicaid vs Medicare rate comparison with pct_of_medicare"
    row_count: 302000
    coverage: "45 states"
    vintage: "CY2022 T-MSIS + CY2025 Medicare PFS"
    quality_tier: dq_conditional
    known_issues:
      - "T-MSIS quality varies by state — check DQ Atlas before using any state"
      - "MC encounter amounts unreliable — FFS analysis path only"
      - "Illinois requires custom claim dedup logic"
      - "See COMPLETE-DATA-REFERENCE Part 1: T-MSIS section"

  - table: dim_procedure
    description: "HCPCS/CPT codes with RVUs, descriptions, status indicators"
    row_count: 16978
    coverage: "All codes"
    vintage: "CY2026 Medicare PFS"
    quality_tier: verified
    known_issues:
      - "CY2026 introduced -2.5% efficiency adjustment — breaks YoY comparability"
      - "Separate CFs for QPP vs non-QPP for first time"
      - "See COMPLETE-DATA-REFERENCE Part 1: RBRVS section"

supporting_tables:
  - fact_medicare_rate
  - fact_medicare_rate_state
  - fact_dq_flag

metrics:
  - pct_of_medicare
  - cpra_pct_of_medicare
  - rate_decay_index
  - implied_conversion_factor

# Per-domain data quality notes surfaced to Intelligence
data_quality_notes: |
  CRITICAL: T-MSIS-derived rate comparisons require DQ Atlas check per state/year.
  Illinois claims use incremental credit/debit adjustments — standard TAF final-action
  algorithm fails. Use FFS analysis path only (CLM_TYPE_CD separation).
  MC encounter payment amounts are frequently $0 or redacted.
  Always specify data vintage in output.
  For full dataset-specific quality rules, see COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md.

intelligence_context: |
  When answering rate questions, always specify:
  - Which states are included and which are missing
  - The data vintage (T-MSIS service year, Medicare PFS year)
  - Whether the comparison uses facility or non-facility Medicare rates
  - Whether supplemental payments are included (they should NOT be for CPRA)
  FL-specific: rates cannot have both facility rate AND PC/TC split (codes 46924, 91124, 91125)
```

---

## Step 4: Generate DuckPGQ Property Graph

### Script: `scripts/generate_ontology.py` (DuckPGQ section)

Read all entity YAML files. Generate the `CREATE PROPERTY GRAPH` statement:

```python
def generate_duckpgq(entities: dict, output_path: str):
    """
    Generate CREATE PROPERTY GRAPH SQL from entity registry.

    Vertex tables = canonical tables from each entity
    Edge tables = fact tables that connect two or more entities
    """
    vertex_tables = []
    edge_tables = []

    for entity_name, entity in entities.items():
        # Each entity's canonical table is a vertex
        vertex_tables.append(entity["canonical_table"])

        # Each relationship creates an edge
        for rel in entity.get("relationships", []):
            edge_tables.append({
                "table": rel["via_table"],
                "source_key": rel["join_key"],
                "source_ref": entity["canonical_table"],
                "source_pk": entity["primary_key"],
                "dest_entity": rel["target_entity"],
                "label": rel["name"],
            })

    # Deduplicate vertex tables
    vertex_tables = list(set(vertex_tables))

    # Generate SQL
    sql = "INSTALL duckpgq FROM community;\nLOAD duckpgq;\n\n"
    sql += "CREATE OR REPLACE PROPERTY GRAPH medicaid\n"
    sql += "VERTEX TABLES (\n"
    sql += ",\n".join(f"    {vt}" for vt in sorted(vertex_tables))
    sql += "\n)\nEDGE TABLES (\n"

    edge_lines = []
    seen_edges = set()
    for edge in edge_tables:
        edge_key = (edge["table"], edge["source_key"], edge["label"])
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        # Look up destination entity's canonical table and PK
        dest = entities[edge["dest_entity"]]
        line = (
            f"    {edge['table']}\n"
            f"        SOURCE KEY ({edge['source_key']}) "
            f"REFERENCES {edge['source_ref']} ({edge['source_pk']})\n"
            f"        DESTINATION KEY ({dest['primary_key']}) "
            f"REFERENCES {dest['canonical_table']} ({dest['primary_key']})\n"
            f"        LABEL {edge['label']}"
        )
        edge_lines.append(line)

    sql += ",\n".join(edge_lines)
    sql += "\n);\n"

    with open(output_path, "w") as f:
        f.write(sql)

    return sql
```

**DECISION POINT FOR USER:** DuckPGQ edge tables require that source and destination keys reference columns in the vertex tables. Some fact tables join on keys that don't match a vertex table's primary key (e.g., `fact_rate_comparison` joins to procedures via `cpt_hcpcs_code` but the table's own grain includes `state_code + cpt_hcpcs_code + category_447 + year`). Claude Code should flag these and ask whether to:
- (a) Create the edge using only the matching key columns (loses some granularity)
- (b) Skip this edge in DuckPGQ and rely on standard SQL for this relationship
- (c) Create an intermediate vertex table (adds complexity)

---

## Step 5: Generate Intelligence System Prompt Section

### Script: `scripts/generate_ontology.py` (prompt section)

```python
def generate_system_prompt_section(entities: dict, domains: dict, metrics: dict) -> str:
    """
    Generate the "What you have access to" section of Intelligence's system prompt.
    Organized by domain, with entity relationships, named metrics, and data quality notes.
    """
    prompt = "## The Aradune Data Lake\n\n"

    for domain_name, domain in sorted(domains.items()):
        prompt += f"### {domain['display_name']}\n"
        prompt += f"{domain['description']}\n\n"

        for table_info in domain["primary_tables"]:
            prompt += (
                f"- **{table_info['table']}**: {table_info['description']} "
                f"({table_info.get('row_count', '?')} rows, {table_info.get('coverage', '?')})"
            )
            # Surface quality tier if not standard
            tier = table_info.get("quality_tier", "standard")
            if tier != "standard":
                prompt += f" [quality: {tier}]"
            prompt += "\n"

            # Surface known issues inline
            for issue in table_info.get("known_issues", []):
                prompt += f"  ⚠ {issue}\n"

        if domain.get("supporting_tables"):
            prompt += f"- Supporting: {', '.join(domain['supporting_tables'])}\n"

        # Named metrics for this domain
        domain_metrics = metrics.get(domain_name, {}).get("metrics", [])
        if domain_metrics:
            prompt += "\n**Named metrics (use these for consistency):**\n"
            for m in domain_metrics:
                prompt += f"- `{m['name']}`: {m['description']}"
                if m.get("formula"):
                    prompt += f" — `{m['formula']}`"
                prompt += "\n"

        # Domain-specific data quality notes
        if domain.get("data_quality_notes"):
            prompt += f"\n**Data quality:** {domain['data_quality_notes']}\n"

        # Domain-specific intelligence context
        if domain.get("intelligence_context"):
            prompt += f"\n{domain['intelligence_context']}\n"

        prompt += "\n"

    # Entity relationships section
    prompt += "## How entities connect\n\n"
    for entity_name, entity in sorted(entities.items()):
        if entity.get("relationships"):
            prompt += f"**{entity.get('display_name', entity_name)}** ({entity['canonical_table']}, key: {entity['primary_key']}):\n"
            for rel in entity["relationships"]:
                prompt += f"- {rel['name']} → {rel['target_entity']} via {rel['via_table']}\n"
            prompt += "\n"

    return prompt
```

The generated prompt replaces the hand-maintained "What you have access to" section in intelligence.py. The rest of the system prompt (rules, response format, behavioral instructions) stays hand-written.

---

## Step 6: Python Registry Module

### `server/ontology/registry.py`

Loaded at server startup. Provides fast lookup for Intelligence and API routes.

```python
import yaml
from pathlib import Path
from functools import lru_cache

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"

@lru_cache(maxsize=1)
def load_entities() -> dict:
    entities = {}
    for f in (ONTOLOGY_DIR / "entities").glob("*.yaml"):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            entities[data["entity"]] = data
    return entities

@lru_cache(maxsize=1)
def load_domains() -> dict:
    domains = {}
    for f in (ONTOLOGY_DIR / "domains").glob("*.yaml"):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            domains[data["domain"]] = data
    return domains

@lru_cache(maxsize=1)
def load_metrics() -> dict:
    metrics = {}
    for f in (ONTOLOGY_DIR / "metrics").glob("*.yaml"):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            metrics[data["domain"]] = data
    return metrics

def get_tables_for_entity(entity_name: str) -> list[str]:
    """All fact tables that reference this entity."""
    entity = load_entities().get(entity_name)
    return entity.get("fact_tables", []) if entity else []

def get_join_key(entity_name: str) -> str:
    """Primary key for joining to this entity."""
    entity = load_entities().get(entity_name)
    return entity.get("primary_key", "") if entity else ""

def get_metric_definition(metric_name: str) -> dict | None:
    """Look up a named metric's formula, source, and caveats."""
    for domain_metrics in load_metrics().values():
        for m in domain_metrics.get("metrics", []):
            if m["name"] == metric_name:
                return m
    return None

def get_related_tables(table_name: str) -> list[dict]:
    """Given a table, find all entities it connects to and their join keys."""
    results = []
    for entity_name, entity in load_entities().items():
        if table_name in entity.get("fact_tables", []):
            results.append({
                "entity": entity_name,
                "join_key": entity["primary_key"],
                "canonical_table": entity["canonical_table"],
            })
    return results
```

### `server/ontology/prompt_generator.py`

```python
from .registry import load_entities, load_domains, load_metrics

def generate_intelligence_prompt_section() -> str:
    """
    Called at server startup (or on-demand).
    Returns the auto-generated portion of the Intelligence system prompt.
    """
    entities = load_entities()
    domains = load_domains()
    metrics = load_metrics()
    # ... (same logic as scripts/generate_ontology.py prompt section)
    return prompt_text
```

This is called in `intelligence.py` to build the system prompt dynamically:

```python
from server.ontology.prompt_generator import generate_intelligence_prompt_section

SYSTEM_PROMPT = f"""
You are Aradune Intelligence...

{generate_intelligence_prompt_section()}

## Rules (always follow)
... (hand-written, not generated)

## How to respond
... (hand-written, not generated)
"""
```

---

## Step 7: Validation Script

### `scripts/validate_ontology.py`

Run as part of CI. Catches errors before they reach production.

```python
def validate_ontology():
    """
    Validate all YAML files in ontology/ directory.
    Checks:
    1. Every entity YAML conforms to schema
    2. Every relationship references a valid target entity
    3. Every via_table exists in the DuckDB lake
    4. Every join_key exists as a column in the via_table
    5. Every metric references a valid source_table
    6. Every domain references valid entities and tables
    7. No orphan tables (tables in db.py not referenced by any entity)
    8. No orphan entities (entities with zero fact tables)
    """
    errors = []
    warnings = []

    # ... validation logic ...

    if errors:
        print(f"FAILED: {len(errors)} errors")
        for e in errors:
            print(f"  ERROR: {e}")
        sys.exit(1)

    if warnings:
        print(f"PASSED with {len(warnings)} warnings")
        for w in warnings:
            print(f"  WARN: {w}")

    print(f"PASSED: {len(entities)} entities, {len(domains)} domains, {len(metrics)} metrics")
```

Add to CI pipeline (`.github/workflows/ci.yml`):
```yaml
- name: Validate ontology
  run: python scripts/validate_ontology.py
```

---

## Step 8: Decision Points for Claude Code

During execution, Claude Code MUST pause and ask the user about:

### 8a. Orphan tables
Tables in `db.py` that don't have `state_code` or any other detectable join key. List them and ask: "These tables don't obviously connect to any entity. Should I create a new entity type, assign them to an existing one, or mark them as standalone?"

### 8b. Duplicate/overlapping tables
Tables that appear to contain similar data (e.g., `fact_mc_enrollment` vs `fact_mc_enrollment_summary` vs `fact_mc_monthly` vs `fact_mc_annual`). Ask: "These tables overlap in content. Which is the canonical source? Should the others be marked as supporting/alternate views?"

### 8c. Column name ambiguity
When a column like `rate` appears without a prefix, or when `code` could be HCPCS, ICD, or SOC. Ask: "This column name is ambiguous. What entity does it refer to?"

### 8d. Domain assignment for cross-domain tables
Tables like `fact_medicaid_opioid_prescribing` could belong to pharmacy, behavioral health, or both. Ask: "Which domain should this table's primary assignment be? It can have secondary assignments too."

### 8e. Metric definitions requiring business logic
When defining metrics, some require business decisions (e.g., "Should pct_of_medicare use simple average or claim-weighted average?"). Ask: "This metric can be calculated multiple ways. Which definition should be canonical?"

### 8f. DuckPGQ edge table limitations
When a fact table can't cleanly map to a source/destination edge (e.g., many-to-many relationships or tables with composite keys). Ask: "This table's relationship can't be expressed as a simple edge in DuckPGQ. Options: (a) skip in graph, use SQL only, (b) create simplified edge, (c) create intermediate vertex."

---

## How to Add a New Dataset After This Is Built

When a Claude Code session ingests a new dataset:

1. **Read the data quality profile first.** Check `COMPLETE-DATA-REFERENCE-FOR-ARADUNE.md` for known issues with this dataset. If not listed, research quality issues before ingesting.
2. Run the ETL script as usual (fetch → parse → validate → normalize → load) using the medallion pattern (raw → bronze, cleaned → silver, aggregated → gold)
3. Add the table to `db.py` `fact_names`
4. Create or update entity YAML in `ontology/entities/` — add the table to `fact_tables` list and any new relationships
5. Update domain YAML in `ontology/domains/` — add the table to primary or supporting tables. Include `quality_tier`, `known_issues`, and `data_quality_notes` from the data reference.
6. If the dataset introduces new metrics, add to `ontology/metrics/`
7. Write Soda Core checks (SodaCL YAML) for the new table: row counts, null rates, value ranges, freshness
8. Write dbt-expectations tests for any cross-table validation (e.g., state enrollment totals should be within 10% of CMS-64 totals)
9. If the dataset has known adversarial edge cases (like Illinois dedup for T-MSIS), add specific tests to `tests/adversarial/`
10. Run `python scripts/validate_ontology.py` — catches broken references
11. Run `python scripts/generate_ontology.py` — regenerates system prompt + DuckPGQ SQL
12. Intelligence immediately knows about the new dataset on next request, including quality caveats

This is the "add a YAML file, run a script, everything updates" workflow.

---

## Success Criteria

The session is complete when:

- [ ] `scripts/introspect_lake.py` runs and produces `ontology/raw_inventory.json`
- [ ] Entity YAML files exist for all ~16 entity types with properties and relationships
- [ ] Domain YAML files exist for all ~13 domains with table assignments, quality tiers, and known issues (sourced from COMPLETE-DATA-REFERENCE)
- [ ] At least 10 named metrics are defined across rate, enrollment, and fiscal domains
- [ ] `scripts/generate_ontology.py` produces a system prompt section (including data quality warnings) and DuckPGQ SQL
- [ ] `scripts/validate_ontology.py` passes with zero errors
- [ ] `server/ontology/registry.py` loads and provides lookup functions
- [ ] Intelligence system prompt in `intelligence.py` uses the auto-generated section
- [ ] `sql/property_graph.sql` contains a valid CREATE PROPERTY GRAPH statement
- [ ] The ontology README explains the system, how to extend it, and references COMPLETE-DATA-REFERENCE for quality rules
- [ ] All decision points have been surfaced and resolved with the user
