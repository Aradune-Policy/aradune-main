#!/usr/bin/env python3
"""
Validate ontology YAML files against the schema and the data lake inventory.

Checks:
  1. All entity YAML files parse and have required fields
  2. All domain YAML files parse and have required fields
  3. All metrics YAML files parse and have required fields
  4. Entity types in YAML match schema.yaml entity_types
  5. Domain entity references point to valid entities
  6. Metric aggregation types match schema.yaml aggregation_types
  7. All tables referenced in domains exist in raw_inventory.json
  8. All fact_tables referenced in entities exist in raw_inventory.json
  9. No orphan tables (in inventory but no domain)
 10. No duplicate table assignments across domains
 11. Metric source_table(s) exist in inventory

Exit code 0 = pass, 1 = failures found.
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
ONTOLOGY = ROOT / "ontology"
SCHEMA = ONTOLOGY / "schema.yaml"
INVENTORY = ONTOLOGY / "raw_inventory.json"
ENTITIES_DIR = ONTOLOGY / "entities"
DOMAINS_DIR = ONTOLOGY / "domains"
METRICS_DIR = ONTOLOGY / "metrics"

# Tables that legitimately have no domain (time dimension, reference tables)
DOMAIN_EXEMPT = {
    "dim_time",
    "ref_aco_reach_participants",
    "ref_crosswalk_icd_drg",
    "ref_hcbs_taxonomy",
    "ref_presumptive_eligibility",
}

errors: list[str] = []
warnings: list[str] = []


def error(msg: str) -> None:
    errors.append(msg)


def warn(msg: str) -> None:
    warnings.append(msg)


def load_yaml(path: Path) -> dict | None:
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        error(f"Failed to parse {path.name}: {e}")
        return None


def validate_schema() -> dict:
    """Load and validate schema.yaml. Returns parsed schema."""
    if not SCHEMA.exists():
        error("schema.yaml not found")
        return {}
    data = load_yaml(SCHEMA)
    if not data:
        return {}
    for field in ("entity_types", "cardinalities", "quality_tiers", "aggregation_types"):
        if field not in data:
            error(f"schema.yaml missing required field: {field}")
    return data or {}


def validate_inventory() -> set[str]:
    """Load raw_inventory.json. Returns set of table names."""
    if not INVENTORY.exists():
        error("raw_inventory.json not found")
        return set()
    try:
        with open(INVENTORY) as f:
            inv = json.load(f)
        return set(inv.keys())
    except Exception as e:
        error(f"Failed to parse raw_inventory.json: {e}")
        return set()


def validate_entities(schema: dict, inventory: set[str]) -> dict[str, dict]:
    """Validate entity YAML files. Returns map of entity_name -> parsed data."""
    valid_types = set(schema.get("entity_types", []))
    entities: dict[str, dict] = {}

    if not ENTITIES_DIR.exists():
        error("ontology/entities/ directory not found")
        return entities

    yaml_files = sorted(ENTITIES_DIR.glob("*.yaml"))
    if not yaml_files:
        error("No entity YAML files found")
        return entities

    for path in yaml_files:
        data = load_yaml(path)
        if not data:
            continue

        name = data.get("entity")
        if not name:
            error(f"{path.name}: missing 'entity' field")
            continue

        # Required fields
        for field in ("display_name", "description", "canonical_table", "primary_key"):
            if field not in data:
                error(f"{path.name}: missing required field '{field}'")

        # Entity type must be in schema
        if valid_types and name not in valid_types:
            error(f"{path.name}: entity '{name}' not in schema.yaml entity_types")

        # Canonical table must exist in inventory
        canonical = data.get("canonical_table", "")
        if inventory and canonical and canonical not in inventory:
            warn(f"{path.name}: canonical_table '{canonical}' not in inventory")

        # fact_tables must exist in inventory
        for ft in data.get("fact_tables", []):
            if inventory and ft not in inventory:
                warn(f"{path.name}: fact_table '{ft}' not in inventory")

        entities[name] = data

    # Check all schema entity_types have a YAML file
    defined = set(entities.keys())
    for et in valid_types:
        if et not in defined:
            error(f"schema.yaml lists entity_type '{et}' but no YAML file defines it")

    return entities


def validate_domains(
    schema: dict, entities: dict[str, dict], inventory: set[str]
) -> tuple[dict[str, dict], set[str]]:
    """Validate domain YAML files. Returns (domains, all_referenced_tables)."""
    domains: dict[str, dict] = {}
    all_tables: set[str] = set()
    table_to_domain: dict[str, str] = {}

    if not DOMAINS_DIR.exists():
        error("ontology/domains/ directory not found")
        return domains, all_tables

    yaml_files = sorted(DOMAINS_DIR.glob("*.yaml"))
    if not yaml_files:
        error("No domain YAML files found")
        return domains, all_tables

    for path in yaml_files:
        data = load_yaml(path)
        if not data:
            continue

        name = data.get("domain")
        if not name:
            error(f"{path.name}: missing 'domain' field")
            continue

        for field in ("display_name", "description"):
            if field not in data:
                error(f"{path.name}: missing required field '{field}'")

        # Entity references must point to valid entities
        for ent in data.get("entities", []):
            if entities and ent not in entities:
                error(f"{path.name}: references entity '{ent}' which has no YAML definition")

        # Primary tables must exist in inventory
        for entry in data.get("primary_tables", []):
            tbl = entry.get("table", "") if isinstance(entry, dict) else entry
            all_tables.add(tbl)
            if inventory and tbl not in inventory:
                warn(f"{path.name}: primary_table '{tbl}' not in inventory")
            if tbl in table_to_domain:
                warn(
                    f"{path.name}: table '{tbl}' also appears in domain "
                    f"'{table_to_domain[tbl]}' (duplicate assignment)"
                )
            table_to_domain[tbl] = name

        # Supporting tables
        for tbl in data.get("supporting_tables", []):
            all_tables.add(tbl)
            if inventory and tbl not in inventory:
                warn(f"{path.name}: supporting_table '{tbl}' not in inventory")

        domains[name] = data

    return domains, all_tables


def validate_metrics(schema: dict, inventory: set[str]) -> None:
    """Validate metrics YAML files."""
    valid_agg = set(schema.get("aggregation_types", []))

    if not METRICS_DIR.exists():
        error("ontology/metrics/ directory not found")
        return

    yaml_files = sorted(METRICS_DIR.glob("*.yaml"))
    if not yaml_files:
        error("No metrics YAML files found")
        return

    for path in yaml_files:
        data = load_yaml(path)
        if not data:
            continue

        if "metrics" not in data:
            error(f"{path.name}: missing 'metrics' list")
            continue

        for m in data["metrics"]:
            name = m.get("name", "<unnamed>")
            for field in ("display_name", "description", "formula", "aggregation", "unit"):
                if field not in m:
                    error(f"{path.name}: metric '{name}' missing field '{field}'")

            # Aggregation type must be in schema
            agg = m.get("aggregation", "")
            if valid_agg and agg and agg not in valid_agg:
                error(
                    f"{path.name}: metric '{name}' uses aggregation '{agg}' "
                    f"not in schema.yaml"
                )

            # source_table(s) must exist
            src = m.get("source_table")
            srcs = m.get("source_tables", [])
            tables_to_check = ([src] if src else []) + srcs
            for tbl in tables_to_check:
                if inventory and tbl not in inventory:
                    warn(f"{path.name}: metric '{name}' references '{tbl}' not in inventory")


def check_orphan_tables(inventory: set[str], domain_tables: set[str]) -> None:
    """Warn about tables in inventory that no domain references."""
    for tbl in sorted(inventory):
        if tbl not in domain_tables and tbl not in DOMAIN_EXEMPT:
            # Only warn for fact tables; dim/ref tables are often only in entities
            if tbl.startswith("fact_"):
                warn(f"Orphan fact table: '{tbl}' not referenced by any domain")


def main() -> int:
    print("Validating Aradune ontology...\n")

    schema = validate_schema()
    inventory = validate_inventory()
    entities = validate_entities(schema, inventory)
    domains, domain_tables = validate_domains(schema, entities, inventory)
    validate_metrics(schema, inventory)
    check_orphan_tables(inventory, domain_tables)

    # Summary
    print(f"  Schema:    {len(schema.get('entity_types', []))} entity types, "
          f"{len(schema.get('aggregation_types', []))} aggregation types")
    print(f"  Entities:  {len(entities)} YAML files")
    print(f"  Domains:   {len(domains)} YAML files")
    metric_files = list(METRICS_DIR.glob("*.yaml")) if METRICS_DIR.exists() else []
    print(f"  Metrics:   {len(metric_files)} YAML files")
    print(f"  Inventory: {len(inventory)} tables")
    print()

    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  [WARN] {w}")
        print()

    if errors:
        print(f"ERRORS ({len(errors)}):")
        for e in errors:
            print(f"  [FAIL] {e}")
        print(f"\nValidation FAILED with {len(errors)} error(s).")
        return 1

    print("Validation PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
