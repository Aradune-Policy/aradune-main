"""
Ontology registry: loads entity/domain/metrics YAML at startup.
Provides fast lookup for Intelligence and API routes.
"""
import yaml
from pathlib import Path
from functools import lru_cache

ONTOLOGY_DIR = Path(__file__).parent.parent.parent / "ontology"


@lru_cache(maxsize=1)
def load_entities() -> dict:
    entities = {}
    d = ONTOLOGY_DIR / "entities"
    if not d.exists():
        return entities
    for f in sorted(d.glob("*.yaml")):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            entities[data["entity"]] = data
    return entities


@lru_cache(maxsize=1)
def load_domains() -> dict:
    domains = {}
    d = ONTOLOGY_DIR / "domains"
    if not d.exists():
        return domains
    for f in sorted(d.glob("*.yaml")):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            domains[data["domain"]] = data
    return domains


@lru_cache(maxsize=1)
def load_metrics() -> dict:
    metrics = {}
    d = ONTOLOGY_DIR / "metrics"
    if not d.exists():
        return metrics
    for f in sorted(d.glob("*.yaml")):
        with open(f) as fh:
            data = yaml.safe_load(fh)
            domain = data.get("domain", f.stem)
            metrics[domain] = data
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


def get_tables_for_domain(domain_name: str) -> list[str]:
    """All tables in a domain (primary + supporting)."""
    domain = load_domains().get(domain_name)
    if not domain:
        return []
    tables = [t["table"] for t in domain.get("primary_tables", [])]
    tables.extend(domain.get("supporting_tables", []))
    return tables


def get_all_table_names() -> list[str]:
    """All table names referenced in any domain."""
    tables = set()
    for domain in load_domains().values():
        for t in domain.get("primary_tables", []):
            tables.add(t["table"])
        for t in domain.get("supporting_tables", []):
            tables.add(t)
    return sorted(tables)
