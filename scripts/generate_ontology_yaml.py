"""
Generate entity, domain, and metrics YAML files from raw_inventory.json.

Usage: python3 scripts/generate_ontology_yaml.py
"""
import json
import yaml
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).parent.parent
INVENTORY = ROOT / "ontology" / "raw_inventory.json"
ENTITIES_DIR = ROOT / "ontology" / "entities"
DOMAINS_DIR = ROOT / "ontology" / "domains"
METRICS_DIR = ROOT / "ontology" / "metrics"


def load_inventory():
    with open(INVENTORY) as f:
        return json.load(f)


def get_columns(inv, table):
    info = inv.get(table, {})
    return [c["name"] for c in info.get("columns", [])]


def get_row_count(inv, table):
    return inv.get(table, {}).get("row_count", 0)


# ── Entity inference rules ──────────────────────────────────────────────

ENTITY_KEY_PATTERNS = {
    "state_code": "state",
    "procedure_code": "procedure",
    "cpt_hcpcs_code": "procedure",
    "hcpcs_code": "procedure",
    "provider_ccn": "hospital",
    "ccn": "hospital",
    "npi": "provider",
    "billing_npi": "provider",
    "ndc": "drug",
    "ndc_code": "drug",
    "plan_id": "mco",
    "county_fips": "geographic_area",
    "fips_code": "geographic_area",
    "fips": "geographic_area",
    "soc_code": "occupation",
    "measure_id": "quality_measure",
    "measure_name": "quality_measure",
    "measure_code": "quality_measure",
}

# Also detect state via "State" or "state" or "State Abbreviation"
STATE_ALIASES = {"state", "state_name", "State", "State Abbreviation", "state_abbreviation"}


# ── Domain assignment rules ─────────────────────────────────────────────

DOMAIN_RULES = {
    "rates": {
        "keywords": ["rate", "comparison", "fee_schedule", "medicare_rate", "medicaid_rate", "cpra",
                      "claims", "claims_categories", "claims_monthly", "dq_atlas"],
        "tables": ["fact_medicaid_rate", "fact_medicare_rate", "fact_medicare_rate_state",
                    "fact_rate_comparison", "fact_dq_flag", "dim_procedure",
                    "dim_medicare_locality", "dim_hcpcs"],
    },
    "enrollment": {
        "keywords": ["enrollment", "eligibility", "unwinding", "chip", "new_adult",
                      "medicaid_application", "renewal", "dual_status", "managed_care",
                      "mc_enroll", "mc_monthly", "mc_annual", "mc_info", "mc_dashboard",
                      "mc_share", "mc_summary", "mc_programs", "benefit_package",
                      "program_annual", "program_monthly", "elig_group",
                      "marketplace_oep", "mc_quality_features", "dental_services",
                      "telehealth_services", "pace"],
        "tables": ["dim_state", "dim_pace_organization"],
    },
    "hospitals": {
        "keywords": ["hospital", "hcris", "dsh", "vbp", "hrrp", "mspb",
                      "hac", "hai", "hcahps", "psi90", "ahead",
                      "timely_effective", "complications", "unplanned_visits",
                      "imaging", "hospital_directory", "hospital_service_area",
                      "hospital_rating", "cms_impact"],
        "tables": [],
    },
    "nursing": {
        "keywords": ["nh_", "five_star", "pbj_", "snf_", "mds_quality",
                      "nh_deficien", "nh_survey", "nh_penalties", "nh_ownership",
                      "nh_provider", "nh_state", "nh_claims"],
        "tables": [],
    },
    "quality": {
        "keywords": ["quality_core_set", "quality_measure", "scorecard", "epsdt",
                      "performance_indicator"],
        "tables": ["dim_scorecard_measure"],
    },
    "workforce": {
        "keywords": ["bls_wage", "hpsa", "workforce", "nursing_workforce",
                      "nursing_earnings", "nhsc_field", "mua_designation"],
        "tables": ["dim_bls_occupation"],
    },
    "pharmacy": {
        "keywords": ["drug_utilization", "sdud", "nadac", "drug_rebate",
                      "aca_ful", "drug_spending", "part_d"],
        "tables": [],
    },
    "behavioral_health": {
        "keywords": ["nsduh", "mh_facility", "teds", "bh_", "mh_sud",
                      "block_grant", "opioid", "otp_provider", "ipf_",
                      "physical_among", "integrated_care", "drug_overdose",
                      "cdc_overdose"],
        "tables": [],
    },
    "ltss_hcbs": {
        "keywords": ["hcbs", "ltss", "cms372", "1915c", "mltss", "waiver"],
        "tables": [],
    },
    "expenditure": {
        "keywords": ["expenditure", "cms64", "ffcra_fmap", "fmr_", "financial_mgmt",
                      "fmap", "caa_fmap", "nhe_state", "sdp_preprint",
                      "macpac", "supplemental"],
        "tables": [],
    },
    "economic": {
        "keywords": ["unemployment", "cpi", "median_income", "state_gdp",
                      "state_population", "saipe", "fair_market_rent",
                      "snap_enrollment", "tanf_enrollment", "food_environment",
                      "acs_state", "ahrf_county"],
        "tables": [],
    },
    "medicare": {
        "keywords": ["medicare_enrollment", "medicare_geo", "medicare_physician",
                      "medicare_telehealth", "medicare_program", "medicare_spending",
                      "medicare_provider_enrollment", "mssp", "aco_",
                      "esrd_", "ma_geo", "market_saturation"],
        "tables": [],
    },
    "providers": {
        "keywords": ["physician_compare", "provider_specific", "fqhc_",
                      "health_center", "dialysis_facility", "hospice_",
                      "home_health", "irf_provider", "ltch_provider",
                      "asc_facility", "asc_quality", "vha_provider",
                      "pos_hospital", "pos_other", "pac_",
                      "dialysis_state", "hha_cost_report", "oas_cahps",
                      "provider"],
        "tables": ["dim_provider_taxonomy", "fact_provider"],
    },
    "public_health": {
        "keywords": ["places_county", "brfss", "vital_stats", "mortality",
                      "maternal", "nas_rates", "pregnant", "smm_",
                      "contraceptive", "respiratory", "vaccination",
                      "blood_lead", "well_child", "cdc_leading",
                      "perinatal", "health_screening", "acute_care",
                      "pregnancy_outcomes"],
        "tables": [],
    },
    "policy": {
        "keywords": ["policy_document", "policy_chunk", "spa"],
        "tables": [],
    },
}


def assign_domain(table_name, inv):
    """Assign a table to a domain. Returns (domain, is_primary)."""
    for domain, rules in DOMAIN_RULES.items():
        if table_name in rules["tables"]:
            return domain
        for kw in rules["keywords"]:
            if kw in table_name.lower():
                return domain
    return None


def detect_entity_links(table_name, columns):
    """Detect which entities a table links to via column patterns."""
    links = {}
    col_set = set(columns)
    for col, entity in ENTITY_KEY_PATTERNS.items():
        if col in col_set:
            links[entity] = col
    # State alias detection
    if col_set.intersection(STATE_ALIASES) and "state" not in links:
        alias = col_set.intersection(STATE_ALIASES).pop()
        links["state"] = alias
    return links


def generate_entities(inv):
    """Generate entity YAML files."""
    # Canonical dimension tables
    dim_entities = {
        "state": {
            "canonical_table": "dim_state",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "State Medicaid Program",
            "description": "A US state or territory's Medicaid program",
        },
        "procedure": {
            "canonical_table": "dim_procedure",
            "primary_key": "cpt_hcpcs_code",
            "key_type": "VARCHAR",
            "display_name": "Medical Procedure",
            "description": "A HCPCS/CPT procedure code with RVUs and descriptions",
        },
        "occupation": {
            "canonical_table": "dim_bls_occupation",
            "primary_key": "soc_code",
            "key_type": "VARCHAR",
            "display_name": "Healthcare Occupation",
            "description": "A BLS Standard Occupational Classification for healthcare workers",
        },
        "quality_measure": {
            "canonical_table": "dim_scorecard_measure",
            "primary_key": "measure_id",
            "key_type": "VARCHAR",
            "display_name": "Quality Measure",
            "description": "A CMS quality measure from Core Set, Five-Star, or Scorecard",
        },
    }

    # Implicit entities (no dim table, inferred from column patterns)
    implicit_entities = {
        "hospital": {
            "canonical_table": "fact_hospital_cost",
            "primary_key": "provider_ccn",
            "key_type": "VARCHAR",
            "display_name": "Hospital",
            "description": "A hospital identified by CMS Certification Number",
        },
        "nursing_facility": {
            "canonical_table": "fact_nh_provider_info",
            "primary_key": "ccn",
            "key_type": "VARCHAR",
            "display_name": "Nursing Facility",
            "description": "A skilled nursing facility with Five-Star ratings and staffing data",
        },
        "provider": {
            "canonical_table": "fact_provider",
            "primary_key": "npi",
            "key_type": "VARCHAR(10)",
            "display_name": "Healthcare Provider",
            "description": "A provider identified by National Provider Identifier",
        },
        "drug": {
            "canonical_table": "fact_drug_utilization",
            "primary_key": "ndc",
            "key_type": "VARCHAR(11)",
            "display_name": "Drug Product",
            "description": "A drug product identified by National Drug Code",
        },
        "mco": {
            "canonical_table": "fact_mc_enrollment_plan",
            "primary_key": "plan_id",
            "key_type": "VARCHAR",
            "display_name": "Managed Care Organization",
            "description": "A Medicaid managed care plan",
        },
        "geographic_area": {
            "canonical_table": "fact_places_county",
            "primary_key": "county_fips",
            "key_type": "VARCHAR(5)",
            "display_name": "Geographic Area",
            "description": "A county or geographic area identified by FIPS code",
        },
        "policy_document": {
            "canonical_table": "fact_policy_document",
            "primary_key": "doc_id",
            "key_type": "VARCHAR",
            "display_name": "Policy Document",
            "description": "A CMS policy document (CIB, SHO, SPA, waiver, Federal Register)",
        },
        "hcbs_program": {
            "canonical_table": "fact_hcbs_waitlist",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "HCBS Program",
            "description": "A Home and Community-Based Services waiver program",
        },
        "enrollment_record": {
            "canonical_table": "fact_enrollment",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "Enrollment Record",
            "description": "Monthly Medicaid enrollment by state",
        },
        "expenditure_record": {
            "canonical_table": "fact_expenditure",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "Expenditure Record",
            "description": "CMS-64 expenditure by state and category",
        },
        "economic_indicator": {
            "canonical_table": "fact_unemployment",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "Economic Indicator",
            "description": "Economic context data (unemployment, poverty, income)",
        },
        "rate_cell": {
            "canonical_table": "fact_rate_comparison",
            "primary_key": "state_code",
            "key_type": "VARCHAR(2)",
            "display_name": "Rate Cell",
            "description": "A Medicaid-to-Medicare rate comparison for a state and procedure",
        },
    }

    all_entities = {}
    all_entities.update(dim_entities)
    all_entities.update(implicit_entities)

    # Find all fact tables that reference each entity
    for entity_name, entity in all_entities.items():
        pk = entity["primary_key"]
        fact_tables = []
        for table_name, info in inv.items():
            if "error" in info:
                continue
            cols = [c["name"] for c in info["columns"]]
            # Check for PK match
            if pk in cols:
                fact_tables.append(table_name)
            # Special handling for state: also check aliases
            elif entity_name == "state":
                if set(cols).intersection(STATE_ALIASES):
                    fact_tables.append(table_name)
            # Hospital: also check 'ccn' and 'CMS Certification Number (CCN)'
            elif entity_name == "hospital":
                if "ccn" in cols or any("CCN" in c or "Certification Number" in c for c in cols):
                    fact_tables.append(table_name)
        entity["fact_tables"] = sorted(set(fact_tables))

    # Generate YAML files
    for entity_name, entity in all_entities.items():
        # Get properties from canonical table
        canonical = entity["canonical_table"]
        props = []
        if canonical in inv and "columns" in inv[canonical]:
            for col in inv[canonical]["columns"]:
                if col["name"] != entity["primary_key"]:
                    props.append({
                        "name": col["name"],
                        "type": col["type"],
                    })

        yaml_data = {
            "entity": entity_name,
            "display_name": entity["display_name"],
            "description": entity["description"],
            "canonical_table": canonical,
            "primary_key": entity["primary_key"],
            "key_type": entity["key_type"],
            "properties": props[:20],  # Cap at 20 most important
            "fact_tables": entity["fact_tables"],
        }

        out_path = ENTITIES_DIR / f"{entity_name}.yaml"
        with open(out_path, "w") as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return all_entities


def generate_domains(inv, entities):
    """Generate domain YAML files."""
    # Assign every table to a domain
    table_domains = {}
    unassigned = []
    for table_name in inv:
        domain = assign_domain(table_name, inv)
        if domain:
            table_domains[table_name] = domain
        else:
            unassigned.append(table_name)

    domains = {}
    display_names = {
        "rates": "Rates & Fee Schedules",
        "enrollment": "Enrollment & Managed Care",
        "hospitals": "Hospitals & Acute Care",
        "nursing": "Nursing Facilities",
        "quality": "Quality & Outcomes",
        "workforce": "Workforce & Shortage Areas",
        "pharmacy": "Pharmacy & Drug Spending",
        "behavioral_health": "Behavioral Health & Substance Use",
        "ltss_hcbs": "LTSS & HCBS",
        "expenditure": "Expenditure & Fiscal",
        "economic": "Economic & Social Context",
        "medicare": "Medicare & ACOs",
        "providers": "Providers & Facilities",
        "public_health": "Public Health & Maternal",
        "policy": "Policy & Regulatory",
    }

    descriptions = {
        "rates": "Medicaid provider payment rates, Medicare benchmarks, and rate comparisons across 47 states",
        "enrollment": "Monthly Medicaid enrollment, eligibility groups, managed care, CHIP, unwinding, and applications",
        "hospitals": "Hospital cost reports (HCRIS), quality ratings, DSH/VBP/HRRP, and AHEAD readiness",
        "nursing": "Nursing facility Five-Star ratings, PBJ staffing, deficiency citations, SNF cost and quality",
        "quality": "CMS Core Set quality measures, Scorecard, EPSDT, and performance indicators",
        "workforce": "BLS healthcare wages, HPSA shortage designations, workforce projections, and nursing workforce",
        "pharmacy": "State Drug Utilization (SDUD), NADAC pricing, drug rebates, and Part D spending",
        "behavioral_health": "NSDUH prevalence, TEDS admissions, MH/SUD facilities, opioid prescribing, and IPF quality",
        "ltss_hcbs": "HCBS waitlists, waiver enrollment, LTSS expenditure/rebalancing, 1915(c) waivers, MLTSS",
        "expenditure": "CMS-64 expenditure, FMAP, supplemental payments, FMR, NHE state, and MACPAC data",
        "economic": "BLS CPI/unemployment, Census ACS, SAIPE poverty, GDP, SNAP/TANF, food environment, HUD FMR",
        "medicare": "Medicare enrollment, geographic variation, ACOs (MSSP/REACH), Part D, ESRD, market saturation",
        "providers": "FQHCs, dialysis facilities, hospice, HHA, IRF, LTCH, ASC, physician compare, POS",
        "public_health": "CDC PLACES, BRFSS, vital statistics, maternal health/mortality, overdose deaths",
        "policy": "CMS policy documents (CIBs, SHO letters, SPAs, waivers) with searchable text chunks",
    }

    for domain_name in DOMAIN_RULES:
        tables = sorted(t for t, d in table_domains.items() if d == domain_name)
        primary = []
        supporting = []
        for t in tables:
            info = inv.get(t, {})
            rows = info.get("row_count", 0)
            entry = {
                "table": t,
                "row_count": rows,
            }
            if rows > 1000 or t.startswith("dim_"):
                primary.append(entry)
            else:
                supporting.append(entry)

        # Related entities
        related_entities = set()
        for t in tables:
            cols = get_columns(inv, t)
            links = detect_entity_links(t, cols)
            related_entities.update(links.keys())

        yaml_data = {
            "domain": domain_name,
            "display_name": display_names.get(domain_name, domain_name.replace("_", " ").title()),
            "description": descriptions.get(domain_name, ""),
            "entities": sorted(related_entities),
            "primary_tables": primary,
            "supporting_tables": [s["table"] for s in supporting],
        }

        out_path = DOMAINS_DIR / f"{domain_name}.yaml"
        with open(out_path, "w") as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        domains[domain_name] = yaml_data

    # Report unassigned
    if unassigned:
        print(f"\nUnassigned tables ({len(unassigned)}):")
        for t in sorted(unassigned):
            print(f"  {t}: {get_row_count(inv, t):,} rows")

    return domains, unassigned


def generate_metrics():
    """Generate named metrics YAML files."""
    metrics = {
        "rate_metrics": {
            "domain": "rates",
            "metrics": [
                {
                    "name": "pct_of_medicare",
                    "display_name": "Medicaid as % of Medicare",
                    "description": "Medicaid FFS rate divided by Medicare non-facility rate",
                    "formula": "medicaid_rate / medicare_nonfac_rate",
                    "source_table": "fact_rate_comparison",
                    "aggregation": "avg",
                    "unit": "percentage",
                    "caveats": [
                        "Uses non-facility Medicare rate per 42 CFR 447.203",
                        "Base rates only, excludes supplemental payments",
                    ],
                },
                {
                    "name": "cpra_pct_of_medicare",
                    "display_name": "CPRA Medicaid-to-Medicare Ratio",
                    "description": "Official CPRA calculation per 42 CFR 447.203",
                    "formula": "SUM(medicaid_rate * claim_count) / SUM(medicare_nonfac_rate * claim_count)",
                    "source_table": "fact_rate_comparison",
                    "aggregation": "weighted_avg",
                    "weight_column": "claim_count",
                    "unit": "percentage",
                    "conversion_factor": 32.3465,
                    "compliance_rule": "42 CFR 447.203",
                    "deadline": "2026-07-01",
                },
                {
                    "name": "rate_decay_index",
                    "display_name": "Rate Decay Index",
                    "description": "How far Medicaid rates have fallen behind Medicare over time",
                    "formula": "current_pct_of_medicare / baseline_pct_of_medicare",
                    "source_table": "fact_rate_comparison",
                    "aggregation": "avg",
                    "unit": "ratio",
                },
                {
                    "name": "implied_conversion_factor",
                    "display_name": "Implied Conversion Factor",
                    "description": "Reverse-engineered state CF from Medicaid rates and RVUs",
                    "formula": "medicaid_rate / total_rvu",
                    "source_tables": ["fact_medicaid_rate", "dim_procedure"],
                    "aggregation": "median",
                    "unit": "currency",
                    "caveats": [
                        "Only valid for RBRVS-based states",
                        "Excludes codes with $0 rates or missing RVUs",
                    ],
                },
            ],
        },
        "enrollment_metrics": {
            "domain": "enrollment",
            "metrics": [
                {
                    "name": "enrollment_change_pct",
                    "display_name": "Enrollment Change %",
                    "description": "Month-over-month or year-over-year enrollment change",
                    "formula": "(current_enrollment - prior_enrollment) / prior_enrollment",
                    "source_table": "fact_enrollment",
                    "aggregation": "pct_of_total",
                    "unit": "percentage",
                },
                {
                    "name": "managed_care_penetration",
                    "display_name": "Managed Care Penetration",
                    "description": "Percent of Medicaid enrollees in managed care",
                    "formula": "mc_enrollment / total_enrollment",
                    "source_table": "fact_mc_enrollment_summary",
                    "aggregation": "ratio",
                    "unit": "percentage",
                },
                {
                    "name": "unwinding_disenrollment_rate",
                    "display_name": "Unwinding Disenrollment Rate",
                    "description": "Percent of renewals resulting in disenrollment during PHE unwinding",
                    "formula": "disenrollments / total_renewals",
                    "source_table": "fact_unwinding",
                    "aggregation": "ratio",
                    "unit": "percentage",
                },
            ],
        },
        "fiscal_metrics": {
            "domain": "expenditure",
            "metrics": [
                {
                    "name": "per_enrollee_spending",
                    "display_name": "Per-Enrollee Spending",
                    "description": "Total Medicaid spending divided by average monthly enrollment",
                    "formula": "total_expenditure / avg_monthly_enrollment",
                    "source_tables": ["fact_expenditure", "fact_enrollment"],
                    "aggregation": "ratio",
                    "unit": "currency",
                    "caveats": [
                        "CHIP excluded from per-enrollee calculations",
                        "Uses CMS-64 expenditure (may differ from state budget data)",
                    ],
                },
                {
                    "name": "federal_share_pct",
                    "display_name": "Federal Share %",
                    "description": "Federal share of total Medicaid expenditure",
                    "formula": "federal_expenditure / total_expenditure",
                    "source_table": "fact_expenditure",
                    "aggregation": "ratio",
                    "unit": "percentage",
                },
                {
                    "name": "fmap_rate",
                    "display_name": "FMAP Rate",
                    "description": "Federal Medical Assistance Percentage for a state",
                    "formula": "fmap (direct lookup)",
                    "source_table": "fact_fmap",
                    "aggregation": "avg",
                    "unit": "percentage",
                },
            ],
        },
        "quality_metrics": {
            "domain": "quality",
            "metrics": [
                {
                    "name": "core_set_measure_rate",
                    "display_name": "Core Set Measure Rate",
                    "description": "State performance rate on a CMS Core Set quality measure",
                    "formula": "measure_value (direct)",
                    "source_table": "fact_quality_core_set_2024",
                    "aggregation": "avg",
                    "unit": "percentage",
                },
                {
                    "name": "five_star_avg",
                    "display_name": "Average Five-Star Rating",
                    "description": "Average CMS overall Five-Star rating for nursing facilities",
                    "formula": "AVG(overall_rating)",
                    "source_table": "fact_five_star",
                    "aggregation": "avg",
                    "unit": "rating",
                },
            ],
        },
        "access_metrics": {
            "domain": "workforce",
            "metrics": [
                {
                    "name": "wage_adequacy_ratio",
                    "display_name": "Wage Adequacy Ratio",
                    "description": "Medicaid rate implied hourly wage vs BLS market wage",
                    "formula": "medicaid_implied_wage / bls_median_wage",
                    "source_tables": ["fact_medicaid_rate", "fact_bls_wage"],
                    "aggregation": "ratio",
                    "unit": "ratio",
                },
                {
                    "name": "hpsa_score",
                    "display_name": "HPSA Shortage Score",
                    "description": "Health Professional Shortage Area designation score",
                    "formula": "hpsa_score (direct)",
                    "source_table": "fact_hpsa",
                    "aggregation": "avg",
                    "unit": "score",
                },
                {
                    "name": "hcbs_waitlist_per_capita",
                    "display_name": "HCBS Waitlist per 1,000 Enrollees",
                    "description": "People on HCBS waitlists per 1,000 Medicaid enrollees",
                    "formula": "waitlist_count / (enrollment / 1000)",
                    "source_tables": ["fact_hcbs_waitlist", "fact_enrollment"],
                    "aggregation": "ratio",
                    "unit": "rate_per_1000",
                },
            ],
        },
    }

    for filename, data in metrics.items():
        out_path = METRICS_DIR / f"{filename}.yaml"
        with open(out_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return metrics


def main():
    print("Loading inventory...")
    inv = load_inventory()
    print(f"  {len(inv)} tables loaded\n")

    print("Generating entity YAML files...")
    entities = generate_entities(inv)
    print(f"  {len(entities)} entities written to {ENTITIES_DIR}/\n")

    print("Generating domain YAML files...")
    domains, unassigned = generate_domains(inv, entities)
    print(f"  {len(domains)} domains written to {DOMAINS_DIR}/\n")

    print("Generating metrics YAML files...")
    metrics = generate_metrics()
    total_metrics = sum(len(m["metrics"]) for m in metrics.values())
    print(f"  {total_metrics} metrics across {len(metrics)} files written to {METRICS_DIR}/\n")

    # Summary
    total_tables = sum(len(d.get("primary_tables", []) + d.get("supporting_tables", [])) for d in domains.values() if isinstance(d, dict))
    print(f"Summary:")
    print(f"  {len(entities)} entities")
    print(f"  {len(domains)} domains")
    print(f"  {total_metrics} named metrics")
    print(f"  {len(unassigned)} unassigned tables")


if __name__ == "__main__":
    main()
