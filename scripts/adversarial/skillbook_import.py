"""
Import adversarial test failures into the Skillbook via API.

Reads a JSON report (produced by `runner.py --json report.json`) and converts
each test failure into a Skillbook entry. Also supports importing known facts
from fixtures/known_facts.json as baseline anchor skills.

This connects the adversarial testing loop to the Skillbook feedback system,
so Intelligence learns from test failures automatically.

Usage:
    python -m scripts.adversarial.skillbook_import --report reports/adversarial_full.json
    python -m scripts.adversarial.skillbook_import --anchors
    python -m scripts.adversarial.skillbook_import --report report.json --anchors
    python -m scripts.adversarial.skillbook_import --dry-run --report report.json
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import requests

from scripts.adversarial.config import API_BASE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("skillbook_import")

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
KNOWN_FACTS_PATH = os.path.join(FIXTURES_DIR, "known_facts.json")

# ---------------------------------------------------------------------------
# Failure-type to Skillbook category mapping
# ---------------------------------------------------------------------------
FAILURE_TYPE_TO_CATEGORY = {
    "hallucination": "factual_accuracy",
    "stale_reference": "factual_accuracy",
    "quarantine_bypass": "caveat_enforcement",
    "missing_caveat": "caveat_enforcement",
    "cross_domain": "edge_case",
    "arithmetic": "factual_accuracy",
    "regulatory": "factual_accuracy",
    "security": "factual_accuracy",
    "edge_case": "edge_case",
    "style": "style_rule",
    "demo_quality": "edge_case",
    "multi_turn_contradiction": "factual_accuracy",
    "domain_rule": "factual_accuracy",
}

# ---------------------------------------------------------------------------
# Domain inference from query text
# ---------------------------------------------------------------------------
DOMAIN_KEYWORDS = {
    "fl_rate_rule": ["florida medicaid rate", "fl rate", "fl fee schedule", "facility rate", "pc/tc"],
    "cpra": ["cpra", "conversion factor", "42 cfr 447", "e&m", "e/m codes"],
    "data_vintage": ["current", "right now", "as of today", "enrollment in"],
    "doge_quarantine": ["doge", "doge data", "doge spending", "doge provider"],
    "rates": ["rate", "fee schedule", "reimbursement", "medicaid rate"],
    "enrollment": ["enrollment", "eligibility", "caseload", "unwinding"],
    "expenditure": ["spending", "cms-64", "expenditure", "fiscal", "fmap", "cost"],
    "hospitals": ["hospital", "hcris", "dsh", "ahead", "ccr"],
    "nursing": ["nursing", "five star", "snf", "pbj", "staffing"],
    "pharmacy": ["pharmacy", "drug", "sdud", "nadac", "ndc", "prescription"],
    "behavioral_health": ["behavioral", "mental health", "opioid", "nsduh", "teds"],
    "quality": ["quality", "core set", "measure", "hedis"],
    "workforce": ["wage", "hpsa", "workforce", "provider supply"],
    "policy": ["policy", "cfr", "regulation", "sho", "cib", "waiver", "1115"],
    "style": ["em-dash", "em dash", "style", "formatting"],
    "security": ["drop table", "insert into", "injection", "sql injection"],
}


def infer_domain(query: str, agent_name: str = "") -> str:
    """Infer the Skillbook domain from query text and/or agent name."""
    # Agent name can directly map to a domain
    agent_domain_map = {
        "florida_rate": "fl_rate_rule",
        "skillbook": "general",
        "persona": "style",
        "browser": "general",
        "api": "general",
        "api_fuzzer": "general",
        "consistency": "general",
    }
    if agent_name in agent_domain_map:
        candidate = agent_domain_map[agent_name]
        if candidate != "general":
            return candidate

    q = query.lower()
    for domain, keywords in DOMAIN_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return domain
    return "general"


def infer_category(failure_type: str) -> str:
    """Map a test failure type to a Skillbook category."""
    return FAILURE_TYPE_TO_CATEGORY.get(failure_type, "failure_mode")


def build_skill_content(result: dict) -> str:
    """
    Derive a clear, concise rule statement from a test failure.

    Prefers specific_issue over reasoning, and truncates to 500 chars.
    Skips results where content would be empty or uninformative.
    """
    # Skip evaluator crashes that have no useful content
    specific = result.get("specific_issue", "")
    if specific == "evaluator_crash":
        specific = ""
    if specific == "timeout":
        return ""

    reasoning = result.get("reasoning", "")
    if reasoning and reasoning.startswith("Evaluation error:"):
        reasoning = ""
    if reasoning and reasoning.startswith("Multi-turn error:"):
        reasoning = ""
    if reasoning and reasoning.startswith("Endpoint error:"):
        reasoning = ""

    # Style violations can supplement the main content
    style_violations = result.get("style_violations", [])
    style_text = "; ".join(style_violations) if style_violations else ""

    # Build content: prefer specific_issue, fall back to reasoning, supplement with style
    content = specific or reasoning
    if not content and style_text:
        content = style_text
    elif content and style_text and style_text not in content:
        # Append style violations if they add new info
        remaining = 500 - len(content) - 3
        if remaining > 50:
            content = f"{content} | Style: {style_text}"

    if not content:
        return ""

    # Truncate
    if len(content) > 500:
        content = content[:497] + "..."

    return content.strip()


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def fetch_existing_skills(api_base: str) -> list:
    """GET /api/skillbook and return all existing skill contents for dedup."""
    url = f"{api_base}/api/skillbook?limit=500&active=true"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("skills", [])
    except Exception as e:
        logger.warning(f"Could not fetch existing skills for dedup: {e}")
        return []


def is_duplicate(content: str, existing_skills: list) -> bool:
    """
    Simple substring-based fuzzy deduplication.

    Returns True if content is a near-match to any existing skill.
    Checks: exact match, or either is a substring of the other (after lowering).
    """
    content_lower = content.lower().strip()
    if len(content_lower) < 10:
        return False

    # Use a shorter key for matching (first 80 chars)
    content_key = content_lower[:80]

    for skill in existing_skills:
        existing = (skill.get("content") or "").lower().strip()
        if not existing:
            continue
        existing_key = existing[:80]

        # Exact match
        if content_lower == existing:
            return True

        # Key substring match (first 80 chars overlap)
        if content_key in existing or existing_key in content_lower:
            return True

    return False


# ---------------------------------------------------------------------------
# Import from adversarial report
# ---------------------------------------------------------------------------

def import_from_report(
    report_path: str,
    api_base: str,
    dry_run: bool = False,
) -> dict:
    """
    Read an adversarial test report JSON and create Skillbook entries
    from each failed test.

    Returns stats dict with counts.
    """
    with open(report_path) as f:
        report = json.load(f)

    timestamp = report.get("timestamp", datetime.now().isoformat())
    existing_skills = [] if dry_run else fetch_existing_skills(api_base)

    stats = {"total_failures": 0, "imported": 0, "skipped_dup": 0,
             "skipped_empty": 0, "skipped_agent": 0, "errors": 0}

    for agent_name, agent_report in report.get("reports", {}).items():
        # Skip API fuzzer results (infrastructure failures, not reasoning)
        if agent_name in ("api", "api_fuzzer"):
            continue

        results = agent_report.get("results", [])
        for r in results:
            if r.get("passed", True):
                continue

            stats["total_failures"] += 1

            # Build the skill
            query = r.get("query", "")
            failure_type = r.get("failure_type", "unknown")
            test_name = r.get("name", r.get("test", query[:60]))

            domain = infer_domain(query, agent_name)
            category = infer_category(failure_type)
            content = build_skill_content(r)

            if not content:
                stats["skipped_empty"] += 1
                logger.debug(f"  Skipped (no content): {test_name[:60]}")
                continue

            provenance = (
                f"adversarial/{agent_name}/{test_name[:60]} - {timestamp}"
            )

            # Dedup check
            if not dry_run and is_duplicate(content, existing_skills):
                stats["skipped_dup"] += 1
                logger.info(f"  Duplicate, skipping: {content[:60]}...")
                continue

            skill_payload = {
                "domain": domain,
                "category": category,
                "content": content,
                "source_type": "adversarial_test",
                "provenance": provenance,
            }

            if dry_run:
                logger.info(f"  [DRY RUN] Would add skill:")
                logger.info(f"    domain={domain}, category={category}")
                logger.info(f"    content={content[:80]}...")
                logger.info(f"    provenance={provenance}")
                stats["imported"] += 1
                continue

            # POST to the API
            try:
                resp = requests.post(
                    f"{api_base}/api/skillbook/add",
                    json=skill_payload,
                    timeout=15,
                )
                resp.raise_for_status()
                result_data = resp.json()
                skill_id = result_data.get("skill_id", "?")
                stats["imported"] += 1
                logger.info(
                    f"  Added skill {skill_id}: [{domain}/{category}] "
                    f"{content[:60]}..."
                )

                # Add to existing_skills for ongoing dedup within this run
                existing_skills.append({"content": content})

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"  Failed to add skill: {e}")

    return stats


# ---------------------------------------------------------------------------
# Import known facts as anchor skills
# ---------------------------------------------------------------------------

def import_anchors(
    api_base: str,
    dry_run: bool = False,
    facts_path: str = None,
) -> dict:
    """
    Import known facts from known_facts.json as baseline anchor skills.

    Each fact becomes a skill with source_type="adversarial_anchor".
    """
    path = facts_path or KNOWN_FACTS_PATH
    if not os.path.exists(path):
        logger.error(f"Known facts file not found: {path}")
        return {"total_facts": 0, "imported": 0, "skipped_dup": 0, "errors": 0}

    with open(path) as f:
        facts = json.load(f)

    existing_skills = [] if dry_run else fetch_existing_skills(api_base)
    timestamp = datetime.now().isoformat()

    stats = {"total_facts": len(facts), "imported": 0,
             "skipped_dup": 0, "errors": 0}

    for fact in facts:
        fact_id = fact.get("id", "unknown")
        domain = fact.get("domain", "general")
        question = fact.get("question", "")

        # Build content from the expected behavior
        content_parts = []
        if fact.get("expected_contains"):
            content_parts.append(
                f"Response to '{question[:80]}' must include: "
                + ", ".join(fact["expected_contains"])
            )
        if fact.get("expected_not_contains"):
            content_parts.append(
                f"Response must NOT include: "
                + ", ".join(fact["expected_not_contains"])
            )
        if fact.get("expected_value") is not None:
            tol = fact.get("tolerance_pct", 0)
            content_parts.append(
                f"Expected value: {fact['expected_value']}"
                + (f" (tolerance: {tol}%)" if tol else "")
            )
        if fact.get("expected_value_range"):
            lo, hi = fact["expected_value_range"]
            content_parts.append(f"Expected value range: {lo} to {hi}")

        content = ". ".join(content_parts)
        if not content:
            content = f"Known fact for {domain}: {question[:200]}"

        if len(content) > 500:
            content = content[:497] + "..."

        # Map domain to a category
        domain_category_map = {
            "fl_rate_rule": "factual_accuracy",
            "cpra": "factual_accuracy",
            "enrollment": "factual_accuracy",
            "expenditure": "factual_accuracy",
            "doge_quarantine": "caveat_enforcement",
            "pharmacy": "factual_accuracy",
            "open_payments": "factual_accuracy",
            "rate_comparison": "factual_accuracy",
            "research_module_stats": "factual_accuracy",
            "data_vintage_rules": "style_rule",
            "territory_edge_cases": "edge_case",
            "security": "factual_accuracy",
            "hospital": "factual_accuracy",
            "claims": "caveat_enforcement",
            "rates": "factual_accuracy",
        }
        category = domain_category_map.get(domain, "factual_accuracy")
        provenance = f"adversarial/anchor/{fact_id} - {timestamp}"

        # Dedup check
        if not dry_run and is_duplicate(content, existing_skills):
            stats["skipped_dup"] += 1
            logger.info(f"  Duplicate anchor, skipping: {fact_id}")
            continue

        skill_payload = {
            "domain": domain,
            "category": category,
            "content": content,
            "source_type": "adversarial_anchor",
            "provenance": provenance,
        }

        if dry_run:
            logger.info(f"  [DRY RUN] Would add anchor skill:")
            logger.info(f"    id={fact_id}, domain={domain}, category={category}")
            logger.info(f"    content={content[:80]}...")
            stats["imported"] += 1
            continue

        try:
            resp = requests.post(
                f"{api_base}/api/skillbook/add",
                json=skill_payload,
                timeout=15,
            )
            resp.raise_for_status()
            result_data = resp.json()
            skill_id = result_data.get("skill_id", "?")
            stats["imported"] += 1
            logger.info(
                f"  Added anchor {skill_id}: [{domain}] {fact_id} - "
                f"{content[:50]}..."
            )
            existing_skills.append({"content": content})

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"  Failed to add anchor {fact_id}: {e}")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Import adversarial test failures and known facts into the Skillbook.",
    )
    parser.add_argument(
        "--report",
        help="Path to adversarial results JSON (produced by runner.py --json)",
    )
    parser.add_argument(
        "--anchors",
        action="store_true",
        help="Import known facts from fixtures/known_facts.json as anchor skills",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without actually posting",
    )
    parser.add_argument(
        "--api-base",
        default=None,
        help=f"Override API base URL (default: {API_BASE})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.report and not args.anchors:
        parser.error("At least one of --report or --anchors is required")

    api_base = args.api_base or API_BASE
    logger.info(f"Skillbook import targeting: {api_base}")
    if args.dry_run:
        logger.info("DRY RUN mode -- no skills will be posted")

    print(f"\n{'='*60}")
    print("SKILLBOOK IMPORT")
    print(f"{'='*60}")

    # Import from report
    if args.report:
        if not os.path.exists(args.report):
            logger.error(f"Report file not found: {args.report}")
            sys.exit(1)

        print(f"\nImporting failures from: {args.report}")
        report_stats = import_from_report(
            args.report, api_base, dry_run=args.dry_run
        )
        print(f"\n  Report import results:")
        print(f"    Total failures found:   {report_stats['total_failures']}")
        print(f"    Skills imported:        {report_stats['imported']}")
        print(f"    Skipped (duplicate):    {report_stats['skipped_dup']}")
        print(f"    Skipped (empty/crash):  {report_stats['skipped_empty']}")
        print(f"    Errors:                 {report_stats['errors']}")

    # Import known facts as anchors
    if args.anchors:
        print(f"\nImporting anchor facts from: {KNOWN_FACTS_PATH}")
        anchor_stats = import_anchors(
            api_base, dry_run=args.dry_run
        )
        print(f"\n  Anchor import results:")
        print(f"    Total facts:            {anchor_stats['total_facts']}")
        print(f"    Anchors imported:       {anchor_stats['imported']}")
        print(f"    Skipped (duplicate):    {anchor_stats['skipped_dup']}")
        print(f"    Errors:                 {anchor_stats['errors']}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
