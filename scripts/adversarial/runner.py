"""
Aradune Adversarial Test Runner

Orchestrates all agents and produces a combined report.

Usage:
    python -m scripts.adversarial.runner                       # Run all agents
    python -m scripts.adversarial.runner --agent intelligence   # Single agent
    python -m scripts.adversarial.runner --agent api            # API fuzzer only
    python -m scripts.adversarial.runner --export report.md     # Export markdown
    python -m scripts.adversarial.runner --quick                # Scripted only, no LLM-generated
"""

import argparse
import json
import logging
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.adversarial.agents.intelligence_agent import IntelligenceAgent
from scripts.adversarial.agents.api_agent import ApiAgent
from scripts.adversarial.agents.consistency_agent import ConsistencyAgent
from scripts.adversarial.agents.persona_agent import PersonaAgent
from scripts.adversarial.agents.florida_rate_agent import FloridaRateAgent
from scripts.adversarial.agents.skillbook_agent import SkillbookAgent
from scripts.adversarial.agents.browser_agent import BrowserAgent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logger = logging.getLogger("adversarial.runner")


def run_agents(agent_filter: str = None, quick: bool = False) -> dict:
    """Run selected agents and return combined results."""
    timestamp = datetime.now().isoformat()
    reports = {}

    agents = {
        "intelligence": lambda: IntelligenceAgent(include_generated=not quick),
        "api": lambda: ApiAgent(),
        "consistency": lambda: ConsistencyAgent(),
        "persona": lambda: PersonaAgent(),
        "florida_rate": lambda: FloridaRateAgent(),
        "skillbook": lambda: SkillbookAgent(),
        "browser": lambda: BrowserAgent(),
    }

    for name, factory in agents.items():
        if agent_filter and name != agent_filter:
            continue
        logger.info(f"\n{'='*60}\nRunning {name} agent\n{'='*60}")
        try:
            agent = factory()
            reports[name] = agent.run()
            logger.info(
                f"{name}: {reports[name].get('pass_rate', reports[name].get('avg_score', 'N/A'))}"
            )
        except Exception as e:
            logger.error(f"{name} agent crashed: {e}", exc_info=True)
            reports[name] = {"agent": name, "error": str(e)}

    return {"timestamp": timestamp, "reports": reports}


def export_markdown(results: dict, filepath: str):
    """Export results as a clean markdown report."""
    os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)

    with open(filepath, "w") as f:
        f.write("# Aradune Adversarial Test Report\n\n")
        f.write(f"**Timestamp:** {results['timestamp']}\n\n")
        f.write("---\n\n")

        for name, report in results["reports"].items():
            f.write(f"## Agent: {name}\n\n")

            if "error" in report:
                f.write(f"**CRASHED:** {report['error']}\n\n")
                continue

            # Summary line
            if "pass_rate" in report:
                total = report.get('total', report.get('total_tests', '?'))
                passed = report.get('passed', report.get('total_tests', 0) - report.get('total_500s', 0) if 'total_tests' in report else '?')
                failed = report.get('failed', report.get('total_500s', '?'))
                f.write(f"**Pass rate:** {report['pass_rate']}  |  ")
                f.write(f"Total: {total}  |  ")
                f.write(f"Passed: {passed}  |  ")
                f.write(f"Failed: {failed}")
                if report.get("slow_responses"):
                    f.write(f"  |  Slow: {report['slow_responses']}")
                f.write("\n\n")
            if "avg_score" in report:
                f.write(f"**Average quality score:** {report['avg_score']}/5  |  ")
                f.write(f"**Would impress consultant:** {report.get('impressive_rate', 'N/A')}")
                if report.get("slow_responses"):
                    f.write(f"  |  Slow: {report['slow_responses']}")
                f.write("\n\n")

            # Failures / low scores table
            all_results = report.get("results", [])
            if name == "persona":
                issues = [r for r in all_results if r.get("score", 5) < 4]
            elif name == "api_fuzzer":
                issues = [r for r in all_results if not r.get("passed", True)]
            else:
                issues = [r for r in all_results if not r.get("passed", True)]

            if issues:
                f.write(f"### Issues Found ({len(issues)})\n\n")
                f.write("| Test | Severity/Score | Detail |\n")
                f.write("|------|----------------|--------|\n")
                for issue in issues[:30]:
                    test_name = issue.get("query", issue.get("name", issue.get("test", "?")))[:60]
                    severity = issue.get("severity", issue.get("score", ""))
                    detail = issue.get("reasoning", issue.get("detail", issue.get("specific_issue", "")))[:80]
                    f.write(f"| {test_name} | {severity} | {detail} |\n")
                f.write("\n")

                # Style violations summary
                all_style = []
                for r in all_results:
                    all_style.extend(r.get("style_violations", []))
                if all_style:
                    f.write(f"### Style Violations ({len(all_style)})\n\n")
                    for v in set(all_style):
                        f.write(f"- {v}\n")
                    f.write("\n")

                # Slow responses
                slow = [r for r in all_results if r.get("slow")]
                if slow:
                    f.write(f"### Slow Responses ({len(slow)})\n\n")
                    for s in slow:
                        q = s.get("query", s.get("endpoint", "?"))[:60]
                        f.write(f"- {q} -- {s.get('latency_s', '?')}s\n")
                    f.write("\n")
            else:
                f.write("All checks passed.\n\n")

            f.write("---\n\n")

        # Summary
        f.write("## Summary\n\n")
        for name, report in results["reports"].items():
            if "error" in report:
                status = "CRASHED"
            elif "pass_rate" in report:
                status = report["pass_rate"]
            elif "avg_score" in report:
                status = f"{report['avg_score']}/5"
            else:
                status = "?"
            f.write(f"- **{name}:** {status}\n")

    logger.info(f"Report exported to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Aradune adversarial testing")
    parser.add_argument("--agent", choices=[
                        "intelligence", "api", "consistency", "persona",
                        "florida_rate", "skillbook", "browser"],
                        help="Run a single agent")
    parser.add_argument("--export", help="Export markdown report to file")
    parser.add_argument("--quick", action="store_true",
                        help="Scripted queries only (no LLM-generated, faster + cheaper)")
    parser.add_argument("--json", help="Export raw JSON results to file")
    args = parser.parse_args()

    results = run_agents(agent_filter=args.agent, quick=args.quick)

    if args.export:
        export_markdown(results, args.export)

    if args.json:
        os.makedirs(os.path.dirname(args.json) if os.path.dirname(args.json) else ".", exist_ok=True)
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2, default=str)

    # Print summary
    print(f"\n{'='*60}")
    print("ADVERSARIAL TEST SUMMARY")
    print(f"{'='*60}")
    for name, report in results["reports"].items():
        if "error" in report:
            print(f"  {name}: CRASHED -- {report['error'][:80]}")
        elif "pass_rate" in report:
            slow = f" ({report.get('slow_responses', 0)} slow)" if report.get("slow_responses") else ""
            print(f"  {name}: {report['pass_rate']} pass rate ({report.get('total', '?')} tests){slow}")
        elif "avg_score" in report:
            slow = f" ({report.get('slow_responses', 0)} slow)" if report.get("slow_responses") else ""
            print(f"  {name}: {report['avg_score']}/5 avg score, {report.get('impressive_rate', '?')} impressive{slow}")
    print(f"{'='*60}\n")

    # Exit code: fail if any critical failures or score <= 1
    for report in results["reports"].values():
        for r in report.get("results", []):
            if not r.get("passed", True) and r.get("severity") == "critical":
                sys.exit(1)
            if r.get("score", 5) <= 1:
                sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
