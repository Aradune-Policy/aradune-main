#!/usr/bin/env python3
"""
Aradune validation runner -- runs data quality checks and prints/exports results.

Usage:
    python3 scripts/run_validation.py                    # Full suite, console output
    python3 scripts/run_validation.py --domain rates     # Single domain prefix
    python3 scripts/run_validation.py --export report.md # Export markdown report
    python3 scripts/run_validation.py --failures-only    # Show only failures
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.engines.validator import run_core_checks


def main():
    parser = argparse.ArgumentParser(description="Aradune data validation runner")
    parser.add_argument("--domain", help="Filter by table prefix (e.g., 'fact_rate')")
    parser.add_argument("--export", help="Export results to markdown file")
    parser.add_argument("--failures-only", action="store_true", help="Show only failures")
    args = parser.parse_args()

    print("Running validation checks...")
    results = run_core_checks()

    if args.domain:
        results = [r for r in results if r["table"].startswith(args.domain)]

    if args.failures_only:
        results = [r for r in results if not r["passed"]]

    passed = sum(1 for r in results if r["passed"])
    failed = len(results) - passed
    total = len(results)
    pass_rate = round(passed / total * 100, 1) if total else 0

    # Console output
    print(f"\n{'='*70}")
    print(f"Aradune Validation Results: {passed}/{total} passed ({pass_rate}%)")
    print(f"{'='*70}\n")

    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        icon = "+" if r["passed"] else "X"
        print(f"  [{icon}] {status}  {r['table']:40s}  {r['check']}")
        if not r["passed"]:
            print(f"         actual: {r['actual']}")
            print(f"         expected: {r['expected']}")

    print(f"\n{'='*70}")
    print(f"Total: {total}  |  Passed: {passed}  |  Failed: {failed}  |  Rate: {pass_rate}%")
    print(f"{'='*70}")

    # Export
    if args.export:
        lines = []
        lines.append("# Aradune Validation Report\n")
        lines.append(f"**Total:** {total} checks  |  **Passed:** {passed}  |  **Failed:** {failed}  |  **Rate:** {pass_rate}%\n")
        lines.append("| Status | Table | Check | Actual | Expected |")
        lines.append("|--------|-------|-------|--------|----------|")
        for r in results:
            status = "PASS" if r["passed"] else "**FAIL**"
            lines.append(f"| {status} | {r['table']} | {r['check']} | {r['actual']} | {r['expected']} |")
        lines.append("")

        with open(args.export, "w") as f:
            f.write("\n".join(lines))
        print(f"\nExported to {args.export}")

    # Exit code: 0 if all pass, 1 if any fail
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
