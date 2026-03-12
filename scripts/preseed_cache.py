"""
Pre-seed the Intelligence response cache.

Runs common questions against the live Intelligence API and saves responses
to server/cache_seeds.json. This file is loaded on startup so common
queries are instant (0 tokens, 0 latency).

Usage:
    python3 scripts/preseed_cache.py [--api URL]

Default API: https://aradune-api.fly.dev
"""

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.request

QUESTIONS = [
    "Which states pay below 50% of Medicare for primary care E/M codes?",
    "What are the top 10 drugs by Medicaid spending in 2023?",
    "Compare Florida's Medicaid enrollment trend to the national average",
    "Show me states with the longest HCBS waitlists and their FMAP rates",
    "Which states have the most severe primary care HPSA designations?",
    "What percentage of Medicaid spending goes to managed care vs FFS by state?",
    "What is the CPRA requirement under 42 CFR 447.203 and when is it due?",
    "What states have expanded Medicaid but still pay below Medicare parity?",
    "Show me the top 10 hospitals by Medicaid days",
    "What is the average Medicaid-to-Medicare rate ratio by state for E/M codes?",
    "Which states spend the most on prescription drugs per Medicaid enrollee?",
    "Compare mental health prevalence rates across all states",
    "What states have the highest FMAP rates?",
    "Show enrollment trends for total Medicaid enrollment by year nationally",
    "How does nursing home quality vary between Medicaid expansion and non-expansion states?",
    "What are the biggest categories of Medicaid spending nationally?",
    "Which states have the highest rates of opioid prescribing?",
    "Show me states where Medicaid enrollment increased the most during COVID",
    "What is the national average Medicaid-to-Medicare payment ratio?",
    "Compare hospital readmission penalties between high and low Medicaid states",
    "What are the most common Medicaid quality measures and how do states compare?",
    "How much does each state spend per Medicaid enrollee?",
    "Which states have the most people on HCBS waitlists?",
    "What is the current status of Medicaid unwinding by state?",
    "Show me the relationship between poverty rates and Medicaid enrollment by state",
    "What are the top behavioral health measures in the Medicaid Core Set?",
    "How do Medicaid rates for office visits compare across states?",
    "Which states have the highest uninsured rates?",
    "What is the total Medicaid spending by state for FY2024?",
    "How has managed care penetration changed over the past 5 years?",
]


def cache_key(message: str) -> str:
    normalized = message.strip().lower()
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return hashlib.sha256(normalized.encode()).hexdigest()[:20]


def call_intelligence(api_url: str, question: str) -> dict | None:
    """Call the sync Intelligence endpoint and return the response."""
    url = f"{api_url}/api/intelligence"
    payload = json.dumps({
        "message": question,
        "history": [],
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
            return data
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Pre-seed Intelligence cache")
    parser.add_argument("--api", default="https://aradune-api.fly.dev", help="API base URL")
    parser.add_argument("--output", default="server/cache_seeds.json", help="Output file")
    parser.add_argument("--limit", type=int, default=len(QUESTIONS), help="Max questions to process")
    args = parser.parse_args()

    print(f"Pre-seeding cache from {args.api}")
    print(f"Processing {min(args.limit, len(QUESTIONS))} questions...\n")

    seeds = []
    for i, q in enumerate(QUESTIONS[:args.limit]):
        print(f"[{i+1}/{min(args.limit, len(QUESTIONS))}] {q[:60]}...")
        t0 = time.time()
        result = call_intelligence(args.api, q)
        elapsed = time.time() - t0

        if result and result.get("response"):
            key = cache_key(q)
            seeds.append({
                "key": key,
                "question": q,
                "response": result["response"],
                "tool_calls": [{"name": tc.get("name", "")} for tc in result.get("tool_calls", [])],
                "queries": [],
            })
            chars = len(result["response"])
            print(f"  OK ({elapsed:.1f}s, {chars} chars)")
        else:
            print(f"  FAILED ({elapsed:.1f}s)")

        # Brief pause between calls
        if i < len(QUESTIONS) - 1:
            time.sleep(2)

    # Save
    with open(args.output, "w") as f:
        json.dump(seeds, f, indent=2)

    print(f"\nDone. {len(seeds)}/{min(args.limit, len(QUESTIONS))} saved to {args.output}")
    print(f"Total size: {len(json.dumps(seeds)) / 1024:.0f} KB")


if __name__ == "__main__":
    main()
