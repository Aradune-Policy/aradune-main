#!/usr/bin/env python3
"""
Build cache_seeds.json by querying the live Intelligence endpoint.
Run this locally with the server running, then bake the output into Docker.

Usage:
    python scripts/build_cache_seeds.py [--api-url http://localhost:8000]
"""

import argparse
import hashlib
import json
import re
import sys
import time
import requests

# The 6 starter prompts from IntelligenceChat.tsx + extra demo queries
SEED_QUERIES = [
    # Starter prompts (exact match to frontend)
    "What is the CPRA requirement under 42 CFR 447.203 and when is it due?",
    "Which states pay below 50% of Medicare for primary care E/M codes?",
    "Show me states with the longest HCBS waitlists and their FMAP rates",
    "What are the top 10 drugs by Medicaid spending in 2023?",
    "Compare Florida's Medicaid enrollment trend to the national average",
    "Which states have the most severe primary care HPSA designations?",

    # High-value demo queries
    "Give me a comprehensive profile of Ohio's Medicaid program",
    "How does Texas's per-enrollee Medicaid spending compare to other large states?",
    "What are the key differences between Medicaid expansion and non-expansion states?",
    "Compare nursing facility quality ratings across the Southeast",
    "What drove enrollment changes during the PHE unwinding?",
    "Which states have the highest Medicaid managed care penetration rates?",
]


def cache_key(message: str) -> str:
    """Same normalization as server/routes/intelligence.py _cache_key()."""
    normalized = message.strip().lower()
    normalized = re.sub(r'[^\w\s]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return hashlib.sha256(normalized.encode()).hexdigest()[:20]


def query_intelligence(api_url: str, message: str) -> dict | None:
    """Send a query to /api/intelligence and return the response."""
    try:
        resp = requests.post(
            f"{api_url}/api/intelligence",
            json={"message": message, "history": [], "context": None},
            timeout=120,
        )
        if resp.status_code == 200:
            return resp.json()
        print(f"  HTTP {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(description="Build Intelligence cache seeds")
    parser.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--output", default="server/cache_seeds.json", help="Output file")
    parser.add_argument("--dry-run", action="store_true", help="Show queries without calling API")
    args = parser.parse_args()

    if args.dry_run:
        for q in SEED_QUERIES:
            print(f"  [{cache_key(q)}] {q[:70]}...")
        print(f"\n{len(SEED_QUERIES)} queries would be seeded.")
        return

    seeds = []
    for i, query in enumerate(SEED_QUERIES):
        key = cache_key(query)
        print(f"[{i+1}/{len(SEED_QUERIES)}] {query[:60]}...", flush=True)

        result = query_intelligence(args.api_url, query)
        if result and result.get("response"):
            seeds.append({
                "key": key,
                "query": query,
                "response": result["response"],
                "tool_calls": result.get("tool_calls", []),
                "queries": [tc.get("input", {}).get("query", "") for tc in result.get("tool_calls", []) if tc.get("name") == "query_database"],
            })
            print(f"  OK ({len(result['response'])} chars)", flush=True)
        else:
            print(f"  FAILED — skipping", flush=True)

        # Brief pause between queries
        if i < len(SEED_QUERIES) - 1:
            time.sleep(2)

    with open(args.output, "w") as f:
        json.dump(seeds, f, indent=2)

    print(f"\nWrote {len(seeds)} cache seeds to {args.output}")


if __name__ == "__main__":
    main()
