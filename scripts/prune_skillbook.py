"""
CRUSP Prune Job for Aradune Skillbook v2

Prune rules:
1. net_score < -2 for 14+ days -> retire (harmful)
2. effective_score < 0.5 AND times_retrieved == 0 for 60+ days -> retire (unused + decayed)
3. content > 500 chars -> flag for split (not retired, just logged)

Usage:
    python -m scripts.prune_skillbook              # dry run (default)
    python -m scripts.prune_skillbook --apply       # actually prune
"""

import argparse
import sys
import logging
from datetime import datetime

# Add project root to path for imports
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.db import get_cursor
from server.engines.skillbook import TABLE, effective_score, retire_skill

logger = logging.getLogger("aradune.prune_skillbook")

HARMFUL_THRESHOLD = -2
HARMFUL_AGE_DAYS = 14
DECAY_SCORE_THRESHOLD = 0.5
UNUSED_AGE_DAYS = 60
SPLIT_CHAR_THRESHOLD = 500


def prune_skillbook(dry_run: bool = True):
    """Run the CRUSP prune job."""
    now = datetime.now()
    summary = {
        "scanned": 0,
        "pruned_harmful": 0,
        "pruned_decayed": 0,
        "flagged_split": 0,
        "skipped_seed": 0,
    }

    try:
        with get_cursor() as cur:
            rows = cur.execute(f"""
                SELECT skill_id, domain, category, content, provenance,
                       helpful_count, harmful_count,
                       helpful_count - harmful_count AS net_score,
                       times_retrieved, created_at, source_type,
                       last_validated_at, decay_half_life_days
                FROM {TABLE}
                WHERE active = true
            """).fetchall()

            summary["scanned"] = len(rows)

            for row in rows:
                skill_id = row[0]
                content = row[3]
                net_score = row[7] if row[7] is not None else 0
                times_retrieved = row[8] if row[8] is not None else 0
                created_at_str = row[9]
                source_type = row[10]
                last_validated_at = row[11]
                half_life = row[12] if row[12] else 30

                # Skip seed skills (manual seeds should not be auto-pruned)
                if source_type == "seed":
                    summary["skipped_seed"] += 1
                    continue

                # Parse created_at for age calculation
                try:
                    created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    created_at = now  # can't parse, treat as new

                age_days = (now - created_at).total_seconds() / 86400.0

                # Rule 1: net_score < -2 for 14+ days
                if net_score < HARMFUL_THRESHOLD and age_days >= HARMFUL_AGE_DAYS:
                    reason = f"Harmful: net_score={net_score}, age={age_days:.0f}d"
                    print(f"  PRUNE (harmful): {skill_id} | {reason} | {content[:80]}...")
                    if not dry_run:
                        retire_skill(skill_id, reason=reason)
                    summary["pruned_harmful"] += 1
                    continue

                # Rule 2: effective_score < 0.5 AND times_retrieved == 0 for 60+ days
                eff = effective_score(net_score, last_validated_at, half_life)
                if eff < DECAY_SCORE_THRESHOLD and times_retrieved == 0 and age_days >= UNUSED_AGE_DAYS:
                    reason = f"Decayed+unused: eff_score={eff:.3f}, retrieved=0, age={age_days:.0f}d"
                    print(f"  PRUNE (decayed): {skill_id} | {reason} | {content[:80]}...")
                    if not dry_run:
                        retire_skill(skill_id, reason=reason)
                    summary["pruned_decayed"] += 1
                    continue

                # Rule 3: content > 500 chars -> flag for split (not pruned)
                if len(content) > SPLIT_CHAR_THRESHOLD:
                    print(f"  SPLIT candidate: {skill_id} | len={len(content)} | {content[:80]}...")
                    summary["flagged_split"] += 1

    except Exception as e:
        print(f"ERROR: Prune job failed: {e}")
        return summary

    # Print summary
    mode = "DRY RUN" if dry_run else "APPLIED"
    print(f"\n--- Skillbook Prune Summary ({mode}) ---")
    print(f"  Scanned:         {summary['scanned']}")
    print(f"  Skipped (seed):  {summary['skipped_seed']}")
    print(f"  Pruned harmful:  {summary['pruned_harmful']}")
    print(f"  Pruned decayed:  {summary['pruned_decayed']}")
    print(f"  Flagged split:   {summary['flagged_split']}")
    total_pruned = summary["pruned_harmful"] + summary["pruned_decayed"]
    print(f"  Total pruned:    {total_pruned}")
    if dry_run and total_pruned > 0:
        print(f"\n  To apply: python -m scripts.prune_skillbook --apply")

    return summary


def main():
    parser = argparse.ArgumentParser(description="CRUSP Prune Job for Aradune Skillbook")
    parser.add_argument("--apply", action="store_true", help="Actually prune (default is dry run)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    prune_skillbook(dry_run=not args.apply)


if __name__ == "__main__":
    main()
