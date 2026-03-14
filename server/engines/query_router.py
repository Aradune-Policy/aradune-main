"""
Query Router: classify Intelligence questions into 4 tiers for resource allocation.

Uses Claude Haiku for fast (~100ms), cheap (~$0.001) classification.
Falls back to keyword heuristics if Haiku is unavailable.

Tiers:
  1 - Lookup:     Simple fact retrieval. 1-2 queries. Sonnet, no thinking.
  2 - Comparison:  Multi-entity comparison. 1-4 queries. Sonnet, no thinking.
  3 - Analysis:   Multi-step analysis. 3-8 queries. Sonnet, 5K thinking budget.
  4 - Synthesis:  Complex cross-domain synthesis. 5-15 queries. Sonnet/Opus, 10K thinking.

Bump-up rules:
  - User data present -> minimum Tier 3
  - Compliance terms detected -> Tier 4
  - System always errs up (borderline medium -> high)
"""

import os
import re
from dataclasses import dataclass

import anthropic


@dataclass
class RouteResult:
    tier: int
    model: str
    thinking_budget: int
    max_queries: int
    label: str


# Tier definitions
TIERS = {
    1: RouteResult(tier=1, model="claude-sonnet-4-6", thinking_budget=0, max_queries=2, label="Lookup"),
    2: RouteResult(tier=2, model="claude-sonnet-4-6", thinking_budget=0, max_queries=4, label="Comparison"),
    3: RouteResult(tier=3, model="claude-sonnet-4-6", thinking_budget=5000, max_queries=12, label="Analysis"),
    4: RouteResult(tier=4, model="claude-opus-4-6", thinking_budget=10000, max_queries=15, label="Synthesis"),
}

# Keywords that force Tier 4 (compliance/regulatory)
_COMPLIANCE_TERMS = re.compile(
    r"\b(cpra|42\s*cfr|447\.203|spa\b|state plan amendment|mcpar|"
    r"core set submission|rate transparency|compliance|regulatory|"
    r"ahead readiness|fiscal impact|draft.*language|submission.ready)\b",
    re.IGNORECASE,
)

# Keywords that suggest at least Tier 3
_ANALYSIS_TERMS = re.compile(
    r"\b(compare.*across|trend|forecast|correlation|impact|"
    r"what.?s driving|explain why|cross.reference|relationship between|"
    r"how does.*relate|scenario|model|project|comprehensive)\b",
    re.IGNORECASE,
)

_CLASSIFICATION_PROMPT = """Classify this Medicaid data question into exactly one tier:

1 = Simple lookup (single fact, one table, one state)
2 = Comparison (multiple states/codes, ranking, top/bottom)
3 = Analysis (multi-step, cross-domain, trends, why questions)
4 = Synthesis (complex regulatory, fiscal impact, multi-domain with policy context)

Question: {question}

Reply with ONLY the tier number (1, 2, 3, or 4). Nothing else."""


def _classify_with_haiku(question: str) -> int | None:
    """Use Haiku to classify the question tier. Returns None on failure."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{
                "role": "user",
                "content": _CLASSIFICATION_PROMPT.format(question=question),
            }],
        )
        text = response.content[0].text.strip()
        tier = int(text[0])  # Take first digit
        if tier in (1, 2, 3, 4):
            return tier
    except Exception:
        pass
    return None


def _classify_with_heuristics(question: str) -> int:
    """Keyword-based fallback classification."""
    q = question.lower()

    if _COMPLIANCE_TERMS.search(question):
        return 4

    if _ANALYSIS_TERMS.search(question):
        return 3

    # Count complexity signals
    signals = 0
    if " vs " in q or " versus " in q or "compare" in q:
        signals += 1
    if "all states" in q or "every state" in q or "50 state" in q:
        signals += 1
    if " and " in q and ("rate" in q or "enroll" in q or "spend" in q):
        signals += 1
    if "?" in question and len(question) > 100:
        signals += 1
    if any(w in q for w in ("why", "explain", "how does", "what caused")):
        signals += 1

    if signals >= 3:
        return 3
    if signals >= 1:
        return 2
    return 1


def classify_query(
    question: str,
    has_user_data: bool = False,
    use_haiku: bool = True,
) -> RouteResult:
    """
    Classify a question and return routing parameters.

    Always errs up: Haiku classification is treated as floor,
    heuristic bump-ups can only increase the tier.
    """
    # Start with heuristic classification
    heuristic_tier = _classify_with_heuristics(question)

    # Try Haiku classification (fast, ~100ms)
    haiku_tier = None
    if use_haiku:
        haiku_tier = _classify_with_haiku(question)

    # Take the higher of the two (always err up)
    if haiku_tier is not None:
        tier = max(haiku_tier, heuristic_tier)
    else:
        tier = heuristic_tier

    # Bump-up rules
    if has_user_data and tier < 3:
        tier = 3
    if _COMPLIANCE_TERMS.search(question) and tier < 4:
        tier = 4

    return TIERS[tier]
