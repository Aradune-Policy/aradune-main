"""
Tests for the query router's heuristic classification.
Does not test Haiku API calls (those require a live API key).
"""

import sys
from pathlib import Path

# Add project root to path for server imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from server.engines.query_router import classify_query, _classify_with_heuristics


class TestHeuristicClassification:
    """Test keyword-based fallback classification."""

    def test_simple_lookup(self):
        assert _classify_with_heuristics("What is Florida's FMAP?") == 1

    def test_comparison_vs(self):
        assert _classify_with_heuristics("Compare FL vs GA Medicaid rates") >= 2

    def test_comparison_all_states(self):
        assert _classify_with_heuristics("Show all states ranked by enrollment") >= 2

    def test_analysis_trend(self):
        assert _classify_with_heuristics("What's driving enrollment growth in the Southeast?") >= 3

    def test_analysis_cross_reference(self):
        assert _classify_with_heuristics(
            "How does rate adequacy correlate with provider participation?"
        ) >= 3

    def test_synthesis_compliance(self):
        assert _classify_with_heuristics("Generate a CPRA for Florida") == 4

    def test_synthesis_regulatory(self):
        assert _classify_with_heuristics(
            "What does 42 CFR 447.203 require for rate transparency?"
        ) == 4

    def test_synthesis_spa(self):
        assert _classify_with_heuristics("Draft SPA language for rate increase") == 4


class TestClassifyQuery:
    """Test the full classify_query function (without Haiku)."""

    def test_basic_routing(self):
        result = classify_query("What is Florida's FMAP?", use_haiku=False)
        assert result.tier in (1, 2)
        assert result.thinking_budget == 0

    def test_user_data_bumps_to_tier_3(self):
        result = classify_query(
            "What is Florida's FMAP?",
            has_user_data=True,
            use_haiku=False,
        )
        assert result.tier >= 3
        assert result.thinking_budget >= 5000

    def test_compliance_forces_tier_4(self):
        result = classify_query(
            "Generate a CPRA compliance report",
            use_haiku=False,
        )
        assert result.tier == 4
        assert result.thinking_budget == 10000

    def test_returns_route_result(self):
        result = classify_query("Hello", use_haiku=False)
        assert hasattr(result, "tier")
        assert hasattr(result, "model")
        assert hasattr(result, "thinking_budget")
        assert hasattr(result, "max_queries")
        assert hasattr(result, "label")
