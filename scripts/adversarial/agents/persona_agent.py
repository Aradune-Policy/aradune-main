"""
Consulting Firm Persona Agent

Simulates the exact questions a Big 5 consulting firm's Medicaid practice
leader, senior analyst, and technical architect would ask during an evaluation.

Unlike the other agents (which test for failures), this one tests for
QUALITY -- are the answers good enough to impress a domain expert?
"""

import json
import time
import logging
from anthropic import Anthropic
from scripts.adversarial.agents.intelligence_agent import call_intelligence
from scripts.adversarial.config import SONNET_MODEL, LATENCY_WARNING_S

logger = logging.getLogger("adversarial.persona")
client = Anthropic()


PERSONA_QUERIES = [
    # --- Practice Leader (business value, competitive positioning) ---
    {
        "persona": "practice_leader",
        "query": "We're advising Ohio on their CPRA submission due July 2026. What does Ohio's Medicaid-to-Medicare rate comparison look like across the three required E&M categories?",
        "quality_bar": "Should produce specific percentages for Primary Care, MH/SUD, and OB/GYN E&M categories for Ohio. Should cite the correct conversion factor ($32.3465) and note it uses non-facility Medicare rates. Should be specific enough to start a client conversation.",
    },
    {
        "persona": "practice_leader",
        "query": "A state Medicaid director asks: what would a 5% across-the-board rate increase cost us, and what's our federal match? Walk me through the fiscal impact for Florida.",
        "quality_bar": "Should connect rate increase to CMS-64 expenditure baseline, apply FMAP (~0.6175 for FL), estimate state vs federal share. Should mention UPL headroom and OBBBA SDP caps. Should feel like a senior analyst's work product.",
    },
    {
        "persona": "practice_leader",
        "query": "Which states are most vulnerable to a Medicaid fiscal cliff in the next 2-3 years? Give me a data-driven answer.",
        "quality_bar": "Should cross-reference enrollment growth, spending per enrollee trends, FMAP, state fiscal capacity. Should name specific states with reasoning. Should cite data vintages.",
    },

    # --- Senior Analyst (technical depth, data quality awareness) ---
    {
        "persona": "senior_analyst",
        "query": "I need to compare nursing home quality between for-profit and nonprofit facilities. What does your data show and how reliable is it?",
        "quality_bar": "Should reference Five Star data (14,710 facilities), show the ownership quality gap, cite the PSM analysis (-0.67 stars). Should proactively note data limitations (self-reported staffing, deficiency survey frequency varies by state).",
    },
    {
        "persona": "senior_analyst",
        "query": "Walk me through how you calculate per-enrollee Medicaid spending by state. What's the denominator? What's the numerator? What are the gotchas?",
        "quality_bar": "Must explain: numerator is CMS-64 total computable, denominator is enrollment EXCLUDING CHIP. Must note CMS-64 vs T-MSIS will never reconcile. Must cite fiscal year. This is a methodology credibility test.",
    },
    {
        "persona": "senior_analyst",
        "query": "How do you handle states with high managed care penetration in your rate comparison? Texas is 90%+ managed care -- how meaningful is their FFS fee schedule?",
        "quality_bar": "Should acknowledge that FFS schedules in high-MC states cover a small slice of spending. Should explain T-MSIS encounter limitations for MC rates. Should note which states' rates are published vs computed. Demonstrates nuanced data understanding.",
    },

    # --- Technical Architect (system design, scalability, trust) ---
    {
        "persona": "technical_architect",
        "query": "Your data lake has 750+ tables. How do you ensure data quality across all of them? What happens when a source changes its schema?",
        "quality_bar": "Should explain the validation framework (15+ checks, 3 types, API endpoints). Should mention ETL hard stops (>90% rate change, >20% code count drop, schema mismatch). Should be honest about what's automated vs Phase 2. This is the validation layer credibility test.",
    },
    {
        "persona": "technical_architect",
        "query": "If I ask you the same question twice, will I get the same numbers? How do you ensure deterministic results?",
        "quality_bar": "Should reference named metrics with deterministic formulas (ontology/metrics/). Should explain that the same query against the same data vintage produces identical SQL. Should note that data refreshes can change results and that's by design (with vintage tracking).",
    },

    # --- Demo flow queries (Act 1, 2, 3 from the build guide) ---
    {
        "persona": "demo_act1",
        "query": "Compare Florida's E&M rates to the Southeast average and Medicare. Show me where Florida is paying well below Medicare and what the fiscal impact of closing those gaps would be.",
        "quality_bar": "This is the Act 1 demo opener. Must produce a table with specific FL rates vs Medicare percentages. Must cross-reference rate data with fiscal/FMAP data. Must be presentation-ready with narrative, tables, and data citations. Latency under 30s strongly preferred.",
    },
    {
        "persona": "demo_act3",
        "query": "Give me a comprehensive profile of Georgia's Medicaid program -- enrollment, spending, rates, quality, workforce, and the key risks.",
        "quality_bar": "Should produce a multi-section analysis touching at least 4 of: enrollment trends, spending per enrollee, rate competitiveness, quality measures, workforce shortages, HCBS waitlists. Should cite multiple data sources. This is the State Profile Intelligence test.",
    },
]


QUALITY_EVALUATOR_PROMPT = """You are a senior Medicaid consulting professional evaluating an AI analytics platform.

The evaluator persona is: {persona}

The question asked was: {query}

The system responded with:
---
{response}
---

The quality bar for this response is:
{quality_bar}

Rate this response on a 1-5 scale:
5 = Exceptional. Would impress a senior Medicaid professional. Specific, accurate, properly caveated.
4 = Strong. Correct and useful. Minor gaps but overall trustworthy.
3 = Adequate. Gets the basics right but missing important nuance or specificity.
2 = Weak. Some correct information but significant gaps, wrong numbers, or missing critical caveats.
1 = Failure. Wrong, misleading, or so generic it adds no value over Google.

Also note these style rules (deduct from score if violated):
- Em-dashes (\u2014), en-dashes (\u2013), and double hyphens (--) used as clause-connecting punctuation are all prohibited. Hyphens in compound words are fine.
- "plain English" or "in plain English" is prohibited
- AI filler phrases ("It's worth noting", "Great question", "Let me help you") are prohibited
- Key numbers should include a data vintage (year/FY)

Respond with JSON:
{{
    "score": 1-5,
    "reasoning": "2-3 sentences explaining the rating",
    "strengths": ["list", "of", "strengths"],
    "weaknesses": ["list", "of", "weaknesses"],
    "would_impress_consultant": true/false,
    "style_violations": ["list of style violations, empty if none"]
}}"""


class PersonaAgent:
    """Simulates consulting firm evaluation scenarios."""

    def run(self) -> dict:
        results = []
        total_score = 0
        impressive_count = 0
        slow_count = 0

        for i, q in enumerate(PERSONA_QUERIES):
            logger.info(f"[{i+1}/{len(PERSONA_QUERIES)}] [{q['persona']}] {q['query'][:60]}...")

            intel_result = call_intelligence(q["query"])

            if intel_result.get("slow"):
                slow_count += 1

            if intel_result["error"]:
                results.append({
                    **q,
                    "score": 0,
                    "reasoning": f"Endpoint error: {intel_result['error']}",
                    "would_impress_consultant": False,
                    "latency_s": intel_result["latency_s"],
                    "slow": intel_result.get("slow", False),
                    "style_violations": [],
                })
                continue

            # Evaluate quality
            try:
                eval_response = client.messages.create(
                    model=SONNET_MODEL,
                    max_tokens=800,
                    messages=[{
                        "role": "user",
                        "content": QUALITY_EVALUATOR_PROMPT.format(
                            persona=q["persona"],
                            query=q["query"],
                            response=intel_result["response_text"][:4000],
                            quality_bar=q["quality_bar"],
                        ),
                    }],
                )
                text = eval_response.content[0].text
                text = text.replace("```json", "").replace("```", "").strip()
                evaluation = json.loads(text)
            except Exception as e:
                evaluation = {
                    "score": 0,
                    "reasoning": f"Evaluation error: {e}",
                    "would_impress_consultant": False,
                    "style_violations": [],
                }

            score = evaluation.get("score", 0)
            total_score += score
            if evaluation.get("would_impress_consultant"):
                impressive_count += 1

            results.append({
                **q,
                "score": score,
                "reasoning": evaluation.get("reasoning", ""),
                "strengths": evaluation.get("strengths", []),
                "weaknesses": evaluation.get("weaknesses", []),
                "would_impress_consultant": evaluation.get("would_impress_consultant", False),
                "style_violations": evaluation.get("style_violations", []),
                "latency_s": intel_result["latency_s"],
                "slow": intel_result.get("slow", False),
                "response_excerpt": intel_result["response_text"][:500],
            })

            time.sleep(2)

        n = len(PERSONA_QUERIES)
        return {
            "agent": "persona",
            "total": n,
            "avg_score": round(total_score / n, 1) if n else 0,
            "impressive_count": impressive_count,
            "impressive_rate": f"{impressive_count / n * 100:.0f}%" if n else "N/A",
            "slow_responses": slow_count,
            "results": results,
        }
