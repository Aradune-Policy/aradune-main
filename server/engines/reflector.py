"""
Aradune Reflector
Async post-response analysis that extracts skills from Intelligence traces.

Runs AFTER the SSE response is complete. Never blocks user response.
Uses Haiku for cost efficiency (~$0.004 per reflection).
"""

import asyncio
import json
import logging
from typing import Optional

logger = logging.getLogger("aradune.reflector")

REFLECTOR_PROMPT = """You are Aradune's Reflector. You analyze completed Medicaid intelligence queries to extract reusable domain knowledge.

Given:
- The user's original query
- The domain classification
- The SQL queries executed
- The final response text
- Any feedback signal (thumbs up/down, user correction, or none)

Your job: identify 0-3 reusable insights that would help answer SIMILAR future queries better.

For each insight, output JSON:
{
  "skills": [
    {
      "category": "strategy|caveat|failure_mode|domain_rule|query_pattern",
      "content": "1-3 sentence insight, specific and actionable",
      "provenance": "CFR section, CMS doc, or data source if applicable",
      "confidence": "high|medium|low"
    }
  ],
  "skill_updates": [
    {
      "skill_id": "existing skill ID",
      "helpful": true
    }
  ],
  "proposed_links": [
    {
      "skill_id_a": "existing skill ID",
      "skill_id_b": "existing skill ID",
      "reason": "why these skills are related"
    }
  ],
  "split_candidates": [
    {
      "skill_id": "existing skill ID with overly broad content",
      "reason": "why this skill should be split into more focused pieces"
    }
  ]
}

Rules:
- Only extract insights that generalize beyond this specific query
- "Florida's enrollment is 5.2M" is NOT a skill (it's a data point)
- "T-MSIS encounter amounts for managed care states undercount actual rates by 15-40%" IS a skill
- If the response was wrong and you know why, that's a failure_mode
- If the response used a clever join or calculation pattern, that's a query_pattern
- If there's no generalizable insight, return {"skills": [], "skill_updates": [], "proposed_links": [], "split_candidates": []}
- Be conservative. 0 skills is better than a bad skill.
- proposed_links: suggest linking two skills that are thematically related or commonly co-retrieved
- split_candidates: flag skills whose content is too broad (>500 chars) or covers multiple distinct concepts
"""


async def reflect_on_response(
    query: str,
    domain: str,
    sql_traces: list[str],
    response_text: str,
    feedback: Optional[str] = None,
    retrieved_skill_ids: list[str] = None,
):
    """
    Async reflection after Intelligence response.
    Called via asyncio.create_task() so it never blocks.
    """
    try:
        from anthropic import AsyncAnthropic
        from server.engines.skillbook import add_skill, update_score, link_skills

        client = AsyncAnthropic()

        sql_summary = "\n".join(sql_traces[:5])
        response_excerpt = response_text[:2000]

        feedback_text = "No explicit feedback yet."
        if feedback == "positive":
            feedback_text = "User gave thumbs up (positive signal)."
        elif feedback == "negative":
            feedback_text = "User gave thumbs down (negative signal)."
        elif feedback and feedback.startswith("correction:"):
            feedback_text = f"User corrected the response: {feedback[11:]}"

        user_msg = f"""Query: {query}
Domain: {domain}
SQL executed:
{sql_summary}

Response excerpt:
{response_excerpt}

Feedback: {feedback_text}

Retrieved skill IDs: {retrieved_skill_ids or 'none'}"""

        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=REFLECTOR_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )

        result = json.loads(response.content[0].text)

        # Process new skills
        for skill in result.get("skills", []):
            if skill.get("confidence") in ("high", "medium"):
                add_skill(
                    domain=domain,
                    category=skill["category"],
                    content=skill["content"],
                    source_type="reflection",
                    source_query=query[:500],
                    provenance=skill.get("provenance"),
                )
                logger.info(f"Skillbook: added {skill['category']} skill from reflection")

        # Process score updates
        for update in result.get("skill_updates", []):
            update_score(update["skill_id"], update.get("helpful", True))

        # Process proposed links
        for link in result.get("proposed_links", []):
            sid_a = link.get("skill_id_a")
            sid_b = link.get("skill_id_b")
            if sid_a and sid_b:
                link_skills(sid_a, sid_b)
                logger.info(f"Reflector linked {sid_a} <-> {sid_b}: {link.get('reason', '')}")

        # Log split candidates (no auto-action, flagged for review)
        for sc in result.get("split_candidates", []):
            logger.info(f"Reflector split candidate: skill_id={sc.get('skill_id')}, reason={sc.get('reason', '')}")

        # Bulk feedback processing
        if feedback == "negative" and retrieved_skill_ids:
            for sid in retrieved_skill_ids:
                update_score(sid, helpful=False)

        if feedback == "positive" and retrieved_skill_ids:
            for sid in retrieved_skill_ids:
                update_score(sid, helpful=True)

    except Exception as e:
        logger.warning(f"Reflection failed (non-blocking): {e}")
