"""
Aradune Skillbook Engine
Persistent, self-curating domain intelligence layer.

Pattern: ACE (ICLR 2026) adapted for Medicaid regulatory analytics.
Skills are structured bullets with metadata, retrieved by domain + BM25
and injected into the Intelligence system prompt.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional
from server.db import get_cursor

logger = logging.getLogger("aradune.skillbook")

# Table name
TABLE = "fact_skillbook"

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def ensure_table():
    """Create the skillbook table if it doesn't exist."""
    try:
        with get_cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    skill_id        VARCHAR PRIMARY KEY,
                    domain          VARCHAR NOT NULL,
                    category        VARCHAR NOT NULL,
                    content         VARCHAR NOT NULL,
                    source_type     VARCHAR,
                    source_query    VARCHAR,
                    source_trace    VARCHAR,
                    provenance      VARCHAR,
                    helpful_count   INTEGER DEFAULT 0,
                    harmful_count   INTEGER DEFAULT 0,
                    times_retrieved INTEGER DEFAULT 0,
                    created_at      VARCHAR DEFAULT (strftime(current_timestamp, '%Y-%m-%d %H:%M:%S')),
                    updated_at      VARCHAR DEFAULT (strftime(current_timestamp, '%Y-%m-%d %H:%M:%S')),
                    superseded_by   VARCHAR,
                    active          BOOLEAN DEFAULT true
                )
            """)
            logger.info("Skillbook table ensured")
    except Exception as e:
        logger.warning(f"Skillbook table creation: {e}")


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

def retrieve_skills(
    domain: str,
    query: str,
    max_skills: int = 10,
    min_score: int = -2,
) -> list[dict]:
    """
    Retrieve relevant skills for injection into the Intelligence prompt.

    Two-stage retrieval:
    1. Domain-filtered domain_rules (always included for the domain)
    2. Keyword match against query for other skill types
    """
    try:
        with get_cursor() as cur:
            # Check if table exists and has data
            try:
                count = cur.execute(f"SELECT COUNT(*) FROM {TABLE} WHERE active = true").fetchone()[0]
                if count == 0:
                    return []
            except Exception:
                return []

            # Stage 1: Domain rules always included
            domain_rules = cur.execute(f"""
                SELECT skill_id, domain, category, content, provenance,
                       helpful_count, harmful_count,
                       helpful_count - harmful_count AS net_score
                FROM {TABLE}
                WHERE domain = $1 AND category = 'domain_rule' AND active = true
                      AND helpful_count - harmful_count >= $2
                ORDER BY helpful_count - harmful_count DESC
                LIMIT $3
            """, [domain, min_score, max_skills // 2]).fetchall()

            # Stage 2: Keyword match for query-relevant skills
            # Simple ILIKE matching (DuckDB FTS requires PRAGMA setup)
            keywords = [w.strip().lower() for w in query.split() if len(w.strip()) > 3][:5]
            relevant = []
            if keywords:
                like_clauses = " OR ".join([f"LOWER(content) LIKE '%{k}%'" for k in keywords])
                relevant = cur.execute(f"""
                    SELECT skill_id, domain, category, content, provenance,
                           helpful_count, harmful_count,
                           helpful_count - harmful_count AS net_score
                    FROM {TABLE}
                    WHERE active = true
                          AND helpful_count - harmful_count >= $1
                          AND category != 'domain_rule'
                          AND ({like_clauses})
                    ORDER BY helpful_count - harmful_count DESC
                    LIMIT $2
                """, [min_score, max_skills - len(domain_rules)]).fetchall()

            # Also get top general skills if we have room
            remaining = max_skills - len(domain_rules) - len(relevant)
            general = []
            if remaining > 0:
                existing_ids = [r[0] for r in domain_rules] + [r[0] for r in relevant]
                if existing_ids:
                    placeholders = ",".join([f"'{sid}'" for sid in existing_ids])
                    general = cur.execute(f"""
                        SELECT skill_id, domain, category, content, provenance,
                               helpful_count, harmful_count,
                               helpful_count - harmful_count AS net_score
                        FROM {TABLE}
                        WHERE active = true
                              AND helpful_count - harmful_count >= $1
                              AND skill_id NOT IN ({placeholders})
                        ORDER BY helpful_count - harmful_count DESC, times_retrieved DESC
                        LIMIT $2
                    """, [min_score, remaining]).fetchall()
                else:
                    general = cur.execute(f"""
                        SELECT skill_id, domain, category, content, provenance,
                               helpful_count, harmful_count,
                               helpful_count - harmful_count AS net_score
                        FROM {TABLE}
                        WHERE active = true AND helpful_count - harmful_count >= $1
                        ORDER BY helpful_count - harmful_count DESC
                        LIMIT $2
                    """, [min_score, remaining]).fetchall()

            all_skills = list(domain_rules) + list(relevant) + list(general)

            # Increment retrieval counter
            skill_ids = [s[0] for s in all_skills]
            if skill_ids:
                for sid in skill_ids:
                    try:
                        cur.execute(f"""
                            UPDATE {TABLE}
                            SET times_retrieved = times_retrieved + 1
                            WHERE skill_id = '{sid}'
                        """)
                    except Exception:
                        pass

            return [_row_to_dict(s) for s in all_skills]
    except Exception as e:
        logger.warning(f"Skill retrieval failed: {e}")
        return []


def format_skills_for_prompt(skills: list[dict]) -> str:
    """Format retrieved skills as a prompt section."""
    if not skills:
        return ""

    lines = ["\n\n## Learned Domain Intelligence (Skillbook)\n"]
    lines.append("These are validated insights from prior analyses. Apply them when relevant.\n")

    for s in skills:
        prefix = {
            "strategy": "STRATEGY",
            "caveat": "CAUTION",
            "failure_mode": "AVOID",
            "domain_rule": "RULE",
            "query_pattern": "PATTERN",
        }.get(s["category"], "NOTE")

        score_note = ""
        if s.get("helpful_count", 0) > 3:
            score_note = f" [validated {s['helpful_count']}x]"

        provenance = f" (Source: {s['provenance']})" if s.get("provenance") else ""
        lines.append(f"- **{prefix}:** {s['content']}{provenance}{score_note}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_skill(
    domain: str,
    category: str,
    content: str,
    source_type: str = "manual",
    source_query: str = None,
    source_trace: str = None,
    provenance: str = None,
) -> str:
    """Add a new skill to the skillbook. Returns skill_id."""
    try:
        with get_cursor() as cur:
            skill_id = str(uuid.uuid4())[:12]
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""
                INSERT INTO {TABLE}
                (skill_id, domain, category, content, source_type,
                 source_query, source_trace, provenance, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, [skill_id, domain, category, content, source_type,
                  source_query, source_trace, provenance, now, now])
            logger.info(f"Skillbook: added {category} skill in {domain}")
            return skill_id
    except Exception as e:
        logger.warning(f"Failed to add skill: {e}")
        return ""


def update_score(skill_id: str, helpful: bool):
    """Increment helpful or harmful counter."""
    try:
        with get_cursor() as cur:
            col = "helpful_count" if helpful else "harmful_count"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""
                UPDATE {TABLE}
                SET {col} = {col} + 1, updated_at = '{now}'
                WHERE skill_id = $1
            """, [skill_id])
    except Exception as e:
        logger.warning(f"Failed to update score: {e}")


def retire_skill(skill_id: str, superseded_by: str = None):
    """Retire a skill (soft delete)."""
    try:
        with get_cursor() as cur:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""
                UPDATE {TABLE}
                SET active = false, superseded_by = $1, updated_at = '{now}'
                WHERE skill_id = $2
            """, [superseded_by, skill_id])
    except Exception as e:
        logger.warning(f"Failed to retire skill: {e}")


def get_all_skills(active_only: bool = True, limit: int = 100) -> list[dict]:
    """Get all skills for admin view."""
    try:
        with get_cursor() as cur:
            where = "WHERE active = true" if active_only else ""
            rows = cur.execute(f"""
                SELECT skill_id, domain, category, content, provenance,
                       helpful_count, harmful_count,
                       helpful_count - harmful_count AS net_score,
                       times_retrieved, created_at, active
                FROM {TABLE} {where}
                ORDER BY helpful_count - harmful_count DESC
                LIMIT $1
            """, [limit]).fetchall()
            return [_row_to_dict_full(r) for r in rows]
    except Exception:
        return []


def _row_to_dict(row) -> dict:
    cols = ["skill_id", "domain", "category", "content", "provenance",
            "helpful_count", "harmful_count", "net_score"]
    return dict(zip(cols, row[:len(cols)]))


def _row_to_dict_full(row) -> dict:
    cols = ["skill_id", "domain", "category", "content", "provenance",
            "helpful_count", "harmful_count", "net_score",
            "times_retrieved", "created_at", "active"]
    return dict(zip(cols, row[:len(cols)]))
