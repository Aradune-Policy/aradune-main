"""
Aradune Skillbook Engine
Persistent, self-curating domain intelligence layer.

Pattern: ACE (ICLR 2026) adapted for Medicaid regulatory analytics.
Skills are structured bullets with metadata, retrieved by domain + BM25
and injected into the Intelligence system prompt.
"""

import uuid
import json
import logging
from datetime import datetime
from typing import Optional
from server.db import get_cursor

logger = logging.getLogger("aradune.skillbook")

# Table names
TABLE = "fact_skillbook"
TRACE_TABLE = "fact_intelligence_trace"

# ---------------------------------------------------------------------------
# Score Decay
# ---------------------------------------------------------------------------

def effective_score(net_score: float, last_validated_at: str = None, half_life_days: float = 30) -> float:
    """
    Compute decayed score: net_score * 2^(-days_elapsed / half_life_days).
    Negative scores don't get decay benefit (they stay negative).
    """
    if net_score <= 0:
        return float(net_score)
    if not last_validated_at:
        return float(net_score)
    try:
        validated = datetime.strptime(last_validated_at, "%Y-%m-%d %H:%M:%S")
        days_elapsed = (datetime.now() - validated).total_seconds() / 86400.0
        if days_elapsed < 0:
            days_elapsed = 0
        return net_score * pow(2, -(days_elapsed / half_life_days))
    except Exception:
        return float(net_score)

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def ensure_table():
    """Create the skillbook table and intelligence trace table if they don't exist."""
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
                    active          BOOLEAN DEFAULT true,
                    last_validated_at VARCHAR,
                    decay_half_life_days FLOAT DEFAULT 30,
                    related_skills  VARCHAR,
                    prune_reason    VARCHAR
                )
            """)

            # Migrate existing tables: add new columns if missing
            for col_def in [
                ("last_validated_at", "VARCHAR"),
                ("decay_half_life_days", "FLOAT DEFAULT 30"),
                ("related_skills", "VARCHAR"),
                ("prune_reason", "VARCHAR"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col_def[0]} {col_def[1]}")
                    logger.info(f"Added column {col_def[0]} to {TABLE}")
                except Exception:
                    pass  # Column already exists

            # Intelligence trace table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TRACE_TABLE} (
                    trace_id          VARCHAR PRIMARY KEY,
                    query_text        VARCHAR,
                    domain            VARCHAR,
                    tier              INTEGER,
                    skill_ids_retrieved VARCHAR,
                    sql_queries       VARCHAR,
                    model_used        VARCHAR,
                    response_length   INTEGER,
                    response_time_ms  INTEGER,
                    feedback          VARCHAR,
                    feedback_at       VARCHAR,
                    created_at        VARCHAR DEFAULT (strftime(current_timestamp, '%Y-%m-%d %H:%M:%S'))
                )
            """)

            logger.info("Skillbook + trace tables ensured")
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

    Three-stage retrieval:
    1. Domain-filtered domain_rules (always included for the domain)
    2. Keyword match against query for other skill types
    3. Graph expansion: 1-hop fetch via related_skills links

    Results sorted by effective_score (decay-adjusted) instead of raw net_score.
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
                       helpful_count - harmful_count AS net_score,
                       last_validated_at, decay_half_life_days
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
                           helpful_count - harmful_count AS net_score,
                           last_validated_at, decay_half_life_days
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
                               helpful_count - harmful_count AS net_score,
                               last_validated_at, decay_half_life_days
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
                               helpful_count - harmful_count AS net_score,
                               last_validated_at, decay_half_life_days
                        FROM {TABLE}
                        WHERE active = true AND helpful_count - harmful_count >= $1
                        ORDER BY helpful_count - harmful_count DESC
                        LIMIT $2
                    """, [min_score, remaining]).fetchall()

            all_skills = list(domain_rules) + list(relevant) + list(general)

            # Sort by effective_score (decay-adjusted) instead of raw net_score
            def _sort_key(row):
                net = row[7] if row[7] is not None else 0  # net_score
                lv = row[8]   # last_validated_at
                hl = row[9] if row[9] else 30  # decay_half_life_days
                return effective_score(net, lv, hl)
            all_skills.sort(key=_sort_key, reverse=True)

            # Convert to dicts (before graph expansion)
            retrieved_ids = set(s[0] for s in all_skills)
            result = [_row_to_dict(s) for s in all_skills]

            # Stage 3: Graph expansion - 1-hop via related_skills
            linked_ids = set()
            for s in all_skills:
                sid = s[0]
                try:
                    rel_row = cur.execute(f"""
                        SELECT related_skills FROM {TABLE}
                        WHERE skill_id = $1 AND related_skills IS NOT NULL
                    """, [sid]).fetchone()
                    if rel_row and rel_row[0]:
                        try:
                            links = json.loads(rel_row[0])
                            for link_id in links:
                                if link_id not in retrieved_ids:
                                    linked_ids.add(link_id)
                        except (json.JSONDecodeError, TypeError):
                            pass
                except Exception:
                    pass

            if linked_ids:
                placeholders = ",".join([f"'{lid}'" for lid in linked_ids])
                linked_rows = cur.execute(f"""
                    SELECT skill_id, domain, category, content, provenance,
                           helpful_count, harmful_count,
                           helpful_count - harmful_count AS net_score,
                           last_validated_at, decay_half_life_days
                    FROM {TABLE}
                    WHERE active = true AND skill_id IN ({placeholders})
                """).fetchall()
                for lr in linked_rows:
                    d = _row_to_dict(lr)
                    d["via_link"] = True
                    result.append(d)

            # Increment retrieval counter
            skill_ids = [s["skill_id"] for s in result]
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

            return result
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
    """Increment helpful or harmful counter. Resets decay clock on validation."""
    try:
        with get_cursor() as cur:
            col = "helpful_count" if helpful else "harmful_count"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""
                UPDATE {TABLE}
                SET {col} = {col} + 1, updated_at = '{now}', last_validated_at = '{now}'
                WHERE skill_id = $1
            """, [skill_id])
    except Exception as e:
        logger.warning(f"Failed to update score: {e}")


def retire_skill(skill_id: str, superseded_by: str = None, reason: str = None):
    """Retire a skill (soft delete) with optional prune reason."""
    try:
        with get_cursor() as cur:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(f"""
                UPDATE {TABLE}
                SET active = false, superseded_by = $1, updated_at = '{now}',
                    prune_reason = $2
                WHERE skill_id = $3
            """, [superseded_by, reason, skill_id])
    except Exception as e:
        logger.warning(f"Failed to retire skill: {e}")


def link_skills(skill_id_a: str, skill_id_b: str):
    """Bidirectional linking of two skills via their related_skills JSON arrays."""
    try:
        with get_cursor() as cur:
            for src, dst in [(skill_id_a, skill_id_b), (skill_id_b, skill_id_a)]:
                row = cur.execute(f"""
                    SELECT related_skills FROM {TABLE} WHERE skill_id = $1
                """, [src]).fetchone()
                if row is None:
                    continue
                existing = []
                if row[0]:
                    try:
                        existing = json.loads(row[0])
                    except (json.JSONDecodeError, TypeError):
                        existing = []
                if dst not in existing:
                    existing.append(dst)
                    cur.execute(f"""
                        UPDATE {TABLE}
                        SET related_skills = $1
                        WHERE skill_id = $2
                    """, [json.dumps(existing), src])
            logger.info(f"Linked skills {skill_id_a} <-> {skill_id_b}")
    except Exception as e:
        logger.warning(f"Failed to link skills: {e}")


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
            "helpful_count", "harmful_count", "net_score",
            "last_validated_at", "decay_half_life_days"]
    d = dict(zip(cols, row[:len(cols)]))
    # Compute effective_score for downstream consumers
    net = d.get("net_score", 0) or 0
    lv = d.get("last_validated_at")
    hl = d.get("decay_half_life_days") or 30
    d["effective_score"] = round(effective_score(net, lv, hl), 3)
    return d


def _row_to_dict_full(row) -> dict:
    cols = ["skill_id", "domain", "category", "content", "provenance",
            "helpful_count", "harmful_count", "net_score",
            "times_retrieved", "created_at", "active"]
    return dict(zip(cols, row[:len(cols)]))
