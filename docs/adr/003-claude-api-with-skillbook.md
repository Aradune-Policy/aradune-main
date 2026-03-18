# ADR 003: Claude API with Self-Improving Skillbook over Fine-Tuning

## Status
Accepted (2025-03)

## Context
Aradune's Intelligence layer needs to improve at answering Medicaid questions over time. The options are: (1) fine-tune a model on Medicaid data, (2) use RAG only, (3) use the Claude API with a persistent knowledge layer (Skillbook) that learns from every query.

Fine-tuning requires training data, ongoing retraining, model hosting, and loses access to Claude's general reasoning. RAG alone is stateless; the same mistake repeats forever.

## Decision
Use the Claude API (Sonnet for analysis, Opus for complex reasoning, Haiku for classification and reflection) with a Skillbook layer inspired by the ACE framework (ICLR 2026). The Skillbook stores validated domain insights (strategies, caveats, failure modes, domain rules, query patterns) in a DuckDB table. Skills are retrieved by domain + BM25 text match and injected into the system prompt. An async Haiku Reflector extracts 0-3 skills per query. User feedback (thumbs up/down) adjusts skill scores. Score decay with 30-day half-life ensures stale skills fade. CRUSP lifecycle (Create, Read, Update, Split, Prune) manages skill quality.

## Consequences
- Positive: No model training or hosting. Intelligence improves with every query at ~$0.005/query incremental cost.
- Positive: Skills are auditable, exportable, and versionable. Every skill has provenance, score history, and a trace back to the query that generated it.
- Positive: The Skillbook is a competitive moat. Replicating the data lake costs ~$500K-$1M. Replicating the Skillbook requires the query volume.
- Negative: Dependent on Anthropic API availability (~99.57% uptime). Mitigated with response caching and circuit breakers.
- Negative: Prompt injection risk via Skillbook poisoning. Mitigated by adversarial testing (7-agent suite) and contamination scanning.
