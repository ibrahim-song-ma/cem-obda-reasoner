# Repository Rules

This repository is an OBDA / ontology QA prototype built around:

- DuckDB
- RDFLib
- `owlrl`
- FastAPI
- the local `obda-query` skill

## Core Role

When working in this repo:

- treat ontology/schema accuracy as more important than fast guessing
- prefer ontology-driven querying over application-side hardcoding
- treat `reasoning_server.py` as a reasoning/query service, not a place to hide business logic

## Environment Rules

1. Always use `.venv`.
2. Do not use global `pip`.
3. Use local `duckdb`, not MySQL/Postgres unless the user explicitly asks.
4. Do not call external LLM APIs for text-to-SPARQL.

## Query Rules

For ontology/data questions, use the `obda-query` skill:

- skill file:
  `/Users/Song/code/cc/reasoner/.agents/skills/obda-query/SKILL.md`

Boundary rule:

- Keep the skill generic. Put repository-specific ontology conventions, naming quirks, and domain semantics in this repo file, not in the shared skill.

Mandatory protocol:

1. Fetch `/schema` first in the current turn.
2. If filtering by phone, ID, score, status, or similar attributes, verify the property's domain.
3. If schema alone is not enough, inspect `/sample/{class_name}` before finalizing SPARQL.
4. For root cause, "why", hidden relation, solution questions, or causal wording such as `因为 / 由于 / 导致`, do one analyzer request before the final answer.
5. Do not invent predicates from memory.
6. For a normal user question, keep the server query budget to roughly 3 round-trips unless earlier results are empty or ambiguous.
7. Do not start by probing a concrete entity with `SELECT ?p ?o`; prefer one structured query.
8. Do not present a strategy or solution as causally justified unless `/analysis/...` or `/causal/{id}` has confirmed the path, or you explicitly say path evidence was not found.
9. For `都有哪些 / 全部 / 列出所有 / 排名 / 统计 / 汇总 / Top N` style questions, never use `/sample` to build the result set. `/sample` is for grounding only.
10. Do not produce a final ontology/data answer from `/sample` alone. Use at least one structured `/sparql` or `/analysis/...` request in the current turn, unless the user asked only for schema/structure inspection.
11. For multi-step investigation, prefer the bundled `run` workflow instead of manually chaining low-level client calls.
12. Do not force all questions through one fixed path. First classify the question, then choose the smallest matching template such as `fact_lookup`, `enumeration`, `causal_lookup`, `causal_enumeration`, or `hidden_relation`.
13. Do not call legacy/non-existent endpoints such as `/analysis/causal` or `/analyzer`. Use `/analysis/paths*` and related analyzer endpoints, or `/causal/{id}`.
14. For `因为...哪些... / 哪些客户因为... / 哪些问题导致...` style questions, prefer `run --json` with template `causal_enumeration` instead of hand-built query chains. The shorthand form `run "<question>" --template ...` is planning-only.
15. Do not call `/causal/{id}` with non-customer IDs, do not hand-write `GET /analysis/paths?...` query strings, and do not confuse event-row counts with distinct customer counts.
16. A failed client command does not by itself prove the local reasoning server is down; distinguish transport/client failures from server availability.

Preferred server clients:

- `bash /Users/Song/code/cc/reasoner/.agents/skills/obda-query/scripts/obda_api.sh ...`
- fallback:
  `.venv/bin/python /Users/Song/code/cc/reasoner/.agents/skills/obda-query/scripts/obda_api.py ...`

## Editing Rules

Before editing `mapping.yaml` or `reasoning_server.py`, follow:

- `/Users/Song/code/cc/reasoner/OBDA_AGENT_PLAYBOOK.md`

Hard requirements:

1. Reproduce the failure first.
2. Verify real DuckDB schema before editing mappings.
3. Do not invent columns.
4. Prefer relationship construction in `mapping.yaml`, not application code.
5. Keep `/sample` available; it is part of the querying workflow, not disposable debug code.

Project schema facts that must not be guessed:

- `event` has no `customer_id`
- `workorder` has no `event_id`
- `perception` has no `event_id`

## Design Reference

Longer-term architecture and roadmap live in:

- `/Users/Song/code/cc/reasoner/.claude/plans/cozy-imagining-canyon.md`
