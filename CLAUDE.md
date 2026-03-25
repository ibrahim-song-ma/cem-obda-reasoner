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

Mandatory protocol:

1. Fetch `/schema` first in the current turn.
2. If filtering by phone, ID, score, status, or similar attributes, verify the property's domain.
3. If schema alone is not enough, inspect `/sample/{class_name}` before finalizing SPARQL.
4. For root cause, "why", hidden relation, or solution questions, do one analyzer request before the final answer.
5. Do not invent predicates from memory.
6. For a normal user question, keep the server query budget to roughly 3 round-trips unless earlier results are empty or ambiguous.
7. Do not start by probing a concrete entity with `SELECT ?p ?o`; prefer one structured query.
8. Do not present a strategy or solution as causally justified unless `/analysis/...` or `/causal/{id}` has confirmed the path, or you explicitly say path evidence was not found.

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
