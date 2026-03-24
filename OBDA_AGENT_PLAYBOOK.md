# OBDA Agent Playbook

This file is for future coding agents working in this repository.

Goal: fix issues with the least token waste, the least code churn, and the highest probability of preserving a working OBDA pipeline.

## Non-Negotiable Rules

1. Reproduce the exact failure before editing anything.
2. Use `.venv/bin/python` and `.venv/bin/uvicorn` only. Do not use global Python or global pip.
3. Do not rewrite `mapping.yaml` broadly until one concrete root cause is proven.
4. Do not invent database columns. Verify the real DuckDB schema first.
5. Prefer expressing semantic relations in `mapping.yaml`, not in `reasoning_server.py`.
6. Application-layer relation补边 is allowed only as a temporary fallback when the mapping engine truly cannot express the relation. If you do this, say so explicitly and keep it temporary.
7. After each change, verify with commands. Do not claim success from inspection alone.

## Project Facts You Must Check First

The current simplified DuckDB schema is defined by `mock_and_map.py`, not by guesswork.

Known important facts:

- `customerbehavior` has `customer_id`, so customer-behavior relations can be mapped directly.
- `event` does not have `customer_id`.
- `workorder` does not have `event_id`.
- `perception` does not have `event_id`.
- `customer <-> event` lives in `customer_event_link`.
- `event <-> workorder` lives in `event_workorder_link`.
- `event <-> perception` lives in `event_perception_link`.
- `perception <-> remediationstrategy` lives in `perception_remediationstrategy_link`.

If a mapping references a column outside these facts, verify it with `DESCRIBE <table>` before touching anything else.

## Required Debugging Order

When the server fails to start, follow this order:

1. Reproduce materialization only.
2. Reproduce full graph loading and reasoning.
3. Only then start `uvicorn`.
4. Only after startup succeeds, test HTTP endpoints.

Use these commands:

```bash
.venv/bin/python - <<'PY'
import reasoning_server
reasoning_server.materialize_graph()
PY
```

```bash
.venv/bin/python - <<'PY'
import reasoning_server
reasoning_server.load_and_reason()
PY
```

```bash
.venv/bin/uvicorn reasoning_server:app --port 8000
```

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/causal/CUST004
```

## Mapping Discipline

Before editing `mapping.yaml`:

1. Inspect the real table schema with DuckDB.
2. Identify the exact broken reference or broken relation.
3. Make the smallest valid mapping change.
4. Re-run materialization immediately.

Good practice:

- Use link tables to emit direct semantic edges in `mapping.yaml`.
- Keep link entities mapped if the ontology models them as middle objects.
- If a direct object IRI can be built from foreign keys already present in the source row, prefer that over moving logic into Python.

Bad practice:

- Adding `customer_id` to `event` mappings when the column does not exist.
- Adding `event_id` to `workorder` or `perception` mappings when those columns do not exist.
- Rewriting large parts of `mapping.yaml` before a single failing reference is proven.
- Treating a DuckDB lock error as the root cause when an earlier materialization error already happened.

## Application Code Discipline

`reasoning_server.py` should do only this:

1. Materialize RDF from DuckDB through `mapping.yaml`.
2. Load ontology.
3. Apply OWL reasoning.
4. Expose query endpoints.

It should not silently recreate mapping semantics in Python unless there is a documented temporary reason.

## Token-Saving Reporting Standard

Before making a substantial edit, state:

- the exact failing command,
- the exact root cause,
- the exact file and lines to change.

After the fix, report:

- what changed,
- which verification commands passed,
- any remaining risk.

## Stop Doing These

- Do not “simplify” the mapping unless you can prove the original structure is wrong.
- Do not delete relation mappings just to get the server up.
- Do not switch architectures in the middle of debugging.
- Do not add external LLM dependencies for text-to-SPARQL.
- Do not keep both mapping-based and Python-based relation construction active unless you intentionally want duplicates.
