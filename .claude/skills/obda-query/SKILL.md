---
name: obda-query
description: |
  Query ontology through natural language using a local reasoning server.
  Use when the user asks questions about data stored in an RDF/OWL knowledge graph
  and a local reasoning server is available (typically at localhost:8000).
  The skill automatically discovers the ontology schema, generates SPARQL queries,
  and provides natural language answers.
---

# OBDA Query Skill

Query RDF/OWL ontologies through natural language via a local reasoning server.

## Prerequisites

Ensure the reasoning server is running:
```bash
.venv/bin/uvicorn reasoning_server:app --port 8000
```

Use the bundled client for all server interactions:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh schema
```

Preferred client:

- `bash .agents/skills/obda-query/scripts/obda_api.sh`

Fallback client:

- `.venv/bin/python .agents/skills/obda-query/scripts/obda_api.py`

Do not hand-write raw `curl` requests unless both bundled clients are unavailable or broken.
For multi-step questions, prefer the single-entry `run` command over manually chaining low-level client calls.
Before using `run`, choose the matching template for the user's question.

## Skill Boundary

This skill should remain thin.

Its job is to:

- fetch `schema` first
- classify the question into the smallest route/template family
- call `run` or one low-level structured command when appropriate
- follow bounded recovery rules
- write the final answer from `presentation` or structured results

Its job is **not** to:

- re-implement semantic grounding in prompt text
- encode phrase-specific planner behavior
- depend on one particular reroute, widening, or fallback strategy
- invent ontology classes, properties, or semantic rewrites
- emulate the planner by hand when question-mode already fits

Responsibility split:

- skill: routing discipline and protocol discipline
- client/planner: semantic routing, grounding, request IR, node plan, lowering, validation, bounded fallback
- server: schema, structured query execution, analysis execution

## Non-Negotiable Protocol

The following rules are mandatory when this skill is active:

1. In the current turn, fetch `/schema` before writing any SPARQL or calling any analyzer endpoint.
2. Do not call `/health` as a routine preflight. Use it only when diagnosing transport/server availability after a failure.
3. If the query filters by a specific attribute or ID-like field, verify the property's domain in `/schema`.
4. For `causal_enumeration`, the normal client path is `schema -> run`. Do not insert generic `/sample` or `/health` calls before the first `run`.
5. If schema alone is not enough to disambiguate population or relationship usage, inspect `/sample/{class_name}` before finalizing SPARQL.
6. Use the bundled client script for server calls instead of ad-hoc `curl`.
7. Do not invent predicates or rely on remembered schema from earlier turns.
8. If the server exposes `/analysis/profiles`, consult it before using analyzer endpoints.
9. If the user asks for root cause, path, hidden relations, solution rationale, or uses explicit causal wording such as "because", "due to", `因为`, `由于`, `导致`, do one analyzer request before the final answer. Do not stop at factual SPARQL alone.
10. If the user asks for enumeration, coverage, ranking, summary, or "all matching entities", never use `/sample` to produce the result set. Use `/sample` only for grounding, then return to `/sparql` or `/analysis/...`.
11. Do not produce a final ontology/data answer from `/sample` alone. In the current turn, at least one structured `/sparql` or `/analysis/...` request must support the result, unless the user explicitly asked only for schema/structure inspection.
12. For multi-step investigation, prefer `obda_api.sh run` or `obda_api.py run` instead of manually sequencing low-level commands.
13. Do not force all questions through the same path. First classify the question, then choose the smallest matching template.
14. Do not call legacy/non-existent endpoints such as `/analysis/causal` or `/analyzer`. Use `/analysis/paths*`, `/analysis/neighborhood`, `/analysis/inferred-relations`, `/analysis/explain`, or `/causal/{id}`.
15. Do not call compatibility endpoints with the wrong entity type or identifier format. Verify the endpoint contract first.
16. Do not infer "entity count" from row count, and do not infer row count from distinct entities. If the answer mentions both, compute both explicitly.
17. If `/schema` already exposes the needed object property, do not inspect `/sample` just to rediscover the relation name.
18. `run` supports both `--json/--json-file` and `run "<question>" --template ...`. Question form now routes through the semantic query planner and executes the locked plan automatically unless `--plan-only` is explicitly requested.
19. Do not hand-write `GET /analysis/paths?...` query strings. Use `analysis-paths --json`, `analysis-paths-batch --json`, or `run --json`.
20. When the user question contains both a cause constraint and an action/state constraint, encode both in the main structured query. Do not silently broaden `complained because of X` into `had any X-related event`.
21. For `causal_lookup` and `causal_enumeration`, the execution order is fixed: main SPARQL first, analyzer second. If the main SPARQL returns zero rows, do not run analyzer.
22. For `causal_enumeration`, use one batch analyzer request. Do not replace it with per-row or per-entity `/causal/{id}` calls after enumeration.
23. For `causal_enumeration`, if the first `run` returns `empty_result` or `partial_success`, at most one targeted grounding recovery is allowed: inspect one relevant class via `/sample`, adjust the main query once, and rerun once. Do not enter open-ended sample/grep/SPARQL exploration loops.

If any of the above steps are skipped, the skill has not been followed correctly.

## Query Budget

For a normal user question, target at most 3 server round-trips:

1. `/schema`
2. one main `/sparql` or one main `/analysis/...`
3. one optional follow-up request only if the first result is empty, ambiguous, or needs path explanation

Do not exceed this budget unless:

- the previous query returned empty results
- schema and sample evidence conflict
- the user explicitly asks for deep investigation or debugging

Slow answers in this repo are usually caused by too many exploratory requests, not by the server itself.

For `causal_enumeration`, the normal external command budget is even smaller:

1. `/schema`
2. one `run --json`

Only exceed that when the first `run` returns `empty_result`, `partial_success`, or a clear schema ambiguity.

## Fast Path For `causal_enumeration`

If the question matches `因为...哪些...`, `哪些实体因为...`, or another causal result-set question, follow this exact first pass:

1. `schema`
2. `run --json`
3. stop and inspect the structured result

Do not do any of the following before that first `run`:

- `/health`
- generic `/sample`
- ad-hoc `grep` over schema output
- hand-written fallback `sparql`

Only after the first `run` returns `empty_result`, `partial_success`, or a clear schema ambiguity may you do one targeted grounding repair and rerun once.

## Workflow

### Step 1: Discover Schema (Required First Step)

**Always fetch schema first** before generating SPARQL:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh schema
```

For multi-step questions, the preferred schema-first shortcut is:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run --json '{"template":"fact_lookup","sparql":{"query":"PREFIX ex: <http://example.com/ontology#> SELECT ?entity WHERE { ?entity a ex:TargetClass . } LIMIT 3"}}'
```

### Step 1.5: Validate Property Locations (Critical)

When filtering by a specific attribute (e.g., external ID, status, score), verify its location in the ontology:

```python
# Check if property's domain matches target entity
property_name = "external_identifier"  # example
domain = None
for prop in schema['data_properties']:
    if prop['local_name'] == property_name:
        domain = prop['domain']
        break

# If domain ≠ target entity, find relationship chain
if domain and domain != target_entity:
    # Find object property connecting target → domain
    for obj_prop in schema['object_properties']:
        if obj_prop['range'] == domain:
            print(f"Relationship: {target_entity} --{obj_prop['local_name']}--> {domain}")
```

**Common RDB2RDF Mapping Pattern**:
- Relational FK -> Object property (for example `hasRelatedEntity`)
- Relational attribute -> Data property on the mapped subject or a related subject
- **Never assume** attribute location — always verify domain in schema

### Step 1.6: Use `/sample` for Grounding (Required When Uncertain)

If schema alone is not enough to disambiguate how a class is actually populated, inspect sample instances:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh sample TargetClass 3
```

Use `/sample/{class_name}` to:

- verify real object-property usage
- check whether a property is populated in practice
- inspect mapped local names before writing SPARQL

`/sample` is a grounding endpoint, not an optional debugging extra.

For `causal_enumeration`, `/sample` is not part of the default first pass. The default is:

```text
schema -> run
```

Use `/sample` only if that first structured attempt comes back empty, ambiguous, or clearly misaligned with actual population.

If you already know the question is `causal_enumeration`, do not preemptively inspect `sample` "just to understand the structure". The first structured attempt must come directly from `schema`.

Do not use `/sample` to:

- enumerate all matching entities
- answer `都有哪些 / 全部 / 列出所有 / 排名 / 统计 / Top N / 汇总`
- infer coverage from the default sample size

If a structured query returns empty results, `/sample` may help debug the schema usage, but the final result set must still come from `/sparql` or `/analysis/...`.

### Step 2: Understand User Intent

Identify what the user is asking:
- **Entity queries**: Find entities matching class or property constraints
- **Relationship queries**: Find connections between entities
- **Aggregate queries**: Counts, averages, rankings, statistics
- **Causal/path queries**: Discover explanation paths or inferred links

### Step 2.1: Choose A Route Template

Supported templates:

- `schema_inspect`
  - use for: ontology/schema inspection only
  - path: `schema` with optional grounding samples
- `fact_lookup`
  - use for: one entity or one fact lookup
  - path: `schema -> sparql`
- `enumeration`
  - use for: `都有哪些 / 全部 / 列出所有 / 统计 / 汇总`
  - path: `schema -> sparql`
- `causal_lookup`
  - use for: `为什么这个实体会关联 / why is this linked`
  - path: `schema -> sparql -> analysis`
- `causal_enumeration`
  - use for: `因为...哪些... / 哪些实体因为... / 哪些问题导致...`
  - path: `schema -> sparql -> batch analysis`
  - counting rule: keep row counts and distinct-entity counts separate
- `hidden_relation`
  - use for: hidden or inferred relation inspection
  - path: `schema -> profiles -> analysis`
- `custom`
  - use only when none of the standard templates fit

Default routing:

- simple one-shot fact question -> `fact_lookup`
- list/summary/count question -> `enumeration`
- one anchor entity + attribute/status/score check -> `fact_lookup` or `causal_lookup`, not `enumeration`
- causal wording with one anchor entity -> `causal_lookup`
- causal wording with a result set -> `causal_enumeration`
- hidden/inferred relation question -> `hidden_relation`
- a conditional suffix such as `如果有，有什么解决方案` does not by itself turn a one-anchor lookup into `causal_enumeration`

Decision rule:

- Use `/sparql` first for factual, relational, and aggregate queries
- Use `/analysis/...` when the user explicitly wants path discovery, hidden relations, explanation, neighborhood exploration, root cause, solution rationale, or uses causal wording such as `因为 / 由于 / 导致`
- For solution-oriented questions, first use `/sparql` to identify the relevant entity set, then use `/analysis/...` or a documented compatibility endpoint once to justify why a result is connected
- If `/analysis/profiles` exists, use it to discover supported analysis intents before calling analyzer
- Use documented compatibility endpoints only as server-specific shortcuts when generic analyzer endpoints are unavailable

Runner rule:

- If the question needs more than one step, use `run` with a chosen template
- Execution form: `run --json '{...}'` or `run --json-file plan.json`
- Question form: `run "natural language question" --template <template>` now fetches schema first, routes the question through the semantic query planner, and executes the locked planner-selected plan automatically
- Planning-only inspection is now explicit: `run "natural language question" --template <template> --plan-only`
- Question form may narrow the requested template to a smaller effective template when the query family is clearer than the caller’s initial guess. Treat that rerouting as client behavior; do not depend on a particular hard-coded example in the skill
- If this skill is launched with arguments that look like `run "question" --template ...`, prefer that exact question form or `run --json {"template":"...","question":"..."}`. Do not expand it into a hand-written builder or raw SPARQL on the first pass
- If the question has one explicit anchor plus a status/score/ID-style predicate, do not force `causal_enumeration` just because the utterance has multiple clauses. Keep it on the smallest anchored lookup route and let the client narrow further if needed
- When question form succeeds, treat its executed result as authoritative. Do not replace it with a fresh builder, a fabricated class name, or a wider raw SPARQL
- For natural-language question form, never invent missing ontology classes such as `complaint` just because the user said “投诉”; the planner and validator own that grounding step
- If a standard-template JSON plan contains both `question` and manual `sparql / analysis / samples`, the client will ignore those manual fields and force locked question-mode execution. Do not rely on mixing them
- If question form performs any bounded fallback internally, treat that as opaque client behavior. It is not permission to start free exploration or to restate internal widening logic as part of the skill contract
- For `causal_enumeration`, the default command sequence is `schema -> run`. Do not prepend `health`, generic `sample`, or ad-hoc low-level probes before the first `run`
- For `causal_enumeration`, if you are about to inspect `sample` before the first `run`, stop. That means you are deviating from the fast path
- For `causal_lookup` and `causal_enumeration`, prefer `sparql.builder` over free-form `sparql.query` whenever the query shape matches `source class -> evidence class -> filters`
- For `causal_lookup` and `causal_enumeration`, when the builder shape is clearly `source class -> evidence class`, prefer omitting `link_property` and let the client infer it
- For `causal_lookup` and `causal_enumeration`, if you still use raw `sparql.query`, set `sparql.source_var` explicitly so the client can validate and auto-derive analyzer anchors
- `schema` now returns a compact `schema_summary` by default. Use `schema --full` only when you explicitly need the full ontology payload for debugging
- `run` execution responses are compact by default: they return `schema_summary` / `profiles_summary` unless you explicitly request `include_schema: true` or `include_profiles: true`
- `run` execution responses also compress analyzer output by default: they return summarized `analysis` unless you explicitly request `include_analysis: true`
- Executed question-mode responses hide planner debug by default. If you explicitly need `planner_summary` / `planner_attempts`, inspect `--plan-only` or pass `include_planner_debug: true` in a JSON question plan
- If SPARQL succeeds but analyzer cannot continue because no URI anchor is available, `run` may return `status: partial_success` plus `analysis_error`; inspect the main SPARQL result instead of treating it as a transport failure
- For `causal_lookup` and `causal_enumeration`, if `run` returns a `presentation` field, use it as the primary input for the final answer. Treat raw `analysis.paths` as supporting evidence, not as the default user-facing wording
- If `presentation.answer_contract` is available, follow its section order and field contract instead of improvising a new answer shape
- If `presentation.answer_contract.count_contract` is available, obey it literally. For `哪些客户 ...` style questions, report `entity_count` as the customer count and never confuse it with `record_count`
- If the question is a direct one-shot lookup, a single low-level command is still acceptable
- Do not manually emulate `run` when a standard template already fits
- For `causal_enumeration`, prefer `run` and do not replace the analyzer step with ad-hoc sample inspection
- For `causal_lookup` and `causal_enumeration`, if you use `sparql.builder`, let the client validate class names, property names, and link direction before execution; that validation may use schema-declared object properties and runtime-mapped relations when available
- That validation may also use runtime-mapped data properties from `mapping.yaml` when the ontology schema does not declare every queryable data field
- When builder relation inference is possible, do not manually choose a schema-only semantic relation if the executable runtime mapping relation is enough
- For `causal_enumeration`, the main SPARQL should return anchored rows such as `entity_id + evidence_anchor + evidence_type`, not only partial projections
- For `causal_lookup` and `causal_enumeration`, treat `run` as query-first-then-analysis. Analyzer is a second stage, not a candidate finder

Failure rule:

- If `obda_api.sh schema` fails, first diagnose the client/transport failure
- A failed `schema` command does not by itself prove the reasoning server is down
- Do not silently switch to a chain of raw `curl` commands and continue as if the protocol succeeded
- If raw `curl` can reach `/schema` but `obda_api.sh` cannot, treat that as a client bug, not a server-down conclusion
- If the planner form returns no executable candidate or low-confidence candidates, do not silently broaden the query. Refine one slot or ask for clarification instead of inventing a wider SPARQL
- If question form returns `planning_required`, treat it as fail closed by default
- If question form returns `planning_required` and there is no `recovery_hint`, stop. Do not issue manual `sparql`, `sample`, or exploratory chains in the same turn
- If a one-anchor status/score question was forced through an enumeration template and comes back empty or misrouted, do not recover by sampling generic classes such as `customer`. Stop, or rerun once with the smaller anchored lookup template only if you are explicitly debugging the route choice
- If `planning_required` also returns `clarification_hint.kind = explicit_metric_or_threshold_required`, do not use `/sample` or hand-written SPARQL to invent the missing semantic threshold yourself
- In that case, do not silently rewrite the user's question into your own explicit-threshold variant such as turning `低满意度` into `满意度评分低于3分`
- The explicit metric/threshold must come from the user's wording or from a planner-grounded numeric constraint already present in the response; if neither exists, stop and ask for a clearer restatement instead of probing data to pick one
- If question-mode also returns `next_action = ask_user_for_clarification`, follow it literally: ask the user the clarification prompt and stop the investigation in the current turn
- This is a hard stop: after `next_action = ask_user_for_clarification`, do not call `sample`, `sparql`, `run`, `causal`, `Read`, or `--plan-only` in the same turn
- Do not inspect tool-result files to hunt for a workaround after `ask_user_for_clarification`; the next assistant message must be the clarification question itself
- Do not restate or answer the original ontology question in the same turn after `ask_user_for_clarification`
- If the user already asked an explicit anchored property question such as `13800138004的满意度评分是多少`, keep it on `fact_lookup`; do not switch to `/sample` first and do not hand-write a second factual SPARQL before trying question-mode
- Only inspect the planner bundle in `--plan-only` mode or when explicitly debugging; do not treat a normal question-mode `planning_required` response as a partially completed plan that should be hand-finished
- If the primary query is an enumeration, do not replace it with sample browsing after the first successful structured result
- If `causal_enumeration` returns `status: empty_result`, stop and report no matches unless one targeted grounding sample is genuinely required to debug the schema
- If `run` returns `recovery_hint`, follow it literally: one targeted sample at most, then rerun once
- If `causal_enumeration` returns `status: partial_success`, inspect the structured SPARQL rows first. Only do one targeted grounding recovery if the missing analyzer input is caused by schema/query shape, then rerun once
- If `presentation` is available, do not restate raw machine paths such as `entity_A --predicate--> entity_B` unless the user explicitly asks for raw path details
- If the compact `analysis` summary is enough to answer the question, do not request `include_analysis: true` just to restate the same path evidence in a more verbose machine form
- For `causal_enumeration`, default answer order is: summary sentence, compact entity table, grouped evidence details, brief causal confirmation
- For `causal_enumeration`, do not mention `planner_attempts`, `execution_variant`, or raw path counts unless the user explicitly asks for debugging detail
- For `causal_enumeration`, if the user asked “哪些客户 / 有几位客户”, the headline count must come from `presentation.summary.entity_count`, not the number of SPARQL rows
- If the executed result exposes only structured evidence rows and compact analysis summaries, describe those directly. Do not speculate about planner-internal matching logic unless the response explicitly exposes it

### Analyzer Contract Roadmap

Treat the analyzer contract as evolving in three stages:

- **Today**: prefer `/causal/{id}` or the simplest generic `/analysis/...` payload the server accepts
- **Near term**: use `/analysis/profiles` and send `mode + profile + source + target? + max_depth`
- **Long term**: let the server derive traversal constraints automatically from ontology metadata and graph statistics

The skill should be written toward the long-term contract, but remain compatible with today's simpler server.

### Step 3: Generate SPARQL

Use discovered schema to build queries:

```sparql
# Template structure
PREFIX ex: <http://example.com/ontology#>

SELECT ?entity ?property
WHERE {
  ?entity a ex:ClassName ;
          ex:propertyName ?property .
  FILTER(?property < value)
}
```

**Pattern matching based on intent**:
- Entity with property → `?entity a ex:Class ; ex:property ?value`
- Related entities → `?a ex:rel1 ?b . ?b ex:rel2 ?c`
- Filters → `FILTER(?value < 3.0)`, `FILTER(?name = "value")`

Rules:

- Never invent predicate names that are not present in `/schema`
- Prefer predicates that are also visible in `/sample` output when uncertain
- If schema does not expose the needed relation, say so explicitly instead of fabricating SPARQL
- Prefer one consolidated query over many object-by-object probes
- Do not start with `SELECT ?p ?o WHERE { <entity> ?p ?o }` unless a previous structured query failed
- Do not treat a strategy lookup as causally explained until one analyzer request has confirmed the path or explicitly shown that no path is available
- For list/coverage/ranking questions, never derive the answer set from `/sample`
- Do not write transient `.rq` files under `/tmp` for normal queries. Prefer passing SPARQL inline with `--query`. Only persist a query file if the user explicitly asks to keep it.

### Step 4: Execute Query

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh sparql --query 'PREFIX ex: <http://example.com/ontology#> SELECT ?entity WHERE { ?entity a ex:TargetClass . }'
```

Preferred multi-step execution:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh run --json '{
  "template": "causal_enumeration",
  "samples": [{"class_name":"TargetClass","limit":3}],
  "sparql": {"query": "PREFIX ex: <http://example.com/ontology#> SELECT ?entity ?evidence WHERE { ?entity a ex:TargetClass ; ex:relatedTo ?evidence . } LIMIT 5"},
  "analysis": {"payload": {"sources": ["http://example.com/ontology#entity_123"], "max_depth": 3}}
}'
```

### Step 5: Explain Results

Transform SPARQL results into natural language:
- Summarize findings
- Separate factual findings from causal/path explanation
- If the question asks for a solution, explain whether the solution came from:
  - a direct graph fact only, or
  - an analyzer-confirmed path
- Suggest related queries

## Server API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Check server status and graph size |
| `/schema` | GET | Get ontology schema (classes, properties) |
| `/sparql` | POST | Execute SPARQL query |
| `/sample/{class_name}` | GET | Get sample instances of a class with all properties |
| `/analysis/profiles` | GET | Discover supported analyzer profiles and default exploration policies |
| compatibility endpoint such as `/causal/{id}` | GET | Optional server-specific shortcut; not part of the generic skill contract |
| `/analysis/...` | GET/POST | Optional generic analyzer endpoints when available |

Legacy aliases such as `/analysis/causal` and `/analyzer` are not part of this server contract.

### Response Formats

**Schema response**:
```json
{
  "classes": [{"uri": "...", "label": "...", "local_name": "..."}],
  "data_properties": [{"uri": "...", "domain": "...", "range": "..."}],
  "object_properties": [...],
  "class_hierarchy": {...}
}
```

**SPARQL response**:
```json
{
  "results": [{"var1": "value1", "var2": "value2"}],
  "count": 5
}
```

## Error Handling

**Server not running**:
```
本地推理机服务未启动。请先运行：
.venv/bin/uvicorn reasoning_server:app --port 8000
```

**Query errors**:
- Check schema for correct property names
- Verify class names match ontology
- Ensure FILTER syntax is valid
- **Verify property domain**: If filtering returns no results, check if the property belongs to the target entity or a related entity (use `/sample/{class}` to inspect actual data)

## Advanced: Causal Chain Queries

For causal discovery or path explanation:

Preferred approach:

- if generic analyzer endpoints exist, use them first
- if not, use any documented compatibility shortcut only when its entity scope matches the query
- do not manually chase long relation chains with many separate probes if `/analysis/paths` or a compatibility endpoint can answer it
- for "why" or "what solution and why" questions, analyzer use is mandatory before the final answer

### Generic Analyzer Request Pattern

Preferred rule:

- Let the server choose low-level traversal constraints
- The skill should primarily express analysis intent
- Only send low-level overrides if the server explicitly requires them

Preferred flow:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh profiles
```

Preferred request:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh analysis-paths \
  --json '{"mode":"paths","profile":"causal","source":"http://example.com/ontology#entity_123","target":"http://example.com/ontology#entity_456","max_depth":4}'
```

Do not replace the command above with a hand-built URL such as `curl ".../analysis/paths?from=...&to=..."`.

### Minimal Causal Answer Pattern

For questions like:

- `为什么这个实体会出现这个结果`
- `有什么解决方案`
- `这个结果为什么和该实体有关`

Use this pattern:

1. `/schema`
2. one consolidated `/sparql` to identify the anchor entity, relevant evidence, or candidate results
3. one analyzer request:
   - `/analysis/paths` when generic analyzer exists
   - otherwise a documented compatibility endpoint
4. final answer must include both:
   - factual result
   - causal/path evidence or an explicit statement that no path evidence was found

Do not end the answer after step 2 for these question types.

Fallback for today's simplest server:

- if `/analysis/profiles` is unavailable but `/analysis/paths` exists, send only `mode/source/target/max_depth` and rely on server defaults
- if generic analyzer endpoints do not exist, use a documented compatibility endpoint only when the server explicitly supports it for the relevant entity type
- do not fabricate `allowed_predicates` unless the server contract explicitly requires them

### Hidden Relation / Inference Inspection

If the user asks for inferred or hidden relations, prefer a dedicated analyzer endpoint:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh analysis-inferred-relations \
  --json '{"mode":"inferred-relations","profile":"inference","source":"http://example.com/ontology#entity_123","max_depth":3}'
```

### Analyzer Safety Rules

- Never do unconstrained neighborhood traversal by default
- Prefer `mode + profile + source + target? + max_depth` over low-level traversal fields
- Do not fabricate `allowed_predicates`, `limit`, or `include_middle_objects` if the server can derive them
- Do not include middle objects unless the user is asking about ontology structure or link entities
- Prefer schema-driven or server-derived predicate policies over open-ended traversal
- If the graph vocabulary is unclear, inspect `/sample` before using analyzer
- Prefer one analyzer request over a chain of manual SPARQL probes when the user is asking for paths or hidden relations

Only use low-level overrides when the server documents them as required:

- `allowed_predicates`
- `exclude_predicates`
- `limit`
- `include_middle_objects`
- `direction`

```bash
# Compatibility shortcut
bash .agents/skills/obda-query/scripts/obda_api.sh causal <entity-id>
```

Or manually traverse in SPARQL, but only after confirming the predicate names in `/schema`:
```sparql
SELECT ?source ?middle ?target
WHERE {
  ?source ex:rel1 ?middle .
  ?middle ex:rel2 ?target .
}
```

## References

- **Detailed examples**: See [references/schema-patterns.md](references/schema-patterns.md) for common query patterns
