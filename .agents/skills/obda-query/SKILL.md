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
bash .agents/skills/obda-query/scripts/obda_api.sh health
```

Preferred client:

- `bash .agents/skills/obda-query/scripts/obda_api.sh`

Fallback client:

- `.venv/bin/python .agents/skills/obda-query/scripts/obda_api.py`

Do not hand-write raw `curl` requests unless both bundled clients are unavailable or broken.

## Non-Negotiable Protocol

The following rules are mandatory when this skill is active:

1. In the current turn, fetch `/schema` before writing any SPARQL or calling any analyzer endpoint.
2. If the query filters by a specific attribute or ID-like field, verify the property's domain in `/schema`.
3. If schema alone is not enough to disambiguate population or relationship usage, inspect `/sample/{class_name}` before finalizing SPARQL.
4. Use the bundled client script for server calls instead of ad-hoc `curl`.
5. Do not invent predicates or rely on remembered schema from earlier turns.
6. If the server exposes `/analysis/profiles`, consult it before using analyzer endpoints.

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

## Workflow

### Step 1: Discover Schema (Required First Step)

**Always fetch schema first** before generating SPARQL:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh schema
```

### Step 1.5: Validate Property Locations (Critical)

When filtering by a specific attribute (e.g., phone number, ID), verify its location in the ontology:

```python
# Check if property's domain matches target entity
property_name = "customerbehavior_手机号"  # example
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
- Relational FK → Object property (e.g., `hasBehavior`, `hasEvent`)
- Relational attribute → Data property on related entity (e.g., `customerbehavior_手机号`)
- **Never assume** attribute location — always verify domain in schema

### Step 1.6: Use `/sample` for Grounding (Required When Uncertain)

If schema alone is not enough to disambiguate how a class is actually populated, inspect sample instances:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh sample customer 3
```

Use `/sample/{class_name}` to:

- verify real object-property usage
- check whether a property is populated in practice
- inspect mapped local names before writing SPARQL

`/sample` is a grounding endpoint, not an optional debugging extra.

### Step 2: Understand User Intent

Identify what the user is asking:
- **Entity queries**: Find customers, events, strategies
- **Relationship queries**: Find connections between entities
- **Aggregate queries**: Counts, averages, statistics
- **Causal queries**: Discover paths (customer → event → perception → strategy)

Decision rule:

- Use `/sparql` first for factual, relational, and aggregate queries
- Use `/analysis/...` only when the user explicitly wants path discovery, hidden relations, explanation, or neighborhood exploration
- If `/analysis/profiles` exists, use it to discover supported analysis intents before calling analyzer
- Use `/causal/{id}` only as a compatibility shortcut when generic analyzer endpoints are unavailable

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
PREFIX ex: <http://ywyinfo.com/example-owl#>

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

### Step 4: Execute Query

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh sparql --query-file /tmp/query.rq
```

### Step 5: Explain Results

Transform SPARQL results into natural language:
- Summarize findings
- Explain causal chains if relevant
- Suggest related queries

## Server API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Check server status and graph size |
| `/schema` | GET | Get ontology schema (classes, properties) |
| `/sparql` | POST | Execute SPARQL query |
| `/sample/{class_name}` | GET | Get sample instances of a class with all properties |
| `/analysis/profiles` | GET | Discover supported analyzer profiles and default exploration policies |
| `/causal/{id}` | GET | Compatibility endpoint for customer-oriented causal paths |
| `/analysis/...` | GET/POST | Optional generic analyzer endpoints when available |

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
- if not, use `/causal/{id}` as a compatibility shortcut for customer-oriented cases
- do not manually chase event -> workorder -> perception -> strategy with many separate probes if `/causal` or `/analysis/paths` can answer it

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
  --json '{"mode":"paths","profile":"causal","source":"http://ywyinfo.com/example-owl#customer_CUST004","target":"http://ywyinfo.com/example-owl#remediationstrategy_STR003","max_depth":4}'
```

Fallback for today's simplest server:

- if `/analysis/profiles` is unavailable but `/analysis/paths` exists, send only `mode/source/target/max_depth` and rely on server defaults
- if generic analyzer endpoints do not exist, use `/causal/{id}` for customer-oriented path questions
- do not fabricate `allowed_predicates` unless the server contract explicitly requires them

### Hidden Relation / Inference Inspection

If the user asks for inferred or hidden relations, prefer a dedicated analyzer endpoint:

```bash
bash .agents/skills/obda-query/scripts/obda_api.sh analysis-inferred-relations \
  --json '{"mode":"inferred-relations","profile":"inference","source":"http://ywyinfo.com/example-owl#customer_CUST004","max_depth":3}'
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
bash .agents/skills/obda-query/scripts/obda_api.sh causal CUST004
```

Or manually traverse in SPARQL, but only after confirming the predicate names in `/schema`:
```sparql
SELECT ?customer ?event ?perception ?strategy
WHERE {
  ?customer ex:hasEvent ?event .
  ?event ex:hasPerception ?perception .
  ?perception ex:suggestsStrategy ?strategy .
}
```

## References

- **Detailed examples**: See [references/schema-patterns.md](references/schema-patterns.md) for common query patterns
