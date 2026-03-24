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

## Workflow

### Step 1: Discover Schema (Required First Step)

**Always fetch schema first** before generating SPARQL:

```python
import requests

# Get ontology schema from server
response = requests.get("http://localhost:8000/schema")
schema = response.json()

# Key schema elements:
# - schema['classes']: List of OWL classes
# - schema['data_properties']: Data properties with domain/range
# - schema['object_properties']: Object properties with domain/range
# - schema['class_hierarchy']: Subclass relationships
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

### Step 2: Understand User Intent

Identify what the user is asking:
- **Entity queries**: Find customers, events, strategies
- **Relationship queries**: Find connections between entities
- **Aggregate queries**: Counts, averages, statistics
- **Causal queries**: Discover paths (customer → event → perception → strategy)

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

### Step 4: Execute Query

```python
response = requests.post(
    "http://localhost:8000/sparql",
    json={"query": sparql_query}
)
results = response.json()['results']
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
| `/causal/{id}` | GET | Get pre-computed causal paths for entity |
| `/sample/{class_name}` | GET | Get sample instances of a class with all properties |

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

For causal discovery (customer → event → perception → strategy):

```python
# Use causal endpoint for pre-computed paths
response = requests.get(f"http://localhost:8000/causal/{customer_id}")
paths = response.json()['causal_paths']
```

Or manually traverse in SPARQL:
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
