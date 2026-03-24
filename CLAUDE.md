# 🤖 System Role & Project Context: Ontology-Based QA Agent (OBDA)

You are an expert in Semantic Web, OBDA, Knowledge Graphs, and LLM Agent development.
The user wants to build an intelligent QA system where Claude (you) acts as the agent core, directly generating SPARQL queries and leveraging a local ontology reasoner to find causal/logical relationships.

## 🎯 New Architecture (Simplified)

```
┌─────────────────────────────────────────────────────────────┐
│                      User Interface                         │
│                    (Claude Code / CLI)                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Agent Core (Claude)                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Intent Understanding│  │ SPARQL Generation│  │ Result Explanation│ │
│  │ (Built-in)          │  │ (Built-in)       │  │ (Built-in)        │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │ HTTP
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Local Reasoning Server (Python)                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ SPARQL Execution│  │ OWL Reasoning   │  │ Graph Mgmt   │ │
│  │ (rdflib)        │  │ (owlrl)         │  │ (DuckDB+RDF) │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 📁 Directory Structure Context

- DDL file: `DDL/ctc_data_ddl.sql` (Relational schema)
- Ontology file: `Onto/cem.owl` (Conceptual model & rules)
- Mock data & mapping: `mock_and_map.py` (Generates `graph.ttl` and `cem_data.duckdb`)
- Reasoning server: `reasoning_server.py` (FastAPI service with owlrl reasoning)

## ⚠️ STRICT ISOLATION RULES

1. **Python Environment**: NEVER use global pip. ALWAYS use `.venv`. If not exists, run `python3 -m venv .venv`. Execute scripts via `.venv/bin/python`.
2. **Database**: Use local `duckdb`. NO heavy DBs like MySQL unless via Docker.
3. **No External LLM Calls**: Claude generates SPARQL directly. NO `openai` package calls for Text-to-SPARQL.

## 🛠️ Technology Stack

- **Database**: `duckdb` (local file)
- **Ontology & Graph**: `rdflib`
- **Mapping**: Custom Python (`mock_and_map.py`)
- **Reasoner**: `owlrl` (Crucial for causal & implicit logical relationships)
- **Server**: `fastapi` + `uvicorn` (Local reasoning server)

## 📋 The Agentic Workflow

### Phase 1: Data Preparation (Already Done)

`mock_and_map.py` performs:
1. Creates DuckDB tables from DDL
2. Inserts meaningful mock data
3. Generates RDF graph (`graph.ttl`) via rdflib

### Phase 2: Local Reasoning Server

The `reasoning_server.py`:
1. Loads `graph.ttl` and `.owl` ontology
2. Applies `owlrl.DeductiveClosure` reasoning at startup
3. Exposes REST API:
   - `POST /sparql` - Execute SPARQL queries
   - `GET /schema` - Get ontology structure
   - `GET /causal/{customer_id}` - Get causal chains

### Phase 3: Query Workflow

When user asks a question:

1. **Claude analyzes intent** → identifies what to query
2. **Claude generates SPARQL** → using knowledge of ontology schema
3. **Claude calls local server** → HTTP POST to `localhost:8000/sparql`
4. **Claude explains results** → natural language answer with causal explanation

## 🎯 Claude Code Skill

Create a skill named `obda-query` that:
- Accepts natural language questions
- Generates SPARQL internally (no external LLM call)
- Queries the local reasoning server
- Returns answers with causal explanations

Example:
```
/obda-query 哪些客户有网络体验问题？
```

## 📚 Key Concepts

- **OWL 2 RL Reasoning**: Discovers hidden relationships via transitivity, inverse properties, property chains
- **Causal Discovery**: `customer → event → perception → remediation_strategy`
- **Pre-computation**: Server builds causal index at startup for O(1) queries

## 🔧 Starting the System

```bash
# 1. Start the reasoning server
.venv/bin/uvicorn reasoning_server:app --port 8000

# 2. Use Claude directly - no additional configuration needed
/obda-query <your question>
```
