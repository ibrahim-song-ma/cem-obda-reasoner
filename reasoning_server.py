#!/usr/bin/env python3
"""
Local Reasoning Server for CEM Ontology

This FastAPI service:
1. Loads RDF graph and OWL ontology at startup
2. Applies OWL 2 RL reasoning via owlrl
3. Builds causal path index for O(1) queries
4. Exposes REST API for SPARQL queries and schema access

NO LLM calls - pure reasoning service.
"""

import os
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rdflib import Graph, Namespace, RDF, RDFS, OWL, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL, XSD
import owlrl

# Configuration
GRAPH_FILE = "graph.ttl"
ONTOLOGY_FILE = "Onto/cem.owl"
REASONED_GRAPH_FILE = "reasoned_graph.ttl"

# Namespaces
EX = Namespace("http://ywyinfo.com/example-owl#")

# Global state
GRAPH: Graph = None
CAUSAL_INDEX: Dict[str, List[List[tuple]]] = {}
SCHEMA_INFO: Dict[str, Any] = {}


class SparqlQuery(BaseModel):
    query: str


class QueryResult(BaseModel):
    results: List[Dict[str, Any]]
    count: int


def load_and_reason() -> Graph:
    """Load graph, apply OWL reasoning, and return reasoned graph."""
    print("Loading RDF graph and ontology...")
    graph = Graph()

    if os.path.exists(GRAPH_FILE):
        graph.parse(GRAPH_FILE, format="turtle")
        print(f"  Loaded {GRAPH_FILE}: {len(graph)} triples")
    else:
        raise FileNotFoundError(f"{GRAPH_FILE} not found. Run mock_and_map.py first.")

    if os.path.exists(ONTOLOGY_FILE):
        graph.parse(ONTOLOGY_FILE, format="turtle")
        print(f"  Loaded {ONTOLOGY_FILE}: {len(graph)} triples total")
    else:
        print(f"  Warning: {ONTOLOGY_FILE} not found")

    original_count = len(graph)

    # Apply OWL 2 RL reasoning
    print("Applying OWL 2 RL reasoning...")
    owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(graph)

    inferred = len(graph) - original_count
    print(f"  Original triples: {original_count}")
    print(f"  After reasoning: {len(graph)}")
    print(f"  Inferred triples: {inferred}")

    # Save reasoned graph
    graph.serialize(destination=REASONED_GRAPH_FILE, format="turtle")
    print(f"  Saved to {REASONED_GRAPH_FILE}")

    return graph


def extract_schema(graph: Graph) -> Dict[str, Any]:
    """Extract ontology schema information."""
    schema = {
        "classes": [],
        "data_properties": [],
        "object_properties": [],
        "class_hierarchy": {},
    }

    # Extract classes with labels
    for cls in graph.subjects(RDF.type, OWL.Class):
        label = graph.value(cls, RDFS.label)
        local_name = cls.split("#")[-1] if "#" in str(cls) else str(cls).split("/")[-1]
        schema["classes"].append({
            "uri": str(cls),
            "label": str(label) if label else local_name,
            "local_name": local_name
        })

    # Extract data properties
    for prop in graph.subjects(RDF.type, OWL.DatatypeProperty):
        label = graph.value(prop, RDFS.label)
        domain = graph.value(prop, RDFS.domain)
        range_val = graph.value(prop, RDFS.range)
        local_name = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]
        schema["data_properties"].append({
            "uri": str(prop),
            "label": str(label) if label else local_name,
            "local_name": local_name,
            "domain": str(domain) if domain else None,
            "range": str(range_val) if range_val else None
        })

    # Extract object properties
    for prop in graph.subjects(RDF.type, OWL.ObjectProperty):
        label = graph.value(prop, RDFS.label)
        domain = graph.value(prop, RDFS.domain)
        range_val = graph.value(prop, RDFS.range)
        local_name = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]
        schema["object_properties"].append({
            "uri": str(prop),
            "label": str(label) if label else local_name,
            "local_name": local_name,
            "domain": str(domain) if domain else None,
            "range": str(range_val) if range_val else None
        })

    # Build class hierarchy
    for cls in graph.subjects(RDF.type, OWL.Class):
        parents = list(graph.objects(cls, RDFS.subClassOf))
        if parents:
            schema["class_hierarchy"][str(cls)] = [str(p) for p in parents]

    print(f"Schema extracted: {len(schema['classes'])} classes, "
          f"{len(schema['data_properties'])} data properties, "
          f"{len(schema['object_properties'])} object properties")

    return schema


def find_causal_paths(graph: Graph, source: URIRef, max_depth: int = 3) -> List[List[tuple]]:
    """
    Find all causal paths starting from a source entity.

    Causal chain: customer -> event -> perception -> strategy
    """
    paths = []
    visited = set()

    # Define causal properties in order
    causal_props = [
        EX.hasEvent,
        EX.hasPerception,
        EX.suggestsStrategy,
        EX.hasRemediationStrategy
    ]

    def dfs(current: URIRef, path: List[tuple], depth: int):
        if depth >= max_depth:
            return

        for prop in causal_props:
            for next_node in graph.objects(current, prop):
                if next_node not in visited:
                    new_path = path + [(str(prop), str(next_node))]
                    paths.append(new_path)
                    visited.add(next_node)
                    dfs(next_node, new_path, depth + 1)

    dfs(source, [], 0)
    return paths


def build_causal_index(graph: Graph) -> Dict[str, List[List[tuple]]]:
    """Build causal path index for all customers at startup."""
    print("Building causal path index...")
    index = {}

    # Find all customers
    for customer in graph.subjects(RDF.type, EX.customer):
        paths = find_causal_paths(graph, customer, max_depth=3)
        if paths:
            index[str(customer)] = paths

    print(f"  Indexed {len(index)} customers with causal paths")
    return index


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize global state on startup."""
    global GRAPH, CAUSAL_INDEX, SCHEMA_INFO

    print("\n" + "=" * 60)
    print("Initializing Local Reasoning Server")
    print("=" * 60)

    # Load and reason
    GRAPH = load_and_reason()

    # Extract schema
    SCHEMA_INFO = extract_schema(GRAPH)

    # Build causal index
    CAUSAL_INDEX = build_causal_index(GRAPH)

    print("=" * 60)
    print("Server ready!")
    print("=" * 60 + "\n")

    yield

    # Cleanup (if needed)
    print("Shutting down...")


app = FastAPI(
    title="CEM Local Reasoning Server",
    description="OWL reasoning + SPARQL execution service",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "triples": len(GRAPH) if GRAPH else 0,
        "indexed_customers": len(CAUSAL_INDEX)
    }


@app.get("/schema")
def get_schema():
    """Get ontology schema information."""
    return SCHEMA_INFO


@app.post("/sparql", response_model=QueryResult)
def execute_sparql(query: SparqlQuery):
    """Execute SPARQL query on the reasoned graph."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    try:
        results = GRAPH.query(query.query)

        # Convert to list of dicts
        output = []
        for row in results:
            row_dict = {}
            for var in results.vars:
                val = row[var]
                if isinstance(val, URIRef):
                    row_dict[str(var)] = str(val)
                elif isinstance(val, Literal):
                    row_dict[str(var)] = val.value
                else:
                    row_dict[str(var)] = str(val)
            output.append(row_dict)

        return QueryResult(results=output, count=len(output))

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SPARQL error: {str(e)}")


@app.get("/causal/{customer_id}")
def get_causal_chain(customer_id: str):
    """
    Get causal chain for a specific customer.

    Returns paths like: customer -> event -> perception -> strategy
    """
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    # Try with and without namespace prefix
    customer_uri = customer_id
    if not customer_id.startswith("http"):
        customer_uri = f"http://ywyinfo.com/example-owl#{customer_id}"

    paths = CAUSAL_INDEX.get(customer_uri, [])

    # Also try direct lookup
    if not paths:
        paths = find_causal_paths(GRAPH, URIRef(customer_uri))

    return {
        "customer_id": customer_id,
        "customer_uri": customer_uri,
        "causal_paths": paths,
        "path_count": len(paths)
    }


@app.get("/inferred")
def get_inferred_relations():
    """
    Get all inferred triples that were added by OWL reasoning.
    (Comparison would require original graph - returns current state for now)
    """
    return {
        "total_triples": len(GRAPH),
        "note": "Run with original graph comparison to see inferred only"
    }


@app.get("/sample/{class_name}")
def get_class_samples(class_name: str, limit: int = 3):
    """
    Get sample instances of a class with their properties.

    Useful for exploring actual data structure before writing queries.
    Returns sample instances with all their data properties.
    """
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    # Build class URI
    class_uri = class_name
    if not class_name.startswith("http"):
        class_uri = f"http://ywyinfo.com/example-owl#{class_name}"

    class_ref = URIRef(class_uri)

    # Check if class exists
    if (class_ref, RDF.type, OWL.Class) not in GRAPH:
        # Try as instance check
        pass

    samples = []
    count = 0

    # Find instances of this class
    for instance in GRAPH.subjects(RDF.type, class_ref):
        if count >= limit:
            break

        instance_data = {
            "uri": str(instance),
            "local_name": instance.split("#")[-1] if "#" in str(instance) else str(instance).split("/")[-1],
            "data_properties": {},
            "object_properties": {}
        }

        # Get all data properties for this instance
        for prop, value in GRAPH.predicate_objects(instance):
            prop_local = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]

            if isinstance(value, Literal):
                instance_data["data_properties"][prop_local] = value.value
            elif isinstance(value, URIRef):
                # Only include object properties that link to other entities
                if prop != RDF.type:
                    value_local = value.split("#")[-1] if "#" in str(value) else str(value).split("/")[-1]
                    if "object_properties" not in instance_data:
                        instance_data["object_properties"] = {}
                    instance_data["object_properties"][prop_local] = value_local

        samples.append(instance_data)
        count += 1

    # Also find what properties are typically used with this class
    property_info = {
        "data_properties": [],
        "object_properties": []
    }

    for prop in GRAPH.subjects(RDF.type, OWL.DatatypeProperty):
        domain = GRAPH.value(prop, RDFS.domain)
        if domain and str(domain) == class_uri:
            prop_local = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]
            property_info["data_properties"].append(prop_local)

    for prop in GRAPH.subjects(RDF.type, OWL.ObjectProperty):
        domain = GRAPH.value(prop, RDFS.domain)
        if domain and str(domain) == class_uri:
            prop_local = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]
            range_val = GRAPH.value(prop, RDFS.range)
            range_local = None
            if range_val:
                range_local = range_val.split("#")[-1] if "#" in str(range_val) else str(range_val).split("/")[-1]
            property_info["object_properties"].append({
                "property": prop_local,
                "range": range_local
            })

    return {
        "class_name": class_name,
        "class_uri": class_uri,
        "sample_count": len(samples),
        "samples": samples,
        "schema_properties": property_info
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
