#!/usr/bin/env python3
"""
真正的 OBDA Reasoning Server for CEM

架构：DuckDB --实时映射--> RDF --推理--> 查询结果
无需静态 TTL 文件
"""

import os
import tempfile
from typing import Any, Dict, List, Optional, Set, Tuple
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL
import owlrl
import morph_kgc

# Configuration
DB_FILE = "cem_data.duckdb"
ONTOLOGY_FILE = "Onto/cem.owl"

# Namespaces
EX = Namespace("http://ywyinfo.com/example-owl#")

# Global state
GRAPH: Graph = None
CAUSAL_INDEX: Dict[str, List[List[tuple]]] = {}
SCHEMA_INFO: Dict[str, Any] = {}
EXPLICIT_TRIPLES: Set[Tuple[Any, Any, Any]] = set()

BASE_RELATION_URIS = [
    str(EX.hasBehavior),
    str(EX.hasCustomer),
    str(EX.hasEmployee),
    str(EX.hasEvent),
    str(EX.hasPerception),
    str(EX.hasRemediationStrategy),
    str(EX.hasWorkOrder),
    str(EX.suggestsStrategy),
]

ANALYSIS_PROFILES: Dict[str, Dict[str, Any]] = {
    "default": {
        "description": "Default constrained graph exploration for nearby business relations.",
        "allowed_predicates": BASE_RELATION_URIS,
        "exclude_predicates": [str(RDF.type), str(RDFS.label)],
        "include_middle_objects": False,
        "default_limit": 15,
        "max_limit": 50,
        "default_max_depth": 2,
        "max_depth_cap": 4,
        "direction": "both",
        "include_inferred_only": False,
        "include_explicit_only": False,
    },
    "causal": {
        "description": "Customer/event/perception/strategy causal exploration.",
        "allowed_predicates": [
            str(EX.hasEvent),
            str(EX.hasPerception),
            str(EX.suggestsStrategy),
            str(EX.hasRemediationStrategy),
            str(EX.hasWorkOrder),
        ],
        "exclude_predicates": [str(RDF.type), str(RDFS.label)],
        "include_middle_objects": False,
        "default_limit": 10,
        "max_limit": 50,
        "default_max_depth": 3,
        "max_depth_cap": 5,
        "direction": "outgoing",
        "include_inferred_only": False,
        "include_explicit_only": False,
    },
    "structural": {
        "description": "Entity neighborhood exploration across known business relations.",
        "allowed_predicates": BASE_RELATION_URIS,
        "exclude_predicates": [str(RDF.type), str(RDFS.label)],
        "include_middle_objects": False,
        "default_limit": 20,
        "max_limit": 80,
        "default_max_depth": 2,
        "max_depth_cap": 4,
        "direction": "both",
        "include_inferred_only": False,
        "include_explicit_only": False,
    },
    "inference": {
        "description": "Inspect inferred-only relations near a source entity.",
        "allowed_predicates": BASE_RELATION_URIS,
        "exclude_predicates": [str(RDF.type), str(RDFS.label)],
        "include_middle_objects": False,
        "default_limit": 20,
        "max_limit": 80,
        "default_max_depth": 2,
        "max_depth_cap": 4,
        "direction": "both",
        "include_inferred_only": True,
        "include_explicit_only": False,
    },
}


class SparqlQuery(BaseModel):
    query: str


class QueryResult(BaseModel):
    results: List[Dict[str, Any]]
    count: int


class AnalysisRequest(BaseModel):
    mode: str = "paths"
    profile: str = "default"
    source: Optional[str] = None
    target: Optional[str] = None
    max_depth: Optional[int] = None
    limit: Optional[int] = None
    direction: Optional[str] = None
    allowed_predicates: Optional[List[str]] = None
    exclude_predicates: Optional[List[str]] = None
    include_middle_objects: Optional[bool] = None
    include_inferred_only: Optional[bool] = None
    include_explicit_only: Optional[bool] = None


class BatchAnalysisRequest(BaseModel):
    mode: str = "paths"
    profile: str = "default"
    sources: List[str]
    target: Optional[str] = None
    max_depth: Optional[int] = None
    limit: Optional[int] = None
    direction: Optional[str] = None
    allowed_predicates: Optional[List[str]] = None
    exclude_predicates: Optional[List[str]] = None
    include_middle_objects: Optional[bool] = None
    include_inferred_only: Optional[bool] = None
    include_explicit_only: Optional[bool] = None


class ExplainRequest(BaseModel):
    profile: str = "causal"
    source: Optional[str] = None
    target: Optional[str] = None
    max_depth: Optional[int] = None
    paths: Optional[List[List[Dict[str, Any]]]] = None


def load_mapping_config() -> str:
    """Generate Morph-KGC config for DuckDB."""
    config = f"""[DataSource1]
mappings=mapping.yaml
"""
    return config


def materialize_graph() -> Graph:
    """从 DuckDB 实时物化 RDF 图谱。"""
    print("从 DuckDB 实时物化 RDF...")

    db_path = os.path.abspath(DB_FILE)

    # 构建 Morph-KGC 配置（INI 格式）
    # 使用正确的 SQLAlchemy URL 格式 for DuckDB
    config_lines = [
        "[DataSource1]",
        f"mappings={os.path.abspath('mapping.yaml')}",
        f"db_url=duckdb:///{db_path}"
    ]
    config = "\n".join(config_lines)

    # 创建临时配置文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
        f.write(config)
        config_path = f.name

    try:
        # 使用 morph_kgc 物化
        graph = morph_kgc.materialize(config_path)
        print(f"  物化完成: {len(graph)} triples")
        return graph
    finally:
        os.unlink(config_path)


def load_and_reason() -> Graph:
    """Load graph from DuckDB via OBDA, apply OWL reasoning."""
    global EXPLICIT_TRIPLES

    print("=" * 60)
    print("OBDA Reasoning Server - 实时映射模式")
    print("=" * 60)

    # 1. 从 DuckDB 实时物化 RDF
    graph = materialize_graph()

    # 2. 加载本体
    if os.path.exists(ONTOLOGY_FILE):
        graph.parse(ONTOLOGY_FILE, format="turtle")
        print(f"  加载本体: {ONTOLOGY_FILE}")
    else:
        print(f"  警告: 未找到本体文件 {ONTOLOGY_FILE}")

    EXPLICIT_TRIPLES = set(graph)
    original_count = len(graph)

    # 3. 应用 OWL 2 RL 推理
    print("应用 OWL 2 RL 推理...")
    owlrl.DeductiveClosure(owlrl.OWLRL_Semantics).expand(graph)

    inferred = len(graph) - original_count
    print(f"  原始 triples: {original_count}")
    print(f"  推理后 triples: {len(graph)}")
    print(f"  推断 triples: {inferred}")

    return graph


def uri_local_name(value: Any) -> str:
    """Get local name from a URIRef or string."""
    value_str = str(value)
    if "#" in value_str:
        return value_str.split("#")[-1]
    return value_str.rstrip("/").split("/")[-1]


def normalize_uri(value: str) -> URIRef:
    """Normalize a local name or URI into a URIRef."""
    if value.startswith("http://") or value.startswith("https://"):
        return URIRef(value)
    if value.startswith("ex:"):
        return URIRef(f"{EX}{value.split(':', 1)[1]}")
    return URIRef(f"{EX}{value}")


def normalize_customer_uri(customer_id: str) -> URIRef:
    """Normalize a customer id or URI into a customer URIRef."""
    if customer_id.startswith("http://") or customer_id.startswith("https://"):
        return URIRef(customer_id)
    local_name = customer_id if customer_id.startswith("customer_") else f"customer_{customer_id}"
    return URIRef(f"{EX}{local_name}")


def is_middle_object(node: Any) -> bool:
    """Best-effort middle-object detection for short-term analyzer filtering."""
    if not isinstance(node, URIRef):
        return False

    local_name = uri_local_name(node).lower()
    if local_name.endswith("_link") or "link_" in local_name or "_link_" in local_name:
        return True

    for cls in GRAPH.objects(node, RDF.type):
        cls_name = uri_local_name(cls).lower()
        if "link" in cls_name or "middle" in cls_name:
            return True

    return False


def is_inferred_triple(triple: Tuple[Any, Any, Any]) -> bool:
    """Check whether a triple was added by the reasoner."""
    return triple not in EXPLICIT_TRIPLES


def serialize_value(value: Any) -> Any:
    """Serialize an RDF term for API output."""
    if isinstance(value, URIRef):
        return str(value)
    if isinstance(value, Literal):
        return value.value
    return str(value)


def serialize_edge(edge: Tuple[URIRef, URIRef, URIRef]) -> Dict[str, Any]:
    """Serialize an object-property edge for analyzer output."""
    return {
        "subject": str(edge[0]),
        "predicate": str(edge[1]),
        "object": str(edge[2]),
        "subject_local_name": uri_local_name(edge[0]),
        "predicate_local_name": uri_local_name(edge[1]),
        "object_local_name": uri_local_name(edge[2]),
        "inferred": is_inferred_triple(edge),
    }


def resolve_profile_settings(request: AnalysisRequest) -> Dict[str, Any]:
    """Resolve profile defaults plus any request overrides."""
    profile_name = request.profile or "default"
    profile = ANALYSIS_PROFILES.get(profile_name)
    if not profile:
        raise HTTPException(status_code=400, detail=f"Unknown analysis profile: {profile_name}")

    include_inferred_only = (
        request.include_inferred_only
        if request.include_inferred_only is not None
        else profile["include_inferred_only"]
    )
    include_explicit_only = (
        request.include_explicit_only
        if request.include_explicit_only is not None
        else profile["include_explicit_only"]
    )
    if include_inferred_only and include_explicit_only:
        raise HTTPException(
            status_code=400,
            detail="include_inferred_only and include_explicit_only cannot both be true",
        )

    direction = request.direction or profile["direction"]
    if direction not in {"outgoing", "incoming", "both"}:
        raise HTTPException(status_code=400, detail=f"Unsupported direction: {direction}")

    max_depth = request.max_depth or profile["default_max_depth"]
    max_depth = max(1, min(max_depth, profile["max_depth_cap"]))

    limit = request.limit or profile["default_limit"]
    limit = max(1, min(limit, profile["max_limit"]))

    allowed_predicates = request.allowed_predicates or profile["allowed_predicates"]
    exclude_predicates = set(profile["exclude_predicates"])
    if request.exclude_predicates:
        exclude_predicates.update(request.exclude_predicates)

    include_middle_objects = (
        request.include_middle_objects
        if request.include_middle_objects is not None
        else profile["include_middle_objects"]
    )

    return {
        "profile": profile_name,
        "mode": request.mode,
        "description": profile["description"],
        "allowed_predicates": set(allowed_predicates) if allowed_predicates else None,
        "exclude_predicates": exclude_predicates,
        "include_middle_objects": include_middle_objects,
        "include_inferred_only": include_inferred_only,
        "include_explicit_only": include_explicit_only,
        "max_depth": max_depth,
        "limit": limit,
        "direction": direction,
    }


def edge_allowed(edge: Tuple[URIRef, URIRef, URIRef], settings: Dict[str, Any]) -> bool:
    """Apply analyzer filters to a candidate edge."""
    predicate_str = str(edge[1])
    if settings["allowed_predicates"] and predicate_str not in settings["allowed_predicates"]:
        return False
    if predicate_str in settings["exclude_predicates"]:
        return False
    if settings["include_inferred_only"] and not is_inferred_triple(edge):
        return False
    if settings["include_explicit_only"] and is_inferred_triple(edge):
        return False
    return True


def iter_edges(graph: Graph, current: URIRef, settings: Dict[str, Any]) -> List[Tuple[URIRef, URIRef, URIRef]]:
    """Collect filtered edges around the current node."""
    edges: List[Tuple[URIRef, URIRef, URIRef]] = []

    if settings["direction"] in {"outgoing", "both"}:
        for predicate, obj in graph.predicate_objects(current):
            if not isinstance(obj, URIRef):
                continue
            edge = (current, predicate, obj)
            if not edge_allowed(edge, settings):
                continue
            if not settings["include_middle_objects"] and is_middle_object(obj):
                continue
            edges.append(edge)

    if settings["direction"] in {"incoming", "both"}:
        for subject, predicate in graph.subject_predicates(current):
            if not isinstance(subject, URIRef):
                continue
            edge = (subject, predicate, current)
            if not edge_allowed(edge, settings):
                continue
            if not settings["include_middle_objects"] and is_middle_object(subject):
                continue
            edges.append(edge)

    return sorted(edges, key=lambda item: (str(item[1]), str(item[0]), str(item[2])))


def edge_neighbor(current: URIRef, edge: Tuple[URIRef, URIRef, URIRef]) -> URIRef:
    """Return the neighboring node for the current node along an edge."""
    return edge[2] if edge[0] == current else edge[0]


def analyze_paths(
    graph: Graph,
    source: URIRef,
    settings: Dict[str, Any],
    target: Optional[URIRef] = None,
) -> Dict[str, Any]:
    """Run constrained path analysis from a source node."""
    results: List[List[Dict[str, Any]]] = []
    truncated = False

    def dfs(current: URIRef, path: List[Dict[str, Any]], visited: Set[URIRef], depth: int):
        nonlocal truncated
        if depth >= settings["max_depth"] or truncated:
            return

        for edge in iter_edges(graph, current, settings):
            next_node = edge_neighbor(current, edge)
            if next_node in visited:
                continue

            new_path = path + [serialize_edge(edge)]

            if target is None:
                results.append(new_path)
            elif next_node == target:
                results.append(new_path)
            else:
                dfs(next_node, new_path, visited | {next_node}, depth + 1)

            if len(results) >= settings["limit"]:
                truncated = True
                return

            if target is None and not truncated:
                dfs(next_node, new_path, visited | {next_node}, depth + 1)

    dfs(source, [], {source}, 0)

    return {
        "mode": "paths",
        "profile": settings["profile"],
        "source": str(source),
        "target": str(target) if target else None,
        "path_count": len(results),
        "truncated": truncated,
        "paths": results,
    }


def analyze_neighborhood(graph: Graph, source: URIRef, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Return a constrained neighborhood around a source entity."""
    nodes: Dict[str, Dict[str, Any]] = {
        str(source): {"uri": str(source), "local_name": uri_local_name(source)}
    }
    edges: List[Dict[str, Any]] = []
    seen_edges: Set[Tuple[str, str, str]] = set()
    frontier: Set[URIRef] = {source}
    truncated = False

    for _ in range(settings["max_depth"]):
        next_frontier: Set[URIRef] = set()
        for current in frontier:
            for edge in iter_edges(graph, current, settings):
                edge_key = (str(edge[0]), str(edge[1]), str(edge[2]))
                if edge_key in seen_edges:
                    continue
                seen_edges.add(edge_key)
                edges.append(serialize_edge(edge))
                next_node = edge_neighbor(current, edge)
                next_node_is_new = str(next_node) not in nodes
                for node in (edge[0], edge[2]):
                    nodes.setdefault(str(node), {"uri": str(node), "local_name": uri_local_name(node)})
                if next_node_is_new:
                    next_frontier.add(next_node)
                if len(edges) >= settings["limit"]:
                    truncated = True
                    break
            if truncated:
                break
        if truncated:
            break
        frontier = next_frontier
        if not frontier:
            break

    return {
        "mode": "neighborhood",
        "profile": settings["profile"],
        "source": str(source),
        "truncated": truncated,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def analyze_inferred_relations(graph: Graph, source: URIRef, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Inspect inferred-only relations in a constrained neighborhood."""
    inferred_settings = dict(settings)
    inferred_settings["include_inferred_only"] = True
    inferred_settings["include_explicit_only"] = False

    neighborhood = analyze_neighborhood(graph, source, inferred_settings)
    candidate_nodes = {URIRef(node["uri"]) for node in neighborhood["nodes"]}

    triples: List[Dict[str, Any]] = []
    seen_triples: Set[Tuple[str, str, str]] = set()
    truncated = False

    for subject, predicate, obj in graph:
        if not isinstance(obj, URIRef):
            continue

        triple = (subject, predicate, obj)
        if not is_inferred_triple(triple):
            continue
        if subject not in candidate_nodes and obj not in candidate_nodes:
            continue
        if not edge_allowed(triple, inferred_settings):
            continue
        if not inferred_settings["include_middle_objects"] and (is_middle_object(subject) or is_middle_object(obj)):
            continue

        triple_key = (str(subject), str(predicate), str(obj))
        if triple_key in seen_triples:
            continue
        seen_triples.add(triple_key)
        triples.append(serialize_edge(triple))
        if len(triples) >= inferred_settings["limit"]:
            truncated = True
            break

    return {
        "mode": "inferred-relations",
        "profile": inferred_settings["profile"],
        "source": str(source),
        "count": len(triples),
        "truncated": truncated,
        "triples": triples,
    }


def explain_paths(paths: List[List[Dict[str, Any]]]) -> List[str]:
    """Generate simple readable explanations from path payloads."""
    explanations = []
    for path in paths:
        if not path:
            continue
        parts = [path[0]["subject_local_name"]]
        for step in path:
            relation = step["predicate_local_name"]
            relation_text = f"--{relation}--> "
            if step["inferred"]:
                relation_text = f"--{relation} [inferred]--> "
            parts.append(relation_text + step["object_local_name"])
        explanations.append(" ".join(parts))
    return explanations


def dedupe_preserve_order(values: List[str]) -> List[str]:
    """Deduplicate a list while preserving original order."""
    seen: Set[str] = set()
    ordered: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def extract_schema(graph: Graph) -> Dict[str, Any]:
    """Extract ontology schema information."""
    schema = {
        "classes": [],
        "data_properties": [],
        "object_properties": [],
        "class_hierarchy": {},
    }

    # Extract classes
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

    print(f"Schema: {len(schema['classes'])} classes, "
          f"{len(schema['data_properties'])} data properties, "
          f"{len(schema['object_properties'])} object properties")

    return schema


def find_causal_paths(graph: Graph, source: URIRef, max_depth: int = 3) -> List[List[tuple]]:
    """Find all causal paths starting from a source entity."""
    settings = resolve_profile_settings(
        AnalysisRequest(profile="causal", source=str(source), max_depth=max_depth, mode="paths")
    )
    return analyze_paths(graph, source, settings)["paths"]


def build_causal_index(graph: Graph) -> Dict[str, List[List[tuple]]]:
    """Build causal path index for all customers."""
    print("构建因果路径索引...")
    index = {}

    for customer in graph.subjects(RDF.type, EX.customer):
        paths = find_causal_paths(graph, customer, max_depth=3)
        if paths:
            index[str(customer)] = paths

    print(f"  索引完成: {len(index)} customers")
    return index


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize global state on startup."""
    global GRAPH, CAUSAL_INDEX, SCHEMA_INFO

    # 实时从 DuckDB 加载并推理
    GRAPH = load_and_reason()
    SCHEMA_INFO = extract_schema(GRAPH)
    CAUSAL_INDEX = build_causal_index(GRAPH)

    print("=" * 60)
    print("OBDA 服务器就绪!")
    print("=" * 60 + "\n")

    yield

    print("Shutting down...")


app = FastAPI(
    title="CEM OBDA Reasoning Server",
    description="真正的 OBDA: DuckDB -> RDF -> OWL Reasoning -> SPARQL",
    version="2.0.0",
    lifespan=lifespan
)


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "mode": "OBDA (real-time mapping from DuckDB)",
        "triples": len(GRAPH) if GRAPH else 0,
        "indexed_customers": len(CAUSAL_INDEX),
        "analysis_profiles": sorted(ANALYSIS_PROFILES.keys()),
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


@app.get("/analysis/profiles")
def get_analysis_profiles():
    """Expose supported analyzer profiles and their defaults."""
    profiles = []
    for name, config in ANALYSIS_PROFILES.items():
        profiles.append({
            "name": name,
            "description": config["description"],
            "allowed_predicates": config["allowed_predicates"],
            "exclude_predicates": config["exclude_predicates"],
            "include_middle_objects": config["include_middle_objects"],
            "default_limit": config["default_limit"],
            "max_limit": config["max_limit"],
            "default_max_depth": config["default_max_depth"],
            "max_depth_cap": config["max_depth_cap"],
            "direction": config["direction"],
            "include_inferred_only": config["include_inferred_only"],
            "include_explicit_only": config["include_explicit_only"],
        })
    return {"profiles": profiles}


@app.get("/analysis/paths")
def get_analysis_paths(
    source: str,
    profile: str = "default",
    target: Optional[str] = None,
    max_depth: Optional[int] = None,
    limit: Optional[int] = None,
):
    """Convenience GET wrapper for constrained path analysis."""
    request = AnalysisRequest(
        mode="paths",
        profile=profile,
        source=source,
        target=target,
        max_depth=max_depth,
        limit=limit,
    )
    return post_analysis_paths(request)


@app.post("/analysis/paths")
def post_analysis_paths(request: AnalysisRequest):
    """Generic constrained path analysis endpoint."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")
    if not request.source:
        raise HTTPException(status_code=400, detail="source is required")

    settings = resolve_profile_settings(request)
    source = normalize_uri(request.source)
    target = normalize_uri(request.target) if request.target else None
    return analyze_paths(GRAPH, source, settings, target=target)


@app.post("/analysis/paths/batch")
def post_analysis_paths_batch(request: BatchAnalysisRequest):
    """Run constrained path analysis for multiple sources under shared settings."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")
    if not request.sources:
        raise HTTPException(status_code=400, detail="sources is required")

    unique_sources = dedupe_preserve_order(request.sources)
    target = normalize_uri(request.target) if request.target else None
    results = []

    for source_value in unique_sources:
        source_request = AnalysisRequest(
            mode=request.mode,
            profile=request.profile,
            source=source_value,
            target=request.target,
            max_depth=request.max_depth,
            limit=request.limit,
            direction=request.direction,
            allowed_predicates=request.allowed_predicates,
            exclude_predicates=request.exclude_predicates,
            include_middle_objects=request.include_middle_objects,
            include_inferred_only=request.include_inferred_only,
            include_explicit_only=request.include_explicit_only,
        )
        settings = resolve_profile_settings(source_request)
        source = normalize_uri(source_value)
        results.append(analyze_paths(GRAPH, source, settings, target=target))

    return {
        "mode": "paths-batch",
        "profile": request.profile,
        "source_count": len(unique_sources),
        "matched_source_count": sum(1 for item in results if item["path_count"] > 0),
        "target": str(target) if target else None,
        "total_path_count": sum(item["path_count"] for item in results),
        "truncated": any(item["truncated"] for item in results),
        "results": results,
    }


@app.post("/analysis/neighborhood")
def post_analysis_neighborhood(request: AnalysisRequest):
    """Return a constrained neighborhood around a source node."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")
    if not request.source:
        raise HTTPException(status_code=400, detail="source is required")

    settings = resolve_profile_settings(request)
    source = normalize_uri(request.source)
    return analyze_neighborhood(GRAPH, source, settings)


@app.post("/analysis/inferred-relations")
def post_analysis_inferred_relations(request: AnalysisRequest):
    """Inspect inferred-only relations around a source node."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")
    if not request.source:
        raise HTTPException(status_code=400, detail="source is required")

    settings = resolve_profile_settings(request)
    source = normalize_uri(request.source)
    return analyze_inferred_relations(GRAPH, source, settings)


@app.post("/analysis/explain")
def post_analysis_explain(request: ExplainRequest):
    """Generate readable explanations for paths or a source-based path query."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    paths = request.paths
    if paths is None:
        if not request.source:
            raise HTTPException(status_code=400, detail="source is required when paths are not provided")
        analysis_request = AnalysisRequest(
            mode="paths",
            profile=request.profile,
            source=request.source,
            target=request.target,
            max_depth=request.max_depth,
        )
        settings = resolve_profile_settings(analysis_request)
        source = normalize_uri(request.source)
        target = normalize_uri(request.target) if request.target else None
        paths = analyze_paths(GRAPH, source, settings, target=target)["paths"]

    explanations = explain_paths(paths)
    return {
        "profile": request.profile,
        "count": len(explanations),
        "explanations": explanations,
    }


@app.get("/causal/{customer_id}")
def get_causal_chain(customer_id: str):
    """Get causal chain for a specific customer."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    customer_uri = normalize_customer_uri(customer_id)
    paths = CAUSAL_INDEX.get(str(customer_uri), [])

    if not paths:
        request = AnalysisRequest(
            mode="paths",
            profile="causal",
            source=str(customer_uri),
            max_depth=3,
        )
        settings = resolve_profile_settings(request)
        paths = analyze_paths(GRAPH, customer_uri, settings)["paths"]

    return {
        "customer_id": customer_id,
        "customer_uri": str(customer_uri),
        "causal_paths": paths,
        "path_count": len(paths)
    }


@app.get("/sample/{class_name}")
def get_class_samples(class_name: str, limit: int = 3):
    """Get sample instances of a class with their properties."""
    if not GRAPH:
        raise HTTPException(status_code=503, detail="Graph not initialized")

    class_uri = class_name
    if not class_name.startswith("http"):
        class_uri = f"http://ywyinfo.com/example-owl#{class_name}"

    class_ref = URIRef(class_uri)

    instance_uris = list(GRAPH.subjects(RDF.type, class_ref))
    samples = []

    for instance in instance_uris[:limit]:

        instance_data = {
            "uri": str(instance),
            "local_name": instance.split("#")[-1] if "#" in str(instance) else str(instance).split("/")[-1],
            "data_properties": {},
            "object_properties": {}
        }

        for prop, value in GRAPH.predicate_objects(instance):
            prop_local = prop.split("#")[-1] if "#" in str(prop) else str(prop).split("/")[-1]

            if isinstance(value, Literal):
                instance_data["data_properties"][prop_local] = value.value
            elif isinstance(value, URIRef) and prop != RDF.type:
                value_local = value.split("#")[-1] if "#" in str(value) else str(value).split("/")[-1]
                instance_data["object_properties"][prop_local] = value_local

        samples.append(instance_data)

    return {
        "class_name": class_name,
        "class_uri": class_uri,
        "purpose": "grounding_only",
        "limit": limit,
        "sample_count": len(samples),
        "returned_count": len(samples),
        "truncated": len(instance_uris) > limit,
        "samples": samples
    }


@app.post("/reload")
def reload_graph():
    """Reload graph from DuckDB (useful after data changes)."""
    global GRAPH, CAUSAL_INDEX, SCHEMA_INFO

    GRAPH = load_and_reason()
    SCHEMA_INFO = extract_schema(GRAPH)
    CAUSAL_INDEX = build_causal_index(GRAPH)

    return {
        "status": "reloaded",
        "triples": len(GRAPH),
        "indexed_customers": len(CAUSAL_INDEX)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
