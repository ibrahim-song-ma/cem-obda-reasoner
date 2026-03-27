#!/usr/bin/env python3
"""Minimal client for the local OBDA reasoning server."""

import argparse
import contextlib
from copy import deepcopy
import io
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MAPPING_FILE = REPO_ROOT / "mapping.yaml"
DEFAULT_STATE_FILE = Path("/tmp/obda_query_client_state.json")
DEFAULT_SCHEMA_TTL_SECONDS = 900
RUN_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "schema_inspect": {
        "description": "Schema-first inspection with optional grounding samples and no business result set.",
        "requires_sparql": False,
        "requires_analysis": False,
        "default_analysis_kind": None,
        "auto_include_profiles": False,
    },
    "fact_lookup": {
        "description": "Single factual lookup backed by one structured SPARQL query.",
        "requires_sparql": True,
        "requires_analysis": False,
        "default_analysis_kind": None,
        "auto_include_profiles": False,
    },
    "enumeration": {
        "description": "List or aggregate matching entities via SPARQL without causal analysis.",
        "requires_sparql": True,
        "requires_analysis": False,
        "default_analysis_kind": None,
        "auto_include_profiles": False,
    },
    "causal_lookup": {
        "description": "Fact lookup followed by single-entity causal/path verification.",
        "requires_sparql": True,
        "requires_analysis": True,
        "default_analysis_kind": "paths",
        "auto_include_profiles": True,
    },
    "causal_enumeration": {
        "description": "Enumerate a result set, then run batch causal/path verification for the matched entities.",
        "requires_sparql": True,
        "requires_analysis": True,
        "default_analysis_kind": "paths-batch",
        "auto_include_profiles": True,
    },
    "hidden_relation": {
        "description": "Inspect inferred or hidden relations through the analyzer without requiring a SPARQL result set.",
        "requires_sparql": False,
        "requires_analysis": True,
        "default_analysis_kind": "inferred-relations",
        "auto_include_profiles": True,
    },
    "custom": {
        "description": "Custom run plan. Use only when no standard template fits.",
        "requires_sparql": False,
        "requires_analysis": False,
        "default_analysis_kind": None,
        "auto_include_profiles": False,
    },
}
EX_LOCAL_NAME_PATTERN = re.compile(r'(?<!<)\bex:([^\s;.,(){}]+)')
SELECT_VARS_PATTERN = re.compile(r"SELECT\s+(?:DISTINCT\s+)?(.*?)\s+WHERE", re.IGNORECASE | re.DOTALL)
MAPPING_NAME_PATTERN = re.compile(r"^\s{2}([^:\s][^:]*)\s*:\s*$")
MAPPING_SUBJECT_PATTERN = re.compile(r"^\s{4}s:\s+(\S+)\s*$")
MAPPING_TYPE_PATTERN = re.compile(r"^\s{6}- \[a,\s*(\S+)\s*\]\s*$")
MAPPING_PO_ARRAY_PATTERN = re.compile(r"^\s{6}- \[(\S+),\s*([^,\]]+)(?:,\s*([^\]]+))?\]\s*$")
MAPPING_PREDICATE_PATTERN = re.compile(r"^\s{6}- p:\s+(\S+)\s*$")
MAPPING_OBJECT_VALUE_PATTERN = re.compile(r"^\s{10}value:\s+(\S+)\s*$")
MAPPING_OBJECT_IRI_TYPE_PATTERN = re.compile(r"^\s{10}type:\s+iri\s*$")
CAUSE_PATTERN = re.compile(r"(?:因为|由于)(?P<cause>.+?)(?=(?:，|,|。|？|\?|哪些|哪个|哪位|哪类|谁|什么))")
WHICH_PATTERN = re.compile(r"(哪些|哪个|哪位|哪类)(?P<tail>[^？?。]*)")
STATUS_CHECK_PATTERN = re.compile(
    r"(?:是否(?:存在|有)?|有无|有没有|是否为|是否属于)(?P<status>.+?)(?=(?:情况|问题|现象|记录|表现)?(?:[？?，,。]|如果|并|以及|$))"
)
ASKS_FOR_PATTERN = re.compile(
    r"(?:有什么|有哪些|什么)(?P<target>.+?)(?=(?:[？?，,。]|如果|并|以及|$))"
)
LOOKUP_TARGET_PATTERN = re.compile(
    r"(?P<target>.+?)(?:是|为)(?:多少|什么|几|啥)(?:[分次个元岁条项]?)(?:[？?，,。!！]*)$"
)
UTTERANCE_SPLIT_PATTERN = re.compile(r"[？?！!；;\n]+")
CONDITIONAL_PREFIX_PATTERN = re.compile(
    r"^(?P<prefix>如果有|如果存在|如果是|如果属于|如果命中|若有|若存在|若是|若属于|若命中|如果没有|如果不存在|如果不是|若没有|若不存在|若不是)[，,\s]*(?P<body>.*)$"
)
NUMERIC_COMPARISON_PATTERN = re.compile(
    r"^(?P<attribute>.+?)\s*(?P<op>不超过|不高于|小于等于|<=|≤|至多|最多|小于|低于|少于|<|不少于|不低于|大于等于|>=|≥|至少|起码|高于|大于|超过|多于|>)\s*(?P<value>-?\d+(?:\.\d+)?)\s*$"
)
VALUE_SUFFIX_COMPARISON_PATTERN = re.compile(
    r"^(?P<attribute>.+?)\s*(?P<value>-?\d+(?:\.\d+)?)\s*(?P<op>以上|以下)\s*$"
)
IDENTIFIER_LITERAL_PATTERN = re.compile(r"(?<![A-Za-z0-9])\d{6,}(?![A-Za-z0-9])")
URI_PATTERN = re.compile(r"https?://[^\s<>\"]+")
RESOURCE_LOCAL_NAME_PATTERN = re.compile(r"(?<![A-Za-z0-9_])[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9]+(?![A-Za-z0-9_])")
ROLE_PATTERNS: Dict[str, List[str]] = {
    "id": ["id", "编号", "编码"],
    "name": ["姓名", "名称", "名字", "name"],
    "type": ["类型", "类别", "type"],
    "description": ["描述", "说明", "内容", "detail", "desc"],
    "status": ["状态", "结果", "status"],
    "phone": ["手机号", "手机号码", "电话号码", "联系方式", "phone", "mobile"],
    "score": ["评分", "得分", "score"],
}
META_CLASS_NAMES = {
    "Entity", "Object", "Action", "Logic", "Parameter", "ListType", "DictType",
    "ontologyName", "Thing", "Nothing",
}
EVENT_LIKE_TERMS = ("事件", "工单", "记录", "行为")
QUESTION_STOP_TERMS = ("问题", "情况", "相关", "原因", "导致", "由于", "因为")
SEMANTIC_LABEL_SUFFIXES = ("问题", "事件", "行为", "策略", "分析", "评估", "网络", "产品", "服务")
NUMERIC_XSD_MARKERS = ("int", "integer", "decimal", "float", "double", "long", "short")
SOLUTION_REQUEST_TERMS = ("解决方案", "解决办法", "解决策略", "修复策略", "改善建议", "建议", "怎么办", "怎么解决")
EXPLANATION_REQUEST_TERMS = ("为什么", "为何", "原因", "根因", "导致", "造成")
EXPLANATION_TARGET_TERMS = ("原因", "根因", "成因", "理由")
REFERENCE_MARKERS = ("这个", "这些", "它", "他们", "她们", "上述", "上面", "其中", "分别", "对应的", "相关的")
FAMILY_SLOT_SCHEMAS: Dict[str, List[Dict[str, Any]]] = {
    "causal_enumeration": [
        {"name": "subject_text", "allowed_node_types": ["class"]},
        {"name": "cause_text", "allowed_node_types": ["attribute", "value"]},
        {"name": "action_or_state_text", "allowed_node_types": ["attribute", "value"]},
    ],
    "causal_lookup": [
        {"name": "target_text", "allowed_node_types": ["class"]},
        {"name": "cause_text", "allowed_node_types": ["attribute", "value"]},
        {"name": "action_or_state_text", "allowed_node_types": ["attribute", "value"]},
    ],
    "anchored_causal_lookup": [
        {"name": "anchor_text", "allowed_node_types": ["attribute", "value"]},
        {"name": "target_text", "allowed_node_types": ["class"]},
        {"name": "status_or_problem_text", "allowed_node_types": ["attribute", "value"]},
    ],
    "anchored_fact_lookup": [
        {"name": "anchor_text", "allowed_node_types": ["attribute", "value"]},
        {"name": "target_text", "allowed_node_types": ["class", "attribute", "value"]},
    ],
    "enumeration": [
        {"name": "target_text", "allowed_node_types": ["class", "attribute", "value"]},
        {"name": "action_or_state_text", "allowed_node_types": ["attribute", "value"]},
    ],
    "explanation_enumeration": [
        {"name": "target_text", "allowed_node_types": ["class", "attribute", "value"]},
        {"name": "action_or_state_text", "allowed_node_types": ["attribute", "value"]},
    ],
    "hidden_relation": [
        {"name": "target_text", "allowed_node_types": ["class", "relation"]},
    ],
}
SEMANTIC_VECTOR_DIM = 384
SEMANTIC_SIMILARITY_THRESHOLD = 0.24
SEMANTIC_SCORE_SCALE = 6
NUMERIC_COMPARISON_OPERATOR_MAP = {
    "不超过": "lte",
    "不高于": "lte",
    "小于等于": "lte",
    "<=": "lte",
    "≤": "lte",
    "至多": "lte",
    "最多": "lte",
    "以下": "lte",
    "小于": "lt",
    "低于": "lt",
    "少于": "lt",
    "<": "lt",
    "不少于": "gte",
    "不低于": "gte",
    "大于等于": "gte",
    ">=": "gte",
    "≥": "gte",
    "至少": "gte",
    "起码": "gte",
    "以上": "gte",
    "高于": "gt",
    "大于": "gt",
    "超过": "gt",
    "多于": "gt",
    ">": "gt",
}
NUMERIC_OPERATOR_PREFIX_PATTERN = re.compile(
    r"是否(?=(?:不超过|不高于|小于等于|<=|≤|至多|最多|小于|低于|少于|<|不少于|不低于|大于等于|>=|≥|至少|起码|高于|大于|超过|多于|>))"
)
NUMERIC_TRAILING_UNIT_PATTERN = re.compile(r"(?P<number>-?\d+(?:\.\d+)?)(?:[^\d\s]{1,3})$")


def load_json_payload(payload: Optional[str], payload_file: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load a JSON payload from a string or a file."""
    if payload and payload_file:
        raise SystemExit("Use either --json or --json-file, not both.")
    if payload == "__AUTO__":
        return None
    if payload_file:
        return json.loads(Path(payload_file).read_text(encoding="utf-8"))
    if payload:
        return json.loads(payload)
    return None


def http_request_json(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Send an HTTP request and parse the JSON response without local fallback."""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request) as response:
        raw = response.read().decode("utf-8")

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def curl_request_json(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Use curl as a transport fallback for environments where urllib is sandboxed."""
    marker = "__OBDA_HTTP_STATUS__:"
    command = [
        "curl",
        "-sS",
        "-X",
        method,
        "-H",
        "Accept: application/json",
        "-w",
        f"\n{marker}%{{http_code}}",
        url,
    ]

    if payload is not None:
        command.extend([
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            json.dumps(payload, ensure_ascii=False),
        ])

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or f"curl exited with {result.returncode}"
        raise RuntimeError(stderr)

    raw = result.stdout
    if marker not in raw:
        raise RuntimeError("curl response missing HTTP status marker")

    body, _, status_text = raw.rpartition(f"\n{marker}")
    try:
        status_code = int(status_text.strip())
    except ValueError as exc:
        raise RuntimeError(f"Invalid curl HTTP status: {status_text!r}") from exc

    if status_code >= 400:
        error_body = body.strip()
        if error_body:
            print(error_body, file=sys.stderr)
        raise SystemExit(status_code)

    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return body


def request_json(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Send an HTTP request and parse the JSON response."""
    try:
        return http_request_json(method, url, payload)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        if 500 <= exc.code < 600:
            try:
                return curl_request_json(method, url, payload)
            except RuntimeError as curl_exc:
                print(
                    f"Request failed with HTTP {exc.code}; curl fallback failed: {curl_exc}",
                    file=sys.stderr,
                )
                if error_body:
                    print(error_body, file=sys.stderr)
                raise SystemExit(1) from exc
            except SystemExit as curl_exit:
                if error_body:
                    print(error_body, file=sys.stderr)
                if curl_exit.code in (502, 503, 504):
                    raise SystemExit(1) from exc
                raise
        if error_body:
            print(error_body, file=sys.stderr)
        print(f"HTTP {exc.code} from {url}", file=sys.stderr)
        raise SystemExit(1) from exc
    except urllib.error.URLError as exc:
        reason = str(exc.reason)
        try:
            return curl_request_json(method, url, payload)
        except RuntimeError as curl_exc:
            curl_reason = str(curl_exc)
            if "Operation not permitted" in reason:
                return local_app_request(method, url, payload)
            print(f"Request failed: {exc}; curl fallback failed: {curl_reason}", file=sys.stderr)
            raise SystemExit(1) from exc
        except SystemExit as curl_exit:
            if curl_exit.code in (502, 503, 504):
                print(f"Curl fallback returned HTTP {curl_exit.code} for {url}", file=sys.stderr)
                raise SystemExit(1) from exc
            raise
        print(f"Request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


def local_app_request(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Fallback to calling the local FastAPI app directly when HTTP is sandboxed."""
    try:
        with local_test_client() as client:
            return client_request_json(client, method, url, payload)
    except Exception as exc:
        message = str(exc)
        if "Could not set lock on file" in message:
            raise SystemExit(
                "Local fallback failed because DuckDB is locked by another process. "
                "Use the running HTTP server directly (localhost:8000) or stop the conflicting process."
            ) from exc
        raise SystemExit(f"Local fallback failed: {message}") from exc


def client_request_json(client: Any, method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Execute a request using a shared FastAPI TestClient."""
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    if parsed.query:
        path = f"{path}?{parsed.query}"

    response = client.request(method, path, json=payload)
    if response.status_code >= 400:
        print(response.text, file=sys.stderr)
        raise SystemExit(response.status_code)
    return response.json()


def local_test_client():
    """Create a TestClient for the local reasoning server app."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    @contextlib.contextmanager
    def _client():
        # Keep fallback output machine-readable for downstream tools (jq/head).
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            from fastapi.testclient import TestClient
            from reasoning_server import app
            with TestClient(app) as client:
                yield client

    return _client()


def print_output(data: Any) -> None:
    """Pretty-print a JSON-compatible response."""
    try:
        if isinstance(data, (dict, list)):
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(data)
    except BrokenPipeError:
        raise SystemExit(0)


def write_schema_state(state_file: Path, base_url: str) -> None:
    state = {
        "base_url": base_url,
        "schema_checked_at": int(time.time()),
    }
    state_file.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def clear_schema_state(state_file: Path) -> None:
    if state_file.exists():
        state_file.unlink()


def require_schema_state(state_file: Path, base_url: str, ttl_seconds: int, requested_command: str) -> None:
    message = (
        "Protocol guard: run "
        "'.venv/bin/python .agents/skills/obda-query/scripts/obda_api.py schema' "
        f"first in this turn before '{requested_command}'."
    )
    if not state_file.exists():
        raise SystemExit(message)

    try:
        state = json.loads(state_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(message) from exc

    if state.get("base_url") != base_url:
        raise SystemExit(message)

    schema_checked_at = state.get("schema_checked_at")
    if not isinstance(schema_checked_at, int):
        raise SystemExit(message)

    if int(time.time()) - schema_checked_at > ttl_seconds:
        raise SystemExit(message)


def normalize_run_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalize a run plan against a supported template."""
    template = plan.get("template", "custom")
    template_config = RUN_TEMPLATES.get(template)
    if template_config is None:
        supported = ", ".join(sorted(RUN_TEMPLATES))
        raise SystemExit(f"Unknown run template: {template}. Supported templates: {supported}")

    normalized = dict(plan)
    normalized["template"] = template

    if template_config["requires_sparql"] and "sparql" not in normalized:
        raise SystemExit(f"Run template '{template}' requires a sparql section")
    if template_config["requires_analysis"] and "analysis" not in normalized:
        default_kind = template_config["default_analysis_kind"]
        if not default_kind:
            raise SystemExit(f"Run template '{template}' requires an analysis section")
        normalized["analysis"] = {
            "kind": default_kind,
            "payload": {},
        }

    analysis_spec = normalized.get("analysis")
    if analysis_spec is not None:
        analysis_spec = dict(analysis_spec)
        if not analysis_spec.get("kind") and template_config["default_analysis_kind"]:
            analysis_spec["kind"] = template_config["default_analysis_kind"]
        if template == "causal_enumeration":
            analysis_spec["kind"] = "paths-batch"
        elif template == "causal_lookup":
            analysis_spec["kind"] = "paths"
        normalized["analysis"] = analysis_spec

    if template == "schema_inspect" and not any(key in normalized for key in ("samples", "sparql", "analysis")):
        normalized["samples"] = []

    return normalized


def is_question_shorthand_plan(plan: Optional[Dict[str, Any]]) -> bool:
    """Detect JSON plans that are really planning-mode shorthand in disguise."""
    if not isinstance(plan, dict):
        return False
    if not isinstance(plan.get("question"), str) or not plan.get("question"):
        return False
    return not any(key in plan for key in ("samples", "sparql", "analysis"))


def is_question_routed_plan(plan: Optional[Dict[str, Any]]) -> bool:
    """Detect plans that should be forced through question-mode planner execution."""
    if not isinstance(plan, dict):
        return False
    question = plan.get("question")
    if not isinstance(question, str) or not question:
        return False
    template = plan.get("template", "custom")
    return isinstance(template, str) and template in RUN_TEMPLATES and template != "custom"


def is_uri_like(value: Any) -> bool:
    return isinstance(value, str) and (value.startswith("http://") or value.startswith("https://"))


def schema_indexes(schema: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Index schema terms by local name for fast validation/compilation."""
    indexes = {
        "classes": {},
        "data_properties": {},
        "object_properties": {},
    }
    if not isinstance(schema, dict):
        return indexes

    for bucket_name in ("classes", "data_properties", "object_properties"):
        items = schema.get(bucket_name)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            local_name = item.get("local_name")
            if isinstance(local_name, str) and local_name:
                indexes[bucket_name][local_name] = item

    for local_name, item in load_runtime_data_property_catalog().items():
        if local_name not in indexes["data_properties"]:
            indexes["data_properties"][local_name] = item
    return indexes


def primary_namespace_from_schema(schema: Optional[Dict[str, Any]]) -> str:
    """Infer the primary ex: namespace from schema URIs."""
    if not isinstance(schema, dict):
        return "http://example.com/ontology#"

    for bucket_name in ("classes", "data_properties", "object_properties"):
        items = schema.get(bucket_name)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            uri = item.get("uri")
            local_name = item.get("local_name")
            if not isinstance(uri, str) or not isinstance(local_name, str):
                continue
            if "#" in uri:
                return uri.rsplit("#", 1)[0] + "#"
            if uri.endswith(local_name):
                return uri[: -len(local_name)]
    return "http://example.com/ontology#"


def infer_class_local_name_from_uri_template(uri_or_template: Any) -> Optional[str]:
    """Infer a class-ish local name from a mapping IRI template like customer_$(id)."""
    local_name = uri_local_name(uri_or_template)
    if not isinstance(local_name, str) or not local_name:
        return None

    for marker in ("_$(", "$("):
        if marker in local_name:
            inferred = local_name.split(marker, 1)[0]
            return inferred or None
    return None


def load_runtime_object_property_catalog(mapping_file: Path = DEFAULT_MAPPING_FILE) -> Dict[str, List[Dict[str, Any]]]:
    """Build a lightweight runtime relation catalog from mapping.yaml."""
    cache = getattr(load_runtime_object_property_catalog, "_cache", None)
    resolved_path = mapping_file.resolve()
    try:
        mtime_ns = resolved_path.stat().st_mtime_ns
    except FileNotFoundError:
        return {}

    if (
        isinstance(cache, dict)
        and cache.get("path") == str(resolved_path)
        and cache.get("mtime_ns") == mtime_ns
        and isinstance(cache.get("catalog"), dict)
    ):
        return cache["catalog"]

    catalog: Dict[str, List[Dict[str, Any]]] = {}
    current_mapping = None
    subject_template = None
    subject_class = None
    pending_predicate = None
    pending_object_value = None

    def flush_pending_relation() -> None:
        nonlocal pending_predicate, pending_object_value
        if not pending_predicate or not pending_object_value:
            pending_predicate = None
            pending_object_value = None
            return

        predicate_local_name = uri_local_name(pending_predicate)
        source_class = subject_class or infer_class_local_name_from_uri_template(subject_template)
        target_class = infer_class_local_name_from_uri_template(pending_object_value)
        if predicate_local_name and source_class and target_class:
            bucket = catalog.setdefault(predicate_local_name, [])
            candidate = {
                "property": predicate_local_name,
                "predicate_uri": pending_predicate,
                "source_class": source_class,
                "target_class": target_class,
                "mapping": current_mapping,
            }
            if candidate not in bucket:
                bucket.append(candidate)

        pending_predicate = None
        pending_object_value = None

    for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
        match = MAPPING_NAME_PATTERN.match(raw_line)
        if match:
            flush_pending_relation()
            current_mapping = match.group(1)
            subject_template = None
            subject_class = None
            continue

        match = MAPPING_SUBJECT_PATTERN.match(raw_line)
        if match:
            subject_template = match.group(1)
            continue

        match = MAPPING_TYPE_PATTERN.match(raw_line)
        if match:
            subject_class = uri_local_name(match.group(1)) or infer_class_local_name_from_uri_template(match.group(1))
            continue

        match = MAPPING_PREDICATE_PATTERN.match(raw_line)
        if match:
            flush_pending_relation()
            pending_predicate = match.group(1)
            pending_object_value = None
            continue

        match = MAPPING_OBJECT_VALUE_PATTERN.match(raw_line)
        if match and pending_predicate:
            pending_object_value = match.group(1)
            continue

        if MAPPING_OBJECT_IRI_TYPE_PATTERN.match(raw_line):
            flush_pending_relation()

    flush_pending_relation()
    load_runtime_object_property_catalog._cache = {
        "path": str(resolved_path),
        "mtime_ns": mtime_ns,
        "catalog": catalog,
    }
    return catalog


def load_runtime_data_property_catalog(mapping_file: Path = DEFAULT_MAPPING_FILE) -> Dict[str, Dict[str, Any]]:
    """Build a lightweight runtime data-property catalog from mapping.yaml."""
    cache = getattr(load_runtime_data_property_catalog, "_cache", None)
    resolved_path = mapping_file.resolve()
    try:
        mtime_ns = resolved_path.stat().st_mtime_ns
    except FileNotFoundError:
        return {}

    if (
        isinstance(cache, dict)
        and cache.get("path") == str(resolved_path)
        and cache.get("mtime_ns") == mtime_ns
        and isinstance(cache.get("catalog"), dict)
    ):
        return cache["catalog"]

    namespace = "http://ywyinfo.com/example-owl#"
    catalog: Dict[str, Dict[str, Any]] = {}
    current_mapping = None
    subject_template = None
    subject_class = None

    for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
        match = MAPPING_NAME_PATTERN.match(raw_line)
        if match:
            current_mapping = match.group(1)
            subject_template = None
            subject_class = None
            continue

        match = MAPPING_SUBJECT_PATTERN.match(raw_line)
        if match:
            subject_template = match.group(1)
            continue

        match = MAPPING_TYPE_PATTERN.match(raw_line)
        if match:
            subject_class = uri_local_name(match.group(1)) or infer_class_local_name_from_uri_template(match.group(1))
            continue

        match = MAPPING_PO_ARRAY_PATTERN.match(raw_line)
        if not match:
            continue

        predicate_uri, _, object_type = match.groups()
        if predicate_uri == "a":
            continue
        local_name = uri_local_name(predicate_uri)
        domain_class = subject_class or infer_class_local_name_from_uri_template(subject_template)
        if not local_name or not domain_class:
            continue

        if isinstance(object_type, str) and object_type.strip().lower() == "iri":
            continue

        catalog[local_name] = {
            "uri": predicate_uri,
            "label": local_name,
            "local_name": local_name,
            "domain": f"{namespace}{domain_class}",
            "range": object_type.strip() if isinstance(object_type, str) and object_type.strip() else None,
            "mapping": current_mapping,
            "validation_source": "mapping",
        }

    load_runtime_data_property_catalog._cache = {
        "path": str(resolved_path),
        "mtime_ns": mtime_ns,
        "catalog": catalog,
    }
    return catalog


def combined_object_property_names(indexes: Dict[str, Dict[str, Dict[str, Any]]]) -> List[str]:
    """Return schema-declared and runtime-mapped object property names."""
    runtime_names = set(load_runtime_object_property_catalog())
    return sorted(set(indexes["object_properties"]) | runtime_names)


def sparql_string_literal(value: Any) -> str:
    """Render a string literal usable in SPARQL."""
    return json.dumps(str(value), ensure_ascii=False)


def extract_select_vars(query_text: str) -> Set[str]:
    """Extract selected variable names from a simple SELECT query."""
    match = SELECT_VARS_PATTERN.search(query_text)
    if not match:
        return set()
    return {var.lstrip("?") for var in re.findall(r"\?[A-Za-z_][A-Za-z0-9_]*", match.group(1))}


def validate_schema_local_name(local_name: str, bucket: Dict[str, Dict[str, Any]], kind: str) -> None:
    """Ensure a schema local name exists in the expected bucket."""
    if local_name not in bucket:
        supported = ", ".join(sorted(bucket)[:20])
        raise SystemExit(f"Unknown {kind} '{local_name}' in schema. Sample supported values: {supported}")


def compile_filter_expression(filter_spec: Dict[str, Any]) -> str:
    """Compile a structured filter clause into a SPARQL FILTER expression fragment."""
    if "any_of" in filter_spec:
        items = filter_spec.get("any_of")
        if not isinstance(items, list) or not items:
            raise SystemExit("any_of filter requires a non-empty list")
        return "(" + " || ".join(compile_filter_expression(item) for item in items) + ")"

    if "all_of" in filter_spec:
        items = filter_spec.get("all_of")
        if not isinstance(items, list) or not items:
            raise SystemExit("all_of filter requires a non-empty list")
        return "(" + " && ".join(compile_filter_expression(item) for item in items) + ")"

    var_name = filter_spec.get("var")
    if not isinstance(var_name, str) or not var_name:
        raise SystemExit("Each builder filter requires a non-empty 'var'")

    op = filter_spec.get("op", "equals")
    case_insensitive = bool(filter_spec.get("case_insensitive", op.startswith("contains")))
    lhs = f"STR(?{var_name})"
    lhs_contains = f"LCASE({lhs})" if case_insensitive else lhs

    def literal(value: Any) -> str:
        text = str(value)
        return sparql_string_literal(text.lower() if case_insensitive and op.startswith("contains") else text)

    if op == "contains":
        if "value" not in filter_spec:
            raise SystemExit("contains filter requires 'value'")
        return f"CONTAINS({lhs_contains}, {literal(filter_spec['value'])})"

    if op == "contains_any":
        values = filter_spec.get("values")
        if not isinstance(values, list) or not values:
            raise SystemExit("contains_any filter requires non-empty 'values'")
        parts = [f"CONTAINS({lhs_contains}, {literal(value)})" for value in values]
        return "(" + " || ".join(parts) + ")"

    if op == "contains_all":
        values = filter_spec.get("values")
        if not isinstance(values, list) or not values:
            raise SystemExit("contains_all filter requires non-empty 'values'")
        parts = [f"CONTAINS({lhs_contains}, {literal(value)})" for value in values]
        return "(" + " && ".join(parts) + ")"

    if op == "equals":
        if "value" not in filter_spec:
            raise SystemExit("equals filter requires 'value'")
        rhs = sparql_string_literal(str(filter_spec["value"]))
        return f"{lhs} = {rhs}"

    if op in {"lt", "lte", "gt", "gte"}:
        if "value" not in filter_spec:
            raise SystemExit(f"{op} filter requires 'value'")
        numeric_lhs = f"?{var_name}"
        value = filter_spec["value"]
        rhs = str(value) if isinstance(value, (int, float)) else sparql_string_literal(value)
        operator_map = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}
        return f"{numeric_lhs} {operator_map[op]} {rhs}"

    if op == "in":
        values = filter_spec.get("values")
        if not isinstance(values, list) or not values:
            raise SystemExit("in filter requires non-empty 'values'")
        rhs_values = ", ".join(sparql_string_literal(value) for value in values)
        return f"{lhs} IN ({rhs_values})"

    raise SystemExit(f"Unsupported builder filter op: {op}")


def validate_filter_spec_tree(filter_spec: Dict[str, Any], selected_vars: Set[str]) -> None:
    """Validate a possibly nested filter tree against selected vars."""
    if not isinstance(filter_spec, dict):
        raise SystemExit("Each sparql.builder.filters item must be an object")

    for group_key in ("any_of", "all_of"):
        if group_key in filter_spec:
            items = filter_spec.get(group_key)
            if not isinstance(items, list) or not items:
                raise SystemExit(f"{group_key} filter requires a non-empty list")
            for item in items:
                validate_filter_spec_tree(item, selected_vars)
            return

    filter_var = filter_spec.get("var")
    if filter_var not in selected_vars:
        raise SystemExit(
            f"builder filter references unknown var '{filter_var}'. Define it in sparql.builder.select first."
        )


def collect_filter_vars(filter_spec: Dict[str, Any]) -> Set[str]:
    """Collect all referenced vars from a nested builder filter tree."""
    if not isinstance(filter_spec, dict):
        return set()
    for group_key in ("any_of", "all_of"):
        if group_key in filter_spec:
            vars_in_group: Set[str] = set()
            for item in filter_spec.get(group_key, []):
                vars_in_group.update(collect_filter_vars(item))
            return vars_in_group
    filter_var = filter_spec.get("var")
    return {filter_var} if isinstance(filter_var, str) and filter_var else set()


def mark_optional_display_selects(
    select_specs: List[Dict[str, Any]],
    filter_specs: List[Dict[str, Any]],
    order_by: List[str],
) -> List[Dict[str, Any]]:
    """Mark non-filtered, non-ordered display fields as optional builder selects."""
    required_vars: Set[str] = {var_name for var_name in order_by if isinstance(var_name, str) and var_name}
    for filter_spec in filter_specs:
        required_vars.update(collect_filter_vars(filter_spec))

    normalized: List[Dict[str, Any]] = []
    for item in select_specs:
        if not isinstance(item, dict):
            continue
        updated = dict(item)
        var_name = updated.get("var")
        if isinstance(var_name, str) and var_name and var_name not in required_vars:
            updated["optional"] = True
        normalized.append(updated)
    return normalized


def resolve_builder_link_direction(
    source_class: str,
    evidence_class: str,
    link_property: Optional[str],
    indexes: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Resolve or infer the object property that links source and evidence classes."""
    object_properties = indexes["object_properties"]
    runtime_object_properties = load_runtime_object_property_catalog()

    def matches(prop: Dict[str, Any], domain_class: str, range_class: str) -> bool:
        domain = prop.get("domain")
        range_ = prop.get("range")
        return (
            isinstance(domain, str)
            and isinstance(range_, str)
            and uri_local_name(domain) == domain_class
            and uri_local_name(range_) == range_class
        )

    def runtime_matches(property_name: str, domain_class: str, range_class: str) -> bool:
        return any(
            edge.get("source_class") == domain_class and edge.get("target_class") == range_class
            for edge in runtime_object_properties.get(property_name, [])
        )

    def select_best_candidate(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None

        forward_runtime = [
            item for item in candidates
            if item.get("validation_source") == "mapping" and item.get("direction") == "forward"
        ]
        if len(forward_runtime) == 1:
            return forward_runtime[0]
        if len(forward_runtime) > 1:
            return None

        runtime_any = [item for item in candidates if item.get("validation_source") == "mapping"]
        if len(runtime_any) == 1:
            return runtime_any[0]
        if len(runtime_any) > 1:
            return None

        forward_schema = [
            item for item in candidates
            if item.get("validation_source") == "schema" and item.get("direction") == "forward"
        ]
        if len(forward_schema) == 1:
            return forward_schema[0]
        if len(forward_schema) > 1:
            return None

        schema_any = [item for item in candidates if item.get("validation_source") == "schema"]
        if len(schema_any) == 1:
            return schema_any[0]
        return None

    if link_property:
        if link_property in object_properties:
            prop = object_properties[link_property]
            if matches(prop, source_class, evidence_class):
                return {"property": link_property, "direction": "forward", "validation_source": "schema"}
            if matches(prop, evidence_class, source_class):
                return {"property": link_property, "direction": "reverse", "validation_source": "schema"}
        if runtime_matches(link_property, source_class, evidence_class):
            return {"property": link_property, "direction": "forward", "validation_source": "mapping"}
        if runtime_matches(link_property, evidence_class, source_class):
            return {"property": link_property, "direction": "reverse", "validation_source": "mapping"}
        supported = ", ".join(combined_object_property_names(indexes)[:20])
        raise SystemExit(
            f"Object property '{link_property}' does not connect {source_class} and {evidence_class} "
            f"according to schema declarations or runtime mapping relations. Sample supported values: {supported}"
        )

    candidates = []
    for local_name, prop in object_properties.items():
        if matches(prop, source_class, evidence_class):
            candidates.append({"property": local_name, "direction": "forward", "validation_source": "schema"})
        elif matches(prop, evidence_class, source_class):
            candidates.append({"property": local_name, "direction": "reverse", "validation_source": "schema"})

    for local_name, edges in runtime_object_properties.items():
        if any(edge.get("source_class") == source_class and edge.get("target_class") == evidence_class for edge in edges):
            candidates.append({"property": local_name, "direction": "forward", "validation_source": "mapping"})
        if any(edge.get("source_class") == evidence_class and edge.get("target_class") == source_class for edge in edges):
            candidates.append({"property": local_name, "direction": "reverse", "validation_source": "mapping"})

    deduped_candidates = []
    seen = set()
    for item in candidates:
        key = (item["property"], item["direction"])
        if key in seen:
            continue
        seen.add(key)
        deduped_candidates.append(item)
    candidates = deduped_candidates

    if not candidates:
        raise SystemExit(
            f"No object property in schema or runtime mappings connects {source_class} and {evidence_class}."
        )
    chosen = select_best_candidate(candidates)
    if chosen is not None:
        return chosen
    if len(candidates) > 1:
        rendered = ", ".join(
            f"{item['property']} ({item['direction']}, {item['validation_source']})" for item in candidates
        )
        raise SystemExit(
            f"Multiple link-property candidates connect {source_class} and {evidence_class}: {rendered}. "
            "Set sparql.builder.link_property explicitly."
        )
    return candidates[0]


def compile_sparql_builder(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Dict[str, Any],
    template: str,
) -> Dict[str, Any]:
    """Compile a structured builder spec into SPARQL and validate it against schema."""
    builder = sparql_spec.get("builder")
    if not isinstance(builder, dict):
        raise SystemExit("sparql.builder must be an object when provided.")

    indexes = schema_indexes(schema)
    class_index = indexes["classes"]
    data_index = indexes["data_properties"]
    object_index = indexes["object_properties"]
    runtime_object_index = load_runtime_object_property_catalog()
    namespace = primary_namespace_from_schema(schema)

    source_class = builder.get("source_class")
    evidence_class = builder.get("evidence_class") or builder.get("target_class")
    source_var = builder.get("source_var", "source")
    evidence_var = builder.get("evidence_var") or builder.get("target_var", "evidence")

    if not isinstance(source_class, str) or not source_class:
        raise SystemExit("sparql.builder.source_class is required")
    if not isinstance(evidence_class, str) or not evidence_class:
        raise SystemExit("sparql.builder.evidence_class is required")
    if not isinstance(source_var, str) or not source_var:
        raise SystemExit("sparql.builder.source_var must be a non-empty string")
    if not isinstance(evidence_var, str) or not evidence_var:
        raise SystemExit("sparql.builder.evidence_var must be a non-empty string")

    validate_schema_local_name(source_class, class_index, "class")
    validate_schema_local_name(evidence_class, class_index, "class")

    link_info = resolve_builder_link_direction(
        source_class,
        evidence_class,
        builder.get("link_property"),
        indexes,
    )

    select_specs = builder.get("select", [])
    if not isinstance(select_specs, list):
        raise SystemExit("sparql.builder.select must be a list")

    normalized_selects: List[Dict[str, Any]] = []
    for uri_var in (source_var, evidence_var):
        normalized_selects.append({"var": uri_var, "kind": "uri"})

    for item in select_specs:
        if not isinstance(item, dict):
            raise SystemExit("Each sparql.builder.select item must be an object")
        var_name = item.get("var")
        if not isinstance(var_name, str) or not var_name:
            raise SystemExit("Each sparql.builder.select item requires non-empty 'var'")
        if any(existing.get("var") == var_name for existing in normalized_selects):
            continue
        normalized_selects.append(dict(item))

    pattern_map: Dict[str, None] = {}

    def add_pattern(text: str) -> None:
        pattern_map[text] = None

    add_pattern(f"?{source_var} a ex:{source_class} .")
    add_pattern(f"?{evidence_var} a ex:{evidence_class} .")
    if link_info["direction"] == "forward":
        add_pattern(f"?{source_var} ex:{link_info['property']} ?{evidence_var} .")
    else:
        add_pattern(f"?{evidence_var} ex:{link_info['property']} ?{source_var} .")

    valid_subjects = {
        "source": source_var,
        source_var: source_var,
        "evidence": evidence_var,
        "target": evidence_var,
        evidence_var: evidence_var,
    }
    selected_vars = {item["var"] for item in normalized_selects}

    for item in normalized_selects:
        if item.get("kind") == "uri":
            continue
        subject_name = item.get("subject")
        property_name = item.get("property")
        var_name = item["var"]
        optional_select = bool(item.get("optional"))
        if not isinstance(subject_name, str) or subject_name not in valid_subjects:
            raise SystemExit(
                f"builder select '{var_name}' requires subject to be one of: {', '.join(sorted(valid_subjects))}"
            )
        if not isinstance(property_name, str) or not property_name:
            raise SystemExit(f"builder select '{var_name}' requires non-empty 'property'")
        if property_name not in data_index and property_name not in object_index and property_name not in runtime_object_index:
            raise SystemExit(f"Unknown property '{property_name}' in builder select '{var_name}'")
        triple_pattern = f"?{valid_subjects[subject_name]} ex:{property_name} ?{var_name} ."
        if optional_select:
            add_pattern(f"OPTIONAL {{ {triple_pattern} }}")
        else:
            add_pattern(triple_pattern)

    filter_specs = builder.get("filters", [])
    if not isinstance(filter_specs, list):
        raise SystemExit("sparql.builder.filters must be a list")
    filter_expressions = []
    for filter_spec in filter_specs:
        validate_filter_spec_tree(filter_spec, selected_vars)
        filter_expressions.append(compile_filter_expression(filter_spec))

    distinct = bool(builder.get("distinct", True))
    order_by = builder.get("order_by", [])
    if not isinstance(order_by, list):
        raise SystemExit("sparql.builder.order_by must be a list")
    for var_name in order_by:
        if not isinstance(var_name, str) or not var_name:
            raise SystemExit("Each sparql.builder.order_by item must be a non-empty string")
        if var_name not in selected_vars:
            raise SystemExit(f"order_by references unknown var '{var_name}'")

    limit = builder.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except (TypeError, ValueError) as exc:
            raise SystemExit("sparql.builder.limit must be an integer") from exc
        if limit <= 0:
            raise SystemExit("sparql.builder.limit must be > 0")

    select_clause = " ".join(f"?{item['var']}" for item in normalized_selects)
    where_lines = [f"  {pattern}" for pattern in pattern_map]
    if filter_expressions:
        where_lines.append("  FILTER(" + " && ".join(filter_expressions) + ")")

    query_lines = [
        f"PREFIX ex: <{namespace}>",
        f"SELECT {'DISTINCT ' if distinct else ''}{select_clause}",
        "WHERE {",
        *where_lines,
        "}",
    ]
    if order_by:
        query_lines.append("ORDER BY " + " ".join(f"?{var_name}" for var_name in order_by))
    if limit is not None:
        query_lines.append(f"LIMIT {limit}")

    return {
        "query": "\n".join(query_lines),
        "source_var": sparql_spec.get("source_var") or source_var,
        "builder_meta": {
            "mode": "builder",
            "source_class": source_class,
            "evidence_class": evidence_class,
            "link_property": link_info["property"],
            "direction": link_info["direction"],
            "validation_source": link_info.get("validation_source"),
            "selected_vars": [item["var"] for item in normalized_selects],
        },
    }


def validate_raw_sparql_query(
    query_text: str,
    schema: Optional[Dict[str, Any]],
    sparql_spec: Dict[str, Any],
    template: str,
) -> Dict[str, Any]:
    """Run lightweight schema validation on a raw SPARQL query before execution."""
    indexes = schema_indexes(schema)
    runtime_object_names = set(load_runtime_object_property_catalog())
    runtime_data_names = set(load_runtime_data_property_catalog())
    known_terms = (
        set(indexes["classes"])
        | set(indexes["data_properties"])
        | set(indexes["object_properties"])
        | runtime_object_names
        | runtime_data_names
    )
    used_terms = {term for term in EX_LOCAL_NAME_PATTERN.findall(query_text) if term}
    unknown_terms = sorted(term for term in used_terms if term not in known_terms)
    if unknown_terms:
        raise SystemExit(
            "Raw SPARQL references unknown schema terms: " + ", ".join(unknown_terms)
        )

    source_var = sparql_spec.get("source_var")
    select_vars = extract_select_vars(query_text)
    if template in ("causal_lookup", "causal_enumeration"):
        if not isinstance(source_var, str) or not source_var:
            raise SystemExit(
                "Raw SPARQL for causal templates requires sparql.source_var so the client can validate and auto-derive analyzer anchors. "
                "Prefer sparql.builder when the query shape matches source class -> evidence class -> filters."
            )
        if select_vars and source_var not in select_vars:
            raise SystemExit(
                f"sparql.source_var='{source_var}' is not projected by the SELECT clause. "
                "Return the anchor URI column or remove source_var."
            )

    return {
        "query": query_text,
        "source_var": source_var,
        "builder_meta": {
            "mode": "raw",
            "source_var": source_var,
            "selected_vars": sorted(select_vars),
            "runtime_validated_terms": sorted(term for term in used_terms if term in runtime_object_names and term not in indexes["object_properties"]),
        },
    }


def prepare_sparql_spec(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Dict[str, Any],
    template: str,
) -> Dict[str, Any]:
    """Prepare a SPARQL spec for execution using either raw query or structured builder mode."""
    if sparql_spec.get("builder") is not None:
        return compile_sparql_builder(schema, sparql_spec, template)

    query_text = sparql_spec.get("query")
    query_file = sparql_spec.get("query_file")
    if query_text and query_file:
        raise SystemExit("sparql plan accepts either query or query_file, not both")
    if query_file:
        query_text = Path(query_file).read_text(encoding="utf-8")
    if not query_text:
        raise SystemExit("sparql plan requires query, query_file, or builder")

    return validate_raw_sparql_query(query_text, schema, sparql_spec, template)


def derive_uri_sources_from_sparql(
    sparql_response: Optional[Dict[str, Any]],
    preferred_var: Optional[str] = None,
    multiple: bool = False,
) -> Dict[str, Any]:
    """Derive source URI values from SPARQL rows for analyzer convenience."""
    if not isinstance(sparql_response, dict):
        return {"values": [], "source_var": None}

    rows = sparql_response.get("results")
    if not isinstance(rows, list) or not rows:
        return {"values": [], "source_var": None}

    chosen_var = None
    if preferred_var:
        preferred_values = [row.get(preferred_var) for row in rows]
        preferred_values = [value for value in preferred_values if is_uri_like(value)]
        if preferred_values:
            chosen_var = preferred_var
            values = list(dict.fromkeys(preferred_values))
            return {"values": values if multiple else values[:1], "source_var": chosen_var}

    first_row = rows[0]
    for candidate_var in first_row.keys():
        candidate_values = []
        valid_column = False
        for row in rows:
            value = row.get(candidate_var)
            if value is None:
                continue
            if not is_uri_like(value):
                candidate_values = []
                valid_column = False
                break
            candidate_values.append(value)
            valid_column = True
        if valid_column and candidate_values:
            chosen_var = candidate_var
            values = list(dict.fromkeys(candidate_values))
            return {"values": values if multiple else values[:1], "source_var": chosen_var}

    return {"values": [], "source_var": None}


def normalize_match_text(value: Any) -> str:
    """Normalize labels/local names/question fragments for lightweight lexical matching."""
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^\w\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def unique_preserve_order(items: List[str]) -> List[str]:
    """Deduplicate a list while preserving order."""
    seen: Set[str] = set()
    results: List[str] = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        results.append(item)
    return results


def rank_value_catalog_classes(
    source_info: Dict[str, Any],
    evidence_candidates: List[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
    limit: int = 4,
) -> List[str]:
    """Rank classes for bounded value-catalog sampling using source/evidence/binding relevance."""
    class_scores: Dict[str, float] = {}
    class_order: Dict[str, int] = {}
    order_counter = 0

    def add_candidate(class_name: Optional[str], score: float) -> None:
        nonlocal order_counter
        if not isinstance(class_name, str) or not class_name:
            return
        class_scores[class_name] = class_scores.get(class_name, 0.0) + float(score)
        if class_name not in class_order:
            class_order[class_name] = order_counter
            order_counter += 1

    selected_source = source_info.get("selected") if isinstance(source_info, dict) else None
    if isinstance(selected_source, dict):
        add_candidate(selected_source.get("class_name"), float(selected_source.get("score", 0.0) or 0.0) + 3.0)

    for evidence in evidence_candidates[:4]:
        if not isinstance(evidence, dict):
            continue
        add_candidate(evidence.get("class_name"), float(evidence.get("score", 0.0) or 0.0) + 1.0)

    for binding in slot_bindings:
        if not isinstance(binding, dict):
            continue
        slot_name = binding.get("slot_name")
        slot_bias = 0.0
        if slot_name == "anchor_text":
            slot_bias = 4.0
        elif slot_name == "status_or_problem_text":
            slot_bias = 3.0
        elif slot_name in {"cause_text", "action_or_state_text"}:
            slot_bias = 2.0
        elif slot_name in {"subject_text", "target_text"}:
            slot_bias = 1.0
        for rank, candidate in enumerate(binding.get("candidates", [])[:3]):
            if not isinstance(candidate, dict):
                continue
            class_name = candidate.get("class_name")
            candidate_score = float(candidate.get("total_score", 0.0) or 0.0)
            add_candidate(class_name, slot_bias + (candidate_score / float(rank + 1)))

    ranked = sorted(
        class_scores.items(),
        key=lambda item: (-item[1], class_order.get(item[0], 0), item[0]),
    )
    return [class_name for class_name, _score in ranked[:limit]]


def infer_class_hint_from_anchor(value: str) -> Optional[str]:
    """Infer a class-like prefix from a URI or ontology-style local resource name."""
    if is_uri_like(value):
        return class_key_from_uri(value)
    if "_" in value:
        return value.split("_", 1)[0]
    return None


def detect_question_anchors(question_text: str) -> List[Dict[str, str]]:
    """Detect strong anchors from natural language without question-specific planner logic."""
    anchors: List[Dict[str, str]] = []
    seen: Set[tuple[str, str]] = set()

    def add_anchor(kind: str, value: str) -> None:
        cleaned = value.strip(" ，,。；;()[]{}<>\"'")
        if not cleaned:
            return
        key = (kind, cleaned)
        if key in seen:
            return
        seen.add(key)
        anchor = {"kind": kind, "value": cleaned}
        class_hint = infer_class_hint_from_anchor(cleaned)
        if isinstance(class_hint, str) and class_hint:
            anchor["class_hint"] = class_hint
        anchors.append(anchor)

    for match in IDENTIFIER_LITERAL_PATTERN.finditer(question_text):
        add_anchor("identifier_like_literal", match.group(0))
    for match in URI_PATTERN.finditer(question_text):
        add_anchor("resource_uri", match.group(0))
    for match in RESOURCE_LOCAL_NAME_PATTERN.finditer(question_text):
        candidate = match.group(0)
        if URI_PATTERN.match(candidate):
            continue
        add_anchor("resource_local_name", candidate)

    return anchors


def normalized_terms_from_text(value: Any) -> List[str]:
    """Generate lexical terms from a schema label/local name."""
    if value is None:
        return []
    raw = str(value).strip()
    normalized = normalize_match_text(raw)
    terms = [raw, normalized]
    if "_" in raw:
        parts = [part for part in raw.split("_") if part]
        terms.extend(parts)
    if " " in normalized:
        terms.extend(part for part in normalized.split(" ") if part)
    return unique_preserve_order([term for term in terms if term])


def compact_match_text(value: Any) -> str:
    """Compact normalized text for generic substring / ngram overlap checks."""
    return normalize_match_text(value).replace(" ", "")


def char_ngram_overlap_score(slot_text: str, search_text: str) -> int:
    """Score generic character-n-gram overlap without domain-specific lexicons."""
    compact_slot = compact_match_text(slot_text)
    compact_search = compact_match_text(search_text)
    if not compact_slot or not compact_search:
        return 0

    score = 0
    seen: Set[str] = set()
    for size, weight in ((4, 4), (3, 3), (2, 1)):
        if len(compact_slot) < size:
            continue
        for start in range(len(compact_slot) - size + 1):
            ngram = compact_slot[start:start + size]
            if ngram in seen:
                continue
            if ngram in compact_search:
                seen.add(ngram)
                score += weight
    return score


def semantic_features_from_text(value: Any) -> List[str]:
    """Build stable subword/token features for local vector retrieval."""
    normalized = normalize_match_text(value)
    if not normalized:
        return []

    tokens = [token for token in normalized.split(" ") if token]
    compact = normalized.replace(" ", "")
    features: List[str] = [f"full:{normalized}"]
    features.extend(f"tok:{token}" for token in tokens)
    for idx in range(len(tokens) - 1):
        features.append(f"bigram:{tokens[idx]}::{tokens[idx + 1]}")
    for size in (2, 3):
        if len(compact) < size:
            continue
        for start in range(len(compact) - size + 1):
            features.append(f"char{size}:{compact[start:start + size]}")
    return features


def hashed_text_vector(value: Any, dim: int = SEMANTIC_VECTOR_DIM) -> Optional[Dict[str, Any]]:
    """Encode text into a small deterministic hashed vector."""
    features = semantic_features_from_text(value)
    if not features:
        return None

    vector = np.zeros(dim, dtype=np.float32)
    for feature in features:
        checksum = zlib.crc32(feature.encode("utf-8"))
        index = checksum % dim
        sign = 1.0 if ((checksum >> 31) & 1) == 0 else -1.0
        vector[index] += sign

    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        return None
    return {"vector": vector, "norm": norm}


def semantic_cosine_similarity(slot_vector: Optional[Dict[str, Any]], node_vector: Optional[Dict[str, Any]]) -> float:
    """Compute cosine similarity for two hashed text vectors."""
    if not isinstance(slot_vector, dict) or not isinstance(node_vector, dict):
        return 0.0
    left = slot_vector.get("vector")
    right = node_vector.get("vector")
    left_norm = float(slot_vector.get("norm", 0.0) or 0.0)
    right_norm = float(node_vector.get("norm", 0.0) or 0.0)
    if not isinstance(left, np.ndarray) or not isinstance(right, np.ndarray):
        return 0.0
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return float(np.dot(left, right) / (left_norm * right_norm))


def with_semantic_vector_index(manifest: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Attach a local hashed-vector index to manifest nodes for binder retrieval."""
    if not isinstance(manifest, dict):
        return {"semantic_vector_index": {}, "semantic_vector_dim": SEMANTIC_VECTOR_DIM}

    updated = dict(manifest)
    index: Dict[str, Dict[str, Any]] = {}
    for collection_name in ("class_nodes", "attribute_nodes", "relation_nodes", "value_nodes"):
        for node in updated.get(collection_name, []):
            if not isinstance(node, dict):
                continue
            node_id = node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue
            vector_entry = hashed_text_vector(node.get("search_text", ""))
            if vector_entry is not None:
                index[node_id] = vector_entry
    updated["semantic_vector_index"] = index
    updated["semantic_vector_dim"] = SEMANTIC_VECTOR_DIM
    return updated


def semantic_node_match_score(slot_text: str, node: Dict[str, Any], manifest: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """Score a slot text against a manifest node using local vector similarity."""
    if not isinstance(manifest, dict):
        return {"semantic_similarity": 0.0, "semantic_score": 0.0}

    node_id = node.get("node_id")
    index = manifest.get("semantic_vector_index", {})
    if not isinstance(node_id, str) or not isinstance(index, dict):
        return {"semantic_similarity": 0.0, "semantic_score": 0.0}

    slot_vector = hashed_text_vector(slot_text, dim=int(manifest.get("semantic_vector_dim", SEMANTIC_VECTOR_DIM)))
    node_vector = index.get(node_id)
    similarity = semantic_cosine_similarity(slot_vector, node_vector)
    if similarity < SEMANTIC_SIMILARITY_THRESHOLD:
        return {"semantic_similarity": similarity, "semantic_score": 0.0}
    return {
        "semantic_similarity": similarity,
        "semantic_score": round(similarity * SEMANTIC_SCORE_SCALE, 3),
    }


def data_properties_by_domain(schema: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group data properties by their domain class local name."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    if not isinstance(schema, dict):
        schema_items: List[Dict[str, Any]] = []
    else:
        schema_items = [item for item in schema.get("data_properties", []) if isinstance(item, dict)]

    runtime_items = list(load_runtime_data_property_catalog().values())
    for item in schema_items + runtime_items:
        if not isinstance(item, dict):
            continue
        domain = item.get("domain")
        if not isinstance(domain, str):
            continue
        domain_local_name = uri_local_name(domain)
        if not domain_local_name:
            continue
        grouped.setdefault(domain_local_name, []).append(item)
    return grouped


def class_catalog(schema: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return user-facing ontology classes, excluding generic meta classes."""
    if not isinstance(schema, dict):
        return []
    results = []
    for item in schema.get("classes", []):
        if not isinstance(item, dict):
            continue
        local_name = item.get("local_name")
        if not isinstance(local_name, str) or not local_name or local_name in META_CLASS_NAMES:
            continue
        results.append(item)
    return results


def relation_catalog(schema: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Combine schema and runtime object relations into one lightweight adjacency list."""
    indexes = schema_indexes(schema)
    relations: List[Dict[str, Any]] = []

    for local_name, prop in indexes["object_properties"].items():
        domain = uri_local_name(prop.get("domain"))
        range_ = uri_local_name(prop.get("range"))
        if not domain or not range_:
            continue
        relations.append({
            "property": local_name,
            "source_class": domain,
            "target_class": range_,
            "validation_source": "schema",
        })

    for local_name, edges in load_runtime_object_property_catalog().items():
        for edge in edges:
            source_class = edge.get("source_class")
            target_class = edge.get("target_class")
            if not isinstance(source_class, str) or not isinstance(target_class, str):
                continue
            relations.append({
                "property": local_name,
                "source_class": source_class,
                "target_class": target_class,
                "validation_source": "mapping",
            })

    deduped = []
    seen = set()
    for item in relations:
        key = (item["property"], item["source_class"], item["target_class"], item["validation_source"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def build_semantic_manifest(schema: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Build a lightweight semantic manifest from schema + runtime catalogs."""
    class_items = class_catalog(schema)
    relation_items = relation_catalog(schema)
    attributes_by_class = data_properties_by_domain(schema)

    classes: List[Dict[str, Any]] = []
    class_index: Dict[str, Dict[str, Any]] = {}
    class_nodes: List[Dict[str, Any]] = []
    attribute_nodes: List[Dict[str, Any]] = []
    seen_class_node_ids: Set[str] = set()
    seen_attribute_node_ids: Set[str] = set()
    for item in class_items:
        class_name = item.get("local_name")
        if not isinstance(class_name, str) or not class_name:
            continue
        label = item.get("label") or class_name
        attributes: List[Dict[str, Any]] = []
        for prop in attributes_by_class.get(class_name, []):
            local_name = prop.get("local_name")
            if not isinstance(local_name, str) or not local_name:
                continue
            role_hints = [
                role
                for role in ROLE_PATTERNS
                if property_role_score(prop, role) > 0
            ]
            attributes.append({
                "local_name": local_name,
                "label": prop.get("label") or local_name,
                "range": prop.get("range"),
                "numeric": is_numeric_data_property(prop),
                "validation_source": prop.get("validation_source", "schema"),
                "role_hints": role_hints,
            })
            attribute_node_id = f"{class_name}.{local_name}"
            if attribute_node_id not in seen_attribute_node_ids:
                seen_attribute_node_ids.add(attribute_node_id)
                attribute_nodes.append({
                    "node_type": "attribute",
                    "node_id": attribute_node_id,
                    "class_name": class_name,
                    "local_name": local_name,
                    "label": prop.get("label") or local_name,
                    "range": prop.get("range"),
                    "numeric": is_numeric_data_property(prop),
                    "validation_source": prop.get("validation_source", "schema"),
                    "role_hints": role_hints,
                    "search_text": " ".join(
                        part for part in [
                            class_name,
                            prop.get("label") or "",
                            local_name,
                            " ".join(role_hints),
                        ] if part
                    ),
                })
        entry = {
            "class_name": class_name,
            "label": label,
            "attributes": attributes,
        }
        classes.append(entry)
        class_index[class_name] = entry
        if class_name not in seen_class_node_ids:
            seen_class_node_ids.add(class_name)
            class_nodes.append({
                "node_type": "class",
                "node_id": class_name,
                "class_name": class_name,
                "label": label,
                "local_name": class_name,
                "search_text": " ".join(part for part in [label, class_name] if part),
            })

    relations_by_source: Dict[str, List[Dict[str, Any]]] = {}
    relation_nodes: List[Dict[str, Any]] = []
    seen_relation_node_ids: Set[str] = set()
    for relation in relation_items:
        source_class = relation.get("source_class")
        if isinstance(source_class, str) and source_class:
            relations_by_source.setdefault(source_class, []).append(relation)
        relation_node_id = f"{relation.get('source_class')}->{relation.get('property')}->{relation.get('target_class')}"
        if relation_node_id not in seen_relation_node_ids:
            seen_relation_node_ids.add(relation_node_id)
            relation_nodes.append({
                "node_type": "relation",
                "node_id": relation_node_id,
                "property": relation.get("property"),
                "source_class": relation.get("source_class"),
                "target_class": relation.get("target_class"),
                "validation_source": relation.get("validation_source"),
                "search_text": " ".join(
                    part for part in [
                        str(relation.get("property") or ""),
                        str(relation.get("source_class") or ""),
                        str(relation.get("target_class") or ""),
                    ] if part
                ),
            })

    return {
        "classes": classes,
        "class_index": class_index,
        "relations": relation_items,
        "relations_by_source": relations_by_source,
        "class_nodes": class_nodes,
        "attribute_nodes": attribute_nodes,
        "relation_nodes": relation_nodes,
        "value_nodes": [],
    }


def manifest_attributes_by_class(manifest: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group manifest attributes by class name."""
    if not isinstance(manifest, dict):
        return {}
    return {
        item.get("class_name"): item.get("attributes", [])
        for item in manifest.get("classes", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str)
    }


def build_family_slot_inputs(
    question: str,
    slots: Dict[str, Any],
    routing: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build abstract slot inputs for the routed query family before manifest binding."""
    family = routing.get("family")
    schema = FAMILY_SLOT_SCHEMAS.get(family, [])
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    asks_explanation = bool(semantic_state.get("asks_explanation"))
    first_anchor = None
    anchors = semantic_state.get("anchors", [])
    if isinstance(anchors, list):
        for anchor in anchors:
            if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
                first_anchor = anchor
                break

    target_text = first_nonempty_text(
        semantic_state.get("result_hint"),
        semantic_state.get("target_text"),
    )

    action_or_state_text = semantic_state.get("action_text")
    if (
        not isinstance(action_or_state_text, str) or not action_or_state_text.strip()
    ) and family == "explanation_enumeration" and asks_explanation:
        action_or_state_text = infer_explanation_subject_text(question, target_text)
    if (
        not isinstance(action_or_state_text, str) or not action_or_state_text.strip()
    ) and family == "enumeration" and asks_explanation:
        action_or_state_text = question

    values = {
        "subject_text": (
            semantic_state.get("result_hint")
            or semantic_state.get("target_text")
            or question
        ),
        "target_text": target_text,
        "anchor_text": first_anchor.get("value") if isinstance(first_anchor, dict) else None,
        "cause_text": semantic_state.get("cause_text"),
        "action_or_state_text": action_or_state_text,
        "status_or_problem_text": (
            semantic_state.get("status_or_problem_text")
            or semantic_state.get("target_text")
            or semantic_state.get("action_text")
            or semantic_state.get("cause_text")
            or semantic_state.get("result_hint")
        ),
    }

    slot_inputs: List[Dict[str, Any]] = []
    for slot_def in schema:
        slot_name = slot_def.get("name")
        text = values.get(slot_name)
        if not isinstance(slot_name, str) or not isinstance(text, str):
            continue
        cleaned = text.strip()
        if not cleaned:
            continue
        slot_input = {
            "slot_name": slot_name,
            "text": cleaned,
            "allowed_node_types": list(slot_def.get("allowed_node_types", [])),
        }
        if slot_name == "anchor_text" and isinstance(first_anchor, dict):
            slot_input["anchor_kind"] = first_anchor.get("kind")
        if slot_name == "status_or_problem_text":
            slot_input["constraint_mode"] = (
                "status_check"
                if semantic_state.get("status_check_requested")
                else "problem_text"
            )
            if isinstance(semantic_state.get("status_numeric_constraint"), dict):
                slot_input["comparison"] = {
                    "op": semantic_state["status_numeric_constraint"].get("op"),
                    "value": semantic_state["status_numeric_constraint"].get("value"),
                }
        slot_inputs.append(slot_input)
    return slot_inputs


def lexical_node_match_score(slot_text: str, search_text: str) -> int:
    """Score a slot text against a manifest node using lightweight lexical overlap."""
    normalized_slot = normalize_match_text(slot_text)
    normalized_search = normalize_match_text(search_text)
    if not normalized_search or not normalized_slot:
        return 0
    score = 0
    if normalized_slot == normalized_search:
        score += 8
    for term in normalized_terms_from_text(slot_text):
        normalized_term = normalize_match_text(term)
        if not normalized_term:
            continue
        if normalized_term in normalized_search:
            score += 4
    for term in normalized_terms_from_text(search_text):
        normalized_term = normalize_match_text(term)
        if not normalized_term or len(normalized_term) < 2:
            continue
        if normalized_term in normalized_slot:
            score += 4
    score += char_ngram_overlap_score(slot_text, search_text)
    return score


def slot_role_match_score(slot_input: Dict[str, Any], node: Dict[str, Any]) -> int:
    """Score how well a manifest node type/capability matches a semantic slot role."""
    slot_name = slot_input.get("slot_name")
    node_type = node.get("node_type")
    if slot_name in ("subject_text", "target_text") and node_type == "class":
        return 6
    if slot_name == "anchor_text" and node_type == "attribute":
        role_hints = set(node.get("role_hints", []))
        score = 4
        if {"id", "phone"} & role_hints:
            score += 4
        return score
    if slot_name in ("cause_text", "action_or_state_text", "status_or_problem_text") and node_type == "attribute":
        role_hints = set(node.get("role_hints", []))
        score = 2
        if {"type", "description", "status", "score"} & role_hints:
            score += 3
        if slot_name == "status_or_problem_text" and slot_input.get("comparison") and bool(node.get("numeric")):
            score += 2
        return score
    if slot_name in ("cause_text", "action_or_state_text", "status_or_problem_text") and node_type == "value":
        return 3
    if slot_name == "target_text" and node_type == "relation":
        return 1
    return 0


def slot_input_requires_status_like_binding(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot should bind only to generic status/score semantics."""
    return (
        slot_input.get("slot_name") == "status_or_problem_text"
        and slot_input.get("constraint_mode") == "status_check"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def node_supports_status_like_binding(node: Dict[str, Any]) -> bool:
    """Whether a manifest node can support a generic status-check constraint."""
    if node.get("node_type") not in {"attribute", "value"}:
        return False
    role_hints = set(node.get("role_hints", []))
    return bool({"status", "score"} & role_hints)


def slot_input_requires_numeric_attribute_binding(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot must bind to numeric attributes because it carries an explicit comparator."""
    return (
        slot_input.get("slot_name") == "status_or_problem_text"
        and isinstance(slot_input.get("comparison"), dict)
    )


def node_supports_numeric_attribute_binding(node: Dict[str, Any]) -> bool:
    """Whether a manifest node can support explicit numeric comparison lowering."""
    return node.get("node_type") == "attribute" and bool(node.get("numeric"))


def slot_input_disallows_numeric_semantics(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot should exclude numeric nodes because it expresses free-text semantics."""
    slot_name = slot_input.get("slot_name")
    if slot_name in {"cause_text", "action_or_state_text"}:
        return True
    return (
        slot_name == "status_or_problem_text"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def manifest_nodes_for_slot(manifest: Optional[Dict[str, Any]], slot_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return manifest nodes that are admissible for a given semantic slot."""
    if not isinstance(manifest, dict):
        return []
    allowed = set(slot_input.get("allowed_node_types", []))
    nodes: List[Dict[str, Any]] = []
    if "class" in allowed:
        nodes.extend(manifest.get("class_nodes", []))
    if "attribute" in allowed:
        nodes.extend(manifest.get("attribute_nodes", []))
    if "relation" in allowed:
        nodes.extend(manifest.get("relation_nodes", []))
    if "value" in allowed:
        nodes.extend(manifest.get("value_nodes", []))
    filtered = [node for node in nodes if isinstance(node, dict)]
    if slot_input_requires_numeric_attribute_binding(slot_input):
        filtered = [node for node in filtered if node_supports_numeric_attribute_binding(node)]
        return filtered
    if slot_input_requires_status_like_binding(slot_input):
        filtered = [node for node in filtered if node_supports_status_like_binding(node)]
    if slot_input_disallows_numeric_semantics(slot_input):
        filtered = [node for node in filtered if not bool(node.get("numeric"))]
    return filtered


def bind_semantic_slots(manifest: Optional[Dict[str, Any]], slot_inputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Bind abstract family slots to typed manifest nodes using hybrid-ready scoring."""
    bindings: List[Dict[str, Any]] = []
    for slot_input in slot_inputs:
        slot_text = slot_input.get("text")
        if not isinstance(slot_text, str) or not slot_text:
            continue
        candidates = []
        for node in manifest_nodes_for_slot(manifest, slot_input):
            lexical_score = lexical_node_match_score(slot_text, node.get("search_text", ""))
            if (
                node.get("node_type") == "value"
                and normalize_match_text(slot_text) == normalize_match_text(node.get("label"))
            ):
                lexical_score += 12
            role_score = slot_role_match_score(slot_input, node)
            semantic_result = semantic_node_match_score(slot_text, node, manifest)
            semantic_score = float(semantic_result.get("semantic_score", 0.0) or 0.0)
            semantic_similarity = float(semantic_result.get("semantic_similarity", 0.0) or 0.0)
            allow_anchor_capability_only = (
                slot_input.get("slot_name") == "anchor_text"
                and node.get("node_type") == "attribute"
                and role_score > 0
            )
            if lexical_score <= 0 and semantic_score <= 0 and not allow_anchor_capability_only:
                continue
            total_score = lexical_score + role_score + semantic_score
            if total_score <= 0:
                continue
            candidates.append({
                "node_type": node.get("node_type"),
                "node_id": node.get("node_id"),
                "label": node.get("label") or node.get("local_name") or node.get("node_id"),
                "class_name": node.get("class_name"),
                "local_name": node.get("local_name"),
                "property_local_name": node.get("property_local_name"),
                "role_hints": node.get("role_hints", []),
                "numeric": bool(node.get("numeric")),
                "range": node.get("range"),
                "lexical_score": lexical_score,
                "semantic_score": semantic_score,
                "semantic_similarity": round(semantic_similarity, 4),
                "slot_role_score": role_score,
                "total_score": total_score,
            })
        candidates.sort(
            key=lambda item: (
                -item["total_score"],
                -item.get("semantic_similarity", 0.0),
                str(item["node_id"]),
            )
        )
        bindings.append({
            "slot_name": slot_input.get("slot_name"),
            "text": slot_text,
            "allowed_node_types": slot_input.get("allowed_node_types", []),
            "candidates": candidates[:5],
        })
    return bindings


def slot_relation_propagation_weight(slot_name: Optional[str]) -> float:
    """Generic structural prior for propagating bound classes toward plausible source entities."""
    weights = {
        "anchor_text": 4.0,
        "target_text": 1.5,
        "cause_text": 1.25,
        "action_or_state_text": 1.25,
        "status_or_problem_text": 1.0,
    }
    return float(weights.get(slot_name, 0.0))


def slot_relation_propagation_min_score(slot_name: Optional[str]) -> float:
    """Minimum binding score required before a non-anchor slot can propagate structurally."""
    if slot_name == "target_text":
        return 5.0
    return 6.0


def relation_propagated_source_candidates(
    manifest: Optional[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Propagate bound attribute/value classes across manifest relations to form generic source candidates."""
    if not isinstance(manifest, dict):
        return {}

    classes = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    }
    relations = [
        item for item in manifest.get("relations", [])
        if isinstance(item, dict)
        and isinstance(item.get("source_class"), str)
        and item.get("source_class")
        and isinstance(item.get("target_class"), str)
        and item.get("target_class")
    ]

    propagated: Dict[str, Dict[str, Any]] = {}
    for binding in slot_bindings:
        if not isinstance(binding, dict):
            continue
        slot_name = binding.get("slot_name")
        slot_weight = slot_relation_propagation_weight(slot_name)
        if slot_weight <= 0:
            continue

        for candidate in binding.get("candidates", []):
            if not isinstance(candidate, dict):
                continue
            if candidate.get("node_type") not in {"attribute", "value"}:
                continue
            bound_class = candidate.get("class_name")
            if not isinstance(bound_class, str) or not bound_class:
                continue

            base_score = float(candidate.get("total_score", 0.0) or 0.0)
            if slot_name != "anchor_text" and base_score < slot_relation_propagation_min_score(slot_name):
                continue

            direct_entry = {
                "class_name": bound_class,
                "label": classes.get(bound_class, bound_class),
                "score": base_score + max(0.5, slot_weight - 0.5),
                "binding_slot": slot_name,
                "binding_source": "bound_class",
            }
            current_direct = propagated.get(bound_class)
            if current_direct is None or direct_entry["score"] > float(current_direct.get("score", 0.0) or 0.0):
                propagated[bound_class] = direct_entry

            for relation in relations:
                source_class = relation.get("source_class")
                target_class = relation.get("target_class")
                relation_bonus = 1.0 if relation.get("validation_source") == "mapping" else 0.0

                if target_class == bound_class and isinstance(source_class, str) and source_class:
                    score = base_score + slot_weight + 1.0 + relation_bonus
                    current = propagated.get(source_class)
                    if current is None or score > float(current.get("score", 0.0) or 0.0):
                        propagated[source_class] = {
                            "class_name": source_class,
                            "label": classes.get(source_class, source_class),
                            "score": score,
                            "binding_slot": slot_name,
                            "binding_source": "relation_source",
                            "via_relation": relation.get("property"),
                            "bound_class": bound_class,
                        }

                if slot_name == "anchor_text" and source_class == bound_class and isinstance(target_class, str) and target_class:
                    score = base_score + relation_bonus - 2.0 + (slot_weight * 0.25)
                    current = propagated.get(target_class)
                    if current is None or score > float(current.get("score", 0.0) or 0.0):
                        propagated[target_class] = {
                            "class_name": target_class,
                            "label": classes.get(target_class, target_class),
                            "score": score,
                            "binding_slot": slot_name,
                            "binding_source": "relation_target",
                            "via_relation": relation.get("property"),
                            "bound_class": bound_class,
                        }

    return propagated


def anchor_propagated_source_candidates(
    manifest: Optional[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Propagate anchor-bound classes across manifest relations to form generic source candidates."""
    anchor_only_bindings = [
        binding
        for binding in slot_bindings
        if isinstance(binding, dict) and binding.get("slot_name") == "anchor_text"
    ]
    return relation_propagated_source_candidates(manifest, anchor_only_bindings)


def merge_source_candidates_from_slot_bindings(
    source_info: Dict[str, Any],
    slot_bindings: List[Dict[str, Any]],
    manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Merge subject/target class bindings into source-class candidate selection."""
    merged: Dict[str, Dict[str, Any]] = {}
    for item in source_info.get("candidates", []):
        if not isinstance(item, dict):
            continue
        class_name = item.get("class_name")
        if isinstance(class_name, str) and class_name:
            merged[class_name] = dict(item)

    for binding in slot_bindings:
        if not isinstance(binding, dict):
            continue
        if binding.get("slot_name") not in ("subject_text", "target_text", "anchor_text"):
            continue
        for candidate in binding.get("candidates", []):
            if not isinstance(candidate, dict):
                continue
            node_type = candidate.get("node_type")
            if node_type == "class":
                class_name = candidate.get("class_name")
            elif binding.get("slot_name") == "anchor_text" and node_type in {"attribute", "value"}:
                class_name = candidate.get("class_name")
            else:
                continue
            if not isinstance(class_name, str) or not class_name:
                continue
            score = float(candidate.get("total_score", 0.0) or 0.0)
            existing = merged.get(class_name)
            if existing is None or score > float(existing.get("score", 0.0) or 0.0):
                merged[class_name] = {
                    "class_name": class_name,
                    "label": candidate.get("label") or class_name,
                    "score": score,
                    "binding_slot": binding.get("slot_name"),
                }

    for class_name, item in relation_propagated_source_candidates(manifest, slot_bindings).items():
        existing = merged.get(class_name)
        if existing is None or float(item.get("score", 0.0) or 0.0) > float(existing.get("score", 0.0) or 0.0):
            merged[class_name] = item

    candidates = sorted(merged.values(), key=lambda item: (-item["score"], item["class_name"]))
    return {
        "selected": candidates[0] if candidates else None,
        "candidates": candidates[:5],
    }


def selected_anchor_binding_for_source(
    slot_bindings: List[Dict[str, Any]],
    source_class: str,
) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper for selecting a source-class anchor binding."""
    return selected_anchor_binding_for_class(slot_bindings, source_class)


def selected_anchor_binding_for_class(
    slot_bindings: List[Dict[str, Any]],
    class_name: str,
) -> Optional[Dict[str, Any]]:
    """Pick the highest-scoring anchor binding for a given class from attribute or value nodes."""
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != "anchor_text":
            continue
        for candidate in binding.get("candidates", []):
            if not isinstance(candidate, dict):
                continue
            if candidate.get("node_type") not in {"attribute", "value"}:
                continue
            if candidate.get("class_name") != class_name:
                continue
            local_name = candidate.get("property_local_name") or candidate.get("local_name")
            if isinstance(local_name, str) and local_name:
                resolved = dict(candidate)
                resolved["local_name"] = local_name
                return resolved
    return None


def load_sample_value_nodes(
    base_url: Optional[str],
    manifest: Optional[Dict[str, Any]],
    class_names: List[str],
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """Load a bounded sample-based value catalog for a few relevant classes and cache it."""
    if not isinstance(base_url, str) or not base_url:
        return []
    cache = getattr(load_sample_value_nodes, "_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        load_sample_value_nodes._cache = cache

    attribute_lookup: Dict[tuple[str, str], Dict[str, Any]] = {}
    if isinstance(manifest, dict):
        for node in manifest.get("attribute_nodes", []):
            if not isinstance(node, dict):
                continue
            class_name = node.get("class_name")
            local_name = node.get("local_name")
            if isinstance(class_name, str) and isinstance(local_name, str):
                attribute_lookup[(class_name, local_name)] = node

    value_nodes: List[Dict[str, Any]] = []
    for class_name in unique_preserve_order([name for name in class_names if isinstance(name, str) and name])[:3]:
        cache_key = (base_url, class_name, limit)
        cached_nodes = cache.get(cache_key)
        if isinstance(cached_nodes, list):
            value_nodes.extend(cached_nodes)
            continue

        query = urllib.parse.urlencode({"limit": limit})
        url = f"{base_url}/sample/{urllib.parse.quote(class_name)}?{query}"
        response = request_json("GET", url)
        class_value_nodes: List[Dict[str, Any]] = []
        seen_node_ids: Set[str] = set()
        samples = response.get("samples", []) if isinstance(response, dict) else []
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            data_props = sample.get("data_properties", {})
            if not isinstance(data_props, dict):
                continue
            for prop_name, raw_value in data_props.items():
                if not isinstance(prop_name, str):
                    continue
                if raw_value in (None, ""):
                    continue
                value_text = str(raw_value).strip()
                if not value_text:
                    continue
                attr_node = attribute_lookup.get((class_name, prop_name), {})
                role_hints = list(attr_node.get("role_hints", [])) if isinstance(attr_node, dict) else []
                if len(value_text) > 80:
                    continue
                node_id = f"{class_name}.{prop_name}={value_text}"
                if node_id in seen_node_ids:
                    continue
                seen_node_ids.add(node_id)
                class_value_nodes.append({
                    "node_type": "value",
                    "node_id": node_id,
                    "class_name": class_name,
                    "property_local_name": prop_name,
                    "label": value_text,
                    "local_name": prop_name,
                    "role_hints": role_hints,
                    "numeric": bool(attr_node.get("numeric")) if isinstance(attr_node, dict) else False,
                    "range": attr_node.get("range") if isinstance(attr_node, dict) else None,
                    "search_text": " ".join(
                        part for part in [
                            class_name,
                            prop_name,
                            attr_node.get("label") if isinstance(attr_node, dict) else "",
                            value_text,
                            " ".join(role_hints),
                        ] if part
                    ),
                })
        cache[cache_key] = class_value_nodes
        value_nodes.extend(class_value_nodes)
    return value_nodes


def with_value_nodes(manifest: Optional[Dict[str, Any]], value_nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a manifest clone with sample-derived value nodes attached."""
    if not isinstance(manifest, dict):
        return {"value_nodes": value_nodes}
    updated = dict(manifest)
    updated["value_nodes"] = [node for node in value_nodes if isinstance(node, dict)]
    return updated


def binding_terms_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    limit: int = 5,
    preferred_node_types: Optional[List[str]] = None,
) -> List[str]:
    """Extract lexical terms from top manifest bindings for a given semantic slot."""
    terms: List[str] = []
    base_minimum = 4.0 if slot_name in {"cause_text", "action_or_state_text"} else 6.0
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        candidates = [item for item in binding.get("candidates", []) if isinstance(item, dict)]
        if not candidates:
            continue
        top_score = max(float(item.get("total_score", 0.0) or 0.0) for item in candidates)
        minimum_score = max(base_minimum, top_score * 0.5)
        filtered_candidates = [
            candidate
            for candidate in candidates
            if float(candidate.get("total_score", 0.0) or 0.0) >= minimum_score
        ]
        if preferred_node_types:
            preferred = [
                candidate
                for candidate in filtered_candidates
                if candidate.get("node_type") in set(preferred_node_types)
            ]
            if preferred:
                filtered_candidates = preferred
        for candidate in filtered_candidates:
            label = candidate.get("label")
            if isinstance(label, str) and label:
                terms.append(label)
            if len(terms) >= limit:
                break
    return unique_preserve_order([term for term in terms if term])[:limit]


def normalize_slot_text(text: Optional[str]) -> Optional[str]:
    """Normalize extracted slot text without imposing domain-specific semantics."""
    if not isinstance(text, str):
        return None
    cleaned = text.strip(" ，,。？?；;:：")
    cleaned = re.sub(r"^(?:是否(?:存在|有)?|有无|有没有|是否为|是否属于)", "", cleaned).strip()
    cleaned = re.sub(r"(?:的)?(?:情况|问题|现象|记录|表现)$", "", cleaned).strip()
    cleaned = re.sub(r"(?:的)+$", "", cleaned).strip()
    return cleaned or None


def is_generic_explanation_target(text: Optional[str]) -> bool:
    """Whether a target slot asks for a generic explanation category rather than a schema term."""
    normalized = normalize_slot_text(text)
    if not normalized:
        return False
    return normalized in EXPLANATION_TARGET_TERMS


def infer_explanation_subject_text(question_text: str, target_text: Optional[str]) -> Optional[str]:
    """Extract the phenomenon being explained from a generic explanation question."""
    if not isinstance(question_text, str):
        return None

    cleaned = question_text.strip(" ，,。？?；;:：")
    if not cleaned:
        return None

    if isinstance(target_text, str) and target_text.strip():
        generic_target = target_text.strip()
    else:
        generic_target = ""

    if generic_target and generic_target in cleaned:
        cleaned = cleaned.replace(generic_target, " ", 1)
    else:
        cleaned = re.sub(r"(原因|根因|成因|理由)", " ", cleaned, count=1)

    cleaned = cleaned.strip()
    cleaned = re.sub(r"(?:有什?么|有哪些|是什么|是啥|什么)$", " ", cleaned).strip()
    cleaned = re.sub(r"(?:分别|都)+$", " ", cleaned).strip()
    cleaned = re.sub(r"^(?:为什么|为何)\s*", "", cleaned).strip()
    cleaned = re.sub(r"^(?:引发|导致|造成|产生|出现|发生)\s*", "", cleaned).strip()
    cleaned = re.sub(r"(?:的)+$", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return normalize_slot_text(cleaned)


def parse_numeric_literal(value: str) -> Any:
    """Parse a numeric literal into int or float when possible."""
    return float(value) if "." in value else int(value)


def parse_numeric_constraint_text(text: Optional[str]) -> Optional[Dict[str, Any]]:
    """Extract a generic explicit numeric comparison from slot text."""
    if not isinstance(text, str):
        return None
    cleaned = normalize_slot_text(text)
    if not cleaned:
        return None

    candidate_texts = unique_preserve_order([
        cleaned,
        NUMERIC_OPERATOR_PREFIX_PATTERN.sub("", cleaned),
        NUMERIC_TRAILING_UNIT_PATTERN.sub(r"\g<number>", cleaned),
        NUMERIC_TRAILING_UNIT_PATTERN.sub(r"\g<number>", NUMERIC_OPERATOR_PREFIX_PATTERN.sub("", cleaned)),
    ])

    for candidate in candidate_texts:
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        match = NUMERIC_COMPARISON_PATTERN.match(candidate)
        if match:
            attribute_text = normalize_slot_text(match.group("attribute"))
            operator = NUMERIC_COMPARISON_OPERATOR_MAP.get(match.group("op"))
            if attribute_text and operator:
                return {
                    "attribute_text": attribute_text,
                    "op": operator,
                    "value": parse_numeric_literal(match.group("value")),
                    "raw_text": candidate,
                }

        match = VALUE_SUFFIX_COMPARISON_PATTERN.match(candidate)
        if match:
            attribute_text = normalize_slot_text(match.group("attribute"))
            operator = NUMERIC_COMPARISON_OPERATOR_MAP.get(match.group("op"))
            if attribute_text and operator:
                return {
                    "attribute_text": attribute_text,
                    "op": operator,
                    "value": parse_numeric_literal(match.group("value")),
                    "raw_text": candidate,
                }
    return None


def build_explicit_metric_clarification_hint(semantic_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build a machine-readable clarification contract for abstract status wording."""
    anchor_value = None
    if isinstance(semantic_state, dict):
        anchors = semantic_state.get("anchors", [])
        if isinstance(anchors, list):
            for anchor in anchors:
                if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
                    anchor_value = anchor["value"]
                    break

    generic_rewrite_shape = "<anchor> 是否存在 <metric> <op> <value> 的情况"
    clarification_prompt = (
        f"请明确你要检查的具体指标和阈值。例如：{anchor_value} 是否存在 <metric> <op> <value> 的情况？"
        if anchor_value
        else f"请明确你要检查的具体指标和阈值。例如：{generic_rewrite_shape}"
    )
    return {
        "kind": "explicit_metric_or_threshold_required",
        "message": (
            "Abstract status wording is not executable until it is grounded to a concrete metric "
            "or numeric threshold."
        ),
        "recommended_next_step": (
            "Ask the user to restate the question with an explicit metric/threshold before rerunning."
        ),
        "generic_rewrite_shape": generic_rewrite_shape,
        "requires_user_clarification": True,
        "user_clarification_prompt": clarification_prompt,
        "disallowed_agent_actions": [
            "manual_sample_grounding",
            "manual_sparql_probe",
            "self_rewrite_question",
        ],
    }


def slot_binding_has_candidates(slot_bindings: List[Dict[str, Any]], slot_name: str) -> bool:
    """Whether a slot binding produced at least one candidate node."""
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        candidates = binding.get("candidates", [])
        if isinstance(candidates, list) and candidates:
            return True
    return False


def slot_binding_candidates(slot_bindings: List[Dict[str, Any]], slot_name: str) -> List[Dict[str, Any]]:
    """Return all candidates produced for a given slot."""
    results: List[Dict[str, Any]] = []
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        for candidate in binding.get("candidates", []):
            if isinstance(candidate, dict):
                results.append(candidate)
    return results


def slot_candidates_have_text_lowering(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    slot_input: Optional[Dict[str, Any]] = None,
) -> bool:
    """Whether a slot has at least one binding that can be lowered as a text-style filter."""
    abstract_status_check = (
        isinstance(slot_input, dict)
        and slot_input.get("constraint_mode") == "status_check"
        and not isinstance(slot_input.get("comparison"), dict)
    )
    for candidate in slot_binding_candidates(slot_bindings, slot_name):
        node_type = candidate.get("node_type")
        role_hints = set(candidate.get("role_hints", []))
        if abstract_status_check:
            if node_type == "attribute" and not bool(candidate.get("numeric")) and {"status"} & role_hints:
                return True
            if node_type == "value" and not bool(candidate.get("numeric")) and {"status"} & role_hints:
                return True
            continue
        if node_type == "value":
            return True
        if node_type == "attribute" and not bool(candidate.get("numeric")) and not {"score"} & role_hints:
            return True
    return False


def slot_input_for_name(slot_inputs: List[Dict[str, Any]], slot_name: str) -> Optional[Dict[str, Any]]:
    """Return the slot input definition for a given slot name."""
    for slot_input in slot_inputs:
        if isinstance(slot_input, dict) and slot_input.get("slot_name") == slot_name:
            return slot_input
    return None


def top_attribute_candidate_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    class_name: Optional[str] = None,
    numeric_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring attribute candidate for a slot, optionally scoped by class."""
    candidates = []
    for candidate in slot_binding_candidates(slot_bindings, slot_name):
        if candidate.get("node_type") != "attribute":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        if numeric_only and not bool(candidate.get("numeric")):
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            -float(item.get("total_score", 0.0) or 0.0),
            -float(item.get("semantic_similarity", 0.0) or 0.0),
            str(item.get("node_id", "")),
        )
    )
    return candidates[0]


def top_value_candidate_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    class_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring value candidate for a slot, optionally scoped by class."""
    candidates = []
    for candidate in slot_binding_candidates(slot_bindings, slot_name):
        if candidate.get("node_type") != "value":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            -float(item.get("total_score", 0.0) or 0.0),
            -float(item.get("semantic_similarity", 0.0) or 0.0),
            str(item.get("node_id", "")),
        )
    )
    return candidates[0]


def split_utterance_into_segments(utterance: str) -> List[str]:
    """Split a user utterance into coarse question-like segments."""
    text = utterance.strip()
    if not text:
        return []

    # Keep conditional follow-ups as separate units even when they are written inline.
    text = re.sub(r"[，,]\s*(如果(?:有|存在|是|属于|命中|没有|不存在|不是)|若(?:有|存在|是|属于|命中|没有|不存在|不是))", r"。\1", text)
    segments = []
    for segment in UTTERANCE_SPLIT_PATTERN.split(text):
        cleaned = segment.strip(" ，,。；;？！! ")
        if cleaned:
            segments.append(cleaned)
    return segments


def strip_conditional_prefix(text: str) -> Dict[str, Any]:
    """Strip a leading conditional prefix and return dependency metadata."""
    match = CONDITIONAL_PREFIX_PATTERN.match(text.strip())
    if not match:
        return {
            "text": text.strip(),
            "condition_type": None,
            "condition_prefix": None,
        }

    prefix = match.group("prefix")
    body = match.group("body").strip(" ，,。；;？！! ")
    lowered_prefix = normalize_match_text(prefix)
    if any(term in lowered_prefix for term in ("没有", "不存在", "不是")):
        condition_type = "empty_or_false"
    else:
        condition_type = "non_empty_or_true"
    return {
        "text": body or text.strip(),
        "condition_type": condition_type,
        "condition_prefix": prefix,
    }


def detect_reference_markers(text: str) -> List[str]:
    """Detect lightweight discourse references such as 这个/这些/分别."""
    markers = []
    for marker in REFERENCE_MARKERS:
        if marker in text:
            markers.append(marker)
    return markers


def decompose_utterance_to_question_units(utterance: str) -> List[Dict[str, Any]]:
    """Decompose one utterance into dependent QuestionUnits."""
    segments = split_utterance_into_segments(utterance)
    units: List[Dict[str, Any]] = []
    active_condition: Optional[Dict[str, Any]] = None

    for index, segment in enumerate(segments, start=1):
        conditional = strip_conditional_prefix(segment)
        unit_text = conditional["text"]
        reference_markers = detect_reference_markers(unit_text)
        explicit_anchors = detect_question_anchors(unit_text)
        dependency: Optional[Dict[str, Any]] = None

        if conditional["condition_type"] and units:
            dependency = {
                "depends_on": units[-1]["unit_id"],
                "condition": conditional["condition_type"],
                "source": "conditional_prefix",
                "prefix": conditional["condition_prefix"],
            }
            active_condition = deepcopy(dependency)
        elif reference_markers and units:
            dependency = {
                "depends_on": units[-1]["unit_id"],
                "condition": "requires_previous_result",
                "source": "reference_marker",
            }
        elif active_condition is not None and units and not explicit_anchors:
            dependency = deepcopy(active_condition)
        else:
            active_condition = None

        units.append({
            "unit_id": f"q{index}",
            "text": unit_text,
            "raw_text": segment,
            "position": index,
            "reference_markers": reference_markers,
            "dependency": dependency,
        })

        if explicit_anchors:
            active_condition = None

    return units


def unit_needs_inherited_context(unit: Dict[str, Any], slots: Dict[str, Any]) -> bool:
    """Whether a unit should inherit anchors/constraints from previous context."""
    if not isinstance(unit, dict) or not isinstance(slots, dict):
        return False
    template = str(slots.get("template") or unit.get("template") or "")
    profile = derive_intent_profile(slots, template, question_unit=unit)
    return bool(profile.get("scope_inheritance_allowed"))


def dependency_allows_semantic_inheritance(unit: Dict[str, Any], slots: Optional[Dict[str, Any]] = None) -> bool:
    """Only some branches should carry forward prior semantic constraints."""
    if not isinstance(unit, dict) or not isinstance(slots, dict):
        return True
    template = str(slots.get("template") or unit.get("template") or "")
    profile = derive_intent_profile(slots, template, question_unit=unit)
    return bool(profile.get("semantic_inheritance_allowed"))


def merge_inherited_slots(
    base_slots: Dict[str, Any],
    inherited_context: Optional[Dict[str, Any]],
    unit: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge carry-forward anchors/constraints into a unit's extracted slots."""
    merged = dict(base_slots)
    if not isinstance(inherited_context, dict) or not unit_needs_inherited_context(unit, merged):
        merged["inherited_context_keys"] = []
        return merged
    if merged.get("has_explicit_anchor"):
        merged["inherited_context_keys"] = []
        return merged

    inherited_keys: List[str] = []
    inherited_intent_ir = inherited_context.get("intent_ir") if isinstance(inherited_context, dict) else None
    inherited_state = semantic_state_from_sources(inherited_context, inherited_intent_ir)

    def inherit_scalar(slot_name: str) -> None:
        if bootstrap_candidate_text(merged, slot_name):
            return
        value = inherited_state.get(slot_name)
        if value in (None, "", [], {}):
            return
        register_bootstrap_candidate(merged, slot_name, value, "inherited_context")
        inherited_keys.append(slot_name)

    if not merged.get("anchors") and inherited_state.get("anchors"):
        merged["anchors"] = deepcopy(inherited_state["anchors"])
        merged["has_anchor"] = bool(merged["anchors"])
        inherited_keys.append("anchors")

    allow_semantic_inheritance = dependency_allows_semantic_inheritance(unit, merged)

    if (
        allow_semantic_inheritance
        and merged.get("status_numeric_constraint") is None
        and inherited_state.get("status_numeric_constraint") is not None
    ):
        merged["status_numeric_constraint"] = deepcopy(inherited_state.get("status_numeric_constraint"))
        numeric_attribute_text = inherited_state["status_numeric_constraint"].get("attribute_text")
        if isinstance(numeric_attribute_text, str) and numeric_attribute_text.strip():
            register_bootstrap_candidate(
                merged,
                "status_or_problem_text",
                numeric_attribute_text,
                "inherited_numeric_constraint",
            )
        inherited_keys.append("status_numeric_constraint")

    if allow_semantic_inheritance:
        for key in ("cause_text", "action_text", "status_or_problem_text", "target_text", "result_hint"):
            inherit_scalar(key)

    bootstrap_signals = merged.setdefault("bootstrap_signals", {})
    if not isinstance(bootstrap_signals, dict):
        bootstrap_signals = {}
        merged["bootstrap_signals"] = bootstrap_signals

    if (
        allow_semantic_inheritance
        and inherited_state.get("status_check_requested")
        and not bootstrap_signals.get("status_check_requested")
    ):
        bootstrap_signals["status_check_requested"] = True
        inherited_keys.append("status_check_requested")

    if inherited_state.get("asks_solution") and not bootstrap_signals.get("asks_solution"):
        bootstrap_signals["asks_solution"] = True
        inherited_keys.append("asks_solution")

    merged["inherited_context_keys"] = unique_preserve_order(inherited_keys)
    return merged


def intent_ir_constraint_snapshot(unit_intent_ir: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Extract a small, typed constraint view from Intent IR for downstream policy decisions."""
    snapshot: Dict[str, Any] = {}
    if not isinstance(unit_intent_ir, dict) or not isinstance(unit_intent_ir.get("constraints"), list):
        return snapshot
    for item in unit_intent_ir.get("constraints", []):
        if not isinstance(item, dict):
            continue
        slot_name = item.get("slot")
        if not isinstance(slot_name, str) or not slot_name:
            continue
        text_value = item.get("text")
        if isinstance(text_value, str) and text_value.strip():
            snapshot[slot_name] = text_value.strip()
            continue
        constraint_value = item.get("constraint")
        if isinstance(constraint_value, dict):
            snapshot[slot_name] = deepcopy(constraint_value)
    return snapshot


def intent_ir_operator_set(unit_intent_ir: Optional[Dict[str, Any]]) -> Set[str]:
    """Return a normalized operator set from Intent IR."""
    if not isinstance(unit_intent_ir, dict) or not isinstance(unit_intent_ir.get("operators"), list):
        return set()
    return {
        item
        for item in unit_intent_ir.get("operators", [])
        if isinstance(item, str) and item
    }


def first_nonempty_text(*values: Any) -> Optional[str]:
    """Return the first non-empty string value after trimming."""
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return None


def register_bootstrap_candidate(
    slots: Dict[str, Any],
    slot_name: str,
    value: Optional[str],
    source: str,
) -> None:
    """Register one bootstrap semantic candidate for later Intent IR construction."""
    if not isinstance(slots, dict) or not isinstance(slot_name, str) or not slot_name:
        return
    cleaned = normalize_slot_text(value)
    if not cleaned:
        return
    candidate_store = slots.setdefault("bootstrap_candidates", {})
    if not isinstance(candidate_store, dict):
        candidate_store = {}
        slots["bootstrap_candidates"] = candidate_store
    bucket = candidate_store.setdefault(slot_name, [])
    if not isinstance(bucket, list):
        bucket = []
        candidate_store[slot_name] = bucket
    candidate = {"text": cleaned, "source": source}
    if candidate not in bucket:
        bucket.append(candidate)


def bootstrap_candidate_text(slots: Dict[str, Any], slot_name: str) -> Optional[str]:
    """Return the primary bootstrap candidate text for one semantic slot."""
    candidate_store = slots.get("bootstrap_candidates")
    if not isinstance(candidate_store, dict):
        return None
    bucket = candidate_store.get(slot_name)
    if not isinstance(bucket, list):
        return None
    for item in bucket:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()
    return None


def semantic_state_from_sources(
    slots: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize semantic control bits from Intent IR first, with bootstrap candidates as fallback."""
    constraint_snapshot = intent_ir_constraint_snapshot(unit_intent_ir)
    intent_operators = intent_ir_operator_set(unit_intent_ir)
    focus = (
        unit_intent_ir.get("focus")
        if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("focus"), dict)
        else None
    )
    focus_kind = (
        focus.get("kind")
        if isinstance(focus, dict) and isinstance(focus.get("kind"), str) and focus.get("kind")
        else None
    )

    anchors: List[Dict[str, Any]] = []
    if isinstance(focus, dict) and focus_kind == "anchored_entity" and isinstance(focus.get("anchors"), list):
        anchors = deepcopy(focus.get("anchors") or [])
    elif isinstance(slots.get("anchors"), list):
        anchors = deepcopy(slots.get("anchors") or [])

    numeric_constraint = constraint_snapshot.get("status_numeric_constraint")
    if not isinstance(numeric_constraint, dict):
        numeric_constraint = slots.get("status_numeric_constraint")
    if not isinstance(numeric_constraint, dict):
        numeric_constraint = None

    references = (
        unit_intent_ir.get("references")
        if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("references"), dict)
        else None
    )
    resolved_reference = None
    if isinstance(references, dict) and isinstance(references.get("resolved"), dict):
        resolved_reference = deepcopy(references.get("resolved"))
    elif isinstance(slots.get("resolved_reference"), dict):
        resolved_reference = deepcopy(slots.get("resolved_reference"))

    bootstrap_signals = slots.get("bootstrap_signals")
    if not isinstance(bootstrap_signals, dict):
        bootstrap_signals = {}

    return {
        "constraint_snapshot": constraint_snapshot,
        "operators": [
            item
            for item in (
                unit_intent_ir.get("operators", [])
                if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("operators"), list)
                else []
            )
            if isinstance(item, str) and item
        ],
        "operator_set": intent_operators,
        "focus": deepcopy(focus) if isinstance(focus, dict) else None,
        "focus_kind": focus_kind,
        "anchors": anchors,
        "has_anchor": focus_kind == "anchored_entity" or bool(anchors) or bool(slots.get("has_anchor")),
        "has_explicit_anchor": bool(slots.get("has_explicit_anchor")) or (focus_kind == "anchored_entity" and bool(anchors)),
        "cause_text": first_nonempty_text(
            constraint_snapshot.get("cause_text"),
            bootstrap_candidate_text(slots, "cause_text"),
        ),
        "action_text": first_nonempty_text(
            constraint_snapshot.get("action_text"),
            bootstrap_candidate_text(slots, "action_text"),
        ),
        "status_or_problem_text": first_nonempty_text(
            constraint_snapshot.get("status_or_problem_text"),
            bootstrap_candidate_text(slots, "status_or_problem_text"),
        ),
        "target_text": first_nonempty_text(
            constraint_snapshot.get("target_text"),
            bootstrap_candidate_text(slots, "target_text"),
        ),
        "result_hint": first_nonempty_text(
            constraint_snapshot.get("result_hint"),
            bootstrap_candidate_text(slots, "result_hint"),
        ),
        "status_numeric_constraint": deepcopy(numeric_constraint),
        "status_check_requested": "status_check" in intent_operators or isinstance(numeric_constraint, dict) or bool(bootstrap_signals.get("status_check_requested")),
        "asks_solution": "remediation" in intent_operators or bool(bootstrap_signals.get("asks_solution")),
        "asks_explanation": "explain" in intent_operators or bool(bootstrap_signals.get("asks_explanation")),
        "resolved_reference": resolved_reference,
    }


def build_bootstrap_intent_view(
    slots: Dict[str, Any],
    template: str,
    question_unit: Optional[Dict[str, Any]] = None,
    resolved_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a shared bootstrap intent view before a full Intent IR exists."""
    semantic_state = semantic_state_from_sources(slots)
    dependency = question_unit.get("dependency") if isinstance(question_unit, dict) and isinstance(question_unit.get("dependency"), dict) else None
    reference_markers = (
        list(question_unit.get("reference_markers", []))
        if isinstance(question_unit, dict) and isinstance(question_unit.get("reference_markers"), list)
        else list(slots.get("reference_markers", []))
        if isinstance(slots.get("reference_markers"), list)
        else []
    )

    operators: List[str] = []
    if semantic_state.get("status_check_requested"):
        operators.append("status_check")
    if semantic_state.get("asks_explanation") or isinstance(semantic_state.get("cause_text"), str):
        operators.append("explain")
    if semantic_state.get("asks_solution"):
        operators.append("remediation")
    if template in ("enumeration", "causal_enumeration"):
        operators.append("enumerate")
    if not operators:
        operators.append("lookup")
    operators = unique_preserve_order(operators)

    if semantic_state.get("has_anchor"):
        focus_kind = "anchored_entity"
        focus = {"kind": "anchored_entity", "anchors": deepcopy(semantic_state.get("anchors", []))}
    elif dependency or reference_markers:
        focus_kind = "previous_result_reference"
        focus = {"kind": "previous_result_reference"}
    elif template in ("enumeration", "causal_enumeration"):
        focus_kind = "result_set"
        focus = {"kind": "result_set"}
    else:
        focus_kind = "implicit"
        focus = {"kind": "implicit"}

    constraints: List[Dict[str, Any]] = []
    for key in ("cause_text", "action_text", "status_or_problem_text", "target_text", "result_hint"):
        value = first_nonempty_text(bootstrap_candidate_text(slots, key), semantic_state.get(key))
        if isinstance(value, str) and value.strip():
            constraints.append({"slot": key, "text": value})
    if isinstance(semantic_state.get("status_numeric_constraint"), dict):
        constraints.append({
            "slot": "status_numeric_constraint",
            "constraint": deepcopy(semantic_state["status_numeric_constraint"]),
        })

    if template == "causal_enumeration":
        output_shape = "entity_set"
    elif template == "enumeration":
        output_shape = "rows"
    else:
        output_shape = "entity"

    references: Dict[str, Any] = {
        "markers": reference_markers,
    }
    if isinstance(dependency, dict):
        references["depends_on"] = dependency.get("depends_on")
        references["condition"] = dependency.get("condition")
    if isinstance(resolved_reference, dict) and not semantic_state.get("has_explicit_anchor"):
        references["resolved"] = deepcopy(resolved_reference)

    return {
        "focus": focus,
        "focus_kind": focus_kind,
        "operators": operators,
        "constraints": constraints,
        "output": {"shape": output_shape},
        "output_shape": output_shape,
        "references": references,
    }


def derive_intent_profile(
    slots: Dict[str, Any],
    template: str,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a minimal profile that lets routing depend on Intent IR before templates."""
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    bootstrap_intent = build_bootstrap_intent_view(slots, template, question_unit=question_unit)
    operators = (
        list(unit_intent_ir.get("operators", []))
        if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("operators"), list)
        else list(bootstrap_intent.get("operators", []))
    )
    focus = unit_intent_ir.get("focus") if isinstance(unit_intent_ir, dict) else None
    if isinstance(focus, dict) and isinstance(focus.get("kind"), str) and focus.get("kind"):
        focus_kind = focus.get("kind")
    else:
        focus_kind = bootstrap_intent.get("focus_kind")
    output = unit_intent_ir.get("output") if isinstance(unit_intent_ir, dict) else bootstrap_intent.get("output")
    output_shape = (
        output.get("shape")
        if isinstance(output, dict) and isinstance(output.get("shape"), str) and output.get("shape")
        else None
    )
    references = unit_intent_ir.get("references") if isinstance(unit_intent_ir, dict) else bootstrap_intent.get("references")
    dependency_condition = (
        references.get("condition")
        if isinstance(references, dict) and isinstance(references.get("condition"), str)
        else None
    )
    reference_markers = (
        list(references.get("markers", []))
        if isinstance(references, dict) and isinstance(references.get("markers"), list)
        else []
    )
    has_semantic_content = any(
        isinstance(semantic_state.get(key), str) and str(semantic_state.get(key)).strip()
        for key in ("cause_text", "action_text", "status_or_problem_text", "target_text", "result_hint")
    ) or isinstance(semantic_state.get("status_numeric_constraint"), dict)
    has_explicit_anchor = bool(semantic_state.get("has_explicit_anchor"))
    wants_context = bool(
        dependency_condition
        or reference_markers
        or (
            any(op in ("remediation", "explain") for op in operators)
            and not has_explicit_anchor
            and not has_semantic_content
        )
    )
    scope_inheritance_allowed = wants_context and not has_explicit_anchor
    semantic_inheritance_allowed = (
        scope_inheritance_allowed
        and dependency_condition != "empty_or_false"
        and not (focus_kind == "previous_result_reference" and "explain" in operators)
    )
    reference_binding_allowed = focus_kind == "previous_result_reference" and not has_explicit_anchor
    explain_requested = "explain" in operators
    enumerate_requested = "enumerate" in operators
    has_explanation_phenomenon = bool(semantic_state.get("action_text"))
    has_causal_constraints = bool(
        semantic_state.get("cause_text") or semantic_state.get("status_or_problem_text")
    )
    has_status_constraints = bool(
        semantic_state.get("status_or_problem_text")
        or isinstance(semantic_state.get("status_numeric_constraint"), dict)
    )
    has_target_constraint = bool(semantic_state.get("target_text"))
    generic_explanation_target = is_generic_explanation_target(semantic_state.get("target_text"))
    family_bias = template
    effective_template_bias = template
    family_rationale: List[str] = []

    if template == "causal_lookup":
        family_bias = "anchored_causal_lookup" if focus_kind == "anchored_entity" else "causal_lookup"
        if family_bias == "anchored_causal_lookup":
            family_rationale.append("anchor_detected")
    elif template == "fact_lookup":
        family_bias = "anchored_fact_lookup" if focus_kind == "anchored_entity" else "fact_lookup"
        if family_bias == "anchored_fact_lookup":
            family_rationale.append("anchor_detected")
    elif template == "causal_enumeration":
        if (
            focus_kind == "anchored_entity"
            and (
                has_status_constraints
                or "status_check" in operators
                or "remediation" in operators
                or "explain" in operators
            )
        ):
            family_bias = "anchored_causal_lookup"
            effective_template_bias = "causal_lookup"
            family_rationale.extend(["smaller_family_reroute", "anchored_lookup_operator"])
        else:
            if (
                focus_kind != "anchored_entity"
                and enumerate_requested
                and explain_requested
                and not has_causal_constraints
                and (generic_explanation_target or has_explanation_phenomenon or has_target_constraint)
            ):
                family_bias = "explanation_enumeration"
                effective_template_bias = "enumeration"
                family_rationale.extend(["smaller_family_reroute", "explanation_operator"])
            else:
                family_bias = "causal_enumeration"
    elif template == "enumeration":
        if (
            focus_kind == "anchored_entity"
            and (
                has_status_constraints
                or "status_check" in operators
                or "remediation" in operators
                or "explain" in operators
            )
        ):
            family_bias = "anchored_causal_lookup"
            effective_template_bias = "causal_lookup"
            family_rationale.extend(["smaller_family_reroute", "anchored_lookup_operator"])
        else:
            if (
                enumerate_requested
                and explain_requested
                and (generic_explanation_target or has_explanation_phenomenon or has_target_constraint)
            ):
                family_bias = "explanation_enumeration"
                family_rationale.append("explanation_operator")
            else:
                family_bias = "enumeration"

    return {
        "operators": unique_preserve_order([str(item) for item in operators if isinstance(item, str) and item]),
        "focus_kind": focus_kind,
        "output_shape": output_shape,
        "scope_inheritance_allowed": scope_inheritance_allowed,
        "semantic_inheritance_allowed": semantic_inheritance_allowed,
        "reference_binding_allowed": reference_binding_allowed,
        "explain_requested": explain_requested,
        "enumerate_requested": enumerate_requested,
        "has_explanation_phenomenon": has_explanation_phenomenon,
        "has_causal_constraints": has_causal_constraints,
        "has_target_constraint": has_target_constraint,
        "generic_explanation_target": generic_explanation_target,
        "family_bias": family_bias,
        "effective_template_bias": effective_template_bias,
        "family_rationale": unique_preserve_order(family_rationale),
    }


def build_question_unit_intent_ir(
    unit: Dict[str, Any],
    slots: Dict[str, Any],
    template: str,
    resolved_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a lightweight Intent IR for one QuestionUnit."""
    bootstrap_intent = build_bootstrap_intent_view(
        slots,
        template,
        question_unit=unit,
        resolved_reference=resolved_reference,
    )

    return {
        "unit_id": unit.get("unit_id"),
        "focus": deepcopy(bootstrap_intent.get("focus", {})),
        "operators": list(bootstrap_intent.get("operators", [])),
        "constraints": deepcopy(bootstrap_intent.get("constraints", [])),
        "output": deepcopy(bootstrap_intent.get("output", {})),
        "references": deepcopy(bootstrap_intent.get("references", {})),
    }


def extract_question_slots(
    question: str,
    template: str,
    inherited_context: Optional[Dict[str, Any]] = None,
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Extract a small set of semantic slots from a natural-language question."""
    question_text = question.strip()
    anchors = detect_question_anchors(question_text)
    asks_solution = any(term in question_text for term in SOLUTION_REQUEST_TERMS)
    asks_explanation = any(term in question_text for term in EXPLANATION_REQUEST_TERMS)
    slots: Dict[str, Any] = {
        "question": question_text,
        "template": template,
        "question_type": template,
        "anchors": anchors,
        "has_anchor": bool(anchors),
        "has_explicit_anchor": bool(anchors),
        "status_numeric_constraint": None,
        "bootstrap_signals": {
            "asks_solution": asks_solution,
            "asks_explanation": asks_explanation,
            "status_check_requested": False,
        },
        "bootstrap_candidates": {},
    }

    stripped_question_text = question_text
    for anchor in anchors:
        if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
            stripped_question_text = stripped_question_text.replace(anchor["value"], " ")
    stripped_question_text = normalize_slot_text(stripped_question_text) or question_text
    global_numeric_constraint = parse_numeric_constraint_text(stripped_question_text)
    if isinstance(global_numeric_constraint, dict):
        register_bootstrap_candidate(
            slots,
            "status_or_problem_text",
            global_numeric_constraint.get("attribute_text"),
            "question_numeric_attribute",
        )
        slots["status_numeric_constraint"] = global_numeric_constraint
        if isinstance(slots.get("bootstrap_signals"), dict):
            slots["bootstrap_signals"]["status_check_requested"] = True

    cause_match = CAUSE_PATTERN.search(question_text)
    if cause_match:
        register_bootstrap_candidate(
            slots,
            "cause_text",
            cause_match.group("cause").strip(" ，,。？? "),
            "cause_pattern",
        )

    status_match = STATUS_CHECK_PATTERN.search(question_text)
    if status_match:
        status_text = normalize_slot_text(status_match.group("status"))
        if status_text:
            numeric_constraint = slots.get("status_numeric_constraint")
            if not isinstance(numeric_constraint, dict):
                numeric_constraint = parse_numeric_constraint_text(status_text)
            if isinstance(numeric_constraint, dict):
                register_bootstrap_candidate(
                    slots,
                    "status_or_problem_text",
                    numeric_constraint.get("attribute_text"),
                    "status_numeric_attribute",
                )
                slots["status_numeric_constraint"] = numeric_constraint
            else:
                register_bootstrap_candidate(
                    slots,
                    "status_or_problem_text",
                    status_text,
                    "status_pattern",
                )
            if isinstance(slots.get("bootstrap_signals"), dict):
                slots["bootstrap_signals"]["status_check_requested"] = True

    which_match = WHICH_PATTERN.search(question_text)
    if which_match:
        tail = which_match.group("tail").strip(" ，,。？? ")
        register_bootstrap_candidate(slots, "result_hint", tail, "which_pattern")
        tail = re.sub(r"(了|过|呢|吗|呀|啊)+$", "", tail).strip()
        for suffix in ("投诉", "报修", "办理", "购买", "订购", "使用", "反馈", "关联", "命中"):
            if tail.endswith(suffix):
                register_bootstrap_candidate(slots, "action_text", suffix, "which_suffix")
                break
        if bootstrap_candidate_text(slots, "action_text") is None and tail:
            compact_tail = re.sub(r"(客户|用户|事件|工单|策略|感知|产品|网络|套餐)+", "", tail).strip()
            compact_tail = re.sub(r"(了|过|的)$", "", compact_tail).strip()
            if compact_tail:
                register_bootstrap_candidate(slots, "action_text", compact_tail, "which_compact_tail")

    asks_for_match = ASKS_FOR_PATTERN.search(question_text)
    if asks_for_match and not bootstrap_candidate_text(slots, "target_text"):
        target_text = normalize_slot_text(asks_for_match.group("target"))
        if target_text and target_text not in SOLUTION_REQUEST_TERMS:
            register_bootstrap_candidate(slots, "target_text", target_text, "asks_for_pattern")

    reference_markers = question_unit.get("reference_markers", []) if isinstance(question_unit, dict) else []
    if (
        bootstrap_candidate_text(slots, "target_text") is None
        and not asks_explanation
        and not bool(reference_markers)
        and not bool(slots.get("bootstrap_signals", {}).get("status_check_requested"))
    ):
        lookup_question_text = stripped_question_text.strip(" ，,。？?!！;；")
        lookup_match = LOOKUP_TARGET_PATTERN.search(lookup_question_text)
        if lookup_match:
            target_text = normalize_slot_text(lookup_match.group("target"))
            if target_text and "的" in target_text:
                target_text = normalize_slot_text(target_text.split("的")[-1])
            if target_text and target_text not in SOLUTION_REQUEST_TERMS:
                register_bootstrap_candidate(slots, "target_text", target_text, "lookup_target_pattern")

    if (
        asks_explanation
        and bootstrap_candidate_text(slots, "action_text") is None
        and bootstrap_candidate_text(slots, "cause_text") is None
        and bootstrap_candidate_text(slots, "status_or_problem_text") is None
    ):
        explanation_subject = infer_explanation_subject_text(question_text, bootstrap_candidate_text(slots, "target_text"))
        if explanation_subject:
            register_bootstrap_candidate(slots, "action_text", explanation_subject, "explanation_subject")

    return merge_inherited_slots(slots, inherited_context, question_unit or {})


def route_query_family(
    question: str,
    template: str,
    slots: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Route the question into a smaller planner family before grounding."""
    intent_profile = derive_intent_profile(slots, template, unit_intent_ir=unit_intent_ir)
    family = intent_profile.get("family_bias") or template
    effective_template = intent_profile.get("effective_template_bias") or template
    rationale = list(intent_profile.get("family_rationale", []))
    if template == "hidden_relation":
        family = "hidden_relation"
        effective_template = "hidden_relation"

    return {
        "family": family,
        "effective_template": effective_template,
        "rationale": rationale,
    }


def class_match_score(question_text: str, tail_text: str, class_item: Dict[str, Any]) -> int:
    """Score how well a class matches the user question."""
    label = class_item.get("label") or class_item.get("local_name") or ""
    local_name = class_item.get("local_name") or ""
    score = 0
    for term in normalized_terms_from_text(label) + normalized_terms_from_text(local_name):
        if not term:
            continue
        if term in question_text:
            score += 4
        if tail_text and term in tail_text:
            score += 6
    if label and tail_text and tail_text.startswith(str(label)):
        score += 4
    return score


def property_surface_text(prop: Dict[str, Any], domain_class: Optional[str] = None) -> str:
    """Return a human-oriented property text with domain-local prefixes stripped when possible."""
    label = prop.get("label")
    local_name = prop.get("local_name")
    parts: List[str] = []
    normalized_label = normalize_match_text(label) if isinstance(label, str) and label else ""
    normalized_local_name = normalize_match_text(local_name) if isinstance(local_name, str) and local_name else ""
    if (
        isinstance(label, str)
        and label
        and normalized_label
        and normalized_label != normalized_local_name
    ):
        parts.append(label)
    if isinstance(local_name, str) and local_name:
        suffix = local_name
        if isinstance(domain_class, str) and domain_class:
            prefix = f"{domain_class}_"
            if local_name.startswith(prefix):
                suffix = local_name[len(prefix):]
        if suffix and suffix not in parts:
            parts.append(suffix)
    return " ".join(parts)


def property_role_score(prop: Dict[str, Any], role: str) -> int:
    """Score a data property against a semantic role such as id/name/type."""
    text = " ".join(
        part for part in [str(prop.get("label", "")), str(prop.get("local_name", ""))] if part
    )
    normalized = normalize_match_text(text)
    score = 0
    for hint in ROLE_PATTERNS.get(role, []):
        if normalize_match_text(hint) in normalized:
            score += 3
    return score


def property_role_selection_score(
    prop: Dict[str, Any],
    role: str,
    domain_class: str,
    class_labels: Optional[Dict[str, str]] = None,
) -> int:
    """Score a role-bearing property within a class, preferring self-descriptive attributes over foreign-key-like ones."""
    base_score = property_role_score(prop, role)
    if base_score <= 0:
        return 0

    domain_label = None
    if isinstance(class_labels, dict):
        candidate_label = class_labels.get(domain_class)
        if isinstance(candidate_label, str) and candidate_label:
            domain_label = candidate_label

    property_text = property_surface_text(prop, domain_class)
    compact_property_text = compact_match_text(property_text)
    class_affinity = 0
    for class_text in unique_preserve_order([domain_label, domain_class]):
        if not isinstance(class_text, str) or not class_text:
            continue
        compact_class_text = compact_match_text(class_text)
        if not compact_class_text:
            continue
        if compact_class_text in compact_property_text:
            class_affinity += 4
        class_affinity += min(6, char_ngram_overlap_score(class_text, property_text))

    # Keep role hints dominant, then use class affinity to break ties toward self-identifying fields.
    return base_score * 10 + class_affinity


def best_role_property(
    domain_class: str,
    role: str,
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Pick the best data property local_name for a semantic role within a class."""
    best_name = None
    best_score = 0
    for prop in domain_properties.get(domain_class, []):
        score = property_role_selection_score(prop, role, domain_class, class_labels)
        if score > best_score:
            best_score = score
            best_name = prop.get("local_name")
    return best_name


def split_constraint_terms(text: Optional[str]) -> List[str]:
    """Split a free-text semantic constraint into a small set of lexical search terms."""
    if not isinstance(text, str):
        return []
    cleaned = text.strip(" ，,。？? ")
    if not cleaned:
        return []

    parts = re.split(r"[、/／,，和及与或]", cleaned)
    terms: List[str] = []
    for part in parts:
        item = part.strip()
        if not item:
            continue
        terms.append(item)
        for suffix in QUESTION_STOP_TERMS:
            if item.endswith(suffix) and len(item) > len(suffix):
                terms.append(item[: -len(suffix)])
    return unique_preserve_order([term for term in terms if term])


def expand_constraint_terms(text: Optional[str], schema: Optional[Dict[str, Any]]) -> List[str]:
    """Expand a semantic constraint with ontology-near lexical variants."""
    base_terms = split_constraint_terms(text)
    if not base_terms:
        return []

    expanded = list(base_terms)
    normalized_base = [normalize_match_text(term) for term in base_terms if normalize_match_text(term)]
    if not normalized_base:
        return expanded

    for item in class_catalog(schema):
        label = item.get("label") or item.get("local_name")
        if not isinstance(label, str) or not label:
            continue
        normalized_label = normalize_match_text(label)
        if not normalized_label:
            continue
        if not any(term in normalized_label for term in normalized_base):
            continue

        expanded.extend(split_constraint_terms(label))
        for suffix in SEMANTIC_LABEL_SUFFIXES:
            if label.endswith(suffix) and len(label) > len(suffix):
                candidate = label[: -len(suffix)].strip()
                if len(candidate) >= 2:
                    expanded.append(candidate)

    return unique_preserve_order([term for term in expanded if term])


def choose_source_class_candidate(question: str, manifest: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Choose the most likely source class from the question."""
    question_text = normalize_match_text(question)
    which_match = WHICH_PATTERN.search(question)
    tail_text = normalize_match_text(which_match.group("tail")) if which_match else ""

    candidates = []
    for item in manifest.get("classes", []) if isinstance(manifest, dict) else []:
        class_item = {
            "label": item.get("label"),
            "local_name": item.get("class_name"),
        }
        score = class_match_score(question_text, tail_text, class_item)
        if score <= 0:
            continue
        candidates.append({
            "class_name": item["class_name"],
            "label": item.get("label") or item["class_name"],
            "score": score,
        })
    candidates.sort(key=lambda item: (-item["score"], item["class_name"]))
    return {
        "selected": candidates[0] if candidates else None,
        "candidates": candidates[:5],
    }


def choose_source_class_candidate_with_anchors(
    question: str,
    manifest: Optional[Dict[str, Any]],
    slots: Dict[str, Any],
) -> Dict[str, Any]:
    """Choose the source class, falling back to generic anchor signals when lexical grounding is absent."""
    source_info = choose_source_class_candidate(question, manifest)

    anchors = slots.get("anchors", [])
    if not isinstance(anchors, list) or not anchors:
        return source_info

    class_index = manifest.get("class_index", {}) if isinstance(manifest, dict) else {}
    domain_properties = manifest_attributes_by_class(manifest)
    class_labels = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(manifest, dict) and isinstance(item, dict) and item.get("class_name")
    }

    anchored_candidates = []
    for anchor in anchors:
        if not isinstance(anchor, dict):
            continue

        class_hint = anchor.get("class_hint")
        if isinstance(class_hint, str) and class_hint in class_index:
            anchored_candidates.append({
                "class_name": class_hint,
                "label": class_labels.get(class_hint, class_hint),
                "score": 14,
                "anchor_kind": anchor.get("kind"),
                "anchor_value": anchor.get("value"),
            })

        if anchor.get("kind") != "identifier_like_literal":
            continue

        for domain_class, props in domain_properties.items():
            best_anchor_property = None
            best_anchor_score = 0
            for prop in props:
                if not isinstance(prop, dict):
                    continue
                score = property_role_score(prop, "id")
                score = max(score, property_role_score(prop, "phone"))
                if score > best_anchor_score:
                    best_anchor_score = score
                    best_anchor_property = prop.get("local_name")
            if not best_anchor_property or best_anchor_score <= 0:
                continue

            anchored_candidates.append({
                "class_name": domain_class,
                "label": class_labels.get(domain_class, domain_class),
                "score": 8 + best_anchor_score,
                "anchor_kind": anchor.get("kind"),
                "anchor_value": anchor.get("value"),
                "anchor_property": best_anchor_property,
            })

    reference_entity_class = slots.get("reference_entity_class")
    if isinstance(reference_entity_class, str) and reference_entity_class in class_index:
        anchored_candidates.append({
            "class_name": reference_entity_class,
            "label": class_labels.get(reference_entity_class, reference_entity_class),
            "score": 18,
            "anchor_kind": "previous_result_reference",
            "anchor_value": slots.get("reference_from_unit_id"),
        })

    combined_by_class: Dict[str, Dict[str, Any]] = {}
    for item in source_info.get("candidates", []) + anchored_candidates:
        if not isinstance(item, dict):
            continue
        class_name = item.get("class_name")
        if not isinstance(class_name, str) or not class_name:
            continue
        current = combined_by_class.get(class_name)
        if current is None or item.get("score", 0) > current.get("score", 0):
            combined_by_class[class_name] = item

    merged_candidates = sorted(
        combined_by_class.values(),
        key=lambda item: (-item["score"], item["class_name"]),
    )
    return {
        "selected": merged_candidates[0] if merged_candidates else None,
        "candidates": merged_candidates[:5],
    }


def build_semantic_request_ir(
    question: str,
    template: str,
    slots: Dict[str, Any],
    routing: Dict[str, Any],
    slot_inputs: List[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
    source_info: Dict[str, Any],
    evidence_candidates: List[Dict[str, Any]],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a generic semantic request IR before plan construction."""
    source_selected = source_info.get("selected") if isinstance(source_info, dict) else None
    constraints: List[Dict[str, Any]] = []
    references: Dict[str, Any] = {}
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    cause_text = semantic_state.get("cause_text")
    if isinstance(cause_text, str) and cause_text:
        constraints.append({
            "kind": "semantic_constraint",
            "intent": "cause",
            "raw_text": cause_text,
        })
    action_text = semantic_state.get("action_text")
    if isinstance(action_text, str) and action_text:
        constraints.append({
            "kind": "semantic_constraint",
            "intent": "action_or_state",
            "raw_text": action_text,
        })
    status_or_problem_text = semantic_state.get("status_or_problem_text")
    numeric_constraint = semantic_state.get("status_numeric_constraint")
    if isinstance(status_or_problem_text, str) and status_or_problem_text:
        status_constraint = {
            "kind": "semantic_constraint",
            "intent": "status_or_problem",
            "raw_text": status_or_problem_text,
            "constraint_mode": (
                "status_check"
                if semantic_state.get("status_check_requested")
                else "problem_text"
            ),
        }
        if isinstance(numeric_constraint, dict):
            status_constraint["comparison"] = {
                "op": numeric_constraint.get("op"),
                "value": numeric_constraint.get("value"),
            }
        constraints.append(status_constraint)

    if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("references"), dict):
        references = deepcopy(unit_intent_ir.get("references") or {})
    resolved_reference = references.get("resolved")
    if not isinstance(resolved_reference, dict):
        resolved_reference = semantic_state.get("resolved_reference")
    if isinstance(resolved_reference, dict):
        references["resolved"] = deepcopy(resolved_reference)

    return {
        "question": question,
        "requested_template": template,
        "effective_template": routing.get("effective_template", template),
        "query_family": routing.get("family", template),
        "routing_rationale": routing.get("rationale", []),
        "anchors": slots.get("anchors", []),
        "slot_inputs": slot_inputs,
        "slot_bindings": slot_bindings,
        "source": {
            "selected_class": source_selected.get("class_name") if isinstance(source_selected, dict) else None,
            "candidates": source_info.get("candidates", []) if isinstance(source_info, dict) else [],
        },
        "evidence": {
            "candidate_classes": [
                item.get("class_name")
                for item in evidence_candidates
                if isinstance(item, dict) and isinstance(item.get("class_name"), str)
            ],
        },
        "references": references,
        "constraints": constraints,
        "output": {
            "grain": "entity"
            if routing.get("family") in ("anchored_fact_lookup", "anchored_causal_lookup", "causal_lookup")
            else "entity_set" if routing.get("family") == "causal_enumeration" else "rows",
            "needs_analysis": routing.get("effective_template") in ("causal_lookup", "causal_enumeration"),
            "asks_solution": bool(semantic_state.get("asks_solution")),
            "asks_explanation": bool(semantic_state.get("asks_explanation")),
        },
    }


def build_node_plan(
    request_ir: Dict[str, Any],
    source_class: str,
    evidence_class: str,
    relation_info: Optional[Dict[str, Any]],
    include_cause: bool,
    include_action: bool,
    include_status: bool = False,
    separate_action_support: bool = False,
) -> Dict[str, Any]:
    """Build a generic node-based plan representation for planner debugging and lowering."""
    nodes: List[Dict[str, Any]] = [
        {
            "type": "SourceScan",
            "class": source_class,
            "var": "source",
        }
    ]

    anchors = request_ir.get("anchors")
    if isinstance(anchors, list):
        for anchor in anchors:
            if not isinstance(anchor, dict):
                continue
            nodes.append({
                "type": "AnchorResolve",
                "anchor_kind": anchor.get("kind"),
                "anchor_value": anchor.get("value"),
            })

    resolved_reference = None
    references = request_ir.get("references")
    if isinstance(references, dict) and isinstance(references.get("resolved"), dict):
        resolved_reference = references["resolved"]
    if isinstance(resolved_reference, dict):
        reference_entity_uris = [
            str(item)
            for item in resolved_reference.get("entity_uris", [])
            if isinstance(item, str) and item
        ]
        if reference_entity_uris:
            nodes.append({
                "type": "ReferenceFilter",
                "scope": "source",
                "from_unit_id": resolved_reference.get("from_unit_id"),
                "entity_class": resolved_reference.get("entity_class"),
                "entity_count": len(reference_entity_uris),
                "grain": resolved_reference.get("grain"),
            })

    if relation_info and relation_info.get("property"):
        nodes.append({
            "type": "EvidenceTraverse",
            "from_var": "source",
            "to_var": "evidence",
            "property": relation_info.get("property"),
            "direction": relation_info.get("direction", "forward"),
            "evidence_class": evidence_class,
        })
    else:
        nodes.append({
            "type": "EvidenceScan",
            "class": evidence_class,
            "var": "evidence",
        })

    if include_cause:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "cause",
            "scope": "evidence",
        })
    if include_status:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "status_or_problem",
            "scope": "evidence",
        })
    if include_action:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "action_or_state",
            "scope": "support_evidence" if separate_action_support else "evidence",
        })

    if request_ir.get("output", {}).get("needs_analysis"):
        nodes.append({
            "type": "AnalyzerRequest",
            "analysis_kind": "paths-batch"
            if request_ir.get("effective_template") == "causal_enumeration"
            else "paths",
        })

    nodes.append({
        "type": "Project",
        "grain": request_ir.get("output", {}).get("grain"),
    })
    return {
        "query_family": request_ir.get("query_family"),
        "nodes": nodes,
    }


def choose_evidence_class_candidates(
    source_class: str,
    manifest: Optional[Dict[str, Any]],
    slots: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Choose plausible evidence classes connected to the selected source class."""
    relations = manifest.get("relations", []) if isinstance(manifest, dict) else []
    domain_properties = manifest_attributes_by_class(manifest)
    class_labels = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(manifest, dict) and isinstance(item, dict) and item.get("class_name")
    }
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    question_text = normalize_match_text(slots.get("question"))
    cause_tokens = expand_constraint_terms(
        semantic_state.get("cause_text"),
        None,
    )
    action_tokens = expand_constraint_terms(
        semantic_state.get("action_text"),
        None,
    )

    candidates: Dict[str, Dict[str, Any]] = {}
    for relation in relations:
        if relation["source_class"] != source_class:
            continue
        evidence_class = relation["target_class"]
        label = class_labels.get(evidence_class, evidence_class)
        score = 0
        if any(term in normalize_match_text(label) for term in EVENT_LIKE_TERMS):
            score += 6
        if normalize_match_text(label) in question_text:
            score += 4
        evidence_props = domain_properties.get(evidence_class, [])
        if best_role_property(evidence_class, "type", domain_properties, class_labels):
            score += 3
        if best_role_property(evidence_class, "description", domain_properties, class_labels):
            score += 3
        if cause_tokens and evidence_props:
            score += 2
        if action_tokens and evidence_props:
            score += 2
        if score <= 0:
            continue
        bucket = candidates.setdefault(evidence_class, {
            "class_name": evidence_class,
            "label": label,
            "score": 0,
            "relations": [],
        })
        bucket["score"] = max(bucket["score"], score)
        bucket["relations"].append({
            "property": relation["property"],
            "validation_source": relation["validation_source"],
        })

    results = list(candidates.values())
    results.sort(key=lambda item: (-item["score"], item["class_name"]))
    return results[:5]


def is_numeric_data_property(prop: Dict[str, Any]) -> bool:
    """Whether a data property range looks numeric."""
    range_uri = prop.get("range")
    if not isinstance(range_uri, str):
        return False
    lowered = range_uri.lower()
    return any(marker in lowered for marker in NUMERIC_XSD_MARKERS)


def matching_action_support_properties(
    action_class: str,
    action_terms: List[str],
    domain_properties: Dict[str, List[Dict[str, Any]]],
    slot_text: Optional[str] = None,
) -> Dict[str, List[str]]:
    """Find text and numeric properties that can support an action constraint."""
    normalized_terms = [normalize_match_text(term) for term in action_terms if normalize_match_text(term)]
    normalized_slot_text = normalize_match_text(slot_text) if isinstance(slot_text, str) else ""
    if not normalized_terms and not normalized_slot_text:
        return {"text": [], "numeric": []}

    text_props: Dict[str, tuple[float, int]] = {}
    numeric_props: Dict[str, tuple[float, int]] = {}
    for prop in domain_properties.get(action_class, []):
        local_name = prop.get("local_name")
        if not isinstance(local_name, str) or not local_name:
            continue
        surface_text = normalize_match_text(prop.get("label") or local_name)
        searchable = normalize_match_text(f"{surface_text} {local_name}")
        term_match_score = 0
        for term in normalized_terms:
            if term in searchable or searchable in term:
                term_match_score += 4
            term_match_score += char_ngram_overlap_score(term, searchable)
        slot_match_score = char_ngram_overlap_score(normalized_slot_text, searchable) if normalized_slot_text else 0
        residual_text = surface_text
        if normalized_slot_text:
            residual_text = residual_text.replace(normalized_slot_text, " ")
        residual_text = re.sub(r"(次数|count|rate|score|值|value)", " ", residual_text)
        residual_text = re.sub(r"\s+", " ", residual_text).strip()
        specificity_penalty = len(residual_text)
        if is_numeric_data_property(prop):
            # Keep numeric support tied to the raw slot phrase so value-term widening
            # does not spill into unrelated metrics like generic service/network scores.
            if slot_match_score <= 0:
                continue
            candidate = (term_match_score + slot_match_score, specificity_penalty)
            existing = numeric_props.get(local_name)
            if existing is None or candidate[0] > existing[0] or (candidate[0] == existing[0] and candidate[1] < existing[1]):
                numeric_props[local_name] = candidate
        else:
            if term_match_score + slot_match_score <= 0:
                continue
            candidate = (term_match_score + slot_match_score, specificity_penalty)
            existing = text_props.get(local_name)
            if existing is None or candidate[0] > existing[0] or (candidate[0] == existing[0] and candidate[1] < existing[1]):
                text_props[local_name] = candidate

    text_items = sorted(text_props.items(), key=lambda item: (-item[1][0], item[1][1], len(item[0]), item[0]))
    numeric_items = sorted(numeric_props.items(), key=lambda item: (-item[1][0], item[1][1], len(item[0]), item[0]))

    return {
        "text": unique_preserve_order([name for name, _ in text_items[:3]]),
        "numeric": unique_preserve_order([name for name, _ in numeric_items[:2]]),
    }


def choose_action_support_classes(
    evidence_candidates: List[Dict[str, Any]],
    action_terms: List[str],
    domain_properties: Dict[str, List[Dict[str, Any]]],
    slot_text: Optional[str] = None,
    limit: int = 5,
) -> List[str]:
    """Choose the strongest support evidence classes for an action/state slot."""
    scored: List[Dict[str, Any]] = []
    for candidate in evidence_candidates:
        if not isinstance(candidate, dict):
            continue
        class_name = candidate.get("class_name")
        if not isinstance(class_name, str) or not class_name:
            continue
        support = matching_action_support_properties(
            class_name,
            action_terms,
            domain_properties,
            slot_text=slot_text,
        )
        support_score = (
            len(support.get("text", []))
            + 2 * len(support.get("numeric", []))
        )
        if support_score <= 0:
            continue
        scored.append({
            "class_name": class_name,
            "score": support_score + float(candidate.get("score", 0.0) or 0.0) * 0.1,
        })
    scored.sort(key=lambda item: (-item["score"], item["class_name"]))
    return [
        item["class_name"]
        for item in scored[:limit]
    ]


def build_text_exists_expression(
    namespace: str,
    source_var: str,
    source_class: str,
    action_class: str,
    relation_info: Dict[str, Any],
    alias_prefix: str,
    action_terms: List[str],
    domain_properties: Dict[str, List[Dict[str, Any]]],
) -> Optional[str]:
    """Build an EXISTS clause for action evidence backed by text properties."""
    text_props: List[str] = []
    type_prop = best_role_property(action_class, "type", domain_properties)
    desc_prop = best_role_property(action_class, "description", domain_properties)
    for prop_name in (type_prop, desc_prop):
        if isinstance(prop_name, str) and prop_name:
            text_props.append(prop_name)
    text_props = unique_preserve_order(text_props)
    if not text_props:
        return None

    action_var = f"{alias_prefix}Evidence"
    lines = [
        f"EXISTS {{",
        f"    ?{action_var} a ex:{action_class} .",
    ]
    if relation_info["direction"] == "forward":
        lines.append(f"    ?{source_var} ex:{relation_info['property']} ?{action_var} .")
    else:
        lines.append(f"    ?{action_var} ex:{relation_info['property']} ?{source_var} .")

    filter_vars = []
    for index, prop_name in enumerate(text_props):
        var_name = f"{alias_prefix}Text{index}"
        filter_vars.append(var_name)
        lines.append(f"    ?{action_var} ex:{prop_name} ?{var_name} .")

    filter_tree = build_constraint_filter(filter_vars, action_terms)
    if filter_tree is None:
        return None
    lines.append(f"    FILTER({compile_filter_expression(filter_tree)})")
    lines.append("  }")
    return "\n".join(lines)


def build_numeric_exists_expression(
    namespace: str,
    source_var: str,
    action_class: str,
    relation_info: Dict[str, Any],
    alias_prefix: str,
    numeric_props: List[str],
) -> Optional[str]:
    """Build an EXISTS clause for action evidence backed by positive numeric counters."""
    if not numeric_props:
        return None

    action_var = f"{alias_prefix}Evidence"
    lines = [
        "EXISTS {",
        f"    ?{action_var} a ex:{action_class} .",
    ]
    if relation_info["direction"] == "forward":
        lines.append(f"    ?{source_var} ex:{relation_info['property']} ?{action_var} .")
    else:
        lines.append(f"    ?{action_var} ex:{relation_info['property']} ?{source_var} .")

    metric_checks = []
    for index, prop_name in enumerate(numeric_props):
        var_name = f"{alias_prefix}Metric{index}"
        lines.append(f"    OPTIONAL {{ ?{action_var} ex:{prop_name} ?{var_name} . }}")
        metric_checks.append(f"(BOUND(?{var_name}) && ?{var_name} > 0)")

    lines.append(f"    FILTER({' || '.join(metric_checks)})")
    lines.append("  }")
    return "\n".join(lines)


def build_source_support_exists_clauses(
    namespace: str,
    source_var: str,
    source_class: str,
    support_terms: List[str],
    support_slot_text: Optional[str],
    support_classes: List[str],
    indexes: Dict[str, Any],
    domain_properties: Dict[str, List[Dict[str, Any]]],
) -> List[str]:
    """Build generic source-support EXISTS clauses across connected evidence classes."""
    support_exists_clauses: List[str] = []
    for index, action_class in enumerate(unique_preserve_order(support_classes)):
        if not isinstance(action_class, str) or not action_class:
            continue
        try:
            relation_info = resolve_builder_link_direction(source_class, action_class, None, indexes)
        except SystemExit:
            continue

        alias_prefix = f"support{index}"
        text_clause = build_text_exists_expression(
            namespace,
            source_var,
            source_class,
            action_class,
            relation_info,
            alias_prefix,
            support_terms,
            domain_properties,
        )
        if text_clause is not None:
            support_exists_clauses.append(text_clause)

        support_props = matching_action_support_properties(
            action_class,
            support_terms,
            domain_properties,
            slot_text=support_slot_text,
        )
        numeric_clause = build_numeric_exists_expression(
            namespace,
            source_var,
            action_class,
            relation_info,
            alias_prefix,
            support_props["numeric"],
        )
        if numeric_clause is not None:
            support_exists_clauses.append(numeric_clause)

    return support_exists_clauses


def build_multi_evidence_relaxed_query(
    schema: Optional[Dict[str, Any]],
    source_class: str,
    evidence_class: str,
    action_terms: List[str],
    cause_terms: List[str],
    source_id_prop: Optional[str],
    source_name_prop: Optional[str],
    evidence_id_prop: Optional[str],
    evidence_type_prop: Optional[str],
    evidence_desc_prop: Optional[str],
    source_uri_values: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a source-grain multi-evidence causal query with separate action support."""
    if not cause_terms or not action_terms:
        return None
    if not evidence_type_prop and not evidence_desc_prop:
        return None

    indexes = schema_indexes(schema)
    namespace = primary_namespace_from_schema(schema)
    domain_properties = data_properties_by_domain(schema)

    try:
        cause_link_info = resolve_builder_link_direction(source_class, evidence_class, None, indexes)
    except SystemExit:
        return None

    source_var = "source"
    evidence_var = "evidence"
    patterns = [
        f"?{source_var} a ex:{source_class} .",
        f"?{evidence_var} a ex:{evidence_class} .",
    ]
    if cause_link_info["direction"] == "forward":
        patterns.append(f"?{source_var} ex:{cause_link_info['property']} ?{evidence_var} .")
    else:
        patterns.append(f"?{evidence_var} ex:{cause_link_info['property']} ?{source_var} .")

    select_vars = ["source", "evidence"]
    if source_id_prop:
        patterns.append(f"?{source_var} ex:{source_id_prop} ?sourceId .")
        select_vars.append("sourceId")
    if source_name_prop:
        patterns.append(f"?{source_var} ex:{source_name_prop} ?sourceName .")
        select_vars.append("sourceName")
    if evidence_id_prop:
        patterns.append(f"?{evidence_var} ex:{evidence_id_prop} ?evidenceId .")
        select_vars.append("evidenceId")

    cause_filter_vars: List[str] = []
    if evidence_type_prop:
        patterns.append(f"?{evidence_var} ex:{evidence_type_prop} ?evidenceType .")
        select_vars.append("evidenceType")
        cause_filter_vars.append("evidenceType")
    if evidence_desc_prop:
        patterns.append(f"?{evidence_var} ex:{evidence_desc_prop} ?evidenceDescription .")
        select_vars.append("evidenceDescription")
        cause_filter_vars.append("evidenceDescription")

    cause_filter = build_constraint_filter(cause_filter_vars, cause_terms)
    if cause_filter is None:
        return None

    action_exists_clauses: List[str] = []
    connected_classes = choose_evidence_class_candidates(source_class, schema, {
        "question": "",
        "cause_text": None,
        "action_text": " ".join(action_terms),
    })
    action_candidates = unique_preserve_order(
        [evidence_class] + [item["class_name"] for item in connected_classes]
    )

    for index, action_class in enumerate(action_candidates):
        try:
            relation_info = resolve_builder_link_direction(source_class, action_class, None, indexes)
        except SystemExit:
            continue

        alias_prefix = f"actionSupport{index}"
        text_clause = build_text_exists_expression(
            namespace,
            source_var,
            source_class,
            action_class,
            relation_info,
            alias_prefix,
            action_terms,
            domain_properties,
        )
        if text_clause is not None:
            action_exists_clauses.append(text_clause)

        support_props = matching_action_support_properties(action_class, action_terms, domain_properties)
        numeric_clause = build_numeric_exists_expression(
            namespace,
            source_var,
            action_class,
            relation_info,
            alias_prefix,
            support_props["numeric"],
        )
        if numeric_clause is not None:
            action_exists_clauses.append(numeric_clause)

    if not action_exists_clauses:
        return None

    query_lines = [
        f"PREFIX ex: <{namespace}>",
        "SELECT DISTINCT " + " ".join(f"?{var_name}" for var_name in select_vars),
        "WHERE {",
    ]
    query_lines.extend(f"  {pattern}" for pattern in patterns)
    normalized_source_uri_values = unique_preserve_order(
        [str(value) for value in (source_uri_values or []) if isinstance(value, str) and value]
    )
    if len(normalized_source_uri_values) == 1:
        query_lines.append(
            f"  FILTER({compile_filter_expression({'var': source_var, 'op': 'equals', 'value': normalized_source_uri_values[0]})})"
        )
    elif normalized_source_uri_values:
        query_lines.append(
            f"  FILTER({compile_filter_expression({'var': source_var, 'op': 'in', 'values': normalized_source_uri_values})})"
        )
    query_lines.append(f"  FILTER({compile_filter_expression(cause_filter)})")
    query_lines.append("  FILTER(")
    for index, clause in enumerate(action_exists_clauses):
        indented = "\n".join(f"    {line}" for line in clause.splitlines())
        separator = " ||" if index < len(action_exists_clauses) - 1 else ""
        query_lines.append(f"{indented}{separator}")
    query_lines.append("  )")
    query_lines.append("}")
    order_vars = [var_name for var_name in ("sourceId", "evidenceType", "evidenceId") if var_name in select_vars]
    if order_vars:
        query_lines.append("ORDER BY " + " ".join(f"?{var_name}" for var_name in order_vars))

    return {
        "query": "\n".join(query_lines),
        "source_var": source_var,
    }


def choose_enumeration_value_projection(
    slot_bindings: List[Dict[str, Any]],
    evidence_class: str,
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Dict[str, str],
    prefer_explanation: bool = False,
    allow_generic_explanation_projection: bool = False,
) -> Optional[Dict[str, Any]]:
    """Choose a stable evidence property to enumerate as the output value."""
    attribute_candidate = top_attribute_candidate_for_slot(
        slot_bindings,
        "target_text",
        class_name=evidence_class,
        numeric_only=False,
    )
    if (
        isinstance(attribute_candidate, dict)
        and not bool(attribute_candidate.get("numeric"))
        and float(attribute_candidate.get("total_score", 0.0) or 0.0) >= 8.0
        and isinstance(attribute_candidate.get("local_name"), str)
        and attribute_candidate.get("local_name")
    ):
        value_property = attribute_candidate["local_name"]
        rationale = ["target_slot_grounded"]
    else:
        value_candidate = top_value_candidate_for_slot(slot_bindings, "target_text", class_name=evidence_class)
        if (
            isinstance(value_candidate, dict)
            and isinstance(value_candidate.get("property_local_name"), str)
            and value_candidate.get("property_local_name")
            and float(value_candidate.get("total_score", 0.0) or 0.0) >= 8.0
        ):
            value_property = value_candidate["property_local_name"]
            rationale = ["target_value_grounded"]
        else:
            preferred_roles = ["type", "description"] if prefer_explanation else ["type", "name", "description"]
            value_property = None
            for role in preferred_roles:
                value_property = best_role_property(evidence_class, role, domain_properties, class_labels)
                if isinstance(value_property, str) and value_property:
                    break
            if not isinstance(value_property, str) or not value_property:
                return None
            rationale = (
                ["target_explanation_projection"]
                if allow_generic_explanation_projection and prefer_explanation
                else ["target_role_fallback"]
            )

    description_property = best_role_property(evidence_class, "description", domain_properties, class_labels)
    if description_property == value_property:
        description_property = None

    return {
        "value_property": value_property,
        "description_property": description_property,
        "rationale": rationale,
    }


def build_value_enumeration_query(
    schema: Optional[Dict[str, Any]],
    source_class: str,
    evidence_class: str,
    relation_info: Optional[Dict[str, Any]],
    value_property: str,
    description_property: Optional[str],
    support_terms: List[str],
    support_mode: str = "none",
    support_slot_text: Optional[str] = None,
    support_classes: Optional[List[str]] = None,
    source_uri_values: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a deterministic raw SPARQL query for value-enumeration over evidence properties."""
    indexes = schema_indexes(schema)
    link_property = relation_info.get("property") if isinstance(relation_info, dict) else None
    try:
        link_info = resolve_builder_link_direction(source_class, evidence_class, link_property, indexes)
    except SystemExit:
        return None

    namespace = primary_namespace_from_schema(schema)
    domain_properties = data_properties_by_domain(schema)
    source_var = "source"
    evidence_var = "evidence"
    query_lines = [
        f"PREFIX ex: <{namespace}>",
    ]

    select_items = ["?valueLabel"]
    if isinstance(description_property, str) and description_property:
        select_items.append("(SAMPLE(?valueDescriptionRaw) AS ?valueDescription)")
    select_items.extend([
        "(COUNT(DISTINCT ?source) AS ?sourceCount)",
        "(COUNT(DISTINCT ?evidence) AS ?evidenceCount)",
    ])
    query_lines.append("SELECT " + " ".join(select_items))
    query_lines.append("WHERE {")
    query_lines.append(f"  ?{source_var} a ex:{source_class} .")
    query_lines.append(f"  ?{evidence_var} a ex:{evidence_class} .")
    if link_info["direction"] == "forward":
        query_lines.append(f"  ?{source_var} ex:{link_info['property']} ?{evidence_var} .")
    else:
        query_lines.append(f"  ?{evidence_var} ex:{link_info['property']} ?{source_var} .")
    query_lines.append(f"  ?{evidence_var} ex:{value_property} ?valueLabel .")
    if isinstance(description_property, str) and description_property:
        query_lines.append(f"  OPTIONAL {{ ?{evidence_var} ex:{description_property} ?valueDescriptionRaw . }}")
    query_lines.append("  FILTER(STRLEN(STR(?valueLabel)) > 0)")
    normalized_source_uri_values = unique_preserve_order(
        [str(value) for value in (source_uri_values or []) if isinstance(value, str) and value]
    )
    if len(normalized_source_uri_values) == 1:
        query_lines.append(
            f"  FILTER({compile_filter_expression({'var': source_var, 'op': 'equals', 'value': normalized_source_uri_values[0]})})"
        )
    elif normalized_source_uri_values:
        query_lines.append(
            f"  FILTER({compile_filter_expression({'var': source_var, 'op': 'in', 'values': normalized_source_uri_values})})"
        )

    normalized_support_terms = unique_preserve_order([term for term in support_terms if isinstance(term, str) and term])
    if support_mode == "same_evidence":
        if not normalized_support_terms:
            return None
        type_prop = best_role_property(evidence_class, "type", domain_properties)
        desc_prop = best_role_property(evidence_class, "description", domain_properties)
        support_vars: List[str] = []
        if isinstance(type_prop, str) and type_prop == value_property:
            support_vars.append("valueLabel")
        if isinstance(desc_prop, str) and desc_prop == value_property and "valueLabel" not in support_vars:
            support_vars.append("valueLabel")
        if (
            isinstance(description_property, str)
            and description_property
            and isinstance(type_prop, str)
            and description_property == type_prop
        ):
            support_vars.append("valueDescriptionRaw")
        if (
            isinstance(description_property, str)
            and description_property
            and isinstance(desc_prop, str)
            and description_property == desc_prop
            and "valueDescriptionRaw" not in support_vars
        ):
            support_vars.append("valueDescriptionRaw")
        support_filter = build_constraint_filter(support_vars, normalized_support_terms)
        if support_filter is None:
            return None
        query_lines.append(f"  FILTER({compile_filter_expression(support_filter)})")
    elif support_mode == "source_support":
        if not normalized_support_terms:
            return None
        support_candidates = [
            evidence_class,
            *(support_classes or []),
        ]
        action_exists_clauses = build_source_support_exists_clauses(
            namespace,
            source_var,
            source_class,
            normalized_support_terms,
            support_slot_text,
            support_candidates,
            indexes,
            domain_properties,
        )
        if not action_exists_clauses:
            return None
        query_lines.append("  FILTER(")
        for index, clause in enumerate(action_exists_clauses):
            indented = "\n".join(f"    {line}" for line in clause.splitlines())
            separator = " ||" if index < len(action_exists_clauses) - 1 else ""
            query_lines.append(f"{indented}{separator}")
        query_lines.append("  )")

    query_lines.append("}")
    query_lines.append("GROUP BY ?valueLabel")
    query_lines.append("ORDER BY DESC(?sourceCount) DESC(?evidenceCount) ?valueLabel")
    return {
        "query": "\n".join(query_lines),
        "source_var": source_var,
    }


def build_constraint_filter(var_names: List[str], terms: List[str]) -> Optional[Dict[str, Any]]:
    """Build a nested filter tree for matching one semantic constraint across several vars."""
    usable_terms = unique_preserve_order([term for term in terms if term])
    usable_vars = unique_preserve_order([name for name in var_names if name])
    if not usable_terms or not usable_vars:
        return None
    filters = [{"var": var_name, "op": "contains_any", "values": usable_terms} for var_name in usable_vars]
    if len(filters) == 1:
        return filters[0]
    return {"any_of": filters}


def build_semantic_query_planner(
    question: str,
    template: str,
    schema: Optional[Dict[str, Any]],
    base_url: Optional[str] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a lightweight semantic query plan suggestion from question + schema."""
    if template not in ("fact_lookup", "causal_lookup", "causal_enumeration", "enumeration"):
        return None

    manifest = with_semantic_vector_index(build_semantic_manifest(schema))
    slots = dict(slots_override) if isinstance(slots_override, dict) else extract_question_slots(question, template)
    effective_unit_intent_ir = unit_intent_ir
    if not isinstance(effective_unit_intent_ir, dict):
        synthetic_unit = {
            "unit_id": "q1",
            "text": question,
            "raw_text": question,
            "position": 1,
            "reference_markers": list(slots.get("reference_markers", []))
            if isinstance(slots.get("reference_markers"), list)
            else [],
            "dependency": None,
        }
        effective_unit_intent_ir = build_question_unit_intent_ir(synthetic_unit, slots, template)
    semantic_state = semantic_state_from_sources(slots, effective_unit_intent_ir)
    constraint_snapshot = semantic_state.get("constraint_snapshot", {})
    asks_explanation = bool(semantic_state.get("asks_explanation"))
    target_text = first_nonempty_text(
        semantic_state.get("target_text"),
        semantic_state.get("result_hint"),
    )
    routing = route_query_family(question, template, slots, unit_intent_ir=effective_unit_intent_ir)
    slots["question_type"] = routing["family"]
    slot_inputs = build_family_slot_inputs(question, slots, routing, unit_intent_ir=effective_unit_intent_ir)
    source_info = choose_source_class_candidate_with_anchors(question, manifest, slots)
    initial_slot_bindings = bind_semantic_slots(manifest, slot_inputs)
    source_info = merge_source_candidates_from_slot_bindings(source_info, initial_slot_bindings, manifest)
    provisional_source = source_info.get("selected")
    provisional_source_class = provisional_source.get("class_name") if isinstance(provisional_source, dict) else None
    provisional_evidence_candidates = (
        choose_evidence_class_candidates(provisional_source_class, manifest, slots)
        if isinstance(provisional_source_class, str) and provisional_source_class
        else []
    )
    value_class_names = rank_value_catalog_classes(
        source_info,
        provisional_evidence_candidates,
        initial_slot_bindings,
    )
    sampled_value_nodes = load_sample_value_nodes(base_url, manifest, value_class_names)
    manifest = with_semantic_vector_index(with_value_nodes(manifest, sampled_value_nodes))
    slot_bindings = bind_semantic_slots(manifest, slot_inputs)
    source_info = choose_source_class_candidate_with_anchors(question, manifest, slots)
    source_info = merge_source_candidates_from_slot_bindings(source_info, slot_bindings, manifest)
    request_ir = build_semantic_request_ir(
        question,
        template,
        slots,
        routing,
        slot_inputs,
        slot_bindings,
        source_info,
        [],
        unit_intent_ir=effective_unit_intent_ir,
    )
    source_selected = source_info.get("selected")
    if not source_selected:
        clarification_hint = None
        if (
            status_check_mode
            and not isinstance(status_numeric_constraint, dict)
            and (
                "status_or_problem_text" in missing_constraint_bindings
                or "status_or_problem_text" in non_lowerable_constraint_bindings
            )
        ):
            clarification_hint = build_explicit_metric_clarification_hint(semantic_state)
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": routing["effective_template"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": [],
            "evidence_candidates": [],
            "candidate_plans": [],
            "ready": False,
            "reason": "no_source_class_grounding",
        }

    effective_template = routing["effective_template"]
    preview_source_class = source_selected["class_name"]
    preview_evidence_candidates = choose_evidence_class_candidates(
        preview_source_class,
        manifest,
        slots,
        unit_intent_ir=effective_unit_intent_ir,
    )
    request_ir = build_semantic_request_ir(
        question,
        template,
        slots,
        routing,
        slot_inputs,
        slot_bindings,
        source_info,
        preview_evidence_candidates,
        unit_intent_ir=effective_unit_intent_ir,
    )
    domain_properties = manifest_attributes_by_class(manifest)
    indexes = schema_indexes(schema)
    class_labels = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    }

    missing_constraint_bindings: List[str] = []
    non_lowerable_constraint_bindings: List[str] = []
    clarification_hint = None
    status_slot_input = slot_input_for_name(slot_inputs, "status_or_problem_text")
    status_check_mode = (
        isinstance(status_slot_input, dict)
        and status_slot_input.get("constraint_mode") == "status_check"
    )
    status_numeric_constraint = semantic_state.get("status_numeric_constraint")
    misrouted_anchored_status_lookup = (
        bool(semantic_state.get("has_anchor"))
        and bool(semantic_state.get("status_check_requested"))
        and routing.get("family") in ("enumeration", "causal_enumeration", "explanation_enumeration")
    )
    if misrouted_anchored_status_lookup:
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": effective_template,
            "routing_rationale": routing["rationale"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": source_info.get("candidates", []),
            "evidence_candidates": preview_evidence_candidates,
            "candidate_plans": [],
            "selected_plan": None,
            "ready": False,
            "reason": "anchored_status_lookup_requires_smaller_family",
        }
    if routing.get("family", "").startswith("anchored_"):
        slot_requirements = [
            ("status_or_problem_text", semantic_state.get("status_or_problem_text")),
            ("cause_text", semantic_state.get("cause_text")),
            ("action_or_state_text", semantic_state.get("action_text")),
        ]
        for slot_name, slot_value in slot_requirements:
            if not isinstance(slot_value, str) or not slot_value.strip():
                continue
            if not slot_binding_has_candidates(slot_bindings, slot_name):
                missing_constraint_bindings.append(slot_name)
            elif (
                slot_name == "status_or_problem_text"
                and status_check_mode
                and not isinstance(status_numeric_constraint, dict)
                and not slot_candidates_have_text_lowering(slot_bindings, slot_name, status_slot_input)
            ):
                non_lowerable_constraint_bindings.append(slot_name)

    if missing_constraint_bindings or non_lowerable_constraint_bindings:
        if (
            status_check_mode
            and not isinstance(status_numeric_constraint, dict)
            and (
                "status_or_problem_text" in missing_constraint_bindings
                or "status_or_problem_text" in non_lowerable_constraint_bindings
            )
        ):
            clarification_hint = build_explicit_metric_clarification_hint(semantic_state)
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": effective_template,
            "routing_rationale": routing["rationale"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": source_info.get("candidates", []),
            "evidence_candidates": preview_evidence_candidates,
            "candidate_plans": [],
            "selected_plan": None,
            "ready": False,
            "reason": "constraint_grounding_not_executable",
            "missing_constraint_bindings": missing_constraint_bindings,
            "non_lowerable_constraint_bindings": non_lowerable_constraint_bindings,
            "clarification_hint": clarification_hint,
        }

    anchor_value = None
    anchors = semantic_state.get("anchors", [])
    if isinstance(anchors, list):
        for anchor in anchors:
            if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
                anchor_value = anchor.get("value")
                break
    reference_entity_uris = unique_preserve_order([
        str(item)
        for item in slots.get("reference_entity_uris", [])
        if isinstance(item, str) and item
    ])
    reference_entity_class = slots.get("reference_entity_class")

    candidate_plans = []
    all_evidence_candidates: List[Dict[str, Any]] = []
    seen_evidence_classes: Set[str] = set()
    source_candidates = [
        item
        for item in source_info.get("candidates", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    ][:5]

    for source_candidate in source_candidates:
        source_class = source_candidate["class_name"]
        if isinstance(reference_entity_class, str) and reference_entity_class and source_class != reference_entity_class:
            continue
        source_score = float(source_candidate.get("score", 0.0) or 0.0)
        source_id_prop = best_role_property(source_class, "id", domain_properties, class_labels)
        source_name_prop = best_role_property(source_class, "name", domain_properties, class_labels)
        evidence_candidates = choose_evidence_class_candidates(
            source_class,
            manifest,
            slots,
            unit_intent_ir=effective_unit_intent_ir,
        )
        for evidence in evidence_candidates:
            evidence_class_name = evidence.get("class_name")
            if isinstance(evidence_class_name, str) and evidence_class_name and evidence_class_name not in seen_evidence_classes:
                seen_evidence_classes.add(evidence_class_name)
                all_evidence_candidates.append(evidence)

        for evidence in evidence_candidates[:3]:
            evidence_class = evidence["class_name"]
            evidence_id_prop = best_role_property(evidence_class, "id", domain_properties, class_labels)
            evidence_type_prop = best_role_property(evidence_class, "type", domain_properties, class_labels)
            evidence_desc_prop = best_role_property(evidence_class, "description", domain_properties, class_labels)
            try:
                relation_info = resolve_builder_link_direction(source_class, evidence_class, None, indexes)
            except SystemExit:
                relation_info = evidence.get("relations", [None])[0] if isinstance(evidence.get("relations"), list) and evidence.get("relations") else None

            select_specs = []
            if source_id_prop:
                select_specs.append({"var": "sourceId", "subject": "source", "property": source_id_prop})
            if source_name_prop:
                select_specs.append({"var": "sourceName", "subject": "source", "property": source_name_prop})
            if evidence_id_prop:
                select_specs.append({"var": "evidenceId", "subject": "evidence", "property": evidence_id_prop})
            if evidence_type_prop:
                select_specs.append({"var": "evidenceType", "subject": "evidence", "property": evidence_type_prop})
            if evidence_desc_prop:
                select_specs.append({"var": "evidenceDescription", "subject": "evidence", "property": evidence_desc_prop})

            text_vars = []
            if evidence_type_prop:
                text_vars.append("evidenceType")
            if evidence_desc_prop:
                text_vars.append("evidenceDescription")

            status_terms = unique_preserve_order(
                expand_constraint_terms(
                    semantic_state.get("status_or_problem_text"),
                    schema,
                )
                + binding_terms_for_slot(slot_bindings, "status_or_problem_text")
            )
            cause_terms = unique_preserve_order(
                binding_terms_for_slot(slot_bindings, "cause_text", preferred_node_types=["value"])
                + expand_constraint_terms(
                    semantic_state.get("cause_text"),
                    schema,
                )
                + binding_terms_for_slot(slot_bindings, "cause_text")
            )
            if not cause_terms and not (status_check_mode and isinstance(status_numeric_constraint, dict)):
                cause_terms = list(status_terms)
            action_terms = unique_preserve_order(
                binding_terms_for_slot(slot_bindings, "action_or_state_text", preferred_node_types=["value"])
                + expand_constraint_terms(
                    semantic_state.get("action_text"),
                    schema,
                )
                + binding_terms_for_slot(slot_bindings, "action_or_state_text")
            )
            action_slot_input = slot_input_for_name(slot_inputs, "action_or_state_text")
            action_slot_text = (
                action_slot_input.get("text")
                if isinstance(action_slot_input, dict) and isinstance(action_slot_input.get("text"), str)
                else None
            )
            cause_filter = build_constraint_filter(text_vars, cause_terms)
            action_filter = build_constraint_filter(text_vars, action_terms)
            support_slot_requested = action_slot_input is not None
            support_evidence_classes = [
                item.get("class_name")
                for item in evidence_candidates
                if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
            ]

            if effective_template == "enumeration":
                explanation_family = routing.get("family") == "explanation_enumeration"
                generic_explanation_target = explanation_family and is_generic_explanation_target(
                    target_text
                )
                if support_slot_requested and action_terms:
                    support_evidence_classes = choose_action_support_classes(
                        evidence_candidates,
                        action_terms,
                        domain_properties,
                        slot_text=action_slot_text,
                        limit=2 if explanation_family else 5,
                    ) or support_evidence_classes
                value_projection = choose_enumeration_value_projection(
                    slot_bindings,
                    evidence_class,
                    domain_properties,
                    class_labels,
                    prefer_explanation=asks_explanation,
                    allow_generic_explanation_projection=generic_explanation_target,
                )
                if value_projection is None:
                    continue

                projection_confidence = 2.0 if "target_slot_grounded" in value_projection.get("rationale", []) else 1.0
                support_terms = list(action_terms)
                if support_slot_requested and not support_terms:
                    continue
                explanation_bonus = 2.0 if asks_explanation else 0.0
                reference_scope_applied = bool(reference_entity_uris)
                reference_rationale = ["reference_scope_bound"] if reference_scope_applied else []
                reference_bonus = 1.0 if reference_scope_applied else 0.0

                enumeration_variants: List[tuple[str, Optional[Dict[str, Any]], List[str], float, bool]] = []
                if support_terms:
                    weak_target_projection = "target_role_fallback" in value_projection.get("rationale", [])
                    broad_support_penalty = (
                        4.0 if asks_explanation and weak_target_projection else 0.0
                    )
                    same_evidence_confidence = (
                        evidence["score"] + source_score + projection_confidence + 2.0 + reference_bonus
                    )
                    source_support_confidence = (
                        evidence["score"] + source_score + projection_confidence + 1.0 + explanation_bonus + reference_bonus - broad_support_penalty
                    )
                    if explanation_family:
                        same_evidence_confidence -= 2.0
                        source_support_confidence += 3.0
                    enumeration_variants.append((
                        "value_enumeration_same_evidence",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            support_terms,
                            support_mode="same_evidence",
                            source_uri_values=reference_entity_uris,
                        ),
                        ["target_value_projected", "action_term_grounded"],
                        same_evidence_confidence,
                        False,
                    ))
                    enumeration_variants.append((
                        "value_enumeration_source_support",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            support_terms,
                            support_mode="source_support",
                            support_slot_text=action_slot_text,
                            support_classes=support_evidence_classes,
                            source_uri_values=reference_entity_uris,
                        ),
                        ["target_value_projected", "action_term_grounded_separate_evidence"],
                        source_support_confidence,
                        True,
                    ))
                else:
                    enumeration_variants.append((
                        "value_enumeration",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            [],
                            support_mode="none",
                            source_uri_values=reference_entity_uris,
                        ),
                        ["target_value_projected"],
                        evidence["score"] + source_score + projection_confidence + reference_bonus,
                        False,
                    ))

                for variant_name, enumeration_query, rationale, confidence, separate_support in enumeration_variants:
                    if enumeration_query is None:
                        continue
                    candidate_plans.append({
                        "variant": variant_name,
                        "confidence_score": confidence,
                        "rationale": reference_rationale + rationale + value_projection.get("rationale", []),
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=bool(support_terms),
                            include_status=False,
                            separate_action_support=separate_support,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": enumeration_query,
                        },
                    })
                continue

            base_builder = {
                "source_class": source_class,
                "source_var": "source",
                "evidence_class": evidence_class,
                "evidence_var": "evidence",
                "distinct": True,
            }
            base_order_by = ["sourceId"] if source_id_prop else []
            base_filters = []
            reference_scope_applied = False
            if reference_entity_uris:
                reference_scope_applied = True
                if len(reference_entity_uris) == 1:
                    base_filters.append({
                        "var": "source",
                        "op": "equals",
                        "value": reference_entity_uris[0],
                    })
                else:
                    base_filters.append({
                        "var": "source",
                        "op": "in",
                        "values": reference_entity_uris,
                    })
            reference_rationale = ["reference_scope_bound"] if reference_scope_applied else []
            reference_bonus = 1.0 if reference_scope_applied else 0.0
            anchor_binding_candidates = []
            if isinstance(anchor_value, str) and anchor_value:
                source_anchor_binding = selected_anchor_binding_for_class(slot_bindings, source_class)
                if isinstance(source_anchor_binding, dict):
                    anchor_binding_candidates.append({
                        "subject": "source",
                        "var": "anchorMatch",
                        "binding": source_anchor_binding,
                    })
                evidence_anchor_binding = selected_anchor_binding_for_class(slot_bindings, evidence_class)
                if isinstance(evidence_anchor_binding, dict):
                    anchor_binding_candidates.append({
                        "subject": "evidence",
                        "var": "anchorMatch",
                        "binding": evidence_anchor_binding,
                    })
            best_anchor_candidate = None
            if anchor_binding_candidates:
                anchor_binding_candidates.sort(
                    key=lambda item: (
                        -float(item["binding"].get("total_score", 0.0) or 0.0),
                        item["subject"],
                        str(item["binding"].get("local_name", "")),
                    )
                )
                best_anchor_candidate = anchor_binding_candidates[0]
                if not any(item.get("var") == "anchorMatch" for item in select_specs):
                    select_specs.append({
                        "var": "anchorMatch",
                        "subject": best_anchor_candidate["subject"],
                        "property": best_anchor_candidate["binding"]["local_name"],
                    })
                base_filters.append({
                    "var": "anchorMatch",
                    "op": "equals",
                    "value": anchor_value,
                })

            if effective_template == "fact_lookup":
                target_attribute_binding = top_attribute_candidate_for_slot(
                    slot_bindings,
                    "target_text",
                    class_name=evidence_class,
                    numeric_only=False,
                )
                if isinstance(target_attribute_binding, dict):
                    fact_select_specs = list(select_specs)
                    if not any(item.get("var") == "evidenceTargetValue" for item in fact_select_specs):
                        fact_select_specs.append({
                            "var": "evidenceTargetValue",
                            "subject": "evidence",
                            "property": target_attribute_binding["local_name"],
                        })
                    rationale = ["target_slot_grounded", "fact_target_projected"]
                    confidence = (
                        evidence["score"]
                        + source_score
                        + float(target_attribute_binding.get("total_score", 0.0) or 0.0)
                        + reference_bonus
                    )
                    if best_anchor_candidate is not None:
                        rationale.append("anchor_bound")
                        confidence += 2.0
                    candidate_plans.append({
                        "variant": "anchored_fact_target_projection",
                        "confidence_score": confidence,
                        "rationale": reference_rationale + rationale,
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=False,
                            include_status=False,
                            separate_action_support=False,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": {
                                "source_var": "source",
                                "builder": {
                                    **base_builder,
                                    "select": mark_optional_display_selects(
                                        fact_select_specs,
                                        base_filters,
                                        base_order_by,
                                    ),
                                    "filters": base_filters,
                                    "order_by": base_order_by,
                                },
                            },
                        },
                    })
                continue

            status_metric_binding = None
            if status_check_mode and isinstance(status_numeric_constraint, dict):
                status_metric_binding = top_attribute_candidate_for_slot(
                    slot_bindings,
                    "status_or_problem_text",
                    class_name=evidence_class,
                    numeric_only=True,
                )
                if isinstance(status_metric_binding, dict):
                    numeric_select_specs = list(select_specs)
                    if not any(item.get("var") == "statusMetric" for item in numeric_select_specs):
                        numeric_select_specs.append({
                            "var": "statusMetric",
                            "subject": "evidence",
                            "property": status_metric_binding["local_name"],
                        })
                    numeric_filters = list(base_filters) + [{
                        "var": "statusMetric",
                        "op": status_numeric_constraint["op"],
                        "value": status_numeric_constraint["value"],
                    }]
                    rationale = ["status_constraint_grounded", "numeric_constraint_lowered"]
                    confidence = (
                        evidence["score"]
                        + source_score
                        + float(status_metric_binding.get("total_score", 0.0) or 0.0)
                        + 2
                        + reference_bonus
                    )
                    if best_anchor_candidate is not None:
                        rationale.append("anchor_bound")
                        confidence += 2
                    candidate_plans.append({
                        "variant": "status_check_numeric",
                        "confidence_score": confidence,
                        "rationale": reference_rationale + rationale,
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=False,
                            include_status=True,
                            separate_action_support=False,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": {
                                "source_var": "source",
                                "builder": {
                                    **base_builder,
                                    "select": mark_optional_display_selects(
                                        numeric_select_specs,
                                        numeric_filters,
                                        base_order_by,
                                    ),
                                    "filters": numeric_filters,
                                    "order_by": base_order_by,
                                },
                            },
                            "analysis": {
                                "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                                "payload": {
                                    "mode": "paths",
                                    "profile": "causal",
                                    "max_depth": 3,
                                },
                            },
                        },
                    })

            if cause_filter is not None:
                strict_filters = list(base_filters) + [cause_filter]
                rationale = ["cause_term_grounded"]
                confidence = evidence["score"] + source_score + 3 + reference_bonus
                if best_anchor_candidate is not None:
                    rationale.append("anchor_bound")
                    confidence += 2
                variant = "cause_only"
                if action_filter is not None:
                    strict_filters.append(action_filter)
                    rationale.append("action_term_grounded")
                    confidence += 3
                    variant = "same_evidence_strict"
                candidate_plans.append({
                    "variant": variant,
                    "confidence_score": confidence,
                    "rationale": reference_rationale + rationale,
                    "query_family": routing["family"],
                    "source_class": source_class,
                    "evidence_class": evidence_class,
                    "node_plan": build_node_plan(
                        request_ir,
                        source_class,
                        evidence_class,
                        relation_info,
                        include_cause=True,
                        include_action=action_filter is not None,
                        separate_action_support=False,
                    ),
                    "plan": {
                        "template": effective_template,
                        "sparql": {
                            "source_var": "source",
                            "builder": {
                                **base_builder,
                                "select": mark_optional_display_selects(
                                    select_specs,
                                    strict_filters,
                                    base_order_by,
                                ),
                                "filters": strict_filters,
                                "order_by": base_order_by,
                            },
                        },
                        "analysis": {
                            "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                            "payload": {
                                "mode": "paths",
                                "profile": "causal",
                                "max_depth": 3,
                            },
                        },
                    },
                })

            if cause_filter is not None and action_filter is not None:
                multi_evidence_query = build_multi_evidence_relaxed_query(
                    schema,
                    source_class,
                    evidence_class,
                    action_terms,
                    cause_terms,
                    source_id_prop,
                    source_name_prop,
                    evidence_id_prop,
                    evidence_type_prop,
                    evidence_desc_prop,
                    source_uri_values=reference_entity_uris,
                )
                if multi_evidence_query is not None and best_anchor_candidate is None:
                    candidate_plans.append({
                        "variant": "source_support_relaxed",
                        "confidence_score": evidence["score"] + source_score + 2 + reference_bonus,
                        "rationale": reference_rationale + ["cause_term_grounded", "action_term_grounded_separate_evidence"],
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=True,
                            include_action=True,
                            separate_action_support=True,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": multi_evidence_query,
                            "analysis": {
                                "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                                "payload": {
                                    "mode": "paths",
                                    "profile": "causal",
                                    "max_depth": 3,
                                },
                            },
                        },
                    })

                candidate_plans.append({
                    "variant": "cause_only_relaxed",
                    "confidence_score": evidence["score"] + source_score + reference_bonus + (2 if best_anchor_candidate is not None else 0),
                    "rationale": reference_rationale + ["cause_term_grounded", "action_term_relaxed"] + (["anchor_bound"] if best_anchor_candidate is not None else []),
                    "query_family": routing["family"],
                    "source_class": source_class,
                    "evidence_class": evidence_class,
                    "node_plan": build_node_plan(
                        request_ir,
                        source_class,
                        evidence_class,
                        relation_info,
                        include_cause=True,
                        include_action=False,
                        separate_action_support=False,
                    ),
                    "plan": {
                        "template": effective_template,
                        "sparql": {
                            "source_var": "source",
                            "builder": {
                                **base_builder,
                                "select": mark_optional_display_selects(
                                    select_specs,
                                    list(base_filters) + [cause_filter],
                                    base_order_by,
                                ),
                                "filters": list(base_filters) + [cause_filter],
                                "order_by": base_order_by,
                            },
                        },
                        "analysis": {
                            "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                            "payload": {
                                "mode": "paths",
                                "profile": "causal",
                                "max_depth": 3,
                            },
                        },
                    },
                })

    candidate_plans.sort(key=lambda item: (-item["confidence_score"], item["variant"]))
    underconstrained_target_projection = (
        effective_template == "enumeration"
        and bool(semantic_state.get("asks_explanation"))
        and isinstance(semantic_state.get("target_text"), str)
        and bool(semantic_state.get("target_text").strip())
        and bool(candidate_plans)
        and all("target_role_fallback" in item.get("rationale", []) for item in candidate_plans)
    )
    selected_plan = None if underconstrained_target_projection else (candidate_plans[0]["plan"] if candidate_plans else None)
    ready = selected_plan is not None
    evidence_candidates = all_evidence_candidates or preview_evidence_candidates
    if candidate_plans:
        selected_source_class = candidate_plans[0].get("source_class")
        selected_source = next(
            (
                item for item in source_info.get("candidates", [])
                if isinstance(item, dict) and item.get("class_name") == selected_source_class
            ),
            source_selected,
        )
        selected_source_info = {
            "selected": selected_source,
            "candidates": source_info.get("candidates", []),
        }
        evidence_candidates = choose_evidence_class_candidates(selected_source_class, manifest, slots)
        request_ir = build_semantic_request_ir(
            question,
            template,
            slots,
            routing,
            slot_inputs,
            slot_bindings,
            selected_source_info,
            evidence_candidates,
            unit_intent_ir=effective_unit_intent_ir,
        )

    return {
        "mode": "semantic_query_planner",
        "slots": slots,
        "request_ir": request_ir,
        "query_family": routing["family"],
        "requested_template": template,
        "effective_template": effective_template,
        "routing_rationale": routing["rationale"],
        "semantic_manifest_summary": {
            "class_count": len(manifest.get("classes", [])),
            "relation_count": len(manifest.get("relations", [])),
            "class_node_count": len(manifest.get("class_nodes", [])),
            "attribute_node_count": len(manifest.get("attribute_nodes", [])),
            "relation_node_count": len(manifest.get("relation_nodes", [])),
            "value_node_count": len(manifest.get("value_nodes", [])),
        },
        "source_candidates": source_info.get("candidates", []),
        "evidence_candidates": evidence_candidates,
        "candidate_plans": candidate_plans,
        "selected_plan": selected_plan,
        "ready": ready,
        "reason": None if ready else (
            "target_projection_underconstrained"
            if underconstrained_target_projection
            else "no_executable_candidate_plan"
        ),
    }


def summarize_schema(schema: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a compact summary of /schema output for run responses."""
    if not isinstance(schema, dict):
        return None

    classes = schema.get("classes")
    data_properties = schema.get("data_properties")
    object_properties = schema.get("object_properties")
    class_hierarchy = schema.get("class_hierarchy")

    class_names = []
    if isinstance(classes, list):
        class_names = [
            item.get("local_name")
            for item in classes
            if isinstance(item, dict) and item.get("local_name")
        ]

    return {
        "class_count": len(classes) if isinstance(classes, list) else 0,
        "data_property_count": len(data_properties) if isinstance(data_properties, list) else 0,
        "object_property_count": len(object_properties) if isinstance(object_properties, list) else 0,
        "class_hierarchy_count": len(class_hierarchy) if isinstance(class_hierarchy, dict) else 0,
        "class_names_sample": class_names[:12],
        "truncated": len(class_names) > 12,
    }


def summarize_profiles(profiles: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a compact summary of /analysis/profiles output for run responses."""
    if not isinstance(profiles, dict):
        return None

    items = profiles.get("profiles")
    if not isinstance(items, list):
        return None

    names = [
        item.get("name")
        for item in items
        if isinstance(item, dict) and item.get("name")
    ]
    return {
        "profile_count": len(items),
        "profile_names": names,
    }


def summarize_analysis_response(analysis: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a compact summary of analyzer output for run responses."""
    if not isinstance(analysis, dict):
        return None

    summary: Dict[str, Any] = {
        "mode": analysis.get("mode"),
        "profile": analysis.get("profile"),
        "truncated": bool(analysis.get("truncated")),
    }

    if isinstance(analysis.get("source"), str):
        summary["source"] = analysis.get("source")
    if isinstance(analysis.get("target"), str) or analysis.get("target") is None:
        summary["target"] = analysis.get("target")
    if isinstance(analysis.get("path_count"), int):
        summary["path_count"] = analysis.get("path_count")
    if isinstance(analysis.get("source_count"), int):
        summary["source_count"] = analysis.get("source_count")
    if isinstance(analysis.get("matched_source_count"), int):
        summary["matched_source_count"] = analysis.get("matched_source_count")
    if isinstance(analysis.get("total_path_count"), int):
        summary["total_path_count"] = analysis.get("total_path_count")

    results = analysis.get("results")
    if isinstance(results, list):
        summary["result_count"] = len(results)
        result_items = []
        for item in results[:5]:
            if not isinstance(item, dict):
                continue
            compact_item = {
                "source": item.get("source"),
                "target": item.get("target"),
                "path_count": item.get("path_count"),
                "truncated": bool(item.get("truncated")),
            }
            result_items.append(compact_item)
        if result_items:
            summary["results_summary"] = result_items
        summary["results_truncated"] = len(results) > len(result_items)

    return summary


def summarize_planner_result(planner: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a compact summary of semantic planner output."""
    if not isinstance(planner, dict):
        return None

    summary: Dict[str, Any] = {
        "mode": planner.get("mode"),
        "ready": bool(planner.get("ready")),
    }
    if planner.get("query_family"):
        summary["query_family"] = planner.get("query_family")
    if planner.get("requested_template"):
        summary["requested_template"] = planner.get("requested_template")
    if planner.get("effective_template"):
        summary["effective_template"] = planner.get("effective_template")
    if isinstance(planner.get("semantic_manifest_summary"), dict):
        summary["semantic_manifest_summary"] = planner.get("semantic_manifest_summary")

    slots = planner.get("slots")
    if isinstance(slots, dict):
        summary["slots"] = slots

    request_ir = planner.get("request_ir")
    if isinstance(request_ir, dict):
        summary["request_ir"] = {
            "query_family": request_ir.get("query_family"),
            "anchors": request_ir.get("anchors"),
            "constraints": request_ir.get("constraints"),
            "output": request_ir.get("output"),
        }

    candidate_plans = planner.get("candidate_plans")
    if isinstance(candidate_plans, list):
        summary["candidate_count"] = len(candidate_plans)
        if candidate_plans and isinstance(candidate_plans[0], dict):
            selected_candidate = candidate_plans[0]
            summary["selected_variant"] = selected_candidate.get("variant")
            summary["selected_confidence_score"] = selected_candidate.get("confidence_score")
            if selected_candidate.get("query_family"):
                summary["selected_query_family"] = selected_candidate.get("query_family")
            summary["source_class"] = selected_candidate.get("source_class")
            summary["evidence_class"] = selected_candidate.get("evidence_class")
            rationale = selected_candidate.get("rationale")
            if isinstance(rationale, list):
                summary["rationale"] = rationale
            if isinstance(selected_candidate.get("node_plan"), dict):
                summary["selected_node_plan"] = selected_candidate.get("node_plan")

    return summary


def sparql_row_count(sparql_response: Optional[Dict[str, Any]]) -> int:
    """Return the row count from a SPARQL response when available."""
    if not isinstance(sparql_response, dict):
        return 0

    count = sparql_response.get("count")
    if isinstance(count, int):
        return count

    rows = sparql_response.get("results")
    if isinstance(rows, list):
        return len(rows)
    return 0


def uri_local_name(value: Any) -> Optional[str]:
    """Return the local name fragment for a URI-like value."""
    if not is_uri_like(value):
        return None
    if "#" in value:
        return value.rsplit("#", 1)[-1]
    return value.rstrip("/").rsplit("/", 1)[-1]


def schema_class_label_map(schema: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Build a local_name -> human label map from schema classes."""
    if not isinstance(schema, dict):
        return {}

    labels: Dict[str, str] = {}
    classes = schema.get("classes")
    if not isinstance(classes, list):
        return labels

    for item in classes:
        if not isinstance(item, dict):
            continue
        local_name = item.get("local_name")
        label = item.get("label") or local_name
        if local_name:
            labels[local_name] = label
    return labels


def class_key_from_uri(value: Any) -> Optional[str]:
    """Infer the class-like prefix from a resource URI local name."""
    local_name = uri_local_name(value)
    if not local_name:
        return None
    if "_" in local_name:
        return local_name.split("_", 1)[0]
    return local_name


def class_label_for_uri(value: Any, class_labels: Dict[str, str]) -> Optional[str]:
    """Resolve a human-friendly class label for a URI-like value."""
    class_key = class_key_from_uri(value)
    if not class_key:
        return None
    return class_labels.get(class_key, class_key)


def is_missing_literal_value(value: Any) -> bool:
    """Whether a literal value should be treated as absent for presentation."""
    if value in (None, ""):
        return True
    if isinstance(value, str) and normalize_match_text(value) in {"none", "null", "nan"}:
        return True
    return False


def pick_first_matching_value(fields: Dict[str, Any], suffixes: List[str]) -> Optional[str]:
    """Pick the first non-empty string value whose key matches one of the suffixes."""
    lowered_suffixes = [suffix.lower() for suffix in suffixes]
    for key, value in fields.items():
        if is_missing_literal_value(value) or not isinstance(value, (str, int, float, bool)):
            continue
        key_lower = key.lower()
        if any(key_lower.endswith(suffix) for suffix in lowered_suffixes):
            return str(value)

    for key, value in fields.items():
        if is_missing_literal_value(value) or not isinstance(value, (str, int, float, bool)):
            continue
        key_lower = key.lower()
        if any(suffix in key_lower for suffix in lowered_suffixes):
            return str(value)
    return None


def prefixed_literal_fields(row: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    """Collect literal fields whose variable names share a common prefix."""
    prefix_lower = prefix.lower()
    results: Dict[str, Any] = {}
    for key, value in row.items():
        if key == prefix or is_missing_literal_value(value) or is_uri_like(value):
            continue
        if key.lower().startswith(prefix_lower):
            results[key] = value
    return results


def merged_prefixed_literal_fields(rows: List[Dict[str, Any]], prefix: str) -> Dict[str, Any]:
    """Merge prefixed literal fields across rows, keeping the first non-empty value."""
    merged: Dict[str, Any] = {}
    for row in rows:
        for key, value in prefixed_literal_fields(row, prefix).items():
            if key not in merged and not is_missing_literal_value(value):
                merged[key] = value
    return merged


def is_metric_like_field(field_name: str, value: Any) -> bool:
    """Whether a field/value pair should be surfaced as a metric-like presentation item."""
    if is_missing_literal_value(value):
        return False
    field_lower = field_name.lower()
    return (
        isinstance(value, (int, float))
        or "score" in field_lower
        or "metric" in field_lower
        or "rate" in field_lower
        or "status" in field_lower
        or "complaint" in field_lower
        or "满意度" in field_name
        or "评分" in field_name
        or "投诉" in field_name
    )


def extract_row_metric_fields(
    rows: List[Dict[str, Any]],
    source_var: str,
    evidence_var: Optional[str],
) -> Dict[str, Any]:
    """Extract metric-like fields that are not part of source/evidence prefixed display columns."""
    metric_fields: Dict[str, Any] = {}
    evidence_prefix = evidence_var.lower() if isinstance(evidence_var, str) and evidence_var else None
    source_prefix = source_var.lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if key in {source_var, evidence_var}:
                continue
            key_lower = key.lower()
            if key_lower.startswith(source_prefix):
                continue
            if evidence_prefix and key_lower.startswith(evidence_prefix):
                continue
            if key_lower.startswith("anchor"):
                continue
            if not is_metric_like_field(key, value):
                continue
            if key not in metric_fields:
                metric_fields[key] = value
    return metric_fields


def metric_items_from_fields(fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize metric-like fields into a stable list for presentation."""
    items: List[Dict[str, Any]] = []
    for key, value in fields.items():
        if not is_metric_like_field(key, value):
            continue
        items.append({
            "field": key,
            "value": value,
        })
    items.sort(key=lambda item: item["field"])
    return items


def choose_source_var(
    sparql_response: Optional[Dict[str, Any]],
    sparql_spec: Optional[Dict[str, Any]],
    analysis_meta: Optional[Dict[str, Any]],
) -> Optional[str]:
    """Choose the primary source URI variable for presentation grouping."""
    if isinstance(analysis_meta, dict):
        source_var = analysis_meta.get("auto_derived_source_var")
        if isinstance(source_var, str) and source_var:
            return source_var

    if isinstance(sparql_spec, dict):
        source_var = sparql_spec.get("source_var")
        if isinstance(source_var, str) and source_var:
            return source_var

    derived = derive_uri_sources_from_sparql(sparql_response, multiple=True)
    return derived.get("source_var")


def choose_evidence_var(rows: List[Dict[str, Any]], source_var: str) -> Optional[str]:
    """Choose the most likely evidence anchor URI column from SPARQL rows."""
    if not rows:
        return None

    first_row = rows[0]
    uri_vars = [key for key, value in first_row.items() if key != source_var and is_uri_like(value)]
    if not uri_vars:
        return None

    priorities = ("event", "evidence", "issue", "problem", "complaint", "target")
    for priority in priorities:
        for key in uri_vars:
            if priority in key.lower():
                return key
    return uri_vars[0]


def build_entity_display(
    source_var: str,
    source_uri: str,
    rows: List[Dict[str, Any]],
    class_labels: Dict[str, str],
) -> Dict[str, Any]:
    """Build a human-oriented entity summary for a grouped source URI."""
    literal_fields = merged_prefixed_literal_fields(rows, source_var)
    display_name = pick_first_matching_value(
        literal_fields,
        ["name", "label", "title", "姓名", "名称", "名字"],
    )
    display_id = pick_first_matching_value(
        literal_fields,
        ["id", "code", "编号", "客户id", "客户_id"],
    )

    local_name = uri_local_name(source_uri)
    return {
        "display_name": display_name or display_id or local_name,
        "display_id": display_id,
        "type_label": class_label_for_uri(source_uri, class_labels),
        "uri": source_uri,
        "local_name": local_name,
        "display_fields": literal_fields,
    }


def build_evidence_items(
    rows: List[Dict[str, Any]],
    source_var: str,
    evidence_var: Optional[str],
    class_labels: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Build de-duplicated evidence items from grouped SPARQL rows."""
    items: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    for row in rows:
        evidence_uri = row.get(evidence_var) if evidence_var else None
        literal_fields = prefixed_literal_fields(row, evidence_var) if evidence_var else {}

        display_label = pick_first_matching_value(
            literal_fields,
            ["type", "label", "name", "title", "kind", "category", "类型", "名称"],
        )
        display_description = pick_first_matching_value(
            literal_fields,
            ["description", "desc", "summary", "reason", "problem", "remark", "描述", "说明"],
        )
        display_id = pick_first_matching_value(
            literal_fields,
            ["id", "code", "编号"],
        )

        if display_label is None and evidence_uri is not None:
            display_label = class_label_for_uri(evidence_uri, class_labels) or uri_local_name(evidence_uri)

        dedupe_key = json.dumps(
            {
                "evidence_uri": evidence_uri,
                "display_id": display_id,
                "display_label": display_label,
                "display_description": display_description,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        refs: Dict[str, Any] = {}
        if evidence_uri is not None:
            refs["uri"] = evidence_uri
            refs["local_name"] = uri_local_name(evidence_uri)
        if display_id is not None:
            refs["id"] = display_id

        items.append({
            "display_label": display_label,
            "display_description": display_description,
            "display_id": display_id,
            "type_label": class_label_for_uri(evidence_uri, class_labels) if evidence_uri is not None else None,
            "display_fields": literal_fields,
            "refs": refs,
        })

    return items


def summarize_uri_groups(uris: Set[str], class_labels: Dict[str, str]) -> List[Dict[str, Any]]:
    """Summarize URI sets by inferred class label for display."""
    by_type: Dict[str, Dict[str, Any]] = {}
    for uri in sorted(uris):
        type_key = class_key_from_uri(uri) or "resource"
        bucket = by_type.setdefault(
            type_key,
            {
                "type_key": type_key,
                "type_label": class_labels.get(type_key, type_key),
                "count": 0,
                "refs": [],
            },
        )
        bucket["count"] += 1
        bucket["refs"].append({
            "uri": uri,
            "local_name": uri_local_name(uri),
        })

    return sorted(
        by_type.values(),
        key=lambda item: (-item["count"], item["type_label"] or ""),
    )


def build_reasoning_summary(
    source_uri: str,
    analysis_result: Optional[Dict[str, Any]],
    evidence_items: List[Dict[str, Any]],
    class_labels: Dict[str, str],
) -> Dict[str, Any]:
    """Compress raw analyzer paths into a human-oriented structural summary."""
    summary: Dict[str, Any] = {
        "available": False,
        "path_count": 0,
        "truncated": False,
        "direct_relation_count": len(evidence_items),
        "direct_summary": [],
        "mediator_summary": [],
        "terminal_summary": [],
    }

    direct_uris = {
        refs["uri"]
        for item in evidence_items
        for refs in [item.get("refs", {})]
        if isinstance(refs, dict) and is_uri_like(refs.get("uri"))
    }
    if direct_uris:
        summary["direct_summary"] = summarize_uri_groups(direct_uris, class_labels)

    if not isinstance(analysis_result, dict):
        summary["trace_refs"] = {
            "source_uri": source_uri,
            "direct_uris": sorted(direct_uris),
            "mediator_uris": [],
            "terminal_uris": [],
        }
        return summary

    paths = analysis_result.get("paths")
    if not isinstance(paths, list):
        summary["trace_refs"] = {
            "source_uri": source_uri,
            "direct_uris": sorted(direct_uris),
            "mediator_uris": [],
            "terminal_uris": [],
        }
        return summary

    mediator_uris: Set[str] = set()
    terminal_uris: Set[str] = set()

    for path in paths:
        if not isinstance(path, list) or not path:
            continue
        objects = [step.get("object") for step in path if isinstance(step, dict) and is_uri_like(step.get("object"))]
        if not objects:
            continue

        terminal_uri = objects[-1]
        if terminal_uri not in direct_uris and terminal_uri != source_uri:
            terminal_uris.add(terminal_uri)

        for uri in objects[1:-1]:
            if uri not in direct_uris and uri != source_uri:
                mediator_uris.add(uri)

    direct_type_keys = {class_key_from_uri(uri) for uri in direct_uris if class_key_from_uri(uri)}
    terminal_uris = {
        uri
        for uri in terminal_uris
        if uri not in mediator_uris and class_key_from_uri(uri) not in direct_type_keys
    }

    summary.update({
        "available": True,
        "path_count": int(analysis_result.get("path_count", 0)) if isinstance(analysis_result.get("path_count"), int) else 0,
        "truncated": bool(analysis_result.get("truncated")),
        "mediator_summary": summarize_uri_groups(mediator_uris, class_labels),
        "terminal_summary": summarize_uri_groups(terminal_uris, class_labels),
        "trace_refs": {
            "source_uri": source_uri,
            "direct_uris": sorted(direct_uris),
            "mediator_uris": sorted(mediator_uris),
            "terminal_uris": sorted(terminal_uris),
        },
    })
    return summary


def extract_fact_metric_items(facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract metric-like literal fields from causal lookup facts for stable answer rendering."""
    if not facts:
        return []

    first_fact = facts[0] if isinstance(facts[0], dict) else {}
    display_fields = first_fact.get("display_fields", {})
    if not isinstance(display_fields, dict):
        return []

    metric_items = []
    for key, value in display_fields.items():
        if key in {"evidenceId", "evidencePhone"}:
            continue
        if is_metric_like_field(key, value):
            metric_items.append({
                "field": key,
                "value": value,
            })
    return metric_items


def build_causal_enumeration_presentation(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Optional[Dict[str, Any]],
    sparql_response: Optional[Dict[str, Any]],
    analysis_response: Optional[Dict[str, Any]],
    analysis_meta: Optional[Dict[str, Any]],
    status: str,
    analysis_error: Optional[Dict[str, Any]],
    analysis_skipped: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build a structured display model for causal enumeration answers."""
    if not isinstance(sparql_response, dict):
        return None

    rows = sparql_response.get("results")
    if not isinstance(rows, list):
        return None

    class_labels = schema_class_label_map(schema)
    source_var = choose_source_var(sparql_response, sparql_spec, analysis_meta)

    grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
    if source_var:
        for row in rows:
            if not isinstance(row, dict):
                continue
            source_uri = row.get(source_var)
            if is_uri_like(source_uri):
                grouped_rows.setdefault(source_uri, []).append(row)

    analysis_results_by_source: Dict[str, Dict[str, Any]] = {}
    if isinstance(analysis_response, dict):
        if isinstance(analysis_response.get("results"), list):
            for item in analysis_response["results"]:
                if isinstance(item, dict) and is_uri_like(item.get("source")):
                    analysis_results_by_source[item["source"]] = item
        elif is_uri_like(analysis_response.get("source")):
            analysis_results_by_source[analysis_response["source"]] = analysis_response

    groups = []
    distinct_evidence_refs: Set[str] = set()
    for source_uri, source_rows in grouped_rows.items():
        evidence_var = choose_evidence_var(source_rows, source_var)
        evidence_items = build_evidence_items(source_rows, source_var, evidence_var, class_labels)
        for item in evidence_items:
            refs = item.get("refs", {})
            if isinstance(refs, dict) and is_uri_like(refs.get("uri")):
                distinct_evidence_refs.add(refs["uri"])

        raw_reasoning_summary = build_reasoning_summary(
            source_uri,
            analysis_results_by_source.get(source_uri),
            evidence_items,
            class_labels,
        )
        reasoning_summary = {
            key: value
            for key, value in raw_reasoning_summary.items()
            if key != "trace_refs"
        }

        trace_refs = dict(raw_reasoning_summary.get("trace_refs", {}))
        trace_refs.update({
            "source_var": source_var,
            "evidence_var": evidence_var,
            "evidence_ids": [
                item["refs"]["id"]
                for item in evidence_items
                if isinstance(item.get("refs"), dict) and item["refs"].get("id")
            ],
        })

        groups.append({
            "entity": build_entity_display(source_var, source_uri, source_rows, class_labels),
            "evidence": evidence_items,
            "metric_fields": extract_row_metric_fields(source_rows, source_var, evidence_var),
            "reasoning_summary": reasoning_summary,
            "trace_refs": trace_refs,
        })

    groups.sort(key=lambda item: (
        item["entity"].get("display_id") or "",
        item["entity"].get("display_name") or "",
    ))

    presentation: Dict[str, Any] = {
        "template": "causal_enumeration",
        "summary": {
            "entity_count": len(groups),
            "record_count": sparql_row_count(sparql_response),
            "evidence_count": len(distinct_evidence_refs) if distinct_evidence_refs else sum(len(group["evidence"]) for group in groups),
        },
        "groups": groups,
        "analysis_status": {
            "status": status,
            "available": bool(analysis_response),
        },
    }

    entity_table_rows = []
    entity_detail_sections = []
    for group in groups:
        entity = group.get("entity", {})
        evidence = group.get("evidence", [])
        entity_table_rows.append({
            "entity_id": entity.get("display_id"),
            "entity_name": entity.get("display_name"),
            "evidence_count": len(evidence),
        })
        entity_detail_sections.append({
            "entity_id": entity.get("display_id"),
            "entity_name": entity.get("display_name"),
            "records": [
                {
                    "evidence_id": item.get("display_id"),
                    "evidence_label": item.get("display_label"),
                    "evidence_description": item.get("display_description"),
                }
                for item in evidence
            ],
        })

    presentation["answer_contract"] = {
        "version": "causal_enumeration_stable_v1",
        "preferred_section_order": [
            "summary",
            "entity_table",
            "entity_details",
            "analysis_note",
        ],
        "count_contract": {
            "primary_count_field": "entity_count",
            "primary_count_label": "客户数",
            "primary_unit": "位客户",
            "secondary_count_field": "record_count",
            "secondary_count_label": "记录数",
            "secondary_unit": "条记录",
            "question_focus": "entity",
            "report_primary_count_first": True,
            "never_treat_record_count_as_entity_count": True,
        },
        "summary": dict(presentation["summary"]),
        "entity_table": {
            "columns": ["entity_id", "entity_name", "evidence_count"],
            "rows": entity_table_rows,
        },
        "entity_details": entity_detail_sections,
        "analysis_note": {
            "available": bool(analysis_response),
            "status": status,
            "matched_entity_count": len(groups),
            "mention_path_count_default": False,
            "mention_raw_paths_default": False,
            "brief_only": True,
        },
    }

    if isinstance(analysis_response, dict):
        presentation["analysis_status"].update({
            "source_count": analysis_response.get("source_count"),
            "matched_source_count": analysis_response.get("matched_source_count"),
            "total_path_count": analysis_response.get("total_path_count"),
            "truncated": analysis_response.get("truncated"),
        })

    if analysis_error is not None:
        presentation["analysis_status"]["error"] = analysis_error
    if analysis_skipped is not None:
        presentation["analysis_status"]["skipped"] = analysis_skipped
    if isinstance(analysis_response, dict):
        matched = analysis_response.get("matched_source_count")
        if isinstance(matched, int):
            presentation["answer_contract"]["analysis_note"]["matched_entity_count"] = matched

    return presentation


def build_causal_lookup_presentation(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Optional[Dict[str, Any]],
    sparql_response: Optional[Dict[str, Any]],
    analysis_response: Optional[Dict[str, Any]],
    analysis_meta: Optional[Dict[str, Any]],
    status: str,
    analysis_error: Optional[Dict[str, Any]],
    analysis_skipped: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build a structured display model for causal lookup answers."""
    enumeration_like = build_causal_enumeration_presentation(
        schema,
        sparql_spec,
        sparql_response,
        analysis_response,
        analysis_meta,
        status,
        analysis_error,
        analysis_skipped,
    )
    if not enumeration_like:
        return None

    first_group = enumeration_like.get("groups", [])
    if not first_group:
        return {
            "template": "causal_lookup",
            "summary": enumeration_like.get("summary"),
            "analysis_status": enumeration_like.get("analysis_status"),
            "entity": None,
            "facts": [],
            "reasoning_summary": {
                "available": False,
                "path_count": 0,
                "truncated": False,
                "direct_relation_count": 0,
                "direct_summary": [],
                "mediator_summary": [],
                "terminal_summary": [],
            },
        }

    group = first_group[0]
    facts = group.get("evidence", [])
    metric_items = extract_fact_metric_items(facts)
    for item in metric_items_from_fields(group.get("metric_fields", {})):
        if item not in metric_items:
            metric_items.append(item)
    answer_contract = {
        "version": "causal_lookup_stable_v1",
        "preferred_section_order": [
            "summary",
            "entity_facts",
            "entity_metrics",
            "analysis_note",
        ],
        "count_contract": {
            "primary_count_field": "entity_count",
            "primary_count_label": "实体数",
            "primary_unit": "个实体",
            "secondary_count_field": "record_count",
            "secondary_count_label": "记录数",
            "secondary_unit": "条记录",
            "question_focus": "entity",
            "report_primary_count_first": True,
            "never_treat_record_count_as_entity_count": True,
        },
        "summary": enumeration_like.get("summary"),
        "entity_facts": {
            "entity_id": group.get("entity", {}).get("display_id"),
            "entity_name": group.get("entity", {}).get("display_name"),
            "facts": [
                {
                    "evidence_id": item.get("display_id"),
                    "evidence_label": item.get("display_label"),
                    "evidence_description": item.get("display_description"),
                }
                for item in facts
            ],
        },
        "entity_metrics": metric_items,
        "analysis_note": {
            "available": bool(analysis_response),
            "status": status,
            "mention_path_count_default": False,
            "mention_raw_paths_default": False,
            "brief_only": True,
        },
    }
    return {
        "template": "causal_lookup",
        "summary": enumeration_like.get("summary"),
        "analysis_status": enumeration_like.get("analysis_status"),
        "entity": group.get("entity"),
        "facts": facts,
        "key_metrics": metric_items,
        "reasoning_summary": group.get("reasoning_summary"),
        "trace_refs": group.get("trace_refs"),
        "answer_contract": answer_contract,
    }


def build_fact_lookup_presentation(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Optional[Dict[str, Any]],
    sparql_response: Optional[Dict[str, Any]],
    status: str,
) -> Optional[Dict[str, Any]]:
    """Build a stable presentation for anchored fact lookup results."""
    lookup_like = build_causal_lookup_presentation(
        schema,
        sparql_spec,
        sparql_response,
        None,
        None,
        status,
        None,
        None,
    )
    if not isinstance(lookup_like, dict):
        return None
    presentation = deepcopy(lookup_like)
    presentation["template"] = "fact_lookup"
    answer_contract = presentation.get("answer_contract")
    if isinstance(answer_contract, dict):
        answer_contract["version"] = "fact_lookup_stable_v1"
    analysis_status = presentation.get("analysis_status")
    if isinstance(analysis_status, dict):
        analysis_status["available"] = False
        analysis_status["status"] = status
    return presentation


def build_enumeration_presentation(
    sparql_response: Optional[Dict[str, Any]],
    status: str,
) -> Optional[Dict[str, Any]]:
    """Build a stable presentation for generic value-enumeration answers."""
    if not isinstance(sparql_response, dict):
        return None
    rows = sparql_response.get("results")
    if not isinstance(rows, list):
        return None

    items: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value_label = row.get("valueLabel")
        if is_missing_literal_value(value_label):
            continue
        item = {
            "value_label": value_label,
            "value_description": row.get("valueDescription"),
            "source_count": row.get("sourceCount"),
            "evidence_count": row.get("evidenceCount"),
            "raw_row": row,
        }
        items.append(item)

    if not items:
        return None

    answer_contract = {
        "version": "enumeration_value_stable_v1",
        "preferred_section_order": [
            "summary",
            "value_items",
        ],
        "summary": {
            "value_count": len(items),
            "record_count": len(rows),
            "status": status,
        },
        "value_items": [
            {
                "label": item.get("value_label"),
                "description": item.get("value_description"),
                "source_count": item.get("source_count"),
                "evidence_count": item.get("evidence_count"),
            }
            for item in items
        ],
        "analysis_note": {
            "available": False,
            "status": status,
            "brief_only": True,
        },
    }
    return {
        "template": "enumeration",
        "summary": {
            "value_count": len(items),
            "record_count": len(rows),
        },
        "items": items,
        "answer_contract": answer_contract,
    }


def build_run_presentation(
    plan: Dict[str, Any],
    schema: Optional[Dict[str, Any]],
    sparql_response: Optional[Dict[str, Any]],
    analysis_response: Optional[Dict[str, Any]],
    analysis_meta: Optional[Dict[str, Any]],
    status: str,
    analysis_error: Optional[Dict[str, Any]],
    analysis_skipped: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build an optional presentation layer for human-friendly answer generation."""
    template = plan.get("template")
    sparql_spec = plan.get("sparql")

    if template == "causal_enumeration":
        return build_causal_enumeration_presentation(
            schema,
            sparql_spec,
            sparql_response,
            analysis_response,
            analysis_meta,
            status,
            analysis_error,
            analysis_skipped,
        )

    if template == "causal_lookup":
        return build_causal_lookup_presentation(
            schema,
            sparql_spec,
            sparql_response,
            analysis_response,
            analysis_meta,
            status,
            analysis_error,
            analysis_skipped,
        )

    if template == "fact_lookup":
        return build_fact_lookup_presentation(
            schema,
            sparql_spec,
            sparql_response,
            status,
        )

    if template == "enumeration":
        return build_enumeration_presentation(
            sparql_response,
            status,
        )

    return None


def _build_single_question_mode_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    schema: Optional[Dict[str, Any]] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a planning bundle for one QUESTION + TEMPLATE shorthand."""
    template_config = RUN_TEMPLATES.get(template)
    if template_config is None:
        supported = ", ".join(sorted(RUN_TEMPLATES))
        raise SystemExit(f"Unknown run template: {template}. Supported templates: {supported}")

    if schema is None:
        schema = request_json("GET", f"{base_url}/schema")
        write_schema_state(state_file, base_url)

    planner = build_semantic_query_planner(
        question,
        template,
        schema,
        base_url=base_url,
        slots_override=slots_override,
        unit_intent_ir=unit_intent_ir,
    )
    effective_template = template
    if isinstance(planner, dict) and isinstance(planner.get("effective_template"), str) and planner.get("effective_template"):
        effective_template = planner["effective_template"]
        effective_config = RUN_TEMPLATES.get(effective_template)
        if effective_config is not None:
            template_config = effective_config

    profiles = None
    if template_config["auto_include_profiles"]:
        profiles = request_json("GET", f"{base_url}/analysis/profiles")

    plan_skeleton: Dict[str, Any] = {"template": effective_template}
    required_fields = []
    if isinstance(planner, dict) and isinstance(planner.get("selected_plan"), dict):
        plan_skeleton = planner["selected_plan"]
        if isinstance(plan_skeleton.get("template"), str) and plan_skeleton.get("template"):
            effective_template = plan_skeleton["template"]

    if template_config["requires_sparql"] and "sparql" not in plan_skeleton:
        if effective_template in ("causal_lookup", "causal_enumeration"):
            required_fields.append("sparql.builder")
            source_var = "source"
            evidence_var = "evidence"
            builder = {
                "source_class": "SourceClass",
                "source_var": source_var,
                "evidence_class": "EvidenceClass",
                "evidence_var": evidence_var,
                "select": [
                    {"var": source_var, "kind": "uri"},
                    {"var": "sourceName", "subject": "source", "property": "source_name"},
                    {"var": "sourceId", "subject": "source", "property": "source_id"},
                    {"var": evidence_var, "kind": "uri"},
                    {"var": "evidenceType", "subject": "evidence", "property": "evidence_type"},
                    {"var": "evidenceDescription", "subject": "evidence", "property": "evidence_description"},
                ],
                "filters": (
                    [{"var": "sourceId", "op": "equals", "value": "ID_123"}]
                    if effective_template == "causal_lookup"
                    else [{"var": "evidenceType", "op": "contains_any", "values": ["keyword1", "keyword2"]}]
                ),
                "distinct": True,
                "order_by": ["sourceId", "evidenceType"],
            }
            plan_skeleton["sparql"] = {
                "source_var": source_var,
                "builder": builder,
            }
        else:
            required_fields.append("sparql.query")
            plan_skeleton["sparql"] = {
                "query": (
                    "PREFIX ex: <http://example.com/ontology#>\n"
                    "SELECT ?entity\n"
                    "WHERE {\n"
                    "  ?entity a ex:TargetClass .\n"
                    "}\n"
                    "LIMIT 10"
                )
            }

    analysis_kind = template_config["default_analysis_kind"]
    if template_config["requires_analysis"] and analysis_kind and "analysis" not in plan_skeleton:
        required_fields.append("analysis.payload")
        analysis_payload: Dict[str, Any]
        if analysis_kind == "paths":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "max_depth": 3,
            }
        elif analysis_kind == "paths-batch":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "max_depth": 3,
            }
        elif analysis_kind == "inferred-relations":
            analysis_payload = {
                "mode": "inferred-relations",
                "profile": "inference",
                "source": "http://example.com/ontology#entity_123",
                "max_depth": 3,
            }
        else:
            analysis_payload = {"mode": analysis_kind}

        plan_skeleton["analysis"] = {
            "kind": analysis_kind,
            "payload": analysis_payload,
        }

    plan_ready = bool(isinstance(planner, dict) and planner.get("ready"))
    if plan_ready:
        required_fields = []

    response: Dict[str, Any] = {
        "mode": "question-template",
        "status": "planner_suggested" if plan_ready else "planning_required",
        "question": question,
        "template": effective_template,
        "message": (
            "Planning-only mode fetched schema first and routed the question through the semantic query planner. "
            "Use this only for debugging or inspection; normal QUESTION + --template flow executes the locked planner plan automatically."
        ),
        "required_fields": required_fields,
        "plan_skeleton": plan_skeleton,
        "planner": planner,
        "plan_executable": plan_ready,
        "rules": [
            "Normal QUESTION + --template flow executes automatically; use --plan-only only when you explicitly need the planner bundle.",
            "Do not hand-write GET /analysis/paths query strings; use analysis-paths --json or analysis-paths-batch --json.",
            "Use schema to verify domains before writing SPARQL.",
            "For causal templates, prefer sparql.builder over raw sparql.query so the client can validate link direction and required anchor columns before execution.",
            "If the planner is low-confidence or ambiguous, do not guess; refine one slot or ask for clarification.",
        ],
        "schema_summary": summarize_schema(schema),
        "schema_included": False,
    }
    if effective_template != template:
        response["requested_template"] = template
        response["effective_template"] = effective_template
    elif isinstance(planner, dict) and planner.get("query_family"):
        response["effective_template"] = effective_template
    if isinstance(planner, dict) and planner.get("query_family"):
        response["query_family"] = planner.get("query_family")
    if profiles is not None:
        response["profiles_summary"] = summarize_profiles(profiles)
        response["profiles_included"] = False
    if isinstance(unit_intent_ir, dict):
        response["intent_ir"] = unit_intent_ir
    return response


def summarize_batch_unit_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact per-unit summary for multi-answer presentation."""
    summary = {
        "status": response.get("status"),
        "template": response.get("effective_template") or response.get("template"),
        "query_family": response.get("query_family"),
    }
    if isinstance(response.get("presentation"), dict):
        presentation = response["presentation"]
        if isinstance(presentation.get("summary"), dict):
            summary["result_summary"] = presentation["summary"]
    planner = response.get("planner")
    if isinstance(planner, dict) and planner.get("reason"):
        summary["planner_reason"] = planner.get("reason")
    if response.get("blocked_reason"):
        summary["blocked_reason"] = response.get("blocked_reason")
    return summary


def extract_focus_refs_from_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Extract focus identifiers from a unit response for conversation state."""
    focus: Dict[str, Any] = {
        "entity_ids": [],
        "entity_names": [],
        "entity_uris": [],
        "entity_local_names": [],
        "entity_classes": [],
        "value_labels": [],
        "grain": None,
        "entity_class": None,
    }
    presentation = response.get("presentation")
    if not isinstance(presentation, dict):
        return focus

    def collect_entity_info(entity_info: Dict[str, Any]) -> None:
        if not isinstance(entity_info, dict):
            return
        if entity_info.get("display_id"):
            focus["entity_ids"].append(entity_info.get("display_id"))
        if entity_info.get("display_name"):
            focus["entity_names"].append(entity_info.get("display_name"))
        uri = entity_info.get("uri")
        if is_uri_like(uri):
            focus["entity_uris"].append(uri)
            class_key = class_key_from_uri(uri)
            if class_key:
                focus["entity_classes"].append(class_key)
        local_name = entity_info.get("local_name")
        if isinstance(local_name, str) and local_name:
            focus["entity_local_names"].append(local_name)
            class_key = class_key_from_uri(local_name)
            if class_key:
                focus["entity_classes"].append(class_key)

    entity = presentation.get("entity")
    if isinstance(entity, dict):
        focus["grain"] = "entity"
        collect_entity_info(entity)

    groups = presentation.get("groups")
    if isinstance(groups, list):
        focus["grain"] = "entity_set"
        for group in groups:
            if not isinstance(group, dict):
                continue
            entity_info = group.get("entity")
            collect_entity_info(entity_info)

    items = presentation.get("items")
    if isinstance(items, list):
        if focus["grain"] is None:
            focus["grain"] = "rows"
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("value_label"):
                focus["value_labels"].append(item.get("value_label"))

    focus["entity_ids"] = unique_preserve_order([str(item) for item in focus["entity_ids"] if item])
    focus["entity_names"] = unique_preserve_order([str(item) for item in focus["entity_names"] if item])
    focus["entity_uris"] = unique_preserve_order([str(item) for item in focus["entity_uris"] if item])
    focus["entity_local_names"] = unique_preserve_order([str(item) for item in focus["entity_local_names"] if item])
    focus["entity_classes"] = unique_preserve_order([str(item) for item in focus["entity_classes"] if item])
    focus["value_labels"] = unique_preserve_order([str(item) for item in focus["value_labels"] if item])
    if focus["entity_classes"]:
        focus["entity_class"] = focus["entity_classes"][0]
    return focus


def build_conversation_state_entry(
    unit: Dict[str, Any],
    slots: Dict[str, Any],
    intent_ir: Dict[str, Any],
    response: Dict[str, Any],
) -> Dict[str, Any]:
    """Build carry-forward conversation state for one executed/planned unit."""
    focus_refs = extract_focus_refs_from_response(response)
    semantic_state = semantic_state_from_sources(slots, intent_ir)
    return {
        "unit_id": unit.get("unit_id"),
        "anchors": deepcopy(semantic_state.get("anchors", [])),
        "has_anchor": bool(semantic_state.get("has_anchor")),
        "bootstrap_signals": deepcopy(slots.get("bootstrap_signals", {}))
        if isinstance(slots.get("bootstrap_signals"), dict)
        else {},
        "bootstrap_candidates": deepcopy(slots.get("bootstrap_candidates", {}))
        if isinstance(slots.get("bootstrap_candidates"), dict)
        else {},
        "status_numeric_constraint": deepcopy(semantic_state.get("status_numeric_constraint")),
        "intent_ir": deepcopy(intent_ir),
        "focus": focus_refs,
        "query_family": response.get("query_family"),
        "effective_template": response.get("effective_template") or response.get("template"),
        "status": response.get("status"),
    }


def conversation_state_has_material_focus(state: Optional[Dict[str, Any]]) -> bool:
    """Whether a conversation state carries a concrete entity/value focus."""
    if not isinstance(state, dict):
        return False
    focus = state.get("focus")
    if not isinstance(focus, dict):
        return False
    for key in ("entity_uris", "entity_ids", "entity_local_names", "value_labels"):
        values = focus.get(key)
        if isinstance(values, list) and values:
            return True
    return False


def find_conversation_state_by_unit_id(
    conversation_states: List[Dict[str, Any]],
    unit_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Locate one conversation state entry by unit id."""
    if not isinstance(unit_id, str) or not unit_id:
        return None
    for state in conversation_states:
        if isinstance(state, dict) and state.get("unit_id") == unit_id:
            return state
    return None


def resolve_reference_context(
    unit: Dict[str, Any],
    conversation_states: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Resolve lightweight discourse references against executed conversation state."""
    if not isinstance(unit, dict):
        return None
    dependency = unit.get("dependency")
    reference_markers = unit.get("reference_markers", [])
    candidate_states: List[Dict[str, Any]] = []

    if isinstance(dependency, dict) and isinstance(dependency.get("depends_on"), str):
        matched = find_conversation_state_by_unit_id(conversation_states, dependency.get("depends_on"))
        if isinstance(matched, dict):
            candidate_states.append(matched)

    for state in reversed(conversation_states):
        if not isinstance(state, dict):
            continue
        if any(existing.get("unit_id") == state.get("unit_id") for existing in candidate_states if isinstance(existing, dict)):
            continue
        candidate_states.append(state)

    selected_state = next((state for state in candidate_states if conversation_state_has_material_focus(state)), None)
    if not isinstance(selected_state, dict):
        return None

    focus = selected_state.get("focus")
    if not isinstance(focus, dict):
        return None

    return {
        "from_unit_id": selected_state.get("unit_id"),
        "markers": list(reference_markers) if isinstance(reference_markers, list) else [],
        "entity_ids": deepcopy(focus.get("entity_ids", [])),
        "entity_names": deepcopy(focus.get("entity_names", [])),
        "entity_uris": deepcopy(focus.get("entity_uris", [])),
        "entity_local_names": deepcopy(focus.get("entity_local_names", [])),
        "entity_class": focus.get("entity_class"),
        "grain": focus.get("grain"),
        "query_family": selected_state.get("query_family"),
        "effective_template": selected_state.get("effective_template"),
        "status": selected_state.get("status"),
    }


def apply_resolved_reference_to_slots(
    slots: Dict[str, Any],
    resolved_reference: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Attach resolved conversation references to slot state for later grounding/lowering."""
    merged = dict(slots)
    for key in (
        "reference_entity_ids",
        "reference_entity_names",
        "reference_entity_uris",
        "reference_entity_local_names",
        "reference_entity_class",
        "reference_grain",
        "reference_from_unit_id",
        "resolved_reference",
    ):
        merged.pop(key, None)

    if merged.get("has_explicit_anchor"):
        return merged

    if not isinstance(resolved_reference, dict):
        return merged

    entity_ids = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_ids", [])
        if isinstance(item, str) and item
    ])
    entity_names = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_names", [])
        if isinstance(item, str) and item
    ])
    entity_uris = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_uris", [])
        if isinstance(item, str) and item
    ])
    entity_local_names = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_local_names", [])
        if isinstance(item, str) and item
    ])
    if not any((entity_ids, entity_names, entity_uris, entity_local_names)):
        return merged

    merged["reference_entity_ids"] = entity_ids
    merged["reference_entity_names"] = entity_names
    merged["reference_entity_uris"] = entity_uris
    merged["reference_entity_local_names"] = entity_local_names
    merged["reference_entity_class"] = resolved_reference.get("entity_class")
    merged["reference_grain"] = resolved_reference.get("grain")
    merged["reference_from_unit_id"] = resolved_reference.get("from_unit_id")
    merged["resolved_reference"] = deepcopy(resolved_reference)
    return merged


def evaluate_dependency_condition(
    condition_type: Optional[str],
    dependency_response: Optional[Dict[str, Any]],
) -> bool:
    """Evaluate whether a dependency condition is satisfied."""
    if not isinstance(dependency_response, dict):
        return False

    status = dependency_response.get("status")
    presentation = dependency_response.get("presentation")
    material_result = False
    if isinstance(presentation, dict) and isinstance(presentation.get("summary"), dict):
        summary = presentation["summary"]
        for key in ("entity_count", "record_count", "value_count", "evidence_count"):
            value = summary.get(key)
            if isinstance(value, int) and value > 0:
                material_result = True
                break
    if not material_result and isinstance(dependency_response.get("sparql"), dict):
        material_result = sparql_row_count(dependency_response.get("sparql")) > 0

    if condition_type == "empty_or_false":
        return status in ("empty_result", "planning_required") or not material_result
    return status in ("ok", "partial_success") and material_result


def build_execution_dag(question_units: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Represent QuestionUnit dependencies as a small execution DAG."""
    nodes = []
    edges = []
    for unit in question_units:
        nodes.append({
            "unit_id": unit.get("unit_id"),
            "text": unit.get("text"),
        })
        dependency = unit.get("dependency")
        if isinstance(dependency, dict) and dependency.get("depends_on"):
            edges.append({
                "from": dependency.get("depends_on"),
                "to": unit.get("unit_id"),
                "condition": dependency.get("condition"),
            })
    return {"nodes": nodes, "edges": edges}


def build_question_batch_presentation(
    utterance: str,
    unit_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a structured multi-answer presentation for batch execution."""
    executed_count = 0
    blocked_count = 0
    planning_required_count = 0
    items = []

    for item in unit_results:
        response = item.get("response", {})
        status = response.get("status")
        if status in ("ok", "partial_success", "empty_result"):
            executed_count += 1
        elif status == "skipped":
            blocked_count += 1
        elif status == "planning_required":
            planning_required_count += 1

        items.append({
            "unit_id": item.get("unit_id"),
            "text": item.get("text"),
            "status": status,
            "summary": summarize_batch_unit_response(response if isinstance(response, dict) else {}),
        })

    answer_contract = {
        "version": "question_batch_v1",
        "preferred_section_order": [
            "summary",
            "unit_answers",
        ],
    }

    return {
        "template": "question_batch",
        "summary": {
            "utterance": utterance,
            "unit_count": len(unit_results),
            "executed_count": executed_count,
            "blocked_count": blocked_count,
            "planning_required_count": planning_required_count,
        },
        "items": items,
        "answer_contract": answer_contract,
    }


def compute_batch_execution_status(unit_results: List[Dict[str, Any]]) -> str:
    """Summarize overall batch execution status without overstating success."""
    statuses = [
        item.get("response", {}).get("status")
        for item in unit_results
        if isinstance(item, dict) and isinstance(item.get("response"), dict)
    ]
    if any(status in ("ok", "partial_success", "empty_result") for status in statuses):
        return "batch_executed"
    if any(status == "planning_required" for status in statuses):
        return "planning_required"
    if any(status == "skipped" for status in statuses):
        return "skipped"
    return "planning_required"


def build_question_batch_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
) -> Dict[str, Any]:
    """Plan a multi-question utterance as a batch of QuestionUnits."""
    schema = request_json("GET", f"{base_url}/schema")
    write_schema_state(state_file, base_url)

    question_units = decompose_utterance_to_question_units(question)
    unit_plans = []
    conversation_states = []
    inherited_context = None

    for unit in question_units:
        slots = extract_question_slots(
            unit["text"],
            template,
            inherited_context=inherited_context,
            question_unit=unit,
        )
        intent_ir = build_question_unit_intent_ir(unit, slots, template)
        response = _build_single_question_mode_run_response(
            base_url,
            unit["text"],
            template,
            state_file,
            schema=schema,
            slots_override=slots,
            unit_intent_ir=intent_ir,
        )
        unit_entry = {
            "unit_id": unit.get("unit_id"),
            "text": unit.get("text"),
            "raw_text": unit.get("raw_text"),
            "dependency": deepcopy(unit.get("dependency")),
            "reference_markers": list(unit.get("reference_markers", [])),
            "resolved_slots": slots,
            "intent_ir": intent_ir,
            "response": response,
        }
        unit_plans.append(unit_entry)
        inherited_context = build_conversation_state_entry(unit, slots, intent_ir, response)
        conversation_states.append(inherited_context)

    executable_count = sum(
        1 for item in unit_plans
        if isinstance(item.get("response"), dict) and item["response"].get("plan_executable")
    )
    overall_status = "batch_planner_suggested" if executable_count else "planning_required"

    return {
        "mode": "question-batch-template",
        "status": overall_status,
        "question": question,
        "template": template,
        "question_units": unit_plans,
        "execution_dag": build_execution_dag(question_units),
        "conversation_state": {
            "last_unit_id": conversation_states[-1]["unit_id"] if conversation_states else None,
            "units": conversation_states,
        },
        "presentation": build_question_batch_presentation(question, unit_plans),
        "schema_summary": summarize_schema(schema),
        "schema_included": False,
        "message": (
            "Planning-only mode decomposed the utterance into QuestionUnits, built Intent IR, "
            "resolved lightweight references, and planned each unit independently."
        ),
    }


def build_question_mode_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
) -> Dict[str, Any]:
    """Return a planning bundle for QUESTION + TEMPLATE shorthand."""
    question_units = decompose_utterance_to_question_units(question)
    if len(question_units) > 1:
        return build_question_batch_run_response(base_url, question, template, state_file)
    single_unit = question_units[0] if question_units else {"unit_id": "q1", "text": question, "reference_markers": []}
    slots = extract_question_slots(question, template, question_unit=single_unit)
    intent_ir = build_question_unit_intent_ir(single_unit, slots, template)
    return _build_single_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        slots_override=slots,
        unit_intent_ir=intent_ir,
    )


def apply_fail_closed_contract_to_question_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Strip executable-looking scaffolding from non-executable question-mode responses."""
    contracted = dict(response)
    contracted["plan_executable"] = False
    contracted["manual_fallback_allowed"] = False
    contracted["planner_bundle_available_via_plan_only"] = True
    contracted["next_action"] = "stop_or_use_plan_only_for_debug"
    planner = contracted.get("planner")
    if isinstance(planner, dict) and isinstance(planner.get("clarification_hint"), dict):
        contracted["clarification_hint"] = deepcopy(planner["clarification_hint"])
    clarification_hint = contracted.get("clarification_hint")
    if isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification"):
        contracted["next_action"] = "ask_user_for_clarification"
        contracted["user_clarification_prompt"] = clarification_hint.get("user_clarification_prompt")
    contracted["recovery_policy"] = {
        "mode": "fail_closed",
        "manual_exploration_allowed": False,
        "bounded_recovery_allowed": bool(contracted.get("recovery_hint")),
        "requires_plan_only_for_debug": True,
        "requires_user_clarification": bool(
            isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification")
        ),
    }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    rule = (
        "If question-mode returns planning_required and no recovery_hint is present, stop. "
        "Do not switch to manual sparql/sample exploration in the same turn."
    )
    if rule not in rules:
        rules.append(rule)
    contracted["rules"] = rules
    contracted.pop("plan_skeleton", None)
    contracted.pop("required_fields", None)
    return contracted


def apply_fail_closed_contract_to_batch_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Surface top-level clarification guidance for batch question-mode failures."""
    contracted = dict(response)
    contracted["manual_fallback_allowed"] = False
    contracted["planner_bundle_available_via_plan_only"] = True
    contracted["next_action"] = "stop_or_use_plan_only_for_debug"
    clarification_hint = None
    clarification_unit_id = None
    for unit in contracted.get("question_units", []):
        if not isinstance(unit, dict):
            continue
        unit_response = unit.get("response")
        if not isinstance(unit_response, dict):
            continue
        unit_hint = unit_response.get("clarification_hint")
        if isinstance(unit_hint, dict):
            clarification_hint = deepcopy(unit_hint)
            clarification_unit_id = unit.get("unit_id")
            break
    if isinstance(clarification_hint, dict):
        contracted["clarification_hint"] = clarification_hint
        if isinstance(clarification_unit_id, str) and clarification_unit_id:
            contracted["clarification_target_unit"] = clarification_unit_id
        if clarification_hint.get("requires_user_clarification"):
            contracted["next_action"] = "ask_user_for_clarification"
            contracted["user_clarification_prompt"] = clarification_hint.get("user_clarification_prompt")
    contracted["recovery_policy"] = {
        "mode": "fail_closed",
        "manual_exploration_allowed": False,
        "bounded_recovery_allowed": False,
        "requires_plan_only_for_debug": True,
        "requires_user_clarification": bool(
            isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification")
        ),
    }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    rule = (
        "If question-mode returns planning_required and no recovery_hint is present, stop. "
        "Do not switch to manual sparql/sample exploration in the same turn."
    )
    if rule not in rules:
        rules.append(rule)
    contracted["rules"] = rules
    return contracted


def _execute_single_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
    planning_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Plan and execute one QUESTION + TEMPLATE shorthand with a locked planner-selected plan."""
    planning = planning_override or _build_single_question_mode_run_response(base_url, question, template, state_file)
    if not planning.get("plan_executable"):
        planning["message"] = (
            "QUESTION + --template fetched schema first but could not produce a high-confidence executable plan."
        )
        return apply_fail_closed_contract_to_question_response(planning)

    planner = planning.get("planner")
    candidate_plans = []
    if isinstance(planner, dict) and isinstance(planner.get("candidate_plans"), list):
        candidate_plans = [item for item in planner["candidate_plans"] if isinstance(item, dict)]

    selected_candidate = candidate_plans[0] if candidate_plans else None
    locked_plan = planning.get("plan_skeleton")
    if selected_candidate and isinstance(selected_candidate.get("plan"), dict):
        locked_plan = selected_candidate["plan"]
    if not isinstance(locked_plan, dict):
        raise SystemExit("Question-mode planner did not return a valid executable plan.")

    def summarize_attempt(candidate: Optional[Dict[str, Any]], result: Dict[str, Any]) -> Dict[str, Any]:
        attempt = {
            "status": result.get("status"),
            "row_count": sparql_row_count(result.get("sparql")),
        }
        if isinstance(candidate, dict):
            attempt["variant"] = candidate.get("variant")
            attempt["confidence_score"] = candidate.get("confidence_score")
            attempt["rationale"] = candidate.get("rationale")
        return attempt

    def attempt_score(candidate: Optional[Dict[str, Any]], result: Dict[str, Any]) -> tuple:
        status = result.get("status")
        status_score = 2 if status == "ok" else 1 if status == "partial_success" else 0
        row_count = sparql_row_count(result.get("sparql"))
        confidence = candidate.get("confidence_score") if isinstance(candidate, dict) else 0
        return (status_score, row_count, confidence)

    def try_execute_candidate_plan(
        candidate: Optional[Dict[str, Any]],
        plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            return execute_run_plan(base_url, plan, state_file)
        except SystemExit as exc:
            return {
                "status": "execution_error",
                "template": plan.get("template") if isinstance(plan.get("template"), str) else template,
                "error": str(exc),
            }

    executed = try_execute_candidate_plan(selected_candidate, locked_plan)
    if executed.get("status") == "execution_error":
        raise SystemExit(executed.get("error") or "Question-mode planner selected an invalid executable plan.")
    attempts = [summarize_attempt(selected_candidate, executed)]
    chosen_candidate = selected_candidate

    relaxed_candidates = [
        candidate
        for candidate in candidate_plans[1:]
        if str(candidate.get("variant", "")).endswith("_relaxed")
    ]

    primary_row_count = sparql_row_count(executed.get("sparql"))
    should_try_relaxed = (
        template == "causal_enumeration"
        and bool(relaxed_candidates)
        and (
            executed.get("status") == "empty_result"
            or (
                executed.get("status") == "ok"
                and primary_row_count <= 1
                and isinstance(selected_candidate, dict)
                and (
                    selected_candidate.get("variant") in ("cause_and_action", "same_evidence_strict")
                    or (
                        isinstance(selected_candidate.get("rationale"), list)
                        and "cause_term_grounded" in selected_candidate["rationale"]
                        and "action_term_grounded" in selected_candidate["rationale"]
                    )
                )
            )
        )
    )

    if should_try_relaxed:
        best_result = executed
        best_candidate = chosen_candidate
        best_score = attempt_score(chosen_candidate, executed)
        for relaxed_candidate in relaxed_candidates:
            if not isinstance(relaxed_candidate.get("plan"), dict):
                continue
            relaxed_result = try_execute_candidate_plan(relaxed_candidate, relaxed_candidate["plan"])
            attempts.append(summarize_attempt(relaxed_candidate, relaxed_result))
            relaxed_score = attempt_score(relaxed_candidate, relaxed_result)
            if relaxed_score > best_score:
                best_result = relaxed_result
                best_candidate = relaxed_candidate
                best_score = relaxed_score
        executed = best_result
        chosen_candidate = best_candidate

    response = dict(executed)
    effective_template = response.get("template") if isinstance(response.get("template"), str) else template
    response.update({
        "mode": "question-template",
        "question": question,
        "template": effective_template,
        "execution_mode": "question_auto_execute",
        "plan_locked": True,
        "plan_executable": True,
        "message": (
            "QUESTION + --template fetched schema first, planned a locked query, "
            "and executed it automatically."
        ),
    })
    if effective_template != template:
        response["requested_template"] = template
        response["effective_template"] = effective_template
    elif isinstance(planning.get("planner"), dict) and planning["planner"].get("query_family"):
        response["effective_template"] = effective_template
    if isinstance(planning.get("planner"), dict) and planning["planner"].get("query_family"):
        response["query_family"] = planning["planner"].get("query_family")
    if include_planner_debug:
        response["planner_summary"] = summarize_planner_result(planning.get("planner"))
        response["planner_attempts"] = attempts
        response["execution_variant"] = chosen_candidate.get("variant") if isinstance(chosen_candidate, dict) else None
    return response


def execute_question_batch_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
) -> Dict[str, Any]:
    """Execute a multi-question utterance via an Execution DAG."""
    planning = build_question_batch_run_response(base_url, question, template, state_file)
    schema = request_json("GET", f"{base_url}/schema")
    write_schema_state(state_file, base_url)
    unit_results = []
    response_by_unit_id: Dict[str, Dict[str, Any]] = {}
    conversation_states = []
    inherited_context = None

    for unit in planning.get("question_units", []):
        if not isinstance(unit, dict):
            continue
        unit_id = unit.get("unit_id")
        dependency = unit.get("dependency")
        should_execute = True
        blocked_reason = None
        if isinstance(dependency, dict) and dependency.get("depends_on"):
            dependency_response = response_by_unit_id.get(dependency["depends_on"])
            if not evaluate_dependency_condition(dependency.get("condition"), dependency_response):
                should_execute = False
                blocked_reason = {
                    "depends_on": dependency.get("depends_on"),
                    "condition": dependency.get("condition"),
                    "reason": "dependency_condition_not_met",
                }

        slots = extract_question_slots(
            unit.get("text") or "",
            template,
            inherited_context=inherited_context,
            question_unit=unit,
        )
        resolved_reference = resolve_reference_context(unit, conversation_states)
        slots = apply_resolved_reference_to_slots(slots, resolved_reference)
        intent_ir = build_question_unit_intent_ir(unit, slots, template, resolved_reference=resolved_reference)
        planned_response = _build_single_question_mode_run_response(
            base_url,
            unit.get("text") or "",
            template,
            state_file,
            schema=schema,
            slots_override=slots,
            unit_intent_ir=intent_ir,
        )

        if not should_execute:
            response = {
                "mode": "question-template",
                "status": "skipped",
                "question": unit.get("text"),
                "template": planned_response.get("effective_template") or planned_response.get("template") or template,
                "effective_template": planned_response.get("effective_template") or planned_response.get("template") or template,
                "query_family": planned_response.get("query_family"),
                "blocked_reason": blocked_reason,
                "message": "Execution DAG skipped this unit because its dependency condition was not met.",
            }
        elif not planned_response.get("plan_executable"):
            response = dict(planned_response)
            response["message"] = (
                "QUESTION + --template decomposed the utterance, but this unit could not produce a high-confidence executable plan."
            )
            response = apply_fail_closed_contract_to_question_response(response)
        else:
            response = _execute_single_question_mode_run(
                base_url,
                unit.get("text"),
                template,
                state_file,
                include_planner_debug=include_planner_debug,
                planning_override=planned_response,
            )

        unit_entry = dict(unit)
        unit_entry["resolved_slots"] = slots
        unit_entry["intent_ir"] = intent_ir
        unit_entry["response"] = response
        unit_results.append(unit_entry)
        if isinstance(unit_id, str):
            response_by_unit_id[unit_id] = response
        inherited_context = build_conversation_state_entry(
            {
                "unit_id": unit_id,
                "text": unit.get("text"),
            },
            slots,
            intent_ir,
            response,
        )
        conversation_states.append(inherited_context)

    batch_response = {
        "mode": "question-batch-template",
        "status": compute_batch_execution_status(unit_results),
        "question": question,
        "template": template,
        "question_units": unit_results,
        "execution_dag": planning.get("execution_dag"),
        "conversation_state": {
            "last_unit_id": conversation_states[-1]["unit_id"] if conversation_states else None,
            "units": conversation_states,
        },
        "presentation": build_question_batch_presentation(question, unit_results),
        "schema_summary": planning.get("schema_summary"),
        "schema_included": False,
        "message": (
            "QUESTION + --template decomposed the utterance, executed the unit DAG, "
            "and produced a structured multi-answer result."
        ),
    }
    if batch_response["status"] == "planning_required":
        batch_response = apply_fail_closed_contract_to_batch_response(batch_response)
    return batch_response


def execute_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
) -> Dict[str, Any]:
    """Plan and execute QUESTION + TEMPLATE shorthand with a locked planner-selected plan."""
    question_units = decompose_utterance_to_question_units(question)
    if len(question_units) > 1:
        return execute_question_batch_run(
            base_url,
            question,
            template,
            state_file,
            include_planner_debug=include_planner_debug,
        )
    single_unit = question_units[0] if question_units else {"unit_id": "q1", "text": question, "reference_markers": []}
    slots = extract_question_slots(question, template, question_unit=single_unit)
    intent_ir = build_question_unit_intent_ir(single_unit, slots, template)
    planning = _build_single_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        slots_override=slots,
        unit_intent_ir=intent_ir,
    )
    return _execute_single_question_mode_run(
        base_url,
        question,
        template,
        state_file,
        include_planner_debug=include_planner_debug,
        planning_override=planning,
    )


def execute_run_plan(
    base_url: str,
    plan: Dict[str, Any],
    state_file: Path,
) -> Dict[str, Any]:
    """Execute a guarded multi-step workflow with schema fetched first."""
    plan = normalize_run_plan(plan)
    if not any(key in plan for key in ("samples", "sparql", "analysis")):
        raise SystemExit("run requires at least one of: samples, sparql, analysis")

    def run_plan(request_fn):
        schema = request_fn("GET", f"{base_url}/schema")
        write_schema_state(state_file, base_url)

        analysis_spec = plan.get("analysis")
        include_profiles = bool(plan.get("include_profiles"))
        profiles = request_fn("GET", f"{base_url}/analysis/profiles") if include_profiles else None

        samples = []
        for sample_spec in plan.get("samples", []):
            class_name = sample_spec.get("class_name")
            if not class_name:
                raise SystemExit("Each sample spec requires class_name")
            limit = int(sample_spec.get("limit", 3))
            query = urllib.parse.urlencode({"limit": limit})
            url = f"{base_url}/sample/{urllib.parse.quote(class_name)}?{query}"
            samples.append({
                "class_name": class_name,
                "limit": limit,
                "response": request_fn("GET", url),
            })

        sparql_response = None
        sparql_meta = None
        sparql_spec = plan.get("sparql")
        if sparql_spec is not None:
            prepared = prepare_sparql_spec(schema, sparql_spec, plan["template"])
            query_text = prepared["query"]
            sparql_meta = prepared.get("builder_meta")
            sparql_response = request_fn("POST", f"{base_url}/sparql", {"query": query_text})

        analysis_response = None
        analysis_meta = None
        analysis_error = None
        analysis_skipped = None
        if analysis_spec is not None:
            kind = analysis_spec.get("kind", "paths")
            row_count = sparql_row_count(sparql_response)

            if plan["template"] in ("causal_lookup", "causal_enumeration") and sparql_spec is not None and row_count == 0:
                analysis_skipped = {
                    "reason": "sparql_no_results",
                    "message": "Main SPARQL returned no rows, so analyzer was not executed.",
                }
            elif kind == "causal":
                customer_id = analysis_spec.get("customer_id")
                if not customer_id:
                    raise SystemExit("analysis kind 'causal' requires customer_id")
                analysis_response = request_fn(
                    "GET",
                    f"{base_url}/causal/{urllib.parse.quote(customer_id)}",
                )
            else:
                payload = analysis_spec.get("payload")
                if payload is None:
                    payload = {k: v for k, v in analysis_spec.items() if k != "kind"}
                payload = dict(payload)

                preferred_source_var = analysis_spec.get("source_var")
                if not preferred_source_var and sparql_spec is not None:
                    preferred_source_var = sparql_spec.get("source_var")
                if not preferred_source_var and sparql_meta is not None:
                    preferred_source_var = prepared.get("source_var")

                if kind == "paths-batch" and not payload.get("sources"):
                    derived = derive_uri_sources_from_sparql(
                        sparql_response,
                        preferred_var=preferred_source_var,
                        multiple=True,
                    )
                    if derived["values"]:
                        payload["sources"] = derived["values"]
                        analysis_meta = {
                            "auto_derived_source_var": derived["source_var"],
                            "auto_derived_source_count": len(derived["values"]),
                        }
                elif kind == "paths" and not payload.get("source"):
                    derived = derive_uri_sources_from_sparql(
                        sparql_response,
                        preferred_var=preferred_source_var,
                        multiple=False,
                    )
                    if derived["values"]:
                        payload["source"] = derived["values"][0]
                        analysis_meta = {
                            "auto_derived_source_var": derived["source_var"],
                            "auto_derived_source_count": 1,
                        }

                if kind == "paths-batch" and not payload.get("sources"):
                    analysis_error = {
                        "kind": "missing_sources",
                        "message": (
                            "paths-batch analysis requires analysis.payload.sources, "
                            "or a SPARQL result column containing URI anchors that the client can auto-derive."
                        ),
                        "hint": (
                            "Return at least one entity URI column from the main SPARQL, "
                            "for example ?source or ?entity, or set sparql.source_var / analysis.payload.sources explicitly."
                        ),
                    }
                elif kind == "paths" and not payload.get("source"):
                    analysis_error = {
                        "kind": "missing_source",
                        "message": (
                            "paths analysis requires analysis.payload.source, "
                            "or a SPARQL result column containing a URI anchor that the client can auto-derive."
                        ),
                        "hint": (
                            "Return at least one entity URI column from the main SPARQL, "
                            "for example ?source or ?entity, or set sparql.source_var / analysis.payload.source explicitly."
                        ),
                    }
                else:
                    if kind in ("paths", "paths-batch") and "mode" not in payload:
                        payload["mode"] = "paths"
                    endpoint_map = {
                        "paths": "/analysis/paths",
                        "paths-batch": "/analysis/paths/batch",
                        "neighborhood": "/analysis/neighborhood",
                        "inferred-relations": "/analysis/inferred-relations",
                        "explain": "/analysis/explain",
                    }
                    endpoint = endpoint_map.get(kind)
                    if endpoint is None:
                        raise SystemExit(f"Unsupported analysis kind: {kind}")
                    analysis_response = request_fn("POST", f"{base_url}{endpoint}", payload)

        response = {
            "template": plan["template"],
            "sparql": sparql_response,
            "sparql_meta": sparql_meta,
            "analysis_meta": analysis_meta,
        }
        if samples:
            response["samples"] = samples
        if analysis_error is not None:
            response["status"] = "partial_success"
            response["analysis_error"] = analysis_error
        elif analysis_skipped is not None:
            response["status"] = "empty_result"
            response["analysis_skipped"] = analysis_skipped
        else:
            response["status"] = "ok"

        if response["status"] == "empty_result" and isinstance(sparql_meta, dict):
            recovery_hint = {
                "strategy": "targeted_grounding_rerun",
                "max_samples": 1,
                "rerun_required": True,
            }
            if sparql_meta.get("mode") == "builder":
                recovery_hint["preferred_classes"] = [
                    value
                    for value in [sparql_meta.get("evidence_class"), sparql_meta.get("source_class")]
                    if isinstance(value, str) and value
                ]
            response["recovery_hint"] = recovery_hint

        if plan.get("include_schema"):
            response["schema"] = schema
            response["schema_included"] = True
        else:
            response["schema_summary"] = summarize_schema(schema)
            response["schema_included"] = False

        if profiles is not None:
            if plan.get("include_profiles"):
                response["profiles"] = profiles
                response["profiles_included"] = True
            else:
                response["profiles_summary"] = summarize_profiles(profiles)
                response["profiles_included"] = False

        presentation = build_run_presentation(
            plan,
            schema,
            sparql_response,
            analysis_response,
            analysis_meta,
            response["status"],
            analysis_error,
            analysis_skipped,
        )
        if presentation is not None:
            response["presentation"] = presentation

        if analysis_response is not None:
            if plan.get("include_analysis"):
                response["analysis"] = analysis_response
                response["analysis_included"] = True
            else:
                response["analysis"] = summarize_analysis_response(analysis_response)
                response["analysis_included"] = False

        return response

    try:
        return run_plan(http_request_json)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        try:
            return run_plan(curl_request_json)
        except RuntimeError as curl_exc:
            print(f"Run plan failed with HTTP {exc.code}; curl fallback failed: {curl_exc}", file=sys.stderr)
            if error_body:
                print(error_body, file=sys.stderr)
            raise SystemExit(1) from exc
        except SystemExit as curl_exit:
            if error_body:
                print(error_body, file=sys.stderr)
            if curl_exit.code in (502, 503, 504):
                raise SystemExit(1) from exc
            raise
    except urllib.error.URLError as exc:
        reason = str(exc.reason)
        try:
            return run_plan(curl_request_json)
        except RuntimeError as curl_exc:
            if "Operation not permitted" not in reason:
                print(f"Request failed: {exc}; curl fallback failed: {curl_exc}", file=sys.stderr)
                raise SystemExit(1) from exc
            with local_test_client() as client:
                return run_plan(lambda method, url, payload=None: client_request_json(client, method, url, payload))
        except SystemExit as curl_exit:
            if curl_exit.code in (502, 503, 504):
                print(f"Run plan curl fallback returned HTTP {curl_exit.code}", file=sys.stderr)
                raise SystemExit(1) from exc
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Client for the local OBDA reasoning server.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Reasoning server base URL.")
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_FILE),
        help="Path to the local protocol state file used to enforce schema-first access.",
    )
    parser.add_argument(
        "--schema-ttl-seconds",
        type=int,
        default=DEFAULT_SCHEMA_TTL_SECONDS,
        help="How long a prior schema call remains valid for protected commands.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="GET /health")
    schema_parser = subparsers.add_parser("schema", help="GET /schema")
    schema_parser.add_argument(
        "--full",
        action="store_true",
        help="Return the full /schema payload instead of the default compact summary.",
    )
    subparsers.add_parser("profiles", help="GET /analysis/profiles")
    subparsers.add_parser("templates", help="List supported run templates.")
    subparsers.add_parser("reload", help="POST /reload")

    sample_parser = subparsers.add_parser("sample", help="GET /sample/{class_name}")
    sample_parser.add_argument("class_name", help="Ontology class local name or URI.")
    sample_parser.add_argument("limit_arg", nargs="?", type=int, help="Optional positional sample limit.")
    sample_parser.add_argument("--limit", type=int, default=3, help="Number of samples to return.")

    causal_parser = subparsers.add_parser("causal", help="GET /causal/{customer_id}")
    causal_parser.add_argument("customer_id", help="Customer local id or full URI suffix.")

    sparql_parser = subparsers.add_parser("sparql", help="POST /sparql")
    sparql_group = sparql_parser.add_mutually_exclusive_group(required=False)
    sparql_group.add_argument("--query", help="SPARQL query string.")
    sparql_group.add_argument("--query-file", help="Path to a file containing the SPARQL query.")
    sparql_parser.add_argument("query_arg", nargs="?", help="Optional positional SPARQL query string.")

    run_parser = subparsers.add_parser("run", help="Execute schema-first workflow in one command.")
    run_group = run_parser.add_mutually_exclusive_group(required=False)
    run_group.add_argument(
        "--json",
        nargs="?",
        const="__AUTO__",
        help="Inline JSON run plan. With QUESTION + --template, a bare --json is treated as shorthand question mode.",
    )
    run_group.add_argument("--json-file", help="Path to a JSON run plan.")
    run_parser.add_argument("question", nargs="?", help="Optional natural-language question for shorthand question mode.")
    run_parser.add_argument("--template", choices=sorted(RUN_TEMPLATES), help="Template to use with QUESTION shorthand.")
    run_parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Return the semantic planner bundle without executing the selected plan.",
    )

    for endpoint in ("analysis-paths", "analysis-paths-batch", "analysis-neighborhood", "analysis-inferred-relations", "analysis-explain"):
        endpoint_parser = subparsers.add_parser(endpoint, help=f"POST /{endpoint.replace('-', '/')}")
        endpoint_group = endpoint_parser.add_mutually_exclusive_group(required=True)
        endpoint_group.add_argument("--json", help="Inline JSON payload.")
        endpoint_group.add_argument("--json-file", help="Path to a JSON payload file.")

    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")
    state_file = Path(args.state_file)
    ttl_seconds = args.schema_ttl_seconds

    if args.command == "health":
        print_output(request_json("GET", f"{base_url}/health"))
        return

    if args.command == "schema":
        schema = request_json("GET", f"{base_url}/schema")
        if args.full:
            print_output(schema)
        else:
            print_output({
                "schema_summary": summarize_schema(schema),
                "schema_included": False,
            })
        write_schema_state(state_file, base_url)
        return

    if args.command == "profiles":
        print_output(request_json("GET", f"{base_url}/analysis/profiles"))
        return

    if args.command == "templates":
        print_output({"templates": RUN_TEMPLATES})
        return

    if args.command == "reload":
        clear_schema_state(state_file)
        print_output(request_json("POST", f"{base_url}/reload"))
        return

    if args.command == "sample":
        require_schema_state(state_file, base_url, ttl_seconds, "sample")
        print("Protocol note: /sample is for grounding only, not for enumerating final answer sets.", file=sys.stderr)
        limit = args.limit_arg if args.limit_arg is not None else args.limit
        query = urllib.parse.urlencode({"limit": limit})
        url = f"{base_url}/sample/{urllib.parse.quote(args.class_name)}?{query}"
        print_output(request_json("GET", url))
        return

    if args.command == "causal":
        require_schema_state(state_file, base_url, ttl_seconds, "causal")
        url = f"{base_url}/causal/{urllib.parse.quote(args.customer_id)}"
        print_output(request_json("GET", url))
        return

    if args.command == "sparql":
        require_schema_state(state_file, base_url, ttl_seconds, "sparql")
        if args.query_file:
            query_text = Path(args.query_file).read_text(encoding="utf-8")
        elif args.query:
            query_text = args.query
        else:
            query_text = args.query_arg
        if not query_text:
            raise SystemExit("sparql requires --query, --query-file, or a positional query string.")
        print_output(request_json("POST", f"{base_url}/sparql", {"query": query_text}))
        return

    if args.command == "run":
        json_supplied = args.json not in (None, "__AUTO__")
        if (json_supplied or args.json_file) and args.question:
            raise SystemExit("Use either run --json/--json-file or run QUESTION --template, not both.")
        plan = load_json_payload(args.json, args.json_file)
        if is_question_routed_plan(plan):
            template = plan.get("template") or "custom"
            ignored_fields = [
                key for key in ("samples", "sparql", "analysis")
                if key in plan
            ]
            include_planner_debug = bool(plan.get("include_planner_debug"))
            json_plan_only = bool(plan.get("plan_only"))
            if args.plan_only or json_plan_only:
                response = build_question_mode_run_response(base_url, plan["question"], template, state_file)
            else:
                response = execute_question_mode_run(
                    base_url,
                    plan["question"],
                    template,
                    state_file,
                    include_planner_debug=include_planner_debug,
                )
            if ignored_fields:
                response = dict(response)
                response["question_mode_override_applied"] = True
                response["ignored_manual_fields"] = ignored_fields
                response["message"] = (
                    f"{response.get('message', '')} Manual fields {ignored_fields} were ignored because "
                    "a standard template with natural-language question must use locked question-mode execution."
                ).strip()
            print_output(response)
            return
        if is_question_shorthand_plan(plan):
            template = plan.get("template") or "custom"
            json_plan_only = bool(plan.get("plan_only"))
            if args.plan_only or json_plan_only:
                print_output(build_question_mode_run_response(base_url, plan["question"], template, state_file))
            else:
                print_output(execute_question_mode_run(base_url, plan["question"], template, state_file))
            return
        if plan is None and args.question:
            template = args.template or "custom"
            if args.plan_only:
                print_output(build_question_mode_run_response(base_url, args.question, template, state_file))
            else:
                print_output(execute_question_mode_run(base_url, args.question, template, state_file))
            return
        if plan is None:
            raise SystemExit("run requires --json/--json-file, or QUESTION with --template.")
        print_output(execute_run_plan(base_url, plan, state_file))
        return

    require_schema_state(state_file, base_url, ttl_seconds, args.command)
    payload = load_json_payload(args.json, args.json_file)
    if args.command == "analysis-paths" and isinstance(payload, dict) and payload.get("sources") and not payload.get("source"):
        args.command = "analysis-paths-batch"
    elif args.command == "analysis-paths-batch" and isinstance(payload, dict) and payload.get("source") and not payload.get("sources"):
        payload = dict(payload)
        payload["sources"] = [payload.pop("source")]
    endpoint_map = {
        "analysis-paths": "/analysis/paths",
        "analysis-paths-batch": "/analysis/paths/batch",
        "analysis-neighborhood": "/analysis/neighborhood",
        "analysis-inferred-relations": "/analysis/inferred-relations",
        "analysis-explain": "/analysis/explain",
    }
    print_output(request_json("POST", f"{base_url}{endpoint_map[args.command]}", payload))


if __name__ == "__main__":
    main()
