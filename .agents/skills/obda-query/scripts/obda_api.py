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
from obda_intent_parser import (
    parse_question_unit,
)
from obda_grounding_contracts import (
    build_grounding_bundle,
    grounding_candidates_for_slot,
    grounding_constraint_effective_text,
    grounding_constraint_record,
    grounding_constraint_requested_text,
    grounding_slot_binding_has_candidates,
    grounding_slot_bindings,
    grounding_slot_candidates_have_text_lowering,
    grounding_slot_input_for_name,
    grounding_slot_inputs,
    grounding_top_attribute_candidate_for_slot,
    grounding_top_value_candidate_for_slot,
)
from obda_grounding_policy import (
    abstract_status_slot_requires_high_confidence as policy_abstract_status_slot_requires_high_confidence,
    binding_terms_for_slot as policy_binding_terms_for_slot,
    grounded_slot_candidates as policy_grounded_slot_candidates,
    manifest_nodes_for_slot as policy_manifest_nodes_for_slot,
    node_catalog_source as policy_node_catalog_source,
    node_source_binding_adjustment as policy_node_source_binding_adjustment,
    relation_propagated_source_candidates as policy_relation_propagated_source_candidates,
    sample_value_candidate_allowed as policy_sample_value_candidate_allowed,
    slot_binding_candidates as policy_slot_binding_candidates,
    slot_binding_has_candidates as policy_slot_binding_has_candidates,
    slot_input_for_name as policy_slot_input_for_name,
    slot_input_requires_numeric_attribute_binding as policy_slot_input_requires_numeric_attribute_binding,
    slot_inputs_need_value_catalog as policy_slot_inputs_need_value_catalog,
    top_attribute_candidate_for_slot as policy_top_attribute_candidate_for_slot,
    top_value_candidate_for_slot as policy_top_value_candidate_for_slot,
)
from obda_ir_contracts import (
    build_intent_ir_from_policy,
    build_request_ir_record,
    constraint_snapshot_from_constraints,
    intent_ir_constraint_snapshot,
    intent_ir_focus_record,
    intent_ir_operator_list,
    intent_ir_operator_set,
    intent_ir_output_record,
    intent_ir_references_record,
    request_ir_anchor_forms,
    request_ir_effective_template,
    request_ir_output_record,
    request_ir_query_family,
    request_ir_references_record,
    request_ir_summary_record,
)
from obda_parser_contracts import attach_intent_irs_to_parser_output
from obda_planner_compiler import (
    build_node_plan as compiler_build_node_plan,
    build_semantic_request_ir as compiler_build_semantic_request_ir,
    select_compiled_plan as compiler_select_compiled_plan,
    summarize_planner_result as compiler_summarize_planner_result,
)
from obda_semantic_planner_runtime import (
    build_semantic_query_planner as runtime_build_semantic_query_planner,
)
from obda_question_mode_runtime import (
    build_question_batch_run_response as runtime_build_question_batch_run_response,
    build_question_mode_run_response as runtime_build_question_mode_run_response,
    execute_question_batch_run as runtime_execute_question_batch_run,
    execute_question_mode_run as runtime_execute_question_mode_run,
)
from obda_question_conversation_runtime import (
    apply_resolved_reference_to_slots as runtime_apply_resolved_reference_to_slots,
    build_conversation_state_entry as runtime_build_conversation_state_entry,
    extract_focus_refs_from_response as runtime_extract_focus_refs_from_response,
    resolve_reference_context as runtime_resolve_reference_context,
)
from obda_question_mode_single_runtime import (
    build_single_question_mode_run_response as runtime_build_single_question_mode_run_response,
    execute_single_question_mode_run as runtime_execute_single_question_mode_run,
)
from obda_run_plan_runtime import (
    execute_run_plan as runtime_execute_run_plan,
)
from obda_cli_command_runtime import (
    dispatch_cli_command,
    handle_analysis_endpoint_cli_command,
    handle_run_cli_command,
)
from obda_question_mode_contracts import (
    apply_bounded_recovery_contract_to_question_response,
    apply_fail_closed_contract_to_question_response,
)
from obda_lexical import (
    bootstrap_candidate_text,
    bootstrap_operator_hints,
    derive_bootstrap_signals,
    extract_which_tail,
    is_numeric_range_uri,
    register_bootstrap_candidate,
    register_bootstrap_operator_hint,
    split_constraint_terms,
)


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
MAPPING_VALUE_PLACEHOLDER_PATTERN = re.compile(r"\$\(([^)]+)\)")
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
        {"name": "status_or_problem_text", "allowed_node_types": ["attribute", "value"]},
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


def is_loopback_url(url: str) -> bool:
    """Whether the URL targets a local loopback host that should bypass proxies."""
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    hostname = (parsed.hostname or "").strip().lower()
    return hostname in {"127.0.0.1", "localhost", "::1"}


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
    if is_loopback_url(url):
        # Claude Code / hosted agent shells can inject proxy settings that break
        # localhost urllib traffic even when curl reaches the real server directly.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        response = opener.open(request)
    else:
        response = urllib.request.urlopen(request)
    with response:
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
    if is_loopback_url(url):
        command.extend(["--noproxy", "*"])

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


def mapping_template_variables(template: Any) -> List[str]:
    """Extract placeholder variable names from one mapping IRI/value template."""
    if not isinstance(template, str):
        return []
    return unique_preserve_order([
        match.group(1)
        for match in MAPPING_VALUE_PLACEHOLDER_PATTERN.finditer(template)
        if isinstance(match.group(1), str) and match.group(1)
    ])


def mapping_value_source_column(value_expr: Any) -> Optional[str]:
    """Extract the source column name from a simple mapping value like $(customer_id)."""
    variables = mapping_template_variables(value_expr)
    return variables[0] if len(variables) == 1 else None


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
    subject_variables: List[str] = []
    mapping_property_order = 0

    for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
        match = MAPPING_NAME_PATTERN.match(raw_line)
        if match:
            current_mapping = match.group(1)
            subject_template = None
            subject_class = None
            subject_variables = []
            mapping_property_order = 0
            continue

        match = MAPPING_SUBJECT_PATTERN.match(raw_line)
        if match:
            subject_template = match.group(1)
            subject_variables = mapping_template_variables(subject_template)
            continue

        match = MAPPING_TYPE_PATTERN.match(raw_line)
        if match:
            subject_class = uri_local_name(match.group(1)) or infer_class_local_name_from_uri_template(match.group(1))
            continue

        match = MAPPING_PO_ARRAY_PATTERN.match(raw_line)
        if not match:
            continue

        predicate_uri, object_value, object_type = match.groups()
        if predicate_uri == "a":
            continue
        local_name = uri_local_name(predicate_uri)
        domain_class = subject_class or infer_class_local_name_from_uri_template(subject_template)
        source_column = mapping_value_source_column(object_value)
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
            "mapping_order": mapping_property_order,
            "source_column": source_column,
            "subject_key": isinstance(source_column, str) and source_column in set(subject_variables),
        }
        mapping_property_order += 1

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


def runtime_participating_classes() -> Set[str]:
    """Return classes that participate in executable runtime mappings."""
    classes: Set[str] = set()
    for item in load_runtime_data_property_catalog().values():
        domain = uri_local_name(item.get("domain"))
        if isinstance(domain, str) and domain:
            classes.add(domain)
    for edges in load_runtime_object_property_catalog().values():
        for edge in edges:
            source_class = edge.get("source_class")
            target_class = edge.get("target_class")
            if isinstance(source_class, str) and source_class:
                classes.add(source_class)
            if isinstance(target_class, str) and target_class:
                classes.add(target_class)
    return classes


def class_catalog(schema: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return user-facing ontology classes that participate in executable runtime mappings."""
    if not isinstance(schema, dict):
        return []
    participating_classes = runtime_participating_classes()
    results = []
    for item in schema.get("classes", []):
        if not isinstance(item, dict):
            continue
        local_name = item.get("local_name")
        if not isinstance(local_name, str) or not local_name:
            continue
        if participating_classes and local_name not in participating_classes:
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
        for prop in merged_domain_properties(class_name, attributes_by_class):
            local_name = prop.get("local_name")
            if not isinstance(local_name, str) or not local_name:
                continue
            role_hints: List[str] = []
            attributes.append({
                "local_name": local_name,
                "label": prop.get("label") or local_name,
                "range": prop.get("range"),
                "numeric": is_numeric_data_property(prop),
                "validation_source": prop.get("validation_source", "schema"),
                "mapping_order": prop.get("mapping_order"),
                "source_column": prop.get("source_column"),
                "subject_key": bool(prop.get("subject_key")),
                "role_hints": role_hints,
            })
            attribute_node_id = f"{class_name}.{local_name}"
            if attribute_node_id not in seen_attribute_node_ids:
                seen_attribute_node_ids.add(attribute_node_id)
                attribute_nodes.append({
                    "node_type": "attribute",
                    "node_id": attribute_node_id,
                    "catalog_source": "manifest_attribute",
                    "class_name": class_name,
                    "local_name": local_name,
                    "label": prop.get("label") or local_name,
                    "range": prop.get("range"),
                    "numeric": is_numeric_data_property(prop),
                    "validation_source": prop.get("validation_source", "schema"),
                    "mapping_order": prop.get("mapping_order"),
                    "source_column": prop.get("source_column"),
                    "subject_key": bool(prop.get("subject_key")),
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
                "catalog_source": "manifest_class",
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
                "catalog_source": "manifest_relation",
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
    constraint_snapshot = intent_ir_constraint_snapshot(unit_intent_ir)
    first_anchor = None
    anchors = semantic_state.get("anchors", [])
    if isinstance(anchors, list):
        for anchor in anchors:
            if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
                first_anchor = anchor
                break

    target_text = first_nonempty_text(
        constraint_snapshot.get("target_text"),
        semantic_state.get("target_text"),
    )
    subject_text = first_nonempty_text(
        constraint_snapshot.get("result_hint"),
        semantic_state.get("result_hint"),
        target_text,
        question,
    )

    action_or_state_text = first_nonempty_text(
        constraint_snapshot.get("action_text"),
        semantic_state.get("action_text"),
    )

    cause_text = first_nonempty_text(
        constraint_snapshot.get("cause_text"),
        semantic_state.get("cause_text"),
    )
    status_or_problem_text = first_nonempty_text(
        constraint_snapshot.get("status_or_problem_text"),
        semantic_state.get("status_or_problem_text"),
    )
    status_numeric_constraint = constraint_snapshot.get("status_numeric_constraint")
    if not isinstance(status_numeric_constraint, dict):
        status_numeric_constraint = semantic_state.get("status_numeric_constraint")
    if not isinstance(status_numeric_constraint, dict):
        status_numeric_constraint = None

    values = {
        "subject_text": subject_text,
        "target_text": target_text,
        "anchor_text": first_anchor.get("value") if isinstance(first_anchor, dict) else None,
        "cause_text": cause_text,
        "action_or_state_text": action_or_state_text,
        "status_or_problem_text": (
            status_or_problem_text
            or target_text
            or action_or_state_text
            or cause_text
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
            if isinstance(status_numeric_constraint, dict):
                slot_input["comparison"] = {
                    "op": status_numeric_constraint.get("op"),
                    "value": status_numeric_constraint.get("value"),
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
        score = 4
        if not bool(node.get("numeric")):
            score += 1
        return score
    if slot_name in ("cause_text", "action_or_state_text", "status_or_problem_text") and node_type == "attribute":
        score = 2
        if slot_name == "status_or_problem_text" and slot_input.get("comparison") and bool(node.get("numeric")):
            score += 5
        return score
    if slot_name in ("cause_text", "action_or_state_text", "status_or_problem_text") and node_type == "value":
        return 3
    if slot_name == "target_text" and node_type == "relation":
        return 1
    return 0


def bind_semantic_slots(manifest: Optional[Dict[str, Any]], slot_inputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Bind abstract family slots to typed manifest nodes using hybrid-ready scoring."""
    bindings: List[Dict[str, Any]] = []
    for slot_input in slot_inputs:
        slot_text = slot_input.get("text")
        if not isinstance(slot_text, str) or not slot_text:
            continue
        candidates = []
        for node in policy_manifest_nodes_for_slot(manifest, slot_input):
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
            if not policy_sample_value_candidate_allowed(
                slot_input,
                node,
                float(lexical_score),
                float(semantic_similarity),
            ):
                continue
            source_score = policy_node_source_binding_adjustment(
                slot_input,
                node,
                float(lexical_score),
                float(semantic_similarity),
            )
            allow_anchor_capability_only = (
                slot_input.get("slot_name") == "anchor_text"
                and node.get("node_type") == "attribute"
                and role_score > 0
            )
            if lexical_score <= 0 and semantic_score <= 0 and source_score <= 0 and not allow_anchor_capability_only:
                continue
            total_score = lexical_score + role_score + semantic_score + source_score
            if total_score <= 0:
                continue
            candidates.append({
                "node_type": node.get("node_type"),
                "node_id": node.get("node_id"),
                "catalog_source": policy_node_catalog_source(node),
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
                "source_score": source_score,
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
    return policy_relation_propagated_source_candidates(manifest, anchor_only_bindings)


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

    for class_name, item in policy_relation_propagated_source_candidates(manifest, slot_bindings).items():
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
                    "catalog_source": "sample_value",
                    "class_name": class_name,
                    "property_local_name": prop_name,
                    "label": value_text,
                    "local_name": prop_name,
                    "role_hints": role_hints,
                    "numeric": bool(attr_node.get("numeric")) if isinstance(attr_node, dict) else False,
                    "validation_source": attr_node.get("validation_source") if isinstance(attr_node, dict) else "sample",
                    "mapping_order": attr_node.get("mapping_order") if isinstance(attr_node, dict) else None,
                    "source_column": attr_node.get("source_column") if isinstance(attr_node, dict) else None,
                    "subject_key": bool(attr_node.get("subject_key")) if isinstance(attr_node, dict) else False,
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


def semantic_state_text_for_slot(semantic_state: Dict[str, Any], slot_name: str) -> Optional[str]:
    """Map one planner slot name to its semantic-state text source."""
    if slot_name == "action_or_state_text":
        return first_nonempty_text(semantic_state.get("action_text"))
    if slot_name == "target_text":
        return first_nonempty_text(semantic_state.get("target_text"))
    return first_nonempty_text(semantic_state.get(slot_name))


def candidate_surface_label(candidate: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return a stable human-readable label for one bound manifest candidate."""
    if not isinstance(candidate, dict):
        return None
    label = candidate.get("label")
    if isinstance(label, str) and label.strip():
        return label.strip()

    local_name = (
        candidate.get("local_name")
        or candidate.get("property_local_name")
        or candidate.get("node_id")
    )
    if not isinstance(local_name, str) or not local_name.strip():
        return None

    cleaned = local_name.strip()
    class_name = candidate.get("class_name")
    if (
        candidate.get("node_type") == "attribute"
        and isinstance(class_name, str)
        and class_name
        and cleaned.startswith(f"{class_name}_")
    ):
        cleaned = cleaned[len(class_name) + 1:]
    return cleaned or None


def top_binding_candidate_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    preferred_node_types: Optional[List[str]] = None,
    slot_input: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Return the strongest overall candidate for one slot."""
    candidates = policy_grounded_slot_candidates(
        slot_bindings,
        slot_name,
        slot_input=slot_input,
        preferred_node_types=preferred_node_types,
    )
    if not candidates:
        return None
    return candidates[0]


def fallback_sample_value_binding_terms_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    top_candidate: Optional[Dict[str, Any]],
    *,
    slot_input: Optional[Dict[str, Any]] = None,
    preferred_node_types: Optional[List[str]] = None,
    limit: int = 5,
) -> List[str]:
    """Recover bounded near-tie sample-value labels when weak colloquial grounding has no manifest terms."""
    if not isinstance(top_candidate, dict):
        return []
    if top_candidate.get("node_type") != "value" or top_candidate.get("catalog_source") != "sample_value":
        return []
    top_label = top_candidate.get("label")
    if not isinstance(top_label, str) or not top_label.strip():
        return []
    top_score = float(top_candidate.get("total_score", 0.0) or 0.0)
    top_class = top_candidate.get("class_name")
    top_property = first_nonempty_text(
        top_candidate.get("property_local_name"),
        top_candidate.get("local_name"),
    )
    terms: List[str] = []
    for candidate in policy_grounded_slot_candidates(
        slot_bindings,
        slot_name,
        slot_input=slot_input,
        preferred_node_types=preferred_node_types,
    ):
        if candidate.get("node_type") != "value" or candidate.get("catalog_source") != "sample_value":
            continue
        if candidate.get("class_name") != top_class:
            continue
        candidate_property = first_nonempty_text(
            candidate.get("property_local_name"),
            candidate.get("local_name"),
        )
        if candidate_property != top_property:
            continue
        candidate_score = float(candidate.get("total_score", 0.0) or 0.0)
        if abs(candidate_score - top_score) > 0.01:
            continue
        label = candidate.get("label")
        if isinstance(label, str) and label.strip():
            terms.append(label)
        if len(terms) >= limit:
            break
    return unique_preserve_order([term for term in terms if isinstance(term, str) and term.strip()])[:limit]


def build_grounded_constraint_view(
    slot_inputs: List[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
    semantic_state: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Build one grounded slot view that separates requested semantics from bound semantics."""
    summaries: Dict[str, Dict[str, Any]] = {}
    for slot_name in ("cause_text", "action_or_state_text", "status_or_problem_text", "target_text"):
        slot_input = policy_slot_input_for_name(slot_inputs, slot_name)
        requested_text = semantic_state_text_for_slot(semantic_state, slot_name)
        preferred_node_types = None
        if slot_name in ("cause_text", "action_or_state_text"):
            preferred_node_types = ["value", "attribute"]
        top_candidate = top_binding_candidate_for_slot(
            slot_bindings,
            slot_name,
            preferred_node_types=preferred_node_types,
            slot_input=slot_input,
        )
        if policy_abstract_status_slot_requires_high_confidence(slot_input):
            binding_terms = []
            for candidate in policy_grounded_slot_candidates(
                slot_bindings,
                slot_name,
                slot_input=slot_input,
                preferred_node_types=preferred_node_types,
            )[:5]:
                label = candidate.get("label")
                if isinstance(label, str) and label:
                    binding_terms.append(label)
        else:
            binding_terms = policy_binding_terms_for_slot(
                slot_bindings,
                slot_name,
                preferred_node_types=preferred_node_types,
            )
            if not binding_terms:
                binding_terms = fallback_sample_value_binding_terms_for_slot(
                    slot_bindings,
                    slot_name,
                    top_candidate,
                    slot_input=slot_input,
                    preferred_node_types=preferred_node_types,
                )
        effective_text = first_nonempty_text(
            binding_terms[0] if binding_terms else None,
            candidate_surface_label(top_candidate),
            requested_text,
        )
        summaries[slot_name] = {
            "slot_name": slot_name,
            "requested_text": requested_text,
            "effective_text": effective_text,
            "has_binding": bool(top_candidate),
            "binding_terms": binding_terms,
            "top_candidate": deepcopy(top_candidate) if isinstance(top_candidate, dict) else None,
            "top_node_id": top_candidate.get("node_id") if isinstance(top_candidate, dict) else None,
            "top_node_type": top_candidate.get("node_type") if isinstance(top_candidate, dict) else None,
            "constraint_mode": slot_input.get("constraint_mode") if isinstance(slot_input, dict) else None,
            "comparison": deepcopy(slot_input.get("comparison")) if isinstance(slot_input, dict) else None,
        }
    return summaries


def grounded_constraint_terms(
    grounded_slot: Optional[Dict[str, Any]],
    schema: Optional[Dict[str, Any]],
) -> List[str]:
    """Derive stable lexical terms from a grounded slot summary."""
    if not isinstance(grounded_slot, dict):
        return []
    terms = []
    effective_text = grounded_slot.get("effective_text")
    requested_text = grounded_slot.get("requested_text")
    if isinstance(effective_text, str) and effective_text.strip():
        terms.extend(expand_constraint_terms(effective_text, schema))
    if (
        isinstance(requested_text, str)
        and requested_text.strip()
        and requested_text != effective_text
    ):
        terms.extend(expand_constraint_terms(requested_text, schema))
    binding_terms = grounded_slot.get("binding_terms")
    if isinstance(binding_terms, list):
        terms.extend(str(item) for item in binding_terms if isinstance(item, str) and item.strip())
    return unique_preserve_order([term for term in terms if term])


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

    if (
        allow_semantic_inheritance
        and inherited_state.get("status_check_requested")
    ):
        register_bootstrap_operator_hint(merged, "status_check", "inherited_context")
        inherited_keys.append("status_check_requested")

    if inherited_state.get("asks_solution"):
        register_bootstrap_operator_hint(merged, "remediation", "inherited_context")
        inherited_keys.append("asks_solution")

    merged["inherited_context_keys"] = unique_preserve_order(inherited_keys)
    sync_bootstrap_signals(merged)
    return merged


def first_nonempty_text(*values: Any) -> Optional[str]:
    """Return the first non-empty string value after trimming."""
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return None


def sync_bootstrap_signals(slots: Dict[str, Any]) -> None:
    """Keep legacy bootstrap_signals in sync as a compatibility output only."""
    if not isinstance(slots, dict):
        return
    slots["bootstrap_signals"] = derive_bootstrap_signals(slots)


def semantic_state_from_sources(
    slots: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Normalize semantic control bits from Intent IR first, with bootstrap candidates as fallback."""
    constraint_snapshot = intent_ir_constraint_snapshot(unit_intent_ir)
    intent_operator_list = intent_ir_operator_list(unit_intent_ir)
    intent_operators = intent_ir_operator_set(unit_intent_ir)
    bootstrap_operators = bootstrap_operator_hints(slots)
    resolved_operator_set = intent_operators or set(bootstrap_operators)
    focus = intent_ir_focus_record(unit_intent_ir)
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

    references = intent_ir_references_record(unit_intent_ir)
    resolved_reference = None
    if isinstance(references, dict) and isinstance(references.get("resolved"), dict):
        resolved_reference = deepcopy(references.get("resolved"))
    elif isinstance(slots.get("resolved_reference"), dict):
        resolved_reference = deepcopy(slots.get("resolved_reference"))

    return {
        "constraint_snapshot": constraint_snapshot,
        "operators": (
            list(intent_operator_list)
            if intent_operator_list
            else list(bootstrap_operators)
        ),
        "operator_set": resolved_operator_set,
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
        "status_check_requested": "status_check" in resolved_operator_set or isinstance(numeric_constraint, dict),
        "asks_solution": "remediation" in resolved_operator_set,
        "asks_explanation": "explain" in resolved_operator_set,
        "resolved_reference": resolved_reference,
    }


def build_intent_policy(
    slots: Dict[str, Any],
    template: str,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
    question_unit: Optional[Dict[str, Any]] = None,
    resolved_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build one canonical intent policy shared by IR construction, routing, and inheritance."""
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    dependency = (
        question_unit.get("dependency")
        if isinstance(question_unit, dict) and isinstance(question_unit.get("dependency"), dict)
        else None
    )
    question_unit_markers = (
        list(question_unit.get("reference_markers", []))
        if isinstance(question_unit, dict) and isinstance(question_unit.get("reference_markers"), list)
        else list(slots.get("reference_markers", []))
        if isinstance(slots.get("reference_markers"), list)
        else []
    )

    focus = intent_ir_focus_record(unit_intent_ir)
    if isinstance(focus, dict) and isinstance(focus.get("kind"), str) and focus.get("kind"):
        focus_kind = focus.get("kind")
    else:
        if semantic_state.get("has_anchor"):
            focus_kind = "anchored_entity"
            focus = {"kind": "anchored_entity", "anchors": deepcopy(semantic_state.get("anchors", []))}
        elif dependency or question_unit_markers:
            focus_kind = "previous_result_reference"
            focus = {"kind": "previous_result_reference"}
        elif template in ("enumeration", "causal_enumeration"):
            focus_kind = "result_set"
            focus = {"kind": "result_set"}
        else:
            focus_kind = "implicit"
            focus = {"kind": "implicit"}

    constraints = (
        deepcopy(unit_intent_ir.get("constraints"))
        if isinstance(unit_intent_ir, dict) and isinstance(unit_intent_ir.get("constraints"), list)
        else None
    )
    if constraints is None:
        constraints = []
        for key in ("cause_text", "action_text", "status_or_problem_text", "target_text", "result_hint"):
            value = first_nonempty_text(bootstrap_candidate_text(slots, key), semantic_state.get(key))
            if isinstance(value, str) and value.strip():
                constraints.append({"slot": key, "text": value})
        if isinstance(semantic_state.get("status_numeric_constraint"), dict):
            constraints.append({
                "slot": "status_numeric_constraint",
                "constraint": deepcopy(semantic_state["status_numeric_constraint"]),
            })
    constraint_snapshot = constraint_snapshot_from_constraints(constraints)
    cause_constraint_text = first_nonempty_text(
        constraint_snapshot.get("cause_text"),
        semantic_state.get("cause_text"),
    )
    action_constraint_text = first_nonempty_text(
        constraint_snapshot.get("action_text"),
        semantic_state.get("action_text"),
    )
    status_constraint_text = first_nonempty_text(
        constraint_snapshot.get("status_or_problem_text"),
        semantic_state.get("status_or_problem_text"),
    )
    target_constraint_text = first_nonempty_text(
        constraint_snapshot.get("target_text"),
        semantic_state.get("target_text"),
    )
    result_hint_text = first_nonempty_text(
        constraint_snapshot.get("result_hint"),
        semantic_state.get("result_hint"),
    )
    status_numeric_constraint = constraint_snapshot.get("status_numeric_constraint")
    if not isinstance(status_numeric_constraint, dict):
        status_numeric_constraint = semantic_state.get("status_numeric_constraint")
    if not isinstance(status_numeric_constraint, dict):
        status_numeric_constraint = None
    template_enumeration_fallback_allowed = (
        template in ("enumeration", "causal_enumeration")
        and not (semantic_state.get("has_anchor") and semantic_state.get("status_check_requested"))
    )

    operators = intent_ir_operator_list(unit_intent_ir)
    if not operators:
        if semantic_state.get("status_check_requested"):
            operators.append("status_check")
        if semantic_state.get("asks_explanation") or isinstance(cause_constraint_text, str):
            operators.append("explain")
        if semantic_state.get("asks_solution"):
            operators.append("remediation")
        if template_enumeration_fallback_allowed:
            operators.append("enumerate")
        if not operators:
            operators.append("lookup")
    operators = unique_preserve_order([str(item) for item in operators if isinstance(item, str) and item])

    output = intent_ir_output_record(unit_intent_ir)
    if not isinstance(output, dict):
        if template == "causal_enumeration" and template_enumeration_fallback_allowed:
            output_shape = "entity_set"
        elif template == "enumeration" and template_enumeration_fallback_allowed:
            output_shape = "rows"
        else:
            output_shape = "entity"
        output = {"shape": output_shape}
    output_shape = (
        output.get("shape")
        if isinstance(output.get("shape"), str) and output.get("shape")
        else None
    )

    references = intent_ir_references_record(unit_intent_ir)
    if not isinstance(references, dict):
        references = {
            "markers": question_unit_markers,
        }
        if isinstance(dependency, dict):
            references["depends_on"] = dependency.get("depends_on")
            references["condition"] = dependency.get("condition")
    reference_markers = (
        list(references.get("markers", []))
        if isinstance(references.get("markers"), list)
        else question_unit_markers
    )
    if "markers" not in references:
        references["markers"] = reference_markers
    if isinstance(dependency, dict):
        references.setdefault("depends_on", dependency.get("depends_on"))
        references.setdefault("condition", dependency.get("condition"))
    if not isinstance(references.get("resolved"), dict):
        fallback_reference = resolved_reference if isinstance(resolved_reference, dict) else semantic_state.get("resolved_reference")
        if isinstance(fallback_reference, dict) and not semantic_state.get("has_explicit_anchor"):
            references["resolved"] = deepcopy(fallback_reference)

    dependency_condition = (
        references.get("condition")
        if isinstance(references.get("condition"), str) and references.get("condition")
        else None
    )
    has_semantic_content = any(
        isinstance(value, str) and value.strip()
        for value in (
            cause_constraint_text,
            action_constraint_text,
            status_constraint_text,
            target_constraint_text,
            result_hint_text,
        )
    ) or isinstance(status_numeric_constraint, dict)
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
    has_explanation_phenomenon = bool(action_constraint_text)
    has_causal_constraints = bool(cause_constraint_text or status_constraint_text)
    has_status_constraints = bool(status_constraint_text or isinstance(status_numeric_constraint, dict))
    has_target_constraint = bool(target_constraint_text)
    generic_explanation_target = False
    previous_result_explanation = focus_kind == "previous_result_reference" and enumerate_requested and explain_requested
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
        elif (
            focus_kind != "anchored_entity"
            and enumerate_requested
            and explain_requested
            and (previous_result_explanation or not has_causal_constraints)
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
        elif (
            enumerate_requested
            and explain_requested
            and (generic_explanation_target or has_explanation_phenomenon or has_target_constraint)
        ):
            family_bias = "explanation_enumeration"
            family_rationale.append("explanation_operator")
        else:
            family_bias = "enumeration"

    return {
        "semantic_state": semantic_state,
        "constraint_snapshot": constraint_snapshot,
        "focus": focus,
        "focus_kind": focus_kind,
        "operators": operators,
        "constraints": constraints,
        "output": output,
        "output_shape": output_shape,
        "references": references,
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


def build_bootstrap_intent_view(
    slots: Dict[str, Any],
    template: str,
    question_unit: Optional[Dict[str, Any]] = None,
    resolved_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a shared bootstrap intent view before a full Intent IR exists."""
    policy = build_intent_policy(
        slots,
        template,
        question_unit=question_unit,
        resolved_reference=resolved_reference,
    )
    return {
        "focus": deepcopy(policy.get("focus", {})),
        "focus_kind": policy.get("focus_kind"),
        "operators": list(policy.get("operators", [])),
        "constraints": deepcopy(policy.get("constraints", [])),
        "output": deepcopy(policy.get("output", {})),
        "output_shape": policy.get("output_shape"),
        "references": deepcopy(policy.get("references", {})),
    }


def derive_intent_profile(
    slots: Dict[str, Any],
    template: str,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a minimal profile that lets routing depend on Intent IR before templates."""
    policy = build_intent_policy(slots, template, unit_intent_ir=unit_intent_ir, question_unit=question_unit)
    return {
        "operators": list(policy.get("operators", [])),
        "focus_kind": policy.get("focus_kind"),
        "output_shape": policy.get("output_shape"),
        "scope_inheritance_allowed": bool(policy.get("scope_inheritance_allowed")),
        "semantic_inheritance_allowed": bool(policy.get("semantic_inheritance_allowed")),
        "reference_binding_allowed": bool(policy.get("reference_binding_allowed")),
        "explain_requested": bool(policy.get("explain_requested")),
        "enumerate_requested": bool(policy.get("enumerate_requested")),
        "has_explanation_phenomenon": bool(policy.get("has_explanation_phenomenon")),
        "has_causal_constraints": bool(policy.get("has_causal_constraints")),
        "has_target_constraint": bool(policy.get("has_target_constraint")),
        "generic_explanation_target": bool(policy.get("generic_explanation_target")),
        "family_bias": policy.get("family_bias"),
        "effective_template_bias": policy.get("effective_template_bias"),
        "family_rationale": list(policy.get("family_rationale", [])),
    }


def build_question_unit_intent_ir(
    unit: Dict[str, Any],
    slots: Dict[str, Any],
    template: str,
    resolved_reference: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a lightweight Intent IR for one QuestionUnit."""
    policy = build_intent_policy(
        slots,
        template,
        question_unit=unit,
        resolved_reference=resolved_reference,
    )

    intent_ir = build_intent_ir_from_policy(unit, policy)
    parser_output = slots.get("parser_output")
    if isinstance(parser_output, dict):
        attach_intent_irs_to_parser_output(parser_output, [intent_ir])
    return intent_ir


def extract_question_slots(
    question: str,
    template: str,
    inherited_context: Optional[Dict[str, Any]] = None,
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Extract a small set of semantic slots from a natural-language question."""
    parse_bundle = parse_question_unit(question, template, question_unit=question_unit)
    slots = dict(parse_bundle.get("slots", {}))
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


def property_value_family(prop: Dict[str, Any]) -> str:
    """Classify one data property by runtime/value shape instead of lexical hints."""
    range_uri = prop.get("range")
    if is_numeric_range_uri(range_uri):
        return "numeric"
    if isinstance(range_uri, str):
        lowered = range_uri.lower()
        if "bool" in lowered:
            return "boolean"
        if "date" in lowered or "time" in lowered:
            return "temporal"
    return "text"


def merged_domain_properties(
    domain_class: str,
    domain_properties: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Merge schema/runtime views of one class's properties without dropping stronger schema metadata."""
    merged: Dict[str, Dict[str, Any]] = {}
    for prop in domain_properties.get(domain_class, []):
        if not isinstance(prop, dict):
            continue
        local_name = prop.get("local_name")
        if not isinstance(local_name, str) or not local_name:
            continue
        existing = merged.get(local_name, {})
        updated = dict(existing)
        for key, value in prop.items():
            if value is None:
                continue
            if isinstance(value, str) and not value:
                continue
            updated[key] = value
        merged[local_name] = updated
    return sorted(
        merged.values(),
        key=lambda item: (
            int(item.get("mapping_order", 10**9) or 10**9),
            str(item.get("local_name", "")),
        ),
    )


def property_sample_stats_by_local_name(
    domain_class: str,
    manifest: Optional[Dict[str, Any]],
) -> Dict[str, Dict[str, float]]:
    """Build lightweight per-property sample stats from manifest value nodes."""
    if not isinstance(manifest, dict):
        return {}
    buckets: Dict[str, Dict[str, Any]] = {}
    for node in manifest.get("value_nodes", []):
        if not isinstance(node, dict):
            continue
        if node.get("class_name") != domain_class:
            continue
        local_name = node.get("property_local_name") or node.get("local_name")
        label = node.get("label")
        if not isinstance(local_name, str) or not local_name:
            continue
        if label in (None, ""):
            continue
        value_text = str(label).strip()
        if not value_text:
            continue
        bucket = buckets.setdefault(local_name, {
            "sample_count": 0,
            "distinct_values": set(),
            "total_length": 0.0,
        })
        bucket["sample_count"] += 1
        bucket["distinct_values"].add(value_text)
        bucket["total_length"] += float(len(value_text))

    stats: Dict[str, Dict[str, float]] = {}
    for local_name, bucket in buckets.items():
        sample_count = int(bucket.get("sample_count", 0) or 0)
        if sample_count <= 0:
            continue
        distinct_count = len(bucket.get("distinct_values", set()))
        total_length = float(bucket.get("total_length", 0.0) or 0.0)
        stats[local_name] = {
            "sample_count": float(sample_count),
            "distinct_count": float(distinct_count),
            "distinct_ratio": float(distinct_count) / float(sample_count),
            "avg_length": total_length / float(sample_count),
        }
    return stats


def score_property_for_role(
    prop: Dict[str, Any],
    role: str,
    *,
    text_rank: Optional[int],
    sample_stats: Optional[Dict[str, float]],
) -> float:
    """Score one property for a structural projection role without lexical phrase tables."""
    local_name = prop.get("local_name")
    if not isinstance(local_name, str) or not local_name:
        return float("-inf")

    family = property_value_family(prop)
    mapping_order = int(prop.get("mapping_order", 10**9) or 10**9)
    subject_key = bool(prop.get("subject_key"))
    sample_count = float(sample_stats.get("sample_count", 0.0) or 0.0) if isinstance(sample_stats, dict) else 0.0
    distinct_ratio = float(sample_stats.get("distinct_ratio", 1.0) or 1.0) if isinstance(sample_stats, dict) else 1.0
    avg_length = float(sample_stats.get("avg_length", 0.0) or 0.0) if isinstance(sample_stats, dict) else 0.0

    score = 0.0
    score += max(0.0, 6.0 - min(float(mapping_order), 30.0) * 0.2)
    score += min(3.0, sample_count * 0.5)

    if role == "id":
        if subject_key:
            score += 20.0
        if family == "text":
            score += 2.0
        return score

    if role == "score":
        if family == "numeric":
            score += 18.0
        if subject_key:
            score -= 10.0
        return score

    if family != "text":
        return float("-inf")
    if subject_key:
        score -= 8.0

    if text_rank is not None:
        score += max(0.0, 4.0 - float(text_rank))

    if role == "name":
        score += 10.0 * distinct_ratio
        score += max(0.0, 4.0 - min(avg_length, 40.0) / 10.0)
        return score

    if role == "type":
        score += 8.0 * (1.0 - min(distinct_ratio, 1.0))
        score += max(0.0, 3.5 - min(avg_length, 35.0) / 10.0)
        return score

    if role == "description":
        score += min(avg_length, 60.0) / 4.0
        if text_rank is not None and text_rank > 0:
            score += 2.0
        return score

    if role == "status":
        score += 7.0 * (1.0 - min(distinct_ratio, 1.0))
        if 2.0 <= avg_length <= 20.0:
            score += 3.0
        return score

    return float("-inf")


def best_role_property(
    domain_class: str,
    role: str,
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Optional[Dict[str, str]] = None,
    manifest: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Pick a property from manifest/runtime structure without role phrase tables."""
    del class_labels
    properties = merged_domain_properties(domain_class, domain_properties)
    if not properties:
        return None

    subject_key_props = [
        prop for prop in properties
        if bool(prop.get("subject_key")) and isinstance(prop.get("local_name"), str) and prop.get("local_name")
    ]
    text_props = [
        prop for prop in properties
        if property_value_family(prop) == "text"
        and not bool(prop.get("subject_key"))
        and isinstance(prop.get("local_name"), str)
        and prop.get("local_name")
    ]
    numeric_props = [
        prop for prop in properties
        if property_value_family(prop) == "numeric"
        and isinstance(prop.get("local_name"), str)
        and prop.get("local_name")
    ]

    leading_text_props: List[Dict[str, Any]] = []
    entered_text_block = False
    exited_text_block = False
    for prop in properties:
        if bool(prop.get("subject_key")):
            continue
        if property_value_family(prop) == "text":
            if not exited_text_block:
                leading_text_props.append(prop)
                entered_text_block = True
            continue
        if entered_text_block:
            exited_text_block = True

    descriptive_text_props = leading_text_props or text_props
    sample_stats_by_name = property_sample_stats_by_local_name(domain_class, manifest)

    if role in {"id", "name", "type", "description", "status", "score"}:
        scored: List[tuple[float, str]] = []
        text_rank_by_name = {
            prop.get("local_name"): index
            for index, prop in enumerate(descriptive_text_props)
            if isinstance(prop.get("local_name"), str) and prop.get("local_name")
        }
        for prop in properties:
            local_name = prop.get("local_name")
            if not isinstance(local_name, str) or not local_name:
                continue
            role_score = score_property_for_role(
                prop,
                role,
                text_rank=text_rank_by_name.get(local_name),
                sample_stats=sample_stats_by_name.get(local_name),
            )
            if role_score == float("-inf"):
                continue
            scored.append((role_score, local_name))
        if scored:
            scored.sort(key=lambda item: (-item[0], item[1]))
            return scored[0][1]

    if role == "id":
        return subject_key_props[0].get("local_name") if subject_key_props else None
    if role in {"name", "type"}:
        return descriptive_text_props[0].get("local_name") if descriptive_text_props else None
    if role == "description":
        if len(descriptive_text_props) >= 2:
            return descriptive_text_props[1].get("local_name")
        return descriptive_text_props[0].get("local_name") if descriptive_text_props else None
    if role == "status":
        if len(descriptive_text_props) >= 3:
            return descriptive_text_props[2].get("local_name")
        if len(descriptive_text_props) >= 2:
            return descriptive_text_props[1].get("local_name")
        return descriptive_text_props[0].get("local_name") if descriptive_text_props else None
    if role == "score":
        return numeric_props[0].get("local_name") if numeric_props else None
    return None


def build_detail_projection_properties(
    class_name: str,
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Dict[str, str],
    manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Build display projection properties using one manifest/profile snapshot."""
    projection_properties: Dict[str, str] = {}
    for role, var_name in (
        ("id", "detailId"),
        ("name", "detailName"),
        ("type", "detailType"),
        ("description", "detailDescription"),
        ("status", "detailStatus"),
        ("score", "detailScore"),
    ):
        property_name = best_role_property(
            class_name,
            role,
            domain_properties,
            class_labels,
            manifest=manifest,
        )
        if isinstance(property_name, str) and property_name:
            projection_properties[var_name] = property_name
    return projection_properties


def detail_projection_needs_sample_support(
    class_name: str,
    projection_properties: Dict[str, str],
    domain_properties: Dict[str, List[Dict[str, Any]]],
    manifest: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return whether detail projection is too weak to rely on manifest-only structure."""
    if property_sample_stats_by_local_name(class_name, manifest):
        return False

    properties = merged_domain_properties(class_name, domain_properties)
    text_candidates = [
        prop for prop in properties
        if property_value_family(prop) == "text"
        and not bool(prop.get("subject_key"))
        and isinstance(prop.get("local_name"), str)
        and prop.get("local_name")
    ]
    numeric_candidates = [
        prop for prop in properties
        if property_value_family(prop) == "numeric"
        and isinstance(prop.get("local_name"), str)
        and prop.get("local_name")
    ]
    if not text_candidates and not numeric_candidates:
        return False

    non_id_projection_props = [
        property_name
        for var_name, property_name in projection_properties.items()
        if var_name != "detailId" and isinstance(property_name, str) and property_name
    ]
    unique_non_id_props = set(non_id_projection_props)

    if not non_id_projection_props:
        return len(text_candidates) >= 2 or bool(numeric_candidates)
    if len(unique_non_id_props) < min(len(non_id_projection_props), 2):
        return len(text_candidates) >= 2 or bool(numeric_candidates)
    if len(non_id_projection_props) < 2 and len(text_candidates) >= 3:
        return True
    return False


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

    return unique_preserve_order([term for term in expanded if term])


def choose_source_class_candidate(
    question: str,
    manifest: Optional[Dict[str, Any]],
    slots: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Choose the most likely source class using structural fit first and lexical fit second."""
    question_text = normalize_match_text(question)
    tail_text = normalize_match_text(extract_which_tail(question))
    domain_properties = manifest_attributes_by_class(manifest)
    relations_by_source = manifest.get("relations_by_source", {}) if isinstance(manifest, dict) else {}
    semantic_state = semantic_state_from_sources(slots or {}, unit_intent_ir)
    anchors = semantic_state.get("anchors", []) if isinstance(semantic_state, dict) else []
    reference_entity_class = slots.get("reference_entity_class") if isinstance(slots, dict) else None

    candidates = []
    for item in manifest.get("classes", []) if isinstance(manifest, dict) else []:
        class_item = {
            "label": item.get("label"),
            "local_name": item.get("class_name"),
        }
        class_name = item.get("class_name")
        if not isinstance(class_name, str) or not class_name:
            continue
        props = merged_domain_properties(class_name, domain_properties)
        text_prop_count = sum(1 for prop in props if property_value_family(prop) == "text")
        numeric_prop_count = sum(1 for prop in props if property_value_family(prop) == "numeric")
        outgoing_relations = relations_by_source.get(class_name, [])

        score = 0.0
        if isinstance(reference_entity_class, str) and reference_entity_class == class_name:
            score += 20.0
        if any(
            isinstance(anchor, dict)
            and isinstance(anchor.get("class_hint"), str)
            and anchor.get("class_hint") == class_name
            for anchor in anchors
        ):
            score += 16.0

        if semantic_state.get("status_check_requested"):
            if isinstance(semantic_state.get("status_numeric_constraint"), dict):
                if numeric_prop_count:
                    score += 6.0
            elif text_prop_count or numeric_prop_count:
                score += 2.5

        if first_nonempty_text(semantic_state.get("cause_text"), semantic_state.get("action_text")):
            if outgoing_relations:
                score += min(4.0, 1.0 + float(len(outgoing_relations)) * 0.75)
            if text_prop_count:
                score += 1.5

        if semantic_state.get("asks_explanation") or semantic_state.get("asks_solution"):
            if outgoing_relations:
                score += min(3.0, 0.75 * float(len(outgoing_relations)))

        lexical_score = class_match_score(question_text, tail_text, class_item)
        score += min(2.0, float(lexical_score) * 0.2)
        if score <= 0:
            continue
        candidates.append({
            "class_name": class_name,
            "label": item.get("label") or class_name,
            "score": score,
            "score_breakdown": {
                "text_prop_count": text_prop_count,
                "numeric_prop_count": numeric_prop_count,
                "outgoing_relation_count": len(outgoing_relations),
                "lexical_bonus": min(2.0, float(lexical_score) * 0.2),
            },
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
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Choose the source class, starting from structural fit and boosting explicit anchors."""
    source_info = choose_source_class_candidate(
        question,
        manifest,
        slots,
        unit_intent_ir=unit_intent_ir,
    )

    anchors = slots.get("anchors", [])
    if not isinstance(anchors, list) or not anchors:
        return source_info

    class_index = manifest.get("class_index", {}) if isinstance(manifest, dict) else {}
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


def choose_evidence_class_candidates(
    source_class: str,
    manifest: Optional[Dict[str, Any]],
    slots: Dict[str, Any],
    unit_intent_ir: Optional[Dict[str, Any]] = None,
    grounding_bundle: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Choose plausible evidence classes using manifest structure first, lexical text second."""
    relations = manifest.get("relations", []) if isinstance(manifest, dict) else []
    domain_properties = manifest_attributes_by_class(manifest)
    class_labels = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(manifest, dict) and isinstance(item, dict) and item.get("class_name")
    }
    semantic_state = semantic_state_from_sources(slots, unit_intent_ir)
    question_text = normalize_match_text(slots.get("question"))
    reference_evidence_classes = {
        item
        for item in slots.get("reference_evidence_classes", [])
        if isinstance(item, str) and item
    }
    cause_tokens = expand_constraint_terms(
        semantic_state.get("cause_text"),
        None,
    )
    action_tokens = expand_constraint_terms(
        semantic_state.get("action_text"),
        None,
    )
    target_text = first_nonempty_text(semantic_state.get("target_text"))
    status_numeric_constraint = semantic_state.get("status_numeric_constraint")

    def property_family_counts(class_name: str) -> Dict[str, int]:
        counts = {"text": 0, "numeric": 0, "boolean": 0, "temporal": 0}
        for prop in domain_properties.get(class_name, []):
            if not isinstance(prop, dict):
                continue
            family = property_value_family(prop)
            counts[family] = counts.get(family, 0) + 1
        return counts

    def evidence_text_profile_bonus(class_name: str) -> float:
        """Prefer evidence classes that expose stable type/description text roles for causal filtering."""
        type_prop = best_role_property(class_name, "type", domain_properties)
        desc_prop = best_role_property(class_name, "description", domain_properties)
        contact_markers = ("phone", "mobile", "手机号", "电话", "联系方式", "contact")

        def prop_quality(local_name: Optional[str]) -> float:
            if not isinstance(local_name, str) or not local_name:
                return 0.0
            compact = normalize_match_text(local_name).replace(" ", "")
            if compact.endswith(("id", "编号", "编码", "代码", "标识", "序号")):
                return -0.5
            if any(marker in compact for marker in contact_markers):
                return -0.5
            return 0.25

        score = prop_quality(type_prop) + prop_quality(desc_prop)
        if type_prop and desc_prop and type_prop != desc_prop and score > 0:
            score += 1.0
        return score

    def grounded_constraint_bonus(class_name: str) -> float:
        def top_candidate_supports_class_bonus(candidate: Optional[Dict[str, Any]]) -> bool:
            if not isinstance(candidate, dict):
                return False
            node_type = candidate.get("node_type")
            if node_type == "attribute":
                return not bool(candidate.get("numeric"))
            if node_type != "value":
                return False
            catalog_source = candidate.get("catalog_source")
            total_score = float(candidate.get("total_score", 0.0) or 0.0)
            lexical_score = float(candidate.get("lexical_score", 0.0) or 0.0)
            semantic_similarity = float(candidate.get("semantic_similarity", 0.0) or 0.0)
            if catalog_source == "sample_value":
                return total_score >= 8.0 and (lexical_score >= 4.0 or semantic_similarity >= 0.2)
            return total_score >= 4.0

        if not isinstance(grounding_bundle, dict):
            return 0.0
        score = 0.0
        for slot_name, top_bonus, candidate_bonus in (
            ("status_or_problem_text", 4.0, 1.5),
            ("target_text", 3.0, 1.25),
            ("cause_text", 2.5, 1.0),
            ("action_or_state_text", 2.5, 1.0),
        ):
            grounded = grounding_constraint_record(grounding_bundle, slot_name)
            top_candidate = grounded.get("top_candidate")
            if (
                isinstance(top_candidate, dict)
                and isinstance(top_candidate.get("class_name"), str)
                and top_candidate.get("class_name") == class_name
            ):
                score += top_bonus if top_candidate_supports_class_bonus(top_candidate) else candidate_bonus
                continue
            if any(
                isinstance(candidate, dict)
                and candidate.get("class_name") == class_name
                for candidate in grounding_candidates_for_slot(grounding_bundle, slot_name)
            ):
                score += candidate_bonus
        return score

    candidates: Dict[str, Dict[str, Any]] = {}
    for relation in relations:
        if relation["source_class"] != source_class:
            continue
        evidence_class = relation["target_class"]
        label = class_labels.get(evidence_class, evidence_class)
        score = 0.0
        if relation.get("validation_source") == "mapping":
            score += 2.0
        else:
            score += 0.5
        if evidence_class in reference_evidence_classes:
            score += 6.0

        family_counts = property_family_counts(evidence_class)
        if family_counts.get("text", 0):
            score += min(2.0, 0.5 * float(family_counts["text"]))
        if isinstance(status_numeric_constraint, dict) and family_counts.get("numeric", 0):
            score += 3.0
        elif semantic_state.get("status_check_requested") and family_counts.get("text", 0):
            score += 1.5
        if cause_tokens and family_counts.get("text", 0):
            score += 1.5
        if action_tokens and family_counts.get("text", 0):
            score += 1.5
        if (cause_tokens or action_tokens) and family_counts.get("text", 0):
            score += evidence_text_profile_bonus(evidence_class)
        if isinstance(target_text, str) and target_text and (family_counts.get("text", 0) or family_counts.get("numeric", 0)):
            score += 1.0

        score += grounded_constraint_bonus(evidence_class)

        normalized_label = normalize_match_text(label)
        if normalized_label and normalized_label in question_text:
            score += 1.0

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
    return is_numeric_range_uri(prop.get("range"))


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
    support_props = matching_action_support_properties(
        action_class,
        action_terms,
        domain_properties,
    )
    text_props = unique_preserve_order(
        [name for name in support_props.get("text", []) if isinstance(name, str) and name]
    )
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
    grounding_bundle: Dict[str, Any],
    evidence_class: str,
    manifest: Optional[Dict[str, Any]],
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Dict[str, str],
    prefer_explanation: bool = False,
    allow_generic_explanation_projection: bool = False,
) -> Optional[Dict[str, Any]]:
    """Choose a stable evidence property to enumerate as the output value."""
    attribute_candidate = grounding_top_attribute_candidate_for_slot(
        grounding_bundle,
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
        value_candidate = grounding_top_value_candidate_for_slot(
            grounding_bundle,
            "target_text",
            class_name=evidence_class,
        )
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
                value_property = best_role_property(
                    evidence_class,
                    role,
                    domain_properties,
                    class_labels,
                    manifest=manifest,
                )
                if isinstance(value_property, str) and value_property:
                    break
            if not isinstance(value_property, str) or not value_property:
                return None
            rationale = (
                ["target_explanation_projection"]
                if allow_generic_explanation_projection and prefer_explanation
                else ["target_role_fallback"]
            )

    description_property = best_role_property(
        evidence_class,
        "description",
        domain_properties,
        class_labels,
        manifest=manifest,
    )
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


def _semantic_planner_runtime_callbacks() -> Dict[str, Any]:
    """Return the repo-owned callback set for the semantic planner runtime."""
    return {
        "with_semantic_vector_index": with_semantic_vector_index,
        "build_semantic_manifest": build_semantic_manifest,
        "extract_question_slots": extract_question_slots,
        "build_question_unit_intent_ir": build_question_unit_intent_ir,
        "semantic_state_from_sources": semantic_state_from_sources,
        "route_query_family": route_query_family,
        "build_family_slot_inputs": build_family_slot_inputs,
        "choose_source_class_candidate_with_anchors": choose_source_class_candidate_with_anchors,
        "bind_semantic_slots": bind_semantic_slots,
        "merge_source_candidates_from_slot_bindings": merge_source_candidates_from_slot_bindings,
        "choose_evidence_class_candidates": choose_evidence_class_candidates,
        "rank_value_catalog_classes": rank_value_catalog_classes,
        "slot_inputs_need_value_catalog": policy_slot_inputs_need_value_catalog,
        "load_sample_value_nodes": load_sample_value_nodes,
        "with_value_nodes": with_value_nodes,
        "build_grounded_constraint_view": build_grounded_constraint_view,
        "build_explicit_metric_clarification_hint": build_explicit_metric_clarification_hint,
        "manifest_attributes_by_class": manifest_attributes_by_class,
        "schema_indexes": schema_indexes,
        "unique_preserve_order": unique_preserve_order,
        "best_role_property": best_role_property,
        "resolve_builder_link_direction": resolve_builder_link_direction,
        "mark_optional_display_selects": mark_optional_display_selects,
        "grounded_constraint_terms": grounded_constraint_terms,
        "build_constraint_filter": build_constraint_filter,
        "choose_action_support_classes": choose_action_support_classes,
        "choose_enumeration_value_projection": choose_enumeration_value_projection,
        "build_value_enumeration_query": build_value_enumeration_query,
        "selected_anchor_binding_for_class": selected_anchor_binding_for_class,
        "build_multi_evidence_relaxed_query": build_multi_evidence_relaxed_query,
    }


def _build_semantic_query_planner_legacy(
    question: str,
    template: str,
    schema: Optional[Dict[str, Any]],
    base_url: Optional[str] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Legacy compatibility shim kept during semantic planner runtime cutover."""
    return runtime_build_semantic_query_planner(
        question,
        template,
        schema,
        base_url=base_url,
        slots_override=slots_override,
        unit_intent_ir=unit_intent_ir,
        runtime=_semantic_planner_runtime_callbacks(),
    )


def build_semantic_query_planner(
    question: str,
    template: str,
    schema: Optional[Dict[str, Any]],
    base_url: Optional[str] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Thin wrapper around the repo-owned semantic planner runtime."""
    return runtime_build_semantic_query_planner(
        question,
        template,
        schema,
        base_url=base_url,
        slots_override=slots_override,
        unit_intent_ir=unit_intent_ir,
        runtime=_semantic_planner_runtime_callbacks(),
    )


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


def resource_instance_id(value: Any) -> Optional[str]:
    """Return the instance-like suffix from a resource local name such as customer_CUST004 -> CUST004."""
    local_name = value if isinstance(value, str) and not is_uri_like(value) else uri_local_name(value)
    if not isinstance(local_name, str) or not local_name:
        return None
    if "_" not in local_name:
        return None
    suffix = local_name.rsplit("_", 1)[-1].strip()
    return suffix or None


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
    display_id = display_id or resource_instance_id(local_name)
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
        display_id = display_id or resource_instance_id(evidence_uri)

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
    terminal_depths: Dict[str, int] = {}

    for path in paths:
        if not isinstance(path, list) or not path:
            continue
        objects = [step.get("object") for step in path if isinstance(step, dict) and is_uri_like(step.get("object"))]
        if not objects:
            continue

        terminal_uri = objects[-1]
        if terminal_uri not in direct_uris and terminal_uri != source_uri:
            terminal_uris.add(terminal_uri)
            terminal_depths[terminal_uri] = max(terminal_depths.get(terminal_uri, 0), len(path))

        for uri in objects[1:-1]:
            if uri not in direct_uris and uri != source_uri:
                mediator_uris.add(uri)

    direct_type_keys = {class_key_from_uri(uri) for uri in direct_uris if class_key_from_uri(uri)}
    terminal_uris = {
        uri
        for uri in terminal_uris
        if uri not in mediator_uris and class_key_from_uri(uri) not in direct_type_keys
    }
    terminal_depths = {
        uri: depth
        for uri, depth in terminal_depths.items()
        if uri in terminal_uris and isinstance(depth, int) and depth > 0
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
            "terminal_depths": terminal_depths,
        },
    })
    return summary


def preferred_terminal_uris_from_trace_refs(trace_refs: Optional[Dict[str, Any]]) -> List[str]:
    """Pick the structurally deepest terminal URIs from analyzer traces."""
    if not isinstance(trace_refs, dict):
        return []
    terminal_depths = trace_refs.get("terminal_depths")
    if not isinstance(terminal_depths, dict):
        return []
    normalized = {
        str(uri): int(depth)
        for uri, depth in terminal_depths.items()
        if is_uri_like(uri) and isinstance(depth, int) and depth > 0
    }
    if not normalized:
        return []
    max_depth = max(normalized.values())
    if max_depth <= 1:
        return []
    return sorted(uri for uri, depth in normalized.items() if depth == max_depth)


def build_related_entity_detail_query(
    schema: Optional[Dict[str, Any]],
    class_name: str,
    uris: List[str],
    projection_properties: Dict[str, str],
) -> Optional[str]:
    """Build a deterministic detail query for a known class and URI set."""
    if not isinstance(class_name, str) or not class_name or not uris:
        return None
    namespace = primary_namespace_from_schema(schema)
    select_vars = ["?entity"] + [f"?{var_name}" for var_name in projection_properties]
    query_lines = [
        f"PREFIX ex: <{namespace}>",
        "SELECT " + " ".join(select_vars),
        "WHERE {",
        "  VALUES ?entity { " + " ".join(f"<{uri}>" for uri in uris) + " }",
        f"  ?entity a ex:{class_name} .",
    ]
    for var_name, property_name in projection_properties.items():
        query_lines.append(f"  OPTIONAL {{ ?entity ex:{property_name} ?{var_name} . }}")
    query_lines.append("}")
    order_by = [var_name for var_name in ("detailId", "detailName", "detailType", "detailStatus") if var_name in projection_properties]
    query_lines.append("ORDER BY " + (" ".join(f"?{var_name}" for var_name in order_by) if order_by else "?entity"))
    return "\n".join(query_lines)


def fetch_related_entity_details_for_class(
    schema: Optional[Dict[str, Any]],
    class_name: str,
    uris: List[str],
    base_url: str,
    request_fn,
    domain_properties: Dict[str, List[Dict[str, Any]]],
    class_labels: Dict[str, str],
    manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Fetch name/description/status-like details for a class-specific URI set."""
    unique_uris = unique_preserve_order([str(uri) for uri in uris if is_uri_like(uri)])
    group = {
        "type_key": class_name,
        "type_label": class_labels.get(class_name, class_name),
        "record_count": len(unique_uris),
        "records": [],
    }
    if not unique_uris:
        return group

    detail_manifest = manifest
    if not isinstance(detail_manifest, dict):
        detail_manifest = build_semantic_manifest(schema)
    projection_properties = build_detail_projection_properties(
        class_name,
        domain_properties,
        class_labels,
        manifest=detail_manifest,
    )
    if detail_projection_needs_sample_support(
        class_name,
        projection_properties,
        domain_properties,
        manifest=detail_manifest,
    ):
        sampled_value_nodes = load_sample_value_nodes(base_url, detail_manifest, [class_name], limit=8)
        if sampled_value_nodes:
            detail_manifest = with_value_nodes(detail_manifest, sampled_value_nodes)
            projection_properties = build_detail_projection_properties(
                class_name,
                domain_properties,
                class_labels,
                manifest=detail_manifest,
            )

    rows: List[Dict[str, Any]] = []
    detail_query = build_related_entity_detail_query(schema, class_name, unique_uris, projection_properties)
    if detail_query is not None:
        detail_response = request_fn("POST", f"{base_url}/sparql", {"query": detail_query})
        if isinstance(detail_response, dict) and isinstance(detail_response.get("results"), list):
            rows = detail_response["results"]

    records_by_uri: Dict[str, Dict[str, Any]] = {
        uri: {
            "uri": uri,
            "local_name": uri_local_name(uri),
            "display_id": None,
            "display_name": None,
            "display_label": None,
            "display_description": None,
            "display_status": None,
            "display_type": None,
            "display_score": None,
            "display_fields": {},
        }
        for uri in unique_uris
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        entity_uri = row.get("entity")
        if not is_uri_like(entity_uri) or entity_uri not in records_by_uri:
            continue
        record = records_by_uri[entity_uri]
        field_updates = {
            "display_id": row.get("detailId"),
            "display_name": row.get("detailName"),
            "display_type": row.get("detailType"),
            "display_description": row.get("detailDescription"),
            "display_status": row.get("detailStatus"),
            "display_score": row.get("detailScore"),
        }
        for key, value in field_updates.items():
            if not is_missing_literal_value(value):
                record[key] = value
        display_fields = record["display_fields"]
        for var_name, value in row.items():
            if var_name == "entity" or is_missing_literal_value(value):
                continue
            display_fields[var_name] = value

    records = []
    for uri in unique_uris:
        record = records_by_uri[uri]
        record["display_id"] = record.get("display_id") or resource_instance_id(uri)
        display_name = (
            record.get("display_name")
            or record.get("display_type")
            or record.get("display_id")
            or record.get("local_name")
        )
        record["display_name"] = display_name
        record["display_label"] = display_name
        records.append(record)

    records.sort(
        key=lambda item: (
            str(item.get("display_id") or ""),
            str(item.get("display_name") or ""),
            str(item.get("local_name") or ""),
        )
    )
    group["records"] = records
    group["record_count"] = len(records)
    return group


def build_related_terminal_detail_index(
    schema: Optional[Dict[str, Any]],
    sparql_spec: Optional[Dict[str, Any]],
    sparql_response: Optional[Dict[str, Any]],
    analysis_response: Optional[Dict[str, Any]],
    analysis_meta: Optional[Dict[str, Any]],
    base_url: str,
    request_fn,
    max_classes: int = 4,
    max_entities_per_class: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch structured details for analysis terminal entities, grouped by source URI."""
    if not isinstance(schema, dict) or not isinstance(sparql_response, dict) or not isinstance(analysis_response, dict):
        return {}

    rows = sparql_response.get("results")
    if not isinstance(rows, list) or not rows:
        return {}

    class_labels = schema_class_label_map(schema)
    domain_properties = data_properties_by_domain(schema)
    detail_manifest = build_semantic_manifest(schema)
    source_var = choose_source_var(sparql_response, sparql_spec, analysis_meta)
    if not isinstance(source_var, str) or not source_var:
        return {}

    grouped_rows: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_uri = row.get(source_var)
        if is_uri_like(source_uri):
            grouped_rows.setdefault(source_uri, []).append(row)
    if not grouped_rows:
        return {}

    analysis_results_by_source: Dict[str, Dict[str, Any]] = {}
    if isinstance(analysis_response.get("results"), list):
        for item in analysis_response["results"]:
            if isinstance(item, dict) and is_uri_like(item.get("source")):
                analysis_results_by_source[item["source"]] = item
    elif is_uri_like(analysis_response.get("source")):
        analysis_results_by_source[analysis_response["source"]] = analysis_response

    detail_index: Dict[str, List[Dict[str, Any]]] = {}
    detail_cache: Dict[tuple[str, tuple[str, ...]], Dict[str, Any]] = {}

    for source_uri, source_rows in grouped_rows.items():
        evidence_var = choose_evidence_var(source_rows, source_var)
        evidence_items = build_evidence_items(source_rows, source_var, evidence_var, class_labels)
        reasoning_summary = build_reasoning_summary(
            source_uri,
            analysis_results_by_source.get(source_uri),
            evidence_items,
            class_labels,
        )
        terminal_summary = reasoning_summary.get("terminal_summary")
        if not isinstance(terminal_summary, list) or not terminal_summary:
            continue

        terminal_details: List[Dict[str, Any]] = []
        for item in terminal_summary[:max_classes]:
            if not isinstance(item, dict):
                continue
            class_name = item.get("type_key")
            refs = item.get("refs")
            if not isinstance(class_name, str) or not class_name or not isinstance(refs, list):
                continue
            uris = unique_preserve_order([
                ref.get("uri")
                for ref in refs[:max_entities_per_class]
                if isinstance(ref, dict) and is_uri_like(ref.get("uri"))
            ])
            if not uris:
                continue
            cache_key = (class_name, tuple(uris))
            if cache_key not in detail_cache:
                detail_cache[cache_key] = fetch_related_entity_details_for_class(
                    schema,
                    class_name,
                    uris,
                    base_url,
                    request_fn,
                    domain_properties,
                    class_labels,
                    manifest=detail_manifest,
                )
            terminal_details.append(deepcopy(detail_cache[cache_key]))

        if terminal_details:
            detail_index[source_uri] = terminal_details

    return detail_index


def flatten_terminal_detail_records(
    detail_groups: List[Dict[str, Any]],
    allowed_uris: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Flatten terminal detail groups into a stable record list, optionally scoped by URI."""
    records: List[Dict[str, Any]] = []
    for group in detail_groups:
        if not isinstance(group, dict):
            continue
        for record in group.get("records", []):
            if not isinstance(record, dict):
                continue
            record_uri = record.get("uri")
            if allowed_uris is not None and record_uri not in allowed_uris:
                continue
            flattened = dict(record)
            flattened["type_key"] = group.get("type_key")
            flattened["type_label"] = group.get("type_label")
            records.append(flattened)
    records.sort(
        key=lambda item: (
            str(item.get("type_label") or ""),
            str(item.get("display_id") or ""),
            str(item.get("display_name") or ""),
            str(item.get("local_name") or ""),
        )
    )
    return records


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
    related_terminal_details_by_source = {}
    if isinstance(analysis_meta, dict) and isinstance(analysis_meta.get("related_terminal_details_by_source"), dict):
        related_terminal_details_by_source = analysis_meta["related_terminal_details_by_source"]

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
        related_terminal_details = deepcopy(related_terminal_details_by_source.get(source_uri, []))
        if related_terminal_details:
            reasoning_summary["terminal_details"] = deepcopy(related_terminal_details)

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
            "related_terminal_details": related_terminal_details,
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
    related_terminal_details = group.get("related_terminal_details", [])
    preferred_terminal_uris = set(preferred_terminal_uris_from_trace_refs(group.get("trace_refs")))
    target_details = (
        flatten_terminal_detail_records(related_terminal_details, preferred_terminal_uris)
        if isinstance(analysis_meta, dict) and analysis_meta.get("target_projection_requested")
        else []
    )
    solution_details = target_details if isinstance(analysis_meta, dict) and analysis_meta.get("asks_solution") else []
    preferred_section_order = [
        "summary",
        "entity_facts",
        "entity_metrics",
    ]
    if target_details:
        preferred_section_order.append("target_details")
    if solution_details:
        preferred_section_order.append("solution_details")
    preferred_section_order.append("analysis_note")
    answer_contract = {
        "version": "causal_lookup_stable_v1",
        "preferred_section_order": preferred_section_order,
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
    if target_details:
        answer_contract["target_details"] = [
            {
                "type_label": item.get("type_label"),
                "target_id": item.get("display_id"),
                "target_name": item.get("display_name"),
                "target_description": item.get("display_description"),
            }
            for item in target_details
        ]
    if solution_details:
        answer_contract["solution_details"] = [
            {
                "type_label": item.get("type_label"),
                "solution_id": item.get("display_id"),
                "solution_name": item.get("display_name"),
                "solution_description": item.get("display_description"),
            }
            for item in solution_details
        ]
    return {
        "template": "causal_lookup",
        "summary": enumeration_like.get("summary"),
        "analysis_status": enumeration_like.get("analysis_status"),
        "entity": group.get("entity"),
        "facts": facts,
        "key_metrics": metric_items,
        "reasoning_summary": group.get("reasoning_summary"),
        "related_terminal_details": related_terminal_details,
        "target_details": target_details,
        "solution_details": solution_details,
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
    """Return a planning bundle through the repo-owned single-question runtime layer."""
    return runtime_build_single_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        run_templates=RUN_TEMPLATES,
        request_schema=lambda request_base_url: request_json("GET", f"{request_base_url}/schema"),
        request_profiles=lambda request_base_url: request_json("GET", f"{request_base_url}/analysis/profiles"),
        write_schema_state=write_schema_state,
        build_semantic_query_planner=build_semantic_query_planner,
        summarize_schema=summarize_schema,
        summarize_profiles=summarize_profiles,
        schema=schema,
        slots_override=slots_override,
        unit_intent_ir=unit_intent_ir,
    )


def build_conversation_state_entry(
    unit: Dict[str, Any],
    slots: Dict[str, Any],
    intent_ir: Dict[str, Any],
    response: Dict[str, Any],
) -> Dict[str, Any]:
    """Build carry-forward conversation state through the repo-owned runtime layer."""
    return runtime_build_conversation_state_entry(
        unit,
        slots,
        intent_ir,
        response,
        extract_focus_refs_from_response=lambda item: runtime_extract_focus_refs_from_response(
            item,
            is_uri_like=is_uri_like,
            class_key_from_uri=class_key_from_uri,
            unique_preserve_order=unique_preserve_order,
        ),
        semantic_state_from_sources=semantic_state_from_sources,
    )


def resolve_reference_context(
    unit: Dict[str, Any],
    conversation_states: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Resolve lightweight discourse references through the repo-owned runtime layer."""
    return runtime_resolve_reference_context(unit, conversation_states)


def apply_resolved_reference_to_slots(
    slots: Dict[str, Any],
    resolved_reference: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Attach resolved references through the repo-owned runtime layer."""
    return runtime_apply_resolved_reference_to_slots(
        slots,
        resolved_reference,
        unique_preserve_order=unique_preserve_order,
    )


def build_question_batch_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
) -> Dict[str, Any]:
    """Plan a multi-question utterance through the repo-owned runtime layer."""
    return runtime_build_question_batch_run_response(
        base_url,
        question,
        template,
        state_file,
        request_schema=lambda request_base_url: request_json("GET", f"{request_base_url}/schema"),
        write_schema_state=write_schema_state,
        merge_inherited_slots=merge_inherited_slots,
        build_question_unit_intent_ir=build_question_unit_intent_ir,
        build_single_question_mode_run_response=_build_single_question_mode_run_response,
        build_conversation_state_entry=build_conversation_state_entry,
        summarize_schema=summarize_schema,
    )


def build_question_mode_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
) -> Dict[str, Any]:
    """Return a planning bundle through the repo-owned runtime layer."""
    return runtime_build_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        request_schema=lambda request_base_url: request_json("GET", f"{request_base_url}/schema"),
        write_schema_state=write_schema_state,
        merge_inherited_slots=merge_inherited_slots,
        build_question_unit_intent_ir=build_question_unit_intent_ir,
        build_single_question_mode_run_response=_build_single_question_mode_run_response,
        build_conversation_state_entry=build_conversation_state_entry,
        summarize_schema=summarize_schema,
    )


def _execute_single_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
    planning_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Plan and execute one question through the repo-owned single-question runtime layer."""
    return runtime_execute_single_question_mode_run(
        base_url,
        question,
        template,
        state_file,
        include_planner_debug=include_planner_debug,
        build_single_question_mode_run_response=_build_single_question_mode_run_response,
        execute_run_plan=execute_run_plan,
        sparql_row_count=sparql_row_count,
        summarize_planner_result=compiler_summarize_planner_result,
        apply_fail_closed_contract_to_question_response=apply_fail_closed_contract_to_question_response,
        apply_bounded_recovery_contract_to_question_response=apply_bounded_recovery_contract_to_question_response,
        planning_override=planning_override,
    )


def execute_question_batch_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
) -> Dict[str, Any]:
    """Execute a multi-question utterance through the repo-owned runtime layer."""
    return runtime_execute_question_batch_run(
        base_url,
        question,
        template,
        state_file,
        include_planner_debug=include_planner_debug,
        request_schema=lambda request_base_url: request_json("GET", f"{request_base_url}/schema"),
        write_schema_state=write_schema_state,
        merge_inherited_slots=merge_inherited_slots,
        resolve_reference_context=resolve_reference_context,
        apply_resolved_reference_to_slots=apply_resolved_reference_to_slots,
        build_question_unit_intent_ir=build_question_unit_intent_ir,
        build_single_question_mode_run_response=_build_single_question_mode_run_response,
        execute_single_question_mode_run=_execute_single_question_mode_run,
        build_conversation_state_entry=build_conversation_state_entry,
        sparql_row_count=sparql_row_count,
        summarize_schema=summarize_schema,
    )


def execute_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    include_planner_debug: bool = False,
) -> Dict[str, Any]:
    """Plan and execute question-mode through the repo-owned runtime layer."""
    return runtime_execute_question_mode_run(
        base_url,
        question,
        template,
        state_file,
        include_planner_debug=include_planner_debug,
        request_schema=lambda request_base_url: request_json("GET", f"{request_base_url}/schema"),
        write_schema_state=write_schema_state,
        merge_inherited_slots=merge_inherited_slots,
        resolve_reference_context=resolve_reference_context,
        apply_resolved_reference_to_slots=apply_resolved_reference_to_slots,
        build_question_unit_intent_ir=build_question_unit_intent_ir,
        build_single_question_mode_run_response=_build_single_question_mode_run_response,
        execute_single_question_mode_run=_execute_single_question_mode_run,
        build_conversation_state_entry=build_conversation_state_entry,
        sparql_row_count=sparql_row_count,
        summarize_schema=summarize_schema,
    )


def execute_run_plan(
    base_url: str,
    plan: Dict[str, Any],
    state_file: Path,
) -> Dict[str, Any]:
    """Execute a guarded run plan through the repo-owned runtime layer."""
    return runtime_execute_run_plan(
        base_url,
        plan,
        state_file,
        normalize_run_plan=normalize_run_plan,
        http_request_json=http_request_json,
        curl_request_json=curl_request_json,
        client_request_json=client_request_json,
        local_test_client_factory=local_test_client,
        write_schema_state=write_schema_state,
        prepare_sparql_spec=prepare_sparql_spec,
        sparql_row_count=sparql_row_count,
        derive_uri_sources_from_sparql=derive_uri_sources_from_sparql,
        build_related_terminal_detail_index=build_related_terminal_detail_index,
        build_run_presentation=build_run_presentation,
        summarize_analysis_response=summarize_analysis_response,
        summarize_schema=summarize_schema,
        summarize_profiles=summarize_profiles,
    )


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
        "--answer-only",
        action="store_true",
        help="Return a compact answer-facing payload without parser/grounding/planner debug bulk.",
    )
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
    print_output(dispatch_cli_command(
        args,
        base_url,
        state_file,
        ttl_seconds,
        request_json=request_json,
        summarize_schema=summarize_schema,
        write_schema_state=write_schema_state,
        clear_schema_state=clear_schema_state,
        require_schema_state=require_schema_state,
        emit_protocol_note=lambda message: print(message, file=sys.stderr),
        run_templates=RUN_TEMPLATES,
        load_json_payload=load_json_payload,
        is_question_routed_plan=is_question_routed_plan,
        is_question_shorthand_plan=is_question_shorthand_plan,
        build_question_mode_run_response=build_question_mode_run_response,
        execute_question_mode_run=execute_question_mode_run,
        execute_run_plan=execute_run_plan,
    ))


if __name__ == "__main__":
    main()
