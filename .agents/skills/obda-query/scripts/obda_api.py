#!/usr/bin/env python3
"""Minimal client for the local OBDA reasoning server."""

import argparse
import contextlib
import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
REPO_ROOT = Path(__file__).resolve().parents[4]
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
        raise SystemExit(f"Run template '{template}' requires an analysis section")

    analysis_spec = normalized.get("analysis")
    if analysis_spec is not None:
        analysis_spec = dict(analysis_spec)
        if not analysis_spec.get("kind") and template_config["default_analysis_kind"]:
            analysis_spec["kind"] = template_config["default_analysis_kind"]
        normalized["analysis"] = analysis_spec

    if template_config["auto_include_profiles"] and "include_profiles" not in normalized:
        normalized["include_profiles"] = True

    if template == "schema_inspect" and not any(key in normalized for key in ("samples", "sparql", "analysis")):
        normalized["samples"] = []

    return normalized


def is_uri_like(value: Any) -> bool:
    return isinstance(value, str) and (value.startswith("http://") or value.startswith("https://"))


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


def build_question_mode_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
) -> Dict[str, Any]:
    """Return a schema-first planning bundle for QUESTION + TEMPLATE shorthand."""
    template_config = RUN_TEMPLATES.get(template)
    if template_config is None:
        supported = ", ".join(sorted(RUN_TEMPLATES))
        raise SystemExit(f"Unknown run template: {template}. Supported templates: {supported}")

    schema = request_json("GET", f"{base_url}/schema")
    write_schema_state(state_file, base_url)

    profiles = None
    if template_config["auto_include_profiles"]:
        profiles = request_json("GET", f"{base_url}/analysis/profiles")

    plan_skeleton: Dict[str, Any] = {"template": template}
    required_fields = []

    if template_config["requires_sparql"]:
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
    if template_config["requires_analysis"] and analysis_kind:
        required_fields.append("analysis.payload")
        analysis_payload: Dict[str, Any]
        if analysis_kind == "paths":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "source": "http://example.com/ontology#entity_123",
                "max_depth": 3,
            }
        elif analysis_kind == "paths-batch":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "sources": ["http://example.com/ontology#entity_123"],
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

    response: Dict[str, Any] = {
        "mode": "question-template",
        "status": "planning_required",
        "question": question,
        "template": template,
        "message": (
            "QUESTION + --template is a planning shortcut. "
            "It fetches schema first and returns a run skeleton, "
            "but it does not execute SPARQL automatically."
        ),
        "required_fields": required_fields,
        "plan_skeleton": plan_skeleton,
        "rules": [
            "Use run --json or run --json-file for execution.",
            "Do not hand-write GET /analysis/paths query strings; use analysis-paths --json or analysis-paths-batch --json.",
            "Use schema to verify domains before writing SPARQL.",
        ],
        "schema": schema,
    }
    if profiles is not None:
        response["profiles"] = profiles
    return response


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
        include_profiles = bool(plan.get("include_profiles")) or analysis_spec is not None
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
        sparql_spec = plan.get("sparql")
        if sparql_spec is not None:
            query_text = sparql_spec.get("query")
            query_file = sparql_spec.get("query_file")
            if query_text and query_file:
                raise SystemExit("sparql plan accepts either query or query_file, not both")
            if query_file:
                query_text = Path(query_file).read_text(encoding="utf-8")
            if not query_text:
                raise SystemExit("sparql plan requires query or query_file")
            sparql_response = request_fn("POST", f"{base_url}/sparql", {"query": query_text})

        analysis_response = None
        analysis_meta = None
        if analysis_spec is not None:
            kind = analysis_spec.get("kind", "paths")
            if kind == "causal":
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

        return {
            "template": plan["template"],
            "schema": schema,
            "profiles": profiles,
            "samples": samples,
            "sparql": sparql_response,
            "analysis": analysis_response,
            "analysis_meta": analysis_meta,
        }

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
    subparsers.add_parser("schema", help="GET /schema")
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
        help="Inline JSON run plan. With QUESTION + --template, a bare --json is treated as shorthand planning mode.",
    )
    run_group.add_argument("--json-file", help="Path to a JSON run plan.")
    run_parser.add_argument("question", nargs="?", help="Optional natural-language question for planning-only shorthand.")
    run_parser.add_argument("--template", choices=sorted(RUN_TEMPLATES), help="Template to use with QUESTION shorthand.")

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
        print_output(request_json("GET", f"{base_url}/schema"))
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
        if plan is None and args.question:
            template = args.template or "custom"
            print_output(build_question_mode_run_response(base_url, args.question, template, state_file))
            return
        if plan is None:
            raise SystemExit("run requires --json/--json-file, or QUESTION with --template for planning-only mode.")
        print_output(execute_run_plan(base_url, plan, state_file))
        return

    require_schema_state(state_file, base_url, ttl_seconds, args.command)
    payload = load_json_payload(args.json, args.json_file)
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
