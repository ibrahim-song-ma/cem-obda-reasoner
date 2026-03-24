#!/usr/bin/env python3
"""Minimal client for the local OBDA reasoning server."""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
REPO_ROOT = Path(__file__).resolve().parents[4]


def load_json_payload(payload: Optional[str], payload_file: Optional[str]) -> Optional[Dict[str, Any]]:
    """Load a JSON payload from a string or a file."""
    if payload and payload_file:
        raise SystemExit("Use either --json or --json-file, not both.")
    if payload_file:
        return json.loads(Path(payload_file).read_text(encoding="utf-8"))
    if payload:
        return json.loads(payload)
    return None


def request_json(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Send an HTTP request and parse the JSON response."""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        if error_body:
            print(error_body, file=sys.stderr)
        raise SystemExit(exc.code) from exc
    except urllib.error.URLError as exc:
        reason = str(exc.reason)
        if "Operation not permitted" in reason:
            return local_app_request(method, url, payload)
        print(f"Request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def local_app_request(method: str, url: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    """Fallback to calling the local FastAPI app directly when HTTP is sandboxed."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from fastapi.testclient import TestClient
    from reasoning_server import app

    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    if parsed.query:
        path = f"{path}?{parsed.query}"

    with TestClient(app) as client:
        response = client.request(method, path, json=payload)
        if response.status_code >= 400:
            print(response.text, file=sys.stderr)
            raise SystemExit(response.status_code)
        return response.json()


def print_output(data: Any) -> None:
    """Pretty-print a JSON-compatible response."""
    if isinstance(data, (dict, list)):
        print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        print(data)


def main() -> None:
    parser = argparse.ArgumentParser(description="Client for the local OBDA reasoning server.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Reasoning server base URL.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="GET /health")
    subparsers.add_parser("schema", help="GET /schema")
    subparsers.add_parser("profiles", help="GET /analysis/profiles")
    subparsers.add_parser("reload", help="POST /reload")

    sample_parser = subparsers.add_parser("sample", help="GET /sample/{class_name}")
    sample_parser.add_argument("class_name", help="Ontology class local name or URI.")
    sample_parser.add_argument("--limit", type=int, default=3, help="Number of samples to return.")

    causal_parser = subparsers.add_parser("causal", help="GET /causal/{customer_id}")
    causal_parser.add_argument("customer_id", help="Customer local id or full URI suffix.")

    sparql_parser = subparsers.add_parser("sparql", help="POST /sparql")
    sparql_group = sparql_parser.add_mutually_exclusive_group(required=True)
    sparql_group.add_argument("--query", help="SPARQL query string.")
    sparql_group.add_argument("--query-file", help="Path to a file containing the SPARQL query.")

    for endpoint in ("analysis-paths", "analysis-neighborhood", "analysis-inferred-relations", "analysis-explain"):
        endpoint_parser = subparsers.add_parser(endpoint, help=f"POST /{endpoint.replace('-', '/')}")
        endpoint_group = endpoint_parser.add_mutually_exclusive_group(required=True)
        endpoint_group.add_argument("--json", help="Inline JSON payload.")
        endpoint_group.add_argument("--json-file", help="Path to a JSON payload file.")

    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")

    if args.command == "health":
        print_output(request_json("GET", f"{base_url}/health"))
        return

    if args.command == "schema":
        print_output(request_json("GET", f"{base_url}/schema"))
        return

    if args.command == "profiles":
        print_output(request_json("GET", f"{base_url}/analysis/profiles"))
        return

    if args.command == "reload":
        print_output(request_json("POST", f"{base_url}/reload"))
        return

    if args.command == "sample":
        query = urllib.parse.urlencode({"limit": args.limit})
        url = f"{base_url}/sample/{urllib.parse.quote(args.class_name)}?{query}"
        print_output(request_json("GET", url))
        return

    if args.command == "causal":
        url = f"{base_url}/causal/{urllib.parse.quote(args.customer_id)}"
        print_output(request_json("GET", url))
        return

    if args.command == "sparql":
        if args.query_file:
            query_text = Path(args.query_file).read_text(encoding="utf-8")
        else:
            query_text = args.query
        print_output(request_json("POST", f"{base_url}/sparql", {"query": query_text}))
        return

    payload = load_json_payload(args.json, args.json_file)
    endpoint_map = {
        "analysis-paths": "/analysis/paths",
        "analysis-neighborhood": "/analysis/neighborhood",
        "analysis-inferred-relations": "/analysis/inferred-relations",
        "analysis-explain": "/analysis/explain",
    }
    print_output(request_json("POST", f"{base_url}{endpoint_map[args.command]}", payload))


if __name__ == "__main__":
    main()
