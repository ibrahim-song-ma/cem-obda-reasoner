#!/usr/bin/env python3
"""Run parser-contract regressions against the repo-owned language intent parser."""

from __future__ import annotations

import argparse
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterator, List
from urllib import request as urllib_request


ROOT = Path(__file__).resolve().parent.parent
SUITE_PATH = ROOT / "tests" / "obda_parser_regressions.json"
PARSER_MODULE_PATH = ROOT / ".agents" / "skills" / "obda-query" / "scripts" / "obda_intent_parser.py"
NO_PROXY_OPENER = urllib_request.build_opener(urllib_request.ProxyHandler({}))


class RegressionFailure(Exception):
    """Raised when one parser regression case fails."""


def load_suite(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_parser_module() -> ModuleType:
    sys.dont_write_bytecode = True
    script_dir = str(PARSER_MODULE_PATH.parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    spec = importlib.util.spec_from_file_location("obda_intent_parser_module", PARSER_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RegressionFailure(f"Could not load parser module from {PARSER_MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tokenize_path(path: str) -> List[Any]:
    tokens: List[Any] = []
    buffer = ""
    index_buffer = ""
    in_index = False
    for char in path:
        if in_index:
            if char == "]":
                if not index_buffer.isdigit():
                    raise RegressionFailure(f"Unsupported path segment in {path!r}")
                tokens.append(int(index_buffer))
                index_buffer = ""
                in_index = False
            else:
                index_buffer += char
            continue
        if char == ".":
            if buffer:
                tokens.append(buffer)
                buffer = ""
            continue
        if char == "[":
            if buffer:
                tokens.append(buffer)
                buffer = ""
            in_index = True
            continue
        buffer += char
    if in_index:
        raise RegressionFailure(f"Unclosed index segment in path {path!r}")
    if buffer:
        tokens.append(buffer)
    return tokens


def get_path(data: Any, path: str) -> Any:
    current = data
    for token in tokenize_path(path):
        if isinstance(token, int):
            if not isinstance(current, list):
                raise RegressionFailure(f"Path {path!r} expected list before index {token}")
            if token >= len(current):
                raise RegressionFailure(f"Path {path!r} index {token} out of range")
            current = current[token]
            continue
        if not isinstance(current, dict):
            raise RegressionFailure(f"Path {path!r} expected object before key {token!r}")
        if token not in current:
            raise RegressionFailure(f"Path {path!r} missing key {token!r}")
        current = current[token]
    return current


def assert_case(response: Dict[str, Any], case: Dict[str, Any]) -> None:
    for assertion in case.get("assertions", []):
        path = assertion["path"]
        actual = get_path(response, path)
        if "equals" in assertion:
            expected = assertion["equals"]
            if actual != expected:
                raise RegressionFailure(
                    f"{case['id']}: {path} expected {expected!r}, got {actual!r}"
                )
        else:
            raise RegressionFailure(f"{case['id']}: unsupported assertion {assertion!r}")


@contextmanager
def temporary_env(overrides: Dict[str, str]) -> Iterator[None]:
    previous: Dict[str, Any] = {}
    try:
        for key, value in overrides.items():
            previous[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _wait_for_server(base_url: str, timeout_sec: float = 5.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with NO_PROXY_OPENER.open(f"{base_url}/health", timeout=1.0) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RegressionFailure(f"Server did not become ready: {base_url}")


@contextmanager
def temporary_server(server_spec: Dict[str, Any]) -> Iterator[Dict[str, str]]:
    if not isinstance(server_spec, dict) or not server_spec:
        yield {}
        return
    response_file = server_spec.get("response_file")
    if not isinstance(response_file, str) or not response_file:
        raise RegressionFailure("server.response_file must be a non-empty string")

    payload = json.loads(Path(response_file).read_text(encoding="utf-8"))
    response_text = json.dumps(payload, ensure_ascii=False)

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, body_payload: dict) -> None:
            body = json.dumps(body_payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self._send_json(200, {"status": "ok"})
                return
            self._send_json(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/chat/completions":
                self._send_json(404, {"error": "not_found"})
                return
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length:
                self.rfile.read(content_length)
            self._send_json(
                200,
                {
                    "id": "chatcmpl-obda-test",
                    "object": "chat.completion",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": response_text,
                            },
                            "finish_reason": "stop",
                        }
                    ],
                },
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{int(server.server_address[1])}"
    try:
        _wait_for_server(base_url)
        yield {"SERVER_URL": base_url}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def run_case(module: ModuleType, case: Dict[str, Any]) -> Dict[str, Any]:
    payload = case.get("payload", {})
    if not isinstance(payload, dict):
        raise RegressionFailure(f"{case['id']}: payload must be an object")
    question = payload.get("question")
    template = payload.get("template")
    mode = payload.get("mode")
    env_overrides = payload.get("env", {})
    server_spec = payload.get("server", {})
    if not isinstance(question, str) or not isinstance(template, str):
        raise RegressionFailure(f"{case['id']}: question/template must be strings")
    if mode not in {"unit", "utterance"}:
        raise RegressionFailure(f"{case['id']}: unsupported mode {mode!r}")
    if not isinstance(env_overrides, dict):
        raise RegressionFailure(f"{case['id']}: env must be an object")

    with temporary_server(server_spec) as server_env:
        env_text = {
            str(key): (
                str(value).replace("__SERVER_URL__", server_env.get("SERVER_URL", ""))
                if isinstance(value, str)
                else str(value)
            )
            for key, value in env_overrides.items()
            if isinstance(key, str)
        }
        with temporary_env(env_text):
            if mode == "unit":
                response = module.parse_question_unit(question, template)
            else:
                response = module.parse_question_utterance(question, template)
    if not isinstance(response, dict):
        raise RegressionFailure(f"{case['id']}: parser returned non-dict response {response!r}")
    assert_case(response, case)
    return response


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite",
        type=Path,
        default=SUITE_PATH,
        help=f"Path to suite JSON. Default: {SUITE_PATH}",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Run only the specified case id. Can be passed multiple times.",
    )
    args = parser.parse_args()

    suite = load_suite(args.suite)
    cases = suite.get("cases", [])
    if not isinstance(cases, list) or not cases:
        raise RegressionFailure(f"Suite has no cases: {args.suite}")
    case_map = {
        case.get("id"): case
        for case in cases
        if isinstance(case, dict) and isinstance(case.get("id"), str)
    }
    selected_cases = [case_map[case_id] for case_id in args.case] if args.case else list(case_map.values())
    module = load_parser_module()
    for case in selected_cases:
        print(f"RUN {case['id']}")
        run_case(module, case)
        print(f"PASS {case['id']}")
    print(f"PASS suite=obda_parser_regressions cases={len(selected_cases)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RegressionFailure as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        raise SystemExit(1)
