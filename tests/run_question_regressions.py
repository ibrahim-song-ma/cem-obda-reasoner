#!/usr/bin/env python3
"""Run NL question regressions against the local OBDA client/server."""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import signal
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
SUITE_PATH = ROOT / "tests" / "obda_question_regressions.json"
OBDA_API = ROOT / ".agents" / "skills" / "obda-query" / "scripts" / "obda_api.py"
BASE_URL = "http://127.0.0.1:8000"
STATE_FILE = Path("/tmp/obda_question_regression_state.json")
DEFAULT_TIMEOUT_SECONDS = 60


class RegressionFailure(Exception):
    """Raised when a regression case fails."""


def load_suite(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_client_module() -> ModuleType:
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location("obda_api_client", OBDA_API)
    if spec is None or spec.loader is None:
        raise RegressionFailure(f"Could not load client module from {OBDA_API}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_with_timeout(timeout_seconds: int, func, *args, **kwargs):
    def _handle_timeout(signum, frame):  # type: ignore[unused-argument]
        raise RegressionFailure(
            f"Call timed out after {timeout_seconds}s: {getattr(func, '__name__', repr(func))}"
        )

    previous = signal.signal(signal.SIGALRM, _handle_timeout)
    signal.alarm(timeout_seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


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


def has_path(data: Any, path: str) -> bool:
    try:
        get_path(data, path)
        return True
    except RegressionFailure:
        return False


def assert_case(response: Dict[str, Any], case: Dict[str, Any]) -> None:
    for assertion in case.get("assertions", []):
        path = assertion["path"]
        if assertion.get("absent") is True:
            if has_path(response, path):
                actual = get_path(response, path)
                raise RegressionFailure(
                    f"{case['id']}: {path} expected to be absent, got {actual!r}"
                )
            continue
        actual = get_path(response, path)
        if "equals" in assertion:
            expected = assertion["equals"]
            if actual != expected:
                raise RegressionFailure(
                    f"{case['id']}: {path} expected {expected!r}, got {actual!r}"
                )
        elif "approx" in assertion:
            expected = float(assertion["approx"])
            tolerance = float(assertion.get("tolerance", 1e-6))
            if not isinstance(actual, (int, float)) or math.fabs(float(actual) - expected) > tolerance:
                raise RegressionFailure(
                    f"{case['id']}: {path} expected approx {expected!r} +/- {tolerance}, got {actual!r}"
                )
        else:
            raise RegressionFailure(f"{case['id']}: unsupported assertion {assertion!r}")


def run_health_check(module: ModuleType, timeout_seconds: int) -> None:
    response = run_with_timeout(
        timeout_seconds,
        module.request_json,
        "GET",
        f"{BASE_URL}/health",
    )
    if response.get("status") != "healthy":
        raise RegressionFailure(f"Health check returned unexpected payload: {response!r}")


def run_case(module: ModuleType, case: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    payload = case["payload"]
    if not isinstance(payload, dict):
        raise RegressionFailure(f"{case['id']}: payload must be an object")

    question = payload.get("question")
    template = payload.get("template")
    if not isinstance(question, str) or not isinstance(template, str):
        raise RegressionFailure(
            f"{case['id']}: regression runner currently supports question-mode payloads with string template/question"
        )

    include_planner_debug = bool(payload.get("include_planner_debug"))
    if payload.get("plan_only"):
        response = run_with_timeout(
            timeout_seconds,
            module.build_question_mode_run_response,
            BASE_URL,
            question,
            template,
            STATE_FILE,
        )
    else:
        response = run_with_timeout(
            timeout_seconds,
            module.execute_question_mode_run,
            BASE_URL,
            question,
            template,
            STATE_FILE,
            include_planner_debug,
        )

    if not isinstance(response, dict):
        raise RegressionFailure(f"{case['id']}: client returned non-dict response {response!r}")
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
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-command timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--emit-case-payload",
        help="Print the payload JSON for the given case id and exit.",
    )
    parser.add_argument(
        "--assert-response-file",
        nargs=2,
        metavar=("CASE_ID", "RESPONSE_JSON"),
        help="Assert a saved JSON response against the given case id and exit.",
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

    if args.emit_case_payload:
        case = case_map.get(args.emit_case_payload)
        if case is None:
            raise RegressionFailure(f"Unknown case id: {args.emit_case_payload}")
        print(json.dumps(case.get("payload", {}), ensure_ascii=False))
        return 0

    if args.assert_response_file:
        case_id, response_path_text = args.assert_response_file
        case = case_map.get(case_id)
        if case is None:
            raise RegressionFailure(f"Unknown case id: {case_id}")
        response_path = Path(response_path_text)
        response = json.loads(response_path.read_text(encoding="utf-8"))
        assert_case(response, case)
        print(f"PASS {case_id}")
        return 0

    selected_ids = set(args.case or [])
    if selected_ids:
        cases = [case for case in cases if case.get("id") in selected_ids]
        missing = selected_ids - {case.get("id") for case in cases}
        if missing:
            raise RegressionFailure(f"Unknown case ids: {', '.join(sorted(missing))}")
    else:
        cases = [case for case in cases if case.get("enabled", True) is not False]

    module = load_client_module()
    run_health_check(module, args.timeout_seconds)

    passed = 0
    for case in cases:
        print(f"RUN {case['id']}", flush=True)
        response = run_case(module, case, args.timeout_seconds)
        passed += 1
        print(f"PASS {case['id']} -> status={response.get('status')}", flush=True)

    print(f"PASS suite={suite.get('suite_name', 'unknown')} cases={passed}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RegressionFailure as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        raise SystemExit(1)
