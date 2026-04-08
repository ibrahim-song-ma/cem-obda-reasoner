#!/usr/bin/env python3
"""Run grounding-policy regressions against the repo-owned grounding policy module."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
SUITE_PATH = ROOT / "tests" / "obda_grounding_regressions.json"
MODULE_PATH = ROOT / ".agents" / "skills" / "obda-query" / "scripts" / "obda_grounding_policy.py"


class RegressionFailure(Exception):
    """Raised when one grounding regression fails."""


def load_suite(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_module() -> ModuleType:
    sys.dont_write_bytecode = True
    script_dir = str(MODULE_PATH.parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    spec = importlib.util.spec_from_file_location("obda_grounding_policy_module", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RegressionFailure(f"Could not load grounding module from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tokenize_path(path: str) -> List[Any]:
    if not path:
        return []
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
    if not path:
        return data
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


def assert_case(response: Any, case: Dict[str, Any]) -> None:
    for assertion in case.get("assertions", []):
        actual = get_path(response, assertion["path"])
        if "equals" in assertion:
            expected = assertion["equals"]
            if actual != expected:
                raise RegressionFailure(
                    f"{case['id']}: {assertion['path'] or '<root>'} expected {expected!r}, got {actual!r}"
                )
        else:
            raise RegressionFailure(f"{case['id']}: unsupported assertion {assertion!r}")


def run_case(module: ModuleType, case: Dict[str, Any]) -> Any:
    call_name = case.get("call")
    if not isinstance(call_name, str) or not call_name:
        raise RegressionFailure(f"{case.get('id')}: missing call")
    target = getattr(module, call_name, None)
    if target is None:
        raise RegressionFailure(f"{case.get('id')}: unknown call {call_name}")
    args = case.get("args", [])
    kwargs = case.get("kwargs", {})
    if not isinstance(args, list):
        raise RegressionFailure(f"{case.get('id')}: args must be a list")
    if not isinstance(kwargs, dict):
        raise RegressionFailure(f"{case.get('id')}: kwargs must be an object")
    response = target(*args, **kwargs)
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
    module = load_module()
    for case in selected_cases:
        print(f"RUN {case['id']}")
        run_case(module, case)
        print(f"PASS {case['id']}")
    print(f"PASS suite=obda_grounding_regressions cases={len(selected_cases)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RegressionFailure as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        raise SystemExit(1)
