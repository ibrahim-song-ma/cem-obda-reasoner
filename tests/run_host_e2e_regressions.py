#!/usr/bin/env python3
"""Run real Claude Code host E2E regressions against obda-query skill behavior."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent.parent
SUITE_PATH = ROOT / "tests" / "obda_host_e2e_regressions.json"
DEFAULT_TIMEOUT_SECONDS = 240


class RegressionFailure(Exception):
    """Raised when one host E2E regression fails."""


def load_suite(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def parse_trace(trace_path: Path) -> Dict[str, Any]:
    tools: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []

    for raw_line in trace_path.read_text(encoding="utf-8").splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            entry = json.loads(raw_line)
        except json.JSONDecodeError:
            continue

        if entry.get("type") == "assistant":
            message = entry.get("message")
            if not isinstance(message, dict):
                continue
            for block in message.get("content", []):
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "tool_use":
                    continue
                tools.append(
                    {
                        "name": block.get("name"),
                        "input": block.get("input", {}),
                    }
                )
            continue

        if entry.get("type") == "result":
            results.append(entry)

    bash_commands = [
        normalize_text(tool.get("input", {}).get("command", ""))
        for tool in tools
        if tool.get("name") == "Bash"
    ]
    return {
        "tools": tools,
        "results": results,
        "bash_commands": bash_commands,
        "final_result": results[-1] if results else None,
    }


def has_skill(trace: Dict[str, Any], skill_name: str) -> bool:
    for tool in trace["tools"]:
        if tool.get("name") != "Skill":
            continue
        tool_input = tool.get("input", {})
        if isinstance(tool_input, dict) and tool_input.get("skill") == skill_name:
            return True
    return False


def assert_case(trace: Dict[str, Any], case: Dict[str, Any]) -> None:
    tools = trace["tools"]
    bash_commands = trace["bash_commands"]
    final_result = trace["final_result"]

    for assertion in case.get("assertions", []):
        kind = assertion.get("kind")
        if kind == "require_skill":
            skill_name = assertion["skill"]
            if not has_skill(trace, skill_name):
                raise RegressionFailure(f"{case['id']}: skill {skill_name!r} was not used")
            continue

        if kind == "forbid_tool":
            tool_name = assertion["name"]
            if any(tool.get("name") == tool_name for tool in tools):
                raise RegressionFailure(f"{case['id']}: forbidden tool used: {tool_name}")
            continue

        if kind == "forbid_tool_path_prefix":
            tool_names = assertion.get("tool_names", [])
            prefix = assertion.get("prefix")
            if not isinstance(tool_names, list) or not tool_names:
                raise RegressionFailure(f"{case['id']}: forbid_tool_path_prefix needs non-empty tool_names")
            if not isinstance(prefix, str) or not prefix:
                raise RegressionFailure(f"{case['id']}: forbid_tool_path_prefix needs prefix")
            for tool in tools:
                if tool.get("name") not in tool_names:
                    continue
                tool_input = tool.get("input", {})
                if not isinstance(tool_input, dict):
                    continue
                for value in tool_input.values():
                    if isinstance(value, str) and value.startswith(prefix):
                        raise RegressionFailure(
                            f"{case['id']}: forbidden {tool.get('name')} path under {prefix}: {value}"
                        )
            continue

        if kind == "require_any_bash_contains":
            values = assertion.get("values", [])
            if not isinstance(values, list) or not values:
                raise RegressionFailure(f"{case['id']}: require_any_bash_contains needs non-empty values")
            if not any(any(value in command for value in values) for command in bash_commands):
                raise RegressionFailure(
                    f"{case['id']}: none of {values!r} found in bash commands: {bash_commands!r}"
                )
            continue

        if kind == "forbid_bash_contains":
            value = assertion["value"]
            offending = [command for command in bash_commands if value in command]
            if offending:
                raise RegressionFailure(f"{case['id']}: forbidden bash fragment {value!r}: {offending!r}")
            continue

        if kind == "max_bash_count":
            maximum = int(assertion["value"])
            if len(bash_commands) > maximum:
                raise RegressionFailure(
                    f"{case['id']}: expected <= {maximum} bash commands, got {len(bash_commands)}"
                )
            continue

        if kind == "require_result_subtype":
            if not isinstance(final_result, dict):
                raise RegressionFailure(f"{case['id']}: missing final result")
            expected = assertion["value"]
            actual = final_result.get("subtype")
            if actual != expected:
                raise RegressionFailure(f"{case['id']}: final result subtype expected {expected!r}, got {actual!r}")
            continue

        raise RegressionFailure(f"{case['id']}: unsupported assertion {assertion!r}")


def run_live_case(case: Dict[str, Any], trace_path: Path, timeout_seconds: int) -> Dict[str, Any]:
    payload = case.get("payload", {})
    question = payload.get("question")
    if not isinstance(question, str) or not question:
        raise RegressionFailure(f"{case['id']}: payload.question must be a non-empty string")

    env = os.environ.copy()
    env.setdefault("ENABLE_TOOL_SEARCH", "false")

    command = [
        env.get("OBDA_HOST_E2E_CLAUDE_CMD", "claude"),
        "-p",
        "--verbose",
        "--permission-mode",
        "bypassPermissions",
        "--output-format",
        "stream-json",
        "--include-partial-messages",
        question,
    ]

    with trace_path.open("w", encoding="utf-8") as handle:
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                env=env,
                stdout=handle,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RegressionFailure(f"{case['id']}: host e2e timed out after {timeout_seconds}s") from exc

    if completed.returncode != 0:
        stderr_text = (completed.stderr or "").strip()
        raise RegressionFailure(
            f"{case['id']}: host command failed with code {completed.returncode}: {stderr_text}"
        )

    trace = parse_trace(trace_path)
    assert_case(trace, case)
    return trace


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
        help=f"Per-case timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--trace-dir",
        type=Path,
        default=None,
        help="Directory to store raw Claude stream-json traces.",
    )
    parser.add_argument(
        "--assert-trace-file",
        nargs=2,
        metavar=("CASE_ID", "TRACE_JSONL"),
        help="Assert a saved trace file against the given case id and exit.",
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

    if args.assert_trace_file:
        case_id, trace_path_text = args.assert_trace_file
        case = case_map.get(case_id)
        if case is None:
            raise RegressionFailure(f"Unknown case id: {case_id}")
        trace = parse_trace(Path(trace_path_text))
        assert_case(trace, case)
        print(f"PASS {case_id}")
        return 0

    selected_cases = [case_map[case_id] for case_id in args.case] if args.case else list(case_map.values())
    trace_dir = args.trace_dir or Path(os.environ.get("OBDA_HOST_E2E_TRACE_DIR", ROOT / "tests" / ".tmp_host_e2e"))
    trace_dir.mkdir(parents=True, exist_ok=True)

    if "ANTHROPIC_API_KEY" not in os.environ and "OPENAI_API_KEY" not in os.environ:
        raise RegressionFailure("No model-host credentials in environment. Set ANTHROPIC_API_KEY or OPENAI_API_KEY.")

    for case in selected_cases:
        trace_path = trace_dir / f"{case['id']}.jsonl"
        print(f"RUN {case['id']}", flush=True)
        print(f"TRACE {trace_path}", flush=True)
        try:
            run_live_case(case, trace_path, args.timeout_seconds)
        except RegressionFailure as exc:
            raise RegressionFailure(f"{exc} [trace={trace_path}]") from exc
        print(f"PASS {case['id']}")

    print(f"PASS suite=obda_host_e2e_regressions cases={len(selected_cases)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RegressionFailure as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        raise SystemExit(1)
