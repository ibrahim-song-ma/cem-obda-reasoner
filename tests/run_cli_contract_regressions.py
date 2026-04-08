#!/usr/bin/env python3
"""Lock answer-facing CLI contract behavior for question-mode run dispatch."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parent.parent
RUNTIME_PATH = ROOT / ".agents" / "skills" / "obda-query" / "scripts" / "obda_cli_command_runtime.py"


def load_runtime_module():
    spec = importlib.util.spec_from_file_location("obda_cli_command_runtime_module", RUNTIME_PATH)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Could not load runtime module from {RUNTIME_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    module = load_runtime_module()

    args = SimpleNamespace(
        json='{"template":"causal_enumeration","question":"有哪些客户因为5G信号差而投诉了?"}',
        json_file=None,
        question=None,
        template=None,
        answer_only=False,
        plan_only=False,
    )

    response = module.handle_run_cli_command(
        args,
        "http://127.0.0.1:8000",
        Path("/tmp/obda_cli_contract_state.json"),
        load_json_payload=lambda payload, payload_file: {
            "template": "causal_enumeration",
            "question": "有哪些客户因为5G信号差而投诉了?",
        },
        is_question_routed_plan=lambda plan: True,
        is_question_shorthand_plan=lambda plan: False,
        build_question_mode_run_response=lambda base_url, question, template, state_file: {
            "mode": "question-template",
            "status": "planning_required",
            "question": question,
            "template": template,
            "message": "planner could not produce a high-confidence executable plan",
            "next_action": "stop_or_report_planning_required",
            "recovery_policy": {
                "mode": "fail_closed",
                "manual_exploration_allowed": False,
                "bounded_recovery_allowed": False,
                "requires_plan_only_for_debug": False,
                "requires_user_clarification": False,
            },
            "rules": ["stop"],
        },
        execute_question_mode_run=lambda base_url, question, template, state_file, include_planner_debug=False: {
            "mode": "question-template",
            "status": "planning_required",
            "question": question,
            "template": template,
            "message": "planner could not produce a high-confidence executable plan",
            "next_action": "stop_or_report_planning_required",
            "recovery_policy": {
                "mode": "fail_closed",
                "manual_exploration_allowed": False,
                "bounded_recovery_allowed": False,
                "requires_plan_only_for_debug": False,
                "requires_user_clarification": False,
            },
            "rules": ["stop"],
        },
        execute_run_plan=lambda base_url, plan, state_file: {"status": "ok"},
    )

    expected_keys = {
        "final_user_reply",
        "must_stop",
        "must_reply_verbatim",
        "forbidden_follow_up",
        "forbidden_reply_additions",
    }
    actual_keys = set(response.keys())
    if actual_keys != expected_keys:
        raise SystemExit(f"Unexpected keys: {sorted(actual_keys)}")
    if response.get("must_stop") is not True or response.get("must_reply_verbatim") is not True:
        raise SystemExit(f"Missing top-level hard-stop contract: {response!r}")
    if "schema" not in response.get("forbidden_follow_up", []):
        raise SystemExit(f"Expected schema to be forbidden: {response!r}")
    final_user_reply = response.get("final_user_reply", "")
    if "ontology" in final_user_reply or "事件类型" in final_user_reply or "指标" in final_user_reply:
        raise SystemExit(f"Hard-stop reply should not invite schema mining: {final_user_reply!r}")
    print("PASS cli_question_json_defaults_to_compact_stop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
