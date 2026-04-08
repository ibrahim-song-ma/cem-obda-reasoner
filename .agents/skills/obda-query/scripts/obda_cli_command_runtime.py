#!/usr/bin/env python3
"""Repo-owned CLI command runtime for run/analyzer dispatch."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional
import urllib.parse


ANALYSIS_ENDPOINT_MAP = {
    "analysis-paths": "/analysis/paths",
    "analysis-paths-batch": "/analysis/paths/batch",
    "analysis-neighborhood": "/analysis/neighborhood",
    "analysis-inferred-relations": "/analysis/inferred-relations",
    "analysis-explain": "/analysis/explain",
}

AGENT_COMPACT_DROP_KEYS = {
    "resolved_slots",
    "parser_output",
    "parser_evidence",
    "bootstrap_candidates",
    "bootstrap_signals",
    "bootstrap_operator_hints",
    "semantic_state",
    "slot_inputs",
    "slot_bindings",
    "grounded_constraints",
    "grounding_bundle",
    "request_ir",
    "planner_bundle",
    "planner_attempts",
    "locked_plan",
    "schema_summary",
    "schema",
    "analysis",
    "analysis_result",
    "records",
    "rows",
    "bindings",
    "sparql",
}


def build_agent_contract(compacted: Dict[str, Any]) -> Dict[str, Any] | None:
    """Build a repo-owned execution contract for host agents from next_action."""
    next_action = compacted.get("next_action")
    if next_action == "ask_user_for_clarification":
        return {
            "must_stop": True,
            "required_next_action": "ask_user_for_clarification",
            "user_clarification_prompt": compacted.get("user_clarification_prompt"),
            "forbidden_follow_up": [
                "schema --full",
                "schema grep",
                "sample",
                "sparql",
                "rerun_with_metric_rewrite",
                "repo_debugging",
            ],
        }
    if next_action in {"stop_or_use_plan_only_for_debug", "stop_or_report_planning_required"}:
        return {
            "must_stop": True,
            "required_next_action": "stop_or_report_planning_required",
            "report_mode": "verbatim_final_user_reply",
            "must_reply_verbatim": True,
            "forbidden_follow_up": [
                "run",
                "--plan-only",
                "question_rewrite_retry",
                "semantic_rephrase_retry",
                "schema",
                "schema --full",
                "schema grep",
                "sample",
                "sparql",
                "manual_metric_probe",
                "repo_debugging",
            ],
            "forbidden_reply_additions": [
                "extra_explanation",
                "possible_reasons",
                "schema_suggestions",
                "ontology_term_examples",
                "question_rewrite_suggestions",
            ],
        }
    if next_action == "follow_recovery_hint_or_report_no_match":
        return {
            "must_stop": False,
            "required_next_action": "bounded_recovery_only",
            "bounded_recovery_contract": compacted.get("bounded_recovery_contract"),
            "forbidden_follow_up": [
                "schema --full",
                "schema grep",
                "alternate_metric_probe",
                "open_ended_exploration",
            ],
        }
    return None


def contains_cjk(text: str) -> bool:
    """Whether text contains CJK characters."""
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def build_final_user_reply(compacted: Dict[str, Any]) -> str:
    """Build a direct user-facing stop reply for hard-stop compact responses."""
    question = compacted.get("question")
    if isinstance(question, str) and contains_cjk(question):
        return (
            "当前系统无法把这个问题可靠地转成高置信查询计划，所以我必须在这里停止，"
            "不能继续猜测、改写问题或做额外探测。请用更明确的条件重新提问。"
        )
    return (
        "The system could not form a high-confidence executable plan for this question, so I must stop rather than guess, "
        "rewrite the question, or keep probing. Please restate it with more explicit conditions."
    )


def compact_question_response_for_agent(value: Dict[str, Any]) -> Dict[str, Any]:
    """Return a truly answer-facing question response without planner/debug bulk."""
    compacted: Dict[str, Any] = {}
    for key in (
        "mode",
        "status",
        "question",
        "template",
        "effective_template",
        "query_family",
        "message",
        "presentation",
        "next_action",
        "user_clarification_prompt",
        "clarification_hint",
        "clarification_target_unit",
        "recovery_policy",
        "bounded_recovery_contract",
        "blocked_reason",
        "rules",
    ):
        if key in value:
            compacted[key] = compact_run_response_for_agent(value[key])
    if compacted.get("status") == "planning_required" and compacted.get("next_action") == "stop_or_use_plan_only_for_debug":
        compacted["next_action"] = "stop_or_report_planning_required"
    recovery_policy = compacted.get("recovery_policy")
    if compacted.get("status") == "planning_required" and isinstance(recovery_policy, dict):
        recovery_policy = dict(recovery_policy)
        recovery_policy["requires_plan_only_for_debug"] = False
        compacted["recovery_policy"] = recovery_policy
    if compacted.get("status") == "planning_required":
        compacted.pop("rules", None)
        compacted["agent_stop_message"] = (
            "The planner could not produce a high-confidence executable plan. Return the provided final user reply and stop."
        )
        compacted["final_user_reply"] = build_final_user_reply(compacted)
    agent_contract = build_agent_contract(compacted)
    if agent_contract is not None:
        compacted["agent_contract"] = agent_contract
    if compacted.get("status") == "planning_required":
        minimal: Dict[str, Any] = {}
        if "final_user_reply" in compacted:
            minimal["final_user_reply"] = compacted["final_user_reply"]
        if isinstance(agent_contract, dict):
            minimal["must_stop"] = bool(agent_contract.get("must_stop"))
            minimal["must_reply_verbatim"] = bool(agent_contract.get("must_reply_verbatim"))
            if isinstance(agent_contract.get("forbidden_follow_up"), list):
                minimal["forbidden_follow_up"] = list(agent_contract["forbidden_follow_up"])
            if isinstance(agent_contract.get("forbidden_reply_additions"), list):
                minimal["forbidden_reply_additions"] = list(agent_contract["forbidden_reply_additions"])
        return minimal
    return compacted


def compact_question_unit_for_agent(value: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only answer-facing unit structure for batch question responses."""
    compacted: Dict[str, Any] = {}
    for key in ("unit_id", "text", "raw_text", "dependency", "reference_markers"):
        if key in value:
            compacted[key] = compact_run_response_for_agent(value[key])
    response = value.get("response")
    if isinstance(response, dict):
        compacted["response"] = compact_question_response_for_agent(response)
    return compacted


def compact_run_response_for_agent(value: Any) -> Any:
    """Strip planner/parser debug bulk while keeping answer-facing structure."""
    if isinstance(value, dict):
        mode = value.get("mode")
        if mode in {"question-template", "question-batch-template"}:
            compacted = compact_question_response_for_agent(value)
            if mode == "question-batch-template" and isinstance(value.get("question_units"), list):
                compacted["question_units"] = [
                    compact_question_unit_for_agent(unit)
                    for unit in value["question_units"]
                    if isinstance(unit, dict)
                ]
            return compacted
        compacted: Dict[str, Any] = {}
        for key, item in value.items():
            if key in AGENT_COMPACT_DROP_KEYS:
                continue
            compacted[key] = compact_run_response_for_agent(item)
        agent_contract = build_agent_contract(compacted)
        if agent_contract is not None:
            compacted["agent_contract"] = agent_contract
        return compacted
    if isinstance(value, list):
        return [compact_run_response_for_agent(item) for item in value]
    return value


def handle_simple_cli_command(
    args: Any,
    base_url: str,
    state_file: Path,
    ttl_seconds: int,
    *,
    request_json: Callable[[str, str, Optional[Dict[str, Any]]], Any],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    clear_schema_state: Callable[[Path], None],
    require_schema_state: Callable[[Path, str, int, str], None],
    emit_protocol_note: Callable[[str], None],
    run_templates: Dict[str, Dict[str, Any]],
) -> Optional[Any]:
    """Handle simple CLI commands that do not need planner/runtime dispatch."""
    if args.command == "health":
        return request_json("GET", f"{base_url}/health")

    if args.command == "schema":
        schema = request_json("GET", f"{base_url}/schema")
        write_schema_state(state_file, base_url)
        if args.full:
            return schema
        return {
            "schema_summary": summarize_schema(schema),
            "schema_included": False,
        }

    if args.command == "profiles":
        return request_json("GET", f"{base_url}/analysis/profiles")

    if args.command == "templates":
        return {"templates": run_templates}

    if args.command == "reload":
        clear_schema_state(state_file)
        return request_json("POST", f"{base_url}/reload")

    if args.command == "sample":
        require_schema_state(state_file, base_url, ttl_seconds, "sample")
        emit_protocol_note("Protocol note: /sample is for grounding only, not for enumerating final answer sets.")
        limit = args.limit_arg if args.limit_arg is not None else args.limit
        query = urllib.parse.urlencode({"limit": limit})
        url = f"{base_url}/sample/{urllib.parse.quote(args.class_name)}?{query}"
        return request_json("GET", url)

    if args.command == "causal":
        require_schema_state(state_file, base_url, ttl_seconds, "causal")
        url = f"{base_url}/causal/{urllib.parse.quote(args.customer_id)}"
        return request_json("GET", url)

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
        return request_json("POST", f"{base_url}/sparql", {"query": query_text})

    return None


def handle_run_cli_command(
    args: Any,
    base_url: str,
    state_file: Path,
    *,
    load_json_payload: Callable[[Optional[str], Optional[str]], Optional[Dict[str, Any]]],
    is_question_routed_plan: Callable[[Optional[Dict[str, Any]]], bool],
    is_question_shorthand_plan: Callable[[Optional[Dict[str, Any]]], bool],
    build_question_mode_run_response: Callable[[str, str, str, Path], Dict[str, Any]],
    execute_question_mode_run: Callable[..., Dict[str, Any]],
    execute_run_plan: Callable[[str, Dict[str, Any], Path], Dict[str, Any]],
) -> Dict[str, Any]:
    """Handle CLI `run` dispatch without owning parser/planner semantics."""
    json_supplied = args.json not in (None, "__AUTO__")
    answer_only = bool(getattr(args, "answer_only", False))
    question_shorthand = isinstance(getattr(args, "question", None), str) and bool(args.question)
    if not answer_only and question_shorthand and not bool(getattr(args, "plan_only", False)):
        # Real agent hosts should not have to remember `--answer-only` for normal
        # QUESTION shorthand. Default to the compact answer-facing contract whenever
        # we are not explicitly in plan/debug mode.
        answer_only = True
    if (json_supplied or args.json_file) and args.question:
        raise SystemExit("Use either run --json/--json-file or run QUESTION --template, not both.")

    plan = load_json_payload(args.json, args.json_file)
    question_routed_plan = is_question_routed_plan(plan)
    question_shorthand_plan = is_question_shorthand_plan(plan)
    if (
        not answer_only
        and not bool(getattr(args, "plan_only", False))
        and (question_routed_plan or question_shorthand_plan)
        and args.json is not None
        and not bool(isinstance(plan, dict) and plan.get("plan_only"))
        and not bool(isinstance(plan, dict) and plan.get("include_planner_debug"))
    ):
        # JSON question-mode plans should follow the same answer-facing default as
        # bare QUESTION shorthand when they arrive as inline host commands. Keep
        # --json-file behavior unchanged for test harnesses and explicit batch/debug
        # workflows.
        answer_only = True

    if question_routed_plan:
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
        return compact_run_response_for_agent(response) if answer_only else response

    if question_shorthand_plan:
        template = plan.get("template") or "custom"
        json_plan_only = bool(plan.get("plan_only"))
        if args.plan_only or json_plan_only:
            response = build_question_mode_run_response(base_url, plan["question"], template, state_file)
        else:
            response = execute_question_mode_run(base_url, plan["question"], template, state_file)
        return compact_run_response_for_agent(response) if answer_only else response

    if plan is None and args.question:
        template = args.template or "custom"
        if args.plan_only:
            response = build_question_mode_run_response(base_url, args.question, template, state_file)
        else:
            response = execute_question_mode_run(base_url, args.question, template, state_file)
        return compact_run_response_for_agent(response) if answer_only else response

    if plan is None:
        raise SystemExit("run requires --json/--json-file, or QUESTION with --template.")
    response = execute_run_plan(base_url, plan, state_file)
    return compact_run_response_for_agent(response) if answer_only else response


def handle_analysis_endpoint_cli_command(
    args: Any,
    base_url: str,
    state_file: Path,
    ttl_seconds: int,
    *,
    require_schema_state: Callable[[Path, str, int, str], None],
    load_json_payload: Callable[[Optional[str], Optional[str]], Optional[Dict[str, Any]]],
    request_json: Callable[[str, str, Optional[Dict[str, Any]]], Any],
) -> Any:
    """Handle CLI analyzer endpoint dispatch with the repo-owned endpoint contract."""
    require_schema_state(state_file, base_url, ttl_seconds, args.command)
    payload = load_json_payload(args.json, args.json_file)
    command = args.command
    if command == "analysis-paths" and isinstance(payload, dict) and payload.get("sources") and not payload.get("source"):
        command = "analysis-paths-batch"
    elif command == "analysis-paths-batch" and isinstance(payload, dict) and payload.get("source") and not payload.get("sources"):
        payload = dict(payload)
        payload["sources"] = [payload.pop("source")]
    endpoint = ANALYSIS_ENDPOINT_MAP[command]
    return request_json("POST", f"{base_url}{endpoint}", payload)


def dispatch_cli_command(
    args: Any,
    base_url: str,
    state_file: Path,
    ttl_seconds: int,
    *,
    request_json: Callable[[str, str, Optional[Dict[str, Any]]], Any],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    clear_schema_state: Callable[[Path], None],
    require_schema_state: Callable[[Path, str, int, str], None],
    emit_protocol_note: Callable[[str], None],
    run_templates: Dict[str, Dict[str, Any]],
    load_json_payload: Callable[[Optional[str], Optional[str]], Optional[Dict[str, Any]]],
    is_question_routed_plan: Callable[[Optional[Dict[str, Any]]], bool],
    is_question_shorthand_plan: Callable[[Optional[Dict[str, Any]]], bool],
    build_question_mode_run_response: Callable[[str, str, str, Path], Dict[str, Any]],
    execute_question_mode_run: Callable[..., Dict[str, Any]],
    execute_run_plan: Callable[[str, Dict[str, Any], Path], Dict[str, Any]],
) -> Any:
    """Dispatch one parsed CLI command through repo-owned command runtime."""
    simple_response = handle_simple_cli_command(
        args,
        base_url,
        state_file,
        ttl_seconds,
        request_json=request_json,
        summarize_schema=summarize_schema,
        write_schema_state=write_schema_state,
        clear_schema_state=clear_schema_state,
        require_schema_state=require_schema_state,
        emit_protocol_note=emit_protocol_note,
        run_templates=run_templates,
    )
    if simple_response is not None:
        return simple_response

    if args.command == "run":
        return handle_run_cli_command(
            args,
            base_url,
            state_file,
            load_json_payload=load_json_payload,
            is_question_routed_plan=is_question_routed_plan,
            is_question_shorthand_plan=is_question_shorthand_plan,
            build_question_mode_run_response=build_question_mode_run_response,
            execute_question_mode_run=execute_question_mode_run,
            execute_run_plan=execute_run_plan,
        )

    return handle_analysis_endpoint_cli_command(
        args,
        base_url,
        state_file,
        ttl_seconds,
        require_schema_state=require_schema_state,
        load_json_payload=load_json_payload,
        request_json=request_json,
    )
