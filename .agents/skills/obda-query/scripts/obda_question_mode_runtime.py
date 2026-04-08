#!/usr/bin/env python3
"""Repo-owned runtime orchestration for question-mode batch/single execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional

from obda_intent_parser import parse_question_utterance
from obda_question_mode_contracts import (
    apply_bounded_recovery_contract_to_batch_response,
    apply_fail_closed_contract_to_question_response,
    apply_fail_closed_contract_to_batch_response,
    build_execution_dag,
    build_question_batch_presentation,
    compute_batch_execution_status,
    evaluate_dependency_condition,
)


def build_question_batch_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    *,
    request_schema: Callable[[str], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    merge_inherited_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]], Dict[str, Any]],
    build_question_unit_intent_ir: Callable[..., Dict[str, Any]],
    build_single_question_mode_run_response: Callable[..., Dict[str, Any]],
    build_conversation_state_entry: Callable[..., Dict[str, Any]],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    parsed_utterance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Plan a multi-question utterance as a batch of QuestionUnits."""
    schema = request_schema(base_url)
    write_schema_state(state_file, base_url)

    parsed_utterance = (
        parsed_utterance
        if isinstance(parsed_utterance, dict)
        else parse_question_utterance(question, template)
    )
    question_units = parsed_utterance.get("question_units", [])
    unit_parses = parsed_utterance.get("unit_parses", [])
    unit_plans = []
    conversation_states = []
    inherited_context = None

    for parsed_unit in unit_parses:
        unit = parsed_unit.get("question_unit") if isinstance(parsed_unit, dict) else None
        if not isinstance(unit, dict):
            continue
        base_slots = parsed_unit.get("slots") if isinstance(parsed_unit.get("slots"), dict) else {}
        slots = merge_inherited_slots(dict(base_slots), inherited_context, unit)
        intent_ir = build_question_unit_intent_ir(unit, slots, template)
        response = build_single_question_mode_run_response(
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
            "dependency": unit.get("dependency"),
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
    *,
    request_schema: Callable[[str], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    merge_inherited_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]], Dict[str, Any]],
    build_question_unit_intent_ir: Callable[..., Dict[str, Any]],
    build_single_question_mode_run_response: Callable[..., Dict[str, Any]],
    build_conversation_state_entry: Callable[..., Dict[str, Any]],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Return a planning bundle for question-mode single or batch utterances."""
    parsed_utterance = parse_question_utterance(question, template)
    question_units = parsed_utterance.get("question_units", [])
    if len(question_units) > 1:
        return build_question_batch_run_response(
            base_url,
            question,
            template,
            state_file,
            request_schema=request_schema,
            write_schema_state=write_schema_state,
            merge_inherited_slots=merge_inherited_slots,
            build_question_unit_intent_ir=build_question_unit_intent_ir,
            build_single_question_mode_run_response=build_single_question_mode_run_response,
            build_conversation_state_entry=build_conversation_state_entry,
            summarize_schema=summarize_schema,
            parsed_utterance=parsed_utterance,
        )
    single_unit = question_units[0] if question_units else {"unit_id": "q1", "text": question, "reference_markers": []}
    parsed_unit = (
        parsed_utterance.get("unit_parses", [None])[0]
        if isinstance(parsed_utterance.get("unit_parses"), list) and parsed_utterance.get("unit_parses")
        else None
    )
    slots = dict(parsed_unit.get("slots", {})) if isinstance(parsed_unit, dict) else {}
    intent_ir = build_question_unit_intent_ir(single_unit, slots, template)
    return build_single_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        slots_override=slots,
        unit_intent_ir=intent_ir,
    )


def execute_question_batch_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    *,
    include_planner_debug: bool,
    request_schema: Callable[[str], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    merge_inherited_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]], Dict[str, Any]],
    resolve_reference_context: Callable[[Dict[str, Any], list[Dict[str, Any]]], Optional[Dict[str, Any]]],
    apply_resolved_reference_to_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Dict[str, Any]],
    build_question_unit_intent_ir: Callable[..., Dict[str, Any]],
    build_single_question_mode_run_response: Callable[..., Dict[str, Any]],
    execute_single_question_mode_run: Callable[..., Dict[str, Any]],
    build_conversation_state_entry: Callable[..., Dict[str, Any]],
    sparql_row_count: Callable[[Optional[Dict[str, Any]]], int],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    planning: Optional[Dict[str, Any]] = None,
    parsed_utterance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute a multi-question utterance via an Execution DAG."""
    parsed_utterance = (
        parsed_utterance
        if isinstance(parsed_utterance, dict)
        else parse_question_utterance(question, template)
    )
    planning = (
        planning
        if isinstance(planning, dict)
        else build_question_batch_run_response(
            base_url,
            question,
            template,
            state_file,
            request_schema=request_schema,
            write_schema_state=write_schema_state,
            merge_inherited_slots=merge_inherited_slots,
            build_question_unit_intent_ir=build_question_unit_intent_ir,
            build_single_question_mode_run_response=build_single_question_mode_run_response,
            build_conversation_state_entry=build_conversation_state_entry,
            summarize_schema=summarize_schema,
            parsed_utterance=parsed_utterance,
        )
    )
    schema = request_schema(base_url)
    write_schema_state(state_file, base_url)
    parsed_units_by_id = {
        unit.get("question_unit", {}).get("unit_id"): unit
        for unit in parsed_utterance.get("unit_parses", [])
        if isinstance(unit, dict) and isinstance(unit.get("question_unit"), dict)
    }
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
            if not evaluate_dependency_condition(
                dependency.get("condition"),
                dependency_response,
                sparql_row_count,
            ):
                should_execute = False
                blocked_reason = {
                    "depends_on": dependency.get("depends_on"),
                    "condition": dependency.get("condition"),
                    "reason": "dependency_condition_not_met",
                }

        parsed_unit = parsed_units_by_id.get(unit_id) if isinstance(unit_id, str) else None
        base_slots = (
            parsed_unit.get("slots")
            if isinstance(parsed_unit, dict) and isinstance(parsed_unit.get("slots"), dict)
            else {}
        )
        slots = merge_inherited_slots(dict(base_slots), inherited_context, unit)
        resolved_reference = resolve_reference_context(unit, conversation_states)
        slots = apply_resolved_reference_to_slots(slots, resolved_reference)
        intent_ir = build_question_unit_intent_ir(
            unit,
            slots,
            template,
            resolved_reference=resolved_reference,
        )
        planned_response = build_single_question_mode_run_response(
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
            response = execute_single_question_mode_run(
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
    elif any(
        isinstance(unit, dict)
        and isinstance(unit.get("response"), dict)
        and unit["response"].get("status") == "empty_result"
        for unit in unit_results
    ):
        batch_response = apply_bounded_recovery_contract_to_batch_response(batch_response)
    return batch_response


def execute_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    *,
    include_planner_debug: bool,
    request_schema: Callable[[str], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    merge_inherited_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]], Dict[str, Any]],
    resolve_reference_context: Callable[[Dict[str, Any], list[Dict[str, Any]]], Optional[Dict[str, Any]]],
    apply_resolved_reference_to_slots: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Dict[str, Any]],
    build_question_unit_intent_ir: Callable[..., Dict[str, Any]],
    build_single_question_mode_run_response: Callable[..., Dict[str, Any]],
    execute_single_question_mode_run: Callable[..., Dict[str, Any]],
    build_conversation_state_entry: Callable[..., Dict[str, Any]],
    sparql_row_count: Callable[[Optional[Dict[str, Any]]], int],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Plan and execute question-mode single or batch utterances."""
    parsed_utterance = parse_question_utterance(question, template)
    question_units = parsed_utterance.get("question_units", [])
    if len(question_units) > 1:
        return execute_question_batch_run(
            base_url,
            question,
            template,
            state_file,
            include_planner_debug=include_planner_debug,
            request_schema=request_schema,
            write_schema_state=write_schema_state,
            merge_inherited_slots=merge_inherited_slots,
            resolve_reference_context=resolve_reference_context,
            apply_resolved_reference_to_slots=apply_resolved_reference_to_slots,
            build_question_unit_intent_ir=build_question_unit_intent_ir,
            build_single_question_mode_run_response=build_single_question_mode_run_response,
            execute_single_question_mode_run=execute_single_question_mode_run,
            build_conversation_state_entry=build_conversation_state_entry,
            sparql_row_count=sparql_row_count,
            summarize_schema=summarize_schema,
            parsed_utterance=parsed_utterance,
        )
    single_unit = question_units[0] if question_units else {"unit_id": "q1", "text": question, "reference_markers": []}
    parsed_unit = (
        parsed_utterance.get("unit_parses", [None])[0]
        if isinstance(parsed_utterance.get("unit_parses"), list) and parsed_utterance.get("unit_parses")
        else None
    )
    slots = dict(parsed_unit.get("slots", {})) if isinstance(parsed_unit, dict) else {}
    intent_ir = build_question_unit_intent_ir(single_unit, slots, template)
    planning = build_single_question_mode_run_response(
        base_url,
        question,
        template,
        state_file,
        slots_override=slots,
        unit_intent_ir=intent_ir,
    )
    return execute_single_question_mode_run(
        base_url,
        question,
        template,
        state_file,
        include_planner_debug=include_planner_debug,
        planning_override=planning,
    )
