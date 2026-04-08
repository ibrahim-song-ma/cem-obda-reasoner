#!/usr/bin/env python3
"""Repo-owned contracts for question-mode batch/single response handling."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional


def summarize_batch_unit_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Return a compact per-unit summary for multi-answer presentation."""
    summary = {
        "status": response.get("status"),
        "template": response.get("effective_template") or response.get("template"),
        "query_family": response.get("query_family"),
    }
    if isinstance(response.get("presentation"), dict):
        presentation = response["presentation"]
        if isinstance(presentation.get("summary"), dict):
            summary["result_summary"] = presentation["summary"]
    planner = response.get("planner")
    if isinstance(planner, dict) and planner.get("reason"):
        summary["planner_reason"] = planner.get("reason")
    if response.get("blocked_reason"):
        summary["blocked_reason"] = response.get("blocked_reason")
    return summary


def evaluate_dependency_condition(
    condition_type: Optional[str],
    dependency_response: Optional[Dict[str, Any]],
    row_count_fn: Callable[[Optional[Dict[str, Any]]], int],
) -> bool:
    """Evaluate whether a dependency condition is satisfied."""
    if not isinstance(dependency_response, dict):
        return False

    status = dependency_response.get("status")
    presentation = dependency_response.get("presentation")
    material_result = False
    if isinstance(presentation, dict) and isinstance(presentation.get("summary"), dict):
        summary = presentation["summary"]
        for key in ("entity_count", "record_count", "value_count", "evidence_count"):
            value = summary.get(key)
            if isinstance(value, int) and value > 0:
                material_result = True
                break
    if not material_result and isinstance(dependency_response.get("sparql"), dict):
        material_result = row_count_fn(dependency_response.get("sparql")) > 0

    if condition_type == "empty_or_false":
        return status in ("empty_result", "planning_required") or not material_result
    return status in ("ok", "partial_success") and material_result


def build_execution_dag(question_units: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Represent QuestionUnit dependencies as a small execution DAG."""
    nodes = []
    edges = []
    for unit in question_units:
        nodes.append({
            "unit_id": unit.get("unit_id"),
            "text": unit.get("text"),
        })
        dependency = unit.get("dependency")
        if isinstance(dependency, dict) and dependency.get("depends_on"):
            edges.append({
                "from": dependency.get("depends_on"),
                "to": unit.get("unit_id"),
                "condition": dependency.get("condition"),
            })
    return {"nodes": nodes, "edges": edges}


def build_question_batch_presentation(
    utterance: str,
    unit_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a structured multi-answer presentation for batch execution."""
    executed_count = 0
    blocked_count = 0
    planning_required_count = 0
    items = []

    for item in unit_results:
        response = item.get("response", {})
        status = response.get("status")
        if status in ("ok", "partial_success", "empty_result"):
            executed_count += 1
        elif status == "skipped":
            blocked_count += 1
        elif status == "planning_required":
            planning_required_count += 1

        items.append({
            "unit_id": item.get("unit_id"),
            "text": item.get("text"),
            "status": status,
            "summary": summarize_batch_unit_response(response if isinstance(response, dict) else {}),
        })

    return {
        "template": "question_batch",
        "summary": {
            "utterance": utterance,
            "unit_count": len(unit_results),
            "executed_count": executed_count,
            "blocked_count": blocked_count,
            "planning_required_count": planning_required_count,
        },
        "items": items,
        "answer_contract": {
            "version": "question_batch_v1",
            "preferred_section_order": ["summary", "unit_answers"],
        },
    }


def compute_batch_execution_status(unit_results: List[Dict[str, Any]]) -> str:
    """Summarize overall batch execution status without overstating success."""
    statuses = [
        item.get("response", {}).get("status")
        for item in unit_results
        if isinstance(item, dict) and isinstance(item.get("response"), dict)
    ]
    if any(status in ("ok", "partial_success", "empty_result") for status in statuses):
        return "batch_executed"
    if any(status == "planning_required" for status in statuses):
        return "planning_required"
    if any(status == "skipped" for status in statuses):
        return "skipped"
    return "planning_required"


def apply_fail_closed_contract_to_question_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Strip executable-looking scaffolding from non-executable question-mode responses."""
    contracted = dict(response)
    contracted["plan_executable"] = False
    contracted["manual_fallback_allowed"] = False
    contracted["planner_bundle_available_via_plan_only"] = True
    contracted["next_action"] = "stop_or_report_planning_required"
    planner = contracted.get("planner")
    if isinstance(planner, dict) and isinstance(planner.get("clarification_hint"), dict):
        contracted["clarification_hint"] = deepcopy(planner["clarification_hint"])
    clarification_hint = contracted.get("clarification_hint")
    if isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification"):
        contracted["next_action"] = "ask_user_for_clarification"
        contracted["user_clarification_prompt"] = clarification_hint.get("user_clarification_prompt")
    contracted["recovery_policy"] = {
        "mode": "fail_closed",
        "manual_exploration_allowed": False,
        "bounded_recovery_allowed": bool(contracted.get("recovery_hint")),
        "requires_plan_only_for_debug": False,
        "requires_user_clarification": bool(
            isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification")
        ),
    }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    rule = (
        "If question-mode returns planning_required and no recovery_hint is present, stop. "
        "Do not switch to manual sparql/sample exploration in the same turn."
    )
    if rule not in rules:
        rules.append(rule)
    contracted["rules"] = rules
    contracted.pop("plan_skeleton", None)
    contracted.pop("required_fields", None)
    return contracted


def apply_fail_closed_contract_to_batch_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Surface top-level clarification guidance for batch question-mode failures."""
    contracted = dict(response)
    contracted["manual_fallback_allowed"] = False
    contracted["planner_bundle_available_via_plan_only"] = True
    contracted["next_action"] = "stop_or_report_planning_required"
    clarification_hint = None
    clarification_unit_id = None
    for unit in contracted.get("question_units", []):
        if not isinstance(unit, dict):
            continue
        unit_response = unit.get("response")
        if not isinstance(unit_response, dict):
            continue
        unit_hint = unit_response.get("clarification_hint")
        if isinstance(unit_hint, dict):
            clarification_hint = deepcopy(unit_hint)
            clarification_unit_id = unit.get("unit_id")
            break
    if isinstance(clarification_hint, dict):
        contracted["clarification_hint"] = clarification_hint
        if isinstance(clarification_unit_id, str) and clarification_unit_id:
            contracted["clarification_target_unit"] = clarification_unit_id
        if clarification_hint.get("requires_user_clarification"):
            contracted["next_action"] = "ask_user_for_clarification"
            contracted["user_clarification_prompt"] = clarification_hint.get("user_clarification_prompt")
    contracted["recovery_policy"] = {
        "mode": "fail_closed",
        "manual_exploration_allowed": False,
        "bounded_recovery_allowed": False,
        "requires_plan_only_for_debug": False,
        "requires_user_clarification": bool(
            isinstance(clarification_hint, dict) and clarification_hint.get("requires_user_clarification")
        ),
    }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    rule = (
        "If question-mode returns planning_required and no recovery_hint is present, stop. "
        "Do not switch to manual sparql/sample exploration in the same turn."
    )
    if rule not in rules:
        rules.append(rule)
    contracted["rules"] = rules
    return contracted


def apply_bounded_recovery_contract_to_question_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Constrain empty-result question responses to one bounded recovery path at most."""
    contracted = dict(response)
    contracted["manual_fallback_allowed"] = False
    recovery_hint = contracted.get("recovery_hint")
    if isinstance(recovery_hint, dict):
        contracted["next_action"] = "follow_recovery_hint_or_report_no_match"
        contracted["bounded_recovery_contract"] = {
            "same_question_only": True,
            "same_constraint_only": True,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "allow_open_ended_exploration": False,
            "max_samples": recovery_hint.get("max_samples", 1),
            "max_reruns": 1,
        }
        contracted["recovery_policy"] = {
            "mode": "bounded_recovery",
            "manual_exploration_allowed": False,
            "bounded_recovery_allowed": True,
            "requires_same_question_rerun": True,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "max_reruns": 1,
        }
    else:
        contracted["next_action"] = "report_no_match"
        contracted["recovery_policy"] = {
            "mode": "terminal_empty_result",
            "manual_exploration_allowed": False,
            "bounded_recovery_allowed": False,
            "requires_same_question_rerun": False,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "max_reruns": 0,
        }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    bounded_rule = (
        "If question-mode returns empty_result with recovery_hint, do at most one targeted grounding sample "
        "for the same metric/question, then rerun the same question once. Do not probe alternate metrics, "
        "grep schema for sibling attributes, or switch to manual sparql/sample exploration."
    )
    terminal_rule = (
        "If question-mode returns empty_result without recovery_hint, report no matches and stop. "
        "Do not broaden the metric, threshold, or entity scope on your own."
    )
    if bounded_rule not in rules:
        rules.append(bounded_rule)
    if terminal_rule not in rules:
        rules.append(terminal_rule)
    contracted["rules"] = rules
    return contracted


def apply_bounded_recovery_contract_to_batch_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Propagate one empty-result recovery contract to the batch top level."""
    contracted = dict(response)
    contracted["manual_fallback_allowed"] = False
    recovery_hint = None
    recovery_unit_id = None
    for unit in contracted.get("question_units", []):
        if not isinstance(unit, dict):
            continue
        unit_response = unit.get("response")
        if not isinstance(unit_response, dict):
            continue
        if unit_response.get("status") != "empty_result":
            continue
        unit_hint = unit_response.get("recovery_hint")
        if isinstance(unit_hint, dict):
            recovery_hint = deepcopy(unit_hint)
            recovery_unit_id = unit.get("unit_id")
            break
    if isinstance(recovery_hint, dict):
        contracted["recovery_hint"] = recovery_hint
        contracted["next_action"] = "follow_recovery_hint_or_report_no_match"
        contracted["bounded_recovery_contract"] = {
            "same_question_only": True,
            "same_constraint_only": True,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "allow_open_ended_exploration": False,
            "max_samples": recovery_hint.get("max_samples", 1),
            "max_reruns": 1,
        }
        if isinstance(recovery_unit_id, str) and recovery_unit_id:
            contracted["recovery_target_unit"] = recovery_unit_id
        contracted["recovery_policy"] = {
            "mode": "bounded_recovery",
            "manual_exploration_allowed": False,
            "bounded_recovery_allowed": True,
            "requires_same_question_rerun": True,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "max_reruns": 1,
        }
    else:
        contracted["next_action"] = "report_no_match"
        contracted["recovery_policy"] = {
            "mode": "terminal_empty_result",
            "manual_exploration_allowed": False,
            "bounded_recovery_allowed": False,
            "requires_same_question_rerun": False,
            "allow_metric_rewrite": False,
            "allow_alternate_metric_probe": False,
            "max_reruns": 0,
        }
    rules = list(contracted.get("rules", [])) if isinstance(contracted.get("rules"), list) else []
    bounded_rule = (
        "If batch question-mode returns empty_result with recovery_hint, do at most one targeted grounding sample "
        "for the same metric/question, then rerun the same question once. Do not probe alternate metrics, "
        "grep schema for sibling attributes, or launch new exploratory runs."
    )
    terminal_rule = (
        "If batch question-mode returns only empty_result/skipped outcomes without a recovery_hint, report no matches "
        "and stop. Do not broaden the metric, threshold, or entity scope on your own."
    )
    if bounded_rule not in rules:
        rules.append(bounded_rule)
    if terminal_rule not in rules:
        rules.append(terminal_rule)
    contracted["rules"] = rules
    return contracted
