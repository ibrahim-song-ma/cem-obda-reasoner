#!/usr/bin/env python3
"""Repo-owned planner compiler helpers for request IR, node plans, and selection."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from obda_grounding_contracts import (
    grounding_constraint_record,
    grounding_constraint_requested_text,
)
from obda_ir_contracts import (
    build_request_ir_record,
    intent_ir_references_record,
    request_ir_anchor_forms,
    request_ir_effective_template,
    request_ir_output_record,
    request_ir_query_family,
    request_ir_references_record,
    request_ir_summary_record,
)


def first_nonempty_text(*values: Any) -> Optional[str]:
    """Return the first non-empty string value."""
    for value in values:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
    return None


def build_semantic_request_ir(
    question: str,
    template: str,
    routing: Dict[str, Any],
    grounding_bundle: Dict[str, Any],
    source_info: Dict[str, Any],
    evidence_candidates: List[Dict[str, Any]],
    semantic_state: Dict[str, Any],
    *,
    anchors: Optional[List[Dict[str, Any]]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a planner-facing semantic request IR from routing + grounding results."""
    source_selected = source_info.get("selected") if isinstance(source_info, dict) else None
    constraints: List[Dict[str, Any]] = []
    references: Dict[str, Any] = {}

    cause_grounding = grounding_constraint_record(grounding_bundle, "cause_text")
    action_grounding = grounding_constraint_record(grounding_bundle, "action_or_state_text")
    status_grounding = grounding_constraint_record(grounding_bundle, "status_or_problem_text")

    cause_text = first_nonempty_text(
        cause_grounding.get("requested_text"),
        semantic_state.get("cause_text"),
    )
    if isinstance(cause_text, str) and cause_text:
        constraints.append({
            "kind": "semantic_constraint",
            "intent": "cause",
            "raw_text": cause_text,
            "grounding": {
                "effective_text": cause_grounding.get("effective_text"),
                "binding_terms": cause_grounding.get("binding_terms", []),
                "top_node_id": cause_grounding.get("top_node_id"),
                "top_node_type": cause_grounding.get("top_node_type"),
                "has_binding": bool(cause_grounding.get("has_binding")),
            },
        })

    action_text = first_nonempty_text(
        action_grounding.get("requested_text"),
        semantic_state.get("action_text"),
    )
    if isinstance(action_text, str) and action_text:
        constraints.append({
            "kind": "semantic_constraint",
            "intent": "action_or_state",
            "raw_text": action_text,
            "grounding": {
                "effective_text": action_grounding.get("effective_text"),
                "binding_terms": action_grounding.get("binding_terms", []),
                "top_node_id": action_grounding.get("top_node_id"),
                "top_node_type": action_grounding.get("top_node_type"),
                "has_binding": bool(action_grounding.get("has_binding")),
            },
        })

    status_or_problem_text = first_nonempty_text(
        status_grounding.get("requested_text"),
        semantic_state.get("status_or_problem_text"),
    )
    numeric_constraint = semantic_state.get("status_numeric_constraint")
    if isinstance(status_or_problem_text, str) and status_or_problem_text:
        status_constraint = {
            "kind": "semantic_constraint",
            "intent": "status_or_problem",
            "raw_text": status_or_problem_text,
            "grounding": {
                "effective_text": status_grounding.get("effective_text"),
                "binding_terms": status_grounding.get("binding_terms", []),
                "top_node_id": status_grounding.get("top_node_id"),
                "top_node_type": status_grounding.get("top_node_type"),
                "has_binding": bool(status_grounding.get("has_binding")),
            },
            "constraint_mode": (
                "status_check"
                if semantic_state.get("status_check_requested")
                else "problem_text"
            ),
        }
        if isinstance(numeric_constraint, dict):
            status_constraint["comparison"] = {
                "op": numeric_constraint.get("op"),
                "value": numeric_constraint.get("value"),
            }
        constraints.append(status_constraint)

    parser_references = intent_ir_references_record(unit_intent_ir)
    if isinstance(parser_references, dict):
        references = deepcopy(parser_references)
    resolved_reference = references.get("resolved")
    if not isinstance(resolved_reference, dict):
        resolved_reference = semantic_state.get("resolved_reference")
    if isinstance(resolved_reference, dict):
        references["resolved"] = deepcopy(resolved_reference)

    target_grounding = grounding_constraint_record(grounding_bundle, "target_text")
    target_constraint_text = first_nonempty_text(
        target_grounding.get("requested_text"),
        semantic_state.get("target_text"),
    )
    target_projection_requested = (
        isinstance(target_constraint_text, str)
        and bool(target_constraint_text.strip())
        and not bool(target_grounding.get("has_binding"))
        and (
            bool(semantic_state.get("status_check_requested"))
            or isinstance(cause_text, str)
            or isinstance(action_text, str)
        )
    )

    return build_request_ir_record(
        question=question,
        requested_template=template,
        effective_template=routing.get("effective_template", template),
        query_family=routing.get("family", template),
        routing_rationale=routing.get("rationale", []),
        anchors=anchors or [],
        slot_inputs=grounding_bundle.get("slot_inputs", []),
        slot_bindings=grounding_bundle.get("slot_bindings", []),
        grounded_constraints={
            key: grounding_constraint_record(grounding_bundle, key)
            for key in ("cause_text", "action_or_state_text", "status_or_problem_text", "target_text")
        },
        source={
            "selected_class": source_selected.get("class_name") if isinstance(source_selected, dict) else None,
            "candidates": source_info.get("candidates", []) if isinstance(source_info, dict) else [],
        },
        evidence={
            "candidate_classes": [
                item.get("class_name")
                for item in evidence_candidates
                if isinstance(item, dict) and isinstance(item.get("class_name"), str)
            ],
        },
        references=references,
        constraints=constraints,
        output={
            "grain": (
                "entity"
                if routing.get("family") in ("anchored_fact_lookup", "anchored_causal_lookup", "causal_lookup")
                else "entity_set" if routing.get("family") == "causal_enumeration" else "rows"
            ),
            "needs_analysis": routing.get("effective_template") in ("causal_lookup", "causal_enumeration"),
            "asks_solution": bool(semantic_state.get("asks_solution")),
            "asks_explanation": bool(semantic_state.get("asks_explanation")),
            "target_projection_requested": target_projection_requested,
        },
    )


def build_node_plan(
    request_ir: Dict[str, Any],
    source_class: str,
    evidence_class: str,
    relation_info: Optional[Dict[str, Any]],
    include_cause: bool,
    include_action: bool,
    include_status: bool = False,
    separate_action_support: bool = False,
) -> Dict[str, Any]:
    """Build a node-based compiler plan representation for debugging and validation."""
    nodes: List[Dict[str, Any]] = [{
        "type": "SourceScan",
        "class": source_class,
        "var": "source",
    }]

    anchors = request_ir_anchor_forms(request_ir)
    if isinstance(anchors, list):
        for anchor in anchors:
            if not isinstance(anchor, dict):
                continue
            nodes.append({
                "type": "AnchorResolve",
                "anchor_kind": anchor.get("kind"),
                "anchor_value": anchor.get("value"),
            })

    resolved_reference = None
    references = request_ir_references_record(request_ir)
    if isinstance(references, dict) and isinstance(references.get("resolved"), dict):
        resolved_reference = references["resolved"]
    if isinstance(resolved_reference, dict):
        reference_entity_uris = [
            str(item)
            for item in resolved_reference.get("entity_uris", [])
            if isinstance(item, str) and item
        ]
        if reference_entity_uris:
            nodes.append({
                "type": "ReferenceFilter",
                "scope": "source",
                "from_unit_id": resolved_reference.get("from_unit_id"),
                "entity_class": resolved_reference.get("entity_class"),
                "entity_count": len(reference_entity_uris),
                "grain": resolved_reference.get("grain"),
            })

    if relation_info and relation_info.get("property"):
        nodes.append({
            "type": "EvidenceTraverse",
            "from_var": "source",
            "to_var": "evidence",
            "property": relation_info.get("property"),
            "direction": relation_info.get("direction", "forward"),
            "evidence_class": evidence_class,
        })
    else:
        nodes.append({
            "type": "EvidenceScan",
            "class": evidence_class,
            "var": "evidence",
        })

    if include_cause:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "cause",
            "scope": "evidence",
        })
    if include_status:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "status_or_problem",
            "scope": "evidence",
        })
    if include_action:
        nodes.append({
            "type": "ConstraintFilter",
            "intent": "action_or_state",
            "scope": "support_evidence" if separate_action_support else "evidence",
        })

    request_output = request_ir_output_record(request_ir)
    if request_output.get("needs_analysis"):
        nodes.append({
            "type": "AnalyzerRequest",
            "analysis_kind": "paths-batch"
            if request_ir_effective_template(request_ir) == "causal_enumeration"
            else "paths",
        })

    nodes.append({
        "type": "Project",
        "grain": request_output.get("grain"),
    })
    return {
        "query_family": request_ir_query_family(request_ir),
        "nodes": nodes,
    }


def select_compiled_plan(
    candidate_plans: List[Dict[str, Any]],
    request_ir: Dict[str, Any],
    semantic_state: Dict[str, Any],
    target_grounding: Dict[str, Any],
    effective_template: str,
) -> Dict[str, Any]:
    """Select the executable compiled plan or return a fail-closed planner reason."""
    request_output = request_ir_output_record(request_ir)
    explicit_target_text = first_nonempty_text(
        grounding_constraint_requested_text({"grounded_constraints": {"target_text": target_grounding}}, "target_text"),
        semantic_state.get("target_text"),
    )

    underconstrained_target_projection = (
        effective_template == "enumeration"
        and bool(semantic_state.get("asks_explanation"))
        and isinstance(explicit_target_text, str)
        and bool(explicit_target_text.strip())
        and not bool(target_grounding.get("has_binding"))
        and bool(candidate_plans)
        and all("target_role_fallback" in item.get("rationale", []) for item in candidate_plans)
    )
    underconstrained_target_lookup = (
        effective_template == "causal_lookup"
        and isinstance(explicit_target_text, str)
        and bool(explicit_target_text.strip())
        and not bool(target_grounding.get("has_binding"))
        and not bool(semantic_state.get("status_check_requested"))
        and not bool(first_nonempty_text(semantic_state.get("cause_text"), semantic_state.get("action_text")))
        and bool(candidate_plans)
    )
    underconstrained_unbound_target_enumeration = (
        effective_template == "enumeration"
        and isinstance(explicit_target_text, str)
        and bool(explicit_target_text.strip())
        and bool(candidate_plans)
        and (
            not bool(target_grounding.get("has_binding"))
            or all("target_role_fallback" in item.get("rationale", []) for item in candidate_plans)
        )
    )
    selected_plan = None if (
        underconstrained_target_projection
        or underconstrained_target_lookup
        or underconstrained_unbound_target_enumeration
    ) else (candidate_plans[0]["plan"] if candidate_plans else None)
    ready = selected_plan is not None
    reason = None if ready else (
        "target_projection_underconstrained"
        if underconstrained_target_projection
        else "no_executable_candidate_plan"
    )
    return {
        "selected_plan": selected_plan,
        "ready": ready,
        "reason": reason,
        "target_projection_requested": bool(request_output.get("target_projection_requested")),
    }


def summarize_planner_result(planner: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return a compact summary of planner/compiler output."""
    if not isinstance(planner, dict):
        return None

    summary: Dict[str, Any] = {
        "mode": planner.get("mode"),
        "ready": bool(planner.get("ready")),
    }
    if planner.get("query_family"):
        summary["query_family"] = planner.get("query_family")
    if planner.get("requested_template"):
        summary["requested_template"] = planner.get("requested_template")
    if planner.get("effective_template"):
        summary["effective_template"] = planner.get("effective_template")
    if isinstance(planner.get("semantic_manifest_summary"), dict):
        summary["semantic_manifest_summary"] = planner.get("semantic_manifest_summary")

    slots = planner.get("slots")
    if isinstance(slots, dict):
        summary["slots"] = slots

    request_ir = planner.get("request_ir")
    if isinstance(request_ir, dict):
        summary["request_ir"] = request_ir_summary_record(request_ir)

    candidate_plans = planner.get("candidate_plans")
    if isinstance(candidate_plans, list):
        summary["candidate_count"] = len(candidate_plans)
        if candidate_plans and isinstance(candidate_plans[0], dict):
            selected_candidate = candidate_plans[0]
            summary["selected_variant"] = selected_candidate.get("variant")
            summary["selected_confidence_score"] = selected_candidate.get("confidence_score")
            if selected_candidate.get("query_family"):
                summary["selected_query_family"] = selected_candidate.get("query_family")
            summary["source_class"] = selected_candidate.get("source_class")
            summary["evidence_class"] = selected_candidate.get("evidence_class")
            rationale = selected_candidate.get("rationale")
            if isinstance(rationale, list):
                summary["rationale"] = rationale
            if isinstance(selected_candidate.get("node_plan"), dict):
                summary["selected_node_plan"] = selected_candidate.get("node_plan")

    return summary
