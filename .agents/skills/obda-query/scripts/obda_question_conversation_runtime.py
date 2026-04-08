#!/usr/bin/env python3
"""Repo-owned conversation/runtime helpers for question-mode references."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional


def extract_focus_refs_from_response(
    response: Dict[str, Any],
    *,
    is_uri_like: Callable[[Any], bool],
    class_key_from_uri: Callable[[Any], Optional[str]],
    unique_preserve_order: Callable[[List[str]], List[str]],
) -> Dict[str, Any]:
    """Extract focus identifiers from one unit response for conversation state."""
    focus: Dict[str, Any] = {
        "entity_ids": [],
        "entity_names": [],
        "entity_uris": [],
        "entity_local_names": [],
        "entity_classes": [],
        "evidence_uris": [],
        "evidence_classes": [],
        "value_labels": [],
        "grain": None,
        "entity_class": None,
    }
    presentation = response.get("presentation")
    if not isinstance(presentation, dict):
        return focus

    def collect_entity_info(entity_info: Dict[str, Any]) -> None:
        if not isinstance(entity_info, dict):
            return
        if entity_info.get("display_id"):
            focus["entity_ids"].append(entity_info.get("display_id"))
        if entity_info.get("display_name"):
            focus["entity_names"].append(entity_info.get("display_name"))
        uri = entity_info.get("uri")
        if is_uri_like(uri):
            focus["entity_uris"].append(uri)
            class_key = class_key_from_uri(uri)
            if class_key:
                focus["entity_classes"].append(class_key)
        local_name = entity_info.get("local_name")
        if isinstance(local_name, str) and local_name:
            focus["entity_local_names"].append(local_name)
            class_key = class_key_from_uri(local_name)
            if class_key:
                focus["entity_classes"].append(class_key)

    def collect_evidence_info(evidence_info: Dict[str, Any]) -> None:
        if not isinstance(evidence_info, dict):
            return
        refs = evidence_info.get("refs")
        if not isinstance(refs, dict):
            return
        uri = refs.get("uri")
        if is_uri_like(uri):
            focus["evidence_uris"].append(uri)
            class_key = class_key_from_uri(uri)
            if class_key:
                focus["evidence_classes"].append(class_key)
        local_name = refs.get("local_name")
        if isinstance(local_name, str) and local_name:
            class_key = class_key_from_uri(local_name)
            if class_key:
                focus["evidence_classes"].append(class_key)

    entity = presentation.get("entity")
    if isinstance(entity, dict):
        focus["grain"] = "entity"
        collect_entity_info(entity)

    groups = presentation.get("groups")
    if isinstance(groups, list):
        focus["grain"] = "entity_set"
        for group in groups:
            if not isinstance(group, dict):
                continue
            collect_entity_info(group.get("entity"))
            for evidence_info in group.get("evidence", []):
                collect_evidence_info(evidence_info)

    items = presentation.get("items")
    if isinstance(items, list):
        if focus["grain"] is None:
            focus["grain"] = "rows"
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("value_label"):
                focus["value_labels"].append(item.get("value_label"))

    focus["entity_ids"] = unique_preserve_order([str(item) for item in focus["entity_ids"] if item])
    focus["entity_names"] = unique_preserve_order([str(item) for item in focus["entity_names"] if item])
    focus["entity_uris"] = unique_preserve_order([str(item) for item in focus["entity_uris"] if item])
    focus["entity_local_names"] = unique_preserve_order([str(item) for item in focus["entity_local_names"] if item])
    focus["entity_classes"] = unique_preserve_order([str(item) for item in focus["entity_classes"] if item])
    focus["evidence_uris"] = unique_preserve_order([str(item) for item in focus["evidence_uris"] if item])
    focus["evidence_classes"] = unique_preserve_order([str(item) for item in focus["evidence_classes"] if item])
    focus["value_labels"] = unique_preserve_order([str(item) for item in focus["value_labels"] if item])
    if focus["entity_classes"]:
        focus["entity_class"] = focus["entity_classes"][0]
    return focus


def build_conversation_state_entry(
    unit: Dict[str, Any],
    slots: Dict[str, Any],
    intent_ir: Dict[str, Any],
    response: Dict[str, Any],
    *,
    extract_focus_refs_from_response: Callable[[Dict[str, Any]], Dict[str, Any]],
    semantic_state_from_sources: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Build carry-forward conversation state for one executed/planned unit."""
    focus_refs = extract_focus_refs_from_response(response)
    semantic_state = semantic_state_from_sources(slots, intent_ir)
    return {
        "unit_id": unit.get("unit_id"),
        "anchors": deepcopy(semantic_state.get("anchors", [])),
        "has_anchor": bool(semantic_state.get("has_anchor")),
        "bootstrap_operator_hints": deepcopy(slots.get("bootstrap_operator_hints", []))
        if isinstance(slots.get("bootstrap_operator_hints"), list)
        else [],
        "bootstrap_signals": deepcopy(slots.get("bootstrap_signals", {}))
        if isinstance(slots.get("bootstrap_signals"), dict)
        else {},
        "bootstrap_candidates": deepcopy(slots.get("bootstrap_candidates", {}))
        if isinstance(slots.get("bootstrap_candidates"), dict)
        else {},
        "status_numeric_constraint": deepcopy(semantic_state.get("status_numeric_constraint")),
        "intent_ir": deepcopy(intent_ir),
        "focus": focus_refs,
        "query_family": response.get("query_family"),
        "effective_template": response.get("effective_template") or response.get("template"),
        "status": response.get("status"),
    }


def conversation_state_has_material_focus(state: Optional[Dict[str, Any]]) -> bool:
    """Whether a conversation state carries a concrete entity/value focus."""
    if not isinstance(state, dict):
        return False
    focus = state.get("focus")
    if not isinstance(focus, dict):
        return False
    for key in ("entity_uris", "entity_ids", "entity_local_names", "value_labels"):
        values = focus.get(key)
        if isinstance(values, list) and values:
            return True
    return False


def find_conversation_state_by_unit_id(
    conversation_states: List[Dict[str, Any]],
    unit_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Locate one conversation state entry by unit id."""
    if not isinstance(unit_id, str) or not unit_id:
        return None
    for state in conversation_states:
        if isinstance(state, dict) and state.get("unit_id") == unit_id:
            return state
    return None


def resolve_reference_context(
    unit: Dict[str, Any],
    conversation_states: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Resolve lightweight discourse references against executed conversation state."""
    if not isinstance(unit, dict):
        return None
    dependency = unit.get("dependency")
    reference_markers = unit.get("reference_markers", [])
    candidate_states: List[Dict[str, Any]] = []

    if isinstance(dependency, dict) and isinstance(dependency.get("depends_on"), str):
        matched = find_conversation_state_by_unit_id(conversation_states, dependency.get("depends_on"))
        if isinstance(matched, dict):
            candidate_states.append(matched)

    for state in reversed(conversation_states):
        if not isinstance(state, dict):
            continue
        if any(existing.get("unit_id") == state.get("unit_id") for existing in candidate_states if isinstance(existing, dict)):
            continue
        candidate_states.append(state)

    selected_state = next((state for state in candidate_states if conversation_state_has_material_focus(state)), None)
    if not isinstance(selected_state, dict):
        return None

    focus = selected_state.get("focus")
    if not isinstance(focus, dict):
        return None

    return {
        "from_unit_id": selected_state.get("unit_id"),
        "markers": list(reference_markers) if isinstance(reference_markers, list) else [],
        "entity_ids": deepcopy(focus.get("entity_ids", [])),
        "entity_names": deepcopy(focus.get("entity_names", [])),
        "entity_uris": deepcopy(focus.get("entity_uris", [])),
        "entity_local_names": deepcopy(focus.get("entity_local_names", [])),
        "entity_class": focus.get("entity_class"),
        "evidence_uris": deepcopy(focus.get("evidence_uris", [])),
        "evidence_classes": deepcopy(focus.get("evidence_classes", [])),
        "grain": focus.get("grain"),
        "query_family": selected_state.get("query_family"),
        "effective_template": selected_state.get("effective_template"),
        "status": selected_state.get("status"),
    }


def apply_resolved_reference_to_slots(
    slots: Dict[str, Any],
    resolved_reference: Optional[Dict[str, Any]],
    *,
    unique_preserve_order: Callable[[List[str]], List[str]],
) -> Dict[str, Any]:
    """Attach resolved conversation references to slot state for later grounding/lowering."""
    merged = dict(slots)
    for key in (
        "reference_entity_ids",
        "reference_entity_names",
        "reference_entity_uris",
        "reference_entity_local_names",
        "reference_entity_class",
        "reference_evidence_uris",
        "reference_evidence_classes",
        "reference_grain",
        "reference_from_unit_id",
        "resolved_reference",
    ):
        merged.pop(key, None)

    if merged.get("has_explicit_anchor"):
        return merged

    if not isinstance(resolved_reference, dict):
        return merged

    entity_ids = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_ids", [])
        if isinstance(item, str) and item
    ])
    entity_names = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_names", [])
        if isinstance(item, str) and item
    ])
    entity_uris = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_uris", [])
        if isinstance(item, str) and item
    ])
    entity_local_names = unique_preserve_order([
        str(item) for item in resolved_reference.get("entity_local_names", [])
        if isinstance(item, str) and item
    ])
    evidence_uris = unique_preserve_order([
        str(item) for item in resolved_reference.get("evidence_uris", [])
        if isinstance(item, str) and item
    ])
    evidence_classes = unique_preserve_order([
        str(item) for item in resolved_reference.get("evidence_classes", [])
        if isinstance(item, str) and item
    ])
    if not any((entity_ids, entity_names, entity_uris, entity_local_names)):
        return merged

    merged["reference_entity_ids"] = entity_ids
    merged["reference_entity_names"] = entity_names
    merged["reference_entity_uris"] = entity_uris
    merged["reference_entity_local_names"] = entity_local_names
    merged["reference_entity_class"] = resolved_reference.get("entity_class")
    merged["reference_evidence_uris"] = evidence_uris
    merged["reference_evidence_classes"] = evidence_classes
    merged["reference_grain"] = resolved_reference.get("grain")
    merged["reference_from_unit_id"] = resolved_reference.get("from_unit_id")
    merged["resolved_reference"] = deepcopy(resolved_reference)
    return merged
