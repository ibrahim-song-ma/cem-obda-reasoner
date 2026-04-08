#!/usr/bin/env python3
"""Repo-owned grounding contracts between parser, grounder, and planner."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Set


GROUNDING_BUNDLE_REQUIRED_KEYS = {
    "slot_inputs",
    "slot_bindings",
    "grounded_constraints",
}


def _sanitize_string_list(value: Any) -> List[str]:
    """Return one normalized list of unique non-empty strings."""
    if not isinstance(value, list):
        return []
    cleaned: List[str] = []
    seen: Set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def sanitize_slot_input_record(value: Any) -> Dict[str, Any]:
    """Return one normalized slot-input record."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    slot_name = record.get("slot_name")
    text = record.get("text")
    sanitized = {
        "slot_name": slot_name.strip() if isinstance(slot_name, str) and slot_name.strip() else None,
        "text": text.strip() if isinstance(text, str) and text.strip() else None,
        "allowed_node_types": _sanitize_string_list(record.get("allowed_node_types")),
    }
    anchor_kind = record.get("anchor_kind")
    if isinstance(anchor_kind, str) and anchor_kind.strip():
        sanitized["anchor_kind"] = anchor_kind.strip()
    constraint_mode = record.get("constraint_mode")
    if isinstance(constraint_mode, str) and constraint_mode.strip():
        sanitized["constraint_mode"] = constraint_mode.strip()
    comparison = record.get("comparison")
    if isinstance(comparison, dict):
        sanitized["comparison"] = deepcopy(comparison)
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in {"slot_name", "text", "allowed_node_types", "anchor_kind", "constraint_mode", "comparison"}
    }
    sanitized.update(extras)
    return {
        key: value
        for key, value in sanitized.items()
        if value is not None
    }


def sanitize_slot_input_list(value: Any) -> List[Dict[str, Any]]:
    """Return one normalized slot-input list."""
    if not isinstance(value, list):
        return []
    results: List[Dict[str, Any]] = []
    for item in value:
        record = sanitize_slot_input_record(item)
        if record.get("slot_name") and record.get("text"):
            results.append(record)
    return results


def sanitize_binding_candidate_record(value: Any) -> Dict[str, Any]:
    """Return one normalized slot-binding candidate."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    sanitized = {}
    for key in (
        "node_type",
        "node_id",
        "label",
        "class_name",
        "local_name",
        "property_local_name",
        "range",
    ):
        item = record.get(key)
        if isinstance(item, str) and item.strip():
            sanitized[key] = item.strip()
    for key in ("role_hints",):
        sanitized[key] = _sanitize_string_list(record.get(key))
    for key in ("numeric",):
        if isinstance(record.get(key), bool):
            sanitized[key] = record.get(key)
    for key in ("lexical_score", "semantic_score", "semantic_similarity", "slot_role_score", "total_score"):
        item = record.get(key)
        if isinstance(item, (int, float)):
            sanitized[key] = float(item)
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in {
            "node_type",
            "node_id",
            "label",
            "class_name",
            "local_name",
            "property_local_name",
            "range",
            "role_hints",
            "numeric",
            "lexical_score",
            "semantic_score",
            "semantic_similarity",
            "slot_role_score",
            "total_score",
        }
    }
    sanitized.update(extras)
    return sanitized


def sanitize_slot_binding_record(value: Any) -> Dict[str, Any]:
    """Return one normalized slot-binding record."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    slot_name = record.get("slot_name")
    text = record.get("text")
    sanitized = {
        "slot_name": slot_name.strip() if isinstance(slot_name, str) and slot_name.strip() else None,
        "text": text.strip() if isinstance(text, str) and text.strip() else None,
        "allowed_node_types": _sanitize_string_list(record.get("allowed_node_types")),
        "candidates": [
            candidate
            for candidate in (
                sanitize_binding_candidate_record(item)
                for item in record.get("candidates", [])
            )
            if candidate
        ],
    }
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in {"slot_name", "text", "allowed_node_types", "candidates"}
    }
    sanitized.update(extras)
    return {
        key: value
        for key, value in sanitized.items()
        if value is not None
    }


def sanitize_slot_binding_list(value: Any) -> List[Dict[str, Any]]:
    """Return one normalized slot-binding list."""
    if not isinstance(value, list):
        return []
    results: List[Dict[str, Any]] = []
    for item in value:
        record = sanitize_slot_binding_record(item)
        if record.get("slot_name") and record.get("text"):
            results.append(record)
    return results


def sanitize_grounded_constraint_record(value: Any) -> Dict[str, Any]:
    """Return one normalized grounded-constraint record."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    sanitized = {}
    for key in ("slot_name", "requested_text", "effective_text", "top_node_id", "top_node_type", "constraint_mode"):
        item = record.get(key)
        if isinstance(item, str) and item.strip():
            sanitized[key] = item.strip()
    for key in ("has_binding",):
        if isinstance(record.get(key), bool):
            sanitized[key] = record.get(key)
    sanitized["binding_terms"] = _sanitize_string_list(record.get("binding_terms"))
    comparison = record.get("comparison")
    if isinstance(comparison, dict):
        sanitized["comparison"] = deepcopy(comparison)
    top_candidate = record.get("top_candidate")
    if isinstance(top_candidate, dict):
        sanitized["top_candidate"] = sanitize_binding_candidate_record(top_candidate)
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in {
            "slot_name",
            "requested_text",
            "effective_text",
            "top_node_id",
            "top_node_type",
            "constraint_mode",
            "has_binding",
            "binding_terms",
            "comparison",
            "top_candidate",
        }
    }
    sanitized.update(extras)
    return sanitized


def sanitize_grounded_constraint_map(value: Any) -> Dict[str, Dict[str, Any]]:
    """Return one normalized grounded-constraint map keyed by slot name."""
    if not isinstance(value, dict):
        return {}
    results: Dict[str, Dict[str, Any]] = {}
    for slot_name, item in value.items():
        if not isinstance(slot_name, str) or not slot_name.strip():
            continue
        record = sanitize_grounded_constraint_record(item)
        record.setdefault("slot_name", slot_name.strip())
        results[slot_name.strip()] = record
    return results


def sanitize_grounding_bundle(value: Any) -> Dict[str, Any]:
    """Return one normalized grounding bundle."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    sanitized = {
        "slot_inputs": sanitize_slot_input_list(record.get("slot_inputs")),
        "slot_bindings": sanitize_slot_binding_list(record.get("slot_bindings")),
        "grounded_constraints": sanitize_grounded_constraint_map(record.get("grounded_constraints")),
    }
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in GROUNDING_BUNDLE_REQUIRED_KEYS
    }
    sanitized.update(extras)
    return sanitized


def build_grounding_bundle(
    *,
    slot_inputs: Any,
    slot_bindings: Any,
    grounded_constraints: Any,
    source: Any = None,
    evidence: Any = None,
) -> Dict[str, Any]:
    """Build one canonical grounding bundle."""
    payload = {
        "slot_inputs": slot_inputs,
        "slot_bindings": slot_bindings,
        "grounded_constraints": grounded_constraints,
    }
    if isinstance(source, dict):
        payload["source"] = deepcopy(source)
    if isinstance(evidence, dict):
        payload["evidence"] = deepcopy(evidence)
    return sanitize_grounding_bundle(payload)


def grounding_slot_inputs(bundle: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return normalized slot inputs from one grounding bundle."""
    if not isinstance(bundle, dict):
        return []
    return sanitize_slot_input_list(bundle.get("slot_inputs"))


def grounding_slot_bindings(bundle: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return normalized slot bindings from one grounding bundle."""
    if not isinstance(bundle, dict):
        return []
    return sanitize_slot_binding_list(bundle.get("slot_bindings"))


def grounding_slot_input_for_name(bundle: Optional[Dict[str, Any]], slot_name: str) -> Optional[Dict[str, Any]]:
    """Return one normalized slot input by slot name."""
    if not isinstance(slot_name, str) or not slot_name.strip():
        return None
    for slot_input in grounding_slot_inputs(bundle):
        if slot_input.get("slot_name") == slot_name:
            return slot_input
    return None


def grounding_slot_binding_candidates(bundle: Optional[Dict[str, Any]], slot_name: str) -> List[Dict[str, Any]]:
    """Return all normalized candidates for one slot."""
    if not isinstance(slot_name, str) or not slot_name.strip():
        return []
    candidates: List[Dict[str, Any]] = []
    for binding in grounding_slot_bindings(bundle):
        if binding.get("slot_name") != slot_name:
            continue
        candidates.extend(
            candidate
            for candidate in binding.get("candidates", [])
            if isinstance(candidate, dict)
        )
    return candidates


def grounding_slot_binding_has_candidates(bundle: Optional[Dict[str, Any]], slot_name: str) -> bool:
    """Whether one slot has at least one normalized candidate."""
    return bool(grounding_slot_binding_candidates(bundle, slot_name))


def abstract_status_slot_requires_high_confidence(slot_input: Optional[Dict[str, Any]]) -> bool:
    """Whether one status slot is an abstract free-text check that must fail closed on weak grounding."""
    return (
        isinstance(slot_input, dict)
        and slot_input.get("constraint_mode") == "status_check"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def candidate_supports_abstract_status_binding(candidate: Optional[Dict[str, Any]]) -> bool:
    """Whether one candidate grounds an abstract status phrase strongly enough to execute."""
    if not isinstance(candidate, dict):
        return False
    if candidate.get("node_type") not in {"attribute", "value"}:
        return False
    if bool(candidate.get("numeric")):
        return False
    lexical_score = float(candidate.get("lexical_score", 0.0) or 0.0)
    semantic_similarity = float(candidate.get("semantic_similarity", 0.0) or 0.0)
    total_score = float(candidate.get("total_score", 0.0) or 0.0)
    if lexical_score >= 12.0:
        return True
    return total_score >= 12.0 and (lexical_score >= 8.0 or semantic_similarity >= 0.28)


def grounding_candidates_for_slot(
    bundle: Optional[Dict[str, Any]],
    slot_name: str,
    *,
    preferred_node_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return admissible normalized candidates for one slot."""
    candidates = grounding_slot_binding_candidates(bundle, slot_name)
    if preferred_node_types:
        preferred_set = {item for item in preferred_node_types if isinstance(item, str) and item}
        preferred = [item for item in candidates if item.get("node_type") in preferred_set]
        if preferred:
            candidates = preferred
    slot_input = grounding_slot_input_for_name(bundle, slot_name)
    if abstract_status_slot_requires_high_confidence(slot_input):
        candidates = [
            candidate
            for candidate in candidates
            if candidate_supports_abstract_status_binding(candidate)
        ]
    candidates.sort(
        key=lambda item: (
            -float(item.get("total_score", 0.0) or 0.0),
            -float(item.get("semantic_similarity", 0.0) or 0.0),
            str(item.get("node_id", "")),
        )
    )
    return candidates


def grounding_slot_candidates_have_text_lowering(bundle: Optional[Dict[str, Any]], slot_name: str) -> bool:
    """Whether one slot has a candidate that can lower to text-style filtering."""
    slot_input = grounding_slot_input_for_name(bundle, slot_name)
    abstract_status_check = abstract_status_slot_requires_high_confidence(slot_input)
    for candidate in grounding_candidates_for_slot(bundle, slot_name):
        node_type = candidate.get("node_type")
        if abstract_status_check:
            if node_type in {"attribute", "value"} and not bool(candidate.get("numeric")):
                return True
            continue
        if node_type == "value":
            return True
        if node_type == "attribute" and not bool(candidate.get("numeric")):
            return True
    return False


def grounding_top_candidate_for_slot(
    bundle: Optional[Dict[str, Any]],
    slot_name: str,
    *,
    preferred_node_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Return the strongest normalized candidate for one slot."""
    candidates = grounding_candidates_for_slot(
        bundle,
        slot_name,
        preferred_node_types=preferred_node_types,
    )
    if not candidates:
        return None
    return candidates[0]


def grounding_top_attribute_candidate_for_slot(
    bundle: Optional[Dict[str, Any]],
    slot_name: str,
    *,
    class_name: Optional[str] = None,
    numeric_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring attribute candidate for one slot."""
    candidates = []
    for candidate in grounding_slot_binding_candidates(bundle, slot_name):
        if candidate.get("node_type") != "attribute":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        if numeric_only and not bool(candidate.get("numeric")):
            continue
        candidates.append(candidate)
    candidates.sort(
        key=lambda item: (
            -float(item.get("total_score", 0.0) or 0.0),
            -float(item.get("semantic_similarity", 0.0) or 0.0),
            str(item.get("node_id", "")),
        )
    )
    return candidates[0] if candidates else None


def grounding_top_value_candidate_for_slot(
    bundle: Optional[Dict[str, Any]],
    slot_name: str,
    *,
    class_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring value candidate for one slot."""
    candidates = []
    for candidate in grounding_slot_binding_candidates(bundle, slot_name):
        if candidate.get("node_type") != "value":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        candidates.append(candidate)
    candidates.sort(
        key=lambda item: (
            -float(item.get("total_score", 0.0) or 0.0),
            -float(item.get("semantic_similarity", 0.0) or 0.0),
            str(item.get("node_id", "")),
        )
    )
    return candidates[0] if candidates else None


def grounding_constraint_record(bundle: Optional[Dict[str, Any]], slot_name: str) -> Dict[str, Any]:
    """Return one normalized grounded-constraint record by slot name."""
    if not isinstance(bundle, dict) or not isinstance(slot_name, str) or not slot_name.strip():
        return {}
    return sanitize_grounded_constraint_map(bundle.get("grounded_constraints")).get(slot_name, {})


def grounding_constraint_requested_text(bundle: Optional[Dict[str, Any]], slot_name: str) -> Optional[str]:
    """Return the requested text of one grounded constraint."""
    value = grounding_constraint_record(bundle, slot_name).get("requested_text")
    return value if isinstance(value, str) and value else None


def grounding_constraint_effective_text(bundle: Optional[Dict[str, Any]], slot_name: str) -> Optional[str]:
    """Return the effective text of one grounded constraint."""
    value = grounding_constraint_record(bundle, slot_name).get("effective_text")
    return value if isinstance(value, str) and value else None
