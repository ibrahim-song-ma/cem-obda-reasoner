#!/usr/bin/env python3
"""Repo-owned IR contracts for parser and planner boundaries."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Set


INTENT_IR_REQUIRED_KEYS = {
    "unit_id",
    "focus",
    "operators",
    "constraints",
    "output",
    "references",
}
REQUEST_IR_REQUIRED_KEYS = {
    "question",
    "requested_template",
    "effective_template",
    "query_family",
    "routing_rationale",
    "anchors",
    "slot_inputs",
    "slot_bindings",
    "grounded_constraints",
    "source",
    "evidence",
    "references",
    "constraints",
    "output",
}


def sanitize_anchor_forms(value: Any) -> List[Dict[str, Any]]:
    """Return anchor forms in one normalized repo-owned shape."""
    anchors: List[Dict[str, Any]] = []
    if not isinstance(value, list):
        return anchors
    for item in value:
        if not isinstance(item, dict):
            continue
        kind = item.get("kind")
        literal = item.get("value")
        if not isinstance(kind, str) or not kind.strip():
            continue
        if not isinstance(literal, str) or not literal.strip():
            continue
        normalized = {"kind": kind.strip(), "value": literal.strip()}
        for optional_key in ("source", "confidence"):
            optional_value = item.get(optional_key)
            if optional_value is not None:
                normalized[optional_key] = deepcopy(optional_value)
        anchors.append(normalized)
    return anchors


def sanitize_string_list(value: Any) -> List[str]:
    """Return one normalized list of non-empty strings."""
    if not isinstance(value, list):
        return []
    cleaned: List[str] = []
    seen = set()
    for item in value:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def sanitize_dict_list(value: Any) -> List[Dict[str, Any]]:
    """Return one normalized list containing only dict items."""
    if not isinstance(value, list):
        return []
    return [deepcopy(item) for item in value if isinstance(item, dict)]


def sanitize_focus_record(value: Any) -> Dict[str, Any]:
    """Return one normalized focus record for Intent IR."""
    if not isinstance(value, dict):
        return {}
    focus = deepcopy(value)
    if "anchors" in focus:
        focus["anchors"] = sanitize_anchor_forms(focus.get("anchors"))
    return focus


def sanitize_output_record(value: Any) -> Dict[str, Any]:
    """Return one normalized output record for Intent IR."""
    if not isinstance(value, dict):
        return {}
    output = deepcopy(value)
    shape = output.get("shape")
    if not isinstance(shape, str) or not shape.strip():
        output.pop("shape", None)
    else:
        output["shape"] = shape.strip()
    return output


def intent_ir_output_record(unit_intent_ir: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return one normalized output record from an Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return None
    output = sanitize_output_record(unit_intent_ir.get("output"))
    return output or None


def sanitize_references_record(value: Any) -> Dict[str, Any]:
    """Return one normalized references record for Intent IR."""
    if not isinstance(value, dict):
        return {}
    references = deepcopy(value)
    references["markers"] = sanitize_string_list(references.get("markers"))

    depends_on = references.get("depends_on")
    if not isinstance(depends_on, str) or not depends_on.strip():
        references.pop("depends_on", None)
    else:
        references["depends_on"] = depends_on.strip()

    condition = references.get("condition")
    if not isinstance(condition, str) or not condition.strip():
        references.pop("condition", None)
    else:
        references["condition"] = condition.strip()

    resolved = references.get("resolved")
    if not isinstance(resolved, dict):
        references.pop("resolved", None)
    else:
        references["resolved"] = deepcopy(resolved)

    return references


def sanitize_request_output_record(value: Any) -> Dict[str, Any]:
    """Return one normalized request-ir output record."""
    if not isinstance(value, dict):
        return {}
    output = deepcopy(value)
    grain = output.get("grain")
    if not isinstance(grain, str) or not grain.strip():
        output.pop("grain", None)
    else:
        output["grain"] = grain.strip()
    for key in ("needs_analysis", "asks_solution", "asks_explanation", "target_projection_requested"):
        if isinstance(output.get(key), bool):
            continue
        output.pop(key, None)
    return output


def sanitize_intent_ir_record(
    value: Any,
    *,
    fallback_unit_id: str,
) -> Dict[str, Any]:
    """Return one normalized repo-owned Intent IR record."""
    record = deepcopy(value) if isinstance(value, dict) else {}
    unit_id = record.get("unit_id")
    intent_ir = {
        "unit_id": unit_id if isinstance(unit_id, str) and unit_id.strip() else fallback_unit_id,
        "focus": sanitize_focus_record(record.get("focus")),
        "operators": sanitize_string_list(record.get("operators")),
        "constraints": sanitize_dict_list(record.get("constraints")),
        "output": sanitize_output_record(record.get("output")),
        "references": sanitize_references_record(record.get("references")),
    }
    extra_keys = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in INTENT_IR_REQUIRED_KEYS
    }
    intent_ir.update(extra_keys)
    return intent_ir


def constraint_snapshot_from_constraints(constraints: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Extract a typed constraint snapshot from one canonical constraints list."""
    snapshot: Dict[str, Any] = {}
    if not isinstance(constraints, list):
        return snapshot
    for item in constraints:
        if not isinstance(item, dict):
            continue
        slot_name = item.get("slot")
        if not isinstance(slot_name, str) or not slot_name:
            continue
        text_value = item.get("text")
        if isinstance(text_value, str) and text_value.strip():
            snapshot[slot_name] = text_value.strip()
            continue
        constraint_value = item.get("constraint")
        if isinstance(constraint_value, dict):
            snapshot[slot_name] = deepcopy(constraint_value)
    return snapshot


def intent_ir_constraint_snapshot(unit_intent_ir: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a typed constraint snapshot from one Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return {}
    return constraint_snapshot_from_constraints(unit_intent_ir.get("constraints"))


def intent_ir_operator_set(unit_intent_ir: Optional[Dict[str, Any]]) -> Set[str]:
    """Return one normalized operator set from an Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return set()
    return set(sanitize_string_list(unit_intent_ir.get("operators")))


def intent_ir_operator_list(unit_intent_ir: Optional[Dict[str, Any]]) -> List[str]:
    """Return one normalized operator list from an Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return []
    return sanitize_string_list(unit_intent_ir.get("operators"))


def intent_ir_focus_record(unit_intent_ir: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return one normalized focus record from an Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return None
    focus = sanitize_focus_record(unit_intent_ir.get("focus"))
    return focus or None


def intent_ir_references_record(unit_intent_ir: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return one normalized references record from an Intent IR record."""
    if not isinstance(unit_intent_ir, dict):
        return None
    references = sanitize_references_record(unit_intent_ir.get("references"))
    return references or None


def sanitize_intent_ir_list(value: Any) -> List[Dict[str, Any]]:
    """Return a normalized list of Intent IR records."""
    if not isinstance(value, list):
        return []
    return [
        sanitize_intent_ir_record(item, fallback_unit_id=f"q{index}")
        for index, item in enumerate(value, start=1)
        if isinstance(item, dict)
    ]


def build_intent_ir_record(
    *,
    unit_id: Any,
    focus: Any,
    operators: Any,
    constraints: Any,
    output: Any,
    references: Any,
) -> Dict[str, Any]:
    """Build one canonical Intent IR record from parser/planner owned pieces."""
    return sanitize_intent_ir_record({
        "unit_id": unit_id,
        "focus": focus,
        "operators": operators,
        "constraints": constraints,
        "output": output,
        "references": references,
    }, fallback_unit_id="q1")


def build_intent_ir_from_policy(
    unit: Dict[str, Any],
    policy: Dict[str, Any],
) -> Dict[str, Any]:
    """Build one canonical Intent IR record directly from a planner intent policy."""
    return build_intent_ir_record(
        unit_id=unit.get("unit_id") if isinstance(unit, dict) else None,
        focus=deepcopy(policy.get("focus", {})) if isinstance(policy, dict) else {},
        operators=list(policy.get("operators", [])) if isinstance(policy, dict) else [],
        constraints=deepcopy(policy.get("constraints", [])) if isinstance(policy, dict) else [],
        output=deepcopy(policy.get("output", {})) if isinstance(policy, dict) else {},
        references=deepcopy(policy.get("references", {})) if isinstance(policy, dict) else {},
    )


def sanitize_request_ir_record(value: Any) -> Dict[str, Any]:
    """Return one normalized semantic request IR record."""
    if not isinstance(value, dict):
        return {}
    record = deepcopy(value)
    sanitized = {
        "question": record.get("question").strip()
        if isinstance(record.get("question"), str) and record.get("question").strip()
        else "",
        "requested_template": record.get("requested_template").strip()
        if isinstance(record.get("requested_template"), str) and record.get("requested_template").strip()
        else None,
        "effective_template": record.get("effective_template").strip()
        if isinstance(record.get("effective_template"), str) and record.get("effective_template").strip()
        else None,
        "query_family": record.get("query_family").strip()
        if isinstance(record.get("query_family"), str) and record.get("query_family").strip()
        else None,
        "routing_rationale": sanitize_string_list(record.get("routing_rationale")),
        "anchors": sanitize_anchor_forms(record.get("anchors")),
        "slot_inputs": sanitize_dict_list(record.get("slot_inputs")),
        "slot_bindings": sanitize_dict_list(record.get("slot_bindings")),
        "grounded_constraints": deepcopy(record.get("grounded_constraints"))
        if isinstance(record.get("grounded_constraints"), dict)
        else {},
        "source": deepcopy(record.get("source")) if isinstance(record.get("source"), dict) else {},
        "evidence": deepcopy(record.get("evidence")) if isinstance(record.get("evidence"), dict) else {},
        "references": sanitize_references_record(record.get("references")),
        "constraints": sanitize_dict_list(record.get("constraints")),
        "output": sanitize_request_output_record(record.get("output")),
    }
    extras = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in REQUEST_IR_REQUIRED_KEYS
    }
    sanitized.update(extras)
    return sanitized


def build_request_ir_record(
    *,
    question: Any,
    requested_template: Any,
    effective_template: Any,
    query_family: Any,
    routing_rationale: Any,
    anchors: Any,
    slot_inputs: Any,
    slot_bindings: Any,
    grounded_constraints: Any,
    source: Any,
    evidence: Any,
    references: Any,
    constraints: Any,
    output: Any,
) -> Dict[str, Any]:
    """Build one canonical semantic request IR record."""
    return sanitize_request_ir_record({
        "question": question,
        "requested_template": requested_template,
        "effective_template": effective_template,
        "query_family": query_family,
        "routing_rationale": routing_rationale,
        "anchors": anchors,
        "slot_inputs": slot_inputs,
        "slot_bindings": slot_bindings,
        "grounded_constraints": grounded_constraints,
        "source": source,
        "evidence": evidence,
        "references": references,
        "constraints": constraints,
        "output": output,
    })


def request_ir_anchor_forms(request_ir: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return normalized anchors from one request IR record."""
    if not isinstance(request_ir, dict):
        return []
    return sanitize_anchor_forms(request_ir.get("anchors"))


def request_ir_references_record(request_ir: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return normalized references from one request IR record."""
    if not isinstance(request_ir, dict):
        return None
    references = sanitize_references_record(request_ir.get("references"))
    return references or None


def request_ir_output_record(request_ir: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return normalized output contract from one request IR record."""
    if not isinstance(request_ir, dict):
        return {}
    return sanitize_request_output_record(request_ir.get("output"))


def request_ir_effective_template(request_ir: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return the effective template from one request IR record."""
    if not isinstance(request_ir, dict):
        return None
    value = request_ir.get("effective_template")
    return value.strip() if isinstance(value, str) and value.strip() else None


def request_ir_query_family(request_ir: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return the query family from one request IR record."""
    if not isinstance(request_ir, dict):
        return None
    value = request_ir.get("query_family")
    return value.strip() if isinstance(value, str) and value.strip() else None


def request_ir_summary_record(request_ir: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Project one compact summary view from a normalized request IR record."""
    sanitized = sanitize_request_ir_record(request_ir)
    if not sanitized:
        return {}
    return {
        "query_family": sanitized.get("query_family"),
        "anchors": sanitized.get("anchors"),
        "constraints": sanitized.get("constraints"),
        "output": sanitized.get("output"),
    }
