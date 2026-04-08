#!/usr/bin/env python3
"""Repo-owned parser output contracts for language intent parsing."""

from __future__ import annotations

from copy import deepcopy
import os
from typing import Any, Dict, List, Optional

from obda_ir_contracts import (
    sanitize_anchor_forms,
    sanitize_dict_list,
    sanitize_intent_ir_list,
    sanitize_string_list,
)
from obda_model_backend_profiles import (
    DEFAULT_MODEL_BACKEND,
    LEGACY_MODEL_BACKEND_ENV,
    MODEL_BACKEND_ENV,
    normalize_model_backend,
    resolve_configured_model_backend,
)
from obda_parser_surface import detect_question_anchors
from obda_lexical import (
    detect_reference_markers,
    lossless_canonicalize_text,
)

DEFAULT_PARSER_STRATEGY = "DeterministicStrategy"
DEFAULT_IR_PROVENANCE = "deterministic_ir"
BACKEND_FAILURE_AMBIGUITY_KINDS = {
    "backend_unavailable",
    "backend_timeout",
    "backend_parse_failed",
    "backend_projection_mismatch",
    "backend_underfilled_parse",
}
PARSER_STRATEGY_ENV = "OBDA_PARSER_STRATEGY"
SUPPORTED_PARSER_STRATEGIES = {
    "DeterministicStrategy",
    "HybridStrategy",
}

QUESTION_UNIT_REQUIRED_KEYS = {
    "unit_id",
    "raw_text",
    "normalized_text",
    "dependency",
    "reference_markers",
    "anchor_forms",
    "comparators",
    "question_acts",
    "surface_constraints",
    "ambiguities",
    "confidence",
}


def normalize_parser_strategy(value: Any) -> str:
    """Return one repo-owned parser strategy label."""
    if isinstance(value, str) and value in SUPPORTED_PARSER_STRATEGIES:
        return value
    return DEFAULT_PARSER_STRATEGY


def parser_output_has_backend_failure(value: Any) -> bool:
    """Return whether one parser output carries a backend-level conservative fallback ambiguity."""
    if not isinstance(value, dict):
        return False
    for item in sanitize_dict_list(value.get("ambiguities")):
        kind = item.get("kind")
        if isinstance(kind, str) and kind in BACKEND_FAILURE_AMBIGUITY_KINDS:
            return True
    return False


def build_parser_input(
    question: str,
    template: str,
    question_unit: Any = None,
) -> Dict[str, Any]:
    """Build one repo-owned parser input contract."""
    strategy = normalize_parser_strategy(os.getenv(PARSER_STRATEGY_ENV))
    model_backend = resolve_configured_model_backend()
    if model_backend != DEFAULT_MODEL_BACKEND and strategy == DEFAULT_PARSER_STRATEGY:
        strategy = "HybridStrategy"
    return {
        "utterance": question if isinstance(question, str) else "",
        "template": template,
        "question_unit": deepcopy(question_unit) if isinstance(question_unit, dict) else None,
        "strategy": strategy,
        "model_backend": model_backend,
    }


def build_parser_exactness_report(
    raw_question: str,
    canonicalization: Dict[str, Any],
    anchors: List[Dict[str, Any]],
    lexical_recall: Dict[str, Any],
) -> Dict[str, Any]:
    """Validate exact parser elements against raw/canonical text without redoing semantics."""
    canonical_text = (
        canonicalization.get("canonical_text")
        if isinstance(canonicalization.get("canonical_text"), str)
        else raw_question
    )
    anchor_support = []
    for anchor in anchors:
        if not isinstance(anchor, dict):
            continue
        value = anchor.get("value")
        if not isinstance(value, str) or not value:
            continue
        anchor_support.append({
            "kind": anchor.get("kind"),
            "value": value,
            "supported": value in raw_question or value in canonical_text,
        })

    numeric_constraint = lexical_recall.get("status_numeric_constraint")
    numeric_support = None
    if isinstance(numeric_constraint, dict):
        raw_text = numeric_constraint.get("raw_text")
        numeric_support = {
            "raw_text": raw_text,
            "value": numeric_constraint.get("value"),
            "supported": (
                isinstance(raw_text, str)
                and bool(raw_text)
                and (raw_text in raw_question or raw_text in canonical_text)
            ) or (
                numeric_constraint.get("value") is not None
                and str(numeric_constraint.get("value")) in canonical_text
            ),
        }

    return {
        "canonicalization": deepcopy(canonicalization),
        "anchor_support": anchor_support,
        "numeric_constraint_support": numeric_support,
    }


def sanitize_bootstrap_signals(value: Any) -> Dict[str, Any]:
    """Return compatibility bootstrap signals in a predictable shape."""
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key:
            continue
        if isinstance(item, bool):
            normalized[key] = item
        elif item is None:
            normalized[key] = None
    return normalized


def sanitize_bootstrap_operator_hint_store(value: Any) -> List[Dict[str, Any]]:
    """Return bootstrap operator hints in the legacy hint-store shape."""
    if not isinstance(value, list):
        return []
    normalized: List[Dict[str, Any]] = []
    seen = set()
    for item in value:
        if isinstance(item, str):
            operator = item.strip()
            source = "parser_question_act"
        elif isinstance(item, dict):
            operator = item.get("operator")
            source = item.get("source")
            operator = operator.strip() if isinstance(operator, str) else ""
            source = source.strip() if isinstance(source, str) and source.strip() else "parser_question_act"
        else:
            continue
        if not operator:
            continue
        signature = (operator, source)
        if signature in seen:
            continue
        seen.add(signature)
        normalized.append({
            "operator": operator,
            "source": source,
        })
    return normalized


def sanitize_bootstrap_candidates(value: Any) -> Dict[str, List[Dict[str, Any]]]:
    """Return bootstrap candidates with one sanitized bucket shape."""
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, List[Dict[str, Any]]] = {}
    for key, bucket in value.items():
        if not isinstance(key, str) or not key:
            continue
        sanitized_bucket = sanitize_dict_list(bucket)
        if sanitized_bucket:
            normalized[key] = sanitized_bucket
    return normalized


def sanitize_surface_constraints(value: Any) -> List[Dict[str, Any]]:
    """Return parser surface constraints in one normalized list shape."""
    return sanitize_dict_list(value)


def derive_bootstrap_operator_hint_store(
    question_units: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Project parser-native question acts into the legacy operator hint store."""
    if not isinstance(question_units, list) or not question_units:
        return []
    primary_unit = question_units[0] if isinstance(question_units[0], dict) else {}
    return sanitize_bootstrap_operator_hint_store(primary_unit.get("question_acts", []))


def derive_surface_constraints_from_question_units(
    question_units: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Project parser-native unit constraints into the legacy top-level constraint list."""
    if not isinstance(question_units, list) or not question_units:
        return []
    primary_unit = question_units[0] if isinstance(question_units[0], dict) else {}
    return sanitize_surface_constraints(primary_unit.get("surface_constraints"))


def derive_status_numeric_constraint(
    question_units: List[Dict[str, Any]],
    surface_constraints: List[Dict[str, Any]],
) -> Dict[str, Any] | None:
    """Recover one numeric comparison from parser-native structures."""
    if isinstance(question_units, list) and question_units:
        primary_unit = question_units[0] if isinstance(question_units[0], dict) else {}
        comparators = sanitize_dict_list(primary_unit.get("comparators"))
        if comparators:
            first = comparators[0]
            if isinstance(first, dict):
                return first
    for item in surface_constraints:
        if not isinstance(item, dict):
            continue
        if item.get("kind") == "comparison" and isinstance(item.get("constraint"), dict):
            return deepcopy(item["constraint"])
    return None


def derive_bootstrap_candidates_from_surface_constraints(
    surface_constraints: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Recover legacy bootstrap candidate buckets from parser-native constraints."""
    candidates: Dict[str, List[Dict[str, Any]]] = {}
    for item in surface_constraints:
        if not isinstance(item, dict):
            continue
        slot_name = item.get("slot")
        text = item.get("text")
        if not isinstance(slot_name, str) or not slot_name:
            continue
        if not isinstance(text, str) or not text.strip():
            continue
        bucket = candidates.setdefault(slot_name, [])
        bucket.append({
            "text": text.strip(),
            "source": item.get("source") or "parser_surface_constraint",
        })
    return sanitize_bootstrap_candidates(candidates)


def sanitize_question_unit_record(
    value: Any,
    *,
    fallback_unit_id: str,
    fallback_raw_text: str,
    fallback_normalized_text: str,
    fallback_dependency: Optional[Dict[str, Any]],
    fallback_reference_markers: List[str],
    fallback_anchor_forms: List[Dict[str, Any]],
    fallback_comparators: List[Dict[str, Any]],
    fallback_question_acts: List[str],
    fallback_surface_constraints: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Normalize one parser question unit record."""
    record = deepcopy(value) if isinstance(value, dict) else {}
    unit_id = record.get("unit_id")
    raw_text = record.get("raw_text")
    normalized_text = record.get("normalized_text")
    dependency = (
        deepcopy(record.get("dependency"))
        if isinstance(record.get("dependency"), dict)
        else deepcopy(fallback_dependency)
        if isinstance(fallback_dependency, dict)
        else None
    )
    question_unit = {
        "unit_id": unit_id if isinstance(unit_id, str) and unit_id.strip() else fallback_unit_id,
        "raw_text": raw_text if isinstance(raw_text, str) and raw_text.strip() else fallback_raw_text,
        "normalized_text": (
            normalized_text
            if isinstance(normalized_text, str) and normalized_text.strip()
            else fallback_normalized_text
        ),
        "dependency": dependency,
        "reference_markers": sanitize_string_list(record.get("reference_markers")) or list(fallback_reference_markers),
        "anchor_forms": sanitize_anchor_forms(record.get("anchor_forms")) or deepcopy(fallback_anchor_forms),
        "comparators": sanitize_dict_list(record.get("comparators")) or deepcopy(fallback_comparators),
        "question_acts": sanitize_string_list(record.get("question_acts")) or list(fallback_question_acts),
        "surface_constraints": (
            sanitize_surface_constraints(record.get("surface_constraints"))
            or deepcopy(fallback_surface_constraints)
        ),
        "ambiguities": sanitize_dict_list(record.get("ambiguities")),
        "confidence": record.get("confidence") if isinstance(record.get("confidence"), (int, float)) else None,
    }
    extra_keys = {
        key: deepcopy(item)
        for key, item in record.items()
        if key not in QUESTION_UNIT_REQUIRED_KEYS
    }
    question_unit.update(extra_keys)
    return question_unit


def validate_and_normalize_parser_output(
    parser_input: Dict[str, Any],
    parser_output: Dict[str, Any],
    *,
    default_ir_provenance: str,
) -> Dict[str, Any]:
    """Validate parser output against the repo-owned contract and fill defaults."""
    raw_question = (
        parser_input.get("utterance")
        if isinstance(parser_input.get("utterance"), str)
        else ""
    )
    canonicalization = lossless_canonicalize_text(raw_question)
    fallback_canonical_question = (
        canonicalization.get("canonical_text")
        if isinstance(canonicalization.get("canonical_text"), str)
        else raw_question
    )
    normalized_output = deepcopy(parser_output) if isinstance(parser_output, dict) else {}
    canonical_question = normalized_output.get("canonical_question")
    if not isinstance(canonical_question, str) or not canonical_question.strip():
        canonical_question = fallback_canonical_question
    else:
        canonical_question = canonical_question.strip()

    anchors = sanitize_anchor_forms(normalized_output.get("anchors"))
    if not anchors:
        anchors = sanitize_anchor_forms(detect_question_anchors(canonical_question))
    status_numeric_constraint = (
        deepcopy(normalized_output.get("status_numeric_constraint"))
        if isinstance(normalized_output.get("status_numeric_constraint"), dict)
        else None
    )
    surface_constraints = sanitize_surface_constraints(normalized_output.get("surface_constraints"))
    bootstrap_operator_hint_store = sanitize_bootstrap_operator_hint_store(
        normalized_output.get("bootstrap_operator_hints")
    )
    bootstrap_operator_hints_list = sanitize_string_list([
        item.get("operator")
        for item in bootstrap_operator_hint_store
        if isinstance(item, dict)
    ])
    fallback_comparators = [deepcopy(status_numeric_constraint)] if status_numeric_constraint else []
    question_units_hint = (
        parser_input.get("question_units_hint")
        if isinstance(parser_input.get("question_units_hint"), list)
        else []
    )
    fallback_question_unit = sanitize_question_unit_record(
        normalized_output.get("question_units", [None])[0]
        if isinstance(normalized_output.get("question_units"), list) and normalized_output.get("question_units")
        else None,
        fallback_unit_id="q1",
        fallback_raw_text=raw_question,
        fallback_normalized_text=canonical_question,
        fallback_dependency=None,
        fallback_reference_markers=detect_reference_markers(raw_question),
        fallback_anchor_forms=anchors,
        fallback_comparators=fallback_comparators,
        fallback_question_acts=bootstrap_operator_hints_list,
        fallback_surface_constraints=surface_constraints,
    )
    question_units_raw = normalized_output.get("question_units")
    if isinstance(question_units_raw, list) and question_units_raw:
        question_units = [
            sanitize_question_unit_record(
                item,
                fallback_unit_id=(
                    hint_unit.get("unit_id")
                    if index - 1 < len(question_units_hint)
                    and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                    and isinstance(hint_unit.get("unit_id"), str)
                    and hint_unit.get("unit_id")
                    else f"q{index}"
                ),
                fallback_raw_text=(
                    hint_unit.get("raw_text")
                    if index - 1 < len(question_units_hint)
                    and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                    and isinstance(hint_unit.get("raw_text"), str)
                    and hint_unit.get("raw_text").strip()
                    else raw_question
                ),
                fallback_normalized_text=(
                    hint_unit.get("normalized_text")
                    if index - 1 < len(question_units_hint)
                    and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                    and isinstance(hint_unit.get("normalized_text"), str)
                    and hint_unit.get("normalized_text").strip()
                    else (
                        hint_unit.get("text")
                        if index - 1 < len(question_units_hint)
                        and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                        and isinstance(hint_unit.get("text"), str)
                        and hint_unit.get("text").strip()
                        else canonical_question
                    )
                ),
                fallback_dependency=(
                    deepcopy(hint_unit.get("dependency"))
                    if index - 1 < len(question_units_hint)
                    and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                    and isinstance(hint_unit.get("dependency"), dict)
                    else None
                ),
                fallback_reference_markers=(
                    sanitize_string_list(hint_unit.get("reference_markers"))
                    if index - 1 < len(question_units_hint)
                    and isinstance((hint_unit := question_units_hint[index - 1]), dict)
                    else fallback_question_unit["reference_markers"]
                ) or fallback_question_unit["reference_markers"],
                fallback_anchor_forms=anchors,
                fallback_comparators=fallback_comparators,
                fallback_question_acts=bootstrap_operator_hints_list,
                fallback_surface_constraints=surface_constraints,
            )
            for index, item in enumerate(question_units_raw, start=1)
        ]
    else:
        question_units = [fallback_question_unit]
    if not surface_constraints:
        surface_constraints = derive_surface_constraints_from_question_units(question_units)
    if not isinstance(status_numeric_constraint, dict):
        status_numeric_constraint = derive_status_numeric_constraint(question_units, surface_constraints)
    if not bootstrap_operator_hint_store:
        bootstrap_operator_hint_store = derive_bootstrap_operator_hint_store(question_units)
        if not bootstrap_operator_hint_store:
            bootstrap_operator_hint_store = sanitize_bootstrap_operator_hint_store(
                fallback_question_unit.get("question_acts", [])
            )
    bootstrap_candidates = sanitize_bootstrap_candidates(normalized_output.get("bootstrap_candidates"))
    if not bootstrap_candidates:
        bootstrap_candidates = derive_bootstrap_candidates_from_surface_constraints(surface_constraints)

    parser_evidence = (
        deepcopy(normalized_output.get("parser_evidence"))
        if isinstance(normalized_output.get("parser_evidence"), dict)
        else build_parser_exactness_report(
            raw_question,
            canonicalization,
            anchors,
            {
                "status_numeric_constraint": deepcopy(status_numeric_constraint),
                "bootstrap_candidates": bootstrap_candidates,
            },
        )
    )
    if "canonicalization" not in parser_evidence:
        parser_evidence["canonicalization"] = deepcopy(canonicalization)

    strategy = normalize_parser_strategy(normalized_output.get("strategy") or parser_input.get("strategy"))
    model_backend = normalize_model_backend(
        normalized_output.get("model_backend") or parser_input.get("model_backend")
    )
    ir_provenance = normalized_output.get("ir_provenance")
    if not isinstance(ir_provenance, str) or not ir_provenance.strip():
        ir_provenance = default_ir_provenance

    return {
        "strategy": strategy,
        "model_backend": model_backend,
        "ir_provenance": ir_provenance,
        "canonical_question": canonical_question,
        "question_units": question_units,
        "dependency_dag": deepcopy(normalized_output.get("dependency_dag"))
        if isinstance(normalized_output.get("dependency_dag"), dict)
        else None,
        "intent_irs": sanitize_intent_ir_list(normalized_output.get("intent_irs")),
        "parser_confidence": (
            normalized_output.get("parser_confidence")
            if isinstance(normalized_output.get("parser_confidence"), (int, float))
            else None
        ),
        "ambiguities": sanitize_dict_list(normalized_output.get("ambiguities")),
        "clarification_candidates": sanitize_dict_list(normalized_output.get("clarification_candidates")),
        "parser_evidence": parser_evidence,
        "anchors": anchors,
        "bootstrap_operator_hints": bootstrap_operator_hint_store,
        "bootstrap_signals": sanitize_bootstrap_signals(normalized_output.get("bootstrap_signals")),
        "bootstrap_candidates": bootstrap_candidates,
        "status_numeric_constraint": deepcopy(status_numeric_constraint),
        "surface_constraints": surface_constraints,
    }


def project_parser_output_for_question_unit(
    question: str,
    template: str,
    parser_output: Dict[str, Any],
    question_unit: Dict[str, Any],
    *,
    unit_index: int = 0,
) -> Dict[str, Any]:
    """Project one utterance-level parser output back into one unit-scoped parser bundle."""
    record = deepcopy(parser_output) if isinstance(parser_output, dict) else {}
    normalized_units = (
        record.get("question_units")
        if isinstance(record.get("question_units"), list)
        else []
    )
    unit_id = question_unit.get("unit_id") if isinstance(question_unit, dict) else None
    selected_unit = None
    if isinstance(unit_id, str) and unit_id:
        for item in normalized_units:
            if isinstance(item, dict) and item.get("unit_id") == unit_id:
                selected_unit = item
                break
    if selected_unit is None and 0 <= unit_index < len(normalized_units):
        item = normalized_units[unit_index]
        if isinstance(item, dict):
            selected_unit = item
    if selected_unit is None:
        selected_unit = deepcopy(question_unit) if isinstance(question_unit, dict) else {}

    selected_surface_constraints = sanitize_surface_constraints(selected_unit.get("surface_constraints"))
    selected_operator_hints = sanitize_bootstrap_operator_hint_store(selected_unit.get("question_acts"))
    selected_status_numeric_constraint = derive_status_numeric_constraint(
        [selected_unit],
        selected_surface_constraints,
    )
    selected_bootstrap_candidates = derive_bootstrap_candidates_from_surface_constraints(
        selected_surface_constraints,
    )
    selected_anchors = sanitize_anchor_forms(selected_unit.get("anchor_forms"))
    selected_intent_irs = []
    for intent_ir in sanitize_intent_ir_list(record.get("intent_irs")):
        if not isinstance(intent_ir, dict):
            continue
        if isinstance(unit_id, str) and unit_id and intent_ir.get("unit_id") == unit_id:
            selected_intent_irs.append(intent_ir)
    if not isinstance(selected_unit, dict):
        selected_unit = {}
    unit_question = (
        selected_unit.get("raw_text")
        if isinstance(selected_unit.get("raw_text"), str) and selected_unit.get("raw_text").strip()
        else (
            selected_unit.get("text")
            if isinstance(selected_unit.get("text"), str) and selected_unit.get("text").strip()
            else question
        )
    )
    unit_parser_input = build_parser_input(unit_question, template, question_unit=selected_unit)
    projected_output = {
        "strategy": record.get("strategy"),
        "model_backend": record.get("model_backend"),
        "ir_provenance": record.get("ir_provenance"),
        "canonical_question": (
            selected_unit.get("normalized_text")
            if isinstance(selected_unit.get("normalized_text"), str) and selected_unit.get("normalized_text").strip()
            else record.get("canonical_question")
        ),
        "question_units": [selected_unit],
        "dependency_dag": None,
        "intent_irs": selected_intent_irs,
        "parser_confidence": record.get("parser_confidence"),
        "ambiguities": sanitize_dict_list(record.get("ambiguities")),
        "clarification_candidates": sanitize_dict_list(record.get("clarification_candidates")),
        "parser_evidence": deepcopy(record.get("parser_evidence", {})),
        "anchors": selected_anchors,
        "bootstrap_operator_hints": selected_operator_hints,
        "bootstrap_signals": sanitize_bootstrap_signals(record.get("bootstrap_signals")),
        "bootstrap_candidates": selected_bootstrap_candidates,
        "status_numeric_constraint": deepcopy(selected_status_numeric_constraint),
        "surface_constraints": selected_surface_constraints,
    }
    return validate_and_normalize_parser_output(
        unit_parser_input,
        projected_output,
        default_ir_provenance=(
            record.get("ir_provenance")
            if isinstance(record.get("ir_provenance"), str) and record.get("ir_provenance")
            else DEFAULT_IR_PROVENANCE
        ),
    )


def build_slots_from_parser_output(
    question: str,
    template: str,
    parser_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Project validated parser output into the slot seed consumed by the planner."""
    question_text = question.strip() if isinstance(question, str) else ""
    canonical_question = (
        parser_output.get("canonical_question")
        if isinstance(parser_output.get("canonical_question"), str)
        else question_text
    )
    anchors = sanitize_anchor_forms(parser_output.get("anchors"))
    if not anchors:
        anchors = sanitize_anchor_forms(detect_question_anchors(canonical_question))
    return {
        "question": question_text,
        "canonical_question": canonical_question,
        "template": template,
        "question_type": template,
        "anchors": anchors,
        "has_anchor": bool(anchors),
        "has_explicit_anchor": bool(anchors),
        "status_numeric_constraint": deepcopy(parser_output.get("status_numeric_constraint")),
        "bootstrap_operator_hints": deepcopy(parser_output.get("bootstrap_operator_hints", [])),
        "bootstrap_signals": deepcopy(parser_output.get("bootstrap_signals", {})),
        "bootstrap_candidates": deepcopy(parser_output.get("bootstrap_candidates", {})),
        "parser_strategy": parser_output.get("strategy"),
        "model_backend": parser_output.get("model_backend"),
        "ir_provenance": parser_output.get("ir_provenance"),
        "parser_output": deepcopy(parser_output),
        "surface_constraints": deepcopy(parser_output.get("surface_constraints", [])),
        "parser_evidence": deepcopy(parser_output.get("parser_evidence", {})),
    }


def attach_intent_irs_to_parser_output(
    parser_output: Dict[str, Any],
    intent_irs: List[Dict[str, Any]],
) -> None:
    """Attach normalized Intent IRs back onto one validated parser output record."""
    if not isinstance(parser_output, dict):
        return
    parser_output["intent_irs"] = sanitize_intent_ir_list(intent_irs)
