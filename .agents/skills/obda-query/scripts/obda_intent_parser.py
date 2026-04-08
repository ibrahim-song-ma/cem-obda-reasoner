#!/usr/bin/env python3
"""Repo-owned language intent parser facade for OBDA question understanding."""

from __future__ import annotations

from typing import Any, Dict, List

from obda_parser_backends import (
    run_no_model_backend_parse,
)
from obda_parser_model_backends import (
    run_model_backend_parse,
)
from obda_parser_contracts import (
    DEFAULT_IR_PROVENANCE,
    DEFAULT_MODEL_BACKEND,
    build_parser_input,
    build_slots_from_parser_output,
    normalize_model_backend,
    normalize_parser_strategy,
    parser_output_has_backend_failure,
    project_parser_output_for_question_unit,
    validate_and_normalize_parser_output,
)
from obda_parser_surface import decompose_utterance_to_question_units


def decompose_question_utterance(utterance: str) -> List[Dict[str, Any]]:
    """Expose parser-owned multi-unit decomposition through the facade."""
    return decompose_utterance_to_question_units(utterance)


def parse_question_unit(
    question: str,
    template: str,
    question_unit: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Parse one question unit into a repo-owned parser bundle."""
    parser_input = build_parser_input(question, template, question_unit=question_unit)
    parser_output = run_language_intent_parser(parser_input)
    slots = build_slots_from_parser_output(question, template, parser_output)
    resolved_unit = dict(question_unit) if isinstance(question_unit, dict) else {
        "unit_id": "q1",
        "text": question,
        "raw_text": question,
        "reference_markers": [],
    }
    return {
        "question_unit": resolved_unit,
        "parser_input": parser_input,
        "parser_output": parser_output,
        "slots": slots,
    }


def build_deterministic_question_unit_bundle(
    question: str,
    template: str,
    question_unit: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build one deterministic parser bundle without invoking model backends."""
    parser_input = build_parser_input(question, template, question_unit=question_unit)
    parser_input = dict(parser_input)
    parser_input["strategy"] = "DeterministicStrategy"
    parser_input["model_backend"] = DEFAULT_MODEL_BACKEND
    parser_output = validate_and_normalize_parser_output(
        parser_input,
        run_no_model_backend_parse(parser_input),
        default_ir_provenance=DEFAULT_IR_PROVENANCE,
    )
    slots = build_slots_from_parser_output(question, template, parser_output)
    resolved_unit = dict(question_unit) if isinstance(question_unit, dict) else {
        "unit_id": "q1",
        "text": question,
        "raw_text": question,
        "reference_markers": [],
    }
    return {
        "question_unit": resolved_unit,
        "parser_input": parser_input,
        "parser_output": parser_output,
        "slots": slots,
    }


def parse_question_utterance(
    utterance: str,
    template: str,
) -> Dict[str, Any]:
    """Decompose one utterance and parse each QuestionUnit through the facade."""
    surface_units = decompose_question_utterance(utterance)
    if not surface_units:
        surface_units = [{
            "unit_id": "q1",
            "text": utterance,
            "raw_text": utterance,
            "reference_markers": [],
            "dependency": None,
        }]

    parser_input = build_parser_input(utterance, template)
    model_backend = normalize_model_backend(parser_input.get("model_backend"))
    utterance_parser_output = None
    if model_backend != DEFAULT_MODEL_BACKEND:
        parser_input = dict(parser_input)
        parser_input["question_units_hint"] = surface_units
        utterance_parser_output = run_language_intent_parser(parser_input)
    utterance_backend_failed = parser_output_has_backend_failure(utterance_parser_output)

    if (
        isinstance(utterance_parser_output, dict)
        and not utterance_backend_failed
        and isinstance(utterance_parser_output.get("question_units"), list)
    ):
        question_units = []
        for unit in utterance_parser_output.get("question_units", []):
            if not isinstance(unit, dict):
                continue
            normalized_unit = dict(unit)
            if not isinstance(normalized_unit.get("text"), str) or not normalized_unit.get("text"):
                normalized_unit["text"] = (
                    normalized_unit.get("raw_text")
                    or normalized_unit.get("normalized_text")
                    or utterance
                )
            question_units.append(normalized_unit)
        question_units = question_units or surface_units
        unit_parses = []
        for index, unit in enumerate(question_units):
            unit_output = project_parser_output_for_question_unit(
                utterance,
                template,
                utterance_parser_output,
                unit,
                unit_index=index,
            )
            unit_question = unit.get("raw_text") or unit.get("text") or utterance
            unit_parses.append({
                "question_unit": unit,
                "parser_input": build_parser_input(unit_question, template, question_unit=unit),
                "parser_output": unit_output,
                "slots": build_slots_from_parser_output(unit_question, template, unit_output),
            })
    else:
        question_units = surface_units
        bundle_builder = (
            build_deterministic_question_unit_bundle
            if utterance_backend_failed
            else parse_question_unit
        )
        unit_parses = [
            bundle_builder(unit.get("text") or utterance, template, question_unit=unit)
            for unit in question_units
        ]
    return {
        "question_units": question_units,
        "unit_parses": unit_parses,
        "parser_output": utterance_parser_output if isinstance(utterance_parser_output, dict) else None,
    }


def run_language_intent_parser(
    parser_input: Dict[str, Any],
) -> Dict[str, Any]:
    """Run the parser facade through one repo-owned backend adapter."""
    strategy = normalize_parser_strategy(parser_input.get("strategy"))
    model_backend = normalize_model_backend(parser_input.get("model_backend"))
    if model_backend != DEFAULT_MODEL_BACKEND and strategy == "DeterministicStrategy":
        strategy = "HybridStrategy"
    normalized_input = dict(parser_input)
    normalized_input["strategy"] = strategy
    normalized_input["model_backend"] = model_backend

    deterministic_output = run_no_model_backend_parse(normalized_input)
    if model_backend == DEFAULT_MODEL_BACKEND:
        return validate_and_normalize_parser_output(
            normalized_input,
            deterministic_output,
            default_ir_provenance=DEFAULT_IR_PROVENANCE,
        )

    parser_output = run_model_backend_parse(normalized_input, deterministic_output)
    return validate_and_normalize_parser_output(
        normalized_input,
        parser_output,
        default_ir_provenance="hybrid_ir",
    )
