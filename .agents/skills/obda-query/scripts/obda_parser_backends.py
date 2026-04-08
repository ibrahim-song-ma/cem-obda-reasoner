#!/usr/bin/env python3
"""Repo-owned parser backend implementations for language intent parsing."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from obda_parser_contracts import (
    DEFAULT_IR_PROVENANCE,
    DEFAULT_MODEL_BACKEND,
    build_parser_exactness_report,
    normalize_parser_strategy,
    sanitize_bootstrap_operator_hint_store,
)
from obda_parser_surface import detect_question_anchors
from obda_lexical import (
    bootstrap_operator_hints,
    collect_lexical_bootstrap_recall,
    detect_reference_markers,
    lossless_canonicalize_text,
)


def build_surface_constraints_from_bootstrap_recall(lexical_recall: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build parser-level surface constraints from deterministic recall only."""
    constraints: List[Dict[str, Any]] = []
    candidate_store = lexical_recall.get("bootstrap_candidates", {})
    if isinstance(candidate_store, dict):
        for slot_name, bucket in candidate_store.items():
            if not isinstance(slot_name, str) or not isinstance(bucket, list):
                continue
            for item in bucket:
                if not isinstance(item, dict):
                    continue
                text = item.get("text")
                if not isinstance(text, str) or not text.strip():
                    continue
                constraints.append({
                    "kind": "surface_constraint",
                    "slot": slot_name,
                    "text": text.strip(),
                    "source": item.get("source"),
                })
    numeric_constraint = lexical_recall.get("status_numeric_constraint")
    if isinstance(numeric_constraint, dict):
        constraints.append({
            "kind": "comparison",
            "slot": "status_numeric_constraint",
            "constraint": deepcopy(numeric_constraint),
        })
    return constraints


def run_no_model_backend_parse(
    parser_input: Dict[str, Any],
) -> Dict[str, Any]:
    """Run the deterministic parser path used by the no-model backend."""
    raw_question = (
        parser_input.get("utterance")
        if isinstance(parser_input.get("utterance"), str)
        else ""
    )
    question_unit = (
        parser_input.get("question_unit")
        if isinstance(parser_input.get("question_unit"), dict)
        else None
    )
    canonicalization = lossless_canonicalize_text(raw_question)
    canonical_question = (
        canonicalization.get("canonical_text")
        if isinstance(canonicalization.get("canonical_text"), str)
        else raw_question
    )
    anchors = detect_question_anchors(canonical_question)
    lexical_recall = collect_lexical_bootstrap_recall(
        canonical_question,
        anchors,
        question_unit=question_unit,
    )
    operator_hints = bootstrap_operator_hints(lexical_recall)
    operator_hint_store = sanitize_bootstrap_operator_hint_store(
        lexical_recall.get("bootstrap_operator_hints", [])
    )
    if not operator_hint_store:
        operator_hint_store = sanitize_bootstrap_operator_hint_store(operator_hints)
    surface_constraints = build_surface_constraints_from_bootstrap_recall(lexical_recall)
    question_unit_record = {
        "unit_id": question_unit.get("unit_id") if isinstance(question_unit, dict) else "q1",
        "raw_text": raw_question,
        "normalized_text": canonical_question,
        "dependency": deepcopy(question_unit.get("dependency")) if isinstance(question_unit, dict) else None,
        "reference_markers": (
            list(question_unit.get("reference_markers", []))
            if isinstance(question_unit, dict) and isinstance(question_unit.get("reference_markers"), list)
            else detect_reference_markers(raw_question)
        ),
        "anchor_forms": deepcopy(anchors),
        "comparators": (
            [deepcopy(lexical_recall["status_numeric_constraint"])]
            if isinstance(lexical_recall.get("status_numeric_constraint"), dict)
            else []
        ),
        "question_acts": operator_hints,
        "surface_constraints": deepcopy(surface_constraints),
        "ambiguities": [],
        "confidence": None,
    }
    return {
        "strategy": normalize_parser_strategy(parser_input.get("strategy")),
        "model_backend": DEFAULT_MODEL_BACKEND,
        "ir_provenance": DEFAULT_IR_PROVENANCE,
        "canonical_question": canonical_question,
        "question_units": [question_unit_record],
        "dependency_dag": None,
        "intent_irs": [],
        "parser_confidence": None,
        "ambiguities": [],
        "clarification_candidates": [],
        "parser_evidence": build_parser_exactness_report(
            raw_question,
            canonicalization,
            anchors,
            lexical_recall,
        ),
        "anchors": deepcopy(anchors),
        "bootstrap_operator_hints": deepcopy(operator_hint_store),
        "bootstrap_signals": deepcopy(lexical_recall.get("bootstrap_signals", {})),
        "bootstrap_candidates": deepcopy(lexical_recall.get("bootstrap_candidates", {})),
        "status_numeric_constraint": deepcopy(lexical_recall.get("status_numeric_constraint")),
        "surface_constraints": deepcopy(surface_constraints),
    }
