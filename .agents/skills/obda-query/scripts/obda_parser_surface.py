#!/usr/bin/env python3
"""Language-agnostic surface parsing utilities for the intent parser."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

from obda_lexical import detect_reference_markers


UTTERANCE_SPLIT_PATTERN = re.compile(r"[？?！!；;\n]+")
CONDITIONAL_PREFIX_PATTERN = re.compile(
    r"^(?P<prefix>如果有|如果存在|如果是|如果属于|如果命中|若有|若存在|若是|若属于|若命中|如果没有|如果不存在|如果不是|若没有|若不存在|若不是)[，,\s]*(?P<body>.*)$"
)
IDENTIFIER_LITERAL_PATTERN = re.compile(r"(?<![A-Za-z0-9])\d{6,}(?![A-Za-z0-9])")
URI_PATTERN = re.compile(r"https?://[^\s<>\"]+")
RESOURCE_LOCAL_NAME_PATTERN = re.compile(r"(?<![A-Za-z0-9_])[A-Za-z][A-Za-z0-9]*_[A-Za-z0-9]+(?![A-Za-z0-9_])")


def is_uri_like(value: Any) -> bool:
    """Whether a surface token looks like a URI."""
    return isinstance(value, str) and (value.startswith("http://") or value.startswith("https://"))


def anchor_uri_local_name(value: Any) -> Optional[str]:
    """Extract a URI local name for parser-level anchor detection."""
    if not isinstance(value, str) or not value:
        return None
    if "#" in value:
        local_name = value.rsplit("#", 1)[-1]
        return local_name or None
    stripped = value.rstrip("/")
    if "/" in stripped:
        local_name = stripped.rsplit("/", 1)[-1]
        return local_name or None
    return value


def infer_class_hint_from_anchor(value: str) -> Optional[str]:
    """Infer a class-like prefix from one URI/local-name anchor token."""
    local_name = anchor_uri_local_name(value) if is_uri_like(value) else value
    if not isinstance(local_name, str) or not local_name:
        return None
    if "_" in local_name:
        return local_name.split("_", 1)[0] or None
    return local_name if is_uri_like(value) else None


def detect_question_anchors(question_text: str) -> List[Dict[str, str]]:
    """Detect strong surface anchors without planner-specific semantics."""
    anchors: List[Dict[str, str]] = []
    seen: Set[tuple[str, str]] = set()

    def add_anchor(kind: str, value: str) -> None:
        cleaned = value.strip(" ，,。；;()[]{}<>\"'")
        if not cleaned:
            return
        key = (kind, cleaned)
        if key in seen:
            return
        seen.add(key)
        anchor = {"kind": kind, "value": cleaned}
        class_hint = infer_class_hint_from_anchor(cleaned)
        if isinstance(class_hint, str) and class_hint:
            anchor["class_hint"] = class_hint
        anchors.append(anchor)

    for match in IDENTIFIER_LITERAL_PATTERN.finditer(question_text):
        add_anchor("identifier_like_literal", match.group(0))
    for match in URI_PATTERN.finditer(question_text):
        add_anchor("resource_uri", match.group(0))
    for match in RESOURCE_LOCAL_NAME_PATTERN.finditer(question_text):
        candidate = match.group(0)
        if URI_PATTERN.match(candidate):
            continue
        add_anchor("resource_local_name", candidate)

    return anchors


def split_utterance_into_segments(utterance: str) -> List[str]:
    """Split a user utterance into coarse question-like segments."""
    text = utterance.strip()
    if not text:
        return []

    text = re.sub(
        r"[，,]\s*(如果(?:有|存在|是|属于|命中|没有|不存在|不是)|若(?:有|存在|是|属于|命中|没有|不存在|不是))",
        r"。\1",
        text,
    )
    segments = []
    for segment in UTTERANCE_SPLIT_PATTERN.split(text):
        cleaned = segment.strip(" ，,。；;？！! ")
        if cleaned:
            segments.append(cleaned)
    return segments


def strip_conditional_prefix(text: str) -> Dict[str, Any]:
    """Strip a leading conditional prefix and return dependency metadata."""
    match = CONDITIONAL_PREFIX_PATTERN.match(text.strip())
    if not match:
        return {
            "text": text.strip(),
            "condition_type": None,
            "condition_prefix": None,
        }

    prefix = match.group("prefix")
    body = match.group("body").strip(" ，,。；;？！! ")
    lowered_prefix = prefix.strip()
    if any(term in lowered_prefix for term in ("没有", "不存在", "不是")):
        condition_type = "empty_or_false"
    else:
        condition_type = "non_empty_or_true"
    return {
        "text": body or text.strip(),
        "condition_type": condition_type,
        "condition_prefix": prefix,
    }


def decompose_utterance_to_question_units(utterance: str) -> List[Dict[str, Any]]:
    """Decompose one utterance into dependent QuestionUnits."""
    segments = split_utterance_into_segments(utterance)
    units: List[Dict[str, Any]] = []
    active_condition: Optional[Dict[str, Any]] = None

    for index, segment in enumerate(segments, start=1):
        conditional = strip_conditional_prefix(segment)
        unit_text = conditional["text"]
        reference_markers = detect_reference_markers(unit_text)
        explicit_anchors = detect_question_anchors(unit_text)
        dependency: Optional[Dict[str, Any]] = None

        if conditional["condition_type"] and units:
            dependency = {
                "depends_on": units[-1]["unit_id"],
                "condition": conditional["condition_type"],
                "source": "conditional_prefix",
                "prefix": conditional["condition_prefix"],
            }
            active_condition = deepcopy(dependency)
        elif reference_markers and units:
            dependency = {
                "depends_on": units[-1]["unit_id"],
                "condition": "requires_previous_result",
                "source": "reference_marker",
            }
        elif active_condition is not None and units and not explicit_anchors:
            dependency = deepcopy(active_condition)
        else:
            active_condition = None

        units.append({
            "unit_id": f"q{index}",
            "text": unit_text,
            "raw_text": segment,
            "position": index,
            "reference_markers": reference_markers,
            "dependency": dependency,
        })

        if explicit_anchors:
            active_condition = None

    return units
