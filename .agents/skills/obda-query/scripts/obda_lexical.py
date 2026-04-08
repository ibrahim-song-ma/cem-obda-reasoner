#!/usr/bin/env python3
"""Lexical recall resources for OBDA question understanding.

This module isolates phrase tables and regex-based recall from planner core.
It performs recall and normalization only, then returns structured candidates
and operator hints for the semantic planner to consume.
"""

from __future__ import annotations

import re
import unicodedata
from copy import deepcopy
from typing import Any, Dict, List, Optional


CAUSE_LINKER_PATTERN = r"(?:而|从而|进而|继而|所以|导致|引发|造成|使得|致使|使|令)"
CAUSE_PATTERN = re.compile(
    rf"(?:因为|由于)(?P<cause>.+?)(?=(?:{CAUSE_LINKER_PATTERN}|，|,|。|？|\?|哪些|哪个|哪位|哪类|谁|什么|$))"
)
WHICH_PATTERN = re.compile(r"(哪些|哪个|哪位|哪类)(?P<tail>[^？?。]*)")
STATUS_CHECK_PATTERN = re.compile(
    r"(?:是否(?:存在|有)?|有无|有没有|是否为|是否属于)(?P<status>.+?)(?=(?:情况|问题|现象|记录|表现)?(?:[？?，,。]|如果|并|以及|$))"
)
ASKS_FOR_PATTERN = re.compile(
    r"(?:有什么|有哪些|什么)(?P<target>.+?)(?=(?:[？?，,。]|如果|并|以及|$))"
)
LOOKUP_TARGET_PATTERN = re.compile(
    r"(?P<target>.+?)(?:是|为)(?:多少|什么|几|啥)(?:[分次个元岁条项]?)(?:[？?，,。!！]*)$"
)
CAUSE_WITH_EFFECT_PATTERN = re.compile(
    rf"(?P<prefix>.*?)(?:因为|由于)(?P<cause>.+?){CAUSE_LINKER_PATTERN}(?P<suffix>.+)$"
)
NUMERIC_COMPARISON_PATTERN = re.compile(
    r"^(?P<attribute>.+?)\s*(?P<op>不超过|不高于|小于等于|<=|≤|至多|最多|小于|低于|少于|<|不少于|不低于|大于等于|>=|≥|至少|起码|高于|大于|超过|多于|>)\s*(?P<value>-?\d+(?:\.\d+)?)\s*$"
)
VALUE_SUFFIX_COMPARISON_PATTERN = re.compile(
    r"^(?P<attribute>.+?)\s*(?P<value>-?\d+(?:\.\d+)?)\s*(?P<op>以上|以下)\s*$"
)
NUMERIC_OPERATOR_PREFIX_PATTERN = re.compile(
    r"是否(?=(?:不超过|不高于|小于等于|<=|≤|至多|最多|小于|低于|少于|<|不少于|不低于|大于等于|>=|≥|至少|起码|高于|大于|超过|多于|>))"
)
NUMERIC_TRAILING_UNIT_PATTERN = re.compile(r"(?P<number>-?\d+(?:\.\d+)?)(?:[^\d\s]{1,3})$")
NUMERIC_LITERAL_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")
COMPARATOR_SYMBOL_PATTERN = re.compile(r"<=|>=|≤|≥|<|>|=")

NUMERIC_XSD_MARKERS = ("int", "integer", "decimal", "float", "double", "long", "short")
REFERENCE_MARKERS = ("这个", "这些", "它", "他们", "她们", "上述", "上面", "其中", "分别", "对应的", "相关的")
SENTENCE_PARTICLE_SUFFIX_PATTERN = re.compile(r"(?:了|过|呢|吗|呀|啊)+$")
NUMERIC_COMPARISON_OPERATOR_MAP = {
    "不超过": "lte",
    "不高于": "lte",
    "小于等于": "lte",
    "<=": "lte",
    "≤": "lte",
    "至多": "lte",
    "最多": "lte",
    "以下": "lte",
    "小于": "lt",
    "低于": "lt",
    "少于": "lt",
    "<": "lt",
    "不少于": "gte",
    "不低于": "gte",
    "大于等于": "gte",
    ">=": "gte",
    "≥": "gte",
    "至少": "gte",
    "起码": "gte",
    "以上": "gte",
    "高于": "gt",
    "大于": "gt",
    "超过": "gt",
    "多于": "gt",
    ">": "gt",
}


def unique_preserve_order(values: List[Any]) -> List[Any]:
    """Preserve order while removing duplicates."""
    seen = set()
    results: List[Any] = []
    for value in values:
        marker = repr(value)
        if marker in seen:
            continue
        seen.add(marker)
        results.append(value)
    return results


def lossless_canonicalize_text(text: Optional[str]) -> Dict[str, Any]:
    """Return a losslessly canonicalized view for parser exactness checks."""
    raw_text = text if isinstance(text, str) else ""
    canonical_text = unicodedata.normalize("NFKC", raw_text)
    canonical_text = canonical_text.replace("\r\n", "\n").replace("\r", "\n")
    canonical_text = canonical_text.replace("\u3000", " ")
    return {
        "raw_text": raw_text,
        "canonical_text": canonical_text,
        "text_changed": canonical_text != raw_text,
        "exact_literals": {
            "numeric_literals": unique_preserve_order(NUMERIC_LITERAL_PATTERN.findall(canonical_text)),
            "comparator_symbols": unique_preserve_order(COMPARATOR_SYMBOL_PATTERN.findall(canonical_text)),
        },
    }


def normalize_match_text(value: Any) -> str:
    """Normalize labels/local names/question fragments for lightweight lexical matching."""
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^\w\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def split_constraint_terms(text: Optional[str]) -> List[str]:
    """Split one semantic constraint into lexical terms without domain-specific suffix tables."""
    if not isinstance(text, str):
        return []

    cleaned = text.strip(" ，,。？? ")
    if not cleaned:
        return []

    parts = re.split(r"[、/／,，和及与或]", cleaned)
    terms: List[str] = []
    for part in parts:
        item = part.strip()
        if not item:
            continue
        terms.append(item)
    return unique_preserve_order([term for term in terms if term])


def extract_which_tail(question: Optional[str]) -> Optional[str]:
    """Extract the surface tail from a `哪些/哪个/哪类` question pattern."""
    if not isinstance(question, str):
        return None
    match = WHICH_PATTERN.search(question)
    if not match:
        return None
    tail = match.group("tail")
    return tail.strip() if isinstance(tail, str) and tail.strip() else None


def is_numeric_range_uri(range_uri: Any) -> bool:
    """Whether a datatype/range string looks numeric."""
    if not isinstance(range_uri, str):
        return False
    lowered = range_uri.lower()
    return any(marker in lowered for marker in NUMERIC_XSD_MARKERS)


def normalize_slot_text(text: Optional[str]) -> Optional[str]:
    """Normalize extracted slot text without imposing domain-specific semantics."""
    if not isinstance(text, str):
        return None
    cleaned = text.strip(" ，,。？?；;:：")
    cleaned = re.sub(r"^(?:是否(?:存在|有)?|有无|有没有|是否为|是否属于)", "", cleaned).strip()
    cleaned = re.sub(r"(?:的)?(?:情况|问题|现象|记录|表现)$", "", cleaned).strip()
    cleaned = re.sub(r"(?:的)+$", "", cleaned).strip()
    return cleaned or None


def parse_numeric_literal(value: str) -> Any:
    """Parse a numeric literal into int or float when possible."""
    return float(value) if "." in value else int(value)


def strip_sentence_particles(text: Optional[str]) -> Optional[str]:
    """Strip lightweight sentence particles from one surface clause."""
    normalized = normalize_slot_text(text)
    if not normalized:
        return None
    normalized = SENTENCE_PARTICLE_SUFFIX_PATTERN.sub("", normalized).strip()
    return normalized or None


def split_causal_surface(text: Optional[str]) -> Optional[Dict[str, Optional[str]]]:
    """Split one surface clause into prefix/cause/suffix using generic causal connectives."""
    if not isinstance(text, str):
        return None
    cleaned = text.strip(" ，,。？?；;:：")
    if not cleaned or ("因为" not in cleaned and "由于" not in cleaned):
        return None

    linked_match = CAUSE_WITH_EFFECT_PATTERN.match(cleaned)
    if linked_match:
        prefix = strip_sentence_particles(linked_match.group("prefix"))
        cause = strip_sentence_particles(linked_match.group("cause"))
        suffix = strip_sentence_particles(linked_match.group("suffix"))
        if cause:
            return {
                "prefix": prefix,
                "cause": cause,
                "suffix": suffix,
            }

    cause_match = CAUSE_PATTERN.search(cleaned)
    if not cause_match:
        return None
    prefix = strip_sentence_particles(cleaned[:cause_match.start()])
    suffix = strip_sentence_particles(cleaned[cause_match.end():])
    cause = strip_sentence_particles(cause_match.group("cause"))
    if not cause:
        return None
    return {
        "prefix": prefix,
        "cause": cause,
        "suffix": suffix,
    }


def parse_numeric_constraint_text(text: Optional[str]) -> Optional[Dict[str, Any]]:
    """Extract a generic explicit numeric comparison from slot text."""
    if not isinstance(text, str):
        return None
    cleaned = normalize_slot_text(text)
    if not cleaned:
        return None

    candidate_texts = unique_preserve_order([
        cleaned,
        NUMERIC_OPERATOR_PREFIX_PATTERN.sub("", cleaned),
        NUMERIC_TRAILING_UNIT_PATTERN.sub(r"\g<number>", cleaned),
        NUMERIC_TRAILING_UNIT_PATTERN.sub(r"\g<number>", NUMERIC_OPERATOR_PREFIX_PATTERN.sub("", cleaned)),
    ])

    for candidate in candidate_texts:
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        match = NUMERIC_COMPARISON_PATTERN.match(candidate)
        if match:
            attribute_text = normalize_slot_text(match.group("attribute"))
            operator = NUMERIC_COMPARISON_OPERATOR_MAP.get(match.group("op"))
            if attribute_text and operator:
                return {
                    "attribute_text": attribute_text,
                    "op": operator,
                    "value": parse_numeric_literal(match.group("value")),
                    "raw_text": candidate,
                }

        match = VALUE_SUFFIX_COMPARISON_PATTERN.match(candidate)
        if match:
            attribute_text = normalize_slot_text(match.group("attribute"))
            operator = NUMERIC_COMPARISON_OPERATOR_MAP.get(match.group("op"))
            if attribute_text and operator:
                return {
                    "attribute_text": attribute_text,
                    "op": operator,
                    "value": parse_numeric_literal(match.group("value")),
                    "raw_text": candidate,
                }
    return None


def detect_reference_markers(text: str) -> List[str]:
    """Detect lightweight discourse references such as 这个/这些/分别."""
    markers = []
    for marker in REFERENCE_MARKERS:
        if marker in text:
            markers.append(marker)
    return markers


def register_bootstrap_candidate(
    slots: Dict[str, Any],
    slot_name: str,
    value: Optional[str],
    source: str,
) -> None:
    """Register one bootstrap semantic candidate for later semantic interpretation."""
    if not isinstance(slots, dict) or not isinstance(slot_name, str) or not slot_name:
        return
    cleaned = normalize_slot_text(value)
    if not cleaned:
        return
    candidate_store = slots.setdefault("bootstrap_candidates", {})
    if not isinstance(candidate_store, dict):
        candidate_store = {}
        slots["bootstrap_candidates"] = candidate_store
    bucket = candidate_store.setdefault(slot_name, [])
    if not isinstance(bucket, list):
        bucket = []
        candidate_store[slot_name] = bucket
    candidate = {"text": cleaned, "source": source}
    if candidate not in bucket:
        bucket.append(candidate)


def register_bootstrap_operator_hint(
    slots: Dict[str, Any],
    operator: Optional[str],
    source: str,
) -> None:
    """Register one bootstrap operator hint for later intent construction."""
    if not isinstance(slots, dict) or not isinstance(operator, str):
        return
    cleaned = operator.strip()
    if not cleaned:
        return
    hint_store = slots.setdefault("bootstrap_operator_hints", [])
    if not isinstance(hint_store, list):
        hint_store = []
        slots["bootstrap_operator_hints"] = hint_store
    hint = {"operator": cleaned, "source": source}
    if hint not in hint_store:
        hint_store.append(hint)


def bootstrap_operator_hints(slots: Dict[str, Any]) -> List[str]:
    """Return bootstrap operator hints in stable order."""
    hint_store = slots.get("bootstrap_operator_hints")
    if not isinstance(hint_store, list):
        return []
    operators: List[str] = []
    for item in hint_store:
        if not isinstance(item, dict):
            continue
        operator = item.get("operator")
        if isinstance(operator, str) and operator.strip():
            operators.append(operator.strip())
    return unique_preserve_order(operators)


def derive_bootstrap_signals(slots: Dict[str, Any]) -> Dict[str, bool]:
    """Derive compatibility bootstrap signals from operator hints and numeric constraints."""
    operator_set = set(bootstrap_operator_hints(slots))
    numeric_constraint = slots.get("status_numeric_constraint")
    return {
        "asks_solution": "remediation" in operator_set,
        "asks_explanation": "explain" in operator_set,
        "status_check_requested": "status_check" in operator_set or isinstance(numeric_constraint, dict),
    }


def bootstrap_candidate_text(slots: Dict[str, Any], slot_name: str) -> Optional[str]:
    """Return the primary bootstrap candidate text for one semantic slot."""
    candidate_store = slots.get("bootstrap_candidates")
    if not isinstance(candidate_store, dict):
        return None
    bucket = candidate_store.get(slot_name)
    if not isinstance(bucket, list):
        return None
    for item in bucket:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            return text.strip()
    return None


def strip_anchor_literals_from_question(question_text: str, anchors: List[Dict[str, Any]]) -> str:
    """Remove explicit anchor literals so lexical recall can focus on semantic surface text."""
    stripped_question_text = question_text
    for anchor in anchors:
        if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
            stripped_question_text = stripped_question_text.replace(anchor["value"], " ")
    return normalize_slot_text(stripped_question_text) or question_text


def build_lexical_recall_context(
    question_text: str,
    anchors: List[Dict[str, Any]],
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build one lexical recall context shared by ordered lexical rules."""
    return {
        "question_text": question_text,
        "stripped_question_text": strip_anchor_literals_from_question(question_text, anchors),
        "reference_markers": (
            question_unit.get("reference_markers", [])
            if isinstance(question_unit, dict) and isinstance(question_unit.get("reference_markers"), list)
            else []
        ),
    }


def apply_question_numeric_status_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall explicit numeric status constraints from the anchor-stripped question text."""
    global_numeric_constraint = parse_numeric_constraint_text(context.get("stripped_question_text"))
    if not isinstance(global_numeric_constraint, dict):
        return
    register_bootstrap_candidate(
        recall,
        "status_or_problem_text",
        global_numeric_constraint.get("attribute_text"),
        "question_numeric_attribute",
    )
    recall["status_numeric_constraint"] = global_numeric_constraint
    register_bootstrap_operator_hint(recall, "status_check", "numeric_constraint")


def apply_cause_pattern_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall explicit cause clauses from surface lexical patterns."""
    causal_surface = split_causal_surface(context.get("question_text", ""))
    if not isinstance(causal_surface, dict):
        return
    register_bootstrap_candidate(
        recall,
        "cause_text",
        causal_surface.get("cause"),
        "cause_pattern",
    )


def apply_status_pattern_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall abstract or numeric status clauses from status-oriented lexical patterns."""
    status_match = STATUS_CHECK_PATTERN.search(context.get("question_text", ""))
    if not status_match:
        return
    status_text = normalize_slot_text(status_match.group("status"))
    if not status_text:
        return
    numeric_constraint = recall.get("status_numeric_constraint")
    if not isinstance(numeric_constraint, dict):
        numeric_constraint = parse_numeric_constraint_text(status_text)
    if isinstance(numeric_constraint, dict):
        register_bootstrap_candidate(
            recall,
            "status_or_problem_text",
            numeric_constraint.get("attribute_text"),
            "status_numeric_attribute",
        )
        recall["status_numeric_constraint"] = numeric_constraint
    else:
        register_bootstrap_candidate(
            recall,
            "status_or_problem_text",
            status_text,
            "status_pattern",
        )
    register_bootstrap_operator_hint(recall, "status_check", "status_pattern")


def apply_which_pattern_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall target/result hints from generic `哪些/都有什么` question patterns."""
    which_match = WHICH_PATTERN.search(context.get("question_text", ""))
    if not which_match:
        return
    tail = which_match.group("tail").strip(" ，,。？? ")
    causal_surface = split_causal_surface(tail)
    result_hint = (
        causal_surface.get("prefix")
        if isinstance(causal_surface, dict) and isinstance(causal_surface.get("prefix"), str) and causal_surface.get("prefix")
        else tail
    )
    register_bootstrap_candidate(recall, "result_hint", result_hint, "which_pattern")
    if isinstance(causal_surface, dict):
        register_bootstrap_candidate(
            recall,
            "cause_text",
            causal_surface.get("cause"),
            "which_cause_pattern",
        )
        if bootstrap_candidate_text(recall, "action_text") is None:
            register_bootstrap_candidate(
                recall,
                "action_text",
                causal_surface.get("suffix"),
                "which_causal_suffix",
            )
    tail = re.sub(r"(了|过|呢|吗|呀|啊)+$", "", tail).strip()
    for suffix in ("投诉", "报修", "办理", "购买", "订购", "使用", "反馈", "关联", "命中"):
        if tail.endswith(suffix):
            register_bootstrap_candidate(recall, "action_text", suffix, "which_suffix")
            break
    if bootstrap_candidate_text(recall, "action_text") is None and tail:
        compact_tail = re.sub(r"(客户|用户|事件|工单|策略|感知|产品|网络|套餐)+", "", tail).strip()
        compact_tail = re.sub(r"(了|过|的)$", "", compact_tail).strip()
        if compact_tail:
            register_bootstrap_candidate(recall, "action_text", compact_tail, "which_compact_tail")


def apply_asks_for_target_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall target nouns from generic `有什么/是什么` ask-for patterns."""
    asks_for_match = ASKS_FOR_PATTERN.search(context.get("question_text", ""))
    if not asks_for_match or bootstrap_candidate_text(recall, "target_text"):
        return
    target_text = normalize_slot_text(asks_for_match.group("target"))
    if isinstance(split_causal_surface(target_text), dict):
        return
    if target_text:
        register_bootstrap_candidate(recall, "target_text", target_text, "asks_for_pattern")


def apply_lookup_target_rule(recall: Dict[str, Any], context: Dict[str, Any]) -> None:
    """Recall direct lookup targets when the question surface is not already status/explanation-shaped."""
    if (
        bootstrap_candidate_text(recall, "target_text") is not None
        or bool(context.get("reference_markers"))
        or recall.get("status_numeric_constraint") is not None
        or bootstrap_candidate_text(recall, "status_or_problem_text") is not None
    ):
        return
    lookup_question_text = str(context.get("stripped_question_text", "")).strip(" ，,。？?!！;；")
    lookup_match = LOOKUP_TARGET_PATTERN.search(lookup_question_text)
    if not lookup_match:
        return
    target_text = normalize_slot_text(lookup_match.group("target"))
    if target_text and "的" in target_text:
        target_text = normalize_slot_text(target_text.split("的")[-1])
    if target_text:
        register_bootstrap_candidate(recall, "target_text", target_text, "lookup_target_pattern")


LEXICAL_BOOTSTRAP_RECALL_RULES = (
    apply_question_numeric_status_rule,
    apply_cause_pattern_rule,
    apply_status_pattern_rule,
    apply_which_pattern_rule,
    apply_asks_for_target_rule,
    apply_lookup_target_rule,
)


def collect_lexical_bootstrap_recall(
    question_text: str,
    anchors: List[Dict[str, Any]],
    question_unit: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Collect lexical recall candidates and operator hints without making planning decisions."""
    recall: Dict[str, Any] = {
        "status_numeric_constraint": None,
        "bootstrap_operator_hints": [],
        "bootstrap_signals": {},
        "bootstrap_candidates": {},
    }
    context = build_lexical_recall_context(question_text, anchors, question_unit=question_unit)
    for rule in LEXICAL_BOOTSTRAP_RECALL_RULES:
        rule(recall, context)
    recall["bootstrap_signals"] = derive_bootstrap_signals(recall)
    return deepcopy(recall)
