#!/usr/bin/env python3
"""Repo-owned grounding policy for slot admissibility, sample recall, and propagation."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from obda_lexical import normalize_match_text


IDENTIFIERISH_FRAGMENT_PATTERN = re.compile(r"[a-z0-9]{2,}")
KEYLIKE_ATTRIBUTE_MARKERS = ("id", "编号", "编码", "代码", "标识", "序号")


def unique_preserve_order(values: List[str]) -> List[str]:
    """Return stable unique strings without importing planner helpers."""
    seen = set()
    results: List[str] = []
    for value in values:
        if not isinstance(value, str) or not value or value in seen:
            continue
        seen.add(value)
        results.append(value)
    return results


def identifierish_fragments(value: Any) -> List[str]:
    """Extract generic alnum fragments such as ids, bands, versions, or model tags."""
    normalized = normalize_match_text(value)
    if not normalized:
        return []
    compact = normalized.replace(" ", "")
    return unique_preserve_order(IDENTIFIERISH_FRAGMENT_PATTERN.findall(compact))


def has_shared_identifierish_fragment(left: Any, right: Any) -> bool:
    """Whether two texts share a compact identifier-like fragment."""
    left_fragments = set(identifierish_fragments(left))
    right_fragments = set(identifierish_fragments(right))
    return bool(left_fragments and right_fragments and left_fragments.intersection(right_fragments))


def compact_normalized_text(value: Any) -> str:
    """Return normalized text without spaces for generic fragment checks."""
    return normalize_match_text(value).replace(" ", "")


def longest_shared_compact_fragment(left: Any, right: Any) -> int:
    """Return the longest shared compact fragment length between two normalized texts."""
    left_text = compact_normalized_text(left)
    right_text = compact_normalized_text(right)
    if not left_text or not right_text:
        return 0

    max_size = min(len(left_text), len(right_text))
    for size in range(max_size, 1, -1):
        for start in range(len(left_text) - size + 1):
            fragment = left_text[start:start + size]
            if fragment and fragment in right_text:
                return size
    return 0


def has_shared_surface_fragment(left: Any, right: Any, min_size: int = 2) -> bool:
    """Whether two surface strings share a sufficiently strong normalized fragment."""
    return longest_shared_compact_fragment(left, right) >= max(2, int(min_size))


def slot_input_requires_status_like_binding(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot should bind only to generic status/score semantics."""
    return (
        slot_input.get("slot_name") == "status_or_problem_text"
        and slot_input.get("constraint_mode") == "status_check"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def node_supports_status_like_binding(node: Dict[str, Any]) -> bool:
    """Whether a manifest node can support a generic status-check constraint."""
    if node.get("node_type") not in {"attribute", "value"}:
        return False
    return not bool(node.get("numeric"))


def slot_input_requires_numeric_attribute_binding(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot must bind to numeric attributes because it carries an explicit comparator."""
    return (
        slot_input.get("slot_name") == "status_or_problem_text"
        and isinstance(slot_input.get("comparison"), dict)
    )


def node_supports_numeric_attribute_binding(node: Dict[str, Any]) -> bool:
    """Whether a manifest node can support explicit numeric comparison lowering."""
    return node.get("node_type") == "attribute" and bool(node.get("numeric"))


def node_is_boolean_like(node: Dict[str, Any]) -> bool:
    """Whether one manifest node behaves like a boolean/classification flag."""
    if node.get("node_type") != "attribute":
        return False
    range_uri = node.get("range")
    if not isinstance(range_uri, str):
        return False
    return "bool" in range_uri.lower()


def node_is_key_like_attribute(node: Dict[str, Any]) -> bool:
    """Whether one attribute is a low-information key/id field that should not ground free-text causes/actions."""
    if node.get("node_type") != "attribute":
        return False
    surfaces = [
        normalize_match_text(node.get("label")),
        normalize_match_text(node.get("local_name")),
    ]
    for surface in surfaces:
        compact = surface.replace(" ", "")
        if compact.endswith(KEYLIKE_ATTRIBUTE_MARKERS):
            return True
    return False


def slot_input_disallows_numeric_semantics(slot_input: Dict[str, Any]) -> bool:
    """Whether a slot should exclude numeric nodes because it expresses free-text semantics."""
    slot_name = slot_input.get("slot_name")
    if slot_name in {"cause_text", "action_or_state_text"}:
        return True
    return (
        slot_name == "status_or_problem_text"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def manifest_nodes_for_slot(manifest: Optional[Dict[str, Any]], slot_input: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return manifest nodes that are admissible for a given semantic slot."""
    if not isinstance(manifest, dict):
        return []
    allowed = set(slot_input.get("allowed_node_types", []))
    nodes: List[Dict[str, Any]] = []
    if "class" in allowed:
        nodes.extend(manifest.get("class_nodes", []))
    if "attribute" in allowed:
        nodes.extend(manifest.get("attribute_nodes", []))
    if "relation" in allowed:
        nodes.extend(manifest.get("relation_nodes", []))
    if "value" in allowed:
        nodes.extend(manifest.get("value_nodes", []))
    filtered = [node for node in nodes if isinstance(node, dict)]
    if slot_input_requires_numeric_attribute_binding(slot_input):
        return [node for node in filtered if node_supports_numeric_attribute_binding(node)]
    if slot_input_requires_status_like_binding(slot_input):
        filtered = [node for node in filtered if node_supports_status_like_binding(node)]
    if slot_input.get("slot_name") in {"cause_text", "action_or_state_text"}:
        filtered = [
            node for node in filtered
            if not node_is_boolean_like(node) and not node_is_key_like_attribute(node)
        ]
    if slot_input_disallows_numeric_semantics(slot_input):
        filtered = [node for node in filtered if not bool(node.get("numeric"))]
    return filtered


def node_catalog_source(node: Optional[Dict[str, Any]]) -> Optional[str]:
    """Return the manifest/value-catalog provenance for one binder node."""
    if not isinstance(node, dict):
        return None
    catalog_source = node.get("catalog_source")
    if isinstance(catalog_source, str) and catalog_source:
        return catalog_source
    node_type = node.get("node_type")
    if node_type == "class":
        return "manifest_class"
    if node_type == "attribute":
        return "manifest_attribute"
    if node_type == "relation":
        return "manifest_relation"
    if node_type == "value":
        return "value_catalog"
    return None


def slot_prefers_literal_value_recall(slot_input: Optional[Dict[str, Any]]) -> bool:
    """Whether one slot genuinely benefits from bounded literal-value recall."""
    if not isinstance(slot_input, dict):
        return False
    slot_name = slot_input.get("slot_name")
    if slot_name in {"anchor_text", "cause_text", "action_or_state_text", "target_text"}:
        return True
    return (
        slot_name == "status_or_problem_text"
        and not isinstance(slot_input.get("comparison"), dict)
    )


def slot_inputs_need_value_catalog(slot_inputs: List[Dict[str, Any]]) -> bool:
    """Whether the planner should pay the cost of bounded sample-value recall."""
    return any(slot_prefers_literal_value_recall(slot_input) for slot_input in slot_inputs)


def node_source_binding_adjustment(
    slot_input: Dict[str, Any],
    node: Dict[str, Any],
    lexical_score: float,
    semantic_similarity: float,
) -> float:
    """Adjust binder scores so manifest structure dominates and sample values stay auxiliary."""
    slot_name = slot_input.get("slot_name")
    catalog_source = node_catalog_source(node)
    node_type = node.get("node_type")

    adjustment = 0.0
    if node_type == "attribute":
        if slot_name == "anchor_text" and bool(node.get("subject_key")):
            adjustment += 3.0
        if slot_name == "anchor_text" and node.get("validation_source") == "mapping":
            adjustment += 2.0
        if (
            slot_input_requires_numeric_attribute_binding(slot_input)
            and bool(node.get("numeric"))
            and node.get("validation_source") == "mapping"
        ):
            adjustment += 1.5
        return adjustment

    if catalog_source != "sample_value":
        return adjustment

    exact_label_match = (
        normalize_match_text(slot_input.get("text")) == normalize_match_text(node.get("label"))
    )
    identifier_overlap = has_shared_identifierish_fragment(slot_input.get("text"), node.get("label"))
    surface_fragment_overlap = has_shared_surface_fragment(slot_input.get("text"), node.get("label"))
    if slot_name == "anchor_text":
        return 2.0 if exact_label_match else -8.0
    if slot_input_requires_numeric_attribute_binding(slot_input):
        return -10.0
    if not slot_prefers_literal_value_recall(slot_input):
        return -7.0
    if identifier_overlap and semantic_similarity >= 0.12:
        return -2.0
    if surface_fragment_overlap and (lexical_score >= 1.0 or semantic_similarity >= 0.12):
        return -3.0
    if exact_label_match:
        return -1.0
    if lexical_score >= 12.0:
        return -2.5
    if lexical_score >= 8.0 and semantic_similarity >= 0.28:
        return -4.0
    return -7.0


def sample_value_candidate_allowed(
    slot_input: Dict[str, Any],
    node: Dict[str, Any],
    lexical_score: float,
    semantic_similarity: float,
) -> bool:
    """Whether one sample-derived value node is strong enough to enter binding competition."""
    if node_catalog_source(node) != "sample_value":
        return True

    slot_name = slot_input.get("slot_name")
    slot_text = normalize_match_text(slot_input.get("text"))
    node_label = normalize_match_text(node.get("label"))
    exact_label_match = bool(slot_text) and slot_text == node_label
    overlapping_label_match = bool(slot_text) and bool(node_label) and (
        slot_text in node_label or node_label in slot_text
    )
    identifier_overlap = has_shared_identifierish_fragment(slot_input.get("text"), node.get("label"))
    surface_fragment_overlap = has_shared_surface_fragment(slot_input.get("text"), node.get("label"))

    if slot_name == "anchor_text":
        return exact_label_match
    if slot_input_requires_numeric_attribute_binding(slot_input):
        return False
    if slot_name in {"cause_text", "action_or_state_text", "target_text"}:
        return (
            exact_label_match
            or overlapping_label_match
            or (identifier_overlap and (lexical_score >= 4.0 or semantic_similarity >= 0.12))
            or (surface_fragment_overlap and (lexical_score >= 1.0 or semantic_similarity >= 0.12))
            or (lexical_score >= 8.0 and semantic_similarity >= 0.28)
        )
    if slot_name == "status_or_problem_text":
        return exact_label_match or overlapping_label_match or (lexical_score >= 12.0 and semantic_similarity >= 0.3)
    return False


def slot_relation_propagation_weight(slot_name: Optional[str]) -> float:
    """Generic structural prior for propagating bound classes toward plausible source entities."""
    weights = {
        "anchor_text": 4.0,
        "target_text": 1.5,
        "cause_text": 1.25,
        "action_or_state_text": 1.25,
        "status_or_problem_text": 1.0,
    }
    return float(weights.get(slot_name, 0.0))


def slot_relation_propagation_min_score(slot_name: Optional[str]) -> float:
    """Minimum binding score required before a non-anchor slot can propagate structurally."""
    if slot_name == "target_text":
        return 5.0
    return 6.0


def candidate_supports_relation_propagation(
    slot_name: Optional[str],
    candidate: Optional[Dict[str, Any]],
) -> bool:
    """Whether one grounded candidate is specific enough to propagate structurally."""
    if not isinstance(candidate, dict):
        return False
    base_score = float(candidate.get("total_score", 0.0) or 0.0)
    lexical_score = float(candidate.get("lexical_score", 0.0) or 0.0)
    semantic_similarity = float(candidate.get("semantic_similarity", 0.0) or 0.0)
    node_type = candidate.get("node_type")

    if slot_name == "anchor_text":
        return base_score >= 6.0
    if node_type == "class":
        return base_score >= 6.0
    if node_type == "value":
        return lexical_score >= 8.0 or base_score >= 12.0
    if node_type == "attribute":
        if slot_name == "target_text":
            return base_score >= 5.0 and (lexical_score >= 4.0 or semantic_similarity >= 0.22)
        return base_score >= 10.0 and (lexical_score >= 8.0 or semantic_similarity >= 0.28)
    return base_score >= 8.0


def relation_propagated_source_candidates(
    manifest: Optional[Dict[str, Any]],
    slot_bindings: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Propagate bound attribute/value classes across manifest relations to form generic source candidates."""
    if not isinstance(manifest, dict):
        return {}

    classes = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    }
    relations = [
        item for item in manifest.get("relations", [])
        if isinstance(item, dict)
        and isinstance(item.get("source_class"), str)
        and item.get("source_class")
        and isinstance(item.get("target_class"), str)
        and item.get("target_class")
    ]

    propagated: Dict[str, Dict[str, Any]] = {}
    for binding in slot_bindings:
        if not isinstance(binding, dict):
            continue
        slot_name = binding.get("slot_name")
        slot_weight = slot_relation_propagation_weight(slot_name)
        if slot_weight <= 0:
            continue

        for candidate in binding.get("candidates", []):
            if not isinstance(candidate, dict):
                continue
            if candidate.get("node_type") not in {"attribute", "value"}:
                continue
            bound_class = candidate.get("class_name")
            if not isinstance(bound_class, str) or not bound_class:
                continue

            base_score = float(candidate.get("total_score", 0.0) or 0.0)
            if slot_name != "anchor_text" and base_score < slot_relation_propagation_min_score(slot_name):
                continue
            if not candidate_supports_relation_propagation(slot_name, candidate):
                continue

            direct_entry = {
                "class_name": bound_class,
                "label": classes.get(bound_class, bound_class),
                "score": base_score + max(0.5, slot_weight - 0.5),
                "binding_slot": slot_name,
                "binding_source": "bound_class",
            }
            current_direct = propagated.get(bound_class)
            if current_direct is None or direct_entry["score"] > float(current_direct.get("score", 0.0) or 0.0):
                propagated[bound_class] = direct_entry

            for relation in relations:
                source_class = relation.get("source_class")
                target_class = relation.get("target_class")
                relation_bonus = 1.0 if relation.get("validation_source") == "mapping" else 0.0
                if source_class == bound_class and isinstance(target_class, str) and target_class:
                    score = base_score + relation_bonus + slot_weight
                    current = propagated.get(source_class)
                    if current is None or score > float(current.get("score", 0.0) or 0.0):
                        propagated[source_class] = {
                            "class_name": source_class,
                            "label": classes.get(source_class, source_class),
                            "score": score,
                            "binding_slot": slot_name,
                            "binding_source": "relation_source",
                            "via_relation": relation.get("property"),
                            "bound_class": bound_class,
                        }

                if slot_name == "anchor_text" and source_class == bound_class and isinstance(target_class, str) and target_class:
                    score = base_score + relation_bonus - 2.0 + (slot_weight * 0.25)
                    current = propagated.get(target_class)
                    if current is None or score > float(current.get("score", 0.0) or 0.0):
                        propagated[target_class] = {
                            "class_name": target_class,
                            "label": classes.get(target_class, target_class),
                            "score": score,
                            "binding_slot": slot_name,
                            "binding_source": "relation_target",
                            "via_relation": relation.get("property"),
                            "bound_class": bound_class,
                        }
    return propagated


def binding_terms_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    limit: int = 5,
    preferred_node_types: Optional[List[str]] = None,
) -> List[str]:
    """Extract lexical terms from top manifest bindings for a given semantic slot."""
    terms: List[str] = []
    base_minimum = 4.0 if slot_name in {"cause_text", "action_or_state_text"} else 6.0
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        candidates = [item for item in binding.get("candidates", []) if isinstance(item, dict)]
        if not candidates:
            continue
        top_score = max(float(item.get("total_score", 0.0) or 0.0) for item in candidates)
        minimum_score = max(base_minimum, top_score * 0.5)
        filtered_candidates = [
            candidate
            for candidate in candidates
            if float(candidate.get("total_score", 0.0) or 0.0) >= minimum_score
        ]
        if preferred_node_types:
            preferred = [
                candidate
                for candidate in filtered_candidates
                if candidate.get("node_type") in set(preferred_node_types)
            ]
            if preferred:
                filtered_candidates = preferred
        for candidate in filtered_candidates:
            label = candidate.get("label")
            if isinstance(label, str) and label:
                terms.append(label)
            if len(terms) >= limit:
                break
    seen = set()
    deduped: List[str] = []
    for term in terms:
        if not term or term in seen:
            continue
        seen.add(term)
        deduped.append(term)
        if len(deduped) >= limit:
            break
    return deduped


def slot_binding_has_candidates(slot_bindings: List[Dict[str, Any]], slot_name: str) -> bool:
    """Whether a slot binding produced at least one candidate node."""
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        candidates = binding.get("candidates", [])
        if isinstance(candidates, list) and candidates:
            return True
    return False


def slot_binding_candidates(slot_bindings: List[Dict[str, Any]], slot_name: str) -> List[Dict[str, Any]]:
    """Return all candidates produced for a given slot."""
    results: List[Dict[str, Any]] = []
    for binding in slot_bindings:
        if not isinstance(binding, dict) or binding.get("slot_name") != slot_name:
            continue
        for candidate in binding.get("candidates", []):
            if isinstance(candidate, dict):
                results.append(candidate)
    return results


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


def grounding_candidate_sort_key(candidate: Dict[str, Any]) -> tuple[float, float, str]:
    """Return the common sort key for grounded candidates."""
    return (
        -float(candidate.get("total_score", 0.0) or 0.0),
        -float(candidate.get("semantic_similarity", 0.0) or 0.0),
        str(candidate.get("node_id", "")),
    )


def grounded_slot_candidates(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    slot_input: Optional[Dict[str, Any]] = None,
    preferred_node_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return planner candidates that are admissible for grounded lowering of one slot."""
    candidates = slot_binding_candidates(slot_bindings, slot_name)
    if preferred_node_types:
        preferred_set = {item for item in preferred_node_types if isinstance(item, str) and item}
        preferred = [item for item in candidates if item.get("node_type") in preferred_set]
        if preferred:
            candidates = preferred
    if abstract_status_slot_requires_high_confidence(slot_input):
        candidates = [candidate for candidate in candidates if candidate_supports_abstract_status_binding(candidate)]
    candidates.sort(key=grounding_candidate_sort_key)
    return candidates


def slot_candidates_have_text_lowering(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    slot_input: Optional[Dict[str, Any]] = None,
) -> bool:
    """Whether a slot has at least one binding that can be lowered as a text-style filter."""
    abstract_status_check = abstract_status_slot_requires_high_confidence(slot_input)
    for candidate in grounded_slot_candidates(slot_bindings, slot_name, slot_input=slot_input):
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


def slot_input_for_name(slot_inputs: List[Dict[str, Any]], slot_name: str) -> Optional[Dict[str, Any]]:
    """Return the slot input definition for a given slot name."""
    for slot_input in slot_inputs:
        if isinstance(slot_input, dict) and slot_input.get("slot_name") == slot_name:
            return slot_input
    return None


def top_attribute_candidate_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    class_name: Optional[str] = None,
    numeric_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring attribute candidate for a slot, optionally scoped by class."""
    candidates = []
    for candidate in slot_binding_candidates(slot_bindings, slot_name):
        if candidate.get("node_type") != "attribute":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        if numeric_only and not bool(candidate.get("numeric")):
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    candidates.sort(key=grounding_candidate_sort_key)
    return candidates[0]


def top_value_candidate_for_slot(
    slot_bindings: List[Dict[str, Any]],
    slot_name: str,
    class_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the highest-scoring value candidate for a slot, optionally scoped by class."""
    candidates = []
    for candidate in slot_binding_candidates(slot_bindings, slot_name):
        if candidate.get("node_type") != "value":
            continue
        if class_name and candidate.get("class_name") != class_name:
            continue
        candidates.append(candidate)
    if not candidates:
        return None
    candidates.sort(key=grounding_candidate_sort_key)
    return candidates[0]
