#!/usr/bin/env python3
"""Repo-owned runtime for semantic planner compilation."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Set

from obda_grounding_contracts import (
    build_grounding_bundle,
    grounding_constraint_effective_text,
    grounding_constraint_record,
    grounding_slot_binding_has_candidates,
    grounding_slot_bindings,
    grounding_slot_candidates_have_text_lowering,
    grounding_slot_input_for_name,
    grounding_top_attribute_candidate_for_slot,
)
from obda_ir_contracts import request_ir_output_record
from obda_planner_compiler import (
    build_node_plan as compiler_build_node_plan,
    build_semantic_request_ir as compiler_build_semantic_request_ir,
    first_nonempty_text,
    select_compiled_plan as compiler_select_compiled_plan,
)


def slot_supports_strict_text_filter(grounded_slot: Optional[Dict[str, Any]]) -> bool:
    """Only treat grounded text/value slots as strict filters when the lowering signal is strong enough."""
    if not isinstance(grounded_slot, dict):
        return False
    top_candidate = grounded_slot.get("top_candidate")
    if not isinstance(top_candidate, dict):
        binding_terms = grounded_slot.get("binding_terms")
        return isinstance(binding_terms, list) and any(isinstance(item, str) and item.strip() for item in binding_terms)
    if top_candidate.get("node_type") == "attribute" and not bool(top_candidate.get("numeric")):
        return True
    binding_terms = grounded_slot.get("binding_terms")

    if top_candidate.get("node_type") != "value":
        if isinstance(binding_terms, list) and any(isinstance(item, str) and item.strip() for item in binding_terms):
            return True
        return False

    catalog_source = top_candidate.get("catalog_source")
    total_score = float(top_candidate.get("total_score", 0.0) or 0.0)
    lexical_score = float(top_candidate.get("lexical_score", 0.0) or 0.0)
    semantic_similarity = float(top_candidate.get("semantic_similarity", 0.0) or 0.0)

    if catalog_source == "sample_value":
        return total_score >= 8.0 and (lexical_score >= 4.0 or semantic_similarity >= 0.2)
    if isinstance(binding_terms, list) and any(isinstance(item, str) and item.strip() for item in binding_terms):
        return True
    return total_score >= 4.0


def build_semantic_query_planner(
    question: str,
    template: str,
    schema: Optional[Dict[str, Any]],
    *,
    base_url: Optional[str] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
    runtime: Dict[str, Callable[..., Any]],
) -> Optional[Dict[str, Any]]:
    """Build a lightweight semantic query plan suggestion from question + schema."""
    if template not in ("fact_lookup", "causal_lookup", "causal_enumeration", "enumeration"):
        return None

    with_semantic_vector_index = runtime["with_semantic_vector_index"]
    build_semantic_manifest = runtime["build_semantic_manifest"]
    extract_question_slots = runtime["extract_question_slots"]
    build_question_unit_intent_ir = runtime["build_question_unit_intent_ir"]
    semantic_state_from_sources = runtime["semantic_state_from_sources"]
    route_query_family = runtime["route_query_family"]
    build_family_slot_inputs = runtime["build_family_slot_inputs"]
    choose_source_class_candidate_with_anchors = runtime["choose_source_class_candidate_with_anchors"]
    bind_semantic_slots = runtime["bind_semantic_slots"]
    merge_source_candidates_from_slot_bindings = runtime["merge_source_candidates_from_slot_bindings"]
    choose_evidence_class_candidates = runtime["choose_evidence_class_candidates"]
    rank_value_catalog_classes = runtime["rank_value_catalog_classes"]
    slot_inputs_need_value_catalog = runtime["slot_inputs_need_value_catalog"]
    load_sample_value_nodes = runtime["load_sample_value_nodes"]
    with_value_nodes = runtime["with_value_nodes"]
    build_grounded_constraint_view = runtime["build_grounded_constraint_view"]
    build_explicit_metric_clarification_hint = runtime["build_explicit_metric_clarification_hint"]
    manifest_attributes_by_class = runtime["manifest_attributes_by_class"]
    schema_indexes = runtime["schema_indexes"]
    unique_preserve_order = runtime["unique_preserve_order"]
    best_role_property = runtime["best_role_property"]
    resolve_builder_link_direction = runtime["resolve_builder_link_direction"]
    mark_optional_display_selects = runtime["mark_optional_display_selects"]
    grounded_constraint_terms = runtime["grounded_constraint_terms"]
    build_constraint_filter = runtime["build_constraint_filter"]
    choose_action_support_classes = runtime["choose_action_support_classes"]
    choose_enumeration_value_projection = runtime["choose_enumeration_value_projection"]
    build_value_enumeration_query = runtime["build_value_enumeration_query"]
    selected_anchor_binding_for_class = runtime["selected_anchor_binding_for_class"]
    build_multi_evidence_relaxed_query = runtime["build_multi_evidence_relaxed_query"]

    manifest = with_semantic_vector_index(build_semantic_manifest(schema))
    slots = dict(slots_override) if isinstance(slots_override, dict) else extract_question_slots(question, template)
    effective_unit_intent_ir = unit_intent_ir
    if not isinstance(effective_unit_intent_ir, dict):
        synthetic_unit = {
            "unit_id": "q1",
            "text": question,
            "raw_text": question,
            "position": 1,
            "reference_markers": list(slots.get("reference_markers", []))
            if isinstance(slots.get("reference_markers"), list)
            else [],
            "dependency": None,
        }
        effective_unit_intent_ir = build_question_unit_intent_ir(synthetic_unit, slots, template)
    semantic_state = semantic_state_from_sources(slots, effective_unit_intent_ir)
    asks_explanation = bool(semantic_state.get("asks_explanation"))
    target_text = first_nonempty_text(
        semantic_state.get("target_text"),
        semantic_state.get("result_hint"),
    )
    routing = route_query_family(question, template, slots, unit_intent_ir=effective_unit_intent_ir)
    slots["question_type"] = routing["family"]
    slot_inputs = build_family_slot_inputs(question, slots, routing, unit_intent_ir=effective_unit_intent_ir)
    source_info = choose_source_class_candidate_with_anchors(
        question,
        manifest,
        slots,
        unit_intent_ir=effective_unit_intent_ir,
    )
    initial_slot_bindings = bind_semantic_slots(manifest, slot_inputs)
    source_info = merge_source_candidates_from_slot_bindings(source_info, initial_slot_bindings, manifest)
    provisional_source = source_info.get("selected")
    provisional_source_class = provisional_source.get("class_name") if isinstance(provisional_source, dict) else None
    provisional_evidence_candidates = (
        choose_evidence_class_candidates(
            provisional_source_class,
            manifest,
            slots,
            grounding_bundle=None,
        )
        if isinstance(provisional_source_class, str) and provisional_source_class
        else []
    )
    value_class_names = rank_value_catalog_classes(
        source_info,
        provisional_evidence_candidates,
        initial_slot_bindings,
    )
    sampled_value_nodes: List[Dict[str, Any]] = []
    if slot_inputs_need_value_catalog(slot_inputs):
        sampled_value_nodes = load_sample_value_nodes(base_url, manifest, value_class_names)
        manifest = with_semantic_vector_index(with_value_nodes(manifest, sampled_value_nodes))
    slot_bindings = bind_semantic_slots(manifest, slot_inputs)
    grounded_constraints = build_grounded_constraint_view(slot_inputs, slot_bindings, semantic_state)
    grounding_bundle = build_grounding_bundle(
        slot_inputs=slot_inputs,
        slot_bindings=slot_bindings,
        grounded_constraints=grounded_constraints,
    )
    source_info = choose_source_class_candidate_with_anchors(
        question,
        manifest,
        slots,
        unit_intent_ir=effective_unit_intent_ir,
    )
    source_info = merge_source_candidates_from_slot_bindings(source_info, slot_bindings, manifest)
    request_ir = compiler_build_semantic_request_ir(
        question,
        template,
        routing,
        grounding_bundle,
        source_info,
        [],
        semantic_state,
        anchors=slots.get("anchors", []),
        unit_intent_ir=effective_unit_intent_ir,
    )
    missing_constraint_bindings: List[str] = []
    non_lowerable_constraint_bindings: List[str] = []
    status_slot_input = grounding_slot_input_for_name(grounding_bundle, "status_or_problem_text")
    status_check_mode = (
        isinstance(status_slot_input, dict)
        and status_slot_input.get("constraint_mode") == "status_check"
    )
    status_numeric_constraint = semantic_state.get("status_numeric_constraint")
    source_selected = source_info.get("selected")
    if not source_selected:
        clarification_hint = None
        if (
            status_check_mode
            and not isinstance(status_numeric_constraint, dict)
            and (
                "status_or_problem_text" in missing_constraint_bindings
                or "status_or_problem_text" in non_lowerable_constraint_bindings
            )
        ):
            clarification_hint = build_explicit_metric_clarification_hint(semantic_state)
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": routing["effective_template"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": [],
            "evidence_candidates": [],
            "candidate_plans": [],
            "ready": False,
            "reason": "no_source_class_grounding",
            "clarification_hint": clarification_hint,
        }

    effective_template = routing["effective_template"]
    preview_source_class = source_selected["class_name"]
    preview_evidence_candidates = choose_evidence_class_candidates(
        preview_source_class,
        manifest,
        slots,
        unit_intent_ir=effective_unit_intent_ir,
        grounding_bundle=grounding_bundle,
    )
    request_ir = compiler_build_semantic_request_ir(
        question,
        template,
        routing,
        grounding_bundle,
        source_info,
        preview_evidence_candidates,
        semantic_state,
        anchors=slots.get("anchors", []),
        unit_intent_ir=effective_unit_intent_ir,
    )
    target_text = first_nonempty_text(
        grounding_constraint_effective_text(grounding_bundle, "target_text"),
        target_text,
    )
    domain_properties = manifest_attributes_by_class(manifest)
    indexes = schema_indexes(schema)
    class_labels = {
        item["class_name"]: item.get("label") or item["class_name"]
        for item in manifest.get("classes", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    }

    clarification_hint = None
    misrouted_anchored_status_lookup = (
        bool(semantic_state.get("has_anchor"))
        and bool(semantic_state.get("status_check_requested"))
        and routing.get("family") in ("enumeration", "causal_enumeration", "explanation_enumeration")
    )
    if misrouted_anchored_status_lookup:
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": effective_template,
            "routing_rationale": routing["rationale"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": source_info.get("candidates", []),
            "evidence_candidates": preview_evidence_candidates,
            "candidate_plans": [],
            "selected_plan": None,
            "ready": False,
            "reason": "anchored_status_lookup_requires_smaller_family",
        }
    if routing.get("family", "").startswith("anchored_"):
        slot_requirements = [
            ("status_or_problem_text", semantic_state.get("status_or_problem_text")),
            ("cause_text", semantic_state.get("cause_text")),
            ("action_or_state_text", semantic_state.get("action_text")),
        ]
        for slot_name, slot_value in slot_requirements:
            if not isinstance(slot_value, str) or not slot_value.strip():
                continue
            if not grounding_slot_binding_has_candidates(grounding_bundle, slot_name):
                missing_constraint_bindings.append(slot_name)
            elif (
                slot_name == "status_or_problem_text"
                and status_check_mode
                and not isinstance(status_numeric_constraint, dict)
                and not grounding_slot_candidates_have_text_lowering(grounding_bundle, slot_name)
            ):
                non_lowerable_constraint_bindings.append(slot_name)

    if missing_constraint_bindings or non_lowerable_constraint_bindings:
        if (
            status_check_mode
            and not isinstance(status_numeric_constraint, dict)
            and (
                "status_or_problem_text" in missing_constraint_bindings
                or "status_or_problem_text" in non_lowerable_constraint_bindings
            )
        ):
            clarification_hint = build_explicit_metric_clarification_hint(semantic_state)
        return {
            "mode": "semantic_query_planner",
            "slots": slots,
            "request_ir": request_ir,
            "query_family": routing["family"],
            "requested_template": template,
            "effective_template": effective_template,
            "routing_rationale": routing["rationale"],
            "semantic_manifest_summary": {
                "class_count": len(manifest.get("classes", [])),
                "relation_count": len(manifest.get("relations", [])),
                "class_node_count": len(manifest.get("class_nodes", [])),
                "attribute_node_count": len(manifest.get("attribute_nodes", [])),
                "relation_node_count": len(manifest.get("relation_nodes", [])),
                "value_node_count": len(manifest.get("value_nodes", [])),
            },
            "source_candidates": source_info.get("candidates", []),
            "evidence_candidates": preview_evidence_candidates,
            "candidate_plans": [],
            "selected_plan": None,
            "ready": False,
            "reason": "constraint_grounding_not_executable",
            "missing_constraint_bindings": missing_constraint_bindings,
            "non_lowerable_constraint_bindings": non_lowerable_constraint_bindings,
            "clarification_hint": clarification_hint,
        }

    anchor_value = None
    anchors = semantic_state.get("anchors", [])
    if isinstance(anchors, list):
        for anchor in anchors:
            if isinstance(anchor, dict) and isinstance(anchor.get("value"), str) and anchor.get("value"):
                anchor_value = anchor.get("value")
                break
    reference_entity_uris = unique_preserve_order([
        str(item)
        for item in slots.get("reference_entity_uris", [])
        if isinstance(item, str) and item
    ])
    reference_entity_class = slots.get("reference_entity_class")

    candidate_plans = []
    all_evidence_candidates: List[Dict[str, Any]] = []
    seen_evidence_classes: Set[str] = set()
    explanation_action_grounding = grounding_constraint_record(grounding_bundle, "action_or_state_text")
    explanation_action_terms = grounded_constraint_terms(
        explanation_action_grounding,
        schema,
    )
    source_candidates = [
        item
        for item in source_info.get("candidates", [])
        if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
    ][:5]

    for source_candidate in source_candidates:
        source_class = source_candidate["class_name"]
        if isinstance(reference_entity_class, str) and reference_entity_class and source_class != reference_entity_class:
            continue
        source_score = float(source_candidate.get("score", 0.0) or 0.0)
        if source_class == preview_source_class:
            source_score += 5.0
        source_id_prop = best_role_property(
            source_class,
            "id",
            domain_properties,
            class_labels,
            manifest=manifest,
        )
        source_name_prop = best_role_property(
            source_class,
            "name",
            domain_properties,
            class_labels,
            manifest=manifest,
        )
        evidence_candidates = choose_evidence_class_candidates(
            source_class,
            manifest,
            slots,
            unit_intent_ir=effective_unit_intent_ir,
            grounding_bundle=grounding_bundle,
        )
        for evidence in evidence_candidates:
            evidence_class_name = evidence.get("class_name")
            if isinstance(evidence_class_name, str) and evidence_class_name and evidence_class_name not in seen_evidence_classes:
                seen_evidence_classes.add(evidence_class_name)
                all_evidence_candidates.append(evidence)

        for evidence in evidence_candidates[:3]:
            evidence_class = evidence["class_name"]
            evidence_score = float(evidence.get("score", 0.0) or 0.0)
            for constraint_key in ("cause_text", "action_or_state_text", "status_or_problem_text", "target_text"):
                grounded = grounding_constraint_record(grounding_bundle, constraint_key)
                if not isinstance(grounded, dict):
                    continue
                top_candidate = grounded.get("top_candidate")
                if (
                    isinstance(top_candidate, dict)
                    and isinstance(top_candidate.get("class_name"), str)
                    and top_candidate.get("class_name") == evidence_class
                ):
                    evidence_score += 2.5
            if (
                routing.get("family") == "explanation_enumeration"
                and effective_template == "enumeration"
                and explanation_action_terms
                and isinstance(explanation_action_grounding, dict)
            ):
                top_candidate = explanation_action_grounding.get("top_candidate")
                if (
                    isinstance(top_candidate, dict)
                    and isinstance(top_candidate.get("class_name"), str)
                    and top_candidate.get("class_name") == evidence_class
                ):
                    evidence_score += 3.0
            evidence_id_prop = best_role_property(
                evidence_class,
                "id",
                domain_properties,
                class_labels,
                manifest=manifest,
            )
            evidence_type_prop = best_role_property(
                evidence_class,
                "type",
                domain_properties,
                class_labels,
                manifest=manifest,
            )
            evidence_desc_prop = best_role_property(
                evidence_class,
                "description",
                domain_properties,
                class_labels,
                manifest=manifest,
            )
            try:
                relation_info = resolve_builder_link_direction(source_class, evidence_class, None, indexes)
            except SystemExit:
                relation_info = evidence.get("relations", [None])[0] if isinstance(evidence.get("relations"), list) and evidence.get("relations") else None

            select_specs = []
            if source_id_prop:
                select_specs.append({"var": "sourceId", "subject": "source", "property": source_id_prop})
            if source_name_prop:
                select_specs.append({"var": "sourceName", "subject": "source", "property": source_name_prop})
            if evidence_id_prop:
                select_specs.append({"var": "evidenceId", "subject": "evidence", "property": evidence_id_prop})
            if evidence_type_prop:
                select_specs.append({"var": "evidenceType", "subject": "evidence", "property": evidence_type_prop})
            if evidence_desc_prop:
                select_specs.append({"var": "evidenceDescription", "subject": "evidence", "property": evidence_desc_prop})

            text_vars = []
            if evidence_type_prop:
                text_vars.append("evidenceType")
            if evidence_desc_prop:
                text_vars.append("evidenceDescription")

            status_terms = grounded_constraint_terms(
                grounding_constraint_record(grounding_bundle, "status_or_problem_text"),
                schema,
            )
            cause_grounding = grounding_constraint_record(grounding_bundle, "cause_text")
            cause_terms = grounded_constraint_terms(
                cause_grounding,
                schema,
            )
            if not cause_terms and not (status_check_mode and isinstance(status_numeric_constraint, dict)):
                cause_terms = list(status_terms)
            action_grounding = grounding_constraint_record(grounding_bundle, "action_or_state_text")
            action_terms = grounded_constraint_terms(
                action_grounding,
                schema,
            )
            action_slot_input = grounding_slot_input_for_name(grounding_bundle, "action_or_state_text")
            action_slot_text = (
                action_slot_input.get("text")
                if isinstance(action_slot_input, dict) and isinstance(action_slot_input.get("text"), str)
                else None
            )
            cause_filter = build_constraint_filter(text_vars, cause_terms)
            strict_action_terms = action_terms if slot_supports_strict_text_filter(action_grounding) else []
            action_filter = build_constraint_filter(text_vars, strict_action_terms)
            support_slot_requested = action_slot_input is not None
            support_evidence_classes = [
                item.get("class_name")
                for item in evidence_candidates
                if isinstance(item, dict) and isinstance(item.get("class_name"), str) and item.get("class_name")
            ]

            base_builder = {
                "source_class": source_class,
                "source_var": "source",
                "evidence_class": evidence_class,
                "evidence_var": "evidence",
            }
            if relation_info and relation_info.get("property"):
                base_builder["link_property"] = relation_info["property"]
                base_builder["direction"] = relation_info.get("direction", "forward")
                base_builder["validation_source"] = relation_info.get("validation_source")

            base_filters: List[Dict[str, Any]] = []
            base_order_by = ["sourceId", "evidenceId"]
            reference_scope_applied = bool(reference_entity_uris)
            if reference_scope_applied:
                base_filters.append({
                    "var": "source",
                    "op": "in",
                    "values": reference_entity_uris,
                })
            reference_rationale = ["reference_scope_bound"] if reference_scope_applied else []
            reference_bonus = 1.0 if reference_scope_applied else 0.0
            anchor_binding_candidates = []
            if isinstance(anchor_value, str) and anchor_value:
                source_anchor_binding = selected_anchor_binding_for_class(
                    grounding_slot_bindings(grounding_bundle),
                    source_class,
                )
                if isinstance(source_anchor_binding, dict):
                    anchor_binding_candidates.append({
                        "subject": "source",
                        "var": "anchorMatch",
                        "binding": source_anchor_binding,
                    })
                evidence_anchor_binding = selected_anchor_binding_for_class(
                    grounding_slot_bindings(grounding_bundle),
                    evidence_class,
                )
                if isinstance(evidence_anchor_binding, dict):
                    anchor_binding_candidates.append({
                        "subject": "evidence",
                        "var": "anchorMatch",
                        "binding": evidence_anchor_binding,
                    })
            best_anchor_candidate = None
            if anchor_binding_candidates:
                anchor_binding_candidates.sort(
                    key=lambda item: (
                        -float(item["binding"].get("total_score", 0.0) or 0.0),
                        item["subject"],
                        str(item["binding"].get("local_name", "")),
                    )
                )
                best_anchor_candidate = anchor_binding_candidates[0]
                if not any(item.get("var") == "anchorMatch" for item in select_specs):
                    select_specs.append({
                        "var": "anchorMatch",
                        "subject": best_anchor_candidate["subject"],
                        "property": best_anchor_candidate["binding"]["local_name"],
                    })
                base_filters.append({
                    "var": "anchorMatch",
                    "op": "equals",
                    "value": anchor_value,
                })

            status_metric_binding = None
            if status_check_mode and isinstance(status_numeric_constraint, dict):
                status_metric_binding = grounding_top_attribute_candidate_for_slot(
                    grounding_bundle,
                    "status_or_problem_text",
                    class_name=evidence_class,
                    numeric_only=True,
                )

            if effective_template == "enumeration":
                explanation_family = routing.get("family") == "explanation_enumeration"
                generic_explanation_target = False
                if support_slot_requested and action_terms:
                    support_evidence_classes = choose_action_support_classes(
                        evidence_candidates,
                        action_terms,
                        domain_properties,
                        slot_text=action_slot_text,
                        limit=2 if explanation_family else 5,
                    ) or support_evidence_classes
                value_projection = choose_enumeration_value_projection(
                    grounding_bundle,
                    evidence_class,
                    manifest,
                    domain_properties,
                    class_labels,
                    prefer_explanation=asks_explanation,
                    allow_generic_explanation_projection=generic_explanation_target,
                )
                if value_projection is None:
                    continue

                projection_confidence = 2.0 if "target_slot_grounded" in value_projection.get("rationale", []) else 1.0
                support_terms = list(action_terms)
                if support_slot_requested and not support_terms:
                    continue
                explanation_bonus = 2.0 if asks_explanation else 0.0

                enumeration_variants: List[tuple[str, Optional[Dict[str, Any]], List[str], float, bool]] = []
                if support_terms:
                    weak_target_projection = "target_role_fallback" in value_projection.get("rationale", [])
                    broad_support_penalty = 4.0 if asks_explanation and weak_target_projection else 0.0
                    same_evidence_confidence = (
                        evidence_score + source_score + projection_confidence + 2.0 + reference_bonus
                    )
                    source_support_confidence = (
                        evidence_score + source_score + projection_confidence + 1.0 + explanation_bonus + reference_bonus - broad_support_penalty
                    )
                    if explanation_family:
                        same_evidence_confidence -= 2.0
                        source_support_confidence += 3.0
                    enumeration_variants.append((
                        "value_enumeration_same_evidence",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            support_terms,
                            support_mode="same_evidence",
                            source_uri_values=reference_entity_uris,
                        ),
                        ["target_value_projected", "action_term_grounded"],
                        same_evidence_confidence,
                        False,
                    ))
                    enumeration_variants.append((
                        "value_enumeration_source_support",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            support_terms,
                            support_mode="source_support",
                            support_slot_text=action_slot_text,
                            support_classes=support_evidence_classes,
                            source_uri_values=reference_entity_uris,
                        ),
                        ["target_value_projected", "action_term_grounded_separate_evidence"],
                        source_support_confidence,
                        True,
                    ))
                else:
                    enumeration_variants.append((
                        "value_enumeration",
                        build_value_enumeration_query(
                            schema,
                            source_class,
                            evidence_class,
                            relation_info,
                            value_projection["value_property"],
                            value_projection.get("description_property"),
                            [],
                            support_mode="none",
                            source_uri_values=reference_entity_uris,
                        ),
                        value_projection.get("rationale", []),
                        evidence_score + source_score + projection_confidence + reference_bonus,
                        False,
                    ))

                for variant, enumeration_query, rationale, confidence, separate_action_support in enumeration_variants:
                    if enumeration_query is None:
                        continue
                    if best_anchor_candidate is not None:
                        rationale = list(rationale) + ["anchor_bound"]
                        confidence += 2.0
                    candidate_plans.append({
                        "variant": variant,
                        "confidence_score": confidence,
                        "rationale": reference_rationale + list(rationale),
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": compiler_build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=bool(support_terms),
                            include_status=False,
                            separate_action_support=separate_action_support,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": enumeration_query,
                        },
                    })
                continue

            if effective_template == "fact_lookup":
                if isinstance(status_metric_binding, dict):
                    numeric_select_specs = list(select_specs)
                    if not any(item.get("var") == "statusMetric" for item in numeric_select_specs):
                        numeric_select_specs.append({
                            "var": "statusMetric",
                            "subject": "evidence",
                            "property": status_metric_binding["local_name"],
                        })
                    numeric_filters = list(base_filters) + [{
                        "var": "statusMetric",
                        "op": status_numeric_constraint["op"],
                        "value": status_numeric_constraint["value"],
                    }]
                    rationale = ["status_constraint_grounded", "numeric_constraint_lowered"]
                    confidence = (
                        evidence_score
                        + source_score
                        + float(status_metric_binding.get("total_score", 0.0) or 0.0)
                        + 2
                        + reference_bonus
                    )
                    if best_anchor_candidate is not None:
                        rationale.append("anchor_bound")
                        confidence += 2
                    candidate_plans.append({
                        "variant": "anchored_fact_status_check_numeric",
                        "confidence_score": confidence,
                        "rationale": reference_rationale + rationale,
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": compiler_build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=False,
                            include_status=True,
                            separate_action_support=False,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": {
                                "source_var": "source",
                                "builder": {
                                    **base_builder,
                                    "select": mark_optional_display_selects(
                                        numeric_select_specs,
                                        numeric_filters,
                                        base_order_by,
                                    ),
                                    "filters": numeric_filters,
                                    "order_by": base_order_by,
                                },
                            },
                        },
                    })
                    continue
                target_attribute_binding = grounding_top_attribute_candidate_for_slot(
                    grounding_bundle,
                    "target_text",
                    class_name=evidence_class,
                    numeric_only=False,
                )
                if isinstance(target_attribute_binding, dict):
                    fact_select_specs = list(select_specs)
                    if not any(item.get("var") == "evidenceTargetValue" for item in fact_select_specs):
                        fact_select_specs.append({
                            "var": "evidenceTargetValue",
                            "subject": "evidence",
                            "property": target_attribute_binding["local_name"],
                        })
                    rationale = ["target_slot_grounded", "fact_target_projected"]
                    confidence = (
                        evidence_score
                        + source_score
                        + float(target_attribute_binding.get("total_score", 0.0) or 0.0)
                        + reference_bonus
                    )
                    if best_anchor_candidate is not None:
                        rationale.append("anchor_bound")
                        confidence += 2.0
                    candidate_plans.append({
                        "variant": "anchored_fact_target_projection",
                        "confidence_score": confidence,
                        "rationale": reference_rationale + rationale,
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": compiler_build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=False,
                            include_action=False,
                            include_status=False,
                            separate_action_support=False,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": {
                                "source_var": "source",
                                "builder": {
                                    **base_builder,
                                    "select": mark_optional_display_selects(
                                        fact_select_specs,
                                        base_filters,
                                        base_order_by,
                                    ),
                                    "filters": base_filters,
                                    "order_by": base_order_by,
                                },
                            },
                        },
                    })
                continue

            if isinstance(status_metric_binding, dict):
                numeric_select_specs = list(select_specs)
                if not any(item.get("var") == "statusMetric" for item in numeric_select_specs):
                    numeric_select_specs.append({
                        "var": "statusMetric",
                        "subject": "evidence",
                        "property": status_metric_binding["local_name"],
                    })
                numeric_filters = list(base_filters) + [{
                    "var": "statusMetric",
                    "op": status_numeric_constraint["op"],
                    "value": status_numeric_constraint["value"],
                }]
                rationale = ["status_constraint_grounded", "numeric_constraint_lowered"]
                confidence = (
                    evidence_score
                    + source_score
                    + float(status_metric_binding.get("total_score", 0.0) or 0.0)
                    + 2
                    + reference_bonus
                )
                if best_anchor_candidate is not None:
                    rationale.append("anchor_bound")
                    confidence += 2
                candidate_plans.append({
                    "variant": "status_check_numeric",
                    "confidence_score": confidence,
                    "rationale": reference_rationale + rationale,
                    "query_family": routing["family"],
                    "source_class": source_class,
                    "evidence_class": evidence_class,
                    "node_plan": compiler_build_node_plan(
                        request_ir,
                        source_class,
                        evidence_class,
                        relation_info,
                        include_cause=False,
                        include_action=False,
                        include_status=True,
                        separate_action_support=False,
                    ),
                    "plan": {
                        "template": effective_template,
                        "sparql": {
                            "source_var": "source",
                            "builder": {
                                **base_builder,
                                "select": mark_optional_display_selects(
                                    numeric_select_specs,
                                    numeric_filters,
                                    base_order_by,
                                ),
                                "filters": numeric_filters,
                                "order_by": base_order_by,
                            },
                        },
                        "analysis": {
                            "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                            "payload": {
                                "mode": "paths",
                                "profile": "causal",
                                "max_depth": 3,
                            },
                        },
                    },
                })

            if cause_filter is not None:
                strict_filters = list(base_filters) + [cause_filter]
                rationale = ["cause_term_grounded"]
                confidence = evidence_score + source_score + 3 + reference_bonus
                if best_anchor_candidate is not None:
                    rationale.append("anchor_bound")
                    confidence += 2
                variant = "cause_only"
                if action_filter is not None:
                    strict_filters.append(action_filter)
                    rationale.append("action_term_grounded")
                    confidence += 3
                    variant = "same_evidence_strict"
                candidate_plans.append({
                    "variant": variant,
                    "confidence_score": confidence,
                    "rationale": reference_rationale + rationale,
                    "query_family": routing["family"],
                    "source_class": source_class,
                    "evidence_class": evidence_class,
                    "node_plan": compiler_build_node_plan(
                        request_ir,
                        source_class,
                        evidence_class,
                        relation_info,
                        include_cause=True,
                        include_action=action_filter is not None,
                        separate_action_support=False,
                    ),
                    "plan": {
                        "template": effective_template,
                        "sparql": {
                            "source_var": "source",
                            "builder": {
                                **base_builder,
                                "select": mark_optional_display_selects(
                                    select_specs,
                                    strict_filters,
                                    base_order_by,
                                ),
                                "filters": strict_filters,
                                "order_by": base_order_by,
                            },
                        },
                        "analysis": {
                            "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                            "payload": {
                                "mode": "paths",
                                "profile": "causal",
                                "max_depth": 3,
                            },
                        },
                    },
                })

            if cause_filter is not None and action_filter is not None:
                multi_evidence_query = build_multi_evidence_relaxed_query(
                    schema,
                    source_class,
                    evidence_class,
                    action_terms,
                    cause_terms,
                    source_id_prop,
                    source_name_prop,
                    evidence_id_prop,
                    evidence_type_prop,
                    evidence_desc_prop,
                    source_uri_values=reference_entity_uris,
                )
                if multi_evidence_query is not None and best_anchor_candidate is None:
                    candidate_plans.append({
                        "variant": "source_support_relaxed",
                        "confidence_score": evidence_score + source_score + 2 + reference_bonus,
                        "rationale": reference_rationale + ["cause_term_grounded", "action_term_grounded_separate_evidence"],
                        "query_family": routing["family"],
                        "source_class": source_class,
                        "evidence_class": evidence_class,
                        "node_plan": compiler_build_node_plan(
                            request_ir,
                            source_class,
                            evidence_class,
                            relation_info,
                            include_cause=True,
                            include_action=True,
                            separate_action_support=True,
                        ),
                        "plan": {
                            "template": effective_template,
                            "sparql": multi_evidence_query,
                            "analysis": {
                                "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                                "payload": {
                                    "mode": "paths",
                                    "profile": "causal",
                                    "max_depth": 3,
                                },
                            },
                        },
                    })

                candidate_plans.append({
                    "variant": "cause_only_relaxed",
                    "confidence_score": evidence_score + source_score + reference_bonus + (2 if best_anchor_candidate is not None else 0),
                    "rationale": reference_rationale + ["cause_term_grounded", "action_term_relaxed"] + (["anchor_bound"] if best_anchor_candidate is not None else []),
                    "query_family": routing["family"],
                    "source_class": source_class,
                    "evidence_class": evidence_class,
                    "node_plan": compiler_build_node_plan(
                        request_ir,
                        source_class,
                        evidence_class,
                        relation_info,
                        include_cause=True,
                        include_action=False,
                        separate_action_support=False,
                    ),
                    "plan": {
                        "template": effective_template,
                        "sparql": {
                            "source_var": "source",
                            "builder": {
                                **base_builder,
                                "select": mark_optional_display_selects(
                                    select_specs,
                                    list(base_filters) + [cause_filter],
                                    base_order_by,
                                ),
                                "filters": list(base_filters) + [cause_filter],
                                "order_by": base_order_by,
                            },
                        },
                        "analysis": {
                            "kind": "paths-batch" if effective_template == "causal_enumeration" else "paths",
                            "payload": {
                                "mode": "paths",
                                "profile": "causal",
                                "max_depth": 3,
                            },
                        },
                    },
                })

    request_output = request_ir_output_record(request_ir)
    target_projection_requested = bool(request_output.get("target_projection_requested"))
    include_terminal_details = (
        effective_template == "causal_lookup"
        and (
            bool(request_output.get("asks_solution"))
            or target_projection_requested
        )
    )
    if include_terminal_details:
        for candidate in candidate_plans:
            if not isinstance(candidate, dict):
                continue
            plan = candidate.get("plan")
            if not isinstance(plan, dict):
                continue
            enrichment = dict(plan.get("response_enrichment", {}))
            enrichment.update({
                "include_terminal_entity_details": True,
                "target_projection_requested": target_projection_requested,
            })
            if request_output.get("asks_solution"):
                enrichment["asks_solution"] = True
            plan["response_enrichment"] = enrichment

    candidate_plans.sort(key=lambda item: (-item["confidence_score"], item["variant"]))
    target_grounding = grounding_constraint_record(grounding_bundle, "target_text")
    evidence_candidates = all_evidence_candidates or preview_evidence_candidates
    if candidate_plans:
        selected_source_class = candidate_plans[0].get("source_class")
        selected_source = next(
            (
                item for item in source_info.get("candidates", [])
                if isinstance(item, dict) and item.get("class_name") == selected_source_class
            ),
            source_selected,
        )
        selected_source_info = {
            "selected": selected_source,
            "candidates": source_info.get("candidates", []),
        }
        evidence_candidates = choose_evidence_class_candidates(
            selected_source_class,
            manifest,
            slots,
            unit_intent_ir=effective_unit_intent_ir,
            grounding_bundle=grounding_bundle,
        )
        request_ir = compiler_build_semantic_request_ir(
            question,
            template,
            routing,
            grounding_bundle,
            selected_source_info,
            evidence_candidates,
            semantic_state,
            anchors=slots.get("anchors", []),
            unit_intent_ir=effective_unit_intent_ir,
        )
    planner_selection = compiler_select_compiled_plan(
        candidate_plans,
        request_ir,
        semantic_state,
        target_grounding,
        effective_template,
    )
    selected_plan = planner_selection.get("selected_plan")
    ready = bool(planner_selection.get("ready"))

    return {
        "mode": "semantic_query_planner",
        "slots": slots,
        "request_ir": request_ir,
        "query_family": routing["family"],
        "requested_template": template,
        "effective_template": effective_template,
        "routing_rationale": routing["rationale"],
        "semantic_manifest_summary": {
            "class_count": len(manifest.get("classes", [])),
            "relation_count": len(manifest.get("relations", [])),
            "class_node_count": len(manifest.get("class_nodes", [])),
            "attribute_node_count": len(manifest.get("attribute_nodes", [])),
            "relation_node_count": len(manifest.get("relation_nodes", [])),
            "value_node_count": len(manifest.get("value_nodes", [])),
        },
        "source_candidates": source_info.get("candidates", []),
        "evidence_candidates": evidence_candidates,
        "candidate_plans": candidate_plans,
        "selected_plan": selected_plan,
        "ready": ready,
        "reason": planner_selection.get("reason"),
    }
