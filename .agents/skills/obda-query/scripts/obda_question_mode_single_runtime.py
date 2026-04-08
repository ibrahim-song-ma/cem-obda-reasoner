#!/usr/bin/env python3
"""Repo-owned runtime support for single question-mode planning/execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional


def build_single_question_mode_run_response(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    *,
    run_templates: Dict[str, Dict[str, Any]],
    request_schema: Callable[[str], Dict[str, Any]],
    request_profiles: Callable[[str], Dict[str, Any]],
    write_schema_state: Callable[[Path, str], None],
    build_semantic_query_planner: Callable[..., Dict[str, Any]],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    summarize_profiles: Callable[[Dict[str, Any]], Dict[str, Any]],
    schema: Optional[Dict[str, Any]] = None,
    slots_override: Optional[Dict[str, Any]] = None,
    unit_intent_ir: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a planning bundle for one QUESTION + TEMPLATE shorthand."""
    template_config = run_templates.get(template)
    if template_config is None:
        supported = ", ".join(sorted(run_templates))
        raise SystemExit(f"Unknown run template: {template}. Supported templates: {supported}")

    if schema is None:
        schema = request_schema(base_url)
        write_schema_state(state_file, base_url)

    planner = build_semantic_query_planner(
        question,
        template,
        schema,
        base_url=base_url,
        slots_override=slots_override,
        unit_intent_ir=unit_intent_ir,
    )
    effective_template = template
    if isinstance(planner, dict) and isinstance(planner.get("effective_template"), str) and planner.get("effective_template"):
        effective_template = planner["effective_template"]
        effective_config = run_templates.get(effective_template)
        if effective_config is not None:
            template_config = effective_config

    profiles = None
    if template_config["auto_include_profiles"]:
        profiles = request_profiles(base_url)

    plan_skeleton: Dict[str, Any] = {"template": effective_template}
    required_fields = []
    if isinstance(planner, dict) and isinstance(planner.get("selected_plan"), dict):
        plan_skeleton = planner["selected_plan"]
        if isinstance(plan_skeleton.get("template"), str) and plan_skeleton.get("template"):
            effective_template = plan_skeleton["template"]

    if template_config["requires_sparql"] and "sparql" not in plan_skeleton:
        if effective_template in ("causal_lookup", "causal_enumeration"):
            required_fields.append("sparql.builder")
            source_var = "source"
            evidence_var = "evidence"
            builder = {
                "source_class": "SourceClass",
                "source_var": source_var,
                "evidence_class": "EvidenceClass",
                "evidence_var": evidence_var,
                "select": [
                    {"var": source_var, "kind": "uri"},
                    {"var": "sourceName", "subject": "source", "property": "source_name"},
                    {"var": "sourceId", "subject": "source", "property": "source_id"},
                    {"var": evidence_var, "kind": "uri"},
                    {"var": "evidenceType", "subject": "evidence", "property": "evidence_type"},
                    {"var": "evidenceDescription", "subject": "evidence", "property": "evidence_description"},
                ],
                "filters": (
                    [{"var": "sourceId", "op": "equals", "value": "ID_123"}]
                    if effective_template == "causal_lookup"
                    else [{"var": "evidenceType", "op": "contains_any", "values": ["keyword1", "keyword2"]}]
                ),
                "distinct": True,
                "order_by": ["sourceId", "evidenceType"],
            }
            plan_skeleton["sparql"] = {
                "source_var": source_var,
                "builder": builder,
            }
        else:
            required_fields.append("sparql.query")
            plan_skeleton["sparql"] = {
                "query": (
                    "PREFIX ex: <http://example.com/ontology#>\n"
                    "SELECT ?entity\n"
                    "WHERE {\n"
                    "  ?entity a ex:TargetClass .\n"
                    "}\n"
                    "LIMIT 10"
                )
            }

    analysis_kind = template_config["default_analysis_kind"]
    if template_config["requires_analysis"] and analysis_kind and "analysis" not in plan_skeleton:
        required_fields.append("analysis.payload")
        analysis_payload: Dict[str, Any]
        if analysis_kind == "paths":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "max_depth": 3,
            }
        elif analysis_kind == "paths-batch":
            analysis_payload = {
                "mode": "paths",
                "profile": "default",
                "max_depth": 3,
            }
        elif analysis_kind == "inferred-relations":
            analysis_payload = {
                "mode": "inferred-relations",
                "profile": "inference",
                "source": "http://example.com/ontology#entity_123",
                "max_depth": 3,
            }
        else:
            analysis_payload = {"mode": analysis_kind}

        plan_skeleton["analysis"] = {
            "kind": analysis_kind,
            "payload": analysis_payload,
        }

    plan_ready = bool(isinstance(planner, dict) and planner.get("ready"))
    if plan_ready:
        required_fields = []

    response: Dict[str, Any] = {
        "mode": "question-template",
        "status": "planner_suggested" if plan_ready else "planning_required",
        "question": question,
        "template": effective_template,
        "message": (
            "Planning-only mode fetched schema first and routed the question through the semantic query planner. "
            "Use this only for debugging or inspection; normal QUESTION + --template flow executes the locked planner plan automatically."
        ),
        "required_fields": required_fields,
        "plan_skeleton": plan_skeleton,
        "planner": planner,
        "plan_executable": plan_ready,
        "rules": [
            "Normal QUESTION + --template flow executes automatically; use --plan-only only when you explicitly need the planner bundle.",
            "Do not hand-write GET /analysis/paths query strings; use analysis-paths --json or analysis-paths-batch --json.",
            "Use schema to verify domains before writing SPARQL.",
            "For causal templates, prefer sparql.builder over raw sparql.query so the client can validate link direction and required anchor columns before execution.",
            "If the planner is low-confidence or ambiguous, do not guess; refine one slot or ask for clarification.",
        ],
        "schema_summary": summarize_schema(schema),
        "schema_included": False,
    }
    if effective_template != template:
        response["requested_template"] = template
        response["effective_template"] = effective_template
    elif isinstance(planner, dict) and planner.get("query_family"):
        response["effective_template"] = effective_template
    if isinstance(planner, dict) and planner.get("query_family"):
        response["query_family"] = planner.get("query_family")
    if profiles is not None:
        response["profiles_summary"] = summarize_profiles(profiles)
        response["profiles_included"] = False
    if isinstance(unit_intent_ir, dict):
        response["intent_ir"] = unit_intent_ir
    return response


def execute_single_question_mode_run(
    base_url: str,
    question: str,
    template: str,
    state_file: Path,
    *,
    include_planner_debug: bool,
    build_single_question_mode_run_response: Callable[..., Dict[str, Any]],
    execute_run_plan: Callable[[str, Dict[str, Any], Path], Dict[str, Any]],
    sparql_row_count: Callable[[Optional[Dict[str, Any]]], int],
    summarize_planner_result: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    apply_fail_closed_contract_to_question_response: Callable[[Dict[str, Any]], Dict[str, Any]],
    apply_bounded_recovery_contract_to_question_response: Callable[[Dict[str, Any]], Dict[str, Any]],
    planning_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Plan and execute one QUESTION + TEMPLATE shorthand with a locked planner-selected plan."""
    planning = planning_override or build_single_question_mode_run_response(base_url, question, template, state_file)
    if not planning.get("plan_executable"):
        planning["message"] = (
            "QUESTION + --template fetched schema first but could not produce a high-confidence executable plan."
        )
        return apply_fail_closed_contract_to_question_response(planning)

    planner = planning.get("planner")
    candidate_plans = []
    if isinstance(planner, dict) and isinstance(planner.get("candidate_plans"), list):
        candidate_plans = [item for item in planner["candidate_plans"] if isinstance(item, dict)]

    selected_candidate = candidate_plans[0] if candidate_plans else None
    locked_plan = planning.get("plan_skeleton")
    if selected_candidate and isinstance(selected_candidate.get("plan"), dict):
        locked_plan = selected_candidate["plan"]
    if not isinstance(locked_plan, dict):
        raise SystemExit("Question-mode planner did not return a valid executable plan.")

    def summarize_attempt(candidate: Optional[Dict[str, Any]], result: Dict[str, Any]) -> Dict[str, Any]:
        attempt = {
            "status": result.get("status"),
            "row_count": sparql_row_count(result.get("sparql")),
        }
        if isinstance(candidate, dict):
            attempt["variant"] = candidate.get("variant")
            attempt["confidence_score"] = candidate.get("confidence_score")
            attempt["rationale"] = candidate.get("rationale")
        return attempt

    def attempt_score(candidate: Optional[Dict[str, Any]], result: Dict[str, Any]) -> tuple:
        status = result.get("status")
        status_score = 2 if status == "ok" else 1 if status == "partial_success" else 0
        row_count = sparql_row_count(result.get("sparql"))
        confidence = candidate.get("confidence_score") if isinstance(candidate, dict) else 0
        return (status_score, row_count, confidence)

    def try_execute_candidate_plan(
        candidate: Optional[Dict[str, Any]],
        plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            return execute_run_plan(base_url, plan, state_file)
        except SystemExit as exc:
            return {
                "status": "execution_error",
                "template": plan.get("template") if isinstance(plan.get("template"), str) else template,
                "error": str(exc),
            }

    executed = try_execute_candidate_plan(selected_candidate, locked_plan)
    if executed.get("status") == "execution_error":
        raise SystemExit(executed.get("error") or "Question-mode planner selected an invalid executable plan.")
    attempts = [summarize_attempt(selected_candidate, executed)]
    chosen_candidate = selected_candidate

    relaxed_candidates = [
        candidate
        for candidate in candidate_plans[1:]
        if str(candidate.get("variant", "")).endswith("_relaxed")
    ]

    primary_row_count = sparql_row_count(executed.get("sparql"))
    should_try_relaxed = (
        template == "causal_enumeration"
        and bool(relaxed_candidates)
        and (
            executed.get("status") == "empty_result"
            or (
                executed.get("status") == "ok"
                and primary_row_count <= 1
                and isinstance(selected_candidate, dict)
                and (
                    selected_candidate.get("variant") in ("cause_and_action", "same_evidence_strict")
                    or (
                        isinstance(selected_candidate.get("rationale"), list)
                        and "cause_term_grounded" in selected_candidate["rationale"]
                        and "action_term_grounded" in selected_candidate["rationale"]
                    )
                )
            )
        )
    )

    if should_try_relaxed:
        best_result = executed
        best_candidate = chosen_candidate
        best_score = attempt_score(chosen_candidate, executed)
        for relaxed_candidate in relaxed_candidates:
            if not isinstance(relaxed_candidate.get("plan"), dict):
                continue
            relaxed_result = try_execute_candidate_plan(relaxed_candidate, relaxed_candidate["plan"])
            attempts.append(summarize_attempt(relaxed_candidate, relaxed_result))
            relaxed_score = attempt_score(relaxed_candidate, relaxed_result)
            if relaxed_score > best_score:
                best_result = relaxed_result
                best_candidate = relaxed_candidate
                best_score = relaxed_score
        executed = best_result
        chosen_candidate = best_candidate

    response = dict(executed)
    effective_template = response.get("template") if isinstance(response.get("template"), str) else template
    response.update({
        "mode": "question-template",
        "question": question,
        "template": effective_template,
        "execution_mode": "question_auto_execute",
        "plan_locked": True,
        "plan_executable": True,
        "message": (
            "QUESTION + --template fetched schema first, planned a locked query, "
            "and executed it automatically."
        ),
    })
    if effective_template != template:
        response["requested_template"] = template
        response["effective_template"] = effective_template
    elif isinstance(planning.get("planner"), dict) and planning["planner"].get("query_family"):
        response["effective_template"] = effective_template
    if isinstance(planning.get("planner"), dict) and planning["planner"].get("query_family"):
        response["query_family"] = planning["planner"].get("query_family")
    if include_planner_debug:
        response["planner_summary"] = summarize_planner_result(planning.get("planner"))
        response["planner_attempts"] = attempts
        response["execution_variant"] = chosen_candidate.get("variant") if isinstance(chosen_candidate, dict) else None
    if response.get("status") == "empty_result":
        response = apply_bounded_recovery_contract_to_question_response(response)
    return response
