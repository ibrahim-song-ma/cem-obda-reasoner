#!/usr/bin/env python3
"""Repo-owned runtime orchestration for guarded run-plan execution."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Callable, Dict, Optional
import urllib.error


def execute_run_plan_once(
    base_url: str,
    plan: Dict[str, Any],
    state_file: Path,
    *,
    request_fn: Callable[[str, str, Optional[Dict[str, Any]]], Any],
    write_schema_state: Callable[[Path, str], None],
    prepare_sparql_spec: Callable[[Dict[str, Any], Dict[str, Any], str], Dict[str, Any]],
    sparql_row_count: Callable[[Optional[Dict[str, Any]]], int],
    derive_uri_sources_from_sparql: Callable[..., Dict[str, Any]],
    build_related_terminal_detail_index: Callable[..., Dict[str, Any]],
    build_run_presentation: Callable[..., Optional[Dict[str, Any]]],
    summarize_analysis_response: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    summarize_profiles: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Execute one normalized run plan with one request transport."""
    schema = request_fn("GET", f"{base_url}/schema")
    write_schema_state(state_file, base_url)

    analysis_spec = plan.get("analysis")
    include_profiles = bool(plan.get("include_profiles"))
    profiles = request_fn("GET", f"{base_url}/analysis/profiles") if include_profiles else None

    samples = []
    for sample_spec in plan.get("samples", []):
        class_name = sample_spec.get("class_name")
        if not class_name:
            raise SystemExit("Each sample spec requires class_name")
        limit = int(sample_spec.get("limit", 3))
        query = urllib.parse.urlencode({"limit": limit})
        url = f"{base_url}/sample/{urllib.parse.quote(class_name)}?{query}"
        samples.append({
            "class_name": class_name,
            "limit": limit,
            "response": request_fn("GET", url),
        })

    sparql_response = None
    sparql_meta = None
    sparql_spec = plan.get("sparql")
    prepared = None
    if sparql_spec is not None:
        prepared = prepare_sparql_spec(schema, sparql_spec, plan["template"])
        query_text = prepared["query"]
        sparql_meta = prepared.get("builder_meta")
        sparql_response = request_fn("POST", f"{base_url}/sparql", {"query": query_text})

    analysis_response = None
    analysis_meta = None
    analysis_error = None
    analysis_skipped = None
    response_enrichment = plan.get("response_enrichment") if isinstance(plan.get("response_enrichment"), dict) else {}
    if analysis_spec is not None:
        kind = analysis_spec.get("kind", "paths")
        row_count = sparql_row_count(sparql_response)

        if plan["template"] in ("causal_lookup", "causal_enumeration") and sparql_spec is not None and row_count == 0:
            analysis_skipped = {
                "reason": "sparql_no_results",
                "message": "Main SPARQL returned no rows, so analyzer was not executed.",
            }
        elif kind == "causal":
            customer_id = analysis_spec.get("customer_id")
            if not customer_id:
                raise SystemExit("analysis kind 'causal' requires customer_id")
            analysis_response = request_fn(
                "GET",
                f"{base_url}/causal/{urllib.parse.quote(customer_id)}",
            )
        else:
            payload = analysis_spec.get("payload")
            if payload is None:
                payload = {k: v for k, v in analysis_spec.items() if k != "kind"}
            payload = dict(payload)

            preferred_source_var = analysis_spec.get("source_var")
            if not preferred_source_var and sparql_spec is not None:
                preferred_source_var = sparql_spec.get("source_var")
            if not preferred_source_var and sparql_meta is not None and isinstance(prepared, dict):
                preferred_source_var = prepared.get("source_var")

            if kind == "paths-batch" and not payload.get("sources"):
                derived = derive_uri_sources_from_sparql(
                    sparql_response,
                    preferred_var=preferred_source_var,
                    multiple=True,
                )
                if derived["values"]:
                    payload["sources"] = derived["values"]
                    analysis_meta = {
                        "auto_derived_source_var": derived["source_var"],
                        "auto_derived_source_count": len(derived["values"]),
                    }
            elif kind == "paths" and not payload.get("source"):
                derived = derive_uri_sources_from_sparql(
                    sparql_response,
                    preferred_var=preferred_source_var,
                    multiple=False,
                )
                if derived["values"]:
                    payload["source"] = derived["values"][0]
                    analysis_meta = {
                        "auto_derived_source_var": derived["source_var"],
                        "auto_derived_source_count": 1,
                    }

            if kind == "paths-batch" and not payload.get("sources"):
                analysis_error = {
                    "kind": "missing_sources",
                    "message": (
                        "paths-batch analysis requires analysis.payload.sources, "
                        "or a SPARQL result column containing URI anchors that the client can auto-derive."
                    ),
                    "hint": (
                        "Return at least one entity URI column from the main SPARQL, "
                        "for example ?source or ?entity, or set sparql.source_var / analysis.payload.sources explicitly."
                    ),
                }
            elif kind == "paths" and not payload.get("source"):
                analysis_error = {
                    "kind": "missing_source",
                    "message": (
                        "paths analysis requires analysis.payload.source, "
                        "or a SPARQL result column containing a URI anchor that the client can auto-derive."
                    ),
                    "hint": (
                        "Return at least one entity URI column from the main SPARQL, "
                        "for example ?source or ?entity, or set sparql.source_var / analysis.payload.source explicitly."
                    ),
                }
            else:
                if kind in ("paths", "paths-batch") and "mode" not in payload:
                    payload["mode"] = "paths"
                endpoint_map = {
                    "paths": "/analysis/paths",
                    "paths-batch": "/analysis/paths/batch",
                    "neighborhood": "/analysis/neighborhood",
                    "inferred-relations": "/analysis/inferred-relations",
                    "explain": "/analysis/explain",
                }
                endpoint = endpoint_map.get(kind)
                if endpoint is None:
                    raise SystemExit(f"Unsupported analysis kind: {kind}")
                analysis_response = request_fn("POST", f"{base_url}{endpoint}", payload)

    if (
        analysis_response is not None
        and bool(response_enrichment.get("include_terminal_entity_details"))
    ):
        terminal_detail_index = build_related_terminal_detail_index(
            schema,
            sparql_spec,
            sparql_response,
            analysis_response,
            analysis_meta,
            base_url,
            request_fn,
        )
        if terminal_detail_index:
            if not isinstance(analysis_meta, dict):
                analysis_meta = {}
            analysis_meta["related_terminal_details_by_source"] = terminal_detail_index
        if response_enrichment.get("target_projection_requested"):
            if not isinstance(analysis_meta, dict):
                analysis_meta = {}
            analysis_meta["target_projection_requested"] = True
        if response_enrichment.get("asks_solution"):
            if not isinstance(analysis_meta, dict):
                analysis_meta = {}
            analysis_meta["asks_solution"] = True

    row_count = sparql_row_count(sparql_response)
    response = {
        "template": plan["template"],
        "sparql": sparql_response,
        "sparql_meta": sparql_meta,
        "analysis_meta": analysis_meta,
    }
    if samples:
        response["samples"] = samples
    if analysis_error is not None:
        response["status"] = "partial_success"
        response["analysis_error"] = analysis_error
    elif row_count == 0:
        response["status"] = "empty_result"
        if analysis_skipped is not None:
            response["analysis_skipped"] = analysis_skipped
    elif analysis_skipped is not None:
        response["status"] = "empty_result"
        response["analysis_skipped"] = analysis_skipped
    else:
        response["status"] = "ok"

    if response["status"] == "empty_result" and isinstance(sparql_meta, dict):
        recovery_hint = {
            "strategy": "targeted_grounding_rerun",
            "max_samples": 1,
            "rerun_required": True,
        }
        if sparql_meta.get("mode") == "builder":
            recovery_hint["preferred_classes"] = [
                value
                for value in [sparql_meta.get("evidence_class"), sparql_meta.get("source_class")]
                if isinstance(value, str) and value
            ]
        response["recovery_hint"] = recovery_hint

    if plan.get("include_schema"):
        response["schema"] = schema
        response["schema_included"] = True
    else:
        response["schema_summary"] = summarize_schema(schema)
        response["schema_included"] = False

    if profiles is not None:
        if plan.get("include_profiles"):
            response["profiles"] = profiles
            response["profiles_included"] = True
        else:
            response["profiles_summary"] = summarize_profiles(profiles)
            response["profiles_included"] = False

    presentation = build_run_presentation(
        plan,
        schema,
        sparql_response,
        analysis_response,
        analysis_meta,
        response["status"],
        analysis_error,
        analysis_skipped,
    )
    if presentation is not None:
        response["presentation"] = presentation

    if analysis_response is not None:
        if plan.get("include_analysis"):
            response["analysis"] = analysis_response
            response["analysis_included"] = True
        else:
            response["analysis"] = summarize_analysis_response(analysis_response)
            response["analysis_included"] = False

    return response


def execute_run_plan(
    base_url: str,
    plan: Dict[str, Any],
    state_file: Path,
    *,
    normalize_run_plan: Callable[[Dict[str, Any]], Dict[str, Any]],
    http_request_json: Callable[[str, str, Optional[Dict[str, Any]]], Any],
    curl_request_json: Callable[[str, str, Optional[Dict[str, Any]]], Any],
    client_request_json: Callable[[Any, str, str, Optional[Dict[str, Any]]], Any],
    local_test_client_factory: Callable[[], Any],
    write_schema_state: Callable[[Path, str], None],
    prepare_sparql_spec: Callable[[Dict[str, Any], Dict[str, Any], str], Dict[str, Any]],
    sparql_row_count: Callable[[Optional[Dict[str, Any]]], int],
    derive_uri_sources_from_sparql: Callable[..., Dict[str, Any]],
    build_related_terminal_detail_index: Callable[..., Dict[str, Any]],
    build_run_presentation: Callable[..., Optional[Dict[str, Any]]],
    summarize_analysis_response: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    summarize_schema: Callable[[Dict[str, Any]], Dict[str, Any]],
    summarize_profiles: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Dict[str, Any]:
    """Execute a guarded multi-step workflow with schema fetched first."""
    normalized_plan = normalize_run_plan(plan)
    if not any(key in normalized_plan for key in ("samples", "sparql", "analysis")):
        raise SystemExit("run requires at least one of: samples, sparql, analysis")

    def run_with_request(request_fn: Callable[[str, str, Optional[Dict[str, Any]]], Any]) -> Dict[str, Any]:
        return execute_run_plan_once(
            base_url,
            normalized_plan,
            state_file,
            request_fn=request_fn,
            write_schema_state=write_schema_state,
            prepare_sparql_spec=prepare_sparql_spec,
            sparql_row_count=sparql_row_count,
            derive_uri_sources_from_sparql=derive_uri_sources_from_sparql,
            build_related_terminal_detail_index=build_related_terminal_detail_index,
            build_run_presentation=build_run_presentation,
            summarize_analysis_response=summarize_analysis_response,
            summarize_schema=summarize_schema,
            summarize_profiles=summarize_profiles,
        )

    try:
        return run_with_request(http_request_json)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        try:
            return run_with_request(curl_request_json)
        except RuntimeError as curl_exc:
            print(f"Run plan failed with HTTP {exc.code}; curl fallback failed: {curl_exc}", file=sys.stderr)
            if error_body:
                print(error_body, file=sys.stderr)
            raise SystemExit(1) from exc
        except SystemExit as curl_exit:
            if error_body:
                print(error_body, file=sys.stderr)
            if curl_exit.code in (502, 503, 504):
                raise SystemExit(1) from exc
            raise
    except urllib.error.URLError as exc:
        reason = str(exc.reason)
        try:
            return run_with_request(curl_request_json)
        except RuntimeError as curl_exc:
            if "Operation not permitted" not in reason:
                print(f"Request failed: {exc}; curl fallback failed: {curl_exc}", file=sys.stderr)
                raise SystemExit(1) from exc
            with local_test_client_factory() as client:
                return run_with_request(
                    lambda method, url, payload=None: client_request_json(client, method, url, payload)
                )
        except SystemExit as curl_exit:
            if curl_exit.code in (502, 503, 504):
                print(f"Run plan curl fallback returned HTTP {curl_exit.code}", file=sys.stderr)
                raise SystemExit(1) from exc
            raise
