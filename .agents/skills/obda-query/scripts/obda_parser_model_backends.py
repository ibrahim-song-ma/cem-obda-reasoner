#!/usr/bin/env python3
"""Generic model backends for the repo-owned language intent parser."""

from __future__ import annotations

import json
import os
import socket
import subprocess
from copy import deepcopy
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from obda_ir_contracts import sanitize_dict_list
from obda_model_backend_profiles import (
    DEFAULT_MODEL_BACKEND,
    LEGACY_AGENT_MODEL_BINARY_ENV,
    LEGACY_AGENT_MODEL_NAME_ENV,
    LEGACY_AGENT_MODEL_TIMEOUT_ENV,
    LEGACY_AGENT_MODEL_UTTERANCE_TIMEOUT_ENV,
    LEGACY_AGENT_MODEL_MAX_ATTEMPTS_ENV,
    MODEL_API_KEY_ENV,
    MODEL_BASE_URL_ENV,
    MODEL_BINARY_ENV,
    MODEL_MAX_ATTEMPTS_ENV,
    MODEL_MOCK_RESPONSE_ENV,
    MODEL_MOCK_RESPONSE_FILE_ENV,
    MODEL_NAME_ENV,
    MODEL_PATH_ENV,
    MODEL_TIMEOUT_ENV,
    MODEL_UTTERANCE_TIMEOUT_ENV,
    model_backend_profile,
    model_backend_transport,
    normalize_model_backend,
)


DEFAULT_ANTHROPIC_CLI_BINARY = "claude"
DEFAULT_OPENAI_COMPATIBLE_PATH = "/chat/completions"

PARSER_OUTPUT_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "question_units": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "unit_id": {"type": "string"},
                    "raw_text": {"type": "string"},
                    "normalized_text": {"type": "string"},
                    "dependency": {
                        "anyOf": [
                            {"type": "null"},
                            {
                                "type": "object",
                                "additionalProperties": True,
                                "properties": {
                                    "depends_on": {"type": "string"},
                                    "condition": {"type": "string"},
                                    "source": {"type": "string"},
                                    "prefix": {"type": "string"},
                                },
                            },
                        ]
                    },
                    "reference_markers": {"type": "array", "items": {"type": "string"}},
                    "anchor_forms": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
                    "comparators": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
                    "question_acts": {"type": "array", "items": {"type": "string"}},
                    "surface_constraints": {
                        "type": "array",
                        "items": {"type": "object", "additionalProperties": True},
                    },
                    "ambiguities": {
                        "type": "array",
                        "items": {"type": "object", "additionalProperties": True},
                    },
                    "confidence": {"type": "number"},
                },
                "required": [
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
                ],
            },
        },
        "parser_confidence": {"type": "number"},
        "ambiguities": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
        "clarification_candidates": {
            "type": "array",
            "items": {"type": "object", "additionalProperties": True},
        },
        "bootstrap_operator_hints": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "operator": {"type": "string"},
                    "source": {"type": "string"},
                },
            },
        },
        "surface_constraints": {
            "type": "array",
            "items": {"type": "object", "additionalProperties": True},
        },
        "status_numeric_constraint": {"type": "object", "additionalProperties": True},
    },
    "required": ["question_units", "ambiguities", "clarification_candidates"],
}


NO_PROXY_OPENER = urllib_request.build_opener(urllib_request.ProxyHandler({}))


def _env_value(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        if not name:
            continue
        value = os.getenv(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return default


def _int_env(*names: str, default: int) -> int:
    value = _env_value(*names)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _timeout_for_parser_input(parser_input: Dict[str, Any]) -> int:
    if isinstance(parser_input.get("question_units_hint"), list):
        return _int_env(
            MODEL_UTTERANCE_TIMEOUT_ENV,
            LEGACY_AGENT_MODEL_UTTERANCE_TIMEOUT_ENV,
            MODEL_TIMEOUT_ENV,
            LEGACY_AGENT_MODEL_TIMEOUT_ENV,
            default=20,
        )
    return _int_env(
        MODEL_TIMEOUT_ENV,
        LEGACY_AGENT_MODEL_TIMEOUT_ENV,
        default=12,
    )


def _max_attempts() -> int:
    return _int_env(
        MODEL_MAX_ATTEMPTS_ENV,
        LEGACY_AGENT_MODEL_MAX_ATTEMPTS_ENV,
        default=2,
    )


def _normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "".join(value.split()).lower()


def _append_ambiguity(output: Dict[str, Any], ambiguity: Dict[str, Any]) -> Dict[str, Any]:
    result = deepcopy(output)
    ambiguities = sanitize_dict_list(result.get("ambiguities"))
    ambiguities.append(ambiguity)
    result["ambiguities"] = ambiguities
    return result


def _build_backend_failure_output(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
    *,
    backend: str,
    kind: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    output = deepcopy(deterministic_output) if isinstance(deterministic_output, dict) else {}
    output["strategy"] = "HybridStrategy"
    output["model_backend"] = backend
    output["ir_provenance"] = "hybrid_ir"
    question_units_hint = (
        deepcopy(parser_input.get("question_units_hint"))
        if isinstance(parser_input.get("question_units_hint"), list)
        else None
    )
    if question_units_hint:
        output["question_units"] = question_units_hint
    ambiguity = {"kind": kind, "backend": backend, "fallback_backend": "NoModelBackend"}
    if isinstance(extra, dict):
        ambiguity.update(extra)
    return _append_ambiguity(output, ambiguity)


def _build_backend_success_output(
    parser_input: Dict[str, Any],
    backend: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    output = deepcopy(payload)
    output["strategy"] = "HybridStrategy"
    output["model_backend"] = backend
    output["ir_provenance"] = "hybrid_ir"
    question_units_hint = parser_input.get("question_units_hint")
    if isinstance(question_units_hint, list) and question_units_hint and not isinstance(output.get("question_units"), list):
        output["question_units"] = deepcopy(question_units_hint)
    return output


def _extract_json_payload(text: str) -> Dict[str, Any]:
    payload = json.loads(text)
    if isinstance(payload, dict) and isinstance(payload.get("structured_output"), dict):
        return deepcopy(payload["structured_output"])
    if isinstance(payload, dict) and isinstance(payload.get("payload"), dict):
        return deepcopy(payload["payload"])
    if isinstance(payload, dict) and isinstance(payload.get("response"), dict):
        return deepcopy(payload["response"])
    if isinstance(payload, dict) and isinstance(payload.get("result"), str):
        try:
            nested = json.loads(payload["result"])
        except (TypeError, ValueError):
            nested = None
        if isinstance(nested, dict):
            return nested
    if isinstance(payload, str):
        nested = json.loads(payload)
        if isinstance(nested, dict):
            return nested
    if isinstance(payload, dict):
        return payload
    raise ValueError("model backend did not return a JSON object payload")


def _payload_has_projection_mismatch(parser_input: Dict[str, Any], payload: Dict[str, Any]) -> bool:
    question_units_hint = parser_input.get("question_units_hint")
    if not isinstance(question_units_hint, list) or not question_units_hint:
        return False
    question_units = payload.get("question_units")
    if not isinstance(question_units, list):
        return True
    return len(question_units) != len(question_units_hint)


def _payload_is_underfilled(payload: Dict[str, Any]) -> bool:
    question_units = payload.get("question_units")
    if not isinstance(question_units, list) or not question_units:
        return True
    for unit in question_units:
        if not isinstance(unit, dict):
            return True
        question_acts = unit.get("question_acts")
        surface_constraints = unit.get("surface_constraints")
        if isinstance(question_acts, list) and question_acts:
            continue
        if isinstance(surface_constraints, list) and surface_constraints:
            continue
        return True
    return False


def _build_parser_prompt(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> str:
    prompt_payload = {
        "task": "Parse the utterance into the repo-owned parser schema only.",
        "rules": {
            "must_return_json_object_only": True,
            "must_not_output_ontology_binding": True,
            "must_not_output_query_family_or_sparql": True,
            "must_preserve_question_unit_count_when_hint_provided": True,
        },
        "parser_input": {
            "utterance": parser_input.get("utterance"),
            "template": parser_input.get("template"),
            "question_unit": parser_input.get("question_unit"),
            "question_units_hint": parser_input.get("question_units_hint"),
        },
        "deterministic_baseline": deterministic_output,
        "required_schema": PARSER_OUTPUT_JSON_SCHEMA,
    }
    return json.dumps(prompt_payload, ensure_ascii=False, indent=2)


def _anthropic_cli_command(prompt: str, *, mode: str = "schema") -> List[str]:
    binary = _env_value(MODEL_BINARY_ENV, LEGACY_AGENT_MODEL_BINARY_ENV, default=DEFAULT_ANTHROPIC_CLI_BINARY)
    command = [
        binary,
        "-p",
        "--permission-mode",
        "bypassPermissions",
        "--tools",
        "",
        "--output-format",
        "json",
        prompt,
    ]
    if mode == "schema":
        command[-1:-1] = [
            "--json-schema",
            json.dumps(PARSER_OUTPUT_JSON_SCHEMA, ensure_ascii=False),
        ]
    model_name = _env_value(MODEL_NAME_ENV, LEGACY_AGENT_MODEL_NAME_ENV)
    if model_name:
        command[1:1] = ["--model", model_name]
    return command


def _run_anthropic_cli_once(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
    *,
    mode: str = "schema",
) -> Dict[str, Any]:
    timeout_sec = _timeout_for_parser_input(parser_input)
    prompt = _build_parser_prompt(parser_input, deterministic_output)
    completed = subprocess.run(
        _anthropic_cli_command(prompt, mode=mode),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(stderr or f"anthropic-compatible cli exited with code {completed.returncode}")
    return _extract_json_payload((completed.stdout or "").strip())


def _run_anthropic_cli_with_modes(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    profile = model_backend_profile("AnthropicCompatibleCliBackend")
    parse_modes = profile.get("parse_modes")
    modes = list(parse_modes) if isinstance(parse_modes, tuple) and parse_modes else ["schema"]
    last_exc: Optional[BaseException] = None
    for mode in modes:
        try:
            return _run_anthropic_cli_once(
                parser_input,
                deterministic_output,
                mode=mode,
            )
        except (subprocess.TimeoutExpired, json.JSONDecodeError, ValueError, RuntimeError) as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("anthropic-compatible cli did not produce a parser payload")


def _extract_openai_content(payload: Dict[str, Any]) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("openai-compatible backend returned no choices")
    first = choices[0]
    if not isinstance(first, dict):
        raise ValueError("openai-compatible backend returned invalid choice")
    message = first.get("message")
    if not isinstance(message, dict):
        raise ValueError("openai-compatible backend returned no message")
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text" and isinstance(item.get("text"), str):
                    text_parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    text_parts.append(item["content"])
            elif isinstance(item, str):
                text_parts.append(item)
        joined = "".join(text_parts).strip()
        if joined:
            return joined
    raise ValueError("openai-compatible backend returned unsupported message content")


def _run_openai_compatible_once(parser_input: Dict[str, Any], deterministic_output: Dict[str, Any]) -> Dict[str, Any]:
    base_url = _env_value(MODEL_BASE_URL_ENV)
    if not base_url:
        raise FileNotFoundError("missing OBDA_MODEL_BASE_URL")
    model_name = _env_value(MODEL_NAME_ENV, LEGACY_AGENT_MODEL_NAME_ENV, default="parser-model")
    timeout_sec = _timeout_for_parser_input(parser_input)
    api_path = _env_value(MODEL_PATH_ENV, default=DEFAULT_OPENAI_COMPATIBLE_PATH)
    api_key = _env_value(MODEL_API_KEY_ENV, default="")
    endpoint = base_url.rstrip("/") + api_path
    body = {
        "model": model_name,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": _build_parser_prompt(parser_input, deterministic_output),
            }
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "obda_parser_output",
                "strict": True,
                "schema": PARSER_OUTPUT_JSON_SCHEMA,
            },
        },
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib_request.Request(
        endpoint,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with NO_PROXY_OPENER.open(request, timeout=timeout_sec) as response:
        raw = response.read().decode("utf-8")
    envelope = json.loads(raw)
    return _extract_json_payload(_extract_openai_content(envelope))


def _run_mock_backend_once(_: Dict[str, Any], __: Dict[str, Any]) -> Dict[str, Any]:
    response_file = _env_value(MODEL_MOCK_RESPONSE_FILE_ENV)
    if response_file:
        with open(response_file, "r", encoding="utf-8") as handle:
            return _extract_json_payload(handle.read())
    response_text = _env_value(MODEL_MOCK_RESPONSE_ENV)
    if response_text:
        return _extract_json_payload(response_text)
    raise FileNotFoundError("missing OBDA_MODEL_MOCK_RESPONSE or OBDA_MODEL_MOCK_RESPONSE_FILE")


def _run_with_attempts(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
    *,
    backend: str,
    runner,
) -> Dict[str, Any]:
    attempts = _max_attempts()
    last_failure: Optional[Dict[str, Any]] = None
    for _ in range(attempts):
        try:
            payload = runner(parser_input, deterministic_output)
        except FileNotFoundError as exc:
            return _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_unavailable",
                extra={"detail": str(exc)},
            )
        except subprocess.TimeoutExpired:
            last_failure = _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_timeout",
            )
            continue
        except (json.JSONDecodeError, ValueError, RuntimeError) as exc:
            last_failure = _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_parse_failed",
                extra={"detail": str(exc)},
            )
            continue
        except (urllib_error.URLError, urllib_error.HTTPError, socket.timeout) as exc:
            detail = getattr(exc, "reason", None) or str(exc)
            last_failure = _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_timeout" if isinstance(exc, socket.timeout) else "backend_parse_failed",
                extra={"detail": str(detail)},
            )
            continue

        if _payload_has_projection_mismatch(parser_input, payload):
            return _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_projection_mismatch",
                extra={
                    "expected_units": len(parser_input.get("question_units_hint", []))
                    if isinstance(parser_input.get("question_units_hint"), list)
                    else None,
                    "actual_units": len(payload.get("question_units", []))
                    if isinstance(payload.get("question_units"), list)
                    else None,
                },
            )
        if _payload_is_underfilled(payload):
            return _build_backend_failure_output(
                parser_input,
                deterministic_output,
                backend=backend,
                kind="backend_underfilled_parse",
            )
        return _build_backend_success_output(parser_input, backend, payload)
    return last_failure or _build_backend_failure_output(
        parser_input,
        deterministic_output,
        backend=backend,
        kind="backend_parse_failed",
    )


def run_anthropic_compatible_cli_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Run one Anthropic-compatible CLI adapter under the repo-owned parser contract."""
    return _run_with_attempts(
        parser_input,
        deterministic_output,
        backend="AnthropicCompatibleCliBackend",
        runner=_run_anthropic_cli_with_modes,
    )


def run_openai_compatible_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Run one OpenAI-compatible HTTP adapter for parser generation."""
    return _run_with_attempts(
        parser_input,
        deterministic_output,
        backend="OpenAICompatibleBackend",
        runner=_run_openai_compatible_once,
    )


def run_mock_model_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Run one mock backend for parser-regression fixtures."""
    return _run_with_attempts(
        parser_input,
        deterministic_output,
        backend="MockModelBackend",
        runner=_run_mock_backend_once,
    )


def run_model_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Dispatch one repo-owned model backend by normalized backend label."""
    backend = normalize_model_backend(parser_input.get("model_backend"))
    transport = model_backend_transport(backend)
    if transport == "anthropic_compatible_cli":
        return run_anthropic_compatible_cli_backend_parse(parser_input, deterministic_output)
    if transport == "openai_compatible_http":
        return run_openai_compatible_backend_parse(parser_input, deterministic_output)
    if transport == "mock":
        return run_mock_model_backend_parse(parser_input, deterministic_output)
    if transport == "none" or backend == DEFAULT_MODEL_BACKEND:
        return deepcopy(deterministic_output) if isinstance(deterministic_output, dict) else {}
    return _build_backend_failure_output(
        parser_input,
        deterministic_output,
        backend=backend,
        kind="backend_not_implemented",
        extra={"transport": model_backend_profile(backend).get("transport")},
    )


def run_agent_model_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Legacy compatibility alias for older imports/tests."""
    return run_anthropic_compatible_cli_backend_parse(parser_input, deterministic_output)


def run_claude_cli_backend_parse(
    parser_input: Dict[str, Any],
    deterministic_output: Dict[str, Any],
) -> Dict[str, Any]:
    """Legacy compatibility alias for earlier backend naming."""
    return run_anthropic_compatible_cli_backend_parse(parser_input, deterministic_output)
