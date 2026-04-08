#!/usr/bin/env python3
"""Protocol-oriented model backend profiles for the repo-owned parser."""

from __future__ import annotations

import os
from copy import deepcopy
from typing import Any, Dict, Optional


DEFAULT_MODEL_BACKEND = "NoModelBackend"
MODEL_BACKEND_ENV = "OBDA_MODEL_BACKEND"
LEGACY_MODEL_BACKEND_ENV = "OBDA_PARSER_MODEL_BACKEND"
MODEL_BINARY_ENV = "OBDA_MODEL_BINARY"
LEGACY_AGENT_MODEL_BINARY_ENV = "OBDA_AGENT_MODEL_BINARY"
MODEL_NAME_ENV = "OBDA_MODEL_NAME"
LEGACY_AGENT_MODEL_NAME_ENV = "OBDA_AGENT_MODEL_NAME"
MODEL_TIMEOUT_ENV = "OBDA_MODEL_TIMEOUT_SEC"
LEGACY_AGENT_MODEL_TIMEOUT_ENV = "OBDA_AGENT_MODEL_TIMEOUT_SEC"
MODEL_UTTERANCE_TIMEOUT_ENV = "OBDA_MODEL_UTTERANCE_TIMEOUT_SEC"
LEGACY_AGENT_MODEL_UTTERANCE_TIMEOUT_ENV = "OBDA_AGENT_MODEL_UTTERANCE_TIMEOUT_SEC"
MODEL_MAX_ATTEMPTS_ENV = "OBDA_MODEL_MAX_ATTEMPTS"
LEGACY_AGENT_MODEL_MAX_ATTEMPTS_ENV = "OBDA_AGENT_MODEL_MAX_ATTEMPTS"
MODEL_BASE_URL_ENV = "OBDA_MODEL_BASE_URL"
MODEL_API_KEY_ENV = "OBDA_MODEL_API_KEY"
MODEL_PATH_ENV = "OBDA_MODEL_PATH"
MODEL_MOCK_RESPONSE_ENV = "OBDA_MODEL_MOCK_RESPONSE"
MODEL_MOCK_RESPONSE_FILE_ENV = "OBDA_MODEL_MOCK_RESPONSE_FILE"
ANTHROPIC_BASE_URL_ENV = "ANTHROPIC_BASE_URL"
ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"

MODEL_BACKEND_ALIASES = {
    "AgentModelBackend": "AnthropicCompatibleCliBackend",
    "ClaudeCliBackend": "AnthropicCompatibleCliBackend",
    "StandaloneModelBackend": "OpenAICompatibleBackend",
}

MODEL_BACKEND_PROFILES: Dict[str, Dict[str, Any]] = {
    "NoModelBackend": {
        "transport": "none",
        "provider_kind": "none",
        "auto_detect": False,
    },
    "AnthropicCompatibleCliBackend": {
        "transport": "anthropic_compatible_cli",
        "provider_kind": "hosted_cli",
        "auto_detect": True,
        "parse_modes": (
            "schema",
            "json_only",
        ),
        "activation_envs": (
            ANTHROPIC_BASE_URL_ENV,
            ANTHROPIC_API_KEY_ENV,
            MODEL_BINARY_ENV,
            LEGACY_AGENT_MODEL_BINARY_ENV,
        ),
    },
    "OpenAICompatibleBackend": {
        "transport": "openai_compatible_http",
        "provider_kind": "http_api",
        "auto_detect": True,
        "activation_envs": (
            MODEL_BASE_URL_ENV,
        ),
    },
    "MockModelBackend": {
        "transport": "mock",
        "provider_kind": "fixture",
        "auto_detect": True,
        "activation_envs": (
            MODEL_MOCK_RESPONSE_ENV,
            MODEL_MOCK_RESPONSE_FILE_ENV,
        ),
    },
}


def supported_model_backends() -> set[str]:
    """Return the repo-owned canonical model backend labels."""
    return set(MODEL_BACKEND_PROFILES)


def normalize_model_backend(value: Any) -> str:
    """Return one canonical model backend label."""
    if isinstance(value, str) and value in MODEL_BACKEND_ALIASES:
        return MODEL_BACKEND_ALIASES[value]
    if isinstance(value, str) and value in MODEL_BACKEND_PROFILES:
        return value
    return DEFAULT_MODEL_BACKEND


def model_backend_profile(value: Any) -> Dict[str, Any]:
    """Return one backend profile record for the canonical backend label."""
    backend = normalize_model_backend(value)
    profile = MODEL_BACKEND_PROFILES.get(backend)
    return deepcopy(profile) if isinstance(profile, dict) else deepcopy(MODEL_BACKEND_PROFILES[DEFAULT_MODEL_BACKEND])


def model_backend_transport(value: Any) -> str:
    """Return the transport family for one canonical backend label."""
    profile = model_backend_profile(value)
    transport = profile.get("transport")
    return transport if isinstance(transport, str) and transport else "none"


def _env_value(*names: str) -> Optional[str]:
    for name in names:
        if not name:
            continue
        value = os.getenv(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def resolve_configured_model_backend(explicit_backend: Any = None) -> str:
    """Resolve the configured backend from explicit value, canonical env, or protocol markers."""
    backend = normalize_model_backend(explicit_backend)
    if backend != DEFAULT_MODEL_BACKEND:
        return backend

    env_backend = normalize_model_backend(
        _env_value(MODEL_BACKEND_ENV, LEGACY_MODEL_BACKEND_ENV)
    )
    if env_backend != DEFAULT_MODEL_BACKEND:
        return env_backend

    for backend_name in ("MockModelBackend", "OpenAICompatibleBackend", "AnthropicCompatibleCliBackend"):
        profile = MODEL_BACKEND_PROFILES.get(backend_name, {})
        activation_envs = profile.get("activation_envs")
        if not isinstance(activation_envs, tuple):
            continue
        if _env_value(*activation_envs):
            return backend_name
    return DEFAULT_MODEL_BACKEND
