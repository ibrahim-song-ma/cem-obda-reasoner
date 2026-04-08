#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API="${ROOT}/.agents/skills/obda-query/scripts/obda_api.sh"
HELPER="${ROOT}/tests/run_host_e2e_regressions.py"
PYTHON_BIN="${ROOT}/.venv/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python virtualenv not found: ${PYTHON_BIN}" >&2
  exit 1
fi

if ! command -v claude >/dev/null 2>&1; then
  echo "claude CLI not found on PATH" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/obda_host_e2e_regressions.XXXXXX")"
SERVER_LOG="${TMP_DIR}/reasoning_server.log"
SCHEMA_WARM_FILE="${TMP_DIR}/schema_warm.json"
OBDA_STATE_FILE="${TMP_DIR}/obda_query_client_state.json"
OBDA_HOST_E2E_TRACE_DIR="${TMP_DIR}/traces"
SERVER_PID=""
RUN_STATUS=0

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${RUN_STATUS}" -eq 0 ]]; then
    rm -rf "${TMP_DIR}"
  else
    echo "KEEP tmp ${TMP_DIR}" >&2
  fi
}

trap cleanup EXIT

SERVER_PORT="$("${PYTHON_BIN}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
OBDA_BASE_URL="http://127.0.0.1:${SERVER_PORT}"
export OBDA_BASE_URL
export OBDA_STATE_FILE
export OBDA_HOST_E2E_TRACE_DIR
export ENABLE_TOOL_SEARCH="${ENABLE_TOOL_SEARCH:-false}"

echo "START server ${OBDA_BASE_URL}"
"${ROOT}/.venv/bin/uvicorn" reasoning_server:app --port "${SERVER_PORT}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"

echo "RUN schema warm"
ready=0
for _ in {1..30}; do
  if bash "${API}" schema > "${SCHEMA_WARM_FILE}" 2>"${TMP_DIR}/schema_warm.err"; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Host E2E regression server failed to become ready at ${OBDA_BASE_URL}" >&2
  if [[ -s "${TMP_DIR}/schema_warm.err" ]]; then
    cat "${TMP_DIR}/schema_warm.err" >&2
  fi
  if [[ -s "${SERVER_LOG}" ]]; then
    cat "${SERVER_LOG}" >&2
  fi
  exit 1
fi

"${PYTHON_BIN}" "${HELPER}" "$@" || RUN_STATUS=$?
exit "${RUN_STATUS}"
