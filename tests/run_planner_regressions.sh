#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API="${ROOT}/.agents/skills/obda-query/scripts/obda_api.sh"
HELPER="${ROOT}/tests/run_question_regressions.py"
SUITE="${ROOT}/tests/obda_planner_regressions.json"
PYTHON_BIN="${ROOT}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python virtualenv not found: ${PYTHON_BIN}" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/obda_planner_regressions.XXXXXX")"
SERVER_LOG="${TMP_DIR}/reasoning_server.log"
SCHEMA_WARM_FILE="${TMP_DIR}/schema_warm.json"
OBDA_STATE_FILE="${TMP_DIR}/obda_query_client_state.json"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

SERVER_PORT="$("${PYTHON_BIN}" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
OBDA_BASE_URL="http://127.0.0.1:${SERVER_PORT}"
export OBDA_BASE_URL
export OBDA_STATE_FILE

echo "START server ${OBDA_BASE_URL}"
"${ROOT}/.venv/bin/uvicorn" reasoning_server:app --port "${SERVER_PORT}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"

echo "RUN schema"
ready=0
for _ in {1..30}; do
  if bash "${API}" schema > "${SCHEMA_WARM_FILE}" 2>"${TMP_DIR}/schema_warm.err"; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Regression server failed to become ready at ${OBDA_BASE_URL}" >&2
  if [[ -s "${TMP_DIR}/schema_warm.err" ]]; then
    cat "${TMP_DIR}/schema_warm.err" >&2
  fi
  if [[ -s "${SERVER_LOG}" ]]; then
    cat "${SERVER_LOG}" >&2
  fi
  exit 1
fi

DEFAULT_CASES=(
  causal_enumeration_network_complaints_plan
  batch_numeric_status_solution_plan
  batch_abstract_status_fail_closed_plan
  anchored_solution_only_fail_closed
  explanation_enumeration_wrong_template_plan
)

if [[ $# -gt 0 ]]; then
  CASES=("$@")
else
  CASES=("${DEFAULT_CASES[@]}")
fi

for case_id in "${CASES[@]}"; do
  payload_file="${TMP_DIR}/${case_id}.payload.json"
  response_file="${TMP_DIR}/${case_id}.response.json"

  echo "RUN ${case_id}"
  "${PYTHON_BIN}" "${HELPER}" --suite "${SUITE}" --emit-case-payload "${case_id}" > "${payload_file}"
  bash "${API}" run --json-file "${payload_file}" > "${response_file}"
  "${PYTHON_BIN}" "${HELPER}" --suite "${SUITE}" --assert-response-file "${case_id}" "${response_file}"
done

echo "PASS suite=obda_planner_regressions cases=${#CASES[@]}"
