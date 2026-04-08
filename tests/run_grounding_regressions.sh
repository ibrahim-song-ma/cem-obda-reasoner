#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${ROOT}/.venv/bin/python"
HELPER="${ROOT}/tests/run_grounding_regressions.py"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python virtualenv not found: ${PYTHON_BIN}" >&2
  exit 1
fi

"${PYTHON_BIN}" "${HELPER}" "$@"
