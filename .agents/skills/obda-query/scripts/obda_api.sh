#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${OBDA_BASE_URL:-http://127.0.0.1:8000}"
STATE_FILE="${OBDA_STATE_FILE:-/tmp/obda_query_client_state.json}"
SCHEMA_TTL_SECONDS="${OBDA_SCHEMA_TTL_SECONDS:-900}"
PY_CLIENT=".venv/bin/python"
PY_SCRIPT=".agents/skills/obda-query/scripts/obda_api.py"

usage() {
  cat <<'EOF'
Usage:
  obda_api.sh health
  obda_api.sh schema
  obda_api.sh profiles
  obda_api.sh templates
  obda_api.sh reload
  obda_api.sh sample <class_name> [limit]
  obda_api.sh causal <customer_id>
  obda_api.sh sparql --query "<SPARQL>"
  obda_api.sh sparql --query-file /path/to/query.rq   # only when you intentionally keep the query file
  obda_api.sh run --json '{"template":"fact_lookup", ...}'
  obda_api.sh run --json-file /path/to/run-plan.json
  obda_api.sh analysis-paths --json '<json>'
  obda_api.sh analysis-paths-batch --json '<json>'
  obda_api.sh analysis-paths --json-file /path/to/payload.json
  obda_api.sh analysis-neighborhood --json '<json>'
  obda_api.sh analysis-inferred-relations --json '<json>'
  obda_api.sh analysis-explain --json '<json>'
EOF
}

run_py_client() {
  if [[ ! -x "${PY_CLIENT}" ]]; then
    echo "Python virtualenv not found: ${PY_CLIENT}" >&2
    echo "Please create it first: python3 -m venv .venv" >&2
    exit 1
  fi

  "${PY_CLIENT}" "${PY_SCRIPT}" \
    --base-url "${BASE_URL}" \
    --state-file "${STATE_FILE}" \
    --schema-ttl-seconds "${SCHEMA_TTL_SECONDS}" \
    "$@"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

command="$1"
shift

case "$command" in
  health|schema|profiles|templates|reload)
    run_py_client "$command" "$@"
    ;;
  sample)
    class_name="${1:-}"
    limit="${2:-3}"
    if [[ -z "$class_name" ]]; then
      echo "sample requires <class_name>" >&2
      exit 1
    fi
    run_py_client sample "$class_name" --limit "$limit"
    ;;
  causal)
    customer_id="${1:-}"
    if [[ -z "$customer_id" ]]; then
      echo "causal requires <customer_id>" >&2
      exit 1
    fi
    run_py_client causal "$customer_id"
    ;;
  sparql)
    run_py_client sparql "$@"
    ;;
  run)
    run_py_client run "$@"
    ;;
  analysis-paths|analysis-paths-batch|analysis-neighborhood|analysis-inferred-relations|analysis-explain)
    run_py_client "$command" "$@"
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage
    exit 1
    ;;
esac
