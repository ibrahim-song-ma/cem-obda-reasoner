#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${OBDA_BASE_URL:-http://127.0.0.1:8000}"

usage() {
  cat <<'EOF'
Usage:
  obda_api.sh health
  obda_api.sh schema
  obda_api.sh profiles
  obda_api.sh reload
  obda_api.sh sample <class_name> [limit]
  obda_api.sh causal <customer_id>
  obda_api.sh sparql --query "<SPARQL>"
  obda_api.sh sparql --query-file /path/to/query.rq
  obda_api.sh analysis-paths --json '<json>'
  obda_api.sh analysis-paths --json-file /path/to/payload.json
  obda_api.sh analysis-neighborhood --json '<json>'
  obda_api.sh analysis-inferred-relations --json '<json>'
  obda_api.sh analysis-explain --json '<json>'
EOF
}

json_payload_from_file() {
  local file_path="$1"
  cat "$file_path"
}

json_payload_from_query() {
  local query_text="$1"
  python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1]}, ensure_ascii=False))' "$query_text"
}

post_json() {
  local endpoint="$1"
  local payload="$2"
  curl -s -X POST "${BASE_URL}${endpoint}" \
    -H "Content-Type: application/json" \
    --data-binary "$payload"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

command="$1"
shift

case "$command" in
  health)
    curl -s "${BASE_URL}/health"
    ;;
  schema)
    curl -s "${BASE_URL}/schema"
    ;;
  profiles)
    curl -s "${BASE_URL}/analysis/profiles"
    ;;
  reload)
    curl -s -X POST "${BASE_URL}/reload"
    ;;
  sample)
    class_name="${1:-}"
    limit="${2:-3}"
    if [[ -z "$class_name" ]]; then
      echo "sample requires <class_name>" >&2
      exit 1
    fi
    curl -s "${BASE_URL}/sample/${class_name}?limit=${limit}"
    ;;
  causal)
    customer_id="${1:-}"
    if [[ -z "$customer_id" ]]; then
      echo "causal requires <customer_id>" >&2
      exit 1
    fi
    curl -s "${BASE_URL}/causal/${customer_id}"
    ;;
  sparql)
    if [[ $# -lt 2 ]]; then
      echo "sparql requires --query or --query-file" >&2
      exit 1
    fi
    case "$1" in
      --query)
        payload="$(json_payload_from_query "$2")"
        ;;
      --query-file)
        if [[ ! -f "$2" ]]; then
          echo "query file not found: $2" >&2
          exit 1
        fi
        query_text="$(cat "$2")"
        payload="$(json_payload_from_query "$query_text")"
        ;;
      *)
        echo "Unknown sparql option: $1" >&2
        exit 1
        ;;
    esac
    post_json "/sparql" "$payload"
    ;;
  analysis-paths|analysis-neighborhood|analysis-inferred-relations|analysis-explain)
    if [[ $# -lt 2 ]]; then
      echo "${command} requires --json or --json-file" >&2
      exit 1
    fi
    case "$1" in
      --json)
        payload="$2"
        ;;
      --json-file)
        if [[ ! -f "$2" ]]; then
          echo "json file not found: $2" >&2
          exit 1
        fi
        payload="$(json_payload_from_file "$2")"
        ;;
      *)
        echo "Unknown ${command} option: $1" >&2
        exit 1
        ;;
    esac
    case "$command" in
      analysis-paths) endpoint="/analysis/paths" ;;
      analysis-neighborhood) endpoint="/analysis/neighborhood" ;;
      analysis-inferred-relations) endpoint="/analysis/inferred-relations" ;;
      analysis-explain) endpoint="/analysis/explain" ;;
    esac
    post_json "$endpoint" "$payload"
    ;;
  *)
    echo "Unknown command: $command" >&2
    usage
    exit 1
    ;;
esac
