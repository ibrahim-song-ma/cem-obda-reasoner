#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API="${ROOT}/.agents/skills/obda-query/scripts/obda_api.sh"
HELPER="${ROOT}/tests/run_question_regressions.py"
PYTHON_BIN="${ROOT}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Python virtualenv not found: ${PYTHON_BIN}" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/obda_question_regressions.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

DEFAULT_CASES=(
  causal_enumeration_network_complaints_plan
  anchored_numeric_status_single_run
  anchored_numeric_status_single_run_second_customer
  anchored_numeric_status_single_run_word_order_variant
  anchored_fact_lookup_metric_value_run
  batch_numeric_status_solution_plan
  batch_numeric_status_solution_run
  batch_numeric_status_solution_run_variant_have_or_not
  batch_numeric_status_reason_solution_run
  batch_explicit_entity_reference_solution_run
  batch_numeric_status_empty_followup_skipped
  batch_negative_branch_drops_positive_constraint
  batch_explicit_anchor_overrides_context_run
  batch_result_set_reference_followup_run
  batch_abstract_status_wrong_template_corrected
  batch_abstract_status_fail_closed_plan
  batch_abstract_status_execute_fail_closed
  anchored_solution_only_fail_closed
  explanation_enumeration_wrong_template_plan
  explanation_enumeration_wrong_template_run
  explanation_enumeration_direct_run
)

if [[ $# -gt 0 ]]; then
  CASES=("$@")
else
  CASES=("${DEFAULT_CASES[@]}")
fi

echo "RUN health"
bash "${API}" health > "${TMP_DIR}/health.json"

for case_id in "${CASES[@]}"; do
  payload_file="${TMP_DIR}/${case_id}.payload.json"
  response_file="${TMP_DIR}/${case_id}.response.json"

  echo "RUN ${case_id}"
  "${PYTHON_BIN}" "${HELPER}" --emit-case-payload "${case_id}" > "${payload_file}"
  bash "${API}" run --json-file "${payload_file}" > "${response_file}"
  "${PYTHON_BIN}" "${HELPER}" --assert-response-file "${case_id}" "${response_file}"
done

echo "PASS suite=obda_question_regressions cases=${#CASES[@]}"
