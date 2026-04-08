#!/usr/bin/env bash
set -euo pipefail

cat <<'JSON'
{
  "question_units": [
    {
      "unit_id": "q1",
      "raw_text": "13800138004是否存在满意度评分低于3的情况？如果有，有什么解决方案？",
      "normalized_text": "13800138004是否存在满意度评分低于3的情况？如果有，有什么解决方案？",
      "dependency": null,
      "reference_markers": [],
      "anchor_forms": [
        {
          "kind": "identifier_like_literal",
          "value": "13800138004"
        }
      ],
      "comparators": [],
      "question_acts": [
        "lookup"
      ],
      "surface_constraints": [],
      "ambiguities": [],
      "confidence": 0.7
    }
  ],
  "parser_confidence": 0.7,
  "ambiguities": [],
  "clarification_candidates": []
}
JSON
