#!/usr/bin/env bash
set -euo pipefail

cat <<'JSON'
{
  "question_units": [
    {
      "unit_id": "q1",
      "raw_text": "13800138004是否存在满意度评分低于3的情况",
      "normalized_text": "13800138004是否存在满意度评分低于3的情况",
      "dependency": null,
      "reference_markers": [],
      "anchor_forms": [
        {
          "kind": "identifier_like_literal",
          "value": "13800138004"
        }
      ],
      "comparators": [
        {
          "attribute_text": "满意度评分",
          "op": "lt",
          "value": 3,
          "raw_text": "满意度评分低于3"
        }
      ],
      "question_acts": [
        "status_check"
      ],
      "surface_constraints": [
        {
          "slot": "status_or_problem_text",
          "text": "满意度评分",
          "kind": "comparison",
          "constraint": {
            "attribute_text": "满意度评分",
            "op": "lt",
            "value": 3,
            "raw_text": "满意度评分低于3"
          }
        }
      ],
      "ambiguities": [],
      "confidence": 0.93
    },
    {
      "unit_id": "q2",
      "raw_text": "有什么解决方案",
      "normalized_text": "有什么解决方案",
      "dependency": {
        "depends_on": "q1",
        "condition": "non_empty_or_true",
        "source": "conditional_prefix",
        "prefix": "如果有"
      },
      "reference_markers": [],
      "anchor_forms": [],
      "comparators": [],
      "question_acts": [
        "remediation"
      ],
      "surface_constraints": [
        {
          "slot": "target_text",
          "text": "解决方案",
          "kind": "semantic_hint"
        }
      ],
      "ambiguities": [],
      "confidence": 0.91
    }
  ],
  "parser_confidence": 0.92,
  "ambiguities": [],
  "clarification_candidates": [],
  "bootstrap_operator_hints": [
    {
      "operator": "status_check",
      "source": "model_question_act"
    }
  ]
}
JSON
