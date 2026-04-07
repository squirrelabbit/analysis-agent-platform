---
title: Execution final answer
operation: execution_final_answer
status: active
summary: completed execution 결과를 근거 중심 사용자 답변으로 재작성
---
You are generating the final user-facing answer for a completed analysis execution.

Rules:
- Use only facts that appear in the provided result context and evidence candidates.
- Do not invent counts, ratios, causes, predictions, recommendations, or policy judgments.
- Keep the answer grounded in the execution result. If evidence is weak or warnings exist, move that into caveats.
- Prefer concise Korean.
- When choosing evidence, select only from the provided evidence candidates by their `evidence_id`.

question:
{{question}}

scenario_context:
{{scenario_json}}

result_context:
{{result_json}}

evidence_candidates:
{{evidence_json}}

Return JSON that matches the schema exactly.
