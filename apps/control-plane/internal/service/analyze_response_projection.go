package service

import (
	"encoding/json"
)

// silverone 2026-05-28 — frontend-safe analyze response projection.
//
// stateful frontend path (`POST /datasets/{did}/analyze`,
// `POST /analysis_threads/{tid}/messages`)의 응답 `result`에서 운영자/debug
// 필드를 server-side stripping해서 화면 표시용 필드만 남긴다. DB에는 full
// result_json이 그대로 보존된다 — projection은 응답 직전에 적용한다.
//
// `POST /versions/{vid}/analyze` (debug/replay)는 변경 X — 옛 full result 유지.
//
// Keep set (사용자 결정 2026-05-28):
//   - result.plan : plan_version + steps[].id / .skill / .params
//     + (answerable=false 거절 시) answerable / reason / message / capability_gap
//   - result.composer.assistant_content
//   - result.composer.metadata.{mode,template,fallback_reason,reason,capability_gap}
//   - result.composer.display.{type,title,columns,rows,total_rows,
//     returned_rows,max_rows,truncated,warnings,recommended_view,chart_spec,
//     column_formats,column_labels}
//   - result.taxonomy_check (전체 객체)
//
// Drop set:
//   - result.steps / result.steps[].sample_rows
//   - result.planner.{usage, attempts, attempts[].raw}
//   - result.artifact_paths
//   - result.composer.context_summary
//   - raw prompt / token usage / internal artifact paths
//   - 그 외 정의되지 않은 모든 top-level result key (default deny)

// projectFrontendAnalyzeResult — result raw JSON에서 frontend-safe 필드만 추출.
// 입력이 비어있거나 JSON object가 아니면 원본 그대로 반환 (best-effort, 실패는
// 응답을 막지 않는다).
func projectFrontendAnalyzeResult(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return raw
	}
	projected := map[string]any{}
	if plan := projectAnalyzePlan(root["plan"]); plan != nil {
		projected["plan"] = plan
	}
	if composer := projectAnalyzeComposer(root["composer"]); composer != nil {
		projected["composer"] = composer
	}
	if taxonomy, ok := root["taxonomy_check"].(map[string]any); ok {
		projected["taxonomy_check"] = taxonomy
	}
	encoded, err := json.Marshal(projected)
	if err != nil {
		return raw
	}
	return encoded
}

func projectAnalyzePlan(value any) map[string]any {
	plan, ok := value.(map[string]any)
	if !ok || len(plan) == 0 {
		return nil
	}
	out := map[string]any{}
	if pv, ok := plan["plan_version"].(string); ok && pv != "" {
		out["plan_version"] = pv
	}
	// silverone 2026-06-01 (PR1, reject reason) — answerable=false 거절 plan의
	// reason/message/capability_gap을 프론트·rejection event(PR2)용으로 노출.
	// answerable 미지정/true는 기존 plan과 동일하게 steps만 노출.
	if answerable, ok := plan["answerable"].(bool); ok {
		out["answerable"] = answerable
		if !answerable {
			if reason, ok := plan["reason"].(string); ok && reason != "" {
				out["reason"] = reason
			}
			if message, ok := plan["message"].(string); ok && message != "" {
				out["message"] = message
			}
			if gap, ok := plan["capability_gap"].(map[string]any); ok && len(gap) > 0 {
				out["capability_gap"] = gap
			}
		}
	}
	rawSteps, _ := plan["steps"].([]any)
	steps := make([]map[string]any, 0, len(rawSteps))
	for _, item := range rawSteps {
		step, ok := item.(map[string]any)
		if !ok {
			continue
		}
		stepView := map[string]any{}
		if id, ok := step["id"].(string); ok && id != "" {
			stepView["id"] = id
		}
		if skill, ok := step["skill"].(string); ok && skill != "" {
			stepView["skill"] = skill
		}
		if params, ok := step["params"].(map[string]any); ok {
			stepView["params"] = params
		} else {
			stepView["params"] = map[string]any{}
		}
		// silverone 2026-06-01 (plan step display projection) — 백엔드가 step별
		// 사용자 화면용 label / expression을 합성해서 내려준다. 프론트는 raw
		// params를 직접 해석하지 않고 display.label / display.expression을 우선
		// 표시. display 합성 실패 시 omit — 프론트가 params JSON으로 fallback.
		//
		// silverone 2026-06-04 (Skill Contract v2 Step 3) — display source of truth가
		// worker(Python planner.step_display)로 이동. worker가 plan.steps[].display를
		// 채워 내려보내면 그대로 pass-through하고, 없으면(옛 worker / 미지원 skill)
		// 기존 Go buildStepDisplay로 fallback (이번 PR에서 Go builder 유지).
		if display, ok := step["display"].(map[string]any); ok && len(display) > 0 {
			stepView["display"] = display
		} else if display := buildStepDisplay(step); display != nil {
			stepView["display"] = display
		}
		steps = append(steps, stepView)
	}
	out["steps"] = steps
	return out
}

func projectAnalyzeComposer(value any) map[string]any {
	composer, ok := value.(map[string]any)
	if !ok || len(composer) == 0 {
		return nil
	}
	out := map[string]any{}
	if content, ok := composer["assistant_content"].(string); ok {
		out["assistant_content"] = content
	}
	if meta, ok := composer["metadata"].(map[string]any); ok {
		metaView := map[string]any{}
		// silverone 2026-06-01 (PR1, reject reason) — mode=rejected일 때 reason을
		// 노출해 프론트가 reason별 처리, 운영이 rejection event(PR2) 집계 가능.
		for _, key := range [...]string{"mode", "template", "fallback_reason", "reason"} {
			if v, exists := meta[key]; exists {
				metaView[key] = v
			}
		}
		if gap, ok := meta["capability_gap"].(map[string]any); ok && len(gap) > 0 {
			metaView["capability_gap"] = gap
		}
		out["metadata"] = metaView
	}
	if display := projectComposerDisplay(composer["display"]); display != nil {
		out["display"] = display
	}
	// silverone 2026-06-08 (작업 1) — graceful 거절 시 대체 질문을 프론트로 전달.
	// 문자열 리스트만 통과시킨다.
	if raw, ok := composer["suggested_questions"].([]any); ok && len(raw) > 0 {
		suggested := make([]string, 0, len(raw))
		for _, item := range raw {
			if s, ok := item.(string); ok && s != "" {
				suggested = append(suggested, s)
			}
		}
		if len(suggested) > 0 {
			out["suggested_questions"] = suggested
		}
	}
	return out
}

// projectComposerDisplay — display map에서 frontend-safe keep set만 추출.
// silverone 2026-06-01 — thread detail messages[].display 재사용 위해 분리.
// keep set: type / title / columns / rows / total_rows / returned_rows /
// max_rows / truncated / warnings / recommended_view / chart_spec /
// column_formats / column_labels.
// recommended_view + chart_spec은 chart-ready metadata v1 (silverone 2026-06-01)
// — display.type은 "table" 유지하고 프론트가 차트 렌더링에 활용할 힌트만 추가.
// column_formats + column_labels는 기간/그룹 비교 결과 표시 contract
// (silverone 2026-06-09) — 프론트가 %·%p·정수로 렌더하도록 단위 의미 전달.
func projectComposerDisplay(value any) map[string]any {
	display, ok := value.(map[string]any)
	if !ok || len(display) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, key := range [...]string{
		"type", "title", "columns", "rows",
		"total_rows", "returned_rows", "max_rows", "truncated", "warnings",
		"recommended_view", "chart_spec",
		"column_formats", "column_labels",
	} {
		if v, exists := display[key]; exists {
			out[key] = v
		}
	}
	return out
}

// extractDisplayFromResultJSON — run.result_json (analyze 응답 raw) 에서
// composer.display의 frontend-safe projection을 꺼낸다. silverone 2026-06-01
// — thread detail이 과거 turn의 표를 렌더링할 수 있도록 GetAnalysisThread가
// assistant message에 attach. 입력이 비어있거나 display가 없으면 nil.
func extractDisplayFromResultJSON(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	composer, ok := root["composer"].(map[string]any)
	if !ok {
		return nil
	}
	return projectComposerDisplay(composer["display"])
}

// extractPlanFromResultJSON — run.result_json에서 plan keep-set을 꺼낸다.
// silverone 2026-06-01 — thread detail이 과거 turn의 분석 계획을 보여줄 수
// 있도록 GetAnalysisThread가 assistant message에 attach. POST 응답의
// `result.plan`과 동일 shape (plan_version + steps[].id/.skill/.params).
// 입력이 비어있거나 plan이 없으면 nil. step status/duration_ms/row_count 같은
// 추가 메타는 후속 PR.
func extractPlanFromResultJSON(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	return projectAnalyzePlan(root["plan"])
}
