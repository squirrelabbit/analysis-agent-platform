package service

import (
	"encoding/json"
	"testing"
)

// silverone 2026-06-01 (PR2) — rejectionEventFromResult 추출 규칙 잠금.
// unsupported_skill / missing_data_or_artifact만 적재, out_of_dataset_scope 제외.

func TestRejectionEventFromResult_UnsupportedSkill(t *testing.T) {
	raw := json.RawMessage(`{
		"composer": {
			"assistant_content": "현재 분석 기능으로는 처리할 수 없습니다.",
			"metadata": {
				"mode": "rejected",
				"reason": "unsupported_skill",
				"capability_gap": {"suggested_skill": "cluster_texts", "requested_capability": "text_clustering"}
			}
		}
	}`)
	ev, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "비슷한 후기끼리 묶어줘", raw)
	if !ok {
		t.Fatalf("expected event for unsupported_skill")
	}
	if ev.Reason != "unsupported_skill" {
		t.Errorf("reason = %q", ev.Reason)
	}
	if ev.ProjectID != "p1" || ev.DatasetID != "d1" || ev.ThreadID != "t1" || ev.MessageID != "m1" {
		t.Errorf("ids not propagated: %+v", ev)
	}
	if ev.UserQuestion != "비슷한 후기끼리 묶어줘" {
		t.Errorf("user_question = %q", ev.UserQuestion)
	}
	if ev.Message == "" {
		t.Errorf("message must come from assistant_content")
	}
	if ev.CapabilityGap["suggested_skill"] != "cluster_texts" {
		t.Errorf("capability_gap missing: %v", ev.CapabilityGap)
	}
	if ev.EventID == "" {
		t.Errorf("event_id must be generated")
	}
}

func TestRejectionEventFromResult_MissingDataStored(t *testing.T) {
	raw := json.RawMessage(`{"composer":{"assistant_content":"created_at 없음","metadata":{"mode":"rejected","reason":"missing_data_or_artifact"}}}`)
	ev, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "날짜별 추이", raw)
	if !ok || ev.Reason != "missing_data_or_artifact" {
		t.Fatalf("missing_data_or_artifact must be stored, ok=%v reason=%q", ok, ev.Reason)
	}
	if ev.CapabilityGap != nil {
		t.Errorf("no capability_gap expected, got %v", ev.CapabilityGap)
	}
}

func TestRejectionEventFromResult_PlannerValidationStored(t *testing.T) {
	// silverone 2026-06-08 — planner self-correct 소진(complex 교차 분석) 거절도
	// 고도화 재료로 적재. (D4 "축제 전후 부정 비율 변화" 류)
	raw := json.RawMessage(`{"composer":{"assistant_content":"실행 가능한 분석 계획으로 변환하지 못했습니다.","metadata":{"mode":"rejected","reason":"planner_validation_error"}}}`)
	ev, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "축제 전후 부정 의견 비율이 어떻게 달라졌는지 보여줘", raw)
	if !ok || ev.Reason != "planner_validation_error" {
		t.Fatalf("planner_validation_error must be stored, ok=%v reason=%q", ok, ev.Reason)
	}
	if ev.UserQuestion != "축제 전후 부정 의견 비율이 어떻게 달라졌는지 보여줘" {
		t.Errorf("user_question = %q", ev.UserQuestion)
	}
	if ev.DatasetID != "d1" || ev.ThreadID != "t1" || ev.MessageID != "m1" {
		t.Errorf("ids not propagated: %+v", ev)
	}
}

func TestRejectionEventFromResult_ExecutionErrorStored(t *testing.T) {
	raw := json.RawMessage(`{"composer":{"assistant_content":"분석 계획 실행 중 오류가 발생했습니다.","metadata":{"mode":"rejected","reason":"execution_error"}}}`)
	ev, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "축제 전후 aspect 증감률", raw)
	if !ok || ev.Reason != "execution_error" {
		t.Fatalf("execution_error must be stored, ok=%v reason=%q", ok, ev.Reason)
	}
}

func TestRejectionEventFromResult_OutOfScopeNotStored(t *testing.T) {
	raw := json.RawMessage(`{"composer":{"assistant_content":"날씨는 범위 밖","metadata":{"mode":"rejected","reason":"out_of_dataset_scope"}}}`)
	if _, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "오늘 날씨", raw); ok {
		t.Errorf("out_of_dataset_scope must NOT be stored")
	}
}

func TestRejectionEventFromResult_NormalResultNotStored(t *testing.T) {
	// 정상 답변(mode=deterministic, reason 없음) → 적재 안 함.
	raw := json.RawMessage(`{"composer":{"assistant_content":"분석 결과 9건","metadata":{"mode":"deterministic"}}}`)
	if _, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "aspect별 건수", raw); ok {
		t.Errorf("normal result must NOT be stored")
	}
}

func TestRejectionEventFromResult_EmptyOrInvalid(t *testing.T) {
	for _, raw := range []json.RawMessage{nil, json.RawMessage(`{}`), json.RawMessage(`not json`)} {
		if _, ok := rejectionEventFromResult("p1", "d1", "t1", "m1", "q", raw); ok {
			t.Errorf("empty/invalid result must NOT produce event: %s", raw)
		}
	}
}
