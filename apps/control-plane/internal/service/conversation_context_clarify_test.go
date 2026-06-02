package service

import "testing"

// silverone 2026-06-02 — 멀티턴 clarify. composer가 reason=missing_data_or_artifact
// 거절의 context_summary에 넣은 pending_clarification이 다음 turn conversation_context로
// forward돼야 planner가 후속 답을 이어받는다. whitelist에 키가 빠지면 신호가 유실된다.
func TestCompactConversationContextItem_ForwardsPendingClarification(t *testing.T) {
	item := compactConversationContextItem(map[string]any{
		"question":             "축제 전후 일주일 문서발생량",
		"answer_summary":       "축제 날짜(기준일)가 필요합니다.",
		"pending_clarification": true,
		"unrelated_field":      "drop me",
	})

	if item["pending_clarification"] != true {
		t.Fatalf("pending_clarification must be forwarded, got %#v", item["pending_clarification"])
	}
	if item["question"] != "축제 전후 일주일 문서발생량" {
		t.Errorf("question not forwarded: %#v", item["question"])
	}
	if item["answer_summary"] != "축제 날짜(기준일)가 필요합니다." {
		t.Errorf("answer_summary not forwarded: %#v", item["answer_summary"])
	}
	if _, ok := item["unrelated_field"]; ok {
		t.Errorf("non-whitelisted field must be dropped, got %#v", item["unrelated_field"])
	}
}
